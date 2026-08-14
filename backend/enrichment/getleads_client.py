"""
GetLeads (app.getleads.io) client for email enrichment.

Wraps the GetLeads "from-person" enrichment endpoint. Given a first name,
last name, and company domain, returns the best-matched email plus
opportunistic phone + LinkedIn data.

GetLeads provides:
- Account limit of 100 requests/minute GLOBAL across all gunicorn workers.
- Up to 100 items per request (server hard cap; 400 if exceeded).
- Batch responses echo input order, one ``results`` entry per input item.

CRITICAL — "found" semantics: GetLeads NEVER returns ``success: false``. A
not-found lookup comes back as ``success: true, email: null, data: null``
(or ``data`` populated with a person record but no ``email_address``). We
therefore gate "found" on ``data.email_address`` being truthy, NOT on the
``success`` flag. See /tmp/getleads_live_shapes.md (authoritative).

Env vars:
    GETLEADS_API_KEY          API key (required for calls; empty = disabled)
    GETLEADS_BASE_URL         Optional base URL override
    GETLEADS_RATE_LIMIT_RPM   Shared cross-process cap in requests/minute
                              (default "95" — account limit is 100 RPM GLOBAL;
                              enforced via shared/rate_limiter.py backed by
                              SQLite so all gunicorn workers share ONE bucket)
    GETLEADS_RATE_LIMIT_RPS   DEPRECATED legacy per-process RPS cap. If set,
                              RPM = RPS * 60 (back-compat only; warns once).
    GETLEADS_RATE_LIMIT_BURST Optional burst capacity in tokens
                              (default = refill_per_sec, i.e. ~1 call of slack)
    ENABLE_GETLEADS           Kill switch. "false" disables calls immediately.

Usage:
    result = await getleads_client.find_email(
        client,
        first_name="John",
        last_name="Doe",
        company_domain="example.com",
    )

    results = await getleads_client.find_emails_batch(
        client,
        contacts=[
            {"first_name": "John", "last_name": "Doe", "email_domain": "example.com"},
            ...
        ],
    )
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
from typing import Any, Optional

import httpx

from . import pipeline  # For _ProviderError (insufficient_credits signaling)
from shared import rate_limiter
from shared.circuit_breaker import get_circuit_breaker, CircuitBreakerError

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_URL = os.getenv(
    "GETLEADS_BASE_URL",
    "https://app.getleads.io",
)
API_KEY = os.getenv("GETLEADS_API_KEY", "")

# Account limit is 100 req/min GLOBAL across all gunicorn workers. The cap is
# enforced by a SHARED SQLite-backed token bucket (shared/rate_limiter.py) so
# all workers draw from ONE bucket — the legacy per-process limiter let 4
# workers collectively run ~4x the cap and eat steady 429s. Default 95 RPM
# leaves headroom under the 100 RPM account limit.
_LEGACY_RPS_RAW = os.getenv("GETLEADS_RATE_LIMIT_RPS")
if _LEGACY_RPS_RAW:
    _RATE_LIMIT_RPM = float(_LEGACY_RPS_RAW) * 60.0
    logger.warning(
        "GETLEADS_RATE_LIMIT_RPS is deprecated (per-process; it under-limited "
        "in multi-worker deployments). Use GETLEADS_RATE_LIMIT_RPM instead. "
        "Continuing with %.1f RPM derived from the legacy value.",
        _RATE_LIMIT_RPM,
    )
else:
    _RATE_LIMIT_RPM = float(os.getenv("GETLEADS_RATE_LIMIT_RPM", "95"))

_REFILL_PER_SEC = _RATE_LIMIT_RPM / 60.0
_CAPACITY = float(os.getenv("GETLEADS_RATE_LIMIT_BURST", str(_REFILL_PER_SEC)))

# Endpoint path for the from-person enrichment API (auth via Bearer header).
_FROM_PERSON_PATH = "/api/v1/enrich/from-person"

# Max contacts per request as enforced by the upstream API (server hard cap;
# 400 with "At most 100 items per request" when exceeded).
_MAX_CONTACTS_PER_REQUEST = 100

# Retry config: up to 3 retries (4 attempts total), full-jitter backoff.
_MAX_RETRIES = 3
_BASE_BACKOFF = 2.0
_MAX_BACKOFF = 30.0
_REQUEST_TIMEOUT = 60.0

# ---------------------------------------------------------------------------
# Circuit breaker — fail fast when the upstream is consistently erroring.
# ---------------------------------------------------------------------------

_getleads_circuit = get_circuit_breaker(
    "getleads_api",
    failure_threshold=10,
    recovery_timeout=60.0,
    half_open_max_calls=3,
)

# ---------------------------------------------------------------------------
# Rate limiter (shared cross-process token bucket — see shared/rate_limiter.py).
# ---------------------------------------------------------------------------


async def _acquire_rate_limit() -> None:
    """Ensure we don't exceed the shared cross-process RPM rate limit.

    Loops until a token is actually GRANTED. A single sleep-and-proceed is not
    sufficient: concurrent denials compute similar waits and would all proceed
    together after sleeping, admitting N calls per ~1 refilled token and
    bursting past the account limit (observed as a 429 storm on 2026-08-14).
    Only a grant consumes a token, so re-acquiring after each sleep bounds the
    true global HTTP rate to refill_per_sec.
    """
    while True:
        wait = await asyncio.to_thread(
            rate_limiter.acquire_token, "getleads", _REFILL_PER_SEC, _CAPACITY
        )
        if wait <= 0:
            return
        await asyncio.sleep(wait)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _is_enabled() -> bool:
    """Return True if the GetLeads kill switch is not active."""
    return os.environ.get("ENABLE_GETLEADS", "true").lower() == "true"


def _should_retry(status_code: int) -> bool:
    """
    Decide whether a status code is worth retrying.

    Retry: 429 (rate limit), 5xx (server errors).
    Do NOT retry: 400 (bad payload / batch>100), 402 (no credits),
                  403/404 and other 4xx (client errors).
    """
    return status_code == 429 or (500 <= status_code < 600)


def _backoff_delay(attempt: int, retry_after: Optional[float] = None) -> float:
    """Compute a sleep duration for the next retry attempt (full jitter)."""
    if retry_after is not None:
        return min(retry_after, _MAX_BACKOFF)
    cap = min(_MAX_BACKOFF, _BASE_BACKOFF * (2 ** attempt))
    return random.uniform(0, cap)


def _normalize_contact(raw: dict[str, str]) -> Optional[dict[str, str]]:
    """
    Validate + normalize a single contact dict into the API payload shape.

    Returns None if the contact is missing required fields. Does NOT mutate
    the input dict (immutable pattern).
    """
    first_name = (raw.get("first_name") or raw.get("firstName") or "").strip()
    last_name = (raw.get("last_name") or raw.get("lastName") or "").strip()
    domain = (
        raw.get("email_domain")
        or raw.get("company_domain")
        or raw.get("companyDomain")
        or raw.get("domain")
        or ""
    ).strip()

    if not first_name or not last_name or not domain:
        return None

    return {
        "first_name": first_name,
        "last_name": last_name,
        "email_domain": domain,
    }


def _clean_str(value: Any) -> str:
    """Coerce a GetLeads data value to a stripped string ('' for missing)."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def _normalize_result_item(item: dict[str, Any]) -> dict[str, Any]:
    """
    Convert one GetLeads ``results[]`` item into the canonical dict shape.

    Does not raise — missing keys become defaults. Email is "" when not found.

    CRITICAL: "found" = ``data.email_address`` truthy. The API NEVER returns
    ``success: false``; not-found is ``success: true`` + ``email: null`` +
    ``data: null`` (or ``data`` populated but no ``email_address``).
    """
    data = item.get("data") or {}
    email_address = data.get("email_address") or ""

    first_name = item.get("first_name") or data.get("first_name") or ""
    last_name = item.get("last_name") or data.get("last_name") or ""
    domain = item.get("email_domain") or data.get("email_domain") or ""

    if not email_address:
        # Not found — data may be null or a partial person record.
        return _not_found_contact(first_name, last_name, domain)

    email_status = data.get("email_status")
    verification_status = "Valid" if email_status == "VALID" else "unknown"
    linkedin_url = data.get("person_linkedin_url") or item.get("profileUrl") or ""
    phone = data.get("cellphone") or data.get("direct_phone") or ""

    return {
        "email": email_address,
        "first_name": first_name,
        "last_name": last_name,
        "domain": domain,
        "verification_status": verification_status,
        "linkedin_url": linkedin_url,
        "phone": phone,
        # Phase 2 (full capture): the remaining GetLeads data fields, mapped
        # from the raw keys documented in /tmp/getleads_live_shapes.md §"data".
        # Values are coerced to stripped strings (e.g. linkedin_connections_
        # count arrives as an int) so downstream consumers see a uniform shape.
        "job_title": _clean_str(data.get("job_title")),
        "linkedin_headline": _clean_str(data.get("linkedin_headline")),
        "person_full_name": _clean_str(data.get("person_full_name")),
        "company_name": _clean_str(data.get("org_company_name")),
        "company_industry": _clean_str(data.get("industry_linkedin_org")),
        "employee_count": _clean_str(data.get("employee_count_range_org")),
        "revenue": _clean_str(data.get("revenue_range_org")),
        "city": _clean_str(data.get("person_city")),
        "country": _clean_str(data.get("person_country_name")),
        "linkedin_connections": _clean_str(data.get("linkedin_connections_count")),
        "email_last_verified_at": _clean_str(data.get("email_last_verified_at")),
        "job_level": _clean_str(data.get("job_level")),
        "job_function": _clean_str(data.get("job_function")),
        # Raw passthrough for forward-compat (new GetLeads fields become
        # available downstream without another client release).
        "_raw_getleads": data,
    }


def _not_found_contact(
    first_name: str = "",
    last_name: str = "",
    domain: str = "",
) -> dict[str, Any]:
    """Build a normalized Not Found entry (used for padding / errors)."""
    return {
        "email": "",
        "first_name": first_name,
        "last_name": last_name,
        "domain": domain,
        "verification_status": "unknown",
        "linkedin_url": "",
        "phone": "",
        # Mirrored Phase 2 keys (all empty) so downstream spreads over a
        # mixed found/not-found list see a uniform shape.
        "job_title": "",
        "linkedin_headline": "",
        "person_full_name": "",
        "company_name": "",
        "company_industry": "",
        "employee_count": "",
        "revenue": "",
        "city": "",
        "country": "",
        "linkedin_connections": "",
        "email_last_verified_at": "",
        "job_level": "",
        "job_function": "",
    }


def _insufficient_credits_error(method: str) -> pipeline._ProviderError:
    """Build the standard insufficient-credits _ProviderError for GetLeads."""
    return pipeline._ProviderError(
        provider="getleads",
        method=method,
        error_type="insufficient_credits",
        message="GetLeads: Insufficient credits. Please top up to continue.",
    )


def _chunk(seq: list[Any], size: int) -> list[list[Any]]:
    """Split a list into chunks of `size`, preserving order (immutable)."""
    return [seq[i : i + size] for i in range(0, len(seq), size)]


# ---------------------------------------------------------------------------
# Core request helper
# ---------------------------------------------------------------------------


async def _post_from_person(
    client: httpx.AsyncClient,
    payload_items: list[dict[str, str]],
) -> Optional[list[dict[str, Any]]]:
    """
    POST one chunk (<=100 items) to the from-person enrichment endpoint.

    Auth is via ``Authorization: Bearer <key>`` header (NOT a query param).

    Returns:
        - list of normalized result dicts on success
        - _ProviderError instance when 402 (insufficient credits) — caller checks
        - None on unrecoverable failure / circuit open / kill switch

    Note: a _ProviderError is falsy, so callers using `if not result:` will
    treat it as a skip; callers that need to distinguish should use
    `pipeline._is_provider_error(result)`.
    """
    if not _is_enabled():
        logger.debug("GetLeads kill switch (ENABLE_GETLEADS=false) active")
        return None

    if not API_KEY:
        logger.warning("GetLeads API key not configured, skipping")
        return None

    # Circuit breaker — fail fast if upstream is broken.
    if not await _getleads_circuit.can_proceed():
        logger.warning("GetLeads API circuit breaker OPEN, failing fast")
        return None

    url = f"{BASE_URL}{_FROM_PERSON_PATH}"

    for attempt in range(_MAX_RETRIES + 1):
        await _acquire_rate_limit()

        try:
            resp = await client.post(
                url,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {API_KEY}",
                },
                json={"items": payload_items},
                timeout=_REQUEST_TIMEOUT,
            )

            # 402 — no credits. Do NOT retry. Surface as _ProviderError.
            if resp.status_code == 402:
                await _getleads_circuit.record_failure()
                logger.warning("GetLeads: Insufficient credits (402)")
                return _insufficient_credits_error("find_emails_batch")

            # 400 — bad payload (batch>100, empty items). Do NOT retry.
            # Treat as a chunk failure (pads with Not Found entries upstream).
            if resp.status_code == 400:
                await _getleads_circuit.record_failure()
                logger.debug(
                    "GetLeads validation error (400): %s",
                    resp.text[:200],
                )
                return None

            # Retryable status (429 / 5xx).
            if _should_retry(resp.status_code):
                # 429 = rate-limit, NOT a failure — don't trip the breaker
                # (mirrors blitz/smartprospect fix). Retry/backoff self-limits.
                if resp.status_code != 429:
                    await _getleads_circuit.record_failure()
                retry_after_raw = resp.headers.get("Retry-After")
                retry_after = float(retry_after_raw) if retry_after_raw else None
                delay = _backoff_delay(attempt, retry_after)
                if attempt < _MAX_RETRIES:
                    logger.warning(
                        "GetLeads returned %d (attempt %d/%d), retrying in %.1fs",
                        resp.status_code,
                        attempt + 1,
                        _MAX_RETRIES + 1,
                        delay,
                    )
                    await asyncio.sleep(delay)
                    continue
                logger.error(
                    "GetLeads returned %d, exhausted retries",
                    resp.status_code,
                )
                return None

            # Non-retryable, non-success status (4xx other than 400/402).
            if resp.status_code >= 400:
                await _getleads_circuit.record_failure()
                logger.error(
                    "GetLeads non-retryable error %d: %s",
                    resp.status_code,
                    resp.text[:200],
                )
                return None

            # Success.
            resp.raise_for_status()
            await _getleads_circuit.record_success()

            data = resp.json()
            items = data.get("results") or []
            return [_normalize_result_item(item) for item in items]

        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code if exc.response is not None else 0
            # 429 = rate-limit, NOT a failure — don't trip the breaker.
            if status != 429:
                await _getleads_circuit.record_failure()
            if status == 402:
                logger.warning("GetLeads: Insufficient credits (402)")
                return _insufficient_credits_error("find_emails_batch")
            if attempt < _MAX_RETRIES and _should_retry(status):
                delay = _backoff_delay(attempt)
                logger.warning(
                    "GetLeads HTTP error (attempt %d/%d): %s — retrying in %.1fs",
                    attempt + 1,
                    _MAX_RETRIES,
                    exc,
                    delay,
                )
                await asyncio.sleep(delay)
                continue
            logger.error("GetLeads HTTP error, giving up: %s", exc)
            return None

        except (httpx.RequestError, asyncio.TimeoutError) as exc:
            await _getleads_circuit.record_failure()
            if attempt < _MAX_RETRIES:
                delay = _backoff_delay(attempt)
                logger.warning(
                    "GetLeads network error (attempt %d/%d): %s — retrying in %.1fs",
                    attempt + 1,
                    _MAX_RETRIES,
                    exc,
                    delay,
                )
                await asyncio.sleep(delay)
                continue
            logger.error("GetLeads network error, exhausted retries: %s", exc)
            return None

        except CircuitBreakerError as exc:
            logger.warning("GetLeads circuit breaker tripped: %s", exc)
            return None

        except Exception as exc:
            await _getleads_circuit.record_failure()
            if attempt < _MAX_RETRIES:
                delay = _backoff_delay(attempt)
                logger.warning(
                    "GetLeads unexpected error (attempt %d/%d): %s — retrying in %.1fs",
                    attempt + 1,
                    _MAX_RETRIES,
                    exc,
                    delay,
                )
                await asyncio.sleep(delay)
                continue
            logger.error("GetLeads unexpected error, giving up: %s", exc)
            return None

    logger.error("GetLeads: exhausted all retries")
    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def find_email(
    client: httpx.AsyncClient,
    first_name: str,
    last_name: str,
    company_domain: str,
) -> Optional[dict[str, Any]]:
    """
    Look up a single contact's email via GetLeads.

    Args:
        client: httpx.AsyncClient used for the HTTP call.
        first_name: Contact first name (non-empty).
        last_name: Contact last name (non-empty).
        company_domain: Contact's company domain (non-empty).

    Returns:
        Normalized dict on success (``data.email_address`` truthy):
            {
                "email": "john.doe@example.com",
                "first_name": "John",
                "last_name": "Doe",
                "domain": "example.com",
                "verification_status": "Valid",  # or "unknown"
                "linkedin_url": "https://www.linkedin.com/in/...",
                "phone": "+1 555-1234",
            }

        Returns None when:
            - kill switch is off
            - API key is missing
            - input is invalid
            - the lookup finds no email (GetLeads returns success:true +
              email:null/data:null — we treat "no email_address" as not found)
            - the call fails after retries
        Returns a pipeline._ProviderError (falsy) when GetLeads reports
            insufficient credits (402).
    """
    if not _is_enabled():
        logger.debug("GetLeads kill switch (ENABLE_GETLEADS=false) active")
        return None

    if not API_KEY:
        logger.warning("GetLeads API key not configured, skipping")
        return None

    if not first_name or not last_name or not company_domain:
        logger.debug(
            "GetLeads requires first_name, last_name, and company_domain"
        )
        return None

    payload_item = {
        "first_name": str(first_name).strip(),
        "last_name": str(last_name).strip(),
        "email_domain": str(company_domain).strip(),
    }

    result = await _post_from_person(client, [payload_item])

    # Distinguish a _ProviderError from a real None.
    if pipeline._is_provider_error(result):
        # Re-wrap with the single-contact method name.
        return _insufficient_credits_error("find_email")

    if not result:
        # None (failure) or empty results array — treat as not found.
        return None

    item = result[0]
    # Not-found items carry email="" — surface as None per the contract.
    if not item.get("email"):
        return None

    return item


async def find_emails_batch(
    client: httpx.AsyncClient,
    contacts: list[dict[str, str]],
) -> list[dict[str, Any]]:
    """
    Batch email lookup. Auto-chunks to groups of 100 (the API max).

    Args:
        client: httpx.AsyncClient used for HTTP calls.
        contacts: List of contact dicts. Each dict may use the API's
            snake_case keys (first_name, last_name, email_domain) or the
            project's alternate keys (firstName, lastName,
            company_domain / companyDomain / domain).

    Returns:
        A list with exactly one entry per input contact, preserving input
        order. Each entry is a normalized dict (same shape as find_email's
        return). Not-found contacts appear with email="" and
        verification_status="unknown".

        If the kill switch is off, the API key is missing, or the input list
        is empty, returns [] (or a list of Not Found entries for non-empty
        input when the key is missing — to keep length == len(contacts)).

        A 402 insufficient-credits response from any chunk short-circuits the
        batch: all valid contacts are returned as Not Found entries and the
        caller is expected to detect the failure via the cascade.
        (We do not raise.)
    """
    if not _is_enabled():
        logger.debug("GetLeads kill switch (ENABLE_GETLEADS=false) active")
        return []

    if not contacts:
        return []

    if not API_KEY:
        logger.warning("GetLeads API key not configured, skipping")
        return [
            _not_found_contact(
                first_name=(c.get("first_name") or c.get("firstName") or ""),
                last_name=(c.get("last_name") or c.get("lastName") or ""),
                domain=(
                    c.get("email_domain")
                    or c.get("company_domain")
                    or c.get("companyDomain")
                    or c.get("domain")
                    or ""
                ),
            )
            for c in contacts
            if isinstance(c, dict)
        ]

    # Normalize + validate. Track which inputs were valid so we can pad
    # invalid ones with Not Found entries in the original positions.
    normalized: list[Optional[dict[str, str]]] = []
    for raw in contacts:
        normalized.append(_normalize_contact(raw) if isinstance(raw, dict) else None)

    valid_payload = [c for c in normalized if c is not None]

    results_by_chunk: list[list[dict[str, Any]]] = []
    credit_error = False

    if valid_payload:
        for chunk in _chunk(valid_payload, _MAX_CONTACTS_PER_REQUEST):
            chunk_result = await _post_from_person(client, chunk)

            if pipeline._is_provider_error(chunk_result):
                # Insufficient credits — short-circuit. All valid contacts
                # become Not Found entries (mirrors smartprospect behavior).
                credit_error = True
                results_by_chunk = []
                break

            if chunk_result is None:
                # Network / server failure for this chunk — Not Found entries.
                results_by_chunk.append(
                    [
                        _not_found_contact(
                            first_name=c["first_name"],
                            last_name=c["last_name"],
                            domain=c["email_domain"],
                        )
                        for c in chunk
                    ]
                )
            else:
                # The API may return fewer items than we sent. Pad defensively.
                padded = list(chunk_result)
                while len(padded) < len(chunk):
                    src = chunk[len(padded)]
                    padded.append(
                        _not_found_contact(
                            first_name=src["first_name"],
                            last_name=src["last_name"],
                            domain=src["email_domain"],
                        )
                    )
                results_by_chunk.append(padded)

    # Flatten chunk results back into a single list aligned to valid_payload.
    flat_valid: list[dict[str, Any]] = []
    for chunk_result in results_by_chunk:
        flat_valid.extend(chunk_result)

    if credit_error:
        # All valid contacts become Not Found; we surface failure via cascade.
        flat_valid = [
            _not_found_contact(
                first_name=c["first_name"],
                last_name=c["last_name"],
                domain=c["email_domain"],
            )
            for c in valid_payload
        ]

    # Walk the original `normalized` list, pulling from flat_valid for valid
    # entries and inserting Not Found entries for invalid ones. This preserves
    # the original input length and order.
    output: list[dict[str, Any]] = []
    valid_idx = 0
    for entry in normalized:
        if entry is None:
            output.append(_not_found_contact())
            continue
        if valid_idx < len(flat_valid):
            output.append(flat_valid[valid_idx])
        else:
            output.append(
                _not_found_contact(
                    first_name=entry["first_name"],
                    last_name=entry["last_name"],
                    domain=entry["email_domain"],
                )
            )
        valid_idx += 1

    return output


async def get_credits_balance(
    client: httpx.AsyncClient,
) -> Optional[dict[str, Any]]:
    """
    Optional monitoring helper for the GetLeads credit balance.

    The GetLeads enrich endpoints surface ``creditsRemaining`` which is
    ``null`` on the unlimited plan (verified live — see
    /tmp/getleads_live_shapes.md). The only explicit meter is
    decision-makers' ``query_credits_used``. There is no clean standalone
    balance endpoint documented for this integration, so this helper
    intentionally returns None rather than guessing a URL. Wire this up
    later if/when a documented balance endpoint is available.
    """
    logger.debug(
        "GetLeads: no reliable credits balance endpoint — returning None"
    )
    return None


# ---------------------------------------------------------------------------
# Demo entry point (reads GETLEADS_API_KEY from env; never hardcodes the key)
# ---------------------------------------------------------------------------

if __name__ == "__main__":  # pragma: no cover
    import json as _json

    async def _demo() -> None:
        if not API_KEY:
            print("Set GETLEADS_API_KEY to run the demo.")
            return
        async with httpx.AsyncClient() as demo_client:
            single = await find_email(
                demo_client, "John", "Doe", "example.com"
            )
            print("find_email:", _json.dumps(single, indent=2))
            batch = await find_emails_batch(
                demo_client,
                [
                    {
                        "first_name": "Jane",
                        "last_name": "Roe",
                        "email_domain": "acme.com",
                    }
                ],
            )
            print("find_emails_batch:", _json.dumps(batch, indent=2))

    asyncio.run(_demo())
