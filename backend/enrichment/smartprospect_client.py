"""
SmartProspect (SmartLead Find Emails API) client for email enrichment.

Wraps the SmartLead "Find Emails" endpoint. Despite the upstream URL
(prospect-api.smartlead.ai), every internal reference in this codebase uses
"smartprospect" — provider key, env vars, log prefix, file name. This matches
the project convention so the provider slots into the cascade uniformly.

SmartProspect provides:
- 2000 requests/minute account limit (~33 RPS). We self-limit to 30 RPS.
- Up to 10 contacts per request (we auto-chunk).
- Per-contact status ("Found" / "Not Found") and verification_status.

Env vars:
    SMARTPROSPECT_API_KEY   API key (required for calls; empty = disabled)
    SMARTPROSPECT_BASE_URL  Optional base URL override
    ENABLE_SMARTPROSPECT    Kill switch. "false" disables calls immediately.

Usage:
    result = await smartprospect_client.find_email(
        client,
        first_name="John",
        last_name="Doe",
        company_domain="example.com",
    )

    results = await smartprospect_client.find_emails_batch(
        client,
        contacts=[
            {"firstName": "John", "lastName": "Doe", "companyDomain": "example.com"},
            ...
        ],
    )
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import time
from typing import Any, Optional

import httpx

from . import pipeline  # For _ProviderError (insufficient_credits signaling)
from shared.circuit_breaker import get_circuit_breaker, CircuitBreakerError

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_URL = os.getenv(
    "SMARTPROSPECT_BASE_URL",
    "https://prospect-api.smartlead.ai",
)
API_KEY = os.getenv("SMARTPROSPECT_API_KEY", "")

# Account limit is 2000 req/min (~33 RPS). Use 30 RPS for headroom.
RATE_LIMIT_RPS = 30

# Endpoint path for the Find Emails API (key passed as query param, not here).
_FIND_EMAILS_PATH = "/api/v1/search-email-leads/search-contacts/find-emails"

# Max contacts per request as enforced by the upstream API.
_MAX_CONTACTS_PER_REQUEST = 10

# Retry config: up to 3 retries (4 attempts total), exponential backoff with jitter.
_MAX_RETRIES = 3
_BASE_BACKOFF = 2.0
_MAX_BACKOFF = 30.0
_REQUEST_TIMEOUT = 20.0

# ---------------------------------------------------------------------------
# Circuit breaker — fail fast when the upstream is consistently erroring.
# ---------------------------------------------------------------------------

_smartprospect_circuit = get_circuit_breaker(
    "smartprospect_api",
    failure_threshold=10,
    recovery_timeout=60.0,
    half_open_max_calls=3,
)

# ---------------------------------------------------------------------------
# Rate limiter (token-interval style — mirrors wizleads_client / blitz_client).
# ---------------------------------------------------------------------------

_rate_limiter_lock = asyncio.Lock()
_last_request_time: float = 0.0
_MIN_REQUEST_INTERVAL = 1.0 / RATE_LIMIT_RPS


async def _acquire_rate_limit() -> None:
    """Ensure we don't exceed the 30 RPS rate limit."""
    global _last_request_time
    async with _rate_limiter_lock:
        now = time.monotonic()
        elapsed = now - _last_request_time
        if elapsed < _MIN_REQUEST_INTERVAL:
            await asyncio.sleep(_MIN_REQUEST_INTERVAL - elapsed)
        _last_request_time = time.monotonic()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _is_enabled() -> bool:
    """Return True if the SmartProspect kill switch is not active."""
    return os.environ.get("ENABLE_SMARTPROSPECT", "true").lower() == "true"


def _should_retry(status_code: int) -> bool:
    """
    Decide whether a status code is worth retrying.

    Retry: 429 (rate limit), 5xx (server errors).
    Do NOT retry: 402 (no credits), 422 (bad data), 4xx (client errors).
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
    first_name = (raw.get("firstName") or raw.get("first_name") or "").strip()
    last_name = (raw.get("lastName") or raw.get("last_name") or "").strip()
    domain = (
        raw.get("companyDomain")
        or raw.get("company_domain")
        or raw.get("domain")
        or ""
    ).strip()

    if not first_name or not last_name or not domain:
        return None

    return {
        "firstName": first_name,
        "lastName": last_name,
        "companyDomain": domain,
    }


def _normalize_response_contact(item: dict[str, Any]) -> dict[str, Any]:
    """
    Convert one API response item into the normalized dict shape.

    Does not raise — missing keys become defaults. Email is "" when not found.

    Status semantics (observed in real API responses, not just docs):
      * "Found"     — usable email returned in email_id
      * "Not Found" — no email; email_id is empty
      * "Invalid"   — SmartLead found a candidate pattern but its verifier
                       flagged the address as bad. email_id may be populated
                       but we DISCARD it so the cascade falls through to the
                       next provider. The raw status is preserved on the
                       returned dict for audit/logging.
    """
    email = item.get("email_id") or ""
    status = item.get("status") or ("Found" if email else "Not Found")
    # Only "Found" indicates a usable email. "Invalid" and "Not Found" both
    # mean the cascade should fall through — clear the email but keep status
    # for downstream visibility.
    if status != "Found":
        email = ""
    return {
        "email": email,
        "status": status,
        "verification_status": item.get("verification_status"),
        "first_name": item.get("firstName") or "",
        "last_name": item.get("lastName") or "",
        "domain": item.get("companyDomain") or "",
    }


def _not_found_contact(
    first_name: str = "",
    last_name: str = "",
    domain: str = "",
) -> dict[str, Any]:
    """Build a normalized Not Found entry (used for padding / errors)."""
    return {
        "email": "",
        "status": "Not Found",
        "verification_status": None,
        "first_name": first_name,
        "last_name": last_name,
        "domain": domain,
    }


def _insufficient_credits_error(method: str) -> pipeline._ProviderError:
    """Build the standard insufficient-credits _ProviderError for SmartProspect."""
    return pipeline._ProviderError(
        provider="smartprospect",
        method=method,
        error_type="insufficient_credits",
        message="SmartProspect: Insufficient credits. Please top up to continue.",
    )


def _chunk(seq: list[Any], size: int) -> list[list[Any]]:
    """Split a list into chunks of `size`, preserving order (immutable)."""
    return [seq[i : i + size] for i in range(0, len(seq), size)]


# ---------------------------------------------------------------------------
# Core request helper
# ---------------------------------------------------------------------------


async def _post_find_emails(
    client: httpx.AsyncClient,
    payload_contacts: list[dict[str, str]],
) -> Optional[list[dict[str, Any]]]:
    """
    POST one chunk (<=10 contacts) to the Find Emails endpoint.

    Returns:
        - list of normalized response dicts on success
        - _ProviderError instance when 402 (insufficient credits) — caller checks
        - None on unrecoverable failure / circuit open / kill switch

    Note: a _ProviderError is falsy, so callers using `if not result:` will
    treat it as a skip; callers that need to distinguish should use
    `pipeline._is_provider_error(result)`.
    """
    if not _is_enabled():
        logger.debug("SmartProspect kill switch (ENABLE_SMARTPROSPECT=false) active")
        return None

    if not API_KEY:
        logger.warning("SmartProspect API key not configured, skipping")
        return None

    # Circuit breaker — fail fast if upstream is broken.
    if not await _smartprospect_circuit.can_proceed():
        logger.warning("SmartProspect API circuit breaker OPEN, failing fast")
        return None

    url = f"{BASE_URL}{_FIND_EMAILS_PATH}"

    for attempt in range(_MAX_RETRIES + 1):
        await _acquire_rate_limit()

        try:
            resp = await client.post(
                url,
                headers={"Content-Type": "application/json"},
                params={"api_key": API_KEY},
                json={"contacts": payload_contacts},
                timeout=_REQUEST_TIMEOUT,
            )

            # 402 — no credits. Do NOT retry. Surface as _ProviderError.
            if resp.status_code == 402:
                await _smartprospect_circuit.record_failure()
                logger.warning("SmartProspect: Insufficient credits (402)")
                return _insufficient_credits_error("find_emails_batch")

            # 422 — bad payload. Do NOT retry. Log and bail.
            if resp.status_code == 422:
                await _smartprospect_circuit.record_failure()
                logger.debug(
                    "SmartProspect validation error (422): %s",
                    resp.text[:200],
                )
                return None

            # Retryable status (429 / 5xx).
            if _should_retry(resp.status_code):
                # 429 = rate-limit, NOT a failure — don't trip the breaker
                # (mirrors blitz fix). Retry/backoff self-limits.
                if resp.status_code != 429:
                    await _smartprospect_circuit.record_failure()
                retry_after_raw = resp.headers.get("Retry-After")
                retry_after = float(retry_after_raw) if retry_after_raw else None
                delay = _backoff_delay(attempt, retry_after)
                if attempt < _MAX_RETRIES:
                    logger.warning(
                        "SmartProspect returned %d (attempt %d/%d), retrying in %.1fs",
                        resp.status_code,
                        attempt + 1,
                        _MAX_RETRIES + 1,
                        delay,
                    )
                    await asyncio.sleep(delay)
                    continue
                logger.error(
                    "SmartProspect returned %d, exhausted retries",
                    resp.status_code,
                )
                return None

            # Non-retryable, non-success status (4xx other than 402/422).
            if resp.status_code >= 400:
                await _smartprospect_circuit.record_failure()
                logger.error(
                    "SmartProspect non-retryable error %d: %s",
                    resp.status_code,
                    resp.text[:200],
                )
                return None

            # Success.
            resp.raise_for_status()
            await _smartprospect_circuit.record_success()

            data = resp.json()
            items = data.get("data") or []
            return [_normalize_response_contact(item) for item in items]

        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code if exc.response is not None else 0
            # 429 = rate-limit, NOT a failure — don't trip the breaker.
            if status != 429:
                await _smartprospect_circuit.record_failure()
            if status == 402:
                logger.warning("SmartProspect: Insufficient credits (402)")
                return _insufficient_credits_error("find_emails_batch")
            if attempt < _MAX_RETRIES and _should_retry(status):
                delay = _backoff_delay(attempt)
                logger.warning(
                    "SmartProspect HTTP error (attempt %d/%d): %s — retrying in %.1fs",
                    attempt + 1,
                    _MAX_RETRIES,
                    exc,
                    delay,
                )
                await asyncio.sleep(delay)
                continue
            logger.error("SmartProspect HTTP error, giving up: %s", exc)
            return None

        except (httpx.RequestError, asyncio.TimeoutError) as exc:
            await _smartprospect_circuit.record_failure()
            if attempt < _MAX_RETRIES:
                delay = _backoff_delay(attempt)
                logger.warning(
                    "SmartProspect network error (attempt %d/%d): %s — retrying in %.1fs",
                    attempt + 1,
                    _MAX_RETRIES,
                    exc,
                    delay,
                )
                await asyncio.sleep(delay)
                continue
            logger.error("SmartProspect network error, exhausted retries: %s", exc)
            return None

        except CircuitBreakerError as exc:
            logger.warning("SmartProspect circuit breaker tripped: %s", exc)
            return None

        except Exception as exc:
            await _smartprospect_circuit.record_failure()
            if attempt < _MAX_RETRIES:
                delay = _backoff_delay(attempt)
                logger.warning(
                    "SmartProspect unexpected error (attempt %d/%d): %s — retrying in %.1fs",
                    attempt + 1,
                    _MAX_RETRIES,
                    exc,
                    delay,
                )
                await asyncio.sleep(delay)
                continue
            logger.error("SmartProspect unexpected error, giving up: %s", exc)
            return None

    logger.error("SmartProspect: exhausted all retries")
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
    Look up a single contact's email via SmartProspect.

    Args:
        client: httpx.AsyncClient used for the HTTP call.
        first_name: Contact first name (non-empty).
        last_name: Contact last name (non-empty).
        company_domain: Contact's company domain (non-empty).

    Returns:
        Normalized dict on success:
            {
                "email": "john.doe@example.com",
                "status": "Found",
                "verification_status": "Valid",
                "first_name": "John",
                "last_name": "Doe",
                "domain": "example.com",
            }
        Not-found contacts still return a dict with email="" and
        status="Not Found" so callers can treat the result uniformly.

        Returns None when:
            - kill switch is off
            - API key is missing
            - input is invalid
            - the call fails after retries
        Returns a pipeline._ProviderError (falsy) when SmartProspect reports
            insufficient credits (402).
    """
    if not _is_enabled():
        logger.debug("SmartProspect kill switch (ENABLE_SMARTPROSPECT=false) active")
        return None

    if not API_KEY:
        logger.warning("SmartProspect API key not configured, skipping")
        return None

    if not first_name or not last_name or not company_domain:
        logger.debug(
            "SmartProspect requires first_name, last_name, and company_domain"
        )
        return None

    payload_contact = {
        "firstName": str(first_name).strip(),
        "lastName": str(last_name).strip(),
        "companyDomain": str(company_domain).strip(),
    }

    result = await _post_find_emails(client, [payload_contact])

    # Distinguish a _ProviderError from a real None.
    if pipeline._is_provider_error(result):
        # Re-wrap with the single-contact method name.
        return _insufficient_credits_error("find_email")

    if result is None:
        return None

    if not result:
        # API returned an empty data array — treat as Not Found.
        return _not_found_contact(
            first_name=payload_contact["firstName"],
            last_name=payload_contact["lastName"],
            domain=payload_contact["companyDomain"],
        )

    return result[0]


async def find_emails_batch(
    client: httpx.AsyncClient,
    contacts: list[dict[str, str]],
) -> list[dict[str, Any]]:
    """
    Batch email lookup. Auto-chunks to groups of 10 (the API max).

    Args:
        client: httpx.AsyncClient used for HTTP calls.
        contacts: List of contact dicts. Each dict may use either the API's
            camelCase keys (firstName, lastName, companyDomain) or the
            project's snake_case keys (first_name, last_name,
            company_domain / domain).

    Returns:
        A list with exactly one entry per input contact, preserving input
        order. Each entry is a normalized dict. Not-found contacts still
        appear with email="" and status="Not Found".

        If the kill switch is off, the API key is missing, or the input list
        is empty, returns [] (or a list of Not Found entries for non-empty
        input when the key is missing — to keep length == len(contacts)).

        A 402 insufficient-credits response from any chunk causes the whole
        batch to short-circuit: the remaining contacts are returned as Not
        Found entries and the caller is expected to detect the failure via
        the cascade. (We do not raise.)
    """
    if not _is_enabled():
        logger.debug("SmartProspect kill switch (ENABLE_SMARTPROSPECT=false) active")
        return []

    if not contacts:
        return []

    if not API_KEY:
        logger.warning("SmartProspect API key not configured, skipping")
        return [
            _not_found_contact(
                first_name=(c.get("firstName") or c.get("first_name") or ""),
                last_name=(c.get("lastName") or c.get("last_name") or ""),
                domain=(
                    c.get("companyDomain")
                    or c.get("company_domain")
                    or c.get("domain")
                    or ""
                ),
            )
            for c in contacts
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
            chunk_result = await _post_find_emails(client, chunk)

            if pipeline._is_provider_error(chunk_result):
                # Insufficient credits — short-circuit. Remaining valid contacts
                # (including the rest of this chunk) become Not Found entries.
                credit_error = True
                results_by_chunk.append(
                    [
                        _not_found_contact(
                            first_name=c["firstName"],
                            last_name=c["lastName"],
                            domain=c["companyDomain"],
                        )
                        for c in chunk
                    ]
                )
                # Pretend we've handled every remaining valid contact as Not Found.
                remaining = valid_payload[len(valid_payload) - len(chunk) :]
                # Wait: simpler to mark all valid as Not Found from here on.
                # We'll just rebuild the output list below from `normalized`.
                results_by_chunk = []
                break

            if chunk_result is None:
                # Network / server failure for this chunk — Not Found entries.
                results_by_chunk.append(
                    [
                        _not_found_contact(
                            first_name=c["firstName"],
                            last_name=c["lastName"],
                            domain=c["companyDomain"],
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
                            first_name=src["firstName"],
                            last_name=src["lastName"],
                            domain=src["companyDomain"],
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
                first_name=c["firstName"],
                last_name=c["lastName"],
                domain=c["companyDomain"],
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
                    first_name=entry["firstName"],
                    last_name=entry["lastName"],
                    domain=entry["companyDomain"],
                )
            )
        valid_idx += 1

    return output


async def get_credits_balance(
    client: httpx.AsyncClient,
) -> Optional[dict[str, Any]]:
    """
    Optional monitoring helper for the SmartProspect credit balance.

    The SmartLead Find Emails API surface documented for this integration does
    not expose a stable, clean balance endpoint, so this helper intentionally
    returns None rather than guessing a URL. Wire this up later if/when a
    documented balance endpoint is available.
    """
    logger.debug(
        "SmartProspect: no documented credits balance endpoint — returning None"
    )
    return None
