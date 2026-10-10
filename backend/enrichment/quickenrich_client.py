"""
QuickEnrich (app.quickenrich.io) client for the enrichment cascade.

Docs snapshot: docs/quickenrich-sdk/ (2026-10-10). Two capabilities are wired
into the waterfall; the Finder/lookup endpoints are intentionally unused.

- ``find_person_email``: GET /api/employees/search — person email by
  ``linkedin_url`` OR (``company_url`` + ``first_name`` + ``last_name``).
  Returns the full EmployeeRecord on match (email, employee_linkedin, title,
  email_verification_date, firmographics).
- ``domain_search``: GET /api/employees/dataset-search — domain -> decision
  makers by ``company_url`` (+ comma-joined titles, ``has_email=true``).
  20 contacts/page, page-capped.

Billing truths (documented):
  - employee/phone/reverse-email search: credit deducted only when a match is
    returned; clean miss (``data: []``) is free.
  - dataset-search WITH a title filter: 1 credit per returned employee that
    has an email or phone; empty page = free. WITHOUT a title filter: flat
    1 credit per results page. We always send titles.
  - Errors never bill.

"Found" semantics: HTTP 200 with ``data: []`` / ``data: null`` is a CLEAN
MISS, not an error. 400 = invalid identity combination (do not retry); 401 =
bad key; 422 = invalid validated filter value; 429/5xx = retryable with
backoff; 402/403 = credit/auth death -> ``_ProviderError`` (falsy, cascade
continues).

Env vars:
    QUICKENRICH_API_KEY                 API key (required; empty = disabled)
    QUICKENRICH_BASE_URL                default https://app.quickenrich.io
    ENABLE_QUICKENRICH                  kill switch ("false" = disabled)
    QUICKENRICH_RATE_LIMIT_RPM          employee-search lane, SHARED
                                        cross-process token bucket (default
                                        900; Standard plan cap 1000/min,
                                        GTM Unlimited 6000/min)
    QUICKENRICH_RATE_LIMIT_BURST        optional burst tokens (default ~1s)
    QUICKENRICH_DATASET_RATE_LIMIT_RPM  dataset-search lane (default 280;
                                        Standard plan cap 300/min)
    QUICKENRICH_DATASET_RATE_LIMIT_BURST optional burst tokens
    QUICKENRICH_DATASET_MAX_PAGES       page cap per domain (default 2 —
                                        bounds credits: 20 contacts/page)

Usage:
    result = await quickenrich_client.find_person_email(
        client, linkedin_url="https://www.linkedin.com/in/johndoe",
    )
    # -> {"found": True, "email": "...", ...} or {"found": False, "email": ""}

    page = await quickenrich_client.domain_search(
        client, company_url="example.com", titles=["CEO", "Founder"],
    )
    # -> {"employees": [...], "credits_used": N, "remaining_credits": M|None}
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
from typing import Any, Optional

import httpx

from . import pipeline  # For _ProviderError (insufficient-credits signaling)
from shared import rate_limiter
from shared.circuit_breaker import get_circuit_breaker

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_URL = os.getenv("QUICKENRICH_BASE_URL", "https://app.quickenrich.io")
API_KEY = os.getenv("QUICKENRICH_API_KEY", "")

# Two documented rate families on the Standard ($99/mo) plan: employee search
# 1,000 req/min, domain search 300 req/min. Defaults leave headroom under
# both. GTM Unlimited (6,000 req/min across endpoints) = raise both envs.
# Buckets are SHARED across gunicorn workers via shared/rate_limiter.py
# (SQLite-backed), so 4 workers draw from ONE bucket per lane.
_RATE_LIMIT_RPM = float(os.getenv("QUICKENRICH_RATE_LIMIT_RPM", "900"))
_REFILL_PER_SEC = _RATE_LIMIT_RPM / 60.0
_CAPACITY = float(os.getenv("QUICKENRICH_RATE_LIMIT_BURST", str(_REFILL_PER_SEC)))

_DATASET_RATE_LIMIT_RPM = float(os.getenv("QUICKENRICH_DATASET_RATE_LIMIT_RPM", "280"))
_DATASET_REFILL_PER_SEC = _DATASET_RATE_LIMIT_RPM / 60.0
_DATASET_CAPACITY = float(
    os.getenv("QUICKENRICH_DATASET_RATE_LIMIT_BURST", str(_DATASET_REFILL_PER_SEC))
)

# Page cap for dataset-search. 20 contacts/page; each page with results under
# a title filter bills per returned employee-with-email, so the cap bounds
# worst-case credits per domain. Default 2 pages = up to 40 candidates.
_DATASET_MAX_PAGES = int(os.getenv("QUICKENRICH_DATASET_MAX_PAGES", "2"))

_EMPLOYEE_SEARCH_PATH = "/api/employees/search"
_DATASET_SEARCH_PATH = "/api/employees/dataset-search"

_MAX_RETRIES = 3
_BASE_BACKOFF = 2.0
_MAX_BACKOFF = 30.0
_REQUEST_TIMEOUT = 30.0

# Last vendor-reported credit telemetry (module-global; informational only —
# NOT an account ledger). None = QuickEnrich has not reported one yet.
_LAST_REPORTED_REMAINING_CREDITS: Optional[int] = None

# ---------------------------------------------------------------------------
# Circuit breaker — fail fast when upstream is consistently erroring.
# ---------------------------------------------------------------------------

_quickenrich_circuit = get_circuit_breaker(
    "quickenrich_api",
    failure_threshold=10,
    recovery_timeout=60.0,
    half_open_max_calls=3,
)


# ---------------------------------------------------------------------------
# Rate limiters (shared cross-process token buckets, one per lane)
# ---------------------------------------------------------------------------


async def _acquire_rate_limit(bucket: str, refill_per_sec: float, capacity: float) -> None:
    """Block until a token is GRANTED on the shared bucket.

    Loops (does not sleep-and-proceed): concurrent denials compute similar
    waits and would all proceed together, admitting N calls per ~1 refilled
    token and bursting past the plan cap — the same 429-storm failure mode
    fixed for GetLeads on 2026-08-14. Only a grant consumes a token.
    """
    while True:
        wait = await asyncio.to_thread(
            rate_limiter.acquire_token, bucket, refill_per_sec, capacity
        )
        if wait <= 0:
            return
        await asyncio.sleep(wait)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def is_configured() -> bool:
    """True when an API key is present (module usable)."""
    return bool(API_KEY)


def _is_enabled() -> bool:
    """Return True unless the kill switch is active."""
    return os.environ.get("ENABLE_QUICKENRICH", "true").lower() == "true"


def _should_retry(status_code: int) -> bool:
    """Retry 429 + 5xx only. 400/401/402/403/404/422 are terminal."""
    return status_code == 429 or (500 <= status_code < 600)


def _backoff_delay(attempt: int, retry_after: Optional[float] = None) -> float:
    """Full-jitter exponential backoff, capped; honors Retry-After."""
    if retry_after is not None:
        return min(retry_after, _MAX_BACKOFF)
    cap = min(_MAX_BACKOFF, _BASE_BACKOFF * (2 ** attempt))
    return random.uniform(0, cap)


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {API_KEY}",
        "Accept": "application/json",
    }


def _clean(value: Any) -> str:
    """Coerce a QuickEnrich field to a stripped string; literal "N/A" -> "".

    Domain Search examples use "N/A" for missing phone/type values
    (docs/quickenrich-sdk/docs/SPEC_NOTES.md §7).
    """
    if value is None:
        return ""
    if isinstance(value, str):
        stripped = value.strip()
        return "" if stripped.lower() == "n/a" else stripped
    return str(value).strip()


def _bare_domain(company_url: str) -> str:
    """Defensively reduce any URL-ish input to a bare domain string."""
    cleaned = (company_url or "").strip()
    for prefix in ("https://", "http://", "www."):
        if cleaned.lower().startswith(prefix):
            cleaned = cleaned[len(prefix):]
    return cleaned.split("/")[0].split("?")[0].strip().lower()


def _record_credits(meta: Any, context: str) -> None:
    """Log + stash vendor-reported credit telemetry when present.

    Missing metadata means UNKNOWN, never zero (vendor semantics).
    """
    global _LAST_REPORTED_REMAINING_CREDITS
    if not isinstance(meta, dict):
        return
    used = meta.get("credits_used")
    remaining = meta.get("remaining_credits")
    if remaining is not None:
        try:
            _LAST_REPORTED_REMAINING_CREDITS = int(remaining)
        except (TypeError, ValueError):
            pass
    if used is not None or remaining is not None:
        logger.info(
            "QuickEnrich credits (%s): used=%s remaining=%s",
            context, used, remaining,
        )


def _parse_employee_record(record: dict[str, Any]) -> dict[str, Any]:
    """Map a QuickEnrich EmployeeRecord to the canonical person dict.

    Immutable: builds a new dict, never touches ``record``.
    """
    first = _clean(record.get("first_name"))
    last = _clean(record.get("last_name"))
    return {
        "found": True,
        "email": _clean(record.get("email")),
        "first_name": first,
        "last_name": last,
        "full_name": " ".join(p for p in (first, last) if p),
        "title": _clean(record.get("title")),
        "linkedin_url": _clean(record.get("employee_linkedin")),
        "phone": _clean(record.get("employee_phone")),
        "phone_type": _clean(record.get("employee_phone_type")),
        "email_verification_date": _clean(record.get("email_verification_date")),
        "company_name": _clean(record.get("company_name")),
        "company_url": _clean(record.get("company_url")),
        "email_domain": _clean(record.get("email_domain")),
        "company_phone": _clean(record.get("company_phone")),
        "industry": _clean(record.get("industry")),
        "employee_count": _clean(record.get("employee_count")),
        "revenue": _clean(record.get("revenue")),
        "city": _clean(record.get("city")),
        "country_code": _clean(record.get("country_code")),
    }


def _as_record_list(data: Any) -> list[dict[str, Any]]:
    """Employee Search may return a dict (single record) or list; normalize."""
    if isinstance(data, dict):
        return [data]
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    return []


def _terminal_error(response: httpx.Response, method: str) -> Any:
    """Classify a terminal (non-retried) HTTP error.

    402/403/401 -> ``_ProviderError`` (falsy; pipeline surfaces it via
    ``pipeline._is_provider_error``). Other 4xx (400/404/422) are logged and
    return None — a bad request shape is a skip, not a cascade event.
    """
    # Status-only messages: raw vendor bodies must NOT flow into
    # _ProviderError.message — it lands in user-visible provider_errors CSV
    # cells / API responses (adversarial-review security finding; sibling
    # clients embed no response bodies).
    if response.status_code in (401, 402, 403):
        error_type = {
            401: "auth_error",
            402: "insufficient_credits",
            403: "auth_error",
        }[response.status_code]
        return pipeline._ProviderError(
            provider="quickenrich",
            method=method,
            error_type=error_type,
            message=f"QuickEnrich {response.status_code} ({error_type})",
        )
    logger.warning("QuickEnrich %s terminal %d", method, response.status_code)
    return None


# ---------------------------------------------------------------------------
# Core request helper (getleads_client pattern: manual breaker + explicit
# retry loop; 429 never trips the breaker, 5xx does)
# ---------------------------------------------------------------------------


async def _request_with_retries(
    client: httpx.AsyncClient,
    method: str,
    path: str,
    *,
    params: Optional[dict[str, Any]] = None,
    bucket: str = "quickenrich",
    refill_per_sec: float = _REFILL_PER_SEC,
    capacity: float = _CAPACITY,
) -> Optional[httpx.Response]:
    """Rate limit -> circuit breaker -> retry/backoff.

    Returns the Response for 2xx AND terminal 4xx (the caller classifies),
    or None when unrecoverable (breaker open, network error / retryable
    status after exhausting retries).
    """
    if not await _quickenrich_circuit.can_proceed():
        logger.warning("QuickEnrich circuit breaker OPEN, failing fast")
        return None

    url = f"{BASE_URL}{path}"

    for attempt in range(_MAX_RETRIES + 1):
        await _acquire_rate_limit(bucket, refill_per_sec, capacity)
        try:
            resp = await client.request(
                method,
                url,
                params=params,
                headers=_headers(),
                timeout=_REQUEST_TIMEOUT,
            )

            if _should_retry(resp.status_code):
                # 429 = rate-limit, NOT a failure — don't trip the breaker
                # (mirrors blitz/getleads). Retry/backoff self-limits.
                if resp.status_code != 429:
                    await _quickenrich_circuit.record_failure()
                retry_after_raw = resp.headers.get("Retry-After")
                retry_after = float(retry_after_raw) if retry_after_raw else None
                delay = _backoff_delay(attempt, retry_after)
                if attempt < _MAX_RETRIES:
                    logger.warning(
                        "QuickEnrich %s returned %d (attempt %d/%d), retrying in %.1fs",
                        path, resp.status_code, attempt + 1, _MAX_RETRIES + 1, delay,
                    )
                    await asyncio.sleep(delay)
                    continue
                logger.error(
                    "QuickEnrich %s returned %d, exhausted retries",
                    path, resp.status_code,
                )
                await _quickenrich_circuit.record_failure()
                return None

            if resp.status_code >= 400:
                await _quickenrich_circuit.record_failure()
                return resp

            await _quickenrich_circuit.record_success()
            return resp

        except httpx.HTTPError as exc:
            if attempt < _MAX_RETRIES:
                delay = _backoff_delay(attempt)
                logger.warning(
                    "QuickEnrich %s network error (%s), retry %d/%d in %.1fs",
                    path, exc, attempt + 1, _MAX_RETRIES + 1, delay,
                )
                await asyncio.sleep(delay)
                continue
            await _quickenrich_circuit.record_failure()
            logger.error("QuickEnrich %s network error, exhausted retries: %s", path, exc)
            return None

    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def find_person_email(
    client: httpx.AsyncClient,
    linkedin_url: Optional[str] = None,
    first_name: Optional[str] = None,
    last_name: Optional[str] = None,
    company_url: Optional[str] = None,
) -> Optional[dict[str, Any]]:
    """Find one person's email via GET /api/employees/search.

    Identity patterns (API contract): ``linkedin_url`` alone, OR
    ``company_url`` + ``first_name`` + ``last_name`` (all four = LinkedIn
    first, name+company fallback server-side).

    Returns:
        ``{"found": True, "email": ..., ...person fields}`` on match,
        ``{"found": False, "email": ""}`` on clean miss,
        ``None`` when disabled/unconfigured/unrecoverable or when the input
        satisfies neither documented identity pattern,
        ``_ProviderError`` (falsy) on terminal auth/credit HTTP errors.
    """
    if not _is_enabled() or not is_configured():
        return None

    params: dict[str, Any] = {}
    if linkedin_url:
        # G7 parity: employee-search by LinkedIn expects a PERSON profile URL
        # (linkedin.com host + /in/ path). Company/garbage URLs would 400 or
        # match wrong entities — stop them at the client boundary.
        from urllib.parse import urlparse as _urlparse

        _li = str(linkedin_url).strip()
        _parsed = _urlparse(_li if "://" in _li else f"https://{_li}")
        _host = (_parsed.netloc or "").lower()
        if (
            (_host == "linkedin.com" or _host.endswith(".linkedin.com"))
            and "/in/" in (_parsed.path or "")
        ):
            params["linkedin_url"] = _li
    if company_url:
        params["company_url"] = _bare_domain(company_url)
    if first_name:
        params["first_name"] = first_name
    if last_name:
        params["last_name"] = last_name

    has_linkedin = bool(params.get("linkedin_url"))
    has_name_company = all(
        params.get(k) for k in ("company_url", "first_name", "last_name")
    )
    if not has_linkedin and not has_name_company:
        # Capability gate (never fight the provider): the API 400s without
        # one of the two documented identity patterns.
        logger.debug("QuickEnrich find_person_email: insufficient identity inputs, skipping")
        return None

    response = await _request_with_retries(
        client, "GET", _EMPLOYEE_SEARCH_PATH, params=params
    )
    if response is None:
        return None

    if response.status_code == 200:
        try:
            payload = response.json()
        except ValueError:
            logger.warning("QuickEnrich employee search: non-JSON 200 body")
            return None
        _record_credits(payload.get("meta"), "employee_search")
        for record in _as_record_list(payload.get("data")):
            parsed = _parse_employee_record(record)
            if parsed["email"]:
                return parsed
        # 200 with no email-bearing record = clean miss (no match billed).
        return {"found": False, "email": ""}

    return _terminal_error(response, "find_person_email")


async def domain_search(
    client: httpx.AsyncClient,
    company_url: str,
    titles: Optional[list[str]] = None,
    max_pages: Optional[int] = None,
) -> Optional[dict[str, Any]]:
    """Domain -> decision-maker candidates via GET /api/employees/dataset-search.

    Always sends a comma-joined title filter (billing: per returned
    employee-with-email; empty page free) and ``has_email=true``. Page count
    is capped (``QUICKENRICH_DATASET_MAX_PAGES``) to bound credits.

    Returns:
        ``{"employees": [person dicts], "credits_used": int,
           "remaining_credits": int|None}`` — empty ``employees`` = clean miss,
        ``None`` when disabled/unconfigured/unrecoverable (or empty domain),
        ``_ProviderError`` (falsy) on terminal auth/credit HTTP errors.
        Mid-pagination terminal errors keep already-fetched pages.
    """
    if not _is_enabled() or not is_configured():
        return None

    domain = _bare_domain(company_url or "")
    if not domain:
        return None

    page_cap = max(1, max_pages if max_pages is not None else _DATASET_MAX_PAGES)
    params: dict[str, Any] = {
        "company_url": domain,
        # Boolean as 1/0, NOT "true"/"false" strings — the API's validator
        # 422s on the string form ("must be true or false"), verified live
        # 2026-10-10 (has_email=1 → 200, has_email=true → 422).
        "has_email": "1",
    }
    if titles:
        joined = ",".join(t for t in titles if t and str(t).strip())
        if joined:
            params["title"] = joined

    employees: list[dict[str, Any]] = []
    credits_used = 0
    remaining: Optional[int] = None

    for page in range(1, page_cap + 1):
        response = await _request_with_retries(
            client,
            "GET",
            _DATASET_SEARCH_PATH,
            params={**params, "page": page},
            bucket="quickenrich_dataset",
            refill_per_sec=_DATASET_REFILL_PER_SEC,
            capacity=_DATASET_CAPACITY,
        )
        if response is None:
            if employees:
                break  # keep partial results already paid for
            return None

        if response.status_code != 200:
            error = _terminal_error(response, "domain_search")
            if employees and error is not None:
                logger.warning(
                    "QuickEnrich dataset search errored mid-pagination for %s — keeping %d fetched",
                    domain, len(employees),
                )
                break
            return error

        try:
            payload = response.json()
        except ValueError:
            logger.warning("QuickEnrich dataset search: non-JSON 200 body for %s", domain)
            break

        meta = payload.get("meta") or {}
        _record_credits(meta, f"dataset_search:{domain}")
        try:
            credits_used += int(meta.get("credits_used") or 0)
        except (TypeError, ValueError):
            pass
        if meta.get("remaining_credits") is not None:
            try:
                remaining = int(meta.get("remaining_credits"))
            except (TypeError, ValueError):
                pass

        page_records = _as_record_list(payload.get("data"))
        if not page_records:
            break
        employees.extend(_parse_employee_record(r) for r in page_records)

        try:
            last_page = meta.get("last_page")
            if last_page is not None and page >= int(last_page):
                break
        except (TypeError, ValueError):
            break

    return {
        "employees": employees,
        "credits_used": credits_used,
        "remaining_credits": remaining,
    }
