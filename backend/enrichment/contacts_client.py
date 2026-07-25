"""Contacts DB API client for person lookups used as email enrichment fallback."""

from __future__ import annotations

import asyncio
import logging
import os
import random
import time
from typing import Any, Optional

import httpx

from shared.circuit_breaker import get_circuit_breaker, CircuitBreakerError

logger = logging.getLogger(__name__)

# Circuit breaker: open after 5 consecutive 500s, recover after 60s
_contacts_db_breaker = get_circuit_breaker(
    name="contacts_db",
    failure_threshold=5,
    recovery_timeout=60.0,
    half_open_max_calls=3,
)

# Retry config: up to 3 retries (4 attempts total), exponential backoff with jitter.
_MAX_RETRIES = 3
_BASE_BACKOFF = 2.0
_MAX_BACKOFF = 30.0

# Rate limiting: Internal Contacts DB allows up to 75 RPS
_CONTACTS_DB_RATE_LIMIT_RPS = int(os.getenv("CONTACTS_DB_RATE_LIMIT_RPS", "75"))
_CONTACTS_DB_MIN_REQUEST_INTERVAL = 1.0 / _CONTACTS_DB_RATE_LIMIT_RPS

# Rate limiter state (thread-safe for async via asyncio lock)
_contacts_db_rate_limiter_lock = asyncio.Lock()
_contacts_db_last_request_time: float = 0.0


async def _acquire_contacts_db_rate_limit() -> None:
    """Ensure we don't exceed the rate limit by sleeping if needed."""
    global _contacts_db_last_request_time

    async with _contacts_db_rate_limiter_lock:
        now = time.monotonic()
        time_since_last = now - _contacts_db_last_request_time

        if time_since_last < _CONTACTS_DB_MIN_REQUEST_INTERVAL:
            wait_time = _CONTACTS_DB_MIN_REQUEST_INTERVAL - time_since_last
            await asyncio.sleep(wait_time)

        _contacts_db_last_request_time = time.monotonic()


# Separate rate limiter for upsert/write operations (also 75 RPS to match reads)
_upsert_rate_limiter_lock = asyncio.Lock()
_upsert_last_request_time: float = 0.0
_UPSERT_MIN_INTERVAL = 1.0 / _CONTACTS_DB_RATE_LIMIT_RPS


async def _acquire_upsert_rate_limit() -> None:
    """Ensure upserts don't exceed the rate limit by sleeping if needed."""
    global _upsert_last_request_time

    async with _upsert_rate_limiter_lock:
        now = time.monotonic()
        time_since_last = now - _upsert_last_request_time

        if time_since_last < _UPSERT_MIN_INTERVAL:
            wait_time = _UPSERT_MIN_INTERVAL - time_since_last
            await asyncio.sleep(wait_time)

        _upsert_last_request_time = time.monotonic()


def _base_url() -> str:
    import os
    return os.getenv("CONTACTS_API_BASE_URL", "https://leadsdatabase.cc").rstrip("/")


def _headers() -> dict[str, str]:
    import os
    token = os.getenv("CONTACTS_API_TOKEN", "")
    if not token:
        raise RuntimeError("CONTACTS_API_TOKEN environment variable is not set")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def _extract_email(data: Any) -> Optional[str]:
    """Try to pull an email string out of various response shapes."""
    if not data:
        return None
    if isinstance(data, str):
        return data if "@" in data else None
    if isinstance(data, dict):
        for key in ("email", "work_email", "primary_email"):
            val = data.get(key)
            if val and isinstance(val, str) and "@" in val:
                return val
        emails = data.get("emails")
        if isinstance(emails, list) and emails:
            first = emails[0]
            if isinstance(first, str):
                return first
            if isinstance(first, dict):
                return first.get("email") or first.get("address")
    return None


def _should_retry(status_code: int) -> bool:
    """
    Determine if a request should be retried based on status code.

    Retry logic:
    - 429 (Too Many Requests): Retry with backoff
    - 500+ (Server errors): Retry with backoff
    - 422 (Validation error): DO NOT retry - bad data, won't succeed
    - 404 (Not found): DO NOT retry - resource doesn't exist
    - 4xx (Client errors): DO NOT retry - bad request
    """
    return status_code == 429 or (status_code >= 500 and status_code < 600)


def _backoff_delay(attempt: int, retry_after: Optional[float] = None) -> float:
    """Return seconds to wait before the next attempt."""
    if retry_after is not None:
        return min(retry_after, _MAX_BACKOFF)
    # Exponential backoff with full jitter
    cap = min(_MAX_BACKOFF, _BASE_BACKOFF * (2 ** attempt))
    return random.uniform(0, cap)


async def _get_with_retry(
    client: httpx.AsyncClient,
    url: str,
    params: dict[str, Any],
    timeout: float = 20.0,
) -> Optional[dict[str, Any]]:
    """Execute GET request with retry logic for transient errors."""
    last_exc: Optional[Exception] = None

    for attempt in range(_MAX_RETRIES + 1):
        # Circuit breaker: fail fast if Contacts DB is unhealthy
        if not await _contacts_db_breaker.can_proceed():
            logger.warning("Contacts DB circuit breaker OPEN, skipping request: %s", url)
            return None

        # Acquire rate limit before each attempt
        await _acquire_contacts_db_rate_limit()

        try:
            resp = await client.get(url, headers=_headers(), params=params, timeout=timeout)

            # 404 means "not found" - don't retry, return None immediately
            if resp.status_code == 404:
                return None

            # 422 (Validation error) - bad data, don't retry
            if resp.status_code == 422:
                logger.debug("Contacts DB validation error (422) - bad data format, skipping: %s", url)
                return None

            # Check if we should retry this error
            if _should_retry(resp.status_code):
                # 429 = rate-limit, NOT a service failure — don't trip the
                # breaker (tripping degrades the cascade onto slower paid
                # providers). Retry/backoff self-limits. Mirrors blitz fix.
                if resp.status_code != 429:
                    await _contacts_db_breaker.record_failure()

                retry_after_raw = resp.headers.get("Retry-After")
                retry_after = float(retry_after_raw) if retry_after_raw else None
                delay = _backoff_delay(attempt, retry_after)

                if attempt < _MAX_RETRIES:
                    logger.warning(
                        "Contacts DB %s returned %d (attempt %d/%d), retrying in %.1fs",
                        url, resp.status_code, attempt + 1, _MAX_RETRIES + 1, delay,
                    )
                    await asyncio.sleep(delay)
                    continue

                # Exhausted retries
                logger.error("Contacts DB %s returned %d, exhausted retries", url, resp.status_code)
                return None

            # Success
            resp.raise_for_status()
            data = resp.json()
            # Record circuit breaker success
            await _contacts_db_breaker.record_success()
            return data if data else None

        except httpx.HTTPStatusError as e:
            # 404 (Not found) - resource doesn't exist, don't retry
            if e.response.status_code == 404:
                return None
            # 422 (Validation error) - bad data format, don't retry
            if e.response.status_code == 422:
                logger.debug("Contacts DB validation error (422) - skipping: %s", e.request.url)
                return None
            # Record circuit breaker failure — but NOT for 429 (rate-limit).
            if e.response.status_code != 429:
                await _contacts_db_breaker.record_failure()

            # For other errors, retry if appropriate
            last_exc = e
            if attempt < _MAX_RETRIES and _should_retry(e.response.status_code):
                delay = _backoff_delay(attempt)
                logger.warning(
                    "Contacts DB HTTP error (attempt %d/%d): %s - retrying in %.1fs",
                    attempt + 1, _MAX_RETRIES + 1, e, delay,
                )
                await asyncio.sleep(delay)
            else:
                logger.error("Contacts DB HTTP error, exhausted retries: %s", e)
                return None

        except Exception as exc:
            # Record circuit breaker failure
            await _contacts_db_breaker.record_failure()

            last_exc = exc
            if attempt < _MAX_RETRIES:
                delay = _backoff_delay(attempt)
                logger.warning(
                    "Contacts DB error (attempt %d/%d): %s - retrying in %.1fs",
                    attempt + 1, _MAX_RETRIES + 1, exc, delay,
                )
                await asyncio.sleep(delay)
            else:
                logger.error("Contacts DB error, exhausted retries: %s", exc)
                return None

    return None


async def person_by_linkedin(
    client: httpx.AsyncClient, linkedin_url: str
) -> Optional[dict[str, Any]]:
    """
    GET /v1/person/by-linkedin?li=<url-or-username>
    Returns person dict with email if found, None if 404/not found.
    Includes retry logic for transient errors.
    """
    return await _get_with_retry(
        client,
        f"{_base_url()}/v1/person/by-linkedin",
        {"li": linkedin_url},
        timeout=20.0,
    )


async def person_by_name_and_domain(
    client: httpx.AsyncClient, full_name: str, domain: str
) -> Optional[dict[str, Any]]:
    """
    GET /v1/person/by-name-and-domain?name=<name>&domain=<domain>
    Returns person dict with email if found, None if 404/not found.
    Includes retry logic for transient errors.
    """
    return await _get_with_retry(
        client,
        f"{_base_url()}/v1/person/by-name-and-domain",
        {"name": full_name, "domain": domain},
        timeout=20.0,
    )


async def company_by_domain(
    client: httpx.AsyncClient, domain: str
) -> Optional[dict[str, Any]]:
    """
    GET /v1/company/by-domain?domain=<domain>
    Returns company dict with linkedin_url if found, None if 404/not found.
    Used as primary lookup for domain → company LinkedIn URL.
    Includes retry logic for transient errors.

    Response shape:
    {
        "company_id": "uuid",
        "name": "Company Name",
        "website": "https://example.com",
        "linkedin_url": "https://linkedin.com/company/example",
        "industry": "Technology",
        ...
    }
    """
    return await _get_with_retry(
        client,
        f"{_base_url()}/v1/company/by-domain",
        {"domain": domain},
        timeout=20.0,
    )


async def company_contacts_enriched(
    client: httpx.AsyncClient, domain: str, limit: int = 5
) -> Optional[list[dict[str, Any]]]:
    """
    GET /v1/company/contacts/enriched?domain=<domain>&limit=<limit>
    Returns list of contacts (decision makers) with emails for a company domain.
    Used as primary lookup for company → decision makers with emails.
    Returns None if 404/not found or empty list if no contacts.
    Includes retry logic for transient errors.

    Response shape: [
        {
            "person_id": "uuid",
            "full_name": "John Doe",
            "email": "john@example.com",
            "title": "Software Engineer",
            "linkedin_url": "...",
            ...
        },
        ...
    ]
    """
    result = await _get_with_retry(
        client,
        f"{_base_url()}/v1/company/contacts/enriched",
        {"domain": domain, "limit": limit},
        timeout=30.0,
    )

    # Return None if not found, otherwise return the list (even if empty)
    if result is None:
        return None

    # Ensure we return a list
    if isinstance(result, list):
        return result
    elif isinstance(result, dict) and "contacts" in result:
        # Contacts DB returns {domain, count, contacts: [...]}
        return result["contacts"]
    elif isinstance(result, dict) and "data" in result:
        return result["data"]
    else:
        logger.warning("Unexpected response format from company_contacts_enriched: %s", type(result))
        return []


async def company_persons_by_domain(
    client: httpx.AsyncClient, domain: str, limit: int = 100,
    source: Optional[str] = None,
    exclude_source: Optional[str] = None,
) -> Optional[list[dict[str, Any]]]:
    """
    GET /v1/company/persons/by-domain?domain=<domain>&limit=<limit>[&source=<source>]
    Returns ALL persons linked to the company whose website = domain
    (company -> person_company_link -> person -> email). Emails are returned
    AS-STORED (no verification) — used for the by-company lookup path (Phase 1)
    so contacts loaded into the Contacts DB are retrievable by domain even when
    their emails are not on the lookup domain. Returns None on 404/not found,
    empty list if no persons. Includes retry/circuit-breaker via _get_with_retry.
    """
    params: dict[str, Any] = {"domain": domain, "limit": limit}
    if source:
        params["source"] = source
    if exclude_source:
        params["exclude_source"] = exclude_source
    result = await _get_with_retry(
        client,
        f"{_base_url()}/v1/company/persons/by-domain",
        params,
        timeout=30.0,
    )
    if result is None:
        return None
    if isinstance(result, list):
        return result
    elif isinstance(result, dict) and "contacts" in result:
        return result["contacts"]
    elif isinstance(result, dict) and "data" in result:
        return result["data"]
    else:
        logger.warning("Unexpected response format from company_persons_by_domain: %s", type(result))
        return []


def extract_email_from_contacts_response(data: Optional[dict[str, Any]]) -> Optional[str]:
    """Extract the best email from a Contacts DB person response."""
    return _extract_email(data)


# ---------------------------------------------------------------------------
# Business/Company upsert (for Google Maps → Contacts DB sync)
# ---------------------------------------------------------------------------

def _upsert_with_retry(
    client: httpx.Client,
    url: str,
    payload: dict[str, Any],
    max_retries: int = 3,
) -> Optional[dict[str, Any]]:
    """Sync POST with retry logic for upsert operations."""
    import time

    last_exc: Optional[Exception] = None

    for attempt in range(max_retries + 1):
        try:
            resp = client.post(
                url,
                headers=_headers(),
                json=payload,
                timeout=30.0,
            )

            # 404 for upsert is unusual, treat as failure
            if resp.status_code == 404:
                logger.warning("Contacts DB upsert returned 404")
                return None

            if _should_retry(resp.status_code):
                retry_after_raw = resp.headers.get("Retry-After")
                retry_after = float(retry_after_raw) if retry_after_raw else None
                delay = _backoff_delay(attempt, retry_after)

                if attempt < max_retries:
                    logger.warning(
                        "Contacts DB upsert returned %d (attempt %d/%d), retrying in %.1fs",
                        resp.status_code, attempt + 1, max_retries + 1, delay,
                    )
                    time.sleep(delay)
                    continue

                logger.error("Contacts DB upsert returned %d, exhausted retries", resp.status_code)
                return None

            resp.raise_for_status()
            return resp.json()

        except Exception as exc:
            last_exc = exc
            if attempt < max_retries:
                delay = _backoff_delay(attempt)
                logger.warning(
                    "Contacts DB upsert error (attempt %d/%d): %s - retrying in %.1fs",
                    attempt + 1, max_retries + 1, exc, delay,
                )
                time.sleep(delay)
            else:
                logger.error("Contacts DB upsert error, exhausted retries: %s", exc)
                return None

    return None


def upsert_business_record(
    client: httpx.Client,
    *,
    domain: str,
    company_name: str,
    company_website: str = "",
    full_address: str = "",
    phone: str = "",
    city: str = "",
    city_state: str = "",
) -> Optional[dict[str, Any]]:
    """
    Upsert a business/company record into the Contacts DB via POST /v1/persons/upsert.
    Uses a placeholder email (contact@{domain}) to create the company; the Contacts API
    auto-creates companies from domain and updates existing ones by domain.
    Includes retry logic for transient errors.
    Returns the API response on success, None on failure.
    """
    if not domain or not domain.strip():
        return None
    domain = domain.strip().lower()
    placeholder_email = f"contact@{domain}"
    payload = {
        "email": placeholder_email,
        "domain": domain,
        "full_name": company_name or "Contact",
        "company_name": company_name or "",
        "company_domain": domain,
        "company_website": company_website or f"https://{domain}",
    }
    return _upsert_with_retry(
        client,
        f"{_base_url()}/v1/persons/upsert",
        payload,
        max_retries=3,
    )


async def upsert_business_record_async(
    client: httpx.AsyncClient,
    *,
    domain: str,
    company_name: str,
    company_website: str = "",
    full_address: str = "",
    phone: str = "",
    city: str = "",
    city_state: str = "",
) -> Optional[dict[str, Any]]:
    """Async version of upsert_business_record with retry logic."""
    if not domain or not domain.strip():
        return None
    domain = domain.strip().lower()
    placeholder_email = f"contact@{domain}"
    payload = {
        "email": placeholder_email,
        "domain": domain,
        "full_name": company_name or "Contact",
        "company_name": company_name or "",
        "company_domain": domain,
        "company_website": company_website or f"https://{domain}",
    }

    last_exc: Optional[Exception] = None

    for attempt in range(_MAX_RETRIES + 1):
        # Acquire rate limit before each upsert attempt
        await _acquire_upsert_rate_limit()

        try:
            resp = await client.post(
                f"{_base_url()}/v1/persons/upsert",
                headers=_headers(),
                json=payload,
                timeout=30.0,
            )

            if resp.status_code == 404:
                return None

            if _should_retry(resp.status_code):
                retry_after_raw = resp.headers.get("Retry-After")
                retry_after = float(retry_after_raw) if retry_after_raw else None
                delay = _backoff_delay(attempt, retry_after)

                if attempt < _MAX_RETRIES:
                    logger.warning(
                        "Contacts DB upsert async returned %d (attempt %d/%d), retrying in %.1fs",
                        resp.status_code, attempt + 1, _MAX_RETRIES + 1, delay,
                    )
                    await asyncio.sleep(delay)
                    continue

                logger.error("Contacts DB upsert async returned %d, exhausted retries", resp.status_code)
                return None

            resp.raise_for_status()
            return resp.json()

        except Exception as exc:
            last_exc = exc
            if attempt < _MAX_RETRIES:
                delay = _backoff_delay(attempt)
                logger.warning(
                    "Contacts DB upsert async error (attempt %d/%d): %s - retrying in %.1fs",
                    attempt + 1, _MAX_RETRIES + 1, exc, delay,
                )
                await asyncio.sleep(delay)
            else:
                logger.error("Contacts DB upsert async error, exhausted retries: %s", exc)
                return None

    return None


async def mark_email_invalid(
    client: httpx.AsyncClient,
    *,
    email: str,
    person_id: Optional[str] = None,
    domain: Optional[str] = None,
) -> Optional[dict[str, Any]]:
    """
    Mark an email as invalid by upserting with empty email field.

    This effectively removes the invalid email from Contacts DB while
    preserving the person record for future updates.

    Args:
        client: Async HTTP client
        email: The invalid email to clear
        person_id: Optional person_id for targeted update
        domain: Optional domain for lookup-based update

    Returns:
        API response on success, None on failure
    """
    if not email:
        return None

    # Prepare payload with empty email to mark as invalid
    payload: dict[str, Any] = {"email": ""}

    # If we have person_id, use it for targeted update
    if person_id:
        payload["person_id"] = person_id

    # If we have domain, include it for lookup
    if domain:
        payload["domain"] = domain

    last_exc: Optional[Exception] = None

    for attempt in range(_MAX_RETRIES + 1):
        await _acquire_upsert_rate_limit()

        try:
            resp = await client.post(
                f"{_base_url()}/v1/persons/upsert",
                headers=_headers(),
                json=payload,
                timeout=30.0,
            )

            if resp.status_code == 404:
                logger.debug("Cannot mark email invalid - person not found: %s", email)
                return None

            if _should_retry(resp.status_code):
                delay = _backoff_delay(attempt, None)
                if attempt < _MAX_RETRIES:
                    logger.warning(
                        "Contacts DB upsert returned %d marking invalid (attempt %d/%d)",
                        resp.status_code, attempt + 1, _MAX_RETRIES + 1,
                    )
                    await asyncio.sleep(delay)
                    continue
                logger.error("Contacts DB upsert returned %d, exhausted retries", resp.status_code)
                return None

            resp.raise_for_status()
            logger.info("Marked email as invalid in Contacts DB: %s", email)
            return resp.json()

        except Exception as exc:
            last_exc = exc
            if attempt < _MAX_RETRIES:
                delay = _backoff_delay(attempt)
                logger.warning(
                    "Contacts DB upsert error marking invalid (attempt %d/%d): %s",
                    attempt + 1, _MAX_RETRIES + 1, exc,
                )
                await asyncio.sleep(delay)

    logger.error("Failed to mark email invalid after retries: %s", email)
    return None
