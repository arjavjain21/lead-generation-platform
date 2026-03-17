"""Contacts DB API client for person lookups used as email enrichment fallback."""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Any, Optional

import httpx

logger = logging.getLogger(__name__)

# Retry config: up to 3 retries (4 attempts total), exponential backoff with jitter.
_MAX_RETRIES = 3
_BASE_BACKOFF = 2.0
_MAX_BACKOFF = 30.0


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
    """Determine if a request should be retried based on status code."""
    return status_code == 429 or status_code >= 500


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
        try:
            resp = await client.get(url, headers=_headers(), params=params, timeout=timeout)

            # 404 means "not found" - don't retry, return None immediately
            if resp.status_code == 404:
                return None

            # Check if we should retry this error
            if _should_retry(resp.status_code):
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
            return data if data else None

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            last_exc = e
            if attempt < _MAX_RETRIES:
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
    GET /v1/person/by-linkedin?linkedin_url=<url>
    Returns person dict with email if found, None if 404/not found.
    Includes retry logic for transient errors.
    """
    return await _get_with_retry(
        client,
        f"{_base_url()}/v1/person/by-linkedin",
        {"linkedin_url": linkedin_url},
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
