"""Blitz API client wrapping the three endpoints used in the enrichment pipeline."""

import asyncio
import logging
import os
import random
import time
from typing import Any, Optional

import httpx

BLITZ_BASE_URL = "https://api.blitz-api.ai"

logger = logging.getLogger(__name__)

# Retry config: up to 3 retries (4 attempts total), exponential backoff with jitter.
_MAX_RETRIES = 3
_BASE_BACKOFF = 2.0   # seconds before first retry
_MAX_BACKOFF = 60.0   # cap so we never wait more than a minute per attempt

# Rate limiting: Blitz API has a 5 RPS limit, so we stay conservative with 4 RPS
_RATE_LIMIT_RPS = 4  # requests per second
_MIN_REQUEST_INTERVAL = 1.0 / _RATE_LIMIT_RPS  # seconds between requests

# Rate limiter state (thread-safe for async via asyncio lock)
_rate_limiter_lock = asyncio.Lock()
_last_request_time: float = 0.0


async def _acquire_rate_limit() -> None:
    """Ensure we don't exceed the rate limit by sleeping if needed."""
    global _last_request_time

    async with _rate_limiter_lock:
        now = time.monotonic()
        time_since_last = now - _last_request_time

        if time_since_last < _MIN_REQUEST_INTERVAL:
            wait_time = _MIN_REQUEST_INTERVAL - time_since_last
            await asyncio.sleep(wait_time)

        _last_request_time = time.monotonic()


def _get_api_key() -> str:
    key = os.getenv("BLITZ_API_KEY", "")
    if not key:
        raise RuntimeError("BLITZ_API_KEY environment variable is not set")
    return key


def _headers() -> dict[str, str]:
    return {
        "x-api-key": _get_api_key(),
        "Content-Type": "application/json",
    }


def _should_retry(status_code: int) -> bool:
    return status_code == 429 or status_code >= 500


def _backoff_delay(attempt: int, retry_after: Optional[float]) -> float:
    """Return seconds to wait before the next attempt."""
    if retry_after is not None:
        return retry_after
    # Exponential backoff with full jitter
    cap = min(_MAX_BACKOFF, _BASE_BACKOFF * (2 ** attempt))
    return random.uniform(0, cap)


async def _post_with_retry(
    client: httpx.AsyncClient,
    url: str,
    payload: dict[str, Any],
    timeout: float,
) -> dict[str, Any]:
    last_exc: Optional[Exception] = None
    for attempt in range(_MAX_RETRIES + 1):
        # Acquire rate limit before each attempt (including retries)
        await _acquire_rate_limit()

        try:
            resp = await client.post(url, headers=_headers(), json=payload, timeout=timeout)
            if _should_retry(resp.status_code):
                retry_after_raw = resp.headers.get("Retry-After")
                retry_after = float(retry_after_raw) if retry_after_raw else None
                delay = _backoff_delay(attempt, retry_after)
                if attempt < _MAX_RETRIES:
                    logger.warning(
                        "Blitz API %s returned %d (attempt %d/%d), retrying in %.1fs",
                        url, resp.status_code, attempt + 1, _MAX_RETRIES + 1, delay,
                    )
                    await asyncio.sleep(delay)
                    continue
                # Exhausted retries — raise so the caller can handle it
                resp.raise_for_status()
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError:
            raise
        except Exception as exc:
            last_exc = exc
            if attempt < _MAX_RETRIES:
                delay = _backoff_delay(attempt, None)
                logger.warning(
                    "Blitz API %s raised %s (attempt %d/%d), retrying in %.1fs",
                    url, exc, attempt + 1, _MAX_RETRIES + 1, delay,
                )
                await asyncio.sleep(delay)
            else:
                raise
    raise last_exc  # type: ignore[misc]


async def domain_to_linkedin(client: httpx.AsyncClient, domain: str) -> dict[str, Any]:
    """
    POST /v2/enrichment/domain-to-linkedin
    Returns: { found: bool, company_linkedin_url: str | None }
    Cost: 1 credit on success, 0 if not found.
    """
    return await _post_with_retry(
        client,
        f"{BLITZ_BASE_URL}/v2/enrichment/domain-to-linkedin",
        {"domain": domain},
        timeout=30.0,
    )


async def waterfall_icp_search(
    client: httpx.AsyncClient,
    company_linkedin_url: str,
    cascade: list[dict[str, Any]],
    max_results: int = 5,
) -> dict[str, Any]:
    """
    POST /v2/search/waterfall-icp-keyword
    Returns: { company_linkedin_url, max_results, results_length, results: [...] }
    Cost: 1 credit per result returned.
    Each result has: { icp, ranking, person: { first_name, last_name, full_name,
      headline, linkedin_url, location, experiences, ... } }
    """
    return await _post_with_retry(
        client,
        f"{BLITZ_BASE_URL}/v2/search/waterfall-icp-keyword",
        {
            "company_linkedin_url": company_linkedin_url,
            "cascade": cascade,
            "max_results": max_results,
        },
        timeout=60.0,
    )


async def find_work_email(
    client: httpx.AsyncClient, person_linkedin_url: str
) -> dict[str, Any]:
    """
    POST /v2/enrichment/email
    Returns: { found: bool, email: str | None, all_emails: [...] }
    Cost: 1 credit on success, 0 if not found.
    """
    return await _post_with_retry(
        client,
        f"{BLITZ_BASE_URL}/v2/enrichment/email",
        {"person_linkedin_url": person_linkedin_url},
        timeout=30.0,
    )


DEFAULT_CASCADE: list[dict[str, Any]] = [
    {
        "include_title": ["Owner", "CEO", "Founder", "Co-Founder", "President"],
        "exclude_title": ["assistant", "intern", "junior", "associate"],
        "location": ["WORLD"],
        "include_headline_search": False,
    },
    {
        "include_title": [
            "CMO",
            "VP Marketing",
            "VP Sales",
            "Chief Revenue Officer",
            "Chief Marketing Officer",
            "VP of Marketing",
            "VP of Sales",
        ],
        "exclude_title": ["assistant", "intern", "junior"],
        "location": ["WORLD"],
        "include_headline_search": False,
    },
    {
        "include_title": [
            "Director of Marketing",
            "Director of Sales",
            "Head of Marketing",
            "Head of Sales",
            "Head of Growth",
            "Marketing Director",
            "Sales Director",
        ],
        "exclude_title": ["assistant", "intern", "junior"],
        "location": ["WORLD"],
        "include_headline_search": False,
    },
]
