"""Blitz API client for Phone Number Enrichment."""

import asyncio
import logging
import os
import random
import time
from typing import Any, Optional

import httpx

logger = logging.getLogger(__name__)

BLITZ_BASE_URL = "https://api.blitz-api.ai"

# Retry config: up to 3 retries (4 attempts total)
_MAX_RETRIES = 3
_BASE_BACKOFF = 2.0
_MAX_BACKOFF = 60.0

# Rate limiting: Phone endpoint has 5 RPS limit (per Blitz API docs)
_RATE_LIMIT_RPS = 5
_MIN_REQUEST_INTERVAL = 1.0 / _RATE_LIMIT_RPS

# Rate limiter state
_rate_limiter_lock = asyncio.Lock()
_last_request_time: float = 0.0


async def _acquire_rate_limit() -> None:
    """Ensure we don't exceed 5 RPS for phone endpoint."""
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


def _backoff_delay(attempt: int, retry_after: Optional[float] = None) -> float:
    """Return seconds to wait before the next attempt."""
    if retry_after is not None:
        return retry_after
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
    raise last_exc


async def find_phone(
    client: httpx.AsyncClient,
    person_linkedin_url: str,
) -> dict[str, Any]:
    """
    Find mobile/direct phone number from a LinkedIn profile URL.

    POST /v2/enrichment/phone

    Args:
        client: Async HTTP client
        person_linkedin_url: Person's LinkedIn profile URL

    Returns:
        {
            "found": true,
            "phone": "+1234567890"
        }
        OR
        {
            "found": false,
            "phone": null
        }

    Note: US contacts only - no international phone coverage.
    """
    payload = {
        "person_linkedin_url": person_linkedin_url
    }

    return await _post_with_retry(
        client,
        f"{BLITZ_BASE_URL}/v2/enrichment/phone",
        payload,
        timeout=30.0,
    )
