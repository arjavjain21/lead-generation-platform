"""
WizLeads API client for email enrichment.

WizLeads provides catch-all verified email enrichment with:
- 10 RPS rate limit
- 3 credits per email found
- All emails are catch-all verified (no additional verification needed)

Usage:
    result = await wizleads_client.find_email(
        client,
        first_name="John",
        last_name="Doe",  # Optional - can pass full name in first_name
        website="google.com"
    )
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
from typing import Any, Optional

import httpx

from . import pipeline  # Import for _ProviderError and _classify_http_error
from shared.circuit_breaker import get_circuit_breaker
from shared.env_utils import env_float

logger = logging.getLogger(__name__)

# Configuration
BASE_URL = os.getenv("WIZLEADS_BASE_URL", "https://api.wizleads.io")
API_KEY = os.getenv("WIZLEADS_API_KEY", "")
# Plan limit is 10 calls/second SHARED by every process (gunicorn workers +
# runner + list-builder fan-out). 2026-10-15 RCA: the old in-process limiter
# let each process offer 10 RPS alone (~60-80 RPS aggregate) -> 40% 429s.
RATE_LIMIT_RPS = env_float("WIZLEADS_RATE_LIMIT_RPS", "10")
_BURST_CAPACITY = env_float("WIZLEADS_RATE_LIMIT_BURST", str(RATE_LIMIT_RPS))

# Retry config: up to 3 retries (4 attempts total), exponential backoff with jitter.
# 2026-10-15: _MAX_BACKOFF 30 -> 8 — a terminal-tier lookup must never sit in
# backoff sleeps for up to ~90s inside a single interactive request.
_MAX_RETRIES = 3
_BASE_BACKOFF = 2.0
_MAX_BACKOFF = 8.0
_REQUEST_TIMEOUT = 20.0

# ---------------------------------------------------------------------------
# Circuit breaker — fail fast when the upstream is consistently erroring.
# ---------------------------------------------------------------------------

_wizleads_circuit = get_circuit_breaker(
    "wizleads_api",
    failure_threshold=10,
    recovery_timeout=60.0,
    half_open_max_calls=3,
)

# ---------------------------------------------------------------------------
# Rate limiter (shared cross-process token bucket — see shared/rate_limiter.py).
# ---------------------------------------------------------------------------

from shared import rate_limiter  # noqa: E402  (import kept local to the section)


async def _acquire_rate_limit() -> None:
    """Take a token from the SHARED cross-process bucket (10 RPS plan limit).

    Loops until a token is actually GRANTED (same over-admission guard as
    smartprospect_client): concurrent denials that merely sleep would all
    proceed together and burst past the account limit.
    """
    while True:
        wait = await asyncio.to_thread(
            rate_limiter.acquire_token, "wizleads", RATE_LIMIT_RPS, _BURST_CAPACITY
        )
        if wait <= 0:
            return
        await asyncio.sleep(wait)


def _headers() -> dict[str, str]:
    """Get headers for WizLeads API."""
    if not API_KEY:
        raise RuntimeError("WIZLEADS_API_KEY environment variable is not set")
    return {
        "x-api-key": API_KEY,
        "Content-Type": "application/json"
    }


def _should_retry(status_code: int) -> bool:
    """
    Determine if a request should be retried based on status code.

    Retry logic:
    - 500+ (Server errors): Retry with backoff
    - 429 (Too Many Requests): DO NOT retry — the shared cross-process
      limiter already paces us to the plan limit, so a 429 means
      account-level pressure that will not clear within one request.
      Sleeping up to 30s inside a user-facing cascade step was the #2
      latency source in the 2026-10-15 RCA. Skip the tier instead.
    - 402 (Insufficient Credits): DO NOT retry - no credits available
    - 422 (Validation Error): DO NOT retry - bad data
    - 404 (Not found): DO NOT retry - resource doesn't exist
    - 4xx (Client errors): DO NOT retry - bad request
    """
    return status_code >= 500 and status_code < 600


def _backoff_delay(attempt: int, retry_after: Optional[float] = None) -> float:
    """Return seconds to wait before the next attempt."""
    if retry_after is not None:
        return min(retry_after, _MAX_BACKOFF)
    # Exponential backoff with full jitter
    cap = min(_MAX_BACKOFF, _BASE_BACKOFF * (2 ** attempt))
    return random.uniform(0, cap)


def _normalize_website(website: str) -> str:
    """
    Normalize website/domain for WizLeads API.

    WizLeads accepts both formats:
    - example.com
    - https://example.com/

    We'll normalize to just the domain if it looks like a URL.
    """
    if not website:
        return ""

    website = website.strip()

    # If it starts with http:// or https://, return as-is
    if website.startswith(("http://", "https://")):
        return website

    # Otherwise, it's likely just a domain - return as-is
    # WizLeads handles both formats
    return website


async def find_email(
    client: httpx.AsyncClient,
    first_name: str,
    last_name: str,
    website: str,
) -> Optional[dict[str, Any]]:
    """
    Find work email using WizLeads API.

    GET /email/find-email

    Args:
        client: httpx AsyncClient
        first_name: First name or full name (WizLeads supports both)
        last_name: Last name (optional - can pass empty string if using full name)
        website: Company website or domain

    Returns:
        Dict with:
        {
            "email": "john@example.com",
            "catchall": "YES",  # YES/NO/UNKNOWN
            "provider": "Google",  # Email provider
            "normalized_fname": "John",
            "normalized_lname": "Doe"
        }
        Or None if not found / failed / not configured.

    Note: All emails returned by WizLeads are catch-all verified.
    """
    if not API_KEY:
        logger.warning("WizLeads API key not configured, skipping")
        return None

    if not first_name or not website:
        logger.debug("WizLeads requires first_name and website")
        return None

    # Circuit breaker — fail fast if upstream is broken.
    if not await _wizleads_circuit.can_proceed():
        logger.warning("WizLeads API circuit breaker OPEN, failing fast")
        return None

    # Normalize website
    normalized_website = _normalize_website(website)

    # Build query parameters
    params = {
        "first_name": first_name,
        "website": normalized_website,
    }
    if last_name:
        params["last_name"] = last_name

    last_exc: Optional[Exception] = None

    for attempt in range(_MAX_RETRIES + 1):
        await _acquire_rate_limit()
        try:
            resp = await client.get(
                f"{BASE_URL}/email/find-email",
                headers=_headers(),
                params=params,
                timeout=_REQUEST_TIMEOUT,
            )

            # Check for insufficient credits - don't retry
            if resp.status_code == 402:
                logger.warning("WizLeads: Insufficient credits - skipping")
                return pipeline._ProviderError(
                    provider="wizleads",
                    method="find_email",
                    error_type="insufficient_credits",
                    message="Wizleads: Insufficient credits. Please top up to continue.",
                )

            # Check for validation error - don't retry
            if resp.status_code == 422:
                logger.debug("WizLeads validation error (422) - bad data format: %s", resp.text[:200])
                return None

            # 429 — rate limit, NOT a provider failure. Do not trip the breaker
            # and do NOT sleep-retry inside this request (2026-10-15 RCA): skip
            # the tier; the shared limiter keeps these rare in the first place.
            if resp.status_code == 429:
                logger.info(
                    "WizLeads 429 for %s — skipping tier (shared limiter paces the account)",
                    normalized_website,
                )
                return None

            # Retryable status (5xx only).
            if _should_retry(resp.status_code):
                await _wizleads_circuit.record_failure()
                retry_after_raw = resp.headers.get("Retry-After")
                retry_after = float(retry_after_raw) if retry_after_raw else None
                delay = _backoff_delay(attempt, retry_after)

                if attempt < _MAX_RETRIES:
                    logger.warning(
                        "WizLeads %s returned %d (attempt %d/%d), retrying in %.1fs",
                        normalized_website, resp.status_code, attempt + 1, _MAX_RETRIES + 1, delay,
                    )
                    await asyncio.sleep(delay)
                    continue

                # Exhausted retries
                logger.error("WizLeads %s returned %d, exhausted retries", normalized_website, resp.status_code)
                return None

            # Success
            resp.raise_for_status()
            await _wizleads_circuit.record_success()
            data = resp.json()

            # Check if email was found
            email = data.get("email")
            if not email:
                logger.info("WizLeads: No email found for %s at %s", first_name, normalized_website)
                return None

            # Return formatted result
            result = {
                "email": email,
                "catchall": data.get("catchall", "UNKNOWN"),
                "provider": data.get("provider"),
            }
            logger.info("WizLeads found email for %s: %s (catchall: %s)", first_name, email, result.get("catchall"))
            return result

        except httpx.HTTPStatusError as e:
            status = e.response.status_code if e.response is not None else 0
            # Handle known status codes
            if status == 402:
                logger.warning("WizLeads: Insufficient credits")
                return pipeline._ProviderError(
                    provider="wizleads",
                    method="find_email",
                    error_type="insufficient_credits",
                    message="Wizleads: Insufficient credits. Please top up to continue.",
                )
            if status == 422:
                logger.debug("WizLeads validation error (422): %s", e.response.text[:200])
                return None
            if status == 429:
                # Rate limit — not a provider failure; skip the tier, no sleep.
                logger.info("WizLeads 429 (%s) — skipping tier", normalized_website)
                return None

            # For other errors, retry if appropriate
            last_exc = e
            await _wizleads_circuit.record_failure()
            if attempt < _MAX_RETRIES and _should_retry(status):
                delay = _backoff_delay(attempt)
                logger.warning(
                    "WizLeads HTTP error (attempt %d/%d): %r - retrying in %.1fs",
                    attempt + 1, _MAX_RETRIES, e, delay,
                )
                await asyncio.sleep(delay)
            else:
                logger.error("WizLeads HTTP error, exhausted retries: %r", e)
                return None

        except Exception as exc:
            last_exc = exc
            await _wizleads_circuit.record_failure()
            if attempt < _MAX_RETRIES:
                delay = _backoff_delay(attempt)
                logger.warning(
                    "WizLeads error (attempt %d/%d): %r - retrying in %.1fs",
                    attempt + 1, _MAX_RETRIES, exc, delay,
                )
                await asyncio.sleep(delay)
            else:
                # %r: httpx timeouts render with an EMPTY str() — the log used
                # to read "WizLeads error: - retrying" with no cause at all.
                logger.error("WizLeads error, exhausted retries: %r", exc)
                return None

    logger.error("WizLeads lookup failed for %s / %s after all retries", first_name, normalized_website)
    return None
