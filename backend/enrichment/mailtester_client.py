"""
Mailtester API client for email verification.

API: https://validation.hyperke.org/ninja?email={email}&key={key}
Response: {"code": "ok"|"mb"|"ko", "message": "...", "email": "..."}

Response codes (default policy — configurable via MAILTESTER_ACCEPT_CODES):
- "ok" → Accept email
- "mb" → Reject (policy-rejected; accepted under legacy 'ok,mb' policy)
- "ko" or anything else → Reject (hard invalid)
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import time
from typing import Any, Optional

import httpx

from shared.circuit_breaker import CircuitBreaker

logger = logging.getLogger(__name__)

MAILTESTER_API_URL = "https://validation.hyperke.org/ninja"
_MAX_RETRIES = int(os.getenv("MAILTESTER_MAX_RETRIES", "1"))  # was 3; cuts 4x -> 2x fan-out
_BASE_BACKOFF = 2.0
_MAX_BACKOFF = 30.0
_RATE_LIMIT_RPS = 10  # Conservative rate limit
_MIN_REQUEST_INTERVAL = 1.0 / _RATE_LIMIT_RPS

_rate_limiter_lock = asyncio.Lock()
_last_request_time: float = 0.0

# Circuit breaker around the mailtester/proxy call. Mailtester is the most
# expensive call on the cascade (up to 4 client x 10 proxy = 40 upstream calls
# per validation). When the proxy/vendor is degraded this fails FAST (FAIL
# OPEN) instead of burning the full retry budget on every one of up to 5
# validations per person. (2026-07-28 throughput fix.)
_mailtester_breaker = CircuitBreaker(
    "mailtester", failure_threshold=5, recovery_timeout=60.0
)

# Short-lived result memo keyed by email. Collapses redundant validations both
# WITHIN a request (the cascade can validate the same candidate several times)
# and ACROSS requests (Clay re-queries). Only definitive (HTTP 200) results are
# cached; failures are never cached so a transient error isn't sticky.
_MAILTESTER_CACHE_TTL = float(os.getenv("MAILTESTER_CACHE_TTL", "120"))
_MAILTESTER_CACHE_MAX = int(os.getenv("MAILTESTER_CACHE_MAX", "10000"))
_mailtester_cache: dict[str, tuple[float, dict[str, Any]]] = {}


def _cache_get(email: str) -> Optional[dict[str, Any]]:
    """Return a cached definitive result, or None if absent/expired."""
    entry = _mailtester_cache.get(email)
    if entry is None:
        return None
    expiry, result = entry
    if expiry > time.monotonic():
        return result
    _mailtester_cache.pop(email, None)
    return None


def _cache_set(email: str, result: dict[str, Any]) -> None:
    """Store a definitive result with a TTL; evict the soonest-expiring on full."""
    if len(_mailtester_cache) >= _MAILTESTER_CACHE_MAX:
        try:
            oldest = min(_mailtester_cache, key=lambda k: _mailtester_cache[k][0])
            _mailtester_cache.pop(oldest, None)
        except ValueError:
            pass
    _mailtester_cache[email] = (time.monotonic() + _MAILTESTER_CACHE_TTL, result)


async def _acquire_rate_limit() -> None:
    """Ensure we don't exceed rate limit."""
    global _last_request_time
    async with _rate_limiter_lock:
        now = time.monotonic()
        time_since_last = now - _last_request_time
        if time_since_last < _MIN_REQUEST_INTERVAL:
            await asyncio.sleep(_MIN_REQUEST_INTERVAL - time_since_last)
        _last_request_time = time.monotonic()


def _get_api_key() -> str:
    key = os.getenv("MAILTESTER_API_KEY", "") or "b0b4ddb5bd42ba18c09247611adfbe08374a7d06a128f404"
    if not key:
        raise RuntimeError("MAILTESTER_API_KEY environment variable is not set")
    return key


def _accepted_codes() -> tuple[str, ...]:
    """Codes treated as valid, from MAILTESTER_ACCEPT_CODES env (default 'ok').

    Comma-separated, e.g. 'ok' (strict, default) or 'ok,mb' (legacy lenient).
    Empty values are ignored.
    """
    raw = os.getenv("MAILTESTER_ACCEPT_CODES", "ok")
    return tuple(c.strip() for c in raw.split(",") if c.strip())


def _is_valid_code(code: str) -> bool:
    """Check if mailtester code indicates valid email per the configured policy."""
    return code in _accepted_codes()


def _backoff_delay(attempt: int) -> float:
    """Calculate retry delay with exponential backoff and jitter."""
    cap = min(_MAX_BACKOFF, _BASE_BACKOFF * (2 ** attempt))
    return random.uniform(0, cap)


async def verify_email(
    client: httpx.AsyncClient,
    email: str,
) -> dict[str, Any]:
    """
    Verify a single email address via mailtester.

    Args:
        client: Async HTTP client
        email: Email address to verify

    Returns:
        {
            "valid": bool,           # True if code is 'ok' or 'mb'
            "code": str,             # Response code from mailtester
            "message": str,          # Message from mailtester
            "email": str,            # Email that was verified
        }

    Raises:
        RuntimeError: If all retries exhausted (FAIL OPEN - caller should accept email)
    """
    if not email or "@" not in email:
        raise ValueError(f"Invalid email format: {email}")

    # Fast path: return a cached definitive result (collapses redundant
    # validations within a request and across Clay's re-queries).
    cached = _cache_get(email)
    if cached is not None:
        return cached

    # Fail fast when the mailtester/proxy circuit is open (degraded upstream)
    # instead of burning the full retry budget on every validation.
    if not await _mailtester_breaker.can_proceed():
        logger.warning("Mailtester circuit OPEN - failing open for %s", email)
        raise RuntimeError("Mailtester circuit open - failing open")

    params = {
        "email": email,
        "key": _get_api_key(),
    }

    last_error: Optional[Exception] = None

    for attempt in range(_MAX_RETRIES + 1):
        await _acquire_rate_limit()

        try:
            resp = await client.get(
                MAILTESTER_API_URL,
                params=params,
                timeout=30.0,
            )

            if resp.status_code == 200:
                data = resp.json()
                code = data.get("code", "")
                result = {
                    "valid": _is_valid_code(code),
                    "code": code,
                    "message": data.get("message", ""),
                    "email": data.get("email", email),
                }
                await _mailtester_breaker.record_success()
                _cache_set(email, result)
                return result
            elif resp.status_code >= 500:
                # Server error - retry
                await _mailtester_breaker.record_failure()
                delay = _backoff_delay(attempt)
                logger.warning(
                    "Mailtester server error (attempt %d/%d): %s - retrying in %.1fs",
                    attempt + 1, _MAX_RETRIES + 1, resp.status_code, delay,
                )
                await asyncio.sleep(delay)
                continue
            else:
                # Client error - don't retry (propagate, do NOT swallow below)
                await _mailtester_breaker.record_failure()
                logger.error("Mailtester client error: %s", resp.status_code)
                # FAIL OPEN on client errors
                raise RuntimeError(f"Mailtester client error: {resp.status_code}")

        except httpx.TimeoutException as e:
            last_error = e
            await _mailtester_breaker.record_failure()
            if attempt < _MAX_RETRIES:
                delay = _backoff_delay(attempt)
                logger.warning(
                    "Mailtester timeout (attempt %d/%d) - retrying in %.1fs",
                    attempt + 1, _MAX_RETRIES + 1, delay,
                )
                await asyncio.sleep(delay)
        except RuntimeError:
            # Propagate FAIL-OPEN (e.g. 4xx client error) instead of swallowing
            # it in the generic handler below (which would wrongly retry).
            raise
        except Exception as e:
            last_error = e
            await _mailtester_breaker.record_failure()
            if attempt < _MAX_RETRIES:
                delay = _backoff_delay(attempt)
                logger.warning(
                    "Mailtester error (attempt %d/%d): %s - retrying in %.1fs",
                    attempt + 1, _MAX_RETRIES + 1, e, delay,
                )
                await asyncio.sleep(delay)

    # All retries exhausted - FAIL OPEN
    logger.error("Mailtester verification failed after %d attempts: %s - FAILING OPEN", _MAX_RETRIES + 1, last_error)
    raise RuntimeError("Mailtester unavailable - failing open")
