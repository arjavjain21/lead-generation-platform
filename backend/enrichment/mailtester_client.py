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

logger = logging.getLogger(__name__)

MAILTESTER_API_URL = "https://validation.hyperke.org/ninja"
_MAX_RETRIES = 3
_BASE_BACKOFF = 2.0
_MAX_BACKOFF = 30.0
_RATE_LIMIT_RPS = 10  # Conservative rate limit
_MIN_REQUEST_INTERVAL = 1.0 / _RATE_LIMIT_RPS

_rate_limiter_lock = asyncio.Lock()
_last_request_time: float = 0.0


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
                return {
                    "valid": _is_valid_code(code),
                    "code": code,
                    "message": data.get("message", ""),
                    "email": data.get("email", email),
                }
            elif resp.status_code >= 500:
                # Server error - retry
                delay = _backoff_delay(attempt)
                logger.warning(
                    "Mailtester server error (attempt %d/%d): %s - retrying in %.1fs",
                    attempt + 1, _MAX_RETRIES + 1, resp.status_code, delay,
                )
                await asyncio.sleep(delay)
                continue
            else:
                # Client error - don't retry
                logger.error("Mailtester client error: %s", resp.status_code)
                # FAIL OPEN on client errors
                raise RuntimeError(f"Mailtester client error: {resp.status_code}")

        except httpx.TimeoutError as e:
            last_error = e
            if attempt < _MAX_RETRIES:
                delay = _backoff_delay(attempt)
                logger.warning(
                    "Mailtester timeout (attempt %d/%d) - retrying in %.1fs",
                    attempt + 1, _MAX_RETRIES + 1, delay,
                )
                await asyncio.sleep(delay)
        except Exception as e:
            last_error = e
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
