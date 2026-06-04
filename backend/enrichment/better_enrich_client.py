"""
BetterEnrich API client for email enrichment.

BetterEnrich provides email enrichment services with:
- V2: Synchronous low-cost endpoint (10 RPS)
- V3: Async/polling with built-in verification and LinkedIn URL support (5 RPS)
- Company email: Generic/catchall company email lookup

Usage (V3 - recommended):
    result = await better_enrich_client.find_work_email_v3(
        client,
        full_name="John Doe",
        company_domain="google.com",
        linkedin_url="https://linkedin.com/in/johndoe"  # Optional, improves coverage
    )

Usage (V2 - fallback):
    result = await better_enrich_client.find_work_email_v2(
        client,
        full_name="John Doe",
        company_domain="google.com"
    )
"""

import asyncio
import logging
import os
import time
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

# Configuration
BASE_URL = os.getenv("BETTER_ENRICH_BASE_URL", "https://app.betterenrich.com")
API_KEY = os.getenv("BETTER_ENRICH_API_KEY", "")
RATE_LIMIT_RPS = 10

# Polling configuration
POLL_INTERVAL = 2.0  # seconds between polls
MAX_POLL_ATTEMPTS = 15  # max 30 seconds wait
REQUEST_TIMEOUT = 10.0  # timeout for each request

# Rate limiting
_rate_limiter_lock = asyncio.Lock()
_last_request_time = 0.0

# V3 Rate limiting (5 RPS as per Better Enrich V3 API)
_rate_limiter_lock_v3 = asyncio.Lock()
_last_request_time_v3 = 0.0
RATE_LIMIT_RPS_V3 = 5


async def _acquire_rate_limit():
    """Acquire rate limit slot (10 RPS)."""
    global _last_request_time
    async with _rate_limiter_lock:
        now = time.monotonic()
        interval = 1.0 / RATE_LIMIT_RPS
        if now - _last_request_time < interval:
            await asyncio.sleep(interval - (now - _last_request_time))
        _last_request_time = time.monotonic()


async def _acquire_rate_limit_v3():
    """Acquire rate limit slot for V3 (5 RPS)."""
    global _last_request_time_v3
    async with _rate_limiter_lock_v3:
        now = time.monotonic()
        interval = 1.0 / RATE_LIMIT_RPS_V3
        if now - _last_request_time_v3 < interval:
            await asyncio.sleep(interval - (now - _last_request_time_v3))
        _last_request_time_v3 = time.monotonic()


def _headers() -> dict:
    """Get headers for BetterEnrich API."""
    return {
        "Authorization": API_KEY,
        "Content-Type": "application/json"
    }


async def find_work_email(
    client: httpx.AsyncClient,
    full_name: str,
    company_domain: str,
    linkedin_url: Optional[str] = None,
) -> Optional[dict]:
    """
    Find work email using BetterEnrich's waterfall endpoint.

    This is an async operation:
    1. POST to start the task → get task_id
    2. Poll GET until status == "completed" or "failed"
    3. Return the email data

    Args:
        client: httpx AsyncClient
        full_name: Full name of the person (required)
        company_domain: Company domain (optional but helps)
        linkedin_url: LinkedIn profile URL (optional but helps)

    Returns:
        Dict with:
        {
            "email": "john@example.com",
            "status": "verified",
            "verifier": "...",
            "esp": "..."
        }
        Or None if not found / failed / not configured.
    """
    if not API_KEY:
        logger.warning("BetterEnrich API key not configured, skipping")
        return None

    await _acquire_rate_limit()

    # Step 1: Start the async task
    payload = {
        "full_name": full_name,
        "company_domain": company_domain,
    }
    if linkedin_url:
        payload["linkedinURL"] = linkedin_url

    try:
        resp = await client.post(
            f"{BASE_URL}/api/v1/find-work-email",
            headers=_headers(),
            json=payload,
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        result = resp.json()
        task_id = result.get("id")

        if not task_id:
            logger.warning("BetterEnrich didn't return task_id")
            return None

        logger.warning("BetterEnrich task started: %s", task_id)

    except httpx.HTTPStatusError as e:
        logger.warning("BetterEnrich HTTP error: %s", e.response.status_code)
        return None
    except Exception as e:
        logger.error("BetterEnrich start task failed: %s", e)
        return None

    # Step 2: Poll for results
    for attempt in range(MAX_POLL_ATTEMPTS):
        await asyncio.sleep(POLL_INTERVAL)

        try:
            resp = await client.get(
                f"{BASE_URL}/api/v1/find-work-email",
                headers=_headers(),
                params={"id": task_id},
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            result = resp.json()
            status = result.get("status")

            if status == "completed":
                data = result.get("data", {})
                email_data = {
                    "email": data.get("email"),
                    "status": data.get("status"),
                    "verifier": data.get("verifier"),
                    "esp": data.get("ESP"),
                }
                logger.warning("BetterEnrich found email: %s", email_data.get("email"))
                return email_data
            elif status == "failed" or status == "not_found":
                logger.warning("BetterEnrich task %s: %s", task_id, status)
                return None
            # else "processing" - continue polling

        except Exception as e:
            logger.warning("BetterEnrich poll failed (attempt %d): %s", attempt + 1, e)
            continue

    logger.warning("BetterEnrich task %s timed out after %d polls", task_id, MAX_POLL_ATTEMPTS)
    return None


async def find_work_email_v2(
    client: httpx.AsyncClient,
    full_name: str,
    company_domain: str,
) -> Optional[dict]:
    """
    Find work email using BetterEnrich's low-cost V2 endpoint.

    This is a synchronous endpoint that returns immediately.

    POST /api/v1/find-work-email-low-cost-v2-alt

    Args:
        client: httpx AsyncClient
        full_name: Full name of the person (required)
        company_domain: Company domain (required)

    Returns:
        Dict with:
        {
            "email": "john@example.com",
            "status": "verified" or "pending"
        }
        Or None if not found / failed / not configured.
    """
    if not API_KEY:
        logger.warning("BetterEnrich API key not configured, skipping")
        return None

    await _acquire_rate_limit()

    payload = {
        "full_name": full_name,
        "company_domain": company_domain,
    }

    try:
        resp = await client.post(
            f"{BASE_URL}/api/v1/find-work-email-low-cost-v2-alt",
            headers=_headers(),
            json=payload,
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        result = resp.json()
        status = result.get("status")

        # Handle both "completed" and "success" statuses (API may return either)
        if status in ("completed", "success"):
            data = result.get("data", {})
            # Check multiple possible paths for email
            email = data.get("email") or data.get("work_email") or data.get("data", {}).get("email")
            if email:
                logger.info("BetterEnrich V2 found email for %s: %s", full_name, email)
                return {
                    "email": email,
                    "status": data.get("status", "verified"),
                }
            else:
                logger.warning("BetterEnrich V2 returned success but no email in data for %s", full_name)
        elif status == "not_found":
            logger.info("BetterEnrich V2: person not found for %s at %s", full_name, company_domain)
            return None

        # Handle other statuses
        logger.warning("BetterEnrich V2 returned unhandled status: %s", status)
        return None

    except httpx.HTTPStatusError as e:
        logger.warning("BetterEnrich V2 HTTP error: %s", e.response.status_code)
        return None
    except Exception as e:
        # Log exception details for debugging
        import traceback
        err_msg = str(e) if str(e) else type(e).__name__
        logger.warning("BetterEnrich V2 request failed: %s | Response: %s",
                      err_msg, getattr(resp, 'text', 'N/A')[:200] if 'resp' in dir() else 'N/A')
        return None


async def find_work_email_v3(
    client: httpx.AsyncClient,
    full_name: str,
    company_domain: str,
    linkedin_url: Optional[str] = None,
) -> Optional[dict]:
    """
    Find work email using BetterEnrich's low-cost V3 endpoint.

    V3 features:
    - Built-in email verification (no need to verify again)
    - LinkedIn URL support for higher coverage
    - May return 201 (in progress) requiring polling

    POST /api/v1/find-work-email-low-cost-v3-alt
    GET /api/v1/find-work-email-low-cost-v3?id={task_id}

    Args:
        client: httpx AsyncClient
        full_name: Full name of the person (required)
        company_domain: Company domain (required)
        linkedin_url: LinkedIn profile URL (optional, improves coverage)

    Returns:
        Dict with:
        {
            "email": "john@example.com",
            "email_status": "verified" or "valid" or other status from API,
            "verifier": "...",
            "esp": "..."
        }
        Or None if not found / failed / not configured.
    """
    if not API_KEY:
        logger.warning("BetterEnrich API key not configured, skipping")
        return None

    await _acquire_rate_limit_v3()

    payload = {
        "full_name": full_name,
        "company_domain": company_domain,
    }
    if linkedin_url:
        payload["linkedinURL"] = linkedin_url

    try:
        resp = await client.post(
            f"{BASE_URL}/api/v1/find-work-email-low-cost-v3-alt",
            headers=_headers(),
            json=payload,
            timeout=REQUEST_TIMEOUT,
        )

        result = resp.json()
        status = result.get("status")

        # 201 means task is in progress, poll for result
        if resp.status_code == 201 or status == "processing":
            task_id = result.get("id")
            if not task_id:
                logger.warning("BetterEnrich V3 returned 201 but no task_id")
                return None
            logger.debug("BetterEnrich V3 task in progress: %s, polling...", task_id)
            return await _poll_v3_result(client, task_id)
        elif resp.status_code == 200:
            # Check if email was found
            if status == "not_found" or status == "failed":
                logger.info("BetterEnrich V3: person not found for %s at %s (status: %s)", full_name, company_domain, status)
                return None

            # Try to get email from various possible locations in response
            email = result.get("email") or result.get("data", {}).get("email")

            if email:
                logger.info("BetterEnrich V3 found email for %s: %s", full_name, email)
                return {
                    "email": email,
                    "email_status": result.get("email_status") or result.get("data", {}).get("email_status") or "verified",
                    "verifier": result.get("verifier") or result.get("data", {}).get("verifier"),
                    "esp": result.get("esp") or result.get("data", {}).get("esp"),
                }
            else:
                logger.info("BetterEnrich V3: person not found for %s at %s (no email in response)", full_name, company_domain)
                return None
        else:
            resp.raise_for_status()

    except httpx.HTTPStatusError as e:
        logger.warning("BetterEnrich V3 HTTP error: %s", e.response.status_code)
        return None
    except Exception as e:
        logger.warning("BetterEnrich V3 request failed: %s", e)
        return None


async def _poll_v3_result(client: httpx.AsyncClient, task_id: str) -> Optional[dict]:
    """Poll V3 endpoint for result."""
    for attempt in range(MAX_POLL_ATTEMPTS):
        await asyncio.sleep(POLL_INTERVAL)

        try:
            resp = await client.get(
                f"{BASE_URL}/api/v1/find-work-email-low-cost-v3",
                headers=_headers(),
                params={"id": task_id},
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            result = resp.json()
            status = result.get("status")

            if status == "completed":
                # Try to get email from various possible locations in response
                email = result.get("email") or result.get("data", {}).get("email")

                if email:
                    logger.info("BetterEnrich V3 found email after polling: %s", email)
                    return {
                        "email": email,
                        "email_status": result.get("email_status") or result.get("data", {}).get("email_status") or "verified",
                        "verifier": result.get("verifier") or result.get("data", {}).get("verifier"),
                        "esp": result.get("esp") or result.get("data", {}).get("esp"),
                    }
                else:
                    logger.warning("BetterEnrich V3 polling completed but no email")
                    return None
            elif status in ("failed", "not_found"):
                logger.info("BetterEnrich V3 task %s: %s", task_id, status)
                return None
            # else "processing" - continue polling

        except Exception as e:
            logger.warning("BetterEnrich V3 poll failed (attempt %d): %s", attempt + 1, e)
            continue

    logger.warning("BetterEnrich V3 task %s timed out after %d polls", task_id, MAX_POLL_ATTEMPTS)
    return None


async def get_credits_balance(client: httpx.AsyncClient) -> Optional[dict]:
    """Get BetterEnrich API credits balance."""
    if not API_KEY:
        return None

    await _acquire_rate_limit()

    try:
        resp = await client.get(
            f"{BASE_URL}/api/v1/credits",
            headers=_headers(),
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        return {
            "message": data.get("message"),
            "onetime_credit": data.get("onetimeCredit"),
            "subscription_credit": data.get("subscriptionCredit"),
            "total_credit": data.get("totalCredit"),
        }
    except Exception as e:
        logger.error("BetterEnrich credits check failed: %s", e)
        return None


async def find_company_email(
    client: httpx.AsyncClient,
    website: str,
) -> Optional[dict]:
    """
    Find company email (catchall/generic) using BetterEnrich.

    This is a synchronous endpoint that returns immediately.
    POST /api/v1/find-company-email

    Args:
        client: httpx AsyncClient
        website: Company website domain (e.g., "google.com")

    Returns:
        Dict with:
        {
            "email": "contact@company.com"
        }
        Or None if not found / failed / not configured.
    """
    if not API_KEY:
        logger.warning("BetterEnrich API key not configured, skipping company email lookup")
        return None

    await _acquire_rate_limit()

    payload = {
        "website": website,
    }

    try:
        resp = await client.post(
            f"{BASE_URL}/api/v1/find-company-email",
            headers=_headers(),
            json=payload,
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        result = resp.json()

        # Check for success
        if result.get("message") == "success":
            data = result.get("data", {})
            email = data.get("email")
            if email:
                logger.warning("BetterEnrich company email found: %s for %s", email, website)
                return {
                    "email": email,
                }
            logger.warning("BetterEnrich company email not found for: %s", website)
            return None

        # Handle not_found
        logger.warning("BetterEnrich company lookup not found for: %s", website)
        return None

    except httpx.HTTPStatusError as e:
        logger.warning("BetterEnrich company email HTTP error: %s for %s", e.response.status_code, website)
        return None
    except Exception as e:
        logger.error("BetterEnrich company email request failed: %s for %s", e, website)
        return None
