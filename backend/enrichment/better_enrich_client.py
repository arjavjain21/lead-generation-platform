"""
BetterEnrich API client for email enrichment.

BetterEnrich provides email enrichment services with:
- V1: Async legacy endpoint (10 RPS)
- V2: Synchronous low-cost endpoint (10 RPS)
- V3: Async/polling with built-in verification and LinkedIn URL support (5 RPS)
- Company email: Generic/catchall company email lookup (5 RPS — shared limiter)
- Facebook page email: Email from Facebook page URL (5 RPS — shared limiter)

Per BetterEnrich docs, V3, company email, and facebook page endpoints each share
a 5 RPS budget. We model this with a single shared 5 RPS limiter across all three
to be conservative and prevent aggregate overshoot.

Usage (V3 - recommended for person lookup):
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

Usage (company fallback):
    result = await better_enrich_client.find_company_email(client, website="google.com")

Usage (Facebook page fallback):
    result = await better_enrich_client.find_email_from_facebook_page(
        client, page_url="https://facebook.com/somepage"
    )
"""

import asyncio
import logging
import os
import time
from typing import Optional

import httpx

from . import pipeline  # Import for _ProviderError and _classify_http_error

logger = logging.getLogger(__name__)

# Configuration
BASE_URL = os.getenv("BETTER_ENRICH_BASE_URL", "https://app.betterenrich.com")
API_KEY = os.getenv("BETTER_ENRICH_API_KEY", "")
RATE_LIMIT_RPS = 10  # Legacy V1/V2 endpoint budget
RATE_LIMIT_RPS_V3 = 5  # V3 budget
# V3, Company email, and Facebook page endpoints each documented at 5 RPS.
# We use a single shared 5 RPS limiter across all three to keep total
# BetterEnrich request rate at or below 5 RPS — see the contract in
# LOOP_BETTERENRICH_FALLBACKS.md acceptance test 7.
RATE_LIMIT_RPS_SHARED = 5

# Polling configuration
POLL_INTERVAL = 2.0  # seconds between polls
MAX_POLL_ATTEMPTS = 15  # max 30 seconds wait
REQUEST_TIMEOUT = 10.0  # timeout for each request

# Rate limiting — V1/V2 (10 RPS)
_rate_limiter_lock = asyncio.Lock()
_last_request_time = 0.0

# Rate limiting — V3 (5 RPS)
_rate_limiter_lock_v3 = asyncio.Lock()
_last_request_time_v3 = 0.0

# Rate limiting — shared 5 RPS for V3 + Company + Facebook (see above).
_rate_limiter_lock_shared = asyncio.Lock()
_last_request_time_shared = 0.0


async def _acquire_rate_limit():
    """Acquire rate limit slot (10 RPS) — V1/V2 endpoints."""
    global _last_request_time
    async with _rate_limiter_lock:
        now = time.monotonic()
        interval = 1.0 / RATE_LIMIT_RPS
        if now - _last_request_time < interval:
            await asyncio.sleep(interval - (now - _last_request_time))
        _last_request_time = time.monotonic()


async def _acquire_rate_limit_v3():
    """Acquire rate limit slot for V3 (5 RPS) — DEPRECATED: use _acquire_shared_rate_limit.

    Kept for back-compat; new code should call _acquire_shared_rate_limit so
    the 5 RPS budget is shared with Company and Facebook endpoints.
    """
    return await _acquire_shared_rate_limit()


async def _acquire_shared_rate_limit():
    """Acquire the shared 5 RPS rate limit slot for V3 + Company + Facebook.

    This is the single throttle that governs all three endpoints. Tests
    that exercise these endpoints together must observe that the combined
    request rate does not exceed 5 RPS.
    """
    global _last_request_time_shared
    async with _rate_limiter_lock_shared:
        now = time.monotonic()
        interval = 1.0 / RATE_LIMIT_RPS_SHARED
        if now - _last_request_time_shared < interval:
            await asyncio.sleep(interval - (now - _last_request_time_shared))
        _last_request_time_shared = time.monotonic()


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
        error_type, message = pipeline._classify_http_error(e, "better_enrich", "find_work_email_v2")
        return pipeline._ProviderError(
            provider="better_enrich",
            method="find_work_email_v2",
            error_type=error_type,
            message=message,
        )
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

    await _acquire_shared_rate_limit()

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
        error_type, message = pipeline._classify_http_error(e, "better_enrich", "find_work_email_v3")
        return pipeline._ProviderError(
            provider="better_enrich",
            method="find_work_email_v3",
            error_type=error_type,
            message=message,
        )
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
            "email": "contact@company.com",
            "email_status": "verified" | "valid" | "unverified" | "unknown",
        }
        Or None if not found / failed / not configured.

    Note: Shares the 5 RPS BetterEnrich shared limiter with V3 and Facebook
    page endpoints. Caller should dedupe by normalized_domain.
    """
    if not API_KEY:
        logger.warning("BetterEnrich API key not configured, skipping company email lookup")
        return None

    await _acquire_shared_rate_limit()

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
                    "email_status": data.get("email_status") or data.get("status") or "unknown",
                }
            logger.warning("BetterEnrich company email not found for: %s", website)
            return None

        # Handle not_found
        logger.warning("BetterEnrich company lookup not found for: %s", website)
        return None

    except httpx.HTTPStatusError as e:
        logger.warning("BetterEnrich company email HTTP error: %s for %s", e.response.status_code, website)
        error_type, message = pipeline._classify_http_error(e, "better_enrich", "find_company_email")
        return pipeline._ProviderError(
            provider="better_enrich",
            method="find_company_email",
            error_type=error_type,
            message=message,
        )
    except Exception as e:
        logger.error("BetterEnrich company email request failed: %s for %s", e, website)
        return None


async def find_email_from_facebook_page(
    client: httpx.AsyncClient,
    page_url: str,
) -> Optional[dict]:
    """
    Find email from a Facebook page using BetterEnrich.

    This is a synchronous endpoint that returns immediately.
    POST /api/v1/find-email-from-facebook-page
    Body: { "pageURL": "<facebook_page_url>" }
    Rate limit: 5 requests/second (shared with V3 and company email).

    The returned email is a page-level / company-level email, not a
    decision-maker email. Callers MUST route it to company_email, not dm_email.

    Args:
        client: httpx AsyncClient
        page_url: A Facebook page URL (e.g. "https://facebook.com/somepage").
            Non-Facebook URLs are rejected (returns None).

    Returns:
        Dict with:
        {
            "email": "page@company.com",
            "email_status": "verified" | "valid" | "unverified" | "unknown",
        }
        Or None if not found / failed / not configured / invalid page URL.
    """
    if not API_KEY:
        logger.warning("BetterEnrich API key not configured, skipping Facebook page email lookup")
        return None

    if not page_url or "facebook.com" not in page_url.lower():
        logger.warning("BetterEnrich facebook: rejected non-Facebook page_url=%r", page_url)
        return None

    await _acquire_shared_rate_limit()

    payload = {
        "pageURL": page_url,
    }

    try:
        resp = await client.post(
            f"{BASE_URL}/api/v1/find-email-from-facebook-page",
            headers=_headers(),
            json=payload,
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        result = resp.json()

        # Check for success (mirrors find-company-email shape)
        if result.get("message") == "success":
            data = result.get("data", {})
            email = data.get("email")
            if email:
                logger.warning("BetterEnrich facebook email found: %s for %s", email, page_url)
                return {
                    "email": email,
                    "email_status": data.get("email_status") or data.get("status") or "unknown",
                }
            logger.warning("BetterEnrich facebook email not found for: %s", page_url)
            return None

        # not_found
        logger.warning("BetterEnrich facebook lookup not found for: %s", page_url)
        return None

    except httpx.HTTPStatusError as e:
        logger.warning("BetterEnrich facebook HTTP error: %s for %s", e.response.status_code, page_url)
        error_type, message = pipeline._classify_http_error(e, "better_enrich", "find_email_from_facebook_page")
        return pipeline._ProviderError(
            provider="better_enrich",
            method="find_email_from_facebook_page",
            error_type=error_type,
            message=message,
        )
    except Exception as e:
        logger.error("BetterEnrich facebook request failed: %s for %s", e, page_url)
        return None
