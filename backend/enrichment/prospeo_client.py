"""
Prospeo API client for person and company enrichment.

Prospeo provides B2B data enrichment including emails, mobile numbers, and company data.
- POST /enrich-person → Person enrichment with email + mobile
- POST /bulk-enrich-person → Batch person enrichment (up to 50)
- POST /enrich-company → Company enrichment
- Rate limit: 30 RPS

Usage:
    result = await prospeo_client.enrich_person(
        client,
        linkedin_url="https://linkedin.com/in/johndoe",
        full_name="John Doe",
        company_website="google.com"
    )
"""

import asyncio
import logging
import os
import time
from typing import Any, Optional

import httpx

logger = logging.getLogger(__name__)

# Configuration
BASE_URL = "https://api.prospeo.io"
API_KEY = os.getenv("PROSPEO_API_KEY", "")
RATE_LIMIT_RPS = 30

# Request configuration
REQUEST_TIMEOUT = 15.0  # seconds
MAX_RETRIES = 1  # retry once on rate limit / server errors

# Rate limiting
_rate_limiter_lock = asyncio.Lock()
_last_request_time = 0.0


async def _acquire_rate_limit():
    """Acquire rate limit slot (30 RPS)."""
    global _last_request_time
    async with _rate_limiter_lock:
        now = time.monotonic()
        interval = 1.0 / RATE_LIMIT_RPS
        if now - _last_request_time < interval:
            await asyncio.sleep(interval - (now - _last_request_time))
        _last_request_time = time.monotonic()


def _headers() -> dict:
    """Get headers for Prospeo API."""
    return {
        "X-KEY": API_KEY,
        "Content-Type": "application/json"
    }


def _is_retryable_error(status_code: int, error_code: Optional[str] = None) -> bool:
    """Check if an error is retryable."""
    # Retry on rate limit
    if status_code == 429:
        return True
    # Retry on server errors
    if status_code >= 500:
        return True
    # Retry on specific error codes
    retryable_codes = {"RATE_LIMITED", "INTERNAL_ERROR"}
    if error_code in retryable_codes:
        return True
    return False


async def enrich_person(
    client: httpx.AsyncClient,
    linkedin_url: Optional[str] = None,
    full_name: Optional[str] = None,
    first_name: Optional[str] = None,
    last_name: Optional[str] = None,
    company_website: Optional[str] = None,
    company_name: Optional[str] = None,
    company_linkedin_url: Optional[str] = None,
    only_verified_email: bool = False,
) -> Optional[dict]:
    """
    Enrich a person with email and company data.

    POST /enrich-person

    Minimum requirements for matching (one of):
    - linkedin_url
    - email
    - first_name + last_name + (company_name OR company_website OR company_linkedin_url)
    - full_name + (company_name OR company_website OR company_linkedin_url)

    Args:
        client: httpx AsyncClient
        linkedin_url: Person's LinkedIn profile URL
        full_name: Full name of the person
        first_name: First name
        last_name: Last name
        company_website: Company website domain
        company_name: Company name
        company_linkedin_url: Company's LinkedIn URL
        only_verified_email: Only return verified emails (default False)

    Returns:
        Dict with person and company data, or None if not found / failed.
        {
            "person": {
                "full_name": "John Doe",
                "first_name": "John",
                "last_name": "Doe",
                "email": {"email": "john@company.com", "status": "VERIFIED"},
                "mobile": {...},
                "linkedin_url": "...",
                "current_job_title": "CEO",
                "location": {...}
            },
            "company": {
                "name": "Company",
                "domain": "company.com",
                "industry": "Technology",
                "employee_count": 100
            }
        }
    """
    if not API_KEY:
        logger.warning("Prospeo API key not configured, skipping")
        return None

    await _acquire_rate_limit()

    # Build data payload
    data: dict[str, Any] = {}

    if linkedin_url:
        data["linkedin_url"] = linkedin_url
    if full_name:
        data["full_name"] = full_name
    if first_name:
        data["first_name"] = first_name
    if last_name:
        data["last_name"] = last_name
    if company_website:
        data["company_website"] = company_website
    if company_name:
        data["company_name"] = company_name
    if company_linkedin_url:
        data["company_linkedin_url"] = company_linkedin_url

    if not data:
        logger.warning("Prospeo: No identifying data provided")
        return None

    payload: dict[str, Any] = {"data": data}
    if only_verified_email:
        payload["only_verified_email"] = True

    for attempt in range(MAX_RETRIES + 1):
        try:
            resp = await client.post(
                f"{BASE_URL}/enrich-person",
                headers=_headers(),
                json=payload,
                timeout=REQUEST_TIMEOUT,
            )

            # Handle error codes in response
            if resp.status_code == 400:
                result = resp.json()
                error_code = result.get("error_code", "")

                if error_code == "NO_MATCH":
                    logger.debug("Prospeo: Person not found")
                    return None
                elif error_code == "INVALID_DATAPOINTS":
                    logger.warning("Prospeo: Invalid datapoints provided: %s", data)
                    return None
                elif error_code == "INSUFFICIENT_CREDITS":
                    logger.warning("Prospeo: Insufficient credits")
                    return None
                elif error_code == "INVALID_API_KEY":
                    logger.error("Prospeo: Invalid API key")
                    return None

            resp.raise_for_status()
            result = resp.json()

            if result.get("error"):
                error_code = result.get("error_code", "")
                if error_code == "NO_MATCH":
                    logger.debug("Prospeo: Person not found")
                    return None
                logger.warning("Prospeo enrichment error: %s", error_code)
                return None

            # Success - return person and company data
            person_data = result.get("person", {})
            company_data = result.get("company", {})

            if not person_data:
                logger.debug("Prospeo: No person data returned")
                return None

            logger.debug("Prospeo: Enriched person %s", person_data.get("full_name", ""))
            return {
                "person": person_data,
                "company": company_data,
                "free_enrichment": result.get("free_enrichment", False),
            }

        except httpx.HTTPStatusError as e:
            if _is_retryable_error(e.response.status_code):
                if attempt < MAX_RETRIES:
                    logger.debug("Prospeo: Retryable error %s, retrying...", e.response.status_code)
                    await asyncio.sleep(0.5)  # Brief delay before retry
                    continue
            logger.warning("Prospeo HTTP error: %s", e.response.status_code)
            return None

        except httpx.TimeoutException:
            if attempt < MAX_RETRIES:
                logger.debug("Prospeo: Timeout, retrying...")
                continue
            logger.warning("Prospeo request timed out")
            return None

        except Exception as e:
            logger.error("Prospeo enrichment failed: %s", e)
            return None

    return None


async def enrich_company(
    client: httpx.AsyncClient,
    company_website: Optional[str] = None,
    company_name: Optional[str] = None,
    company_linkedin_url: Optional[str] = None,
) -> Optional[dict]:
    """
    Enrich a company with full B2B data.

    POST /enrich-company

    Minimum requirements (one of):
    - company_website
    - company_linkedin_url
    - company_name

    Args:
        client: httpx AsyncClient
        company_website: Company website domain
        company_name: Company name
        company_linkedin_url: Company's LinkedIn URL

    Returns:
        Dict with company data, or None if not found / failed.
        {
            "company": {
                "name": "Company",
                "domain": "company.com",
                "industry": "Technology",
                "employee_count": 100,
                "location": {...},
                "linkedin_url": "..."
            }
        }
    """
    if not API_KEY:
        logger.warning("Prospeo API key not configured, skipping company lookup")
        return None

    await _acquire_rate_limit()

    # Build data payload
    data: dict[str, Any] = {}

    if company_website:
        data["company_website"] = company_website
    if company_name:
        data["company_name"] = company_name
    if company_linkedin_url:
        data["company_linkedin_url"] = company_linkedin_url

    if not data:
        logger.warning("Prospeo: No company data provided")
        return None

    for attempt in range(MAX_RETRIES + 1):
        try:
            resp = await client.post(
                f"{BASE_URL}/enrich-company",
                headers=_headers(),
                json={"data": data},
                timeout=REQUEST_TIMEOUT,
            )

            if resp.status_code == 400:
                result = resp.json()
                error_code = result.get("error_code", "")

                if error_code == "NO_MATCH":
                    logger.debug("Prospeo: Company not found")
                    return None
                elif error_code == "INSUFFICIENT_CREDITS":
                    logger.warning("Prospeo: Insufficient credits for company lookup")
                    return None
                elif error_code == "INVALID_API_KEY":
                    logger.error("Prospeo: Invalid API key")
                    return None

            resp.raise_for_status()
            result = resp.json()

            if result.get("error"):
                error_code = result.get("error_code", "")
                if error_code == "NO_MATCH":
                    logger.debug("Prospeo: Company not found")
                    return None
                logger.warning("Prospeo company enrichment error: %s", error_code)
                return None

            # Success
            company_data = result.get("company", {})
            if not company_data:
                logger.debug("Prospeo: No company data returned")
                return None

            logger.debug("Prospeo: Enriched company %s", company_data.get("name", ""))
            return {
                "company": company_data,
                "free_enrichment": result.get("free_enrichment", False),
            }

        except httpx.HTTPStatusError as e:
            if _is_retryable_error(e.response.status_code):
                if attempt < MAX_RETRIES:
                    logger.debug("Prospeo company: Retryable error %s, retrying...", e.response.status_code)
                    await asyncio.sleep(0.5)
                    continue
            logger.warning("Prospeo company HTTP error: %s", e.response.status_code)
            return None

        except httpx.TimeoutException:
            if attempt < MAX_RETRIES:
                logger.debug("Prospeo company: Timeout, retrying...")
                continue
            logger.warning("Prospeo company request timed out")
            return None

        except Exception as e:
            logger.error("Prospeo company enrichment failed: %s", e)
            return None

    return None


async def get_account_info(client: httpx.AsyncClient) -> Optional[dict]:
    """
    Get Prospeo account information and credit balance.

    GET /account-information

    Returns:
        Dict with account info:
        {
            "current_plan": "STARTER",
            "remaining_credits": 99,
            "used_credits": 1,
            "next_quota_renewal_days": 25
        }
    """
    if not API_KEY:
        return None

    # This endpoint is free, but still respect rate limits
    await _acquire_rate_limit()

    try:
        resp = await client.get(
            f"{BASE_URL}/account-information",
            headers=_headers(),
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()

        if data.get("error"):
            logger.warning("Prospeo account info error: %s", data.get("error_code"))
            return None

        return data.get("response")

    except httpx.HTTPStatusError as e:
        logger.warning("Prospeo account info HTTP error: %s", e.response.status_code)
        return None
    except Exception as e:
        logger.error("Prospeo account info failed: %s", e)
        return None


def extract_email_from_prospeo(result: Optional[dict]) -> str:
    """
    Extract email from Prospeo enrichment result.

    Args:
        result: Prospeo enrichment result dict

    Returns:
        Email string or empty string if not found
    """
    if not result:
        return ""

    person = result.get("person", {})
    email_obj = person.get("email", {})

    if isinstance(email_obj, dict):
        return email_obj.get("email", "")
    elif isinstance(email_obj, str):
        return email_obj

    return ""


def extract_verified_status(result: Optional[dict]) -> str:
    """
    Extract email verification status from Prospeo result.

    Returns:
        "verified", "pending", or "unknown"
    """
    if not result:
        return "unknown"

    person = result.get("person", {})
    email_obj = person.get("email", {})

    if isinstance(email_obj, dict):
        status = email_obj.get("status", "").lower()
        if "verified" in status:
            return "verified"
        elif status:
            return "pending"

    return "unknown"
