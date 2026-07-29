"""Blitz API client wrapping the three endpoints used in the enrichment pipeline."""

import asyncio
import logging
import os
import random
import time
from typing import Any, Optional

import httpx

from shared.circuit_breaker import get_circuit_breaker, CircuitBreakerError

BLITZ_BASE_URL = "https://api.blitz-api.ai"

logger = logging.getLogger(__name__)

# Circuit breaker for Blitz API
_blitz_circuit = get_circuit_breaker(
    "blitz_api",
    failure_threshold=10,
    recovery_timeout=60.0,
    half_open_max_calls=3,
)

# Retry config: up to 3 retries (4 attempts total), exponential backoff with jitter.
_MAX_RETRIES = 3
_BASE_BACKOFF = 2.0   # seconds before first retry
_MAX_BACKOFF = 60.0   # cap so we never wait more than a minute per attempt

# Rate limiting: Blitz API allows higher rate limits, using 25 RPS
_RATE_LIMIT_RPS = 25  # requests per second
_MIN_REQUEST_INTERVAL = 1.0 / _RATE_LIMIT_RPS  # seconds between requests

# Rate limiter state (thread-safe for async via asyncio.Lock)
_rate_limiter_lock = asyncio.Lock()
_last_request_time: float = 0.0

# Cache for domain→LinkedIn lookups (1 hour TTL)
_domain_linkedin_cache: dict[str, tuple[str, float]] = {}  # domain -> (result_json, timestamp)
_DOMAIN_CACHE_TTL = 3600  # 1 hour in seconds


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
    # Check circuit breaker before making request
    if not await _blitz_circuit.can_proceed():
        logger.warning(f"Blitz API circuit breaker OPEN, failing fast for {url}")
        raise CircuitBreakerError("blitz_api")

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
            # Record success with circuit breaker
            await _blitz_circuit.record_success()
            return resp.json()
        except httpx.HTTPStatusError as e:
            # 429 (rate-limit) and 4xx payload errors (400/404/422) must NOT trip
            # the circuit breaker — they mean the request was bad or over-limit,
            # not that Blitz is down. Tripping on a bad-payload storm (e.g.
            # un-normalized deep-URL domains -> 422) blacks out Blitz for valid
            # rows for 60s, degrading the cascade onto the slower BetterEnrich
            # fallback. Only real service failures (5xx etc.) trip it.
            if e.response.status_code not in (400, 404, 422, 429):
                await _blitz_circuit.record_failure()
            raise
        except Exception as exc:
            last_exc = exc
            # Record failure with circuit breaker
            await _blitz_circuit.record_failure()
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

    Uses 1-hour cache for domain→LinkedIn lookups to avoid redundant API calls.
    """
    domain_lower = domain.lower().strip()

    # Check cache first
    if domain_lower in _domain_linkedin_cache:
        cached_result, cached_time = _domain_linkedin_cache[domain_lower]
        if time.time() - cached_time < _DOMAIN_CACHE_TTL:
            logger.debug("Using cached domain→LinkedIn for %s", domain_lower)
            import json
            return json.loads(cached_result)

    # Make API call
    result = await _post_with_retry(
        client,
        f"{BLITZ_BASE_URL}/v2/enrichment/domain-to-linkedin",
        {"domain": domain},
        timeout=30.0,
    )

    # Cache the result
    import json
    _domain_linkedin_cache[domain_lower] = (json.dumps(result), time.time())
    logger.debug("Cached domain→LinkedIn for %s", domain_lower)

    return result


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


# =============================================================================
# NEW ENDPOINTS FOR LIST BUILDING TOOL
# =============================================================================

async def company_search(
    client: httpx.AsyncClient,
    *,
    # Company filters
    name: Optional[str] = None,
    industry: Optional[list[str]] = None,
    employee_range: Optional[list[str]] = None,
    company_type: Optional[list[str]] = None,
    country_code: Optional[str] = None,
    # Pagination
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    """
    POST /v2/search/companies
    Search for companies by various criteria.

    Args:
        client: Async HTTP client
        name: Company name to search for (partial match)
        industry: List of industries (e.g., ["Computer Software", "Information Technology"])
        employee_range: List of employee ranges (e.g., ["11-50", "51-200"])
        company_type: List of company types (e.g., ["Privately Held", "Public Company"])
        country_code: Country code (ISO 3166-1 alpha-2, e.g., "US", "GB")
        limit: Max results to return (default 100, max 1000)
        offset: Pagination offset

    Returns:
        {
            "count": 150,
            "total": 5000,
            "results": [
                {
                    "linkedin_url": "https://linkedin.com/company/acme",
                    "name": "Acme Corp",
                    "industry": "Computer Software",
                    "employee_count": 150,
                    "company_type": "Privately Held",
                    "hq": {"country_code": "US", "city": "San Francisco"}
                },
                ...
            ]
        }
    Cost: 1 credit per 5 results returned (minimum 1).
    """
    payload: dict[str, Any] = {}

    if name:
        payload["name"] = name
    if industry:
        payload["industry"] = {"include": industry}
    if employee_range:
        payload["employee_range"] = {"include": employee_range}
    if company_type:
        payload["company"] = {"type": {"include": company_type}}
    if country_code:
        payload["hq"] = {"country_code": country_code}

    payload["limit"] = min(limit, 1000)
    payload["offset"] = offset

    return await _post_with_retry(
        client,
        f"{BLITZ_BASE_URL}/v2/search/companies",
        payload,
        timeout=60.0,
    )


async def employee_finder(
    client: httpx.AsyncClient,
    *,
    # Company identification
    company_linkedin_url: Optional[str] = None,
    company_name: Optional[str] = None,
    domain: Optional[str] = None,
    # Person filters
    job_levels: Optional[list[str]] = None,
    job_functions: Optional[list[str]] = None,
    keywords: Optional[str] = None,
    # Location
    country_code: Optional[str] = None,
    sales_region: Optional[str] = None,
    continent: Optional[str] = None,
    # Pagination
    limit: int = 100,
) -> dict[str, Any]:
    """
    POST /v2/search/employees
    Find employees/people by company and/or criteria.

    Args:
        client: Async HTTP client
        company_linkedin_url: Company LinkedIn URL (e.g., https://linkedin.com/company/acme)
        company_name: Company name (alternative to LinkedIn URL)
        domain: Company domain (alternative to LinkedIn URL)
        job_levels: List of job levels (e.g., ["C-Team", "VP", "Director"])
        job_functions: List of job functions (e.g., ["Sales & Business Development", "Engineering"])
        keywords: Additional keyword search
        country_code: Country code filter (ISO 3166-1 alpha-2)
        sales_region: Sales region (NORAM, LATAM, EMEA, APAC)
        continent: Continent filter
        limit: Max results to return (default 100, max 100)

    Returns:
        {
            "count": 50,
            "results": [
                {
                    "linkedin_url": "https://linkedin.com/in/johndoe",
                    "full_name": "John Doe",
                    "first_name": "John",
                    "last_name": "Doe",
                    "headline": "VP of Sales at Acme Corp",
                    "title": "VP of Sales",
                    "job_level": "VP",
                    "job_function": "Sales & Business Development",
                    "company": {"linkedin_url": "...", "name": "Acme Corp"},
                    "location": {"country_code": "US", "city": "San Francisco"}
                },
                ...
            ]
        }
    Cost: 1 credit per result returned.
    """
    payload: dict[str, Any] = {}

    # Company identification (at least one required)
    if company_linkedin_url:
        payload["company_linkedin_url"] = company_linkedin_url
    elif company_name:
        payload["company_name"] = company_name
    elif domain:
        payload["domain"] = domain

    # Person filters
    if job_levels:
        payload["job_level"] = job_levels
    if job_functions:
        payload["job_function"] = job_functions
    if keywords:
        payload["keywords"] = keywords

    # Location
    if country_code:
        payload["country_code"] = country_code
    if sales_region:
        payload["sales_region"] = sales_region
    if continent:
        payload["continent"] = continent

    payload["limit"] = min(limit, 100)

    return await _post_with_retry(
        client,
        f"{BLITZ_BASE_URL}/v2/search/employees",
        payload,
        timeout=60.0,
    )


async def person_enrich(
    client: httpx.AsyncClient,
    *,
    # At least one of: linkedin_url OR (first_name + last_name + domain)
    linkedin_url: Optional[str] = None,
    first_name: Optional[str] = None,
    last_name: Optional[str] = None,
    full_name: Optional[str] = None,
    domain: Optional[str] = None,
    # Include additional data
    include_phone: bool = False,
    include_company: bool = True,
) -> dict[str, Any]:
    """
    POST /v2/enrichment/person
    Enrich a person profile with verified email and additional data.

    Args:
        client: Async HTTP client
        linkedin_url: Person's LinkedIn profile URL
        first_name: Person's first name
        last_name: Person's last name
        full_name: Person's full name (alternative to first_name + last_name)
        domain: Company domain (required if using name-based lookup)
        include_phone: Include phone number in response (if available)
        include_company: Include company details in response

    Returns:
        {
            "found": true,
            "person": {
                "linkedin_url": "https://linkedin.com/in/johndoe",
                "full_name": "John Doe",
                "first_name": "John",
                "last_name": "Doe",
                "headline": "VP of Sales at Acme Corp",
                "emails": ["john@acme.com"],
                "verified_email": "john@acme.com",
                "phone": "+1234567890" (if requested),
                "company": {...} (if requested)
            }
        }
    Cost: 1 credit on success, 0 if not found.
    """
    payload: dict[str, Any] = {}

    if linkedin_url:
        payload["linkedin_url"] = linkedin_url
    else:
        # Name-based lookup requires domain
        if full_name:
            parts = full_name.split(" ", 1)
            payload["first_name"] = parts[0]
            if len(parts) > 1:
                payload["last_name"] = parts[1]
        else:
            if first_name:
                payload["first_name"] = first_name
            if last_name:
                payload["last_name"] = last_name

        if domain:
            payload["domain"] = domain
        else:
            raise ValueError("domain is required when not using linkedin_url")

    # Optional fields
    if include_phone:
        payload["phone_number"] = True
    if not include_company:
        payload["company"] = False

    return await _post_with_retry(
        client,
        f"{BLITZ_BASE_URL}/v2/enrichment/person",
        payload,
        timeout=30.0,
    )


async def person_enrich_by_linkedin(
    client: httpx.AsyncClient,
    linkedin_url: str,
    include_phone: bool = False,
) -> dict[str, Any]:
    """
    POST /v2/enrichment/email
    Get work email from a LinkedIn profile URL.

    Args:
        client: Async HTTP client
        linkedin_url: Person's LinkedIn profile URL
        include_phone: Include phone number in response (not used, phone requires separate endpoint)

    Returns:
        {
            "found": true,
            "email": "john@acme.com",
            "all_emails": ["john@acme.com"]
        }
    Cost: 1 credit on success, 0 if not found.
    """
    payload: dict[str, Any] = {
        "person_linkedin_url": linkedin_url,
    }

    return await _post_with_retry(
        client,
        f"{BLITZ_BASE_URL}/v2/enrichment/email",
        payload,
        timeout=30.0,
    )
