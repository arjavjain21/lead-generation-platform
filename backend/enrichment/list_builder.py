"""
List Building Pipeline - Unified Enrichment Tool

Supports 3 flows:
1. Domain Upload → Generic Emails + Decision Makers
2. Search Criteria → Companies → Enrich
3. LinkedIn URLs → Full Enrichment

Always prioritizes internal Contacts DB over Blitz API for cost savings.
"""

from __future__ import annotations

import asyncio
import csv
import logging
import uuid
from pathlib import Path
from typing import Any, Callable, Optional

import httpx

from . import blitz_client
from . import contacts_client
from . import providers

logger = logging.getLogger(__name__)


# Valid provider values for force_provider parameter
VALID_PROVIDERS = frozenset({"contacts_db", "blitz", "better_enrich"})


def _should_skip_provider(provider: str, force_provider: Optional[str]) -> bool:
    """
    Determine if a provider should be skipped.

    Args:
        provider: The current provider being considered (e.g., "contacts_db", "blitz")
        force_provider: The forced provider from request (or None for normal cascade)

    Returns:
        True if the provider should be skipped, False otherwise

    Checks:
      1. Is the provider globally disabled in ENABLED_PROVIDERS?
      2. If force_provider is set, does it match the current provider?
    """
    # First check: is the provider globally disabled?
    if not providers.is_provider_enabled(provider):
        logger.debug("_should_skip_provider: %s disabled in ENABLED_PROVIDERS", provider)
        return True

    # Second check: force_provider constraint
    if force_provider:
        return provider != force_provider

    return False

# Concurrency settings - increased for better throughput
DOMAIN_CONCURRENCY = 25  # Increased from 5 for faster domain processing
LINKEDIN_CONCURRENCY = 15  # Increased from 10
SEARCH_CONCURRENCY = 5  # Increased from 3

# Rate limits
CONTACTS_DB_RATE_LIMIT = 50  # requests per second
BLITZ_API_RATE_LIMIT = 25  # requests per second


# =============================================================================
# Data Types
# =============================================================================

OutputRow = dict[str, Any]

# Status values
STATUS_ENRICHED = "enriched"
STATUS_NO_LINKEDIN = "no_linkedin"
STATUS_NO_CONTACTS = "no_contacts"
STATUS_NOT_FOUND = "not_found"
STATUS_ERROR = "error"
STATUS_SKIPPED = "skipped"

# Email source values (prioritized)
SOURCE_CONTACTS_DB_LINKEDIN = "contacts_db_linkedin"
SOURCE_CONTACTS_DB_NAME = "contacts_db_name"
SOURCE_CONTACTS_DB_DOMAIN = "contacts_db_domain"
SOURCE_BLITZ_LINKEDIN = "blitz_linkedin"
SOURCE_BLITZ_NAME = "blitz_name"
SOURCE_BLITZ_DOMAIN = "blitz_domain"
SOURCE_NOT_FOUND = "not_found"


# =============================================================================
# Helper Functions
# =============================================================================

def _empty_enriched() -> dict[str, Any]:
    """Return empty enriched columns."""
    return {
        "company_linkedin_url": "",
        "company_name": "",
        "company_industry": "",
        "company_employee_count": "",
        "dm_first_name": "",
        "dm_last_name": "",
        "dm_full_name": "",
        "dm_title": "",
        "dm_job_level": "",
        "dm_job_function": "",
        "dm_linkedin_url": "",
        "dm_email": "",
        "dm_email_source": "",
        "dm_email_verified": "unknown",
        "dm_phone": "",
        "dm_headline": "",
        "dm_location_city": "",
        "dm_location_country": "",
        "dm_icp_tier": "",
        "row_status": "",
    }


def _current_title(experiences: list[dict]) -> str:
    """Extract current job title from experiences."""
    for exp in experiences or []:
        if exp.get("job_is_current"):
            return exp.get("job_title", "")
    if experiences:
        return experiences[0].get("job_title", "")
    return ""


def _normalize_source(source: str) -> str:
    """Map raw source value to provider group."""
    if source.startswith("contacts_db"):
        return "contacts_db"
    elif source.startswith("blitz"):
        return "blitz"
    elif source.startswith("better_enrich"):
        return "better_enrich"
    elif source.startswith("prospeo"):
        return "prospeo"
    return source


# =============================================================================
# Flow 1: Domain → Generic Emails + Decision Makers
# =============================================================================

async def _resolve_person_email(
    blitz_http: httpx.AsyncClient,
    contacts_http: httpx.AsyncClient,
    person_data: dict[str, Any],
    domain: str,
    input_full_name: str = "",
    force_provider: Optional[str] = None,
) -> tuple[str, str, str, str]:
    """
    Resolve email for a person using Contacts DB first, then Blitz fallback.

    Returns: (email, phone, source, verified)

    Args:
        force_provider: If set, only use that specific provider.
    """
    linkedin_url = person_data.get("linkedin_url", "")
    full_name = person_data.get("full_name", "")
    first_name = person_data.get("first_name", "")
    last_name = person_data.get("last_name", "")

    # Determine search name (prefer person full_name, fall back to input_full_name)
    search_name = full_name or input_full_name

    # Strategy 1: Contacts DB by name + domain (PRIMARY - FREE)
    # When both name and LinkedIn URL are available, prefer name+domain to avoid
    # returning stale emails from previous employers via person_by_linkedin
    if search_name and domain and not _should_skip_provider("contacts_db", force_provider):
        try:
            contacts_data = await contacts_client.person_by_name_and_domain(
                contacts_http, search_name, domain
            )
            email = contacts_client.extract_email_from_contacts_response(contacts_data)
            if email:
                phone = contacts_data.get("phone", "") if contacts_data else ""
                return email, phone, SOURCE_CONTACTS_DB_NAME, "no"
        except Exception as e:
            logger.debug("Contacts DB name+domain lookup failed: %s", e)

    # Strategy 2: Contacts DB by LinkedIn URL (SECONDARY - FREE)
    # Fall back to LinkedIn if name+domain didn't find email, or if name not available
    if linkedin_url and not _should_skip_provider("contacts_db", force_provider):
        try:
            contacts_data = await contacts_client.person_by_linkedin(
                contacts_http, linkedin_url
            )
            email = contacts_client.extract_email_from_contacts_response(contacts_data)
            if email:
                phone = contacts_data.get("phone", "") if contacts_data else ""
                # Contacts DB emails are from internal database - mark as unverified (no verification check)
                return email, phone, SOURCE_CONTACTS_DB_LINKEDIN, "no"
        except Exception as e:
            logger.debug("Contacts DB LinkedIn lookup failed: %s", e)

    # Strategy 3: Blitz person enrich by name + domain (PRIMARY - PAID)
    # Prioritize name+domain for the same reason as Contacts DB
    if search_name and domain and not _should_skip_provider("blitz", force_provider):
        try:
            result = await blitz_client.person_enrich(
                blitz_http,
                full_name=search_name,
                domain=domain,
                include_phone=True,
            )
            if result.get("found") and result.get("person"):
                person = result.get("person", {})
                # Check for verified_email field - if present, it's verified
                verified_email = person.get("verified_email", "")
                if verified_email:
                    email = verified_email
                    verified = "yes"
                else:
                    # Fall back to unverified emails list
                    emails_list = person.get("emails", [])
                    email = emails_list[0].get("email", "") if emails_list else ""
                    verified = "no"
                phone = person.get("phone", "")
                if email:
                    source = SOURCE_BLITZ_NAME
                    return email, phone, source, verified
        except Exception as e:
            logger.debug("Blitz person enrich failed: %s", e)

    # Strategy 4: Blitz API by LinkedIn URL (SECONDARY - PAID)
    # Fall back to LinkedIn if name+domain didn't find email, or if name not available
    if linkedin_url and not _should_skip_provider("blitz", force_provider):
        try:
            result = await blitz_client.find_work_email(blitz_http, linkedin_url)
            if result.get("found") and result.get("email"):
                # Blitz email endpoint - check if there's verification info
                all_emails = result.get("all_emails", [])
                verified = "yes" if all_emails and all_emails[0].get("verified") else "unknown"
                return result.get("email", ""), "", SOURCE_BLITZ_LINKEDIN, verified
        except Exception as e:
            logger.debug("Blitz email lookup failed: %s", e)

    return "", "", SOURCE_NOT_FOUND, "unknown"


async def _enrich_single_domain(
    blitz_http: httpx.AsyncClient,
    contacts_http: httpx.AsyncClient,
    base_row: dict[str, Any],
    domain: str,
    max_decision_makers: int = 5,
    include_generic_emails: bool = True,
    domain_semaphore: asyncio.Semaphore = None,
    email_semaphore: asyncio.Semaphore = None,
    force_provider: Optional[str] = None,
) -> list[OutputRow]:
    """
    Enrich a single domain: get company info, generic emails, and decision makers.

    Returns list of output rows (one per decision maker + generic emails if requested).

    Args:
        force_provider: If set, only use that specific provider.
    """
    if not domain_semaphore:
        domain_semaphore = asyncio.Semaphore(DOMAIN_CONCURRENCY)
    if not email_semaphore:
        email_semaphore = asyncio.Semaphore(LINKEDIN_CONCURRENCY)

    output_rows = []
    company_linkedin_url = ""
    company_name = ""
    company_industry = ""
    company_employee_count = ""

    async with domain_semaphore:
        # Step 1: Get company info from Contacts DB (PRIMARY - FREE)
        try:
            contacts_company = await contacts_client.company_by_domain(contacts_http, domain)
            if contacts_company:
                company_linkedin_url = contacts_company.get("linkedin_url", "")
                company_name = contacts_company.get("name", "")
                company_industry = contacts_company.get("industry", "")
                # Employee count might be in different field
                company_employee_count = str(contacts_company.get("employee_count", ""))
                logger.debug("Found company via Contacts DB: %s", company_name)
        except Exception as e:
            logger.debug("Contacts DB company lookup failed: %s", e)

        # Fallback: Blitz API if not found in Contacts DB
        if not company_linkedin_url:
            try:
                d2l = await blitz_client.domain_to_linkedin(blitz_http, domain)
                if d2l.get("found"):
                    company_linkedin_url = d2l.get("company_linkedin_url", "")
                    logger.debug("Found company via Blitz: %s", company_linkedin_url)
            except Exception as e:
                logger.debug("Blitz domain_to_linkedin failed: %s", e)

    if not company_linkedin_url:
        # No company found - return error row
        row = {**base_row, **_empty_enriched()}
        row["row_status"] = STATUS_NO_LINKEDIN
        output_rows.append(row)

        # Still try to get generic emails from Contacts DB by domain
        if include_generic_emails:
            try:
                contacts = await contacts_client.company_contacts_enriched(
                    contacts_http, domain, limit=10
                )
                if contacts and len(contacts) > 0:
                    # Extract any emails found
                    for contact in contacts:
                        email = contact.get("email", "")
                        if email and "@" in email:
                            row = {**base_row, **_empty_enriched()}
                            row["company_name"] = domain
                            row["dm_full_name"] = contact.get("full_name", "")
                            row["dm_email"] = email
                            row["dm_email_source"] = SOURCE_CONTACTS_DB_DOMAIN
                            row["row_status"] = STATUS_ENRICHED
                            output_rows.append(row)
            except Exception as e:
                logger.debug("Contacts DB domain contacts lookup failed: %s", e)

        return output_rows if output_rows else [{**base_row, **_empty_enriched(), "row_status": STATUS_NOT_FOUND}]

    # Step 2: Get decision makers from Contacts DB first (FREE)
    persons: list[dict[str, Any]] = []
    use_blitz = False

    async with domain_semaphore:
        try:
            contacts_contacts = await contacts_client.company_contacts_enriched(
                contacts_http, domain, limit=max_decision_makers
            )
            if contacts_contacts and len(contacts_contacts) > 0:
                # Check quality: need at least 1 person with email
                emails_count = sum(1 for c in contacts_contacts if c.get("email"))
                if len(contacts_contacts) >= 1 and emails_count >= 1:
                    # Convert to standard format
                    for contact in contacts_contacts[:max_decision_makers]:
                        persons.append({
                            "person": {
                                "first_name": contact.get("first_name", ""),
                                "last_name": contact.get("last_name", ""),
                                "full_name": contact.get("full_name", ""),
                                "headline": contact.get("headline", ""),
                                "linkedin_url": contact.get("linkedin_url", ""),
                                "location": {
                                    "city": contact.get("city", ""),
                                    "country_code": contact.get("country_code", ""),
                                },
                                "experiences": [],
                            },
                            "icp": 0,  # Contacts DB doesn't provide ICP
                        })
                    logger.debug("Using Contacts DB for %d decision makers", len(persons))
        except Exception as e:
            logger.debug("Contacts DB contacts lookup failed: %s", e)
            use_blitz = True

        # Fallback: Blitz waterfall search if Contacts DB didn't meet quality threshold
        if not persons:
            use_blitz = True

        if use_blitz:
            try:
                cascade = blitz_client.DEFAULT_CASCADE
                icp_result = await blitz_client.waterfall_icp_search(
                    blitz_http, company_linkedin_url, cascade, max_decision_makers
                )
                persons = icp_result.get("results", [])
                logger.debug("Using Blitz for %d decision makers", len(persons))
            except Exception as e:
                logger.debug("Blitz waterfall search failed: %s", e)
                row = {**base_row, **_empty_enriched()}
                row["company_linkedin_url"] = company_linkedin_url
                row["row_status"] = STATUS_ERROR
                return [row]

    if not persons:
        row = {**base_row, **_empty_enriched()}
        row["company_linkedin_url"] = company_linkedin_url
        row["row_status"] = STATUS_NO_CONTACTS
        return [row]

    # Step 3: Resolve emails for each person
    tasks = []
    for item in persons:
        person = item.get("person", {})
        tasks.append(
            _resolve_person_email(
                blitz_http,
                contacts_http,
                person,
                domain,
                force_provider=force_provider,
            )
        )

    email_results = await asyncio.gather(*tasks)

    # Build output rows
    for item, (email, phone, source, verified) in zip(persons, email_results):
        person = item.get("person", {})
        icp_tier = item.get("icp", 0)

        row = {**base_row, **_empty_enriched()}
        row["company_linkedin_url"] = company_linkedin_url
        row["company_name"] = company_name or domain
        row["company_industry"] = company_industry
        row["company_employee_count"] = company_employee_count
        row["dm_first_name"] = person.get("first_name", "")
        row["dm_last_name"] = person.get("last_name", "")
        row["dm_full_name"] = person.get("full_name", "")
        row["dm_title"] = _current_title(person.get("experiences", []))
        row["dm_linkedin_url"] = person.get("linkedin_url", "")
        row["dm_email"] = email
        row["dm_email_source"] = source
        row["dm_email_verified"] = verified
        row["dm_phone"] = phone
        row["dm_headline"] = person.get("headline", "")
        loc = person.get("location", {})
        row["dm_location_city"] = loc.get("city", "")
        row["dm_location_country"] = loc.get("country_code", "")
        row["dm_icp_tier"] = str(icp_tier)
        row["row_status"] = STATUS_ENRICHED if email else STATUS_NO_CONTACTS

        output_rows.append(row)

    return output_rows


async def run_domain_enrichment(
    rows: list[dict[str, Any]],
    domain_col: str,
    name_col: Optional[str] = None,
    first_name_col: Optional[str] = None,
    last_name_col: Optional[str] = None,
    max_decision_makers: int = 5,
    include_generic_emails: bool = True,
    on_progress: Callable[[dict[str, Any]], None] = None,
    force_provider: Optional[str] = None,
) -> list[OutputRow]:
    """
    Main entry point for Flow 1: Domain → Generic Emails + Decision Makers

    Args:
        rows: List of input rows from CSV
        domain_col: Column name containing domain
        name_col: Optional column with full name
        first_name_col: Optional column with first name
        last_name_col: Optional column with last name
        max_decision_makers: Max decision makers per company (default 5)
        include_generic_emails: Whether to include generic emails
        on_progress: Callback for progress updates
        force_provider: If set, only use that specific provider.

    Returns:
        List of enriched output rows
    """
    domain_semaphore = asyncio.Semaphore(DOMAIN_CONCURRENCY)
    email_semaphore = asyncio.Semaphore(LINKEDIN_CONCURRENCY)

    blitz_http = httpx.AsyncClient()
    contacts_http = httpx.AsyncClient()

    all_output: list[OutputRow] = []
    total = len(rows)

    async def process_row(idx: int, row: dict[str, Any]) -> list[OutputRow]:
        domain = str(row.get(domain_col, "")).strip()

        if not domain:
            result = [{**row, **_empty_enriched(), "row_status": STATUS_SKIPPED}]

        else:
            result = await _enrich_single_domain(
                blitz_http,
                contacts_http,
                row,
                domain,
                max_decision_makers,
                include_generic_emails,
                domain_semaphore,
                email_semaphore,
                force_provider=force_provider,
            )

        if on_progress:
            # Collect source counts from results
            source_counts: dict[str, int] = {}
            for r in result:
                source = r.get("dm_email_source", "")
                if source:
                    provider = _normalize_source(source)
                    source_counts[provider] = source_counts.get(provider, 0) + 1
            emails_found = sum(1 for r in result if r.get("dm_email"))
            on_progress({
                "index": idx,
                "total": total,
                "domain": domain,
                "status": result[0].get("row_status", STATUS_ERROR),
                "contacts_found": len(result),
                "emails_found": emails_found,
                "source_counts": source_counts,
            })

        return result

    # Process all rows
    tasks = [process_row(i, row) for i, row in enumerate(rows)]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    await blitz_http.aclose()
    await contacts_http.aclose()

    # Collect results
    for result in results:
        if isinstance(result, Exception):
            logger.error("Row processing failed: %s", result)
        else:
            all_output.extend(result)

    return all_output


# =============================================================================
# Flow 2: Search Criteria → Companies → Enrich
# =============================================================================

async def search_companies(
    blitz_http: httpx.AsyncClient,
    *,
    name: Optional[str] = None,
    industry: Optional[list[str]] = None,
    employee_range: Optional[list[str]] = None,
    company_type: Optional[list[str]] = None,
    country_code: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    """
    Search for companies using Blitz Company Search.

    Returns: {count, total, results: [...]}
    """
    return await blitz_client.company_search(
        blitz_http,
        name=name,
        industry=industry,
        employee_range=employee_range,
        company_type=company_type,
        country_code=country_code,
        limit=limit,
        offset=offset,
    )


async def search_employees(
    blitz_http: httpx.AsyncClient,
    *,
    company_linkedin_url: Optional[str] = None,
    company_name: Optional[str] = None,
    domain: Optional[str] = None,
    job_levels: Optional[list[str]] = None,
    job_functions: Optional[list[str]] = None,
    keywords: Optional[str] = None,
    country_code: Optional[str] = None,
    limit: int = 100,
) -> dict[str, Any]:
    """
    Search for employees using Blitz Employee Finder.

    Returns: {count, results: [...]}
    """
    return await blitz_client.employee_finder(
        blitz_http,
        company_linkedin_url=company_linkedin_url,
        company_name=company_name,
        domain=domain,
        job_levels=job_levels,
        job_functions=job_functions,
        keywords=keywords,
        country_code=country_code,
        limit=limit,
    )


async def enrich_companies_from_search(
    company_results: list[dict[str, Any]],
    blitz_http: httpx.AsyncClient,
    contacts_http: httpx.AsyncClient,
    max_decision_makers: int = 5,
    on_progress: Callable[[dict[str, Any]], None] = None,
    force_provider: Optional[str] = None,
) -> list[OutputRow]:
    """
    Enrich a list of company search results with decision makers.

    Args:
        company_results: List of company dicts from company search
        blitz_http: Blitz API client
        contacts_http: Contacts DB client
        max_decision_makers: Max DMs per company
        on_progress: Progress callback
        force_provider: If set, only use that specific provider.

    Returns:
        List of enriched output rows
    """
    domain_semaphore = asyncio.Semaphore(DOMAIN_CONCURRENCY)
    output_rows = []

    async def process_company(idx: int, company: dict[str, Any]):
        domain = company.get("domain", "")
        if not domain:
            # Try to extract domain from LinkedIn URL or name
            linkedin_url = company.get("linkedin_url", "")
            # Domain extraction logic would go here
            return []

        base_row = {
            "company_name": company.get("name", ""),
            "company_linkedin_url": company.get("linkedin_url", ""),
            "company_industry": company.get("industry", ""),
            "company_employee_count": str(company.get("employee_count", "")),
        }

        result = await _enrich_single_domain(
            blitz_http,
            contacts_http,
            base_row,
            domain,
            max_decision_makers,
            True,
            domain_semaphore,
            force_provider=force_provider,
        )

        if on_progress:
            # Collect source counts from results (all from Blitz company search)
            source_counts: dict[str, int] = {}
            for r in result:
                source = r.get("dm_email_source", "")
                if source:
                    provider = _normalize_source(source)
                    source_counts[provider] = source_counts.get(provider, 0) + 1
            emails_found = sum(1 for r in result if r.get("dm_email"))
            on_progress({
                "index": idx,
                "total": len(company_results),
                "domain": domain,
                "status": result[0].get("row_status", STATUS_ERROR),
                "contacts_found": len(result),
                "emails_found": emails_found,
                "source_counts": source_counts,
            })

        return result

    tasks = [process_company(i, c) for i, c in enumerate(company_results)]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for result in results:
        if not isinstance(result, Exception):
            output_rows.extend(result)

    return output_rows


# =============================================================================
# Flow 3: LinkedIn URLs → Full Enrichment
# =============================================================================

async def _enrich_single_linkedin(
    blitz_http: httpx.AsyncClient,
    contacts_http: httpx.AsyncClient,
    base_row: dict[str, Any],
    linkedin_url: str,
    include_company: bool = True,
    semaphore: asyncio.Semaphore = None,
) -> OutputRow:
    """
    Enrich a single LinkedIn URL with person + company details.

    Returns single output row.
    """
    if not semaphore:
        semaphore = asyncio.Semaphore(LINKEDIN_CONCURRENCY)

    row = {**base_row, **_empty_enriched()}

    async with semaphore:
        # Step 1: Try Contacts DB first (FREE)
        contacts_data = None
        try:
            contacts_data = await contacts_client.person_by_linkedin(contacts_http, linkedin_url)
        except Exception as e:
            logger.debug("Contacts DB lookup failed: %s", e)

        if contacts_data:
            # Use Contacts DB data
            row["dm_full_name"] = contacts_data.get("full_name", "")
            row["dm_first_name"] = contacts_data.get("first_name", "")
            row["dm_last_name"] = contacts_data.get("last_name", "")
            row["dm_title"] = contacts_data.get("title", "")
            row["dm_headline"] = contacts_data.get("headline", "")
            row["dm_linkedin_url"] = linkedin_url
            row["dm_email"] = contacts_client.extract_email_from_contacts_response(contacts_data) or ""
            row["dm_phone"] = contacts_data.get("phone", "")
            row["dm_email_source"] = SOURCE_CONTACTS_DB_LINKEDIN
            row["dm_email_verified"] = "no"  # Contacts DB emails are not verified

            if include_company:
                row["company_name"] = contacts_data.get("company_name", "")
                row["company_linkedin_url"] = contacts_data.get("company_linkedin_url", "")

            row["row_status"] = STATUS_ENRICHED if row["dm_email"] else STATUS_NO_CONTACTS
            return row

        # Step 2: Fallback to Blitz API (PAID)
        try:
            result = await blitz_client.person_enrich_by_linkedin(blitz_http, linkedin_url, include_phone=True)
            if result.get("found") and result.get("person"):
                person = result.get("person", {})
                row["dm_full_name"] = person.get("full_name", "")
                row["dm_first_name"] = person.get("first_name", "")
                row["dm_last_name"] = person.get("last_name", "")
                row["dm_title"] = person.get("title", "")
                row["dm_headline"] = person.get("headline", "")
                row["dm_linkedin_url"] = linkedin_url
                # Check for verified_email - if present, email is verified
                verified_email = person.get("verified_email", "")
                if verified_email:
                    row["dm_email"] = verified_email
                    row["dm_email_verified"] = "yes"
                else:
                    row["dm_email"] = person.get("emails", [{}])[0].get("email", "") if person.get("emails") else ""
                    row["dm_email_verified"] = "no"
                row["dm_phone"] = person.get("phone", "")
                row["dm_email_source"] = SOURCE_BLITZ_LINKEDIN

                if include_company and person.get("company"):
                    company = person.get("company", {})
                    row["company_name"] = company.get("name", "")
                    row["company_linkedin_url"] = company.get("linkedin_url", "")

                row["row_status"] = STATUS_ENRICHED if row["dm_email"] else STATUS_NO_CONTACTS
                return row
        except Exception as e:
            logger.debug("Blitz person enrich failed: %s", e)

    # Not found anywhere
    row["dm_linkedin_url"] = linkedin_url
    row["row_status"] = STATUS_NOT_FOUND
    return row


async def run_linkedin_enrichment(
    rows: list[dict[str, Any]],
    linkedin_col: str,
    on_progress: Callable[[dict[str, Any]], None] = None,
) -> list[OutputRow]:
    """
    Main entry point for Flow 3: LinkedIn URLs → Full Enrichment

    Args:
        rows: List of input rows from CSV
        linkedin_col: Column name containing LinkedIn URL
        on_progress: Callback for progress updates

    Returns:
        List of enriched output rows
    """
    semaphore = asyncio.Semaphore(LINKEDIN_CONCURRENCY)

    blitz_http = httpx.AsyncClient()
    contacts_http = httpx.AsyncClient()

    all_output: list[OutputRow] = []
    total = len(rows)

    async def process_row(idx: int, row: dict[str, Any]) -> OutputRow:
        linkedin_url = str(row.get(linkedin_col, "")).strip()

        if not linkedin_url or "linkedin.com" not in linkedin_url:
            result = {**row, **_empty_enriched(), "row_status": STATUS_SKIPPED}
        else:
            result = await _enrich_single_linkedin(
                blitz_http,
                contacts_http,
                row,
                linkedin_url,
                True,
                semaphore,
            )

        if on_progress:
            # Collect source counts from result
            source_counts: dict[str, int] = {}
            source = result.get("dm_email_source", "")
            if source:
                provider = _normalize_source(source)
                source_counts[provider] = 1
            on_progress({
                "index": idx,
                "total": total,
                "linkedin_url": linkedin_url,
                "status": result.get("row_status", STATUS_ERROR),
                "email_found": bool(result.get("dm_email")),
                "source_counts": source_counts,
            })

        return result

    # Process all rows
    tasks = [process_row(i, row) for i, row in enumerate(rows)]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    await blitz_http.aclose()
    await contacts_http.aclose()

    # Collect results
    for result in results:
        if isinstance(result, Exception):
            logger.error("Row processing failed: %s", result)
        else:
            all_output.append(result)

    return all_output
