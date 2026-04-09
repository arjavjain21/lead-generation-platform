"""
Enrichment pipeline orchestrator.

Per-domain workflow:
  1. Contacts DB: domain → company LinkedIn URL (primary, free)
  2. Blitz fallback: domain → company LinkedIn URL (if Contacts DB fails)
  3. Contacts DB: company → decision makers with emails (primary, free)
  4. Blitz fallback: decision makers via waterfall ICP (if Contacts DB quality insufficient)
  5. For each person:
       a. Blitz: person LinkedIn URL → work email
       b. Fallback: Contacts DB by LinkedIn URL
       c. Fallback: Contacts DB by name + domain
  6. If no decision makers found: BetterEnrich → generic company email (fallback)
  7. If no email from above: Prospeo → person/company enrichment (final fallback)
  8. If domain_to_linkedin fails AND input has name columns:
       Contacts DB by name + domain (directly)

Supports incremental CSV writes for partial downloads.
"""

from __future__ import annotations

import asyncio
import csv
import logging
from pathlib import Path
from typing import Any, Callable, Optional

import httpx

from . import blitz_client
from . import contacts_client
from . import better_enrich_client
from . import prospeo_client

logger = logging.getLogger(__name__)


# Valid provider values for force_provider parameter
VALID_PROVIDERS = frozenset({"contacts_db", "blitz", "better_enrich", "prospeo"})


def _should_skip_provider(provider: str, force_provider: Optional[str]) -> bool:
    """
    Determine if a provider should be skipped based on force_provider setting.

    Args:
        provider: The current provider being considered (e.g., "contacts_db", "blitz")
        force_provider: The forced provider from request (or None for normal cascade)

    Returns:
        True if the provider should be skipped, False otherwise

    When force_provider is None, all providers are used (normal cascade).
    When force_provider is set, only that provider is used.
    """
    if not force_provider:
        return False  # No force, use all providers
    result = provider != force_provider
    logger.debug("_should_skip_provider(provider=%s, force_provider=%s) = %s", provider, force_provider, result)
    return result

# Max concurrent Blitz calls to avoid hammering the API
DOMAIN_CONCURRENCY = 5
EMAIL_CONCURRENCY = 10


# ---------------------------------------------------------------------------
# Data shapes
# ---------------------------------------------------------------------------

OutputRow = dict[str, Any]

# Status values for row_status column
STATUS_ENRICHED = "enriched"
STATUS_NO_LINKEDIN = "no_linkedin"
STATUS_NO_CONTACTS = "no_contacts"
STATUS_ERROR = "error"

# Email source values
SOURCE_BLITZ = "blitz_email"
SOURCE_CONTACTS_LINKEDIN = "contacts_db_linkedin"
SOURCE_CONTACTS_NAME = "contacts_db_name"
SOURCE_NOT_FOUND = "not_found"

# Additional source tracking for Contacts DB-first approach
SOURCE_CONTACTS_DB_LINKEDIN = "contacts_db_linkedin"      # Company LinkedIn from Contacts DB
SOURCE_CONTACTS_DB_CONTACTS = "contacts_db_contacts"      # Decision makers from Contacts DB
SOURCE_CONTACTS_DB_EMAIL = "contacts_db_email"            # Email from Contacts DB
SOURCE_BLITZ_LINKEDIN = "blitz_linkedin"                  # Company LinkedIn from Blitz
SOURCE_BLITZ_CONTACTS = "blitz_contacts"                  # Decision makers from Blitz
SOURCE_BLITZ_EMAIL = "blitz_email"                        # Email from Blitz
SOURCE_BETTER_ENRICH_COMPANY = "better_enrich_company"    # Generic company email from BetterEnrich
SOURCE_BETTER_ENRICH_PERSON = "better_enrich_person"      # Person email from BetterEnrich
SOURCE_PROSPEO = "prospeo"                               # Email/data from Prospeo
SOURCE_PROSPEO_PERSON = "prospeo_person"                  # Person email from Prospeo

ENRICHED_COLUMNS = [
    "company_linkedin_url",
    "dm_first_name",
    "dm_last_name",
    "dm_full_name",
    "dm_title",
    "dm_linkedin_url",
    "dm_email",
    "dm_email_source",
    "dm_headline",
    "dm_location_city",
    "dm_location_country",
    "dm_icp_tier",
    "row_status",
]


def _empty_enriched() -> dict[str, Any]:
    return {col: "" for col in ENRICHED_COLUMNS}


def _current_title(experiences: list[dict]) -> str:
    for exp in experiences or []:
        if exp.get("job_is_current"):
            return exp.get("job_title", "")
    if experiences:
        return experiences[0].get("job_title", "")
    return ""


def _build_person_row(
    base_row: dict[str, Any],
    company_linkedin_url: str,
    person: dict[str, Any],
    icp_tier: int,
    email: str,
    email_source: str,
) -> OutputRow:
    loc = person.get("location") or {}
    experiences = person.get("experiences") or []
    row = {**base_row, **_empty_enriched()}
    row["company_linkedin_url"] = company_linkedin_url
    row["dm_first_name"] = person.get("first_name", "")
    row["dm_last_name"] = person.get("last_name", "")
    row["dm_full_name"] = person.get("full_name", "")
    row["dm_title"] = _current_title(experiences)
    row["dm_linkedin_url"] = person.get("linkedin_url", "")
    row["dm_email"] = email
    row["dm_email_source"] = email_source
    row["dm_headline"] = person.get("headline", "")
    row["dm_location_city"] = loc.get("city", "")
    row["dm_location_country"] = loc.get("country_code", "")
    row["dm_icp_tier"] = icp_tier
    row["row_status"] = STATUS_ENRICHED if email else STATUS_NO_CONTACTS
    return row


def _no_linkedin_row(base_row: dict[str, Any]) -> OutputRow:
    row = {**base_row, **_empty_enriched()}
    row["row_status"] = STATUS_NO_LINKEDIN
    return row


def _no_contacts_row(base_row: dict[str, Any], company_linkedin_url: str = "") -> OutputRow:
    row = {**base_row, **_empty_enriched()}
    row["company_linkedin_url"] = company_linkedin_url
    row["row_status"] = STATUS_NO_CONTACTS
    return row


def _error_row(base_row: dict[str, Any], company_linkedin_url: str = "") -> OutputRow:
    row = {**base_row, **_empty_enriched()}
    row["company_linkedin_url"] = company_linkedin_url
    row["row_status"] = STATUS_ERROR
    return row


def _company_email_row(
    base_row: dict[str, Any],
    company_linkedin_url: str,
    email: str,
    email_source: str,
) -> OutputRow:
    """Create a row with generic company email from BetterEnrich."""
    row = {**base_row, **_empty_enriched()}
    row["company_linkedin_url"] = company_linkedin_url
    row["dm_email"] = email
    row["dm_email_source"] = email_source
    row["row_status"] = STATUS_ENRICHED
    return row


def _prospeo_company_row(
    base_row: dict[str, Any],
    company_linkedin_url: str,
    company_data: dict[str, Any],
    source: str,
) -> OutputRow:
    """Create a row with company data from Prospeo."""
    row = {**base_row, **_empty_enriched()}
    row["company_linkedin_url"] = company_data.get("linkedin_url", company_linkedin_url) or company_linkedin_url
    row["row_status"] = STATUS_ENRICHED
    row["dm_email_source"] = source
    # Store company data in a format that can be synced later
    row["_prospeo_company"] = company_data
    return row


# ---------------------------------------------------------------------------
# Per-person email resolution
# ---------------------------------------------------------------------------

async def _resolve_email_for_person(
    blitz_client_inst: httpx.AsyncClient,
    contacts_client_inst: httpx.AsyncClient,
    person: dict[str, Any],
    domain: str,
    input_full_name: str,
    email_semaphore: asyncio.Semaphore,
    force_provider: Optional[str] = None,
) -> tuple[str, str]:
    """
    Returns (email, source).
    Priority cascade:
      1. Contacts DB by person's name + domain (PRIMARY - avoids stale LinkedIn emails)
      2. Contacts DB by LinkedIn URL (SECONDARY)
      3. Blitz person enrich by name + domain (PRIMARY PAID)
      4. Blitz email from LinkedIn URL (SECONDARY PAID)
      5. BetterEnrich work email (person lookup)
      6. Prospeo person enrichment
      7. Contacts DB by name + domain (name from input row, if different)

    Args:
        force_provider: If set, only use that specific provider.
    """
    linkedin_url = person.get("linkedin_url", "")
    full_name = person.get("full_name", "")
    first_name = person.get("first_name", "")
    last_name = person.get("last_name", "")

    async with email_semaphore:
        # Step 1: Contacts DB by person's name + domain (PRIMARY - FREE)
        # When both name and LinkedIn URL are available, prefer name+domain to avoid
        # returning stale emails from previous employers via person_by_linkedin
        if full_name and domain and not _should_skip_provider("contacts_db", force_provider):
            try:
                contacts_data = await contacts_client.person_by_name_and_domain(
                    contacts_client_inst, full_name, domain
                )
                email = contacts_client.extract_email_from_contacts_response(contacts_data)
                if email:
                    return email, SOURCE_CONTACTS_DB_EMAIL
            except Exception as e:
                logger.warning("Contacts DB name+domain lookup failed for %s / %s: %s", full_name, domain, e)

        # Step 2: Contacts DB by LinkedIn URL (SECONDARY - FREE)
        # Fall back to LinkedIn if name+domain didn't find email, or if name not available
        if linkedin_url and not _should_skip_provider("contacts_db", force_provider):
            try:
                contacts_data = await contacts_client.person_by_linkedin(
                    contacts_client_inst, linkedin_url
                )
                email = contacts_client.extract_email_from_contacts_response(contacts_data)
                if email:
                    return email, SOURCE_CONTACTS_DB_EMAIL
            except Exception as e:
                logger.warning("Contacts DB LinkedIn lookup failed for %s: %s", linkedin_url, e)

        # Step 3: Blitz person enrich by name + domain (PRIMARY PAID)
        # Prioritize name+domain for the same reason as Contacts DB
        if full_name and domain and not _should_skip_provider("blitz", force_provider):
            try:
                result = await blitz_client.person_enrich(
                    blitz_client_inst,
                    full_name=full_name,
                    domain=domain,
                    include_phone=False,
                )
                if result.get("found") and result.get("person"):
                    person_data = result.get("person", {})
                    # Check for verified_email field first
                    verified_email = person_data.get("verified_email", "")
                    if verified_email:
                        return verified_email, SOURCE_BLITZ_EMAIL
                    # Fall back to unverified emails list
                    emails_list = person_data.get("emails", [])
                    if emails_list:
                        return emails_list[0].get("email", ""), SOURCE_BLITZ_EMAIL
            except Exception as e:
                logger.warning("Blitz person enrich failed for %s / %s: %s", full_name, domain, e)

        # Step 4: Blitz email from LinkedIn URL (SECONDARY PAID)
        # Fall back to LinkedIn if name+domain didn't find email, or if name not available
        if linkedin_url and not _should_skip_provider("blitz", force_provider):
            try:
                result = await blitz_client.find_work_email(blitz_client_inst, linkedin_url)
                if result.get("found") and result.get("email"):
                    return result["email"], SOURCE_BLITZ_EMAIL
            except Exception as e:
                logger.warning("Blitz email lookup failed for %s: %s", linkedin_url, e)

        # Step 5: BetterEnrich person email
        if full_name and domain and not _should_skip_provider("better_enrich", force_provider):
            try:
                result = await better_enrich_client.find_work_email_v2(
                    blitz_client_inst, full_name, domain
                )
                if result and result.get("data", {}).get("email"):
                    email = result["data"]["email"]
                    return email, SOURCE_BETTER_ENRICH_PERSON
            except Exception as e:
                logger.warning("BetterEnrich person lookup failed for %s / %s: %s", full_name, domain, e)

        # Step 6: Prospeo person enrichment
        if not _should_skip_provider("prospeo", force_provider):
            try:
                result = await prospeo_client.enrich_person(
                    blitz_client_inst,
                    linkedin_url=linkedin_url,
                    full_name=full_name,
                    first_name=first_name,
                    last_name=last_name,
                    company_website=domain,
                )
                # Use helper to extract email from Prospeo result
                email = prospeo_client.extract_email_from_prospeo(result)
                if email:
                    return email, SOURCE_PROSPEO_PERSON
            except Exception as e:
                logger.warning("Prospeo person enrichment failed for %s / %s: %s", full_name, domain, e)

        # Step 7: Contacts DB by input row name + domain (if different from person name)
        # This handles edge cases where the input name differs from the person's current name
        if input_full_name and input_full_name != full_name and domain and not _should_skip_provider("contacts_db", force_provider):
            try:
                contacts_data = await contacts_client.person_by_name_and_domain(
                    contacts_client_inst, input_full_name, domain
                )
                email = contacts_client.extract_email_from_contacts_response(contacts_data)
                if email:
                    return email, SOURCE_CONTACTS_DB_EMAIL
            except Exception as e:
                logger.warning("Contacts DB input name lookup failed: %s", e)

        return "", SOURCE_NOT_FOUND


# ---------------------------------------------------------------------------
# Per-domain enrichment
# ---------------------------------------------------------------------------

async def _enrich_domain(
    blitz_http: httpx.AsyncClient,
    contacts_http: httpx.AsyncClient,
    base_row: dict[str, Any],
    domain: str,
    full_name: str,
    cascade: list[dict[str, Any]],
    max_results: int,
    domain_semaphore: asyncio.Semaphore,
    email_semaphore: asyncio.Semaphore,
    skip_contacts_db: bool = False,
    force_provider: Optional[str] = None,  # "contacts_db", "blitz", "better_enrich", "prospeo"
) -> list[OutputRow]:
    """
    Enrich a domain with decision-maker contacts.

    Args:
        force_provider: If set, only use that specific provider.
    """
    logger.info("_enrich_domain called with force_provider=%s for domain=%s", force_provider, domain)
    company_linkedin_url = ""
    linkedin_source = ""

    # Determine if we should skip Contacts DB for contacts (not for company lookup)
    # Skip Contacts DB if custom cascade is provided (indicated by skip_contacts_db=True)
    # or if cascade is not the default
    use_custom_cascade = skip_contacts_db or cascade != blitz_client.DEFAULT_CASCADE

    async with domain_semaphore:
        # Step 1: domain → company LinkedIn URL (Contacts DB FIRST)
        try:
            contacts_company = await contacts_client.company_by_domain(contacts_http, domain)
            if contacts_company and contacts_company.get("linkedin_url"):
                company_linkedin_url = contacts_company.get("linkedin_url", "")
                linkedin_source = SOURCE_CONTACTS_DB_LINKEDIN
                logger.debug("Found company LinkedIn via Contacts DB for %s: %s", domain, company_linkedin_url)
        except Exception as e:
            logger.warning("Contacts DB company lookup failed for %s: %s", domain, e)

        # Fallback: Blitz API if Contacts DB didn't find it
        if not company_linkedin_url:
            try:
                d2l = await blitz_client.domain_to_linkedin(blitz_http, domain)
                if d2l.get("found"):
                    company_linkedin_url = d2l.get("company_linkedin_url", "")
                    linkedin_source = SOURCE_BLITZ_LINKEDIN
                    logger.debug("Found company LinkedIn via Blitz API for %s: %s", domain, company_linkedin_url)
            except Exception as e:
                logger.warning("Blitz domain_to_linkedin failed for %s: %s", domain, e)

    if not company_linkedin_url:
        # No company LinkedIn found — try fallback if we have a name
        if full_name and not _should_skip_provider("contacts_db", force_provider):
            try:
                contacts_data = await contacts_client.person_by_name_and_domain(
                    contacts_http, full_name, domain
                )
                email = contacts_client.extract_email_from_contacts_response(contacts_data)
                if contacts_data:
                    person_stub: dict[str, Any] = {
                        "first_name": contacts_data.get("first_name", ""),
                        "last_name": contacts_data.get("last_name", ""),
                        "full_name": contacts_data.get("full_name", full_name),
                        "headline": contacts_data.get("headline", ""),
                        "linkedin_url": contacts_data.get("linkedin_url", ""),
                        "location": {
                            "city": contacts_data.get("city", ""),
                            "country_code": contacts_data.get("country_code", ""),
                        },
                        "experiences": [],
                    }
                    return [
                        _build_person_row(
                            base_row, "", person_stub, 0, email or "", SOURCE_CONTACTS_NAME
                        )
                    ]
            except Exception as e:
                logger.warning("Contacts DB fallback for no-linkedin failed: %s", e)
        return [_no_linkedin_row(base_row)]

    # Step 2: Get decision makers (Contacts DB FIRST, unless custom cascade or force_provider provided)
    persons: list[dict[str, Any]] = []
    contacts_db_quality_met = False

    skip_contacts = _should_skip_provider("contacts_db", force_provider)
    logger.info("Step 2: force_provider=%s, use_custom_cascade=%s, skip_contacts=%s",
                force_provider, use_custom_cascade, skip_contacts)

    async with domain_semaphore:
        # Skip Contacts DB contacts lookup if custom cascade is provided OR if force_provider is set
        if not use_custom_cascade and not skip_contacts:
            logger.info("Using Contacts DB for decision makers")
            # Try Contacts DB first
            try:
                contacts_contacts = await contacts_client.company_contacts_enriched(
                    contacts_http, domain, limit=max_results
                )
                if contacts_contacts and len(contacts_contacts) > 0:
                    # Quality check: need at least 1 decision maker AND 1 email
                    emails_count = sum(1 for c in contacts_contacts if c.get("email"))
                    if len(contacts_contacts) >= 1 and emails_count >= 1:
                        # Convert Contacts DB format to Blitz format for compatibility
                        persons = []
                        for contact in contacts_contacts[:max_results]:
                            person_dict = {
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
                                "icp": 0,  # Contacts DB doesn't provide ICP scoring
                            }
                            persons.append(person_dict)

                        contacts_db_quality_met = True
                        logger.debug("Using Contacts DB for %d decision makers from %s", len(persons), domain)
            except Exception as e:
                logger.warning("Contacts DB contacts lookup failed for %s: %s", domain, e)

        # Fallback: Blitz API if Contacts DB didn't meet quality threshold
        if not contacts_db_quality_met and not _should_skip_provider("blitz", force_provider):
            try:
                icp_result = await blitz_client.waterfall_icp_search(
                    blitz_http, company_linkedin_url, cascade, max_results
                )
                persons = icp_result.get("results", [])
                logger.debug("Using Blitz API for %d decision makers from %s", len(persons), domain)
            except Exception as e:
                logger.warning("Blitz waterfall_icp_search failed for %s: %s", company_linkedin_url, e)
                return [_error_row(base_row, company_linkedin_url)]

    if not persons:
        # No decision makers found — try BetterEnrich for generic company email
        if not _should_skip_provider("better_enrich", force_provider):
            try:
                be_result = await better_enrich_client.find_company_email(
                    blitz_http,
                    website=domain,
                )
                if be_result and be_result.get("email"):
                    logger.info("BetterEnrich company email found for %s: %s", domain, be_result.get("email"))
                    return [_company_email_row(
                        base_row,
                        company_linkedin_url,
                        be_result.get("email", ""),
                        SOURCE_BETTER_ENRICH_COMPANY,
                    )]
            except Exception as e:
                logger.debug("BetterEnrich company email lookup failed for %s: %s", domain, e)

        # Final fallback: try Prospeo for company enrichment
        if not _should_skip_provider("prospeo", force_provider):
            try:
                prospeo_result = await prospeo_client.enrich_company(
                    blitz_http,
                    company_website=domain,
                )
                if prospeo_result:
                    company_data = prospeo_result.get("company", {})
                    if company_data:
                        logger.info("Prospeo found company data for %s", domain)
                        return [_prospeo_company_row(
                            base_row,
                            company_linkedin_url,
                            company_data,
                            SOURCE_PROSPEO,
                        )]
            except Exception as e:
                logger.debug("Prospeo company lookup failed for %s: %s", domain, e)

        return [_no_contacts_row(base_row, company_linkedin_url)]

    # Step 3: resolve email for each person concurrently
    tasks = [
        _resolve_email_for_person(
            blitz_http,
            contacts_http,
            item.get("person", {}),
            domain,
            full_name,
            email_semaphore,
            force_provider=force_provider,
        )
        for item in persons
    ]
    email_results = await asyncio.gather(*tasks)

    output_rows: list[OutputRow] = []
    for item, (email, source) in zip(persons, email_results):
        person = item.get("person", {})
        icp_tier = item.get("icp", 0)
        output_rows.append(
            _build_person_row(base_row, company_linkedin_url, person, icp_tier, email, source)
        )

    return output_rows


# ---------------------------------------------------------------------------
# Main pipeline entry point (called by FastAPI background task)
# ---------------------------------------------------------------------------

async def run_pipeline(
    rows: list[dict[str, Any]],
    domain_col: str,
    name_col: Optional[str],
    first_name_col: Optional[str],
    last_name_col: Optional[str],
    cascade: list[dict[str, Any]],
    max_results: int,
    on_progress: Callable[[dict[str, Any]], None],
    write_incremental: bool = False,
    output_path: Optional[Path] = None,
    cancelled_jobs: Optional[set[str]] = None,
    job_id: Optional[str] = None,
) -> list[OutputRow]:
    """
    Runs the full pipeline over all rows.
    Calls on_progress(event_dict) after each domain is processed.
    Returns the list of all output rows.

    If write_incremental=True, writes results to CSV as they are processed
    for partial download support.

    If cancelled_jobs is provided, checks if job_id is in the set and stops processing.
    """
    domain_semaphore = asyncio.Semaphore(DOMAIN_CONCURRENCY)
    email_semaphore = asyncio.Semaphore(EMAIL_CONCURRENCY)

    # Connection pooling with limits to prevent resource exhaustion
    # Keepalive connections are reused, max_connections limits total open connections
    http_limits = httpx.Limits(
        max_keepalive_connections=20,
        max_connections=100,
        keepalive_expiry=30.0,
    )
    blitz_http = httpx.AsyncClient(
        limits=http_limits,
        timeout=httpx.Timeout(30.0, connect=10.0),
    )
    contacts_http = httpx.AsyncClient(
        limits=http_limits,
        timeout=httpx.Timeout(30.0, connect=10.0),
    )

    all_output: list[OutputRow] = []
    total = len(rows)

    # Set up incremental CSV writing if enabled
    write_lock = asyncio.Lock()
    csv_file = None
    csv_writer = None

    if write_incremental and output_path:
        # Get all columns from first row + enriched columns
        all_columns = list(rows[0].keys()) + ENRICHED_COLUMNS
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Open file and write header
        csv_file = open(output_path, "w", newline="", encoding="utf-8")
        csv_writer = csv.DictWriter(csv_file, fieldnames=all_columns, extrasaction="ignore")
        csv_writer.writeheader()
        csv_file.flush()

    async def process_row(idx: int, row: dict[str, Any]) -> list[OutputRow]:
        # Check if job was cancelled
        if cancelled_jobs and job_id and job_id in cancelled_jobs:
            logger.info("Job %s cancelled, stopping processing at row %d", job_id, idx)
            raise RuntimeError(f"Job {job_id} was cancelled")

        domain = str(row.get(domain_col, "")).strip()

        # Resolve full name from available columns
        if name_col and row.get(name_col):
            full_name = str(row[name_col]).strip()
        elif first_name_col or last_name_col:
            first = str(row.get(first_name_col or "", "")).strip()
            last = str(row.get(last_name_col or "", "")).strip()
            full_name = f"{first} {last}".strip()
        else:
            full_name = ""

        if not domain:
            result_rows = [_error_row(row)]
            result_rows[0]["row_status"] = "skipped_no_domain"
        else:
            result_rows = await _enrich_domain(
                blitz_http,
                contacts_http,
                row,
                domain,
                full_name,
                cascade,
                max_results,
                domain_semaphore,
                email_semaphore,
            )

        emails_found = sum(1 for r in result_rows if r.get("dm_email"))
        await on_progress(
            {
                "index": idx,
                "total": total,
                "domain": domain,
                "status": result_rows[0].get("row_status", STATUS_ERROR),
                "contacts_found": len(result_rows),
                "emails_found": emails_found,
            }
        )

        # Write to CSV incrementally if enabled
        if write_incremental and csv_writer:
            async with write_lock:
                for r in result_rows:
                    csv_writer.writerow(r)
                csv_file.flush()

        return result_rows

    # Run all domains concurrently (semaphores handle actual throttling)
    # Use return_exceptions=True to prevent one bad row from failing entire job
    tasks = [process_row(i, row) for i, row in enumerate(rows)]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    await blitz_http.aclose()
    await contacts_http.aclose()

    # Close CSV file if open
    if csv_file:
        csv_file.close()

    # Process results, handling exceptions from failed rows
    exception_count = 0
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            # Log the error and create an error row for this input row
            logger.error("Row %d failed: %s", i, result)
            exception_count += 1
            # Create an error row with the original input data
            error_row = {**rows[i], **_empty_enriched()}
            error_row["row_status"] = STATUS_ERROR
            all_output.append(error_row)
        else:
            # Normal case: result is a list of OutputRow objects
            all_output.extend(result)

    if exception_count > 0:
        logger.warning(
            "Pipeline completed with %d row errors out of %d total rows (%.1f%% success rate)",
            exception_count, len(rows), (len(rows) - exception_count) / len(rows) * 100
        )

    return all_output
