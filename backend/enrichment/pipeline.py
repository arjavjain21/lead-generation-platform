"""
Enrichment pipeline orchestrator.

Per-domain workflow:
  1. Blitz: domain → company LinkedIn URL
  2. Blitz: company LinkedIn URL → waterfall ICP (up to 5 decision makers)
  3. For each person:
       a. Blitz: person LinkedIn URL → work email
       b. Fallback: Contacts DB by LinkedIn URL
       c. Fallback: Contacts DB by name + domain
  4. If domain_to_linkedin fails AND input has name columns:
       d. Contacts DB by name + domain (directly)

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

logger = logging.getLogger(__name__)

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
) -> tuple[str, str]:
    """
    Returns (email, source).
    Priority (Contacts DB FIRST):
      1. Contacts DB by LinkedIn URL
      2. Contacts DB by person's name + domain
      3. Blitz work email from LinkedIn URL
      4. Contacts DB by name + domain (name from input row, if available)
    """
    linkedin_url = person.get("linkedin_url", "")
    full_name = person.get("full_name", "")

    async with email_semaphore:
        # Step 1: Contacts DB by LinkedIn URL (PRIMARY)
        if linkedin_url:
            try:
                contacts_data = await contacts_client.person_by_linkedin(
                    contacts_client_inst, linkedin_url
                )
                email = contacts_client.extract_email_from_contacts_response(contacts_data)
                if email:
                    return email, SOURCE_CONTACTS_DB_EMAIL
            except Exception as e:
                logger.warning("Contacts DB LinkedIn lookup failed for %s: %s", linkedin_url, e)

        # Step 2: Contacts DB by person's name + domain
        if full_name and domain:
            try:
                contacts_data = await contacts_client.person_by_name_and_domain(
                    contacts_client_inst, full_name, domain
                )
                email = contacts_client.extract_email_from_contacts_response(contacts_data)
                if email:
                    return email, SOURCE_CONTACTS_DB_EMAIL
            except Exception as e:
                logger.warning("Contacts DB name+domain lookup failed for %s / %s: %s", full_name, domain, e)

        # Step 3: Blitz email enrichment (FALLBACK)
        if linkedin_url:
            try:
                result = await blitz_client.find_work_email(blitz_client_inst, linkedin_url)
                if result.get("found") and result.get("email"):
                    return result["email"], SOURCE_BLITZ
            except Exception as e:
                logger.warning("Blitz email lookup failed for %s: %s", linkedin_url, e)

        # Step 4: Contacts DB by input row name + domain (if different from person name)
        if input_full_name and input_full_name != full_name and domain:
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
) -> list[OutputRow]:
    company_linkedin_url = ""
    linkedin_source = ""

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
        if full_name:
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

    # Step 2: Get decision makers (Contacts DB FIRST)
    persons: list[dict[str, Any]] = []
    contacts_db_quality_met = False

    async with domain_semaphore:
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
        if not contacts_db_quality_met:
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

    blitz_http = httpx.AsyncClient()
    contacts_http = httpx.AsyncClient()

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
