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
import os
import urllib.parse
import uuid
from pathlib import Path
from typing import Any, Callable, Optional

import httpx

from . import blitz_client
from . import contacts_client
from . import better_enrich_client
from . import wizleads_client
from . import smartprospect_client
from . import providers
from . import mailtester_client
from . import job_store
from . import identifier_utils
from . import company_fallback
from . import fallback_config as fb_cfg

logger = logging.getLogger(__name__)


def _is_company_linkedin_url(url: str) -> bool:
    """Check if LinkedIn URL is a company page (not personal profile)."""
    if not url or "linkedin.com" not in url:
        return False
    return "/company/" in url or "/school/" in url or "/organization/" in url


def _is_personal_linkedin_url(url: str) -> bool:
    """Check if LinkedIn URL is a personal profile."""
    if not url or "linkedin.com" not in url:
        return False
    return "/in/" in url


def _detect_linkedin_url_type(url: str) -> str:
    """Detect if URL is 'personal', 'company', or 'unknown'."""
    if _is_company_linkedin_url(url):
        return "company"
    elif _is_personal_linkedin_url(url):
        return "personal"
    return "unknown"


# Valid provider values for force_provider parameter
VALID_PROVIDERS = frozenset({"contacts_db", "blitz", "smartprospect", "wizleads", "better_enrich"})


def _should_skip_provider(
    provider: str,
    force_provider: Optional[str] = None,
    selected_providers: Optional[list[str]] = None,
) -> bool:
    """
    Determine if a provider should be skipped.

    Args:
        provider: The current provider being considered (e.g., "contacts_db", "blitz")
        force_provider: The forced provider from request (or None for normal cascade)
        selected_providers: List of user-selected providers to use (or None for all enabled)

    Returns:
        True if the provider should be skipped, False otherwise

    Checks:
      1. Is the provider globally disabled in ENABLED_PROVIDERS?
      2. If force_provider is set, does it match the current provider?
      3. If selected_providers is set, is the provider in the list?
    """
    # force_provider takes precedence - if set, only use that provider
    if force_provider:
        return provider != force_provider

    # If selected_providers is set, it takes precedence over global enablement
    # This allows user to disable a globally-enabled provider
    if selected_providers is not None:
        # contacts_db is ALWAYS included (mandatory first step) - cannot be skipped
        if provider == "contacts_db":
            return False
        return provider not in selected_providers

    # No user selection - use global enablement check
    if not providers.is_provider_enabled(provider):
        logger.debug("_should_skip_provider: %s disabled in ENABLED_PROVIDERS", provider)
        return True

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
SOURCE_WIZLEADS = "wizleads_email"
SOURCE_SMARTPROSPECT = "smartprospect_email"
SOURCE_BETTER_ENRICH = "better_enrich"
SOURCE_BLITZ_COMPANY = "blitz_company"
SOURCE_NOT_FOUND = "not_found"


# =============================================================================
# Helper Functions
# =============================================================================

# All enrichment columns (for output ordering)
ENRICHED_COLUMNS = [
    "company_linkedin_url",
    "company_name",
    "company_industry",
    "company_employee_count",
    "dm_first_name",
    "dm_last_name",
    "dm_full_name",
    "dm_title",
    "dm_job_level",
    "dm_job_function",
    "dm_linkedin_url",
    "dm_email",
    "dm_email_source",
    "dm_email_verified",
    "mailtester_code",
    "mailtester_message",
    "dm_phone",
    "dm_headline",
    "dm_location_city",
    "dm_location_country",
    "dm_icp_tier",
    "row_status",
    # Input identifiers for visibility and debugging
    "input_domain",
    "input_full_name",
    "input_linkedin_url",
    "input_phone",
    "input_company_name",
    "input_existing_email",
    "input_facebook_url",
    "normalized_linkedin_url",
    "linkedin_username",
    "input_fields_used",
    # Routing diagnostics (audit trail)
    "source_path",
    "provider_attempts",
    "provider_attempts_json",
    "providers_called",
    "providers_skipped",
    "no_email_reason",
    "final_email_status",
    "final_email_verification_source",
    # Provider errors (user-facing error information)
    "provider_errors",
    # Company / page-level fallback outputs (BetterEnrich).
    "company_email",
    "company_email_source",
    "company_email_verified",
    "company_email_type",
    "company_email_source_path",
    "final_email",
    "final_email_level",
    "final_email_source_path",
]


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
        "mailtester_code": "",
        "mailtester_message": "",
        "dm_phone": "",
        "dm_headline": "",
        "dm_location_city": "",
        "dm_location_country": "",
        "dm_icp_tier": "",
        "row_status": "",
        "input_domain": "",
        "input_full_name": "",
        "input_linkedin_url": "",
        "input_phone": "",
        "input_company_name": "",
        "input_existing_email": "",
        "input_facebook_url": "",
        "normalized_linkedin_url": "",
        "linkedin_username": "",
        "input_fields_used": "",
        "source_path": "",
        "provider_attempts": "",
        "provider_attempts_json": "",
        "providers_called": "",
        "providers_skipped": "",
        "no_email_reason": "",
        "final_email_status": "",
        "final_email_verification_source": "",
        "provider_errors": "",
        "company_email": "",
        "company_email_source": "",
        "company_email_verified": "",
        "company_email_type": "",
        "company_email_source_path": "",
        "final_email": "",
        "final_email_level": "",
        "final_email_source_path": "",
    }


def _current_title(experiences: list[dict], direct_title: str = "") -> str:
    """
    Extract current job title from experiences or direct title field.

    Args:
        experiences: List of experience dicts from Blitz API
        direct_title: Direct title field from Contacts DB or other sources

    Returns:
        Current job title, prioritizing experiences array over direct_title
    """
    # First try experiences array (Blitz data - has historical context)
    for exp in experiences or []:
        if exp.get("job_is_current"):
            return exp.get("job_title", "")
    if experiences:
        return experiences[0].get("job_title", "")
    # Fallback to direct title field (Contacts DB data)
    return direct_title or ""


def _normalize_source(source: str) -> str:
    """Map raw source value to provider group."""
    if source.startswith("contacts_db"):
        return "contacts_db"
    elif source.startswith("blitz"):
        return "blitz"
    elif source.startswith("smartprospect"):
        return "smartprospect"
    elif source.startswith("better_enrich"):
        return "better_enrich"
    elif source.startswith("wizleads"):
        return "wizleads"
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
    selected_providers: Optional[list[str]] = None,
    validate_email: bool = True,  # NEW PARAMETER
    record_provider_use: Optional[callable] = None,  # NEW: callback to record provider usage
) -> tuple[str, str, str, str, str, str]:
    """
    Resolve email for a person using Contacts DB first, then Blitz fallback.

    Returns: (email, phone, source, verified, mailtester_code, mailtester_message)

    Args:
        force_provider: If set, only use that specific provider.
        selected_providers: List of user-selected providers to use (or None for all enabled).
        validate_email: If True, verify Contacts DB emails with mailtester.
        record_provider_use: Optional callback called with provider name when that provider is actually queried.
    """
    linkedin_url = person_data.get("linkedin_url", "")
    full_name = person_data.get("full_name", "")
    first_name = person_data.get("first_name", "")
    last_name = person_data.get("last_name", "")

    # Initialize verification fields
    mailtester_code = ""
    mailtester_message = ""
    verified = "unknown"

    # Determine search name (prefer person full_name, fall back to input_full_name)
    search_name = full_name or input_full_name

    # Strategy 1: Contacts DB by name + domain (PRIMARY - FREE)
    # When both name and LinkedIn URL are available, prefer name+domain to avoid
    # returning stale emails from previous employers via person_by_linkedin
    if search_name and domain and not _should_skip_provider("contacts_db", force_provider, selected_providers):
        if record_provider_use:
            record_provider_use("contacts_db")
        try:
            contacts_data = await contacts_client.person_by_name_and_domain(
                contacts_http, search_name, domain
            )
            email = contacts_client.extract_email_from_contacts_response(contacts_data)
            if email:
                phone = contacts_data.get("phone", "") if contacts_data else ""

                # Verify email with mailtester if enabled
                if validate_email:
                    try:
                        result = await mailtester_client.verify_email(blitz_http, email)
                        mailtester_code = result["code"]
                        mailtester_message = result["message"]
                        verified = "yes" if result["valid"] else "no"

                        if result["valid"]:
                            # Email is valid - use it
                            logger.debug("Email verified: %s (code: %s)", email, result["code"])
                            return email, phone, SOURCE_CONTACTS_DB_NAME, verified, mailtester_code, mailtester_message
                        else:
                            # Email rejected. Only poison the Contacts DB row
                            # on hard-invalid (ko); policy-rejected codes (e.g.
                            # mb under the ok-only policy) skip the write so the
                            # row can resurface if the policy is later relaxed.
                            if mailtester_code == "ko":
                                logger.info("Email verification failed: %s (code: %s) - marking invalid", email, mailtester_code)
                                await contacts_client.mark_email_invalid(
                                    contacts_http,
                                    email=email,
                                    domain=domain,
                                )
                            else:
                                logger.info("Email rejected by policy: %s (code: %s) - skipping mark_email_invalid", email, mailtester_code)
                            # Continue to next strategy
                    except RuntimeError:
                        # Mailtester unavailable - FAIL OPEN
                        logger.warning("Mailtester unavailable for %s - accepting without verification", email)
                        mailtester_code = "unavailable"
                        verified = "unknown"
                        return email, phone, SOURCE_CONTACTS_DB_NAME, verified, mailtester_code, mailtester_message
                else:
                    # Verification disabled - use email as-is
                    return email, phone, SOURCE_CONTACTS_DB_NAME, "no", "", ""
        except Exception as e:
            logger.debug("Contacts DB name+domain lookup failed: %s", e)

    # Strategy 2: Contacts DB by LinkedIn URL (SECONDARY - FREE)
    # Fall back to LinkedIn if name+domain didn't find email, or if name not available
    if linkedin_url and not _should_skip_provider("contacts_db", force_provider, selected_providers):
        if record_provider_use:
            record_provider_use("contacts_db")
        try:
            contacts_data = await contacts_client.person_by_linkedin(
                contacts_http, linkedin_url
            )
            email = contacts_client.extract_email_from_contacts_response(contacts_data)
            if email:
                phone = contacts_data.get("phone", "") if contacts_data else ""

                # Verify email with mailtester if enabled
                if validate_email:
                    try:
                        result = await mailtester_client.verify_email(blitz_http, email)
                        mailtester_code = result["code"]
                        mailtester_message = result["message"]
                        verified = "yes" if result["valid"] else "no"

                        if result["valid"]:
                            # Email is valid - use it
                            logger.debug("Email verified: %s (code: %s)", email, result["code"])
                            return email, phone, SOURCE_CONTACTS_DB_LINKEDIN, verified, mailtester_code, mailtester_message
                        else:
                            # Email rejected. Only poison the Contacts DB row
                            # on hard-invalid (ko); policy-rejected codes (e.g.
                            # mb under the ok-only policy) skip the write so the
                            # row can resurface if the policy is later relaxed.
                            if mailtester_code == "ko":
                                logger.info("Email verification failed: %s (code: %s) - marking invalid", email, mailtester_code)
                                await contacts_client.mark_email_invalid(
                                    contacts_http,
                                    email=email,
                                )
                            else:
                                logger.info("Email rejected by policy: %s (code: %s) - skipping mark_email_invalid", email, mailtester_code)
                            # Continue to next strategy
                    except RuntimeError:
                        # Mailtester unavailable - FAIL OPEN
                        logger.warning("Mailtester unavailable for %s - accepting without verification", email)
                        mailtester_code = "unavailable"
                        verified = "unknown"
                        return email, phone, SOURCE_CONTACTS_DB_LINKEDIN, verified, mailtester_code, mailtester_message
                else:
                    # Verification disabled - use email as-is
                    return email, phone, SOURCE_CONTACTS_DB_LINKEDIN, "no", "", ""
        except Exception as e:
            logger.debug("Contacts DB LinkedIn lookup failed: %s", e)

    # Strategy 3: Blitz person enrich by name + domain (PRIMARY - PAID)
    # Prioritize name+domain for the same reason as Contacts DB
    if search_name and domain and not _should_skip_provider("blitz", force_provider, selected_providers):
        if record_provider_use:
            record_provider_use("blitz")
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
                    return email, phone, source, verified, "", ""
        except Exception as e:
            logger.debug("Blitz person enrich failed: %s", e)

    # Strategy 4: Blitz API by LinkedIn URL (SECONDARY - PAID)
    # Fall back to LinkedIn if name+domain didn't find email, or if name not available
    if linkedin_url and not _should_skip_provider("blitz", force_provider, selected_providers):
        if record_provider_use:
            record_provider_use("blitz")
        try:
            result = await blitz_client.find_work_email(blitz_http, linkedin_url)
            if result.get("found") and result.get("email"):
                # Blitz email endpoint - check if there's verification info
                all_emails = result.get("all_emails", [])
                verified = "yes" if all_emails and all_emails[0].get("verified") else "unknown"
                return result.get("email", ""), "", SOURCE_BLITZ_LINKEDIN, verified, "", ""
        except Exception as e:
            logger.debug("Blitz email lookup failed: %s", e)

    # Strategy 5: SmartProspect by first + last + domain (self-verifying, 30 RPS, batch-capable)
    # Inserted between Blitz and WizLeads. Gates on first_name + last_name +
    # domain presence only — decoupled from Blitz (per user requirement).
    if search_name and domain and not _should_skip_provider("smartprospect", force_provider, selected_providers):
        first_name = search_name.split(" ")[0] if search_name else ""
        last_name = " ".join(search_name.split(" ")[1:]) if " " in search_name else ""
        if first_name and last_name:
            if record_provider_use:
                record_provider_use("smartprospect")
            try:
                result = await smartprospect_client.find_email(
                    blitz_http,
                    first_name=first_name,
                    last_name=last_name,
                    company_domain=domain,
                )
                if result and result.get("email"):
                    email = result["email"]
                    vs = result.get("verification_status")
                    verified = "yes" if vs == "Valid" else "unknown"
                    logger.debug("SmartProspect found email for %s: %s (verification_status: %s)", search_name, email, vs)
                    return email, "", SOURCE_SMARTPROSPECT, verified, "", ""
            except Exception as e:
                logger.debug("SmartProspect lookup failed: %s", e)

    # Strategy 6: WizLeads by name + domain (catchall verified, 10 RPS)
    # Inserted between Blitz and BetterEnrich per user-confirmed cascade order.
    if search_name and domain and not _should_skip_provider("wizleads", force_provider, selected_providers):
        if record_provider_use:
            record_provider_use("wizleads")
        first_name = search_name.split(" ")[0] if search_name else ""
        last_name = " ".join(search_name.split(" ")[1:]) if " " in search_name else ""
        try:
            result = await wizleads_client.find_email(blitz_http, first_name=first_name, last_name=last_name, website=domain)
            if result and result.get("email"):
                email = result["email"]
                logger.debug("WizLeads found email for %s: %s (catchall: %s)", search_name, email, result.get("catchall"))
                return email, "", SOURCE_WIZLEADS, "yes", "", ""
        except Exception as e:
            logger.debug("WizLeads lookup failed: %s", e)

    # Strategy 7: Better Enrich by name + domain (TERTIARY - PAID)
    # Only try if name is available and Better Enrich is selected
    if search_name and domain and not _should_skip_provider("better_enrich", force_provider, selected_providers):
        if record_provider_use:
            record_provider_use("better_enrich")
        try:
            result = await better_enrich_client.find_work_email_v3(contacts_http, search_name, domain, linkedin_url)
            if result and result.get("email"):
                email = result["email"]
                # V3 provides email_status - map to verified status
                email_status = result.get("email_status", "verified")
                if email_status in ("verified", "valid"):
                    verified = "yes"
                else:
                    verified = "unknown"
                logger.debug("Better Enrich V3 found email for %s: %s (status: %s)", search_name, email, email_status)
                return email, "", SOURCE_BETTER_ENRICH, verified, "", ""
        except Exception as e:
            logger.debug("Better Enrich V3 lookup failed: %s", e)

    return "", "", SOURCE_NOT_FOUND, "unknown", "", ""


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
    selected_providers: Optional[list[str]] = None,
    validate_email: bool = True,  # NEW PARAMETER
    record_provider_use: Optional[callable] = None,  # NEW: callback to record provider usage
    cascade_config: Optional[str] = None,  # NEW: JSON cascade config from job
    collector: Optional[Any] = None,  # RawContactCollector; Phase 1 capture
) -> list[OutputRow]:
    """
    Enrich a single domain: get company info, generic emails, and decision makers.

    Returns list of output rows (one per decision maker + generic emails if requested).

    Args:
        force_provider: If set, only use that specific provider.
        selected_providers: List of user-selected providers to use (or None for all enabled).
        record_provider_use: Optional callback to record which providers were actually queried.
        cascade_config: Optional JSON string with custom cascade config from job.
        collector: Optional ``RawContactCollector``. Captures every contact
            returned at the company-level lookup step (Contacts DB + Blitz).
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
                # FIX: previously hardcoded ``limit=10`` regardless of the
                # user-selected ``max_decision_makers``. Use the user cap
                # so the audit capture and the user-facing CSV agree on
                # how many records the provider returned for this domain.
                contacts = await contacts_client.company_contacts_enriched(
                    contacts_http, domain, limit=max_decision_makers
                )
                if contacts and len(contacts) > 0:
                    # Phase 1: capture every contact before any filtering.
                    if collector is not None:
                        for _gc in contacts:
                            try:
                                collector.capture_company_contact(
                                    source="contacts_db",
                                    domain=domain,
                                    company_linkedin_url="",
                                    contact=_gc,
                                )
                            except Exception:
                                pass
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
                # Phase 1: capture every Contacts DB contact BEFORE the
                # ``[:max_decision_makers]`` truncation in the loop below.
                if collector is not None:
                    for _cc in contacts_contacts:
                        try:
                            collector.capture_company_contact(
                                source="contacts_db",
                                domain=domain,
                                company_linkedin_url=company_linkedin_url,
                                contact=_cc,
                            )
                        except Exception:
                            pass
                # Check quality: need at least 1 person with email
                emails_count = sum(1 for c in contacts_contacts if c.get("email"))
                if len(contacts_contacts) >= 1 and emails_count >= 1:
                    # Convert to standard format
                    for contact in contacts_contacts[:max_decision_makers]:
                        persons.append({
                            "person": {
                                "title": contact.get("title", ""),  # Preserve title field
                                "first_name": contact.get("first_name", ""),
                                "last_name": contact.get("last_name", ""),
                                "full_name": contact.get("full_name", ""),
                                "headline": contact.get("headline", ""),
                                "linkedin_url": contact.get("linkedin_url", ""),
                                "location": {
                                    "city": contact.get("city", ""),
                                    "country_code": contact.get("country_code", ""),
                                },
                                "experiences": [],  # Empty - Contacts DB doesn't provide historical data
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
                # Use cascade_config from job if available, otherwise use default
                if cascade_config:
                    import json
                    cascade = json.loads(cascade_config)
                else:
                    cascade = blitz_client.DEFAULT_CASCADE
                icp_result = await blitz_client.waterfall_icp_search(
                    blitz_http, company_linkedin_url, cascade, max_decision_makers
                )
                persons = icp_result.get("results", [])
                # Phase 1: capture every Blitz contact.
                if collector is not None and persons:
                    for _bp in persons:
                        try:
                            collector.capture_company_contact(
                                source="blitz",
                                domain=domain,
                                company_linkedin_url=company_linkedin_url,
                                contact=_bp,
                            )
                        except Exception:
                            pass
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
                selected_providers=selected_providers,
                validate_email=validate_email,
                record_provider_use=record_provider_use,
            )
        )

    email_results = await asyncio.gather(*tasks)

    # Build output rows
    for item, (email, phone, source, verified, mailtester_code, mailtester_message) in zip(persons, email_results):
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
        row["dm_title"] = _current_title(person.get("experiences", []), person.get("title", ""))
        row["dm_linkedin_url"] = person.get("linkedin_url", "")
        row["dm_email"] = email
        row["dm_email_source"] = source
        row["dm_email_verified"] = verified
        row["mailtester_code"] = mailtester_code
        row["mailtester_message"] = mailtester_message
        row["dm_phone"] = phone
        row["dm_headline"] = person.get("headline", "")
        loc = person.get("location", {})
        row["dm_location_city"] = loc.get("city", "")
        row["dm_location_country"] = loc.get("country_code", "")
        row["dm_icp_tier"] = str(icp_tier)
        row["row_status"] = STATUS_ENRICHED if email else STATUS_NO_CONTACTS

        output_rows.append(row)

    return output_rows


async def _merge_by_company_contacts(
    contacts_http: httpx.AsyncClient,
    output_rows: list[dict[str, Any]],
    domain: str,
    base_row: dict[str, Any],
    force_provider: Optional[str],
    source: Optional[str] = None,
) -> list[dict[str, Any]]:
    """Phase 1B (2026-07-21): append EVERY Contacts DB person filed under the
    company to ``output_rows``, emails preserved as stored (no mailtester).
    Backs the bulk list-building flow so loaded/outscraper data is retrievable
    by domain. Gated by ENABLE_COMPANY_LOOKUP; skipped when ``force_provider``
    is set. Additive + deduped by email/name; a cascade row is replaced only
    if its email was stripped/empty (validated emails are never overwritten →
    no data loss). Tagged dm_email_source="contacts_db_by_company" so they're
    distinguishable in the CSV.

    Phase 2 (2026-07-22): optional ``source`` (e.g. "outscraper") narrows the
    internal-DB lookup to contacts tagged with that source only. ``None`` →
    all sources (today's behavior — no regression).
    """
    if force_provider:
        return output_rows
    if os.getenv("ENABLE_COMPANY_LOOKUP", "").strip().lower() not in ("1", "true", "yes"):
        return output_rows
    try:
        by_company = await contacts_client.company_persons_by_domain(
            contacts_http, domain, limit=500, source=source
        )
    except Exception as e:
        logger.debug("by-company lookup failed for %s: %s", domain, e)
        return output_rows
    if not by_company:
        return output_rows
    logger.info("by-company %s: fetched=%d cascade=%d", domain, len(by_company), len(output_rows))
    seen_emails = {(r.get("dm_email") or "").strip().lower()
                   for r in output_rows if r.get("dm_email")}
    seen_names = {(r.get("dm_full_name") or "").strip().lower()
                  for r in output_rows if r.get("dm_full_name")}
    for bc in by_company:
        em = (bc.get("email") or "").strip()
        nm = (bc.get("full_name") or "").strip()
        if not em and not nm:
            continue
        key_em = em.lower() if em else ""
        key_nm = nm.lower() if nm else ""
        if key_em and key_em in seen_emails:
            continue
        if key_nm and key_nm in seen_names:
            existing = next((r for r in output_rows
                             if (r.get("dm_full_name") or "").strip().lower() == key_nm), None)
            if existing and (existing.get("dm_email") or "").strip():
                continue  # keep validated cascade email; don't duplicate
            output_rows = [r for r in output_rows
                           if (r.get("dm_full_name") or "").strip().lower() != key_nm]
        if key_em:
            seen_emails.add(key_em)
        if key_nm:
            seen_names.add(key_nm)
        row = {**base_row, **_empty_enriched()}
        row["company_name"] = domain
        row["dm_full_name"] = nm
        row["dm_first_name"] = bc.get("first_name", "") or ""
        row["dm_last_name"] = bc.get("last_name", "") or ""
        row["dm_title"] = bc.get("title", "") or ""
        row["dm_email"] = em
        row["dm_email_source"] = "contacts_db_by_company"
        row["dm_email_verified"] = "unverified"
        row["dm_headline"] = bc.get("headline", "") or ""
        row["dm_location_city"] = bc.get("city", "") or ""
        row["dm_location_country"] = bc.get("country", "") or ""
        row["row_status"] = STATUS_ENRICHED if em else STATUS_NO_CONTACTS
        output_rows.append(row)
    return output_rows


async def _apply_company_fallback_to_output_rows(
    blitz_http: httpx.AsyncClient,
    output_rows: list[dict[str, Any]],
    *,
    domain: str,
    facebook_url: str,
    dedupe: company_fallback.CompanyFallbackDedupe,
    record_provider_use: Optional[Callable[[str], None]] = None,
    source_path_prefix: str = "",
    collector: Optional[Any] = None,
) -> None:
    """Run the company/page-level fallback once per domain and apply
    to all output rows that lack a person-level email.

    Mirrors `_maybe_apply_company_fallbacks` in pipeline.py for the
    list_builder flows (Flows 1, 3). No-op when both fallback flags
    are off or every row already has dm_email.
    """
    if not fb_cfg.ENABLE_COMPANY_EMAIL_FALLBACK and not fb_cfg.ENABLE_FACEBOOK_EMAIL_FALLBACK:
        return

    any_missing = any(not r.get("dm_email", "") for r in output_rows)
    if not any_missing:
        for row in output_rows:
            person_email = row.get("dm_email", "")
            if person_email:
                row["final_email"] = person_email
                row["final_email_level"] = "person"
                if not row.get("final_email_source_path"):
                    row["final_email_source_path"] = row.get("source_path", "")
        return

    fb_result = await company_fallback.run_company_fallbacks(
        blitz_http,
        domain=domain,
        facebook_url=facebook_url,
        source_path_prefix=source_path_prefix,
        validate_email=True,
        dedupe=dedupe,
        record_provider_use=record_provider_use,
        collector=collector,
        company_linkedin_url=(output_rows[0].get("company_linkedin_url", "") if output_rows else ""),
    )

    for row in output_rows:
        company_fallback.apply_company_fallbacks_to_row(
            row, fb_result,
            person_email=row.get("dm_email", ""),
            person_source_path=row.get("source_path", ""),
        )


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
    selected_providers: Optional[list[str]] = None,
    cancelled_jobs: Optional[set[str]] = None,
    check_cancelled: Optional[Callable[[str], bool]] = None,
    job_id: Optional[str] = None,
    validate_email: bool = True,  # NEW PARAMETER
    company_linkedin_col: Optional[str] = None,  # NEW: column name for company LinkedIn URLs
    linkedin_url_col: Optional[str] = None,  # NEW: column name for person LinkedIn URLs (auto-detect /company/ URLs)
    record_provider_use: Optional[Callable[[str], None]] = None,  # NEW: callback to record provider usage
    get_store_fn: Optional[Callable[[], Any]] = None,  # Injected to avoid module-level ref
    normalize_domains: bool = True,  # Pre-processing flag: gate the per-row normalize_domain() call
    cascade_config: Optional[str] = None,  # NEW: JSON cascade config from job store
    collector: Optional[Any] = None,  # RawContactCollector; Phase 1 capture
    source: Optional[str] = None,  # Phase 2: filter by-company Contacts DB lookup by source ("outscraper")
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
        selected_providers: List of user-selected providers to use (or None for all enabled).
        cancelled_jobs: Set of cancelled job IDs (checked in-memory)
        check_cancelled: Function to check if job is cancelled (DB check)
        job_id: Job ID for cancellation tracking
        record_provider_use: Optional callback called with provider name when that provider is queried.
        cascade_config: Optional JSON string with custom cascade config from job store.
        collector: Optional ``RawContactCollector``. When provided, every
            company-level provider response is captured for audit/write-back.

    Returns:
        List of enriched output rows
    """
    domain_semaphore = asyncio.Semaphore(DOMAIN_CONCURRENCY)
    email_semaphore = asyncio.Semaphore(LINKEDIN_CONCURRENCY)

    blitz_http = httpx.AsyncClient()
    contacts_http = httpx.AsyncClient()

    all_output: list[OutputRow] = []
    total = len(rows)

    # Fetch cascade_config from job store if not provided
    # This allows the cascade to be customized per job
    if job_id and not cascade_config:
        try:
            _get_store = get_store_fn or job_store.get_store
            store = _get_store()
            job = store.get_job(job_id)
            if job:
                cascade_config = job.get("cascade_config")
                if cascade_config:
                    logger.info("Using cascade_config from job %s", job_id)
        except Exception as e:
            logger.warning("Failed to fetch cascade_config from job %s: %s", job_id, e)

    async def process_row(idx: int, row: dict[str, Any]) -> list[OutputRow]:
        # Note: Cancellation is now checked at batch level, not per-row
        # This allows the batch to finish but stops subsequent batches

        # Normalize the raw CSV domain to bare-domain form so provider
        # APIs don't 404 on full URLs like "https://acme.com/?utm_source=x"
        # When normalize_domains=False, the raw stripped value flows
        # through (useful for franchise locations with unique URL paths).
        raw_domain = str(row.get(domain_col, "") or "")
        if normalize_domains:
            domain = identifier_utils.normalize_domain(raw_domain)
        else:
            domain = raw_domain.strip()
        logger.debug(
            "Row %d: normalize_domains=%s, raw=%r, domain=%r",
            idx, normalize_domains, raw_domain, domain,
        )

        # Extract company LinkedIn URL per-row. Three sources, in priority:
        #   1. Explicit `company_linkedin_col` value
        #   2. Auto-detect: `linkedin_url_col` value that is a /company/ URL
        #   3. Otherwise: None (fall through to domain cascade)
        company_linkedin_url_raw = ""
        if company_linkedin_col and row.get(company_linkedin_col):
            company_linkedin_url_raw = str(row.get(company_linkedin_col) or "").strip()
        elif linkedin_url_col and row.get(linkedin_url_col):
            _li_val = str(row.get(linkedin_url_col) or "").strip()
            if _is_company_linkedin_url(_li_val):
                company_linkedin_url_raw = _li_val

        if company_linkedin_url_raw:
            company_linkedin_url = identifier_utils.normalize_linkedin_url(company_linkedin_url_raw)
            # Short-circuit: use the company-URL orchestrator directly
            result = await _enrich_by_company_linkedin(
                blitz_http=blitz_http,
                contacts_http=contacts_http,
                base_row={**row, "domain": domain, "company_linkedin_url": company_linkedin_url},
                company_linkedin_url=company_linkedin_url,
                domain=domain,
                max_dms=max_decision_makers,
                domain_semaphore=domain_semaphore,
                email_semaphore=email_semaphore,
                force_provider=force_provider,
                selected_providers=selected_providers,
                validate_email=validate_email,
                record_provider_use=record_provider_use,
                collector=collector,
            )
        elif not domain:
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
                selected_providers=selected_providers,
                validate_email=validate_email,
                record_provider_use=record_provider_use,
                cascade_config=cascade_config,
                collector=collector,
            )
            # Phase 1B (2026-07-21): by-company Contacts DB augment (flag-gated,
            # additive, emails preserved). See _merge_by_company_contacts.
            result = await _merge_by_company_contacts(
                contacts_http, result, domain, row, force_provider, source=source
            )

        # Call progress callback with exception handling
        # This prevents progress callback errors from crashing the entire batch
        try:
            if on_progress:
                # Collect source counts from results
                source_counts: dict[str, int] = {}
                for r in result:
                    src = r.get("dm_email_source", "")
                    if src:
                        provider = _normalize_source(src)
                        source_counts[provider] = source_counts.get(provider, 0) + 1
                emails_found = sum(1 for r in result if r.get("dm_email"))
                progress_event = {
                    "index": idx,
                    "total": total,
                    "domain": domain,
                    "status": result[0].get("row_status", STATUS_ERROR),
                    "contacts_found": len(result),
                    "emails_found": emails_found,
                    "source_counts": source_counts,
                }
                # Handle both sync and async callbacks
                if asyncio.iscoroutinefunction(on_progress):
                    await on_progress(progress_event)
                else:
                    on_progress(progress_event)
        except Exception as prog_err:
            logger.warning("Progress callback failed for domain %s: %s", domain, prog_err)

        # Company/page-level fallback (BetterEnrich Facebook + company
        # email). Runs once per domain; applied to all output rows
        # that lack a person-level email. Mirrors run_pipeline wiring.
        if domain:
            row_facebook_url = str(row.get("facebook_url", "") or row.get("facebook", "") or "").strip()
            domain_dedupe = company_fallback.CompanyFallbackDedupe()
            await _apply_company_fallback_to_output_rows(
                blitz_http,
                result,
                domain=domain,
                facebook_url=row_facebook_url,
                dedupe=domain_dedupe,
                record_provider_use=record_provider_use,
                collector=collector,
            )

        return result

    async def check_cancelled_and_raise():
        """Check if job was cancelled and raise exception if so."""
        if job_id:
            if cancelled_jobs and job_id in cancelled_jobs:
                logger.info("Job %s cancelled (in-memory set), stopping", job_id)
                raise RuntimeError(f"Job {job_id} was cancelled by user")
            if check_cancelled and check_cancelled(job_id):
                # Check why it was cancelled
                _get_store = get_store_fn or job_store.get_store
                check_store = _get_store()
                job = check_store.get_job(job_id)
                if job:
                    status = job.get("status", "unknown")
                    error = job.get("error", "")
                    if status == "abandoned":
                        logger.info("Job %s found abandoned (server restart), stopping", job_id)
                        raise RuntimeError(f"Job {job_id} was abandoned due to server restart. Please retry.")
                    elif status == "cancelled":
                        logger.info("Job %s found cancelled (by user), stopping", job_id)
                        raise RuntimeError(f"Job {job_id} was cancelled by user")
                    else:
                        logger.info("Job %s cancelled (DB status=%s), stopping", job_id, status)
                        raise RuntimeError(f"Job {job_id} was cancelled (status: {status})")

    # Process in batches to allow cancellation between batches
    BATCH_SIZE = 50  # Process 50 rows at a time
    all_output = []

    # Track row-level failures so we can surface them when the job produces
    # zero output. Without this, asyncio.gather(return_exceptions=True)
    # silently swallows per-row exceptions and the job finishes as "done"
    # with an empty CSV — exactly the 2026-07-12..13 outage pattern.
    first_row_exception: Optional[Exception] = None
    row_exception_count = 0

    # Phase 3 (2026-07-22): when the incremental-persistence flag is on, a
    # mid-run cancellation RETURNS the partial output instead of raising — so
    # the caller (_run_domain_enrich_job) can persist it (CSV + collector
    # drain) rather than losing every row found so far. Default off = the
    # legacy raise-and-lose behavior, so no regression until enabled.
    _persist_flag_on = os.getenv("ENABLE_INCREMENTAL_PERSISTENCE", "").strip().lower() in ("1", "true", "yes")
    _stopped_mid_run = False

    for batch_start in range(0, total, BATCH_SIZE):
        # Check cancellation at start of each batch
        if _persist_flag_on:
            try:
                await check_cancelled_and_raise()
            except RuntimeError:
                logger.info("Job %s cancelled before batch %d — returning %d partial rows", job_id, batch_start, len(all_output))
                _stopped_mid_run = True
                break
        else:
            await check_cancelled_and_raise()

        batch_end = min(batch_start + BATCH_SIZE, total)
        batch_rows = rows[batch_start:batch_end]

        # Process batch concurrently
        tasks = [process_row(batch_start + i, row) for i, row in enumerate(batch_rows)]
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Collect batch results
        for result in batch_results:
            if isinstance(result, Exception):
                # Check if it's a cancellation error - if so, stop immediately
                if "was cancelled" in str(result):
                    logger.info("Job %s cancellation raised, stopping batch processing", job_id)
                    if _persist_flag_on:
                        _stopped_mid_run = True
                        break
                    raise result
                logger.error("Row processing failed: %s", result)
                row_exception_count += 1
                if first_row_exception is None:
                    first_row_exception = result
            else:
                all_output.extend(result)
        if _stopped_mid_run:
            break

    await blitz_http.aclose()
    await contacts_http.aclose()

    # Phase 3: cancelled mid-run -> return partial output so the caller persists it.
    if _stopped_mid_run:
        logger.info("Job %s stopped mid-run — returning %d partial output rows", job_id, len(all_output))
        return all_output

    # If every single row failed, surface the exception so the caller marks
    # the job as failed rather than "done with 0 rows". This is the safety
    # net that would have caught the 2026-07-11 linkedin_url_col regression
    # at the first failing job instead of letting 22 jobs silently fail.
    if total > 0 and len(all_output) == 0 and first_row_exception is not None:
        raise RuntimeError(
            f"All {row_exception_count}/{total} rows failed. "
            f"First error: {type(first_row_exception).__name__}: {first_row_exception}"
        )

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
    record_provider_use: Optional[Callable[[str], None]] = None,
) -> OutputRow:
    """
    Enrich a single LinkedIn URL with person + company details.

    Args:
        record_provider_use: Optional callback invoked with each provider that is
            actually queried (``"contacts_db"``, ``"blitz"``) so the job's
            ``used_providers`` tally stays accurate.

    Returns single output row.
    """
    if not semaphore:
        semaphore = asyncio.Semaphore(LINKEDIN_CONCURRENCY)

    row = {**base_row, **_empty_enriched()}

    async with semaphore:
        # Step 1: Try Contacts DB first (FREE)
        contacts_data = None
        if record_provider_use:
            record_provider_use("contacts_db")
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
        # /v2/enrichment/email returns a FLAT shape {found, email, all_emails}
        # (no "person" object) — same endpoint as blitz_client.find_work_email.
        # The old code read result["person"], which never exists, so every Blitz
        # email was silently dropped (2026-07-15 zero-email bug).
        if record_provider_use:
            record_provider_use("blitz")
        try:
            result = await blitz_client.person_enrich_by_linkedin(blitz_http, linkedin_url, include_phone=True)
            if result.get("found") and result.get("email"):
                all_emails = result.get("all_emails") or []
                verified = "yes" if (all_emails and all_emails[0].get("verified")) else "unknown"
                row["dm_linkedin_url"] = linkedin_url
                row["dm_email"] = result.get("email", "")
                row["dm_email_verified"] = verified
                row["dm_email_source"] = SOURCE_BLITZ_LINKEDIN
                # email-only endpoint: name/title/company are not returned here
                row["row_status"] = STATUS_ENRICHED
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
    validate_email: bool = True,
    record_provider_use: Optional[Callable[[str], None]] = None,
) -> list[OutputRow]:
    """
    Main entry point for Flow 3: LinkedIn URLs → Full Enrichment

    Args:
        rows: List of input rows from CSV
        linkedin_col: Column name containing LinkedIn URL
        on_progress: Callback for progress updates
        validate_email: If True, verify emails with mailtester (currently
            unused by ``_enrich_single_linkedin`` but kept for parity with
            other entry points).
        record_provider_use: Optional callback invoked with a provider name
            when that provider is queried. Used for usage telemetry.

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

        # Company/page-level fallback for LinkedIn-only flows. The
        # person's domain is the input; if no person email was found
        # and the row carries a facebook_url, attempt the page-level
        # fallback.
        row_facebook_url = str(row.get("facebook_url", "") or row.get("facebook", "") or "").strip()
        if not result.get("dm_email"):
            row_domain = identifier_utils.normalize_domain(
                str(row.get("domain", "") or row.get("company_domain", "") or "")
            )
            if row_domain or row_facebook_url:
                li_dedupe = company_fallback.CompanyFallbackDedupe()
                await _apply_company_fallback_to_output_rows(
                    blitz_http,
                    [result],
                    domain=row_domain,
                    facebook_url=row_facebook_url,
                    dedupe=li_dedupe,
                    record_provider_use=record_provider_use,
                    source_path_prefix=result.get("source_path", ""),
                    collector=None,  # run_linkedin_enrichment has no collector param yet
                )

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


# =============================================================================
# UNIFIED LINKEDIN ENRICHMENT (Personal + Company URLs)
# =============================================================================

# Local cascade constants (same as blitz_client.DEFAULT_CASCADE but defined
# locally so this module can use them directly without importing from blitz_client)
CASCADE_TIER_1 = {
    "include_title": ["Owner", "CEO", "Founder", "Co-Founder", "President"],
    "exclude_title": ["assistant", "intern", "junior", "associate"],
    "location": ["WORLD"],
    "include_headline_search": False,
}
CASCADE_TIER_2 = {
    "include_title": ["CMO", "VP Marketing", "VP Sales", "Chief Revenue Officer",
                      "Chief Marketing Officer", "VP of Marketing", "VP of Sales"],
    "exclude_title": ["assistant", "intern", "junior"],
    "location": ["WORLD"],
    "include_headline_search": False,
}
CASCADE_TIER_3 = {
    "include_title": ["Director of Marketing", "Director of Sales", "Head of Marketing",
                      "Head of Sales", "Head of Growth", "Marketing Director", "Sales Director"],
    "exclude_title": ["assistant", "intern", "junior"],
    "location": ["WORLD"],
    "include_headline_search": False,
}
DEFAULT_CASCADE = [CASCADE_TIER_1, CASCADE_TIER_2, CASCADE_TIER_3]


async def _enrich_by_company_waterfall(
    blitz_http: httpx.AsyncClient,
    company_url: str,
    cascade: list[dict[str, Any]],
    max_dms: int = 5,
    semaphore: asyncio.Semaphore = None,
    collector: Optional[Any] = None,  # RawContactCollector; Phase 1 capture
) -> list[dict[str, Any]]:
    """
    Use Blitz waterfall_icp_search to find decision makers from company LinkedIn URL.

    Returns list of person dictionaries with: first_name, last_name, full_name,
    title, job_level, linkedin_url, email, verified_email.

    Args:
        collector: Optional ``RawContactCollector``. When provided, every
            Blitz response person is captured before normalization to the
            function's flat dict shape.
    """
    if not semaphore:
        semaphore = asyncio.Semaphore(LINKEDIN_CONCURRENCY)

    results = []
    async with semaphore:
        try:
            response = await blitz_client.waterfall_icp_search(
                blitz_http,
                company_linkedin_url=company_url,
                cascade=cascade,
                max_results=max_dms,
            )

            if response.get("results"):
                # Phase 1: capture every Blitz result before any filtering.
                if collector is not None:
                    for _br in response["results"]:
                        try:
                            collector.capture_company_contact(
                                source="blitz",
                                domain="",
                                company_linkedin_url=company_url,
                                contact=_br,
                            )
                        except Exception:
                            pass
                for result in response["results"]:
                    person = result.get("person", {})
                    first_name = person.get("first_name", "")
                    last_name = person.get("last_name", "")
                    full_name = person.get("full_name", "") or f"{first_name} {last_name}".strip()

                    # Try verified_email first, fallback to emails list
                    verified_email = person.get("verified_email", "")
                    if not verified_email and person.get("emails"):
                        emails_list = person["emails"]
                        verified_email = emails_list[0].get("email", "") if isinstance(emails_list, list) and emails_list else ""

                    results.append({
                        "first_name": first_name,
                        "last_name": last_name,
                        "full_name": full_name,
                        "title": person.get("title", ""),
                        "job_level": _get_job_level(person.get("title", "")),
                        "linkedin_url": person.get("linkedin_url", ""),
                        "email": verified_email,
                        "verified_email": verified_email,
                        "headline": person.get("headline", ""),
                        "location_city": person.get("location", {}).get("city", "") if isinstance(person.get("location"), dict) else "",
                        "location_country": person.get("location", {}).get("country_code", "") if isinstance(person.get("location"), dict) else "",
                        "icp_tier": result.get("icp", ""),
                        "ranking": result.get("ranking", 0),
                    })
        except Exception as e:
            logger.debug("Company waterfall search failed for %s: %s", company_url, e)

    return results


async def _enrich_by_company_linkedin(
    blitz_http: httpx.AsyncClient,
    contacts_http: httpx.AsyncClient,
    base_row: dict[str, Any],
    company_linkedin_url: str,
    domain: str = "",
    cascade: Optional[list[dict[str, Any]]] = None,
    max_dms: int = 5,
    domain_semaphore: Optional[asyncio.Semaphore] = None,
    email_semaphore: Optional[asyncio.Semaphore] = None,
    force_provider: Optional[str] = None,
    selected_providers: Optional[list[str]] = None,
    validate_email: bool = True,
    record_provider_use: Optional[Callable[[str], None]] = None,
    collector: Optional[Any] = None,
) -> list[OutputRow]:
    """Enrich a single company LinkedIn URL → decision makers + emails.

    Entry point for company-URL-only inputs (Unified API `company_linkedin_url`
    field, Flow 1 CSV `company_linkedin_col`, auto-detected `/company/` URLs).

    Composition (reuses existing helpers — no new cascade logic):
      1. Validate URL via ``_is_company_linkedin_url``.
      2. Call ``_enrich_by_company_waterfall`` (Blitz title-waterfall) → persons.
      3. For each person, call ``_resolve_person_email`` (multi-provider cascade).
      4. Build ``OutputRow`` inline following the same pattern as
         ``_enrich_single_domain`` (list_builder.py:772-799).

    Args:
        company_linkedin_url: LinkedIn URL matching ``/company/``, ``/school/``,
            or ``/organization/``. Invalid URLs return a single
            ``STATUS_NO_LINKEDIN`` row.
        domain: Optional domain for email resolution (passed to
            ``_resolve_person_email``). May be empty.
        cascade: Optional title tiers. Defaults to ``blitz_client.DEFAULT_CASCADE``.
        max_dms: Cap on decision-makers returned.
        collector: Optional ``RawContactCollector`` propagated to the waterfall.

    Returns:
        List of ``OutputRow`` dicts. Empty waterfall → single
        ``STATUS_NO_CONTACTS`` row. Invalid URL → single ``STATUS_NO_LINKEDIN``
        row.
    """
    # Validate URL
    if not _is_company_linkedin_url(company_linkedin_url):
        row = {**base_row, **_empty_enriched()}
        row["row_status"] = STATUS_NO_LINKEDIN
        return [row]

    if cascade is None:
        cascade = blitz_client.DEFAULT_CASCADE

    # Run title-waterfall
    persons_raw = await _enrich_by_company_waterfall(
        blitz_http=blitz_http,
        company_url=company_linkedin_url,
        cascade=cascade,
        max_dms=max_dms,
        semaphore=domain_semaphore,
        collector=collector,
    )

    if not persons_raw:
        row = {**base_row, **_empty_enriched()}
        row["company_linkedin_url"] = company_linkedin_url
        row["row_status"] = STATUS_NO_CONTACTS
        return [row]

    # Resolve emails in parallel. persons_raw items are flat dicts (from
    # _enrich_by_company_waterfall); wrap in {"person": ...} shape expected
    # by _resolve_person_email consumers downstream.
    semaphore = email_semaphore or asyncio.Semaphore(LINKEDIN_CONCURRENCY)
    tasks = []
    for person in persons_raw:
        tasks.append(
            _resolve_person_email(
                blitz_http,
                contacts_http,
                person,
                domain,
                force_provider=force_provider,
                selected_providers=selected_providers,
                validate_email=validate_email,
                record_provider_use=record_provider_use,
            )
        )
    email_results = await asyncio.gather(*tasks)

    output_rows: list[OutputRow] = []
    for person, (email, phone, source, verified, mailtester_code, mailtester_message) in zip(
        persons_raw, email_results
    ):
        row = {**base_row, **_empty_enriched()}
        row["company_linkedin_url"] = company_linkedin_url
        row["dm_first_name"] = person.get("first_name", "")
        row["dm_last_name"] = person.get("last_name", "")
        row["dm_full_name"] = person.get("full_name", "")
        row["dm_title"] = _current_title([], person.get("title", ""))
        row["dm_linkedin_url"] = person.get("linkedin_url", "")
        row["dm_email"] = email
        row["dm_email_source"] = source
        row["dm_email_verified"] = verified
        row["mailtester_code"] = mailtester_code
        row["mailtester_message"] = mailtester_message
        row["dm_phone"] = phone
        row["dm_headline"] = person.get("headline", "")
        row["dm_location_city"] = person.get("location_city", "")
        row["dm_location_country"] = person.get("location_country", "")
        row["dm_icp_tier"] = str(person.get("icp_tier", ""))
        row["row_status"] = STATUS_ENRICHED if email else STATUS_NO_CONTACTS
        output_rows.append(row)

    return output_rows


def _get_job_level(title: str) -> str:
    """Map title to job level for output column."""
    title_lower = title.lower()
    if any(t in title_lower for t in ["owner", "ceo", "founder", "co-founder", "president"]):
        return "owner"
    elif any(t in title_lower for t in ["chief", "vp ", "vice president"]):
        return "vp"
    elif "director" in title_lower or "head of" in title_lower:
        return "director"
    elif any(t in title_lower for t in ["manager", "lead", "head"]):
        return "manager"
    return "other"


async def run_unified_linkedin_enrichment(
    rows: list[dict[str, Any]],
    personal_col: Optional[str] = None,
    company_col: Optional[str] = None,
    max_dms: int = 5,
    include_company: bool = True,
    on_progress: Optional[Callable[[dict[str, Any]], None]] = None,
    collector: Optional[Any] = None,  # RawContactCollector; Phase 1 capture
    record_provider_use: Optional[Callable[[str], None]] = None,
) -> list[OutputRow]:
    """
    Unified enrichment for CSV with personal and/or company LinkedIn URLs.

    Args:
        rows: List of input rows from CSV
        personal_col: Column name containing personal LinkedIn URLs (optional)
        company_col: Column name containing company LinkedIn URLs (optional)
        max_dms: Max decision makers to return from company waterfall (default 5)
        include_company: Include company details in output
        on_progress: Callback for progress updates
        collector: Optional ``RawContactCollector``. When provided, every
            Blitz response from the company waterfall is captured.

    Returns:
        List of enriched output rows (can be > len(rows) due to waterfall expansion)
    """
    semaphore = asyncio.Semaphore(LINKEDIN_CONCURRENCY)
    blitz_http = httpx.AsyncClient()
    contacts_http = httpx.AsyncClient()

    total = len(rows)

    async def _emit(idx: int, status: str, emails_count: int, source_counts: dict[str, int]) -> None:
        """Fire on_progress with the keys append_event expects (emails_found count)."""
        if not on_progress:
            return
        event = {
            "index": idx,
            "total": total,
            "status": status,
            "emails_found": emails_count,
            "source_counts": source_counts,
        }
        try:
            if asyncio.iscoroutinefunction(on_progress):
                await on_progress(event)
            else:
                on_progress(event)
        except Exception as prog_err:
            logger.warning("LinkedIn progress callback failed for row %s: %s", idx, prog_err)

    async def process_row(idx: int, row: dict[str, Any]) -> list[OutputRow]:
        personal_url = str(row.get(personal_col, "")).strip() if personal_col else ""
        company_url = str(row.get(company_col, "")).strip() if company_col else ""

        # No usable URL at all → genuinely skipped
        if not personal_url and not company_url:
            await _emit(idx, STATUS_SKIPPED, 0, {})
            return [{**row, **_empty_enriched(), "row_status": STATUS_SKIPPED}]

        results: list[OutputRow] = []
        found = False
        person_result: Optional[OutputRow] = None

        # Step 1: personal LinkedIn URL
        if personal_url and "linkedin.com" in personal_url:
            person_result = await _enrich_single_linkedin(
                blitz_http, contacts_http, row, personal_url,
                include_company=include_company, semaphore=semaphore,
                record_provider_use=record_provider_use,
            )
            if person_result.get("dm_email") or person_result.get("row_status") == STATUS_ENRICHED:
                results.append(person_result)
                found = True
                has_email = bool(person_result.get("dm_email"))
                source = person_result.get("dm_email_source", "")
                provider = _normalize_source(source) if source else "unknown"
                await _emit(
                    idx, STATUS_ENRICHED, 1 if has_email else 0,
                    {provider: 1} if has_email else {},
                )
            # else: NO_CONTACTS or NOT_FOUND — keep person_result for Step 3

        # Step 2: company LinkedIn waterfall fallback
        if not found and company_url and "linkedin.com" in company_url:
            # Blitz title-waterfall
            if record_provider_use:
                record_provider_use("blitz")
            company_dms = await _enrich_by_company_waterfall(
                blitz_http, company_url, DEFAULT_CASCADE, max_dms, semaphore,
                collector=collector,
            )
            if company_dms:
                for dm in company_dms:
                    output_row = {
                        **row,
                        "dm_first_name": dm.get("first_name", ""),
                        "dm_last_name": dm.get("last_name", ""),
                        "dm_full_name": dm.get("full_name", ""),
                        "dm_title": dm.get("title", ""),
                        "dm_job_level": dm.get("job_level", ""),
                        "dm_linkedin_url": dm.get("linkedin_url", ""),
                        "dm_email": dm.get("email", ""),
                        "dm_email_verified": "yes" if dm.get("verified_email") else "no",
                        "dm_headline": dm.get("headline", ""),
                        "dm_location_city": dm.get("location_city", ""),
                        "dm_location_country": dm.get("location_country", ""),
                        "dm_icp_tier": dm.get("icp_tier", ""),
                        "company_linkedin_url": company_url,
                        "row_status": STATUS_ENRICHED if dm.get("email") else STATUS_NO_CONTACTS,
                        "dm_email_source": SOURCE_BLITZ_COMPANY,
                    }
                    if include_company:
                        output_row["company_name"] = _extract_company_name_from_url(company_url)
                    results.append(output_row)
                found = True
                emails_count = sum(1 for dm in company_dms if dm.get("email"))
                await _emit(
                    idx, STATUS_ENRICHED, emails_count,
                    {"blitz": emails_count} if emails_count else {},
                )
            else:
                nr = {**row, **_empty_enriched(), "row_status": STATUS_NOT_FOUND}
                nr["company_linkedin_url"] = company_url
                results.append(nr)
                found = True
                await _emit(idx, STATUS_NOT_FOUND, 0, {})

        # Step 3: URL present but no data found
        if not found:
            if person_result is not None:
                # Preserve provider detail; status is already NOT_FOUND/NO_CONTACTS.
                results.append(person_result)
                await _emit(idx, person_result.get("row_status", STATUS_NOT_FOUND), 0, {})
            else:
                # Personal URL wasn't a linkedin.com URL and no company URL
                results.append({**row, **_empty_enriched(), "row_status": STATUS_SKIPPED})
                await _emit(idx, STATUS_SKIPPED, 0, {})

        return results

    # Process in batches for real concurrency (mirrors run_domain_enrichment).
    # Previously this was a sequential for…await loop (effective concurrency 1),
    # making LinkedIn enrichment ~15–25x slower than domain enrichment.
    BATCH_SIZE = 50
    all_output: list[OutputRow] = []
    for batch_start in range(0, total, BATCH_SIZE):
        batch_rows = rows[batch_start:batch_start + BATCH_SIZE]
        tasks = [process_row(batch_start + i, row) for i, row in enumerate(batch_rows)]
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in batch_results:
            if isinstance(result, Exception):
                logger.error("LinkedIn row processing failed: %s", result)
            else:
                all_output.extend(result)

    await blitz_http.aclose()
    await contacts_http.aclose()

    return all_output


def _extract_company_name_from_url(url: str) -> str:
    """Extract company name from LinkedIn company URL."""
    if not url:
        return ""
    # URL format: https://www.linkedin.com/company/acme-corp
    parts = url.split("/company/")
    if len(parts) > 1:
        name = parts[-1].rstrip("/")
        # Decode URL encoding
        return urllib.parse.unquote(name.replace("-", " ").replace("_", " ").title())
    return ""
