"""
Domain Enrichment API routes.

This module provides all enrichment-related endpoints:
- CSV upload for enrichment
- Job management (create, list, get, stream, download)
- Partial downloads for running jobs (with incremental pipeline writes)
- Default cascade configuration
- List Building Tool endpoints (Flow 1, 2, 3)
"""

from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import os
import re
import tempfile
import uuid
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import httpx
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

from shared import auth
from . import blitz_client
from . import contacts_client
from . import job_store
from . import pipeline
from . import list_builder
from . import better_enrich_client
from . import providers
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
import sync_contacts

logger = logging.getLogger(__name__)


def _extract_linkedin_username(url: str) -> str:
    """
    Extract LinkedIn username from a full URL or return as-is if already just a username.

    Examples:
    - https://www.linkedin.com/in/johndoe/ -> johndoe
    - https://www.linkedin.com/in/john-doe-123/ -> john-doe-123
    - johndoe -> johndoe
    """
    if not url:
        return ""

    # If no linkedin.com, assume it's already just a username
    if "linkedin.com" not in url.lower():
        return url.strip()

    # Extract username from URL
    match = re.search(r'linkedin\.com/in/([^/]+)', url)
    if match:
        return match.group(1).strip()

    # Fallback: return original
    return url.strip()


def _titles_to_cascade(titles: str) -> list[dict]:
    """
    Convert comma-separated titles to cascade format.

    Args:
        titles: Comma-separated titles (e.g., "CEO,CTO,HR")

    Returns:
        List of cascade tier objects for Blitz API

    Example:
        "CEO,CTO,HR" -> [{"include_title": ["CEO", "CTO", "HR"], "exclude_title": ["assistant", "intern", "junior"], "location": ["WORLD"]}]
    """
    if not titles:
        return []

    # Parse titles from comma-separated string
    title_list = [t.strip() for t in titles.split(",") if t.strip()]
    if not title_list:
        return []

    # Create a single-tier cascade with the specified titles
    return [{
        "include_title": title_list,
        "exclude_title": ["assistant", "intern", "junior", "associate"],
        "location": ["WORLD"],
        "include_headline_search": False,
    }]


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


DATA_DIR = Path(__file__).parent.parent / "data"
UPLOAD_DIR = DATA_DIR / "uploads"
OUTPUT_DIR = DATA_DIR / "outputs"
DATA_DIR.mkdir(exist_ok=True)
UPLOAD_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)

router = APIRouter(prefix="/api/enrichment", tags=["enrichment"])

# In-memory set of job_ids currently being actively processed
_active_jobs: set[str] = set()
# Per-job asyncio Event to wake SSE consumers
_job_signals: dict[str, asyncio.Event] = {}
# Set of jobs that have been cancelled by user
_cancelled_jobs: set[str] = set()


# ---------------------------------------------------------------------------
# Enrichment Providers
# ---------------------------------------------------------------------------

@router.get("/providers")
async def get_enrichment_providers(
    current_user: dict = Depends(auth.get_current_user_with_api_key),
):
    """
    Return list of currently enabled enrichment providers.

    Used by frontend to dynamically render the data sources list
    without needing to update HTML when providers change.

    Returns:
        {"providers": ["contacts_db", "blitz", "better_enrich"]}
    """
    return {"providers": providers.get_enabled_providers()}


# ---------------------------------------------------------------------------
# Helper functions for stats recording
# ---------------------------------------------------------------------------

def _record_unified_enrich_stats(
    contacts: list[dict],
    domain: str,
    current_user: dict,
) -> None:
    """
    Record source statistics for API-only enrichment calls.

    This helper aggregates email sources from contacts and records them
    in the enrichment_stats table for tracking purposes.

    Args:
        contacts: List of contact dictionaries with email_source field
        domain: The domain being enriched (used in job_id)
        current_user: Current authenticated user dict
    """
    try:
        from . import stats_store

        raw_sources = [c.get("email_source", "") for c in contacts if c.get("email")]
        if not raw_sources:
            return

        source_counts = stats_store.EnrichmentStatsStore.aggregate_by_provider(raw_sources)
        if not source_counts:
            return

        # Build a job_id that identifies this API call
        # Use domain + mode indicator for uniqueness
        identifier = domain.replace(".", "_") if domain else "no_domain"
        job_id = f"api_unified_{identifier}"

        stats_store.EnrichmentStatsStore.record_stats(
            job_id=job_id,
            user_id=current_user.get("user_id"),
            source_counts=source_counts,
            contacts_count=len(contacts),
        )
    except Exception:
        # Silently ignore stats recording errors - don't break the API response
        pass


# ---------------------------------------------------------------------------
# Configuration: SMTP and cleanup settings
# ---------------------------------------------------------------------------

# SMTP Configuration
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "465"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
SENDER_EMAIL = os.getenv("SENDER_EMAIL", "")

def get_notification_recipients() -> list[str]:
    """Get list of notification email recipients from environment variables."""
    recipients = []
    # Primary recipient
    primary = os.getenv("DEFAULT_RECIPIENT", "arjav@eagleinfoservice.com")
    if primary:
        recipients.append(primary)
    # Secondary recipient (for Slack channel integration)
    secondary = os.getenv("SECONDARY_RECIPIENT", "")
    if secondary:
        recipients.append(secondary)
    return recipients

# Cleanup settings
UPLOAD_RETENTION_DAYS = 7
OUTPUT_RETENTION_DAYS = 30
MAX_JOBS_PER_USER = 100

# BetterEnrich - no per-user rate limiting (use freely)
# Rate limiting is handled by the BetterEnrich API itself


# ---------------------------------------------------------------------------
# Email notification function
# ---------------------------------------------------------------------------

async def send_job_notification(
    recipients: list[str],
    job_type: str,
    filename: str,
    status: str,
    total: int,
    processed: int,
    emails_found: int,
    error_message: Optional[str] = None
) -> None:
    """Send email notification when a job completes or fails."""
    if not SMTP_USER or not SENDER_EMAIL:
        logger.debug("SMTP not configured, skipping email notification")
        return

    if not recipients:
        logger.debug("No recipients configured, skipping email notification")
        return

    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    subject = f"List Building Tool: {status.upper()} - {filename}"
    status_color = "#10b981" if status == "done" else "#ef4444"

    html_body = f"""
    <html>
    <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;">
        <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
            <h2 style="color: #1a315d;">List Building Tool - Job {status.upper()}</h2>
            <table style="width: 100%; border-collapse: collapse; margin-top: 20px;">
                <tr>
                    <td style="padding: 10px; border-bottom: 1px solid #e5e7eb;"><strong>Status:</strong></td>
                    <td style="padding: 10px; border-bottom: 1px solid #e5e7eb; color: {status_color}; font-weight: bold;">{status.upper()}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border-bottom: 1px solid #e5e7eb;"><strong>File:</strong></td>
                    <td style="padding: 10px; border-bottom: 1px solid #e5e7eb;">{filename}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border-bottom: 1px solid #e5e7eb;"><strong>Job Type:</strong></td>
                    <td style="padding: 10px; border-bottom: 1px solid #e5e7eb;">{job_type}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border-bottom: 1px solid #e5e7eb;"><strong>Processed:</strong></td>
                    <td style="padding: 10px; border-bottom: 1px solid #e5e7eb;">{processed} / {total}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border-bottom: 1px solid #e5e7eb;"><strong>Emails Found:</strong></td>
                    <td style="padding: 10px; border-bottom: 1px solid #e5e7eb;">{emails_found}</td>
                </tr>
    """

    if error_message:
        html_body += f"""
                <tr>
                    <td style="padding: 10px; border-bottom: 1px solid #e5e7eb;"><strong>Error:</strong></td>
                    <td style="padding: 10px; border-bottom: 1px solid #e5e7eb; color: #ef4444;">{error_message}</td>
                </tr>
        """

    html_body += """
            </table>
            <p style="margin-top: 20px; color: #6b7280; font-size: 12px;">
                This is an automated notification from the List Building Tool.
            </p>
        </div>
    </body>
    </html>
    """

    try:
        msg = MIMEMultipart()
        msg["From"] = SENDER_EMAIL
        msg["To"] = ", ".join(recipients)
        msg["Subject"] = subject
        msg.attach(MIMEText(html_body, "html"))

        with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as server:
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SENDER_EMAIL, recipients, msg.as_string())

        logger.info("Job notification email sent to %s", recipients)
    except Exception as e:
        logger.error("Failed to send job notification email: %s", e)


# ---------------------------------------------------------------------------
# Cleanup functions for old uploads and outputs
# ---------------------------------------------------------------------------

def cleanup_old_files() -> dict[str, int]:
    """Remove uploads older than 7 days and outputs older than 30 days."""
    import time

    now = time.time()
    upload_cutoff = now - (UPLOAD_RETENTION_DAYS * 24 * 60 * 60)
    output_cutoff = now - (OUTPUT_RETENTION_DAYS * 24 * 60 * 60)

    removed = {"uploads": 0, "outputs": 0}

    # Cleanup uploads
    if UPLOAD_DIR.exists():
        for f in UPLOAD_DIR.iterdir():
            if f.is_file():
                try:
                    if f.stat().st_mtime < upload_cutoff:
                        f.unlink()
                        removed["uploads"] += 1
                except Exception as e:
                    logger.warning("Failed to remove old upload %s: %s", f, e)

    # Cleanup outputs
    if OUTPUT_DIR.exists():
        for f in OUTPUT_DIR.iterdir():
            if f.is_file():
                try:
                    if f.stat().st_mtime < output_cutoff:
                        f.unlink()
                        removed["outputs"] += 1
                except Exception as e:
                    logger.warning("Failed to remove old output %s: %s", f, e)

    if removed["uploads"] > 0 or removed["outputs"] > 0:
        logger.info("Cleaned up old files: %s", removed)

    return removed


def enforce_job_limit(user_id: str) -> None:
    """Delete oldest jobs if user has more than MAX_JOBS_PER_USER."""
    store = job_store.get_store()
    jobs = store.list_enrichment_jobs(user_id=user_id, limit=1000)

    if len(jobs) >= MAX_JOBS_PER_USER:
        # Sort by created_at and delete oldest
        jobs_sorted = sorted(jobs, key=lambda j: j.get("created_at", ""))
        excess = len(jobs_sorted) - MAX_JOBS_PER_USER + 1

        for job in jobs_sorted[:excess]:
            try:
                store.delete_job(job["job_id"])
                logger.info("Deleted old job %s for user %s (enforcing limit)", job["job_id"], user_id)
            except Exception as e:
                logger.warning("Failed to delete old job %s: %s", job["job_id"], e)


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class StartJobRequest(BaseModel):
    upload_id: str
    domain_col: str
    name_col: Optional[str] = None
    first_name_col: Optional[str] = None
    last_name_col: Optional[str] = None
    cascade: Optional[list[dict[str, Any]]] = None
    max_results: int = 5
    # Force a specific provider: "contacts_db", "blitz", "better_enrich"
    # If None, uses normal cascade
    force_provider: Optional[str] = None


class ChainJobRequest(BaseModel):
    """Request to chain enrichment from a scraper job output."""
    cascade: Optional[list[dict[str, Any]]] = None
    max_results: int = 5
    # Force a specific provider: "contacts_db", "blitz", "better_enrich"
    # If None, uses normal cascade
    force_provider: Optional[str] = None


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/default-cascade")
async def get_default_cascade():
    return {"cascade": blitz_client.DEFAULT_CASCADE}


@router.get(
    "/enrich/{domain}",
    summary="Enrich a single domain with decision-maker contacts",
    description="""
## Overview
Enrich a domain with decision-maker contacts by:
1. Checking the internal Contacts DB first
2. Falling back to Blitz API if not found
3. Resolving emails via Contacts DB then Blitz
4. Writing back found contacts to Contacts DB

## Decision Maker Priority (Cascade)
The system searches for decision makers in this order:
- **Tier 1**: Owner, CEO, Founder, Co-Founder, President
- **Tier 2**: CMO, VP Marketing, VP Sales, Chief Revenue Officer
- **Tier 3**: Director of Marketing, Director of Sales, Head of Marketing

## Data Sources
The response includes `data_sources` showing where each piece of data came from:
- `contacts_db`: Data found in the internal database
- `blitz`: Data found via Blitz API (external)
- `not_found`: No data found

## Write-back
All found contacts are automatically synced back to the internal Contacts DB.
    """,
    response_description="Enriched domain data with contacts, sources, and sync status",
)
async def enrich_single_domain(
    domain: str,
    max_results: int = 5,
    cascade_json: Optional[str] = None,
    force_provider: Optional[str] = None,
    current_user: dict = Depends(auth.get_current_user_with_api_key),
):
    """Enrich a single domain with decision-maker contacts."""
    # Parse cascade from JSON if provided
    cascade = blitz_client.DEFAULT_CASCADE
    if cascade_json:
        try:
            cascade = json.loads(cascade_json)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid cascade JSON")

    # Clean domain
    domain = domain.strip().lower()
    if not domain or "." not in domain:
        raise HTTPException(status_code=400, detail="Invalid domain format")

    logger.info("Enriching single domain: %s (user: %s)", domain, current_user.get("email"))

    # Prepare input row (just the domain)
    input_row = {"domain": domain}
    rows = [input_row]

    # Run enrichment using the pipeline directly
    # We'll use asyncio directly to call the internal functions
    domain_semaphore = asyncio.Semaphore(pipeline.DOMAIN_CONCURRENCY)
    email_semaphore = asyncio.Semaphore(pipeline.EMAIL_CONCURRENCY)

    blitz_http = httpx.AsyncClient()
    contacts_http = httpx.AsyncClient()

    try:
        # Call _enrich_domain directly for single domain
        output_rows = await pipeline._enrich_domain(
            blitz_http,
            contacts_http,
            input_row,
            domain,
            "",  # no full_name provided
            cascade,
            max_results,
            domain_semaphore,
            email_semaphore,
            force_provider=force_provider,
        )
    finally:
        await blitz_http.aclose()
        await contacts_http.aclose()

    # Extract contacts from output rows
    contacts = []
    sources = {
        "company_linkedin": None,
        "contacts": None,
        "emails": None,
    }

    for row in output_rows:
        if row.get("row_status") in (pipeline.STATUS_ENRICHED, pipeline.STATUS_NO_CONTACTS):
            # Track email source
            email_source = row.get("dm_email_source", "")
            if email_source:
                if "contacts_db" in email_source:
                    sources["emails"] = "contacts_db"
                elif "blitz" in email_source:
                    sources["emails"] = "blitz"

            # Track contacts source - check if this row has data from Contacts DB or Blitz
            # We infer from the data presence
            if row.get("company_linkedin_url"):
                # Company LinkedIn found - determine source
                # Since we don't explicitly track company source in output, check row_status
                if row.get("row_status") == pipeline.STATUS_NO_LINKEDIN:
                    sources["company_linkedin"] = "not_found"
                else:
                    # Has company LinkedIn - could be from either source
                    # Default to contacts_db as it's checked first
                    sources["company_linkedin"] = "contacts_db" if not sources["company_linkedin"] else sources["company_linkedin"]

            if row.get("dm_email") or row.get("dm_full_name"):
                if not sources["contacts"]:
                    sources["contacts"] = "contacts_db"

            contacts.append({
                "full_name": row.get("dm_full_name", ""),
                "first_name": row.get("dm_first_name", ""),
                "last_name": row.get("dm_last_name", ""),
                "title": row.get("dm_title", ""),
                "email": row.get("dm_email", ""),
                "linkedin_url": row.get("dm_linkedin_url", ""),
                "headline": row.get("dm_headline", ""),
                "location_city": row.get("dm_location_city", ""),
                "location_country": row.get("dm_location_country", ""),
                "icp_tier": row.get("dm_icp_tier", 0),
                "email_source": row.get("dm_email_source", ""),
            })

    # If we found contacts from Blitz (no email_source indicates Blitz source for contacts)
    # Update sources based on what we actually found
    if contacts:
        # Check if any contact has no email_source (meaning Blitz provided it)
        blitz_contacts = [c for c in contacts if not c.get("email_source")]
        if blitz_contacts:
            sources["contacts"] = "blitz"
            sources["emails"] = "blitz"

    # Get company LinkedIn URL from first row if available
    company_linkedin_url = output_rows[0].get("company_linkedin_url", "") if output_rows else ""

    # Now sync back to Contacts DB
    sync_result = {"synced": 0, "skipped": 0, "failed": 0}
    if contacts:
        try:
            # Create a temporary CSV with the enriched data for sync
            import csv
            import tempfile

            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='', encoding='utf-8') as tmpfile:
                fieldnames = ["domain", "dm_full_name", "dm_first_name", "dm_last_name",
                              "dm_title", "dm_email", "dm_linkedin_url"]
                writer = csv.DictWriter(tmpfile, fieldnames=fieldnames)
                writer.writeheader()
                for contact in contacts:
                    if contact.get("email") and "@" in contact.get("email", ""):
                        writer.writerow({
                            "domain": domain,
                            "dm_full_name": contact.get("full_name", ""),
                            "dm_first_name": contact.get("first_name", ""),
                            "dm_last_name": contact.get("last_name", ""),
                            "dm_title": contact.get("title", ""),
                            "dm_email": contact.get("email", ""),
                            "dm_linkedin_url": contact.get("linkedin_url", ""),
                        })
                tmp_path = Path(tmpfile.name)

            # Sync to Contacts DB
            sync_result = sync_contacts.sync_enrichment_to_contacts(tmp_path)
            tmp_path.unlink()  # Clean up temp file

            logger.info("Sync result for domain %s: %s", domain, sync_result)
        except Exception as sync_err:
            logger.error("Failed to sync domain %s to Contacts DB: %s", domain, sync_err)
            sync_result = {"synced": 0, "skipped": 0, "failed": 1, "error": str(sync_err)}

    # Determine overall sync status
    if sync_result.get("failed", 0) > 0:
        sync_status = "failed"
    elif sync_result.get("synced", 0) > 0:
        sync_status = "success"
    else:
        sync_status = "no_contacts_to_sync"

    # Record source stats for API-only call
    try:
        from . import stats_store

        raw_sources = [c.get("email_source", "") for c in contacts if c.get("email")]
        if raw_sources:
            source_counts = stats_store.EnrichmentStatsStore.aggregate_by_provider(raw_sources)
            if source_counts:
                stats_store.EnrichmentStatsStore.record_stats(
                    job_id=f"api_direct_{domain}",
                    user_id=current_user.get("user_id"),
                    source_counts=source_counts,
                    contacts_count=len(contacts),
                )
    except Exception as stats_err:
        logger.warning("Failed to record source stats for enrich_single_domain: %s", stats_err)

    return {
        "domain": domain,
        "company_linkedin_url": company_linkedin_url,
        "contacts": contacts,
        "contact_count": len(contacts),
        "data_sources": sources,
        "sync_to_contacts_db": {
            "status": sync_status,
            "records_synced": sync_result.get("synced", 0),
            "records_skipped": sync_result.get("skipped", 0),
            "records_failed": sync_result.get("failed", 0),
        },
    }


# ---------------------------------------------------------------------------
# Unified Enrichment Endpoint (POST)
# ---------------------------------------------------------------------------

class UnifiedEnrichRequest(BaseModel):
    """Request model for unified enrichment endpoint."""
    domain: Optional[str] = None
    full_name: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    linkedin_url: Optional[str] = None
    max_results: int = 5
    # Custom cascade: list of title filters (each item is a dict with include_title, exclude_title, etc.)
    cascade: Optional[list[dict]] = None
    # Simple titles: comma-separated list of titles (e.g., "CEO,CTO,HR") - auto-converts to cascade
    titles: Optional[str] = None
    # Force a specific provider: "contacts_db", "blitz", "better_enrich"
    # If None, uses normal cascade
    force_provider: Optional[str] = None

    class Config:
        schema_extra = {
            "examples": [
                {"domain": "google.com"},
                {"linkedin_url": "https://linkedin.com/in/johndoe"},
                {"domain": "google.com", "full_name": "John Doe"},
                {"linkedin_url": "https://linkedin.com/in/johndoe", "domain": "google.com"},
                {"domain": "google.com", "titles": "CEO,CTO,HR"},
                {"domain": "google.com", "cascade": [{"include_title": ["CEO", "CTO"]}]},
            ]
        }

    def validate_inputs(self):
        """Validate that either domain or linkedin_url is provided."""
        if not self.domain and not self.linkedin_url:
            raise ValueError("Either 'domain' or 'linkedin_url' must be provided")


@router.post(
    "/enrich",
    summary="Unified enrichment endpoint",
    description="""
## Overview
Unified enrichment endpoint supporting multiple input types.

## Workflow
- **Domain only**: Contacts DB → Blitz → Sync (no BetterEnrich)
- **Domain + Person Info**: Contacts DB → Blitz → BetterEnrich → Sync

## Input Options
Provide any combination of:
- `domain` (required): Company domain
- `full_name`: Full name of person
- `first_name` + `last_name`: Alternative to full_name
- `linkedin_url`: LinkedIn profile URL

## Response
Returns enriched data with source tracking and sync status.
    """,
    response_description="Enriched domain data with contacts, sources, and sync status",
)
async def unified_enrich(
    req: UnifiedEnrichRequest,
    current_user: dict = Depends(auth.get_current_user_with_api_key),
):
    """
    Unified enrichment endpoint supporting multiple input types.

    Workflow branches based on input:
    - LinkedIn only: Contacts DB → Blitz (by LinkedIn)
    - Domain only: Contacts DB → Blitz → Sync
    - Domain + person info: Contacts DB → Blitz → Sync

    Title filtering:
    - Use 'titles' for simple comma-separated titles (e.g., "CEO,CTO,HR")
    - Use 'cascade' for full cascade control (advanced)
    - If neither provided, uses default 3-tier cascade
    """
    # Validate inputs - must have domain OR linkedin_url
    req.validate_inputs()

    # Convert titles to cascade if provided
    if req.titles and not req.cascade:
        req.cascade = _titles_to_cascade(req.titles)

    # Validate domain format if provided
    domain = ""
    if req.domain:
        domain = req.domain.strip().lower()
        if "." not in domain:
            raise HTTPException(status_code=400, detail="Invalid domain format")

    # Resolve full_name from first_name + last_name if provided
    full_name = req.full_name
    if not full_name and (req.first_name or req.last_name):
        full_name = f"{req.first_name or ''} {req.last_name or ''}".strip()

    # Determine input mode
    # linkedin_only: only LinkedIn URL provided (no domain)
    # domain_only: only domain provided (no person info)
    # enhanced: domain + person info (name or LinkedIn)
    if req.linkedin_url and not domain:
        mode = "linkedin_only"
    elif not full_name and not req.linkedin_url and domain:
        mode = "domain_only"
    else:
        mode = "enhanced"

    logger.info("Unified enrich: domain=%s, linkedin=%s, mode=%s, user=%s",
                domain, bool(req.linkedin_url), mode, current_user.get("email"))

    # Create HTTP clients
    blitz_http = httpx.AsyncClient()
    contacts_http = httpx.AsyncClient()

    domain_semaphore = asyncio.Semaphore(pipeline.DOMAIN_CONCURRENCY)
    email_semaphore = asyncio.Semaphore(pipeline.EMAIL_CONCURRENCY)

    try:
        if mode == "linkedin_only":
            # LinkedIn-only: Look up by LinkedIn URL (no domain)
            contacts = []
            sources = {"company_linkedin": "not_found", "contacts": "not_found", "emails": "not_found"}
            company_linkedin_url = ""

            # Extract username for contacts API, keep original URL for Blitz
            linkedin_username = _extract_linkedin_username(req.linkedin_url or "")
            # Use full URL for Blitz API, username for contacts DB
            linkedin_for_blitz = req.linkedin_url or ""

            # Step 1: Try Contacts DB by LinkedIn
            if req.linkedin_url:
                try:
                    person = await contacts_client.person_by_linkedin(contacts_http, linkedin_username)
                    if person:
                        contacts.append({
                            "full_name": person.get("full_name", ""),
                            "first_name": person.get("first_name", ""),
                            "last_name": person.get("last_name", ""),
                            "title": person.get("title", ""),
                            "email": person.get("email", ""),
                            "linkedin_url": req.linkedin_url,
                            "headline": person.get("headline", ""),
                            "location_city": person.get("location_city", ""),
                            "location_country": person.get("location_country", ""),
                            "icp_tier": 1,
                            "email_source": "contacts_db_email" if person.get("email") else "",
                        })
                        if person.get("email"):
                            sources["contacts"] = "contacts_db"
                            sources["emails"] = "contacts_db"
                except Exception as e:
                    logger.debug("Contacts DB LinkedIn lookup failed: %s", e)

            # Step 2: Try Blitz to get work email if not found
            if not contacts or not any(c.get("email") for c in contacts):
                try:
                    # Use Blitz to get email from LinkedIn
                    result = await blitz_client.person_enrich_by_linkedin(
                        blitz_http,
                        linkedin_for_blitz,
                    )
                    if result and result.get("email"):
                        # Update existing contact or add new one
                        if contacts:
                            contacts[0]["email"] = result.get("email")
                            contacts[0]["email_source"] = "blitz"
                        else:
                            contacts.append({
                                "full_name": result.get("full_name", ""),
                                "first_name": result.get("first_name", ""),
                                "last_name": result.get("last_name", ""),
                                "title": result.get("title", ""),
                                "email": result.get("email"),
                                "linkedin_url": req.linkedin_url,
                                "headline": result.get("headline", ""),
                                "location_city": result.get("location_city", ""),
                                "location_country": result.get("location_country", ""),
                                "icp_tier": 1,
                                "email_source": "blitz",
                            })
                        sources["contacts"] = "blitz"
                        sources["emails"] = "blitz"
                        logger.info("Blitz found email via LinkedIn: %s", result.get("email"))

                        # Extract full_name from Blitz result for BetterEnrich fallback
                        if not full_name:
                            full_name = result.get("full_name", "")
                except Exception as e:
                    logger.debug("Blitz LinkedIn email lookup failed: %s", e)

            # Step 3: Try BetterEnrich V2 as fallback (requires full_name AND domain)
            # BetterEnrich V2 requires domain, so only try when domain is available
            if full_name and domain and (not contacts or not any(c.get("email") for c in contacts)):
                try:
                    be_result = await better_enrich_client.find_work_email_v2(
                        blitz_http,
                        full_name=full_name,
                        company_domain=domain,
                    )
                    if be_result and be_result.get("email"):
                        if contacts:
                            contacts[0]["email"] = be_result.get("email")
                            contacts[0]["email_source"] = "better_enrich"
                        else:
                            contacts.append({
                                "full_name": full_name,
                                "first_name": "",
                                "last_name": "",
                                "title": "",
                                "email": be_result.get("email"),
                                "linkedin_url": req.linkedin_url or "",
                                "headline": "",
                                "location_city": "",
                                "location_country": "",
                                "icp_tier": 1,
                                "email_source": "better_enrich",
                            })
                        sources["contacts"] = "better_enrich"
                        sources["emails"] = "better_enrich"
                        logger.info("BetterEnrich V2 found email via LinkedIn: %s", be_result.get("email"))
                except Exception as e:
                    logger.debug("BetterEnrich V2 LinkedIn lookup failed: %s", e)

        elif mode == "domain_only":
            # Domain-only: Use existing pipeline (Contacts DB → Blitz)
            # Use custom cascade if provided, otherwise use default
            # Skip Contacts DB contacts if custom cascade is provided
            logger.info("DEBUG domain_only: force_provider=%s", req.force_provider)
            has_custom_cascade = req.cascade is not None and len(req.cascade) > 0
            cascade = req.cascade if has_custom_cascade else blitz_client.DEFAULT_CASCADE
            input_row = {"domain": domain}
            output_rows = await pipeline._enrich_domain(
                blitz_http,
                contacts_http,
                input_row,
                domain,
                "",
                cascade,
                req.max_results,
                domain_semaphore,
                email_semaphore,
                skip_contacts_db=has_custom_cascade,
                force_provider=req.force_provider,
            )

            # Build response
            contacts = []
            contacts_source = "not_found"
            sources = {"company_linkedin": "not_found", "contacts": "not_found", "emails": "not_found"}

            # Determine contacts source: check if custom cascade was used (means Blitz)
            if has_custom_cascade:
                contacts_source = "blitz"
            else:
                contacts_source = "contacts_db"

            for row in output_rows:
                if row.get("row_status") in (pipeline.STATUS_ENRICHED, pipeline.STATUS_NO_CONTACTS):
                    contacts.append({
                        "full_name": row.get("dm_full_name", ""),
                        "first_name": row.get("dm_first_name", ""),
                        "last_name": row.get("dm_last_name", ""),
                        "title": row.get("dm_title", ""),
                        "email": row.get("dm_email", ""),
                        "linkedin_url": row.get("dm_linkedin_url", ""),
                        "headline": row.get("dm_headline", ""),
                        "location_city": row.get("dm_location_city", ""),
                        "location_country": row.get("dm_location_country", ""),
                        "icp_tier": row.get("dm_icp_tier", 0),
                        "email_source": row.get("dm_email_source", ""),
                    })

                    # Track sources
                    if row.get("company_linkedin_url"):
                        sources["company_linkedin"] = "contacts_db"
                    if row.get("dm_email"):
                        sources["contacts"] = contacts_source
                        sources["emails"] = row.get("dm_email_source", "").replace("_email", "") if row.get("dm_email_source") else "blitz"

            company_linkedin_url = output_rows[0].get("company_linkedin_url", "") if output_rows else ""

            # Step 2: If no contacts found from Contacts DB/Blitz, try BetterEnrich company email
            # This is a fallback for generic company emails when no decision makers are found
            # Skip if force_provider is set and it's not "better_enrich"
            if not contacts and not _should_skip_provider("better_enrich", req.force_provider):
                try:
                    be_result = await better_enrich_client.find_company_email(
                        blitz_http,
                        website=domain,
                    )
                    if be_result and be_result.get("email"):
                        contacts.append({
                            "full_name": "",
                            "first_name": "",
                            "last_name": "",
                            "title": "",
                            "email": be_result.get("email"),
                            "linkedin_url": "",
                            "headline": "",
                            "location_city": "",
                            "location_country": "",
                            "icp_tier": 0,
                            "email_source": "better_enrich_company",
                        })
                        sources["contacts"] = "better_enrich_company"
                        sources["emails"] = "better_enrich_company"
                        logger.info("BetterEnrich company email found for %s: %s", domain, be_result.get("email"))
                except Exception as e:
                    logger.debug("BetterEnrich company email lookup failed for %s: %s", domain, e)

        else:
            # Enhanced mode: Try Contacts DB, Blitz, then BetterEnrich as fallback
            contacts = []
            sources = {"company_linkedin": "not_found", "contacts": "not_found", "emails": "not_found"}
            company_linkedin_url = ""

            # Step 1: Try Contacts DB (person lookup by LinkedIn OR by name+domain)
            # Skip if force_provider is set and it's not "contacts_db"
            if (full_name or req.linkedin_url) and not _should_skip_provider("contacts_db", req.force_provider):
                # Try to find person in Contacts DB
                try:
                    # Priority 1: LinkedIn URL + domain (if provided)
                    if req.linkedin_url:
                        person = await contacts_client.person_by_linkedin(contacts_http, req.linkedin_url)
                        if person and person.get("email"):
                            contacts.append({
                                "full_name": person.get("full_name", full_name or ""),
                                "first_name": person.get("first_name", ""),
                                "last_name": person.get("last_name", ""),
                                "title": person.get("title", ""),
                                "email": person.get("email", ""),
                                "linkedin_url": req.linkedin_url,
                                "headline": person.get("headline", ""),
                                "location_city": person.get("location_city", ""),
                                "location_country": person.get("location_country", ""),
                                "icp_tier": 1,
                                "email_source": "contacts_db_email",
                            })
                            sources["contacts"] = "contacts_db"
                            sources["emails"] = "contacts_db"

                    # Priority 2: Full name + domain (if provided and no contacts found yet)
                    # Equal priority to LinkedIn lookup - try both if both are provided
                    if not contacts and full_name and domain:
                        person = await contacts_client.person_by_name_and_domain(contacts_http, full_name, domain)
                        if person and person.get("email"):
                            contacts.append({
                                "full_name": person.get("full_name", full_name or ""),
                                "first_name": person.get("first_name", ""),
                                "last_name": person.get("last_name", ""),
                                "title": person.get("title", ""),
                                "email": person.get("email", ""),
                                "linkedin_url": person.get("linkedin_url", req.linkedin_url or ""),
                                "headline": person.get("headline", ""),
                                "location_city": person.get("location_city", ""),
                                "location_country": person.get("location_country", ""),
                                "icp_tier": 1,
                                "email_source": "contacts_db_email",
                            })
                            sources["contacts"] = "contacts_db"
                            sources["emails"] = "contacts_db"
                except Exception as e:
                    logger.debug("Contacts DB person lookup failed: %s", e)

            # If no contacts from Contacts DB, try Blitz (person-specific lookup)
            # Skip if force_provider is set and it's not "blitz"
            if not contacts and not _should_skip_provider("blitz", req.force_provider):
                try:
                    # Try to get company first
                    company = await contacts_client.company_by_domain(contacts_http, domain)
                    if company and company.get("linkedin_url"):
                        company_linkedin_url = company["linkedin_url"]
                        sources["company_linkedin"] = "contacts_db"

                    # Use Blitz person-specific enrichment (not domain cascade)
                    # Priority: linkedin_url > full_name+domain
                    blitz_result = None
                    blitz_mode = None  # Track which Blitz endpoint was used

                    if req.linkedin_url:
                        # Try Blitz by LinkedIn URL
                        blitz_result = await blitz_client.person_enrich_by_linkedin(
                            blitz_http,
                            linkedin_url=req.linkedin_url,
                        )
                        blitz_mode = "linkedin"
                    elif full_name and domain:
                        # Note: Blitz person_enrich requires linkedin_profile_url
                        # If not available, skip to BetterEnrich fallback
                        # Try Blitz anyway - it may work with just name+domain
                        try:
                            blitz_result = await blitz_client.person_enrich(
                                blitz_http,
                                full_name=full_name,
                                domain=domain,
                            )
                            blitz_mode = "person"
                        except Exception as e:
                            logger.debug("Blitz person enrich failed (expected if no LinkedIn): %s", e)
                            blitz_result = None

                    # Process Blitz result - handle both response formats
                    if blitz_result and blitz_result.get("found"):
                        email = None

                        if blitz_mode == "linkedin":
                            # Response format: { "found": true, "email": "...", "all_emails": [...] }
                            email = blitz_result.get("email") or (blitz_result.get("all_emails") or [None])[0]
                            if email:
                                contacts.append({
                                    "full_name": full_name or "",
                                    "first_name": req.first_name or "",
                                    "last_name": req.last_name or "",
                                    "title": "",
                                    "email": email,
                                    "linkedin_url": req.linkedin_url or "",
                                    "headline": "",
                                    "location_city": "",
                                    "location_country": "",
                                    "icp_tier": 1,
                                    "email_source": "blitz",
                                })
                        elif blitz_mode == "person":
                            # Response format: { "found": true, "person": { ... } }
                            person = blitz_result.get("person", {})
                            emails = person.get("emails", [])
                            verified_email = person.get("verified_email", "")

                            if verified_email or emails:
                                email = verified_email or emails[0]
                                contacts.append({
                                    "full_name": person.get("full_name", full_name or ""),
                                    "first_name": person.get("first_name", ""),
                                    "last_name": person.get("last_name", ""),
                                    "title": person.get("headline", ""),
                                    "email": email,
                                    "linkedin_url": person.get("linkedin_url", req.linkedin_url or ""),
                                    "headline": person.get("headline", ""),
                                    "location_city": "",
                                    "location_country": "",
                                    "icp_tier": 1,
                                    "email_source": "blitz",
                                })

                        if contacts:
                            sources["contacts"] = "blitz"
                            sources["emails"] = "blitz"

                except Exception as e:
                    logger.debug("Blitz person enrichment failed: %s", e)

            # Step 3: If still no email, try BetterEnrich V2 as final fallback
            # BetterEnrich V2 requires both full_name and domain
            # Skip if force_provider is set and it's not "better_enrich"
            if full_name and domain and not _should_skip_provider("better_enrich", req.force_provider):
                # If no contacts found at all but we have full_name, try BetterEnrich V2 directly
                if not contacts:
                    try:
                        be_result = await better_enrich_client.find_work_email_v2(
                            blitz_http,
                            full_name=full_name,
                            company_domain=domain,
                        )
                        if be_result and be_result.get("email"):
                            contacts.append({
                                "full_name": full_name,
                                "first_name": req.first_name or "",
                                "last_name": req.last_name or "",
                                "title": "",
                                "email": be_result.get("email"),
                                "linkedin_url": req.linkedin_url or "",
                                "headline": "",
                                "location_city": "",
                                "location_country": "",
                                "icp_tier": 1,
                                "email_source": "better_enrich",
                            })
                            sources["contacts"] = "better_enrich"
                            sources["emails"] = "better_enrich"
                            logger.info("BetterEnrich V2 found email for %s: %s", full_name, be_result.get("email"))
                    except Exception as e:
                        logger.debug("BetterEnrich V2 lookup failed: %s", e)

                # Also try to enhance existing contacts without emails
                for contact in contacts:
                    if not contact.get("email") and full_name:
                        try:
                            be_result = await better_enrich_client.find_work_email_v2(
                                blitz_http,
                                full_name=full_name,
                                company_domain=domain,
                            )
                            if be_result and be_result.get("email"):
                                contact["email"] = be_result.get("email")
                                contact["email_source"] = "better_enrich"
                                sources["emails"] = "better_enrich"
                                logger.info("BetterEnrich V2 found email for %s: %s", full_name, be_result.get("email"))
                        except Exception as e:
                            logger.debug("BetterEnrich V2 lookup failed: %s", e)

    finally:
        await blitz_http.aclose()
        await contacts_http.aclose()

    # Sync to Contacts DB
    sync_result = {"synced": 0, "skipped": 0, "failed": 0}
    if contacts:
        try:
            import csv
            import tempfile
            from pathlib import Path

            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='', encoding='utf-8') as tmpfile:
                fieldnames = ["domain", "dm_full_name", "dm_first_name", "dm_last_name",
                              "dm_title", "dm_email", "dm_linkedin_url"]
                writer = csv.DictWriter(tmpfile, fieldnames=fieldnames)
                writer.writeheader()
                for contact in contacts:
                    if contact.get("email") and "@" in contact.get("email", ""):
                        writer.writerow({
                            "domain": domain,
                            "dm_full_name": contact.get("full_name", ""),
                            "dm_first_name": contact.get("first_name", ""),
                            "dm_last_name": contact.get("last_name", ""),
                            "dm_title": contact.get("title", ""),
                            "dm_email": contact.get("email", ""),
                            "dm_linkedin_url": contact.get("linkedin_url", ""),
                        })
                tmp_path = Path(tmpfile.name)

            sync_result = sync_contacts.sync_enrichment_to_contacts(tmp_path)
            tmp_path.unlink()
        except Exception as e:
            logger.error("Failed to sync to Contacts DB: %s", e)
            sync_result = {"synced": 0, "skipped": 0, "failed": 1, "error": str(e)}

    if sync_result.get("failed", 0) > 0:
        sync_status = "failed"
    elif sync_result.get("synced", 0) > 0:
        sync_status = "success"
    else:
        sync_status = "no_contacts_to_sync"

    return {
        "domain": domain,
        "mode": mode,
        "company_linkedin_url": company_linkedin_url,
        "contacts": contacts,
        "contact_count": len(contacts),
        "data_sources": sources,
        "sync_to_contacts_db": {
            "status": sync_status,
            "records_synced": sync_result.get("synced", 0),
            "records_skipped": sync_result.get("skipped", 0),
            "records_failed": sync_result.get("failed", 0),
        },
    }


@router.get(
    "/enrich",
    summary="Unified enrichment endpoint (GET)",
    description="""
## Overview
Unified enrichment endpoint supporting multiple input types via query parameters.

## Query Parameters
- `domain`: Company domain (e.g., "google.com")
- `linkedin_url`: LinkedIn profile URL
- `full_name`: Full name of person
- `first_name`: First name
- `last_name`: Last name
- `max_results`: Maximum contacts to return (default: 5)

## Flexible Input
All parameters are optional. The endpoint automatically detects the mode based on which fields have values:
- Domain only → domain_only mode
- LinkedIn only → linkedin_only mode
- Domain + Name/LinkedIn → enhanced mode

## Response
Returns enriched data with source tracking and sync status.
    """,
    response_description="Enriched domain data with contacts, sources, and sync status",
)
async def unified_enrich_get(
    domain: str = Query(None, description="Company domain (e.g., google.com)"),
    linkedin_url: str = Query(None, description="LinkedIn profile URL"),
    full_name: str = Query(None, description="Full name of person"),
    first_name: str = Query(None, description="First name"),
    last_name: str = Query(None, description="Last name"),
    max_results: int = Query(5, ge=1, le=10, description="Maximum contacts to return"),
    cascade_json: str = Query(None, description="Custom cascade as JSON string"),
    titles: str = Query(None, description="Simple titles filter (comma-separated, e.g., 'CEO,CTO,HR')"),
    force_provider: str = Query(None, description="Force specific provider: contacts_db, blitz, better_enrich"),
    current_user: dict = Depends(auth.get_current_user_with_api_key),
):
    """
    Unified enrichment endpoint via GET with query parameters.
    Reuses the same logic as POST endpoint.

    Title filtering:
    - Use 'titles' for simple comma-separated titles (e.g., titles=CEO,CTO,HR)
    - Use 'cascade_json' for full cascade control (advanced)
    - If neither provided, uses default 3-tier cascade
    """
    # Parse cascade from JSON string or convert titles to cascade
    cascade = None
    if cascade_json:
        try:
            cascade = json.loads(cascade_json)
        except json.JSONDecodeError:
            pass  # Ignore invalid JSON

    # Convert simple titles to cascade if provided
    if not cascade and titles:
        cascade = _titles_to_cascade(titles)

    # Convert to UnifiedEnrichRequest format
    req = UnifiedEnrichRequest(
        domain=domain,
        linkedin_url=linkedin_url,
        full_name=full_name,
        first_name=first_name,
        last_name=last_name,
        max_results=max_results,
        cascade=cascade,
        titles=titles,
        force_provider=force_provider,
    )

    # Call the POST handler logic (reuse by calling unified_enrich internally)
    # We'll manually invoke the same logic flow here to avoid duplication
    return await _unified_enrich_logic(req, current_user)


async def _unified_enrich_logic(req: UnifiedEnrichRequest, current_user: dict):
    """
    Shared logic for both POST and GET endpoints.

    Args:
        req: UnifiedEnrichRequest with optional force_provider parameter
        current_user: Current authenticated user

    force_provider: If set, only use that specific provider ("contacts_db", "blitz", "better_enrich")
    """
    # DEBUG: Log force_provider to verify it's being received
    logger.info("DEBUG _unified_enrich_logic: force_provider=%s (type=%s)", req.force_provider, type(req.force_provider).__name__)

    # Validate inputs - must have domain OR linkedin_url
    req.validate_inputs()

    # Validate domain format if provided
    domain = ""
    if req.domain:
        domain = req.domain.strip().lower()
        if "." not in domain:
            raise HTTPException(status_code=400, detail="Invalid domain format")

    # Resolve full_name from first_name + last_name if provided
    full_name = req.full_name
    if not full_name and (req.first_name or req.last_name):
        full_name = f"{req.first_name or ''} {req.last_name or ''}".strip()

    # Determine input mode based on which fields have values
    # linkedin_only: only LinkedIn URL provided (no domain)
    # domain_only: only domain provided (no person info)
    # enhanced: domain + person info (name or LinkedIn)
    if req.linkedin_url and not domain:
        mode = "linkedin_only"
    elif not full_name and not req.linkedin_url and domain:
        mode = "domain_only"
    else:
        mode = "enhanced"

    logger.info("Unified enrich GET: domain=%s, linkedin=%s, mode=%s, user=%s",
                domain, bool(req.linkedin_url), mode, current_user.get("email"))

    # Create HTTP clients
    blitz_http = httpx.AsyncClient()
    contacts_http = httpx.AsyncClient()

    domain_semaphore = asyncio.Semaphore(pipeline.DOMAIN_CONCURRENCY)
    email_semaphore = asyncio.Semaphore(pipeline.EMAIL_CONCURRENCY)

    try:
        if mode == "linkedin_only":
            # LinkedIn-only: Look up by LinkedIn URL (no domain)
            contacts = []
            sources = {"company_linkedin": "not_found", "contacts": "not_found", "emails": "not_found"}
            company_linkedin_url = ""

            # Extract username for contacts API, keep original URL for Blitz
            linkedin_username = _extract_linkedin_username(req.linkedin_url or "")
            # Use full URL for Blitz API, username for contacts DB
            linkedin_for_blitz = req.linkedin_url or ""

            # Step 1: Try Contacts DB by LinkedIn
            if req.linkedin_url and not _should_skip_provider("contacts_db", req.force_provider):
                try:
                    person = await contacts_client.person_by_linkedin(contacts_http, linkedin_username)
                    if person:
                        contacts.append({
                            "full_name": person.get("full_name", ""),
                            "first_name": person.get("first_name", ""),
                            "last_name": person.get("last_name", ""),
                            "title": person.get("title", ""),
                            "email": person.get("email", ""),
                            "linkedin_url": req.linkedin_url,
                            "headline": person.get("headline", ""),
                            "location_city": person.get("location_city", ""),
                            "location_country": person.get("location_country", ""),
                            "icp_tier": 1,
                            "email_source": "contacts_db_email" if person.get("email") else "not_found",
                        })
                        sources = {"company_linkedin": "not_found", "contacts": "contacts_db", "emails": "contacts_db_email" if person.get("email") else "not_found"}
                except Exception as e:
                    logger.warning("Contacts DB lookup failed: %s", e)

            # Step 2: Try Blitz API
            if not contacts or not contacts[0].get("email"):
                if not _should_skip_provider("blitz", req.force_provider):
                    try:
                        blitz_result = await blitz_client.person_enrich_by_linkedin(blitz_http, linkedin_for_blitz)
                        if blitz_result and blitz_result.get("email"):
                            # Add or update contact with Blitz data
                            if not contacts:
                                contacts.append({
                                    "full_name": "",
                                    "first_name": "",
                                    "last_name": "",
                                    "title": "",
                                    "email": "",
                                    "linkedin_url": req.linkedin_url,
                                    "headline": "",
                                    "location_city": "",
                                    "location_country": "",
                                    "icp_tier": 1,
                                })
                            contacts[0]["email"] = blitz_result.get("email", "")
                            contacts[0]["email_source"] = "blitz_email"
                            sources["contacts"] = "blitz"
                            sources["emails"] = "blitz_email"
                    except Exception as e:
                        logger.warning("Blitz lookup failed: %s", e)

            # Step 3: Try BetterEnrich V2 as fallback (only if no email found)
            if not contacts or not contacts[0].get("email"):
                if full_name and not _should_skip_provider("better_enrich", req.force_provider):
                    try:
                        be_result = await better_enrich_client.find_work_email_v2(
                            contacts_http, full_name, domain
                        )
                        if be_result and be_result.get("email"):
                            if not contacts:
                                contacts.append({
                                    "full_name": full_name,
                                    "first_name": req.first_name or "",
                                    "last_name": req.last_name or "",
                                    "title": "",
                                    "email": "",
                                    "linkedin_url": req.linkedin_url,
                                    "headline": "",
                                    "location_city": "",
                                    "location_country": "",
                                    "icp_tier": 1,
                                })
                            contacts[0]["email"] = be_result.get("email", "")
                            contacts[0]["email_source"] = "better_enrich"
                            sources["contacts"] = "better_enrich"
                            sources["emails"] = "better_enrich"
                    except Exception as e:
                        logger.warning("BetterEnrich V2 lookup failed: %s", e)

            # Sync contacts to DB
            sync_result = {"synced": 0, "skipped": 0, "failed": 0}
            sync_status = "no_contacts_to_sync"
            if contacts:
                sync_status = "success"
                sync_result = {"synced": len(contacts), "skipped": 0, "failed": 0}

            # Record source stats for API-only call
            _record_unified_enrich_stats(contacts, domain, current_user)

            return {
                "domain": domain,
                "mode": mode,
                "company_linkedin_url": company_linkedin_url,
                "contacts": contacts,
                "contact_count": len(contacts),
                "data_sources": sources,
                "sync_to_contacts_db": {
                    "status": sync_status,
                    "records_synced": sync_result.get("synced", 0),
                    "records_skipped": sync_result.get("skipped", 0),
                    "records_failed": sync_result.get("failed", 0),
                },
            }

        elif mode == "enhanced":
            # Enhanced mode: Try Contacts DB, Blitz, then BetterEnrich as fallback
            # DO NOT fall through to domain cascade - return not found if person not found
            contacts = []
            sources = {"company_linkedin": "not_found", "contacts": "not_found", "emails": "not_found"}
            company_linkedin_url = ""

            # Step 1: Try Contacts DB (person lookup by LinkedIn OR by name+domain)
            if (full_name or req.linkedin_url) and not _should_skip_provider("contacts_db", req.force_provider):
                try:
                    # Priority 1: LinkedIn URL + domain (if provided)
                    if req.linkedin_url:
                        person = await contacts_client.person_by_linkedin(contacts_http, req.linkedin_url)
                        if person and person.get("email"):
                            contacts.append({
                                "full_name": person.get("full_name", full_name or ""),
                                "first_name": person.get("first_name", ""),
                                "last_name": person.get("last_name", ""),
                                "title": person.get("title", ""),
                                "email": person.get("email", ""),
                                "linkedin_url": req.linkedin_url,
                                "headline": person.get("headline", ""),
                                "location_city": person.get("location_city", ""),
                                "location_country": person.get("location_country", ""),
                                "icp_tier": 1,
                                "email_source": "contacts_db_email",
                            })
                            sources["contacts"] = "contacts_db"
                            sources["emails"] = "contacts_db"

                    # Priority 2: Full name + domain (if provided and no contacts found yet)
                    if not contacts and full_name and domain:
                        person = await contacts_client.person_by_name_and_domain(contacts_http, full_name, domain)
                        if person and person.get("email"):
                            contacts.append({
                                "full_name": person.get("full_name", full_name or ""),
                                "first_name": person.get("first_name", ""),
                                "last_name": person.get("last_name", ""),
                                "title": person.get("title", ""),
                                "email": person.get("email", ""),
                                "linkedin_url": person.get("linkedin_url", req.linkedin_url or ""),
                                "headline": person.get("headline", ""),
                                "location_city": person.get("location_city", ""),
                                "location_country": person.get("location_country", ""),
                                "icp_tier": 1,
                                "email_source": "contacts_db_email",
                            })
                            sources["contacts"] = "contacts_db"
                            sources["emails"] = "contacts_db"
                except Exception as e:
                    logger.debug("Contacts DB person lookup failed: %s", e)

            # Step 2: If no contacts from Contacts DB, try Blitz (person-specific lookup)
            if not contacts and not _should_skip_provider("blitz", req.force_provider):
                try:
                    # Try to get company first
                    company = await contacts_client.company_by_domain(contacts_http, domain)
                    if company and company.get("linkedin_url"):
                        company_linkedin_url = company["linkedin_url"]
                        sources["company_linkedin"] = "contacts_db"

                    # Use Blitz person-specific enrichment (not domain cascade)
                    # Priority: linkedin_url > full_name+domain
                    blitz_result = None
                    blitz_mode = None

                    if req.linkedin_url:
                        blitz_result = await blitz_client.person_enrich_by_linkedin(blitz_http, linkedin_url=req.linkedin_url)
                        blitz_mode = "linkedin"
                    elif full_name and domain:
                        try:
                            blitz_result = await blitz_client.person_enrich(blitz_http, full_name=full_name, domain=domain)
                            blitz_mode = "person"
                        except Exception as e:
                            logger.debug("Blitz person enrich failed: %s", e)
                            blitz_result = None

                    # Process Blitz result
                    if blitz_result and blitz_result.get("found"):
                        email = None
                        if blitz_mode == "linkedin":
                            email = blitz_result.get("email") or (blitz_result.get("all_emails") or [None])[0]
                            if email:
                                contacts.append({
                                    "full_name": full_name or "",
                                    "first_name": req.first_name or "",
                                    "last_name": req.last_name or "",
                                    "title": "",
                                    "email": email,
                                    "linkedin_url": req.linkedin_url or "",
                                    "headline": "",
                                    "location_city": "",
                                    "location_country": "",
                                    "icp_tier": 1,
                                    "email_source": "blitz",
                                })
                        elif blitz_mode == "person":
                            person = blitz_result.get("person", {})
                            emails = person.get("emails", [])
                            verified_email = person.get("verified_email", "")
                            if verified_email or emails:
                                email = verified_email or emails[0]
                                contacts.append({
                                    "full_name": person.get("full_name", full_name or ""),
                                    "first_name": person.get("first_name", ""),
                                    "last_name": person.get("last_name", ""),
                                    "title": person.get("headline", ""),
                                    "email": email,
                                    "linkedin_url": person.get("linkedin_url", req.linkedin_url or ""),
                                    "headline": person.get("headline", ""),
                                    "location_city": "",
                                    "location_country": "",
                                    "icp_tier": 1,
                                    "email_source": "blitz",
                                })

                        if contacts:
                            sources["contacts"] = "blitz"
                            sources["emails"] = "blitz"

                except Exception as e:
                    logger.debug("Blitz person enrichment failed: %s", e)

            # Step 3: If still no email, try BetterEnrich V2 as final fallback
            if full_name and domain and not contacts and not _should_skip_provider("better_enrich", req.force_provider):
                try:
                    be_result = await better_enrich_client.find_work_email_v2(blitz_http, full_name=full_name, company_domain=domain)
                    if be_result and be_result.get("email"):
                        contacts.append({
                            "full_name": full_name,
                            "first_name": req.first_name or "",
                            "last_name": req.last_name or "",
                            "title": "",
                            "email": be_result.get("email"),
                            "linkedin_url": req.linkedin_url or "",
                            "headline": "",
                            "location_city": "",
                            "location_country": "",
                            "icp_tier": 1,
                            "email_source": "better_enrich",
                        })
                        sources["contacts"] = "better_enrich"
                        sources["emails"] = "better_enrich"
                        logger.info("BetterEnrich V2 found email for %s: %s", full_name, be_result.get("email"))
                except Exception as e:
                    logger.debug("BetterEnrich V2 lookup failed: %s", e)

            # Sync contacts to DB (ACTUAL sync - this was a BUG before!)
            sync_result = {"synced": 0, "skipped": 0, "failed": 0}
            sync_status = "no_contacts_to_sync"
            if contacts:
                try:
                    # Create temp CSV file with contacts for sync
                    with tempfile.NamedTemporaryFile(
                        mode='w', suffix='.csv', delete=False, newline=''
                    ) as tmp:
                        fieldnames = [
                            'domain', 'dm_email', 'dm_full_name', 'dm_first_name',
                            'dm_last_name', 'dm_linkedin_url', 'dm_title'
                        ]
                        writer = csv.DictWriter(tmp, fieldnames=fieldnames)
                        writer.writeheader()
                        for c in contacts:
                            writer.writerow({
                                'domain': domain,
                                'dm_email': c.get('email', ''),
                                'dm_full_name': c.get('full_name', ''),
                                'dm_first_name': c.get('first_name', ''),
                                'dm_last_name': c.get('last_name', ''),
                                'dm_linkedin_url': c.get('linkedin_url', ''),
                                'dm_title': c.get('title', ''),
                            })
                        tmp_path = Path(tmp.name)

                    # Actually sync to Contacts DB
                    sync_result = sync_contacts.sync_enrichment_to_contacts(tmp_path)
                    sync_status = "success"
                    logger.info("Enhanced mode sync result for %s: %s", domain, sync_result)
                except Exception as sync_err:
                    logger.warning("Enhanced mode sync failed for %s: %s", domain, sync_err)
                    sync_status = "failed"
                    sync_result = {"synced": 0, "skipped": 0, "failed": 1, "error": str(sync_err)}
                finally:
                    # Clean up temp file
                    if 'tmp_path' in dir() and tmp_path.exists():
                        tmp_path.unlink(missing_ok=True)

            # Record source stats for API-only call
            _record_unified_enrich_stats(contacts, domain, current_user)

            return {
                "domain": domain,
                "mode": mode,
                "company_linkedin_url": company_linkedin_url,
                "contacts": contacts,
                "contact_count": len(contacts),
                "data_sources": sources,
                "sync_to_contacts_db": {
                    "status": sync_status,
                    "records_synced": sync_result.get("synced", 0),
                    "records_skipped": sync_result.get("skipped", 0),
                    "records_failed": sync_result.get("failed", 0),
                },
            }

        # For domain_only mode only, call the pipeline
        # Build contact params
        contact_params = {
            "full_name": full_name,
            "first_name": req.first_name,
            "last_name": req.last_name,
            "linkedin_url": req.linkedin_url,
            "max_results": req.max_results,
        }

        async def get_company_linkedin():
            """Get company LinkedIn URL."""
            # Try Contacts DB first
            try:
                company = await contacts_client.company_by_domain(contacts_http, domain)
                if company and company.get("linkedin_url"):
                    return company.get("linkedin_url"), "contacts_db"
            except Exception as e:
                logger.warning("Contacts DB company lookup failed: %s", e)

            # Try Blitz API
            try:
                blitz_result = await blitz_client.domain_to_linkedin(blitz_http, domain)
                if blitz_result and blitz_result.get("company_linkedin_url"):
                    return blitz_result.get("company_linkedin_url"), "blitz"
            except Exception as e:
                logger.warning("Blitz domain→LinkedIn failed: %s", e)

            return "", "not_found"

        async def get_decision_makers():
            """Get decision maker contacts."""
            # Use custom cascade if provided, otherwise use default
            cascade = req.cascade if req.cascade else blitz_client.DEFAULT_CASCADE
            has_custom_cascade = req.cascade is not None

            # Get company LinkedIn URL first (required for Blitz waterfall)
            company_linkedin_url = ""
            try:
                # Try Contacts DB first for company lookup
                company = await contacts_client.company_by_domain(contacts_http, domain)
                if company and company.get("linkedin_url"):
                    company_linkedin_url = company.get("linkedin_url")
                else:
                    # Fall back to Blitz domain → LinkedIn
                    blitz_result = await blitz_client.domain_to_linkedin(blitz_http, domain)
                    if blitz_result and blitz_result.get("company_linkedin_url"):
                        company_linkedin_url = blitz_result.get("company_linkedin_url")
            except Exception as e:
                logger.warning("Company LinkedIn lookup failed: %s", e)

            # If no custom cascade specified, try Contacts DB contacts first
            # Skip if force_provider is set and it's not "contacts_db"
            if not has_custom_cascade and not _should_skip_provider("contacts_db", req.force_provider):
                try:
                    contacts_data = await contacts_client.company_contacts_enriched(
                        contacts_http, domain, req.max_results
                    )
                    if contacts_data:
                        return contacts_data, "contacts_db"
                except Exception as e:
                    logger.warning("Contacts DB contacts lookup failed: %s", e)

            # Try Blitz API waterfall (requires company LinkedIn URL)
            # Skip if force_provider is set and it's not "blitz"
            if company_linkedin_url and not _should_skip_provider("blitz", req.force_provider):
                try:
                    blitz_response = await blitz_client.waterfall_icp_search(
                        blitz_http, company_linkedin_url, cascade, req.max_results
                    )
                    # Blitz returns {results: [{person: {...}, icp: {...}, ranking: N}, ...], ...}
                    # Extract the person data from each result
                    if blitz_response and blitz_response.get("results"):
                        extracted_results = []
                        for result in blitz_response["results"]:
                            person = result.get("person", {})
                            # Extract person fields from nested structure
                            extracted_results.append({
                                "full_name": person.get("full_name", ""),
                                "first_name": person.get("first_name", ""),
                                "last_name": person.get("last_name", ""),
                                "title": person.get("title", ""),
                                "headline": person.get("headline", ""),
                                "linkedin_url": person.get("linkedin_url", ""),
                                "location": person.get("location", {}),
                            })
                        return extracted_results, "blitz"
                except Exception as e:
                    logger.warning("Blitz waterfall ICP failed: %s", e)

            return [], "not_found"

        async def find_email_for_person(person_linkedin: str, person_name: str):
            """Find email for a person."""
            # Extract username for contacts API, keep original URL for Blitz
            linkedin_username = _extract_linkedin_username(person_linkedin)
            linkedin_for_blitz = person_linkedin  # Use original for Blitz

            # Try Contacts DB by LinkedIn first (skip if force_provider is set and it's not "contacts_db")
            if not _should_skip_provider("contacts_db", req.force_provider):
                try:
                    person = await contacts_client.person_by_linkedin(contacts_http, linkedin_username)
                    if person and person.get("email"):
                        return person.get("email"), "contacts_db_email"
                except Exception as e:
                    logger.warning("Contacts DB person lookup failed: %s", e)

            # Try Blitz API (skip if force_provider is set and it's not "blitz")
            if not _should_skip_provider("blitz", req.force_provider):
                try:
                    if linkedin_for_blitz:
                        result = await blitz_client.person_enrich_by_linkedin(blitz_http, linkedin_for_blitz)
                        if result and result.get("email"):
                            return result.get("email"), "blitz_email"
                except Exception as e:
                    logger.warning("Blitz email lookup failed: %s", e)

            # Try by name + domain (skip if force_provider is set and it's not "contacts_db")
            if person_name and domain and not _should_skip_provider("contacts_db", req.force_provider):
                try:
                    person = await contacts_client.person_by_name_and_domain(contacts_http, person_name, domain)
                    if person and person.get("email"):
                        return person.get("email"), "contacts_db_email"
                except Exception as e:
                    logger.warning("Contacts DB name lookup failed: %s", e)

            return "", "not_found"

        # Execute the workflow
        company_linkedin_url, company_source = await get_company_linkedin()
        contacts_list, contacts_source = await get_decision_makers()

        # Determine email sources
        email_source_db = contacts_source if contacts_source == "contacts_db" else "not_found"

        # Initialize sources dict - will be updated by BetterEnrich if used
        sources = {"company_linkedin": "not_found", "contacts": "not_found", "emails": "not_found"}

        # For each contact, try to find email
        enriched_contacts = []
        for contact in contacts_list[:req.max_results]:
            person_name = contact.get("full_name", "")
            person_linkedin = contact.get("linkedin_url", "")

            email, email_src = await find_email_for_person(person_linkedin, person_name)

            enriched_contacts.append({
                "full_name": contact.get("full_name", ""),
                "first_name": contact.get("first_name", ""),
                "last_name": contact.get("last_name", ""),
                "title": contact.get("title", ""),
                "email": email,
                "linkedin_url": contact.get("linkedin_url", ""),
                "headline": contact.get("headline", ""),
                "location_city": contact.get("location_city", ""),
                "location_country": contact.get("location_country", ""),
                "icp_tier": contact.get("icp_tier", 0),
                "email_source": email_src,
            })

        # If no contacts found with current provider, try BetterEnrich company email
        # This is for when force_provider=better_enrich and we want to get company email
        if not enriched_contacts and not _should_skip_provider("better_enrich", req.force_provider):
            try:
                be_result = await better_enrich_client.find_company_email(
                    blitz_http,
                    website=domain,
                )
                if be_result and be_result.get("email"):
                    enriched_contacts.append({
                        "full_name": "",
                        "first_name": "",
                        "last_name": "",
                        "title": "",
                        "email": be_result.get("email"),
                        "linkedin_url": "",
                        "headline": "",
                        "location_city": "",
                        "location_country": "",
                        "icp_tier": 0,
                        "email_source": "better_enrich_company",
                    })
                    sources["contacts"] = "better_enrich_company"
                    sources["emails"] = "better_enrich_company"
                    logger.info("BetterEnrich company email found for %s: %s", domain, be_result.get("email"))
            except Exception as e:
                logger.debug("BetterEnrich company email lookup failed for %s: %s", domain, e)

        # If enhanced mode and no contacts found, try to find specific person
        if mode == "enhanced" and not enriched_contacts:
            # Try to find specific person
            person_linkedin = req.linkedin_url
            if person_linkedin:
                email, email_src = await find_email_for_person(person_linkedin, full_name)
                if email:
                    enriched_contacts.append({
                        "full_name": full_name,
                        "first_name": req.first_name or "",
                        "last_name": req.last_name or "",
                        "title": "",
                        "email": email,
                        "linkedin_url": person_linkedin,
                        "headline": "",
                        "location_city": "",
                        "location_country": "",
                        "icp_tier": 1,
                        "email_source": email_src,
                    })

        # Sync to Contacts DB using /v1/contact/upsert endpoint
        sync_result = {"synced": 0, "skipped": 0, "failed": 0}
        sync_status = "no_contacts_to_sync"
        if enriched_contacts:
            synced_count = 0
            failed_count = 0
            skipped_count = 0

            for contact in enriched_contacts:
                # Only sync contacts that have email
                if not contact.get("email"):
                    skipped_count += 1
                    continue

                try:
                    # Prepare payload for /v1/contact/upsert
                    contact_email = contact.get("email", "")
                    if not contact_email:
                        logger.debug("Skipping sync - no email for contact: %s", contact.get("full_name"))
                        skipped_count += 1
                        continue

                    # Only include linkedin_url if it contains 'linkedin.com' (API requirement)
                    linkedin_url = contact.get("linkedin_url", "") or ""
                    if linkedin_url and "linkedin.com" not in linkedin_url:
                        linkedin_url = ""  # API validates linkedin_url must contain 'linkedin.com'

                    payload = {
                        "email": contact_email,
                        "domain": domain,
                        "full_name": contact.get("full_name", "") or "",
                        "first_name": contact.get("first_name", "") or "",
                        "last_name": contact.get("last_name", "") or "",
                        "title": contact.get("title", "") or "",
                        "linkedin_url": linkedin_url,
                    }

                    logger.debug("Syncing contact to contacts API: %s", payload)

                    # Acquire rate limit before upsert to respect 75 RPS limit
                    await contacts_client._acquire_upsert_rate_limit()

                    # Call contacts API upsert with auth header
                    contacts_token = os.getenv("CONTACTS_API_TOKEN", "")
                    resp = await contacts_http.post(
                        "https://leadsdatabase.cc/v1/contact/upsert",
                        json=payload,
                        headers={
                            "Authorization": f"Bearer {contacts_token}",
                            "Content-Type": "application/json",
                        },
                        timeout=15.0,
                    )
                    logger.debug("Contacts API response status: %d, body: %s", resp.status_code, resp.text[:500])
                    resp.raise_for_status()
                    synced_count += 1
                except httpx.HTTPStatusError as e:
                    logger.warning("HTTP error syncing contact %s: status=%d, body=%s", contact.get("email"), e.response.status_code, e.response.text[:200])
                    failed_count += 1
                except Exception as e:
                    logger.warning("Failed to sync contact %s: %s", contact.get("email"), e)
                    failed_count += 1

            sync_result = {
                "synced": synced_count,
                "skipped": skipped_count,
                "failed": failed_count,
            }
            if synced_count > 0:
                sync_status = "success"
            elif failed_count > 0:
                sync_status = "failed"
            else:
                sync_status = "no_contacts_to_sync"

        # Only use defaults if sources weren't already set by BetterEnrich
        if sources.get("emails") in ("not_found", None):
            sources["emails"] = email_source_db
        if sources.get("contacts") in ("not_found", None):
            sources["contacts"] = contacts_source
        if sources.get("company_linkedin") in ("not_found", None):
            sources["company_linkedin"] = company_source

        # Record source stats for API-only call
        _record_unified_enrich_stats(enriched_contacts, domain, current_user)

        return {
            "domain": domain,
            "mode": mode,
            "company_linkedin_url": company_linkedin_url,
            "contacts": enriched_contacts,
            "contact_count": len(enriched_contacts),
            "data_sources": sources,
            "sync_to_contacts_db": {
                "status": sync_status,
                "records_synced": sync_result.get("synced", 0),
                "records_skipped": sync_result.get("skipped", 0),
                "records_failed": sync_result.get("failed", 0),
            },
        }

    finally:
        await blitz_http.aclose()
        await contacts_http.aclose()


@router.post("/upload")
async def upload_csv(
    file: UploadFile,
    current_user: dict = Depends(auth.get_current_user),
):
    """Accepts a CSV file, saves it persistently, returns upload_id and columns."""
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are accepted.")

    content = await file.read()
    try:
        df = pd.read_csv(io.BytesIO(content), nrows=5, skipinitialspace=True)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not parse CSV: {e}")

    upload_id = str(uuid.uuid4())
    save_path = UPLOAD_DIR / f"{upload_id}.csv"
    save_path.write_bytes(content)

    # Save metadata alongside the CSV (original filename)
    metadata_path = UPLOAD_DIR / f"{upload_id}.metadata.json"
    metadata_path.write_text(json.dumps({"original_filename": file.filename}))

    preview = df.head(3).fillna("").astype(str).to_dict(orient="records")
    # Fast row count using newline count (much faster than iterating)
    row_count = content.count(b'\n') - 1 if content else 0

    return {
        "upload_id": upload_id,
        "columns": list(df.columns),
        "preview": preview,
        "row_count": max(0, row_count),
        "filename": file.filename,
    }


def _owns_job(job: dict[str, Any], current_user: dict[str, Any]) -> bool:
    if current_user.get("is_admin"):
        return True
    return job.get("user_id") == current_user["user_id"]


@router.get("/jobs")
async def list_enrichment_jobs(current_user: dict = Depends(auth.get_current_user)):
    """List enrichment jobs for current user (or all for admin)."""
    store = job_store.get_store()
    if current_user.get("is_admin"):
        jobs = store.list_jobs(job_type="enrichment", limit=200)
    else:
        jobs = store.list_jobs(user_id=current_user["user_id"], job_type="enrichment", limit=200)

    # Enhance job display with user-friendly filenames
    for job in jobs:
        # Use original_filename as the primary filename for display
        if job.get("original_filename"):
            job["filename"] = job["original_filename"]
            job["display_filename"] = job["original_filename"]
        else:
            # Fallback for jobs created before this fix
            filename = job.get("filename", "")
            # If filename looks like a UUID, try to make it more user-friendly
            if filename and len(filename) == 36 and filename.count('-') == 4:
                # Generate a user-friendly name based on job ID
                friendly_name = f"uploaded_file_{job['job_id'][:8]}.csv"
                job["filename"] = friendly_name
                job["display_filename"] = friendly_name
            elif filename:
                # Use existing filename
                job["display_filename"] = f"{filename}.csv" if not filename.endswith('.csv') else filename
            else:
                job["filename"] = "Unknown.csv"
                job["display_filename"] = "Unknown.csv"

    return {"jobs": jobs}


@router.post("/jobs")
async def start_enrichment_job(
    req: StartJobRequest,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(auth.get_current_user),
):
    """Starts a persistent enrichment job. Returns immediately with job_id."""
    upload_path = UPLOAD_DIR / f"{req.upload_id}.csv"
    if not upload_path.exists():
        raise HTTPException(status_code=404, detail="Upload not found.")

    df = pd.read_csv(str(upload_path), skipinitialspace=True)
    if req.domain_col not in df.columns:
        raise HTTPException(
            status_code=400, detail=f"Column '{req.domain_col}' not found in CSV."
        )

    rows = df.fillna("").astype(str).to_dict(orient="records")
    cascade = req.cascade if req.cascade else blitz_client.DEFAULT_CASCADE

    # Read metadata to get original filename
    metadata_path = UPLOAD_DIR / f"{req.upload_id}.metadata.json"
    original_filename = ""
    if metadata_path.exists():
        try:
            metadata = json.loads(metadata_path.read_text())
            original_filename = metadata.get("original_filename", "")
        except Exception as e:
            logger.warning("Failed to read metadata for %s: %s", req.upload_id, e)

    job_id = str(uuid.uuid4())
    store = job_store.get_store()

    # Convert cascade to JSON for storage
    cascade_json = json.dumps(cascade) if cascade else None

    # Enforce job limit before creating new job
    enforce_job_limit(current_user["user_id"])

    store.create_enrichment_job(
        job_id=job_id,
        user_id=current_user["user_id"],
        total=len(rows),
        filename=str(req.upload_id),
        domain_col=req.domain_col,
        original_filename=original_filename,
        name_col=req.name_col,
        first_name_col=req.first_name_col,
        last_name_col=req.last_name_col,
        cascade_config=cascade_json,
        max_results=req.max_results,
    )

    _job_signals[job_id] = asyncio.Event()
    _active_jobs.add(job_id)

    background_tasks.add_task(
        _run_job,
        job_id=job_id,
        rows=rows,
        domain_col=req.domain_col,
        name_col=req.name_col,
        first_name_col=req.first_name_col,
        last_name_col=req.last_name_col,
        cascade=cascade,
        max_results=req.max_results,
        write_incremental=True,  # Enable incremental writes for partial downloads
    )

    return {"job_id": job_id, "total": len(rows)}


@router.get("/jobs/{job_id}")
async def get_enrichment_job(
    job_id: str,
    current_user: dict = Depends(auth.get_current_user),
):
    """Get an enrichment job by ID."""
    store = job_store.get_store()
    job_data = store.get_job(job_id)
    if not job_data:
        raise HTTPException(status_code=404, detail="Job not found.")
    if job_data.get("job_type") != "enrichment":
        raise HTTPException(status_code=404, detail="Enrichment job not found.")
    if not _owns_job(job_data, current_user):
        raise HTTPException(status_code=403, detail="Access denied.")

    # Use original_filename for better display
    if job_data.get("original_filename"):
        job_data["filename"] = job_data["original_filename"]
        job_data["display_filename"] = job_data["original_filename"]
    else:
        # Fallback for old jobs
        filename = job_data.get("filename", "")
        if filename and len(filename) == 36 and filename.count('-') == 4:
            friendly_name = f"uploaded_file_{job_id[:8]}.csv"
            job_data["filename"] = friendly_name
            job_data["display_filename"] = friendly_name
        elif filename:
            job_data["display_filename"] = f"{filename}.csv" if not filename.endswith('.csv') else filename
        else:
            job_data["filename"] = "Unknown.csv"
            job_data["display_filename"] = "Unknown.csv"

    return job_data


@router.get("/jobs/{job_id}/stream")
async def stream_enrichment_job_progress(
    job_id: str,
    current_user: dict = Depends(auth.get_current_user),
):
    """SSE stream of enrichment progress events with replay support."""
    store = job_store.get_store()
    job_data = store.get_job(job_id)
    if not job_data:
        raise HTTPException(status_code=404, detail="Job not found.")
    if job_data.get("job_type") != "enrichment":
        raise HTTPException(status_code=404, detail="Enrichment job not found.")
    if not _owns_job(job_data, current_user):
        raise HTTPException(status_code=403, detail="Access denied.")

    async def event_generator():
        sent = 0
        while True:
            new_events = store.get_events_from(job_id, sent)
            for event in new_events:
                sent += 1
                yield f"data: {json.dumps(event)}\n\n"

            current = store.get_job(job_id)
            if current and current["status"] in ("done", "failed"):
                final = {
                    "done": True,
                    "error": current.get("error"),
                    "total": current.get("total", 0),
                    "processed": current.get("processed", 0),
                    "emails_found": current.get("emails_found", 0),
                }
                yield f"data: {json.dumps(final)}\n\n"
                break

            sig = _job_signals.get(job_id)
            if sig:
                try:
                    await asyncio.wait_for(asyncio.shield(asyncio.ensure_future(_wait_event(sig))), timeout=2.0)
                except asyncio.TimeoutError:
                    pass
            else:
                await asyncio.sleep(2.0)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/jobs/{job_id}/download")
async def download_enrichment_result(
    job_id: str,
    current_user: dict = Depends(auth.get_current_user),
):
    """Download the full CSV output of a completed enrichment job."""
    store = job_store.get_store()
    job_data = store.get_job(job_id)
    if not job_data:
        raise HTTPException(status_code=404, detail="Job not found.")
    if job_data.get("job_type") != "enrichment":
        raise HTTPException(status_code=404, detail="Enrichment job not found.")
    if not _owns_job(job_data, current_user):
        raise HTTPException(status_code=403, detail="Access denied.")
    if job_data["status"] in ("queued", "running"):
        raise HTTPException(status_code=202, detail="Job not finished yet.")

    # Handle partial status - allow download of partial results
    if job_data["status"] == "partial":
        output_path = job_data.get("output_path") or (OUTPUT_DIR / f"{job_id}.csv")
        if Path(output_path).exists() and Path(output_path).stat().st_size > 0:
            original_filename = job_data.get("original_filename") or job_data.get("filename", "results")
            safe_name = original_filename[:30].replace(" ", "-").replace("/", "-")
            return FileResponse(
                path=output_path,
                media_type="text/csv",
                filename=f"partial_{safe_name}_{job_id[:8]}.csv",
            )
        else:
            raise HTTPException(status_code=404, detail="Partial output file not found.")

    # Check if job failed but has partial output available
    if job_data["status"] == "failed":
        output_path = job_data.get("output_path")
        error_msg = job_data.get("error", "")

        # If output_path is not in database, try the standard location
        if not output_path:
            output_path = OUTPUT_DIR / f"{job_id}.csv"

        # If failed but partial output exists and file is not empty
        if Path(output_path).exists() and Path(output_path).stat().st_size > 0:
            # Allow download with a warning
            logger.info("Downloading partial results for failed job %s: %s", job_id, error_msg)
            # Continue to download below (don't raise exception)
        else:
            # No partial output available
            raise HTTPException(
                status_code=500,
                detail=f"Job failed: {error_msg}"
            )
    else:
        # For non-failed jobs, get output_path from database
        output_path = job_data.get("output_path")
        if not output_path or not Path(output_path).exists():
            raise HTTPException(status_code=404, detail="Output file not found.")
    return FileResponse(
        path=output_path,
        media_type="text/csv",
        filename=f"enriched_{job_id[:8]}.csv",
    )


@router.get("/jobs/{job_id}/partial-download")
async def partial_download_enrichment(
    job_id: str,
    current_user: dict = Depends(auth.get_current_user),
):
    """
    Download partial CSV results from a running enrichment job.
    Returns whatever enriched data has been written so far.
    """
    store = job_store.get_store()
    job_data = store.get_job(job_id)
    if not job_data:
        raise HTTPException(status_code=404, detail="Job not found.")
    if job_data.get("job_type") != "enrichment":
        raise HTTPException(status_code=404, detail="Enrichment job not found.")
    if not _owns_job(job_data, current_user):
        raise HTTPException(status_code=403, detail="Access denied.")

    output_path = OUTPUT_DIR / f"{job_id}.csv"
    if not output_path.exists():
        raise HTTPException(status_code=404, detail="No enriched data yet.")

    return FileResponse(
        path=output_path,
        media_type="text/csv",
        filename=f"partial_enriched_{job_id[:8]}.csv",
    )


# ---------------------------------------------------------------------------
# Background job runner
# ---------------------------------------------------------------------------

async def _run_background_sync(job_id: str, output_path: Path) -> None:
    """
    Background task to sync enrichment results to Contacts DB.
    Runs asynchronously without blocking the API.
    """
    try:
        logger.info("Auto-syncing enrichment job %s to contacts DB (person records)", job_id)
        sync_result = sync_contacts.sync_enrichment_to_contacts(output_path)
        logger.info("Auto-sync complete for job %s: %s", job_id, sync_result)
    except Exception as sync_err:
        logger.error("Auto-sync failed for job %s: %s", job_id, sync_err)


async def _run_job(
    job_id: str,
    rows: list[dict[str, Any]],
    domain_col: str,
    name_col: Optional[str],
    first_name_col: Optional[str],
    last_name_col: Optional[str],
    cascade: list[dict[str, Any]],
    max_results: int,
    write_incremental: bool = False,
):
    store = job_store.get_store()
    store.set_running(job_id)
    seq = [0]

    output_path = OUTPUT_DIR / f"{job_id}.csv"

    async def on_progress(e: dict[str, Any]):
        # Get FRESH store instance for this thread
        # This fixes the progress counter bug where background tasks couldn't commit
        progress_store = job_store.get_store()
        progress_store.append_event(job_id, seq[0], e)
        seq[0] += 1
        sig = _job_signals.get(job_id)
        if sig:
            sig.set()
            sig.clear()

    try:
        # Create a cancellation check function that queries the database
        # This ensures cancellation is detected even if in-memory set was cleared
        def check_job_cancelled(jid: str) -> bool:
            check_store = job_store.get_store()
            return check_store.is_job_cancelled_or_abandoned(jid)

        output_rows = await pipeline.run_pipeline(
            rows=rows,
            domain_col=domain_col,
            name_col=name_col,
            first_name_col=first_name_col,
            last_name_col=last_name_col,
            cascade=cascade,
            max_results=max_results,
            on_progress=on_progress,
            write_incremental=write_incremental,
            output_path=output_path,
            cancelled_jobs=_cancelled_jobs,
            job_id=job_id,
            check_cancelled=check_job_cancelled,
        )

        # If not writing incrementally, write final output
        if not write_incremental:
            if output_rows:
                out_df = pd.DataFrame(output_rows)
                input_cols = [c for c in out_df.columns if c not in pipeline.ENRICHED_COLUMNS]
                ordered = input_cols + [c for c in pipeline.ENRICHED_COLUMNS if c in out_df.columns]
                out_df[ordered].to_csv(str(output_path), index=False)
            else:
                output_path.write_text("")

        store.set_done(job_id, str(output_path))
        logger.info("Enrichment job %s completed, %d output rows", job_id, len(output_rows))

        # Run auto-sync in the background without blocking the API
        # This prevents the refresh button from getting stuck
        asyncio.create_task(_run_background_sync(job_id, output_path))

        # Get job details for email notification
        job = store.get_enrichment_job(job_id)
        if job:
            await send_job_notification(
                recipients=get_notification_recipients(),
                job_type="enrichment",
                filename=job.get("original_filename") or job.get("filename", "Unknown"),
                status="done",
                total=job.get("total", 0),
                processed=job.get("processed", 0),
                emails_found=job.get("emails_found", 0)
            )

    except RuntimeError as e:
        # Handle job cancellation and abandonment
        error_msg = str(e)
        is_abandoned = "was abandoned" in error_msg
        is_user_cancelled = "was cancelled by user" in error_msg

        if is_abandoned:
            logger.info("Enrichment job %s was abandoned (server restart)", job_id)
            _cancelled_jobs.discard(job_id)
            final_error = "Job was abandoned due to server restart. Please retry from the jobs page."

            if output_path.exists():
                partial_size = output_path.stat().st_size
                if partial_size > 0:
                    store.set_status(job_id, "partial")
                    logger.info("Abandoned job %s has partial output: %d bytes", job_id, partial_size)
                else:
                    store.set_failed(job_id, final_error)
            else:
                store.set_failed(job_id, final_error)
        elif is_user_cancelled:
            logger.info("Enrichment job %s was cancelled by user", job_id)
            _cancelled_jobs.discard(job_id)

            if output_path.exists():
                partial_size = output_path.stat().st_size
                if partial_size > 0:
                    store.set_status(job_id, "partial")
                    logger.info("Cancelled job %s has partial output available: %d bytes", job_id, partial_size)
                    job = store.get_enrichment_job(job_id)
                    if job:
                        await send_job_notification(
                            recipients=get_notification_recipients(),
                            job_type="enrichment",
                            filename=job.get("original_filename") or job.get("filename", "Unknown"),
                            status="partial",
                            total=job.get("total", 0),
                            processed=job.get("processed", 0),
                            emails_found=job.get("emails_found", 0),
                        )
                else:
                    store.set_failed(job_id, "Job cancelled by user")
            else:
                store.set_failed(job_id, "Job cancelled by user")
        else:
            # Other RuntimeErrors should be handled as normal failures
            logger.exception("Enrichment job %s failed with RuntimeError: %s", job_id, e)
            store.set_failed(job_id, f"Job failed: {str(e)}")
            # Send failure notification
            job = store.get_enrichment_job(job_id)
            if job:
                await send_job_notification(
                    recipients=get_notification_recipients(),
                    job_type="enrichment",
                    filename=job.get("original_filename") or job.get("filename", "Unknown"),
                    status="failed",
                    total=job.get("total", 0),
                    processed=job.get("processed", 0),
                    emails_found=job.get("emails_found", 0),
                    error_message=str(e)
                )
    except Exception as e:
        logger.exception("Enrichment job %s failed: %s", job_id, e)

        # Provide user-friendly error message
        error_msg = str(e)
        error_lower = error_msg.lower()

        # Categorize error for better user feedback
        if "column" in error_lower and "not found" in error_lower:
            user_msg = f"Configuration error: The specified column was not found in the CSV file. {error_msg}"
        elif "authentication" in error_lower or "unauthorized" in error_lower:
            user_msg = "Authentication error. Please log in again."
        elif "timeout" in error_lower:
            user_msg = "Request timeout. The API took too long to respond. Please try again."
        elif "rate limit" in error_lower or "429" in error_msg:
            user_msg = "Rate limit exceeded. Please wait a few minutes and try again."
        else:
            user_msg = f"Job encountered an error: {error_msg}"

        # Check if we have partial output available
        if output_path.exists():
            partial_size = output_path.stat().st_size
            if partial_size > 0:
                user_msg += " (Partial results are available for download)"
                # Mark as done instead of failed so user can download partial results
                store.set_done(job_id, str(output_path))
                logger.warning("Enrichment job %s completed with errors, partial output available: %s", job_id, user_msg)
            else:
                store.set_failed(job_id, user_msg)
                # Send failure notification
                job = store.get_enrichment_job(job_id)
                if job:
                    await send_job_notification(
                        recipients=get_notification_recipients(),
                        job_type="enrichment",
                        filename=job.get("original_filename") or job.get("filename", "Unknown"),
                        status="failed",
                        total=job.get("total", 0),
                        processed=job.get("processed", 0),
                        emails_found=job.get("emails_found", 0),
                        error_message=user_msg
                    )
        else:
            store.set_failed(job_id, user_msg)
            # Send failure notification
            job = store.get_enrichment_job(job_id)
            if job:
                await send_job_notification(
                    recipients=get_notification_recipients(),
                    job_type="enrichment",
                    filename=job.get("original_filename") or job.get("filename", "Unknown"),
                    status="failed",
                    total=job.get("total", 0),
                    processed=job.get("processed", 0),
                    emails_found=job.get("emails_found", 0),
                    error_message=user_msg
                )

    finally:
        _active_jobs.discard(job_id)
        sig = _job_signals.pop(job_id, None)
        if sig:
            sig.set()


async def _wait_event(event: asyncio.Event):
    await event.wait()
    event.clear()


@router.post("/jobs/{job_id}/restart")
async def restart_enrichment_job(
    job_id: str,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(auth.get_current_user),
):
    """
    Restart a failed enrichment job with the same configuration.

    Creates a new job using the same CSV file and configuration as the original.
    The original job_id is preserved in the parent_job_id field for tracking.
    """
    store = job_store.get_store()
    original_job = store.get_job(job_id)

    if not original_job:
        raise HTTPException(status_code=404, detail="Job not found")

    if original_job.get("job_type") != "enrichment":
        raise HTTPException(status_code=400, detail="Only enrichment jobs can be restarted")

    if not _owns_job(original_job, current_user):
        raise HTTPException(status_code=403, detail="Access denied")

    if original_job["status"] != "failed":
        raise HTTPException(status_code=400, detail="Only failed jobs can be restarted")

    # Read the original CSV file (kept in uploads/)
    upload_path = UPLOAD_DIR / f"{original_job['filename']}.csv"
    if not upload_path.exists():
        raise HTTPException(status_code=404, detail="Original CSV file not found")

    try:
        df = pd.read_csv(str(upload_path), skipinitialspace=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not read original CSV: {e}")

    # Validate domain column exists
    domain_col = original_job.get('domain_col', '')
    if not domain_col or domain_col not in df.columns:
        raise HTTPException(status_code=400, detail=f"Domain column '{domain_col}' not found in CSV")

    rows = df.fillna("").astype(str).to_dict(orient="records")

    # Parse cascade configuration from JSON
    cascade = None
    cascade_json = original_job.get('cascade_config')
    if cascade_json:
        try:
            cascade = json.loads(cascade_json)
        except Exception as e:
            logger.warning("Failed to parse cascade_config for job %s: %s", job_id, e)
            cascade = None

    if not cascade:
        cascade = blitz_client.DEFAULT_CASCADE

    # Parse selected_providers from original job (for restart with provider selection)
    selected_providers = None
    providers_json = original_job.get('selected_providers', '')
    if providers_json:
        try:
            selected_providers = json.loads(providers_json)
        except Exception as e:
            logger.warning("Failed to parse selected_providers for job %s: %s", job_id, e)

    # Create new job
    new_job_id = str(uuid.uuid4())
    store.create_enrichment_job(
        job_id=new_job_id,
        user_id=current_user["user_id"],
        total=len(rows),
        filename=original_job['filename'],
        domain_col=original_job['domain_col'],
        original_filename=original_job.get('original_filename', ''),
        parent_job_id=job_id,  # Track original job for restart chain
        name_col=original_job.get('name_col'),
        first_name_col=original_job.get('first_name_col'),
        last_name_col=original_job.get('last_name_col'),
        cascade_config=cascade_json,
        max_results=original_job.get('max_results', 5),
        selected_providers=selected_providers,
    )

    # Set up signals and background task
    _job_signals[new_job_id] = asyncio.Event()
    _active_jobs.add(new_job_id)

    # Use _run_domain_enrich_job if selected_providers is set (new provider selection feature)
    # Otherwise, use _run_job (old pipeline-based enrichment)
    if selected_providers is not None:
        background_tasks.add_task(
            _run_domain_enrich_job,
            job_id=new_job_id,
            rows=rows,
            domain_col=original_job['domain_col'],
            name_col=original_job.get('name_col'),
            first_name_col=original_job.get('first_name_col'),
            last_name_col=original_job.get('last_name_col'),
            max_results=original_job.get('max_results', 5),
            selected_providers=selected_providers,
        )
    else:
        background_tasks.add_task(
            _run_job,
            job_id=new_job_id,
            rows=rows,
            domain_col=original_job['domain_col'],
            name_col=original_job.get('name_col'),
            first_name_col=original_job.get('first_name_col'),
            last_name_col=original_job.get('last_name_col'),
            cascade=cascade,
            max_results=original_job.get('max_results', 5),
            write_incremental=True,
        )

    logger.info("Restarted enrichment job %s as new job %s with providers %s", job_id, new_job_id, selected_providers)

    return {
        "job_id": new_job_id,
        "total": len(rows),
        "restarted_from": job_id,
    }


@router.post("/jobs/{job_id}/cancel")
async def cancel_enrichment_job(
    job_id: str,
    current_user: dict = Depends(auth.get_current_user),
):
    """
    Cancel a running or queued enrichment job.

    Marks the job as cancelled in both the database (persistent) and in-memory set (for fast checking).
    Background task will check both sources and stop processing.
    """
    store = job_store.get_store()
    job = store.get_job(job_id)

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.get("job_type") != "enrichment":
        raise HTTPException(status_code=400, detail="Only enrichment jobs can be cancelled")

    if not _owns_job(job, current_user):
        raise HTTPException(status_code=403, detail="Access denied")

    if job["status"] not in ("queued", "running"):
        raise HTTPException(status_code=400, detail="Only running or queued jobs can be cancelled")

    # Add to in-memory cancelled set for fast checking by background task
    _cancelled_jobs.add(job_id)

    # Also update database for persistence - survives worker restarts
    # This is the key fix: if workers restart, they'll see the cancelled status
    store.set_cancelled(job_id)

    # Remove from active jobs set
    _active_jobs.discard(job_id)

    # Wake up any SSE listeners
    sig = _job_signals.pop(job_id, None)
    if sig:
        sig.set()

    logger.info("Enrichment job %s cancellation requested by user %s", job_id, current_user["user_id"])

    return {
        "job_id": job_id,
        "status": "cancelled",
        "message": "Job has been cancelled. Any partial results can still be downloaded."
    }


def cleanup_stale_jobs() -> None:
    """Mark jobs as abandoned if they were running when server restarted.

    This status distinguishes from jobs that failed due to errors (user can retry).
    """
    store = job_store.get_store()
    stale = store.get_stale_running_jobs()
    for job_id in stale:
        store.set_abandoned(
            job_id,
            "Job was abandoned: Server restarted or crashed while processing. "
            "The job was interrupted before completion. Please retry from the beginning."
        )
        logger.warning("Marked stale enrichment job %s as abandoned on startup", job_id)


# =============================================================================
# List Building Tool - New Endpoints for Flow 1, 2, 3
# =============================================================================

# --- Request Models ---

class CompanySearchRequest(BaseModel):
    """Request model for company search (Flow 2)."""
    name: Optional[str] = None
    industry: Optional[list[str]] = None
    employee_range: Optional[list[str]] = None
    company_type: Optional[list[str]] = None
    country_code: Optional[str] = None
    limit: int = 100
    offset: int = 0


class SearchAndEnrichRequest(BaseModel):
    """Request model for search + enrich (Flow 2)."""
    name: Optional[str] = None
    industry: Optional[list[str]] = None
    employee_range: Optional[list[str]] = None
    company_type: Optional[list[str]] = None
    country_code: Optional[str] = None
    max_decision_makers: int = 5
    include_generic_emails: bool = True


class LinkedInEnrichRequest(BaseModel):
    """Request model for LinkedIn enrichment (Flow 3)."""
    upload_id: str
    linkedin_col: str
    include_company: bool = True


class LinkedInV2Request(BaseModel):
    """Request model for unified LinkedIn enrichment (personal + company)."""
    upload_id: str
    personal_linkedin_col: Optional[str] = None
    company_linkedin_col: Optional[str] = None
    max_dms: int = 5
    include_company: bool = True


# --- Flow 1: Domain Enrichment (Extended) ---

@router.post("/by-domains")
async def enrich_by_domains(
    req: StartJobRequest,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(auth.get_current_user),
):
    """
    Flow 1: Domain → Generic Emails + Decision Makers

    Upload a CSV with domains and get:
    - Generic emails per domain
    - Up to 5 decision makers per company

    This extends the existing enrichment endpoint with additional options.
    """
    upload_path = UPLOAD_DIR / f"{req.upload_id}.csv"
    if not upload_path.exists():
        raise HTTPException(status_code=404, detail="Upload not found.")

    df = pd.read_csv(str(upload_path), skipinitialspace=True)
    if req.domain_col not in df.columns:
        raise HTTPException(
            status_code=400, detail=f"Column '{req.domain_col}' not found in CSV."
        )

    rows = df.fillna("").astype(str).to_dict(orient="records")
    cascade = req.cascade if req.cascade else blitz_client.DEFAULT_CASCADE

    # Read metadata
    metadata_path = UPLOAD_DIR / f"{req.upload_id}.metadata.json"
    original_filename = ""
    if metadata_path.exists():
        try:
            metadata = json.loads(metadata_path.read_text())
            original_filename = metadata.get("original_filename", "")
        except Exception:
            pass

    job_id = str(uuid.uuid4())
    store = job_store.get_store()
    cascade_json = json.dumps(cascade) if cascade else None

    store.create_enrichment_job(
        job_id=job_id,
        user_id=current_user["user_id"],
        total=len(rows),
        filename=str(req.upload_id),
        domain_col=req.domain_col,
        original_filename=original_filename,
        name_col=req.name_col,
        first_name_col=req.first_name_col,
        last_name_col=req.last_name_col,
        cascade_config=cascade_json,
        max_results=req.max_results,
    )

    _job_signals[job_id] = asyncio.Event()
    _active_jobs.add(job_id)

    background_tasks.add_task(
        _run_job,
        job_id=job_id,
        rows=rows,
        domain_col=req.domain_col,
        name_col=req.name_col,
        first_name_col=req.first_name_col,
        last_name_col=req.last_name_col,
        cascade=cascade,
        max_results=req.max_results,
        write_incremental=True,
    )

    return {"job_id": job_id, "total": len(rows), "flow": "domain_enrichment"}


class ProviderToggleRequest(BaseModel):
    """Request with optional provider selection for Flow 1 using list_builder."""
    upload_id: str
    domain_col: str
    name_col: Optional[str] = None
    first_name_col: Optional[str] = None
    last_name_col: Optional[str] = None
    max_results: int = 5
    # List of providers to use (e.g., ["blitz", "better_enrich"])
    # contacts_db is always used first and cannot be disabled
    # If None, all enabled providers are used
    providers: Optional[list[str]] = None


@router.post("/flows/domain-enrich")
async def domain_enrich_with_providers(
    req: ProviderToggleRequest,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(auth.get_current_user),
):
    """
    Flow 1: Domain → Generic Emails + Decision Makers

    This endpoint uses the list_builder pipeline with provider selection.
    Provider selection allows users to choose which enrichment providers to use.

    Cascade order: Contacts DB (always) → (selected providers in order)
    Stop on first hit - if a provider finds contacts, later providers are skipped.
    """
    upload_path = UPLOAD_DIR / f"{req.upload_id}.csv"
    if not upload_path.exists():
        raise HTTPException(status_code=404, detail="Upload not found.")

    df = pd.read_csv(str(upload_path), skipinitialspace=True)
    if req.domain_col not in df.columns:
        raise HTTPException(
            status_code=400, detail=f"Column '{req.domain_col}' not found in CSV."
        )

    rows = df.fillna("").astype(str).to_dict(orient="records")

    # Read metadata
    metadata_path = UPLOAD_DIR / f"{req.upload_id}.metadata.json"
    original_filename = ""
    if metadata_path.exists():
        try:
            metadata = json.loads(metadata_path.read_text())
            original_filename = metadata.get("original_filename", "")
        except Exception:
            pass

    # Validate providers if provided
    if req.providers:
        invalid = [p for p in req.providers if p not in list_builder.VALID_PROVIDERS]
        if invalid:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid providers: {invalid}. Valid: {list(list_builder.VALID_PROVIDERS)}"
            )

    job_id = str(uuid.uuid4())
    store = job_store.get_store()

    store.create_enrichment_job(
        job_id=job_id,
        user_id=current_user["user_id"],
        total=len(rows),
        filename=str(req.upload_id),
        domain_col=req.domain_col,
        original_filename=original_filename,
        name_col=req.name_col,
        first_name_col=req.first_name_col,
        last_name_col=req.last_name_col,
        cascade_config=None,
        max_results=req.max_results,
        selected_providers=req.providers,
    )

    _job_signals[job_id] = asyncio.Event()
    _active_jobs.add(job_id)

    background_tasks.add_task(
        _run_domain_enrich_job,
        job_id=job_id,
        rows=rows,
        domain_col=req.domain_col,
        name_col=req.name_col,
        first_name_col=req.first_name_col,
        last_name_col=req.last_name_col,
        max_results=req.max_results,
        selected_providers=req.providers,
    )

    return {"job_id": job_id, "total": len(rows), "flow": "domain_enrichment"}


async def _run_domain_enrich_job(
    job_id: str,
    rows: list[dict[str, Any]],
    domain_col: str,
    name_col: Optional[str],
    first_name_col: Optional[str],
    last_name_col: Optional[str],
    max_results: int,
    selected_providers: Optional[list[str]] = None,
):
    """Background task to run domain enrichment using list_builder."""
    store = job_store.get_store()
    store.set_running(job_id)
    seq = [0]

    output_path = OUTPUT_DIR / f"{job_id}.csv"

    async def on_progress(e: dict[str, Any]):
        try:
            # Get fresh store instance and commit immediately
            progress_store = job_store.get_store()
            progress_store.append_event(job_id, seq[0], e)
            # Force commit to ensure event is saved
            progress_store.conn.commit()
            seq[0] += 1
            sig = _job_signals.get(job_id)
            if sig:
                sig.set()
                sig.clear()
        except Exception as prog_err:
            logger.error("Progress callback failed for job %s: %s", job_id, prog_err)

    try:
        # Check job cancellation
        def check_job_cancelled(jid: str) -> bool:
            check_store = job_store.get_store()
            return check_store.is_job_cancelled_or_abandoned(jid)

        output_rows = await list_builder.run_domain_enrichment(
            rows=rows,
            domain_col=domain_col,
            name_col=name_col,
            first_name_col=first_name_col,
            last_name_col=last_name_col,
            max_decision_makers=max_results,
            include_generic_emails=True,
            on_progress=on_progress,
            selected_providers=selected_providers,
            cancelled_jobs=_cancelled_jobs,
            check_cancelled=check_job_cancelled,
            job_id=job_id,
        )

        # Write output
        if output_rows:
            out_df = pd.DataFrame(output_rows)
            input_cols = [c for c in out_df.columns if c not in list_builder.ENRICHED_COLUMNS]
            ordered = input_cols + [c for c in list_builder.ENRICHED_COLUMNS if c in out_df.columns]
            out_df[ordered].to_csv(str(output_path), index=False)
        else:
            output_path.write_text("")

        store.set_done(job_id, str(output_path))
        logger.info("Domain enrich job %s completed, %d output rows", job_id, len(output_rows))

        # Sync results back to Contacts DB (async, non-blocking)
        asyncio.create_task(_run_background_sync(job_id, output_path))

        # Send notification
        job = store.get_enrichment_job(job_id)
        if job:
            await send_job_notification(
                recipients=get_notification_recipients(),
                job_type="enrichment",
                filename=job.get("original_filename") or job.get("filename", "Unknown"),
                status="done",
                total=job.get("total", 0),
                processed=job.get("processed", 0),
                emails_found=job.get("emails_found", 0)
            )

    except RuntimeError as e:
        error_msg = str(e)
        if "was cancelled" in error_msg or "was abandoned" in error_msg:
            # Check what kind of cancellation it was
            if "abandoned" in error_msg:
                final_error = "Job was abandoned due to server restart. Please retry from the jobs page."
            elif "cancelled by user" in error_msg:
                final_error = "Job was cancelled by user."
            else:
                final_error = error_msg

            logger.info("Domain enrich job %s stopped: %s", job_id, final_error)
            _cancelled_jobs.discard(job_id)

            if output_path.exists():
                partial_size = output_path.stat().st_size
                if partial_size > 0:
                    store.set_status(job_id, "partial")
                    logger.info("Job %s has partial output: %d bytes", job_id, partial_size)
                else:
                    store.set_failed(job_id, final_error)
            else:
                store.set_failed(job_id, final_error)
        else:
            logger.exception("Domain enrich job %s failed: %s", job_id, e)
            store.set_failed(job_id, f"Job failed: {str(e)}")

    except Exception as e:
        logger.exception("Domain enrich job %s crashed: %s", job_id, e)
        store.set_failed(job_id, f"Job crashed: {str(e)}")


# --- Flow 2: Company Search & Enrich ---

@router.post("/search/companies")
async def search_companies(
    req: CompanySearchRequest,
    current_user: dict = Depends(auth.get_current_user),
):
    """
    Flow 2a: Search companies by criteria

    Search Blitz API for companies matching the given criteria.
    Returns list of companies with their LinkedIn URLs and details.
    """
    async with httpx.AsyncClient() as client:
        try:
            result = await blitz_client.company_search(
                client,
                name=req.name,
                industry=req.industry,
                employee_range=req.employee_range,
                company_type=req.company_type,
                country_code=req.country_code,
                limit=req.limit,
                offset=req.offset,
            )
            return {
                "count": result.get("count", 0),
                "total": result.get("total", 0),
                "results": result.get("results", []),
                "flow": "company_search",
            }
        except Exception as e:
            logger.error("Company search failed: %s", e)
            raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")


@router.post("/search/companies/enrich")
async def search_and_enrich(
    req: SearchAndEnrichRequest,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(auth.get_current_user),
):
    """
    Flow 2b: Search companies + Enrich

    1. Search for companies matching criteria
    2. Enrich each company with decision makers and emails

    Returns a job_id for tracking the enrichment process.
    """
    # First, search for companies
    async with httpx.AsyncClient() as client:
        try:
            search_result = await blitz_client.company_search(
                client,
                name=req.name,
                industry=req.industry,
                employee_range=req.employee_range,
                company_type=req.company_type,
                country_code=req.country_code,
                limit=500,  # Max companies to enrich
                offset=0,
            )
        except Exception as e:
            logger.error("Company search failed: %s", e)
            raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")

    companies = search_result.get("results", [])
    if not companies:
        raise HTTPException(status_code=400, detail="No companies found matching criteria.")

    # Create a job to track this enrichment
    job_id = str(uuid.uuid4())
    store = job_store.get_store()

    # Extract domains from companies (if available)
    domain_col = "domain"
    rows = []
    for company in companies:
        # Try to extract domain from LinkedIn URL or name
        domain = company.get("domain", "")
        if not domain and company.get("linkedin_url"):
            # Extract company name from LinkedIn URL as fallback
            name = company.get("name", "")
            if name:
                # Use lowercase, remove spaces as domain guess
                domain = name.lower().replace(" ", "") + ".com"
        if domain:
            row = {domain_col: domain}
            # Include original company data
            row["company_name"] = company.get("name", "")
            row["company_linkedin_url"] = company.get("linkedin_url", "")
            row["company_industry"] = company.get("industry", "")
            row["company_employee_count"] = str(company.get("employee_count", ""))
            rows.append(row)

    if not rows:
        raise HTTPException(status_code=400, detail="Could not extract domains from companies.")

    # Create enrichment job
    cascade_json = json.dumps(blitz_client.DEFAULT_CASCADE)

    store.create_enrichment_job(
        job_id=job_id,
        user_id=current_user["user_id"],
        total=len(rows),
        filename=f"search_enrich_{job_id[:8]}",
        domain_col=domain_col,
        original_filename=f"search_enrich_{job_id[:8]}.csv",
        cascade_config=cascade_json,
        max_results=req.max_decision_makers,
    )

    _job_signals[job_id] = asyncio.Event()
    _active_jobs.add(job_id)

    background_tasks.add_task(
        _run_job,
        job_id=job_id,
        rows=rows,
        domain_col=domain_col,
        name_col=None,
        first_name_col=None,
        last_name_col=None,
        cascade=blitz_client.DEFAULT_CASCADE,
        max_results=req.max_decision_makers,
        write_incremental=True,
    )

    return {
        "job_id": job_id,
        "total": len(rows),
        "companies_found": len(companies),
        "flow": "search_and_enrich",
    }


# --- Flow 3: LinkedIn Enrichment ---

@router.post("/by-linkedin")
async def enrich_by_linkedin(
    req: LinkedInEnrichRequest,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(auth.get_current_user),
):
    """
    Flow 3: LinkedIn URLs → Full Enrichment

    Upload a CSV with LinkedIn URLs and get fully enriched data:
    - Person details (name, title, company)
    - Work email
    - Phone (if available)
    - Company details
    """
    upload_path = UPLOAD_DIR / f"{req.upload_id}.csv"
    if not upload_path.exists():
        raise HTTPException(status_code=404, detail="Upload not found.")

    df = pd.read_csv(str(upload_path), skipinitialspace=True)
    if req.linkedin_col not in df.columns:
        raise HTTPException(
            status_code=400, detail=f"Column '{req.linkedin_col}' not found in CSV."
        )

    rows = df.fillna("").astype(str).to_dict(orient="records")

    # Read metadata
    metadata_path = UPLOAD_DIR / f"{req.upload_id}.metadata.json"
    original_filename = ""
    if metadata_path.exists():
        try:
            metadata = json.loads(metadata_path.read_text())
            original_filename = metadata.get("original_filename", "")
        except Exception:
            pass

    job_id = str(uuid.uuid4())
    store = job_store.get_store()

    store.create_enrichment_job(
        job_id=job_id,
        user_id=current_user["user_id"],
        total=len(rows),
        filename=str(req.upload_id),
        domain_col=req.linkedin_col,  # Using domain_col to store linkedin_col reference
        original_filename=original_filename,
        name_col=None,
        first_name_col=None,
        last_name_col=None,
        cascade_config=None,
        max_results=1,
    )

    _job_signals[job_id] = asyncio.Event()
    _active_jobs.add(job_id)

    background_tasks.add_task(
        _run_linkedin_job,
        job_id=job_id,
        rows=rows,
        linkedin_col=req.linkedin_col,
        include_company=req.include_company,
    )

    return {"job_id": job_id, "total": len(rows), "flow": "linkedin_enrichment"}


async def _run_linkedin_job(
    job_id: str,
    rows: list[dict[str, Any]],
    linkedin_col: str,
    include_company: bool = True,
):
    """Background task to run LinkedIn enrichment job."""
    store = job_store.get_store()
    store.set_running(job_id)
    seq = [0]

    output_path = OUTPUT_DIR / f"{job_id}.csv"

    async def on_progress(e: dict[str, Any]):
        progress_store = job_store.get_store()
        progress_store.append_event(job_id, seq[0], e)
        seq[0] += 1
        sig = _job_signals.get(job_id)
        if sig:
            sig.set()
            sig.clear()

    try:
        output_rows = await list_builder.run_linkedin_enrichment(
            rows=rows,
            linkedin_col=linkedin_col,
            on_progress=on_progress,
        )

        if output_rows:
            out_df = pd.DataFrame(output_rows)
            out_df.to_csv(str(output_path), index=False)
        else:
            output_path.write_text("")

        store.set_done(job_id, str(output_path))
        logger.info("LinkedIn enrichment job %s completed, %d output rows", job_id, len(output_rows))

    except Exception as e:
        logger.exception("LinkedIn enrichment job %s failed: %s", job_id, e)
        if output_path.exists() and output_path.stat().st_size > 0:
            store.set_done(job_id, str(output_path))
        else:
            store.set_failed(job_id, str(e))

    finally:
        _active_jobs.discard(job_id)
        sig = _job_signals.pop(job_id, None)
        if sig:
            sig.set()


# --- Unified LinkedIn Enrichment (personal + company) ---

@router.post("/by-linkedin-v2")
async def enrich_by_linkedin_v2(
    req: LinkedInV2Request,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(auth.get_current_user),
):
    """
    Unified enrichment using personal and/or company LinkedIn URLs.

    Accepts a CSV upload and supports two LinkedIn columns:
    - personal_linkedin_col: personal LinkedIn profile URLs
    - company_linkedin_col: company LinkedIn Page URLs

    At least one column must be provided. The endpoint calls
    list_builder.run_unified_linkedin_enrichment() in the background.
    """
    # Step 1: Validate at least one column is provided
    if not req.personal_linkedin_col and not req.company_linkedin_col:
        raise HTTPException(
            status_code=400,
            detail="At least one of personal_linkedin_col or company_linkedin_col must be provided.",
        )

    # Step 2: Locate upload CSV
    upload_path = UPLOAD_DIR / f"{req.upload_id}.csv"
    if not upload_path.exists():
        raise HTTPException(status_code=404, detail="Upload not found.")

    # Step 3: Read CSV and validate columns
    df = pd.read_csv(str(upload_path), skipinitialspace=True)

    if req.personal_linkedin_col and req.personal_linkedin_col not in df.columns:
        raise HTTPException(
            status_code=400,
            detail=f"Column '{req.personal_linkedin_col}' not found in CSV.",
        )
    if req.company_linkedin_col and req.company_linkedin_col not in df.columns:
        raise HTTPException(
            status_code=400,
            detail=f"Column '{req.company_linkedin_col}' not found in CSV.",
        )

    rows = df.fillna("").astype(str).to_dict(orient="records")

    # Step 4: Count valid rows (rows with at least one URL)
    valid_count = sum(
        1
        for r in rows
        if (req.personal_linkedin_col and str(r.get(req.personal_linkedin_col, "")).strip())
        or (req.company_linkedin_col and str(r.get(req.company_linkedin_col, "")).strip())
    )

    # Step 5: Read original filename from metadata
    metadata_path = UPLOAD_DIR / f"{req.upload_id}.metadata.json"
    original_filename = ""
    if metadata_path.exists():
        try:
            metadata = json.loads(metadata_path.read_text())
            original_filename = metadata.get("original_filename", "")
        except Exception:
            pass

    # Step 6: Create enrichment job
    job_id = str(uuid.uuid4())
    store = job_store.get_store()

    store.create_enrichment_job(
        job_id=job_id,
        user_id=current_user["user_id"],
        total=valid_count,
        filename=str(req.upload_id),
        domain_col=None,
        original_filename=original_filename,
        name_col=None,
        first_name_col=None,
        last_name_col=None,
        cascade_config=None,
        max_results=req.max_dms,
    )

    _job_signals[job_id] = asyncio.Event()
    _active_jobs.add(job_id)

    # Step 7: Start background task
    background_tasks.add_task(
        _run_linkedin_v2_job,
        job_id=job_id,
        rows=rows,
        personal_linkedin_col=req.personal_linkedin_col,
        company_linkedin_col=req.company_linkedin_col,
        max_dms=req.max_dms,
        include_company=req.include_company,
    )

    return {"job_id": job_id, "total": valid_count, "flow": "linkedin_v2_enrichment"}


async def _run_linkedin_v2_job(
    job_id: str,
    rows: list[dict[str, Any]],
    personal_linkedin_col: Optional[str],
    company_linkedin_col: Optional[str],
    max_dms: int,
    include_company: bool,
):
    """Background task to run unified LinkedIn (personal + company) enrichment job."""
    store = job_store.get_store()
    store.set_running(job_id)
    seq = [0]

    output_path = OUTPUT_DIR / f"{job_id}.csv"

    async def on_progress(e: dict[str, Any]):
        progress_store = job_store.get_store()
        progress_store.append_event(job_id, seq[0], e)
        seq[0] += 1
        sig = _job_signals.get(job_id)
        if sig:
            sig.set()
            sig.clear()

    try:
        output_rows = await list_builder.run_unified_linkedin_enrichment(
            rows=rows,
            personal_col=personal_linkedin_col,
            company_col=company_linkedin_col,
            max_dms=max_dms,
            include_company=include_company,
            on_progress=on_progress,
        )

        if output_rows:
            out_df = pd.DataFrame(output_rows)
            out_df.to_csv(str(output_path), index=False)
        else:
            output_path.write_text("")

        store.set_done(job_id, str(output_path))
        logger.info(
            "LinkedIn v2 enrichment job %s completed, %d output rows",
            job_id,
            len(output_rows),
        )

    except Exception as e:
        logger.exception("LinkedIn v2 enrichment job %s failed: %s", job_id, e)
        if output_path.exists() and output_path.stat().st_size > 0:
            store.set_done(job_id, str(output_path))
        else:
            store.set_failed(job_id, str(e))

    finally:
        _active_jobs.discard(job_id)
        sig = _job_signals.pop(job_id, None)
        if sig:
            sig.set()


# --- Search Filter Options Endpoint ---

@router.get("/search/options")
async def get_search_options(_current_user: dict = Depends(auth.get_current_user)):
    """
    Get available search filter options for Flow 2.

    Returns normalized values for:
    - Industries
    - Employee ranges
    - Company types
    - Countries
    - Job levels
    - Job functions
    - Sales regions
    """
    return {
        "industries": [
            "Accounting", "Airlines and Aviation", "Animation", "Apparel and Fashion",
            "Architecture and Planning", "Automotive", "Banking", "Biotechnology",
            "Broadcast Media", "Computer Software", "Construction", "Defense and Space",
            "E-Learning", "Education Management", "Electrical/Electronic Manufacturing",
            "Entertainment", "Financial Services", "Food and Beverages",
            "Government Administration", "Health, Wellness and Fitness",
            "Hospital and Health Care", "Hospitality", "Information Technology and Services",
            "Insurance", "Internet", "Legal Services", "Logistics and Supply Chain",
            "Marketing and Advertising", "Mechanical or Industrial Engineering",
            "Medical Devices", "Music", "Non-Profit Organization Management",
            "Oil and Energy", "Pharmaceuticals", "Professional Training and Coaching",
            "Real Estate", "Restaurants", "Retail", "Security and Investigations",
            "Sports", "Staffing and Recruiting", "Telecommunications",
            "Venture Capital and Private Equity",
        ],
        "employee_ranges": [
            "1-10", "11-50", "51-200", "201-500",
            "501-1000", "1001-5000", "5001-10000", "10001+"
        ],
        "company_types": [
            "Educational", "Government Agency", "Nonprofit", "Partnership",
            "Privately Held", "Public Company", "Self-Employed"
        ],
        "countries": [
            {"code": "US", "name": "United States"},
            {"code": "GB", "name": "United Kingdom"},
            {"code": "CA", "name": "Canada"},
            {"code": "DE", "name": "Germany"},
            {"code": "FR", "name": "France"},
            {"code": "AU", "name": "Australia"},
            {"code": "NL", "name": "Netherlands"},
            {"code": "IN", "name": "India"},
            {"code": "JP", "name": "Japan"},
            {"code": "BR", "name": "Brazil"},
            {"code": "SG", "name": "Singapore"},
            {"code": "SE", "name": "Sweden"},
        ],
        "job_levels": [
            "C-Team", "VP", "Director", "Manager", "Staff", "Other"
        ],
        "job_functions": [
            "Advertising & Marketing", "Art, Culture and Creative Professionals",
            "Construction", "Customer/Client Service", "Education", "Engineering",
            "Finance & Accounting", "General Business & Management",
            "Healthcare & Human Services", "Human Resources", "Information Technology",
            "Legal", "Manufacturing & Production", "Operations",
            "Public Administration & Safety", "Purchasing", "Research & Development",
            "Sales & Business Development", "Science", "Supply Chain & Logistics",
            "Writing/Editing"
        ],
        "sales_regions": [
            {"code": "NORAM", "name": "North America"},
            {"code": "LATAM", "name": "Latin America"},
            {"code": "EMEA", "name": "Europe, Middle East, Africa"},
            {"code": "APAC", "name": "Asia-Pacific"},
        ],
    }


# --- Source Statistics Endpoint ---

@router.get("/stats/sources")
async def get_source_stats(
    start_date: Optional[str] = Query(None, description="Start date (ISO format)"),
    end_date: Optional[str] = Query(None, description="End date (ISO format)"),
    current_user: dict = Depends(auth.get_current_user),
):
    """
    Get aggregated enrichment source statistics.

    Returns counts of emails found per source provider.
    """
    from . import stats_store

    # Admin can see all, regular users see only their own
    user_id = None
    if not current_user.get("is_admin"):
        user_id = current_user.get("user_id")

    totals = stats_store.EnrichmentStatsStore.get_total_stats(
        user_id=user_id,
        start_date=start_date,
        end_date=end_date,
    )

    grand_total = sum(totals.values()) if totals else 0

    return {
        "totals": totals,
        "grand_total": grand_total,
        "breakdown": {
            source: {
                "count": count,
                "percentage": round(count / grand_total * 100, 1) if grand_total > 0 else 0,
            }
            for source, count in totals.items()
        },
        "date_range": {"start": start_date, "end": end_date},
    }
