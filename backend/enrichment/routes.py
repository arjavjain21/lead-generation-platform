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
import time
import sqlite3
import tempfile
import uuid
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import httpx
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

from shared import auth, db
from . import blitz_client
from . import contacts_client
from . import job_store
from . import pipeline
from . import list_builder
from . import better_enrich_client
from . import wizleads_client
from . import providers
from . import identifier_utils
from . import contacts_writer
from .raw_contact_collector import RawContactCollector
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
        "include_headline_search": True,
    }]


def _build_contacts_writer_payloads(
    contacts: list[dict],
    domain: str,
    *,
    job_id: Optional[str] = None,
) -> list[dict]:
    """Map routes.py contact dicts (email/full_name/title/linkedin_url) to
    contacts_writer payloads (dm_email/dm_full_name/dm_title/dm_linkedin_url).
    Skips contacts without a meaningful email."""
    payloads: list[dict] = []
    for idx, c in enumerate(contacts):
        email = (c.get("email") or "").strip()
        if not email or "@" not in email or email.lower() in {"no_email", "n/a", "none"}:
            continue
        payloads.append({
            "dm_email": email,
            "dm_full_name": (c.get("full_name") or "").strip(),
            "dm_first_name": (c.get("first_name") or "").strip(),
            "dm_last_name": (c.get("last_name") or "").strip(),
            "dm_title": (c.get("title") or "").strip(),
            "dm_linkedin_url": (c.get("linkedin_url") or "").strip(),
            "domain": domain,
            "job_id": job_id,
            "row_index": idx,
            "source_path": (c.get("email_source") or "").strip(),
        })
    return payloads


def _csv_rows_to_payloads(output_path: Path) -> list[dict]:
    """Read an enrichment job's output CSV and convert each row to a
    contacts_writer payload. CSV columns already use dm_ prefix."""
    payloads: list[dict] = []
    if not output_path.exists():
        return payloads
    with open(output_path, newline="", encoding="utf-8") as f:
        for idx, row in enumerate(csv.DictReader(f)):
            email = (row.get("dm_email") or "").strip()
            if not email or "@" not in email or email.lower() in {"no_email", "n/a", "none"}:
                continue
            payloads.append({
                "dm_email": email,
                "dm_full_name": (row.get("dm_full_name") or "").strip(),
                "dm_first_name": (row.get("dm_first_name") or "").strip(),
                "dm_last_name": (row.get("dm_last_name") or "").strip(),
                "dm_title": (row.get("dm_title") or "").strip(),
                "dm_linkedin_url": (row.get("dm_linkedin_url") or "").strip(),
                "domain": (row.get("domain") or "").strip(),
                "job_id": (row.get("job_id") or "").strip() or None,
                "row_index": idx,
                "source_path": (row.get("dm_email_source") or row.get("source_path") or "").strip(),
            })
    return payloads


async def _run_contacts_writer_v2(
    contacts: list[dict],
    domain: str,
    *,
    job_id: Optional[str] = None,
) -> tuple[dict, str]:
    """Call contacts_writer.write_enrichment_result_batch and translate its
    WriteResult into the legacy {synced, skipped, failed} response shape
    plus a new records_queued field. LoudFailure is re-raised so callers'
    outer try/except does not swallow operator-facing signals.

    Returns (sync_result_dict, sync_status_string).
    """
    payloads = _build_contacts_writer_payloads(contacts, domain, job_id=job_id)
    if not payloads:
        return {"synced": 0, "skipped": 0, "failed": 0, "records_queued": 0}, "no_contacts_to_sync"
    result = await contacts_writer.write_enrichment_result_batch(payloads, job_id=job_id)
    synced = result.inserted + result.updated
    sync_result = {
        "synced": synced,
        "skipped": result.skipped,
        "failed": result.failed,
        "records_queued": result.queued,
    }
    if result.failed == 0:
        sync_status = "success"
    elif synced == 0:
        sync_status = "failed"
    else:
        sync_status = "partial"
    return sync_result, sync_status


# Valid provider values for force_provider / selected_providers parameter
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
        selected_providers: Optional allowlist from request (or None for all enabled).
            Mirrors list_builder.py semantics — contacts_db is always allowed
            even if not in the list.

    Returns:
        True if the provider should be skipped, False otherwise

    Checks (in priority order):
      1. Global ENABLED_PROVIDERS kill switch (beats everything below)
      2. force_provider (if set) — only that provider passes
      3. selected_providers (if set) — only those providers pass; contacts_db
         is always allowed
    """
    # Global enablement check — beats force_provider and selected_providers.
    # If a provider is globally disabled, no per-request override re-enables it.
    if not providers.is_provider_enabled(provider):
        logger.debug("_should_skip_provider: %s disabled in ENABLED_PROVIDERS", provider)
        return True

    # force_provider takes precedence over selected_providers - if set, only
    # use that provider.
    if force_provider:
        return provider != force_provider

    # selected_providers is an allowlist. contacts_db is always allowed
    # (mandatory first step), matching list_builder.py convention.
    if selected_providers is not None:
        if provider == "contacts_db":
            return False
        return provider not in selected_providers

    return False


def _build_routing_response(
    route: dict[str, Any],
    route_result: dict[str, Any],
    *,
    debug: bool = False,
) -> dict[str, Any]:
    """Build the `routing` block of the unified enrich response.

    When `debug=False` (the default), the response is compact: only the
    source_path, no_email_reason, and the legacy provider_attempts list
    (human-readable strings) are returned. This keeps the default response
    size small for production traffic.

    When `debug=True`, the response includes the full structured
    `provider_attempts_json`, `providers_called`, `providers_skipped`,
    `final_email_status`, and `final_email_verification_source` fields
    so callers can inspect every provider call in detail.
    """
    # Extract provider errors from provider_attempts_json
    provider_errors = []
    attempts_json = route_result.get("provider_attempts_json", [])
    for attempt in attempts_json:
        if attempt.get("error_type"):
            provider_errors.append({
                "provider": attempt.get("provider", ""),
                "method": attempt.get("method", ""),
                "error_type": attempt.get("error_type", ""),
                "message": _get_error_message_from_attempt(attempt),
            })

    base = {
        "mode": route.get("mode", ""),
        "source_path": route_result.get("source_path", ""),
        "provider_attempts": route_result.get("provider_attempts", []),
        "no_email_reason": route_result.get("no_email_reason", ""),
        "provider_errors": provider_errors,  # Always include for visibility
    }
    if not debug:
        return base

    return {
        **base,
        "provider_attempts_json": route_result.get("provider_attempts_json", []),
        "providers_called": route_result.get("providers_called", []),
        "providers_skipped": route_result.get("providers_skipped", []),
        "final_email_status": route_result.get("final_email_status", ""),
        "final_email_verification_source": route_result.get(
            "final_email_verification_source", ""
        ),
    }


def _build_domain_only_fallback_route_result(
    data_sources: dict[str, str],
    company_linkedin_url: str,
) -> dict[str, Any]:
    """Synthesize a route_result for domain_only mode showing what was tried.

    The domain_only cascade doesn't use ``route_enrichment`` (which produces
    the structured attempts list automatically for the per-person cascades).
    Instead it uses ``_enrich_domain`` (POST endpoint) or inline helpers
    (GET endpoint) that try Contacts DB and Blitz at the company level before
    falling back to per-DM lookups.

    Without this helper, domain_only responses with 0 contacts show
    ``provider_attempts=[]``, which looks like the system didn't try
    anything. That's misleading — we DID try Contacts DB and Blitz, we
    just couldn't surface it through the routing layer.

    This helper reconstructs the attempts list from the ``data_sources``
    signals (which ARE populated by the domain_only handlers) so the
    response accurately reflects what was attempted.

    Args:
        data_sources: The ``data_sources`` dict from the response. Must
            include ``company_linkedin`` and ``contacts`` keys.
        company_linkedin_url: The resolved company LinkedIn URL (empty
            string when not found).

    Returns:
        A synthetic route_result dict with ``mode``, ``source_path``,
        ``provider_attempts``, and ``no_email_reason`` populated to
        reflect the actual domain_only cascade attempts.
    """
    company_source = data_sources.get("company_linkedin", "")
    contacts_source = data_sources.get("contacts", "")

    attempts: list[str] = []

    # Company LinkedIn URL resolution: Contacts DB is always tried first.
    attempts.append("company_by_domain@contacts_db")
    # Blitz domain_to_linkedin is tried when Contacts DB missed or errored.
    # We can infer this: if company_source is "blitz" or "not_found", Contacts
    # DB didn't return a usable URL, so Blitz was attempted as the fallback.
    if company_source in ("blitz", "not_found"):
        attempts.append("domain_to_linkedin@blitz")

    # Decision-maker discovery.
    # Contacts DB company_contacts_enriched runs unconditionally (it doesn't
    # need a LinkedIn URL). Blitz waterfall_icp_search DOES need one.
    attempts.append("company_contacts_enriched@contacts_db")
    if company_linkedin_url:
        attempts.append("waterfall_icp_search@blitz")

    # Determine the most-specific no_email_reason.
    if not company_linkedin_url:
        no_email_reason = pipeline.NO_EMAIL_REASON_DOMAIN_ONLY_NO_LINKEDIN
        source_path = (
            "domain -> contacts_db.company_by_domain (no match) -> "
            "blitz.domain_to_linkedin (no match) -> no company LinkedIn URL -> "
            "cascade cannot proceed (Blitz waterfall requires LinkedIn URL)"
        )
    elif contacts_source == "not_found":
        no_email_reason = pipeline.NO_EMAIL_REASON_DOMAIN_ONLY_NO_CONTACTS
        source_path = (
            "domain -> company LinkedIn found -> "
            "contacts_db.company_contacts_enriched (no DMs) -> "
            "blitz.waterfall_icp_search (no DMs) -> no decision makers"
        )
    else:
        # DMs were found but none yielded an email — per-DM cascades ran
        # and would have populated last_route_result normally. This branch
        # is a defensive fallback and should rarely trigger.
        no_email_reason = ""
        source_path = "domain -> company found -> DMs found -> per-DM cascade ran"

    return {
        "mode": "domain_only",
        "source_path": source_path,
        "provider_attempts": attempts,
        "no_email_reason": no_email_reason,
        "provider_attempts_json": [],
        "providers_called": [],
        "providers_skipped": [],
    }


def _get_error_message_from_attempt(attempt: dict[str, Any]) -> str:
    """Generate a user-friendly error message from a provider attempt record."""
    provider = attempt.get("provider", "")
    error_type = attempt.get("error_type", "")

    # Map error types to user-friendly messages
    error_messages = {
        "insufficient_credits": f"{provider}: Insufficient credits. Please top up to continue.",
        "authentication_failed": f"{provider}: Authentication failed. Check API key.",
        "rate_limited": f"{provider}: Rate limited. Please try again later.",
        "service_unavailable": f"{provider}: Service temporarily unavailable. Please try again.",
    }

    return error_messages.get(error_type, f"{provider}: An error occurred.")


DATA_DIR = Path(__file__).parent.parent / "data"
UPLOAD_DIR = DATA_DIR / "uploads"
OUTPUT_DIR = DATA_DIR / "outputs"
DATA_DIR.mkdir(exist_ok=True)
UPLOAD_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)

# Upper bound on how many jobs the `file_ready` filter scans for on-disk output.
# output_exists is a per-job filesystem check (not a DB column), so the filter
# fetches candidates then keeps only those whose file is present; this caps that
# scan. Enrichment jobs are in the hundreds today — the cap is a backstop, and a
# log line fires if it's ever reached.
FILE_READY_SCAN_CAP = 2000

router = APIRouter(prefix="/api/enrichment", tags=["enrichment"])

# In-memory set of job_ids currently being actively processed
_active_jobs: set[str] = set()
# Per-job asyncio Event to wake SSE consumers
_job_signals: dict[str, asyncio.Event] = {}
# Set of jobs that have been cancelled by user
_cancelled_jobs: set[str] = set()


# ---------------------------------------------------------------------------
# Enrichment concurrency & HTTP-client hardening (2026-07-28)
#
# The /enrich hot path previously had NO process-global bound on concurrent
# cascades: every request built its own per-request semaphores AND two fresh
# httpx.AsyncClient()s with no connection limits, so aggregate fan-in scaled
# linearly with request count — the OOM driver under the 2026-07-27 AWS flood.
#
# _ENRICH_SEMAPHORE caps concurrent in-flight cascades PER WORKER. With N
# gunicorn workers the effective app-wide ceiling is N x ENRICH_MAX_CONCURRENT.
# Requests that cannot acquire within ENRICH_QUEUE_TIMEOUT get a fast 429
# (cheap) instead of piling up in memory. Both are env-tunable (no redeploy).
#
# The shared cascade httpx clients (Limits + Timeout, mirroring the batch path
# at pipeline.py ~L2902) are reused across requests — caps connection memory
# and kills per-request TLS/pool churn. They are NOT closed per-request.
ENRICH_MAX_CONCURRENT = int(os.getenv("ENRICH_MAX_CONCURRENT", "16"))
ENRICH_QUEUE_TIMEOUT = float(os.getenv("ENRICH_QUEUE_TIMEOUT", "5.0"))
_ENRICH_SEMAPHORE = asyncio.Semaphore(ENRICH_MAX_CONCURRENT)

_ENRICH_HTTP_LIMITS = httpx.Limits(
    max_keepalive_connections=20,
    max_connections=100,
    keepalive_expiry=30.0,
)
_ENRICH_HTTP_TIMEOUT = httpx.Timeout(30.0, connect=10.0)
_shared_blitz_http: Optional[httpx.AsyncClient] = None
_shared_contacts_http: Optional[httpx.AsyncClient] = None


def _get_blitz_http() -> httpx.AsyncClient:
    """Lazy shared cascade client (one per worker). Do NOT close per-request."""
    global _shared_blitz_http
    if _shared_blitz_http is None:
        _shared_blitz_http = httpx.AsyncClient(
            limits=_ENRICH_HTTP_LIMITS, timeout=_ENRICH_HTTP_TIMEOUT
        )
    return _shared_blitz_http


def _get_contacts_http() -> httpx.AsyncClient:
    """Lazy shared cascade client (one per worker). Do NOT close per-request."""
    global _shared_contacts_http
    if _shared_contacts_http is None:
        _shared_contacts_http = httpx.AsyncClient(
            limits=_ENRICH_HTTP_LIMITS, timeout=_ENRICH_HTTP_TIMEOUT
        )
    return _shared_contacts_http


async def _acquire_enrich_slot() -> None:
    """Acquire a global cascade slot, or fail fast with HTTP 429 if saturated.

    Raises HTTPException(429, Retry-After) on timeout so the client backs off
    instead of the request piling up in worker memory.
    """
    try:
        await asyncio.wait_for(_ENRICH_SEMAPHORE.acquire(), ENRICH_QUEUE_TIMEOUT)
    except asyncio.TimeoutError:
        raise HTTPException(
            status_code=429,
            detail="Enrichment API at capacity, please retry shortly.",
            headers={"Retry-After": "3"},
        )


# ---------------------------------------------------------------------------
# /enrich response cache (2026-07-28)
#
# Measured on live traffic: ~60% of /enrich requests are repeat queries for the
# same domain within 5 minutes. This short-TTL cache serves those instantly,
# skipping the whole cascade (contacts_db -> blitz -> mailtester -> sync). Cache
# HITS also bypass the concurrency semaphore entirely (so they never 429 and
# consume no slot). Net effect at the current rate: ~60% fewer provider calls.
# Gated by ENRICH_RESPONSE_CACHE (default on); TTL/maxsize env-tunable. Bypassed
# when debug=True. GET path only (that is 100% of Clay's traffic).
ENRICH_RESPONSE_CACHE = os.getenv("ENRICH_RESPONSE_CACHE", "true").lower() in ("1", "true", "yes", "on")
ENRICH_CACHE_TTL = float(os.getenv("ENRICH_CACHE_TTL", "300"))
ENRICH_CACHE_MAX = int(os.getenv("ENRICH_CACHE_MAX", "10000"))
_enrich_response_cache: dict[str, tuple[float, Any]] = {}


def _enrich_cache_key(req: Any) -> str:
    """Stable cache key from the request fields that affect the result.

    Domain is scheme/slash-normalized so `http://x.com/` and `x.com` share an
    entry. Uses getattr defensively so an unexpected request shape can never
    500 the endpoint (worst case: a cache miss).
    """
    dom = (getattr(req, "domain", None) or "").strip().lower()
    for pref in ("https://", "http://"):
        if dom.startswith(pref):
            dom = dom[len(pref):]
    dom = dom.strip("/")
    sp = getattr(req, "selected_providers", None) or []
    sp = ",".join(sorted(sp))
    titles = getattr(req, "titles", None)
    if isinstance(titles, (list, tuple)):
        titles = ",".join(str(t) for t in titles)
    else:
        titles = str(titles or "")
    cascade = getattr(req, "cascade", None)
    cascade = json.dumps(cascade, sort_keys=True, default=str) if cascade else ""
    return "|".join((
        "v1",
        dom,
        (getattr(req, "full_name", None) or "").strip().lower(),
        (getattr(req, "first_name", None) or "").strip().lower(),
        (getattr(req, "last_name", None) or "").strip().lower(),
        (getattr(req, "linkedin_url", None) or "").strip(),
        (getattr(req, "company_linkedin_url", None) or "").strip(),
        getattr(req, "force_provider", None) or "",
        sp,
        str(getattr(req, "max_results", None)),
        titles,
        getattr(req, "source", None) or "",
        cascade,
    ))


def _enrich_cache_get(key: str) -> Optional[Any]:
    """Return a cached response if present and unexpired, else None."""
    if not ENRICH_RESPONSE_CACHE:
        return None
    entry = _enrich_response_cache.get(key)
    if entry is None:
        return None
    expiry, value = entry
    if expiry > time.monotonic():
        return value
    _enrich_response_cache.pop(key, None)
    return None


def _enrich_cache_set(key: str, value: Any) -> None:
    """Store a response with a TTL; evict the soonest-expiring entry when full."""
    if not ENRICH_RESPONSE_CACHE:
        return
    if len(_enrich_response_cache) >= ENRICH_CACHE_MAX:
        try:
            oldest = min(_enrich_response_cache, key=lambda k: _enrich_response_cache[k][0])
            _enrich_response_cache.pop(oldest, None)
        except ValueError:
            pass
    _enrich_response_cache[key] = (time.monotonic() + ENRICH_CACHE_TTL, value)


# Hit/miss counters for the response cache (per-worker). Mutated via helpers so
# no `global` declaration is needed; increments are single statements with no
# await -> safe under concurrency within one worker's event loop. Logged every
# 100 lookups so all 4 workers aggregate in journald for live hit-rate reading.
_ENRICH_CACHE_STATS: dict[str, int] = {"hits": 0, "misses": 0}


def _enrich_cache_record_hit() -> None:
    _ENRICH_CACHE_STATS["hits"] += 1
    _enrich_cache_maybe_log()


def _enrich_cache_record_miss() -> None:
    _ENRICH_CACHE_STATS["misses"] += 1
    _enrich_cache_maybe_log()


def _enrich_cache_maybe_log() -> None:
    s = _ENRICH_CACHE_STATS
    lookups = s["hits"] + s["misses"]
    if lookups > 0 and lookups % 100 == 0:
        logger.info(
            "enrich_cache: hits=%d misses=%d hit_rate=%.1f%% size=%d",
            s["hits"], s["misses"], 100.0 * s["hits"] / lookups,
            len(_enrich_response_cache),
        )


# ---------------------------------------------------------------------------
# JSON Response Freeze (Option C Phase 3, 2026-07-07)
#
# These 6 fields are populated internally by Phase 2's cascade collector
# wiring but must NOT appear in the JSON API response. External consumers
# (Clay, Zapier, custom scripts) must see byte-for-byte identical JSON
# before and after Phase 2.
#
# CSV downloads intentionally include these fields (separate code path).
#
# See: docs/RESPONSE_SHAPE_BASELINE_2026-07-07.md for the full allowlist.
# ---------------------------------------------------------------------------

# Row-level fields that get stripped from contact objects in JSON responses.
# NOTE: routing.provider_errors is a DIFFERENT concept (built by
# _build_routing_response at the routing-block level) and is NOT stripped.
_ROW_LEVEL_INTERNAL_FIELDS: frozenset[str] = frozenset({
    "company_name",
    "company_industry",
    "company_employee_count",
    "dm_job_level",
    "dm_job_function",
    "provider_errors",  # row-level only; routing.provider_errors stays
})


def _strip_internal_fields_from_response(response: Any) -> Any:
    """
    Remove the 6 internal-only fields from contact objects in the response.

    Applies to:
    - response["contacts"][*]  (the contact object list)

    Does NOT touch:
    - response["routing"]["provider_errors"]  (different concept, different location)
    - Top-level keys, data_sources, sync_to_contacts_db
    - CSV column data (CSV is generated separately, not from this response)

    Idempotent: running twice == running once. O(n) over contacts list.
    Returns the response unchanged if it's not a dict or has no contacts list.
    """
    if not isinstance(response, dict):
        return response

    # Shallow copy so we don't mutate the caller's dict.
    response = dict(response)

    contacts = response.get("contacts")
    if isinstance(contacts, list):
        response["contacts"] = [
            {
                k: v
                for k, v in c.items()
                if k not in _ROW_LEVEL_INTERNAL_FIELDS
            }
            if isinstance(c, dict)
            else c
            for c in contacts
        ]

    return response


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
# In-API documentation (mirror of docs/LIST_BUILDING_API_2026-07-05.md)
# ---------------------------------------------------------------------------

_LIST_BUILDING_HELP_PAYLOAD: dict[str, Any] = {
    "generated_at": "2026-07-05",
    "document_version": "1.0",
    "base_url": "https://listbuilding.eagleinfoservice.com",
    "local_backend": "http://localhost:8765",
    "markdown_file": "docs/LIST_BUILDING_API_2026-07-05.md",
    "auth": {
        "credentials": [
            {
                "type": "JWT bearer",
                "header": "Authorization: Bearer <token>",
                "obtained_from": "POST /api/auth/login",
                "expiry": "7 days",
                "accepted_on": "all endpoints",
            },
            {
                "type": "API key",
                "header": "X-API-Key: <key>  OR  Authorization: Bearer <key>",
                "obtained_from": "POST /api/api-keys",
                "expiry": "does not expire until revoked",
                "accepted_on": "single /enrich, search, providers, stats, /flows/help (NOT upload/jobs/flows)",
            },
        ],
        "note": "CSV upload, job create/list/cancel/restart, downloads, Flow 1 and Flow 3 require JWT — API key is not accepted.",
    },
    "providers": {
        "cascade_order": [
            {"name": "contacts_db", "rate": "75 RPS", "role": "Internal PostgreSQL DB, always first, free"},
            {"name": "blitz", "rate": "25 RPS", "role": "LinkedIn-based enrichment with title cascade"},
            {"name": "smartprospect", "rate": "30 RPS", "role": "SmartLead Find Emails — self-verifying person-email finder, batch up to 10. Gates on firstName+lastName+domain."},
            {"name": "wizleads", "rate": "10 RPS", "role": "Catch-all verified email enrichment"},
            {"name": "better_enrich", "rate": "10 RPS", "role": "Person + company email (final fallback)"},
            {"name": "prospeo", "rate": "n/a", "role": "DISABLED — code present, end-to-end off"},
        ],
        "cascade_behavior": "Stop on first provider that returns a usable contact. Later providers are skipped for that row.",
        "title_tiers_default": {
            "tier_1": ["Owner", "CEO", "Founder", "Co-Founder", "President"],
            "tier_2": ["CMO", "CTO", "COO", "VP-level"],
            "tier_3": ["Director of Marketing", "Director of Sales", "Head of Marketing"],
        },
        "domain_normalization": "Raw URLs like https://mesterh-service.de/?utm_source=gmb are normalized to mesterh-service.de before any provider call.",
        "email_verification": "Contacts DB emails run through MailTester (validation.hyperke.org). Invalid emails are marked and cascade continues.",
    },
    "endpoints": [
        {
            "method": "GET",
            "path": "/api/enrichment/flows/help",
            "auth": "none",
            "summary": "This documentation as JSON.",
            "response_shape": "{ generated_at, base_url, auth, providers, endpoints[], examples, errors }",
        },
        {
            "method": "GET",
            "path": "/api/enrichment/providers",
            "auth": "jwt or api key",
            "summary": "List currently enabled providers.",
            "response_shape": '{"providers": ["contacts_db","blitz","smartprospect","wizleads","better_enrich"]}',
        },
        {
            "method": "GET",
            "path": "/api/enrichment/default-cascade",
            "auth": "none",
            "summary": "Default 3-tier title cascade used by Blitz.",
            "response_shape": '{"cascade": [{include_title, exclude_title, location, include_headline_search}, ...]}',
        },
        {
            "method": "GET",
            "path": "/api/enrichment/search/options",
            "auth": "jwt or api key",
            "summary": "Industries, employee ranges, company types, countries, job levels/functions, sales regions.",
        },
        {
            "method": "GET",
            "path": "/api/enrichment/stats/sources",
            "auth": "jwt or api key",
            "summary": "Email-source aggregation (admin sees all, users see own).",
            "query_params": [
                {"name": "start_date", "type": "ISO date", "required": "no"},
                {"name": "end_date", "type": "ISO date", "required": "no"},
            ],
        },
        {
            "method": "GET",
            "path": "/api/enrichment/enrich/{domain}",
            "auth": "jwt or api key",
            "summary": "Quick single-domain lookup with cascade.",
            "query_params": [
                {"name": "max_results", "type": "int", "default": "5"},
                {"name": "cascade_json", "type": "string (URL-encoded JSON)", "required": "no"},
                {"name": "force_provider", "type": "string", "required": "no", "values": ["contacts_db", "blitz", "smartprospect", "wizleads", "better_enrich"]},
            ],
        },
        {
            "method": "GET",
            "path": "/api/enrichment/enrich",
            "auth": "jwt or api key",
            "summary": "Unified single lookup (GET form). Same params as POST below, plus debug=bool.",
        },
        {
            "method": "POST",
            "path": "/api/enrichment/enrich",
            "auth": "jwt or api key",
            "summary": "Unified single lookup with provider cascade + sync to Contacts DB. Auto-detects mode: domain_only / linkedin_only / enhanced.",
            "body_fields": [
                {"name": "domain", "type": "string", "required": "one of domain|linkedin_url"},
                {"name": "linkedin_url", "type": "string", "required": "one of domain|linkedin_url"},
                {"name": "full_name", "type": "string"},
                {"name": "first_name", "type": "string", "notes": "use with last_name"},
                {"name": "last_name", "type": "string", "notes": "use with first_name"},
                {"name": "phone", "type": "string"},
                {"name": "company_name", "type": "string"},
                {"name": "existing_email", "type": "string"},
                {"name": "max_results", "type": "int", "default": "5", "range": "1-10"},
                {"name": "titles", "type": "string", "notes": "comma-separated, e.g. 'CEO,CTO', max 50"},
                {"name": "cascade", "type": "array<dict>", "notes": "advanced — overrides titles"},
                {"name": "force_provider", "type": "string", "values": ["contacts_db", "blitz", "smartprospect", "wizleads", "better_enrich"]},
            ],
            "query_params": [{"name": "debug", "type": "bool", "default": "false", "notes": "adds routing block"}],
            "modes": {
                "domain_only": "domain only — full cascade, all decision makers",
                "linkedin_only": "linkedin_url only — specific person via cascade",
                "enhanced": "domain + full_name/linkedin_url — specific person only; 0 results if not found",
            },
        },
        {
            "method": "POST",
            "path": "/api/enrichment/upload",
            "auth": "jwt only",
            "summary": "Upload CSV, returns upload_id + column preview.",
            "content_type": "multipart/form-data",
            "form_field": "file (must end in .csv)",
            "response_shape": '{"upload_id","columns","preview","row_count","filename"}',
        },
        {
            "method": "POST",
            "path": "/api/enrichment/jobs",
            "auth": "jwt only",
            "summary": "Start basic enrichment job (legacy — prefer /flows/domain-enrich).",
            "body_fields": [
                {"name": "upload_id", "type": "string", "required": True},
                {"name": "domain_col", "type": "string", "required": True},
                {"name": "name_col", "type": "string"},
                {"name": "first_name_col", "type": "string"},
                {"name": "last_name_col", "type": "string"},
                {"name": "linkedin_url_col", "type": "string"},
                {"name": "phone_col", "type": "string"},
                {"name": "company_name_col", "type": "string"},
                {"name": "existing_email_col", "type": "string"},
                {"name": "cascade", "type": "array<dict>"},
                {"name": "max_results", "type": "int", "default": "5"},
                {"name": "force_provider", "type": "string"},
                {"name": "validate_email", "type": "bool", "default": "true"},
            ],
            "response_shape": '{"job_id","total"}',
        },
        {
            "method": "GET",
            "path": "/api/enrichment/jobs",
            "auth": "jwt only",
            "summary": "List my jobs (admin sees all).",
            "response_shape": '{"jobs": [{job_id, status, total, processed, emails_found, ...}]}',
        },
        {
            "method": "GET",
            "path": "/api/enrichment/jobs/{job_id}",
            "auth": "jwt only",
            "summary": "Job status + config.",
            "status_values": ["queued", "running", "completed", "failed", "cancelled", "partial"],
        },
        {
            "method": "GET",
            "path": "/api/enrichment/jobs/{job_id}/stream",
            "auth": "jwt only",
            "summary": "SSE progress stream with replay; closes on terminal status.",
            "content_type": "text/event-stream",
        },
        {
            "method": "GET",
            "path": "/api/enrichment/jobs/{job_id}/download",
            "auth": "jwt only",
            "summary": "Full enriched CSV (works for completed/failed/partial).",
            "content_type": "text/csv",
        },
        {
            "method": "GET",
            "path": "/api/enrichment/jobs/{job_id}/partial-download",
            "auth": "jwt only",
            "summary": "Whatever has been written so far for running jobs.",
            "content_type": "text/csv",
        },
        {
            "method": "POST",
            "path": "/api/enrichment/jobs/{job_id}/restart",
            "auth": "jwt only",
            "summary": "Restart failed/abandoned job. Re-reads original CSV, skips processed rows, dedupes.",
            "response_shape": '{"job_id","total","restarted_from","deduped_count"}',
        },
        {
            "method": "POST",
            "path": "/api/enrichment/jobs/{job_id}/cancel",
            "auth": "jwt only",
            "summary": "Cancel running/queued job. Partial results remain downloadable.",
            "response_shape": '{"job_id","status":"cancelled","message"}',
        },
        {
            "method": "POST",
            "path": "/api/enrichment/flows/domain-enrich",
            "auth": "jwt only",
            "summary": "FLOW 1 (recommended): domains → generic emails + decision makers, with provider selection, fuzzy titles, dedupe.",
            "body_fields": [
                {"name": "upload_id", "type": "string", "required": True},
                {"name": "domain_col", "type": "string", "required": True},
                {"name": "name_col", "type": "string"},
                {"name": "first_name_col", "type": "string"},
                {"name": "last_name_col", "type": "string"},
                {"name": "linkedin_url_col", "type": "string"},
                {"name": "phone_col", "type": "string"},
                {"name": "company_name_col", "type": "string"},
                {"name": "existing_email_col", "type": "string"},
                {"name": "max_results", "type": "int", "default": "5"},
                {"name": "providers", "type": "array<string>", "values": ["blitz", "wizleads", "better_enrich"], "notes": "contacts_db always runs first and cannot be disabled"},
                {"name": "titles", "type": "string", "notes": "comma-separated fuzzy titles, max 50, e.g. 'dentist,orthodontist,dmd'"},
                {"name": "normalize_domains", "type": "bool", "default": "true"},
                {"name": "dedupe_by_domain", "type": "bool", "default": "true"},
            ],
            "response_shape": '{"job_id","total"}',
            "concurrency": "25 domains in parallel",
            "errors": [
                "404 — Upload not found.",
                "400 — Column '<name>' not found in CSV.",
                "400 — Titles cannot be empty",
                "400 — Maximum 50 titles allowed.",
                "400 — Invalid providers: [...].",
            ],
        },
        {
            "method": "POST",
            "path": "/api/enrichment/search/companies",
            "auth": "jwt or api key",
            "summary": "FLOW 2: search companies by criteria (returns matches only).",
            "body_fields": [
                {"name": "name", "type": "string"},
                {"name": "industry", "type": "array<string>"},
                {"name": "employee_range", "type": "array<string>", "examples": ["11-50", "51-200"]},
                {"name": "company_type", "type": "array<string>", "examples": ["Privately Held", "Public Company"]},
                {"name": "country_code", "type": "string", "format": "ISO 3166-1 alpha-2"},
                {"name": "limit", "type": "int", "default": "100"},
                {"name": "offset", "type": "int", "default": "0"},
            ],
            "response_shape": '{"count","total","results":[{domain, linkedin_url, name}]}',
        },
        {
            "method": "POST",
            "path": "/api/enrichment/search/companies/enrich",
            "auth": "jwt or api key",
            "summary": "FLOW 2 legacy: search + start enrichment job in one call.",
            "extra_fields": [
                {"name": "max_decision_makers", "type": "int", "default": "5"},
                {"name": "include_generic_emails", "type": "bool", "default": "true"},
            ],
            "response_shape": '{"job_id","total","companies_found"}',
        },
        {
            "method": "POST",
            "path": "/api/enrichment/by-linkedin-v2",
            "auth": "jwt only",
            "summary": "FLOW 3 (recommended): unified LinkedIn enrichment for personal AND/OR company URLs.",
            "body_fields": [
                {"name": "upload_id", "type": "string", "required": True},
                {"name": "personal_linkedin_col", "type": "string", "notes": "column with linkedin.com/in/..."},
                {"name": "company_linkedin_col", "type": "string", "notes": "column with linkedin.com/company/..."},
                {"name": "max_dms", "type": "int", "default": "5"},
                {"name": "include_company", "type": "bool", "default": "true"},
            ],
            "constraint": "At least one of personal_linkedin_col or company_linkedin_col must be supplied and must exist in the CSV.",
            "response_shape": '{"job_id","total","flow":"linkedin_v2_enrichment"}',
        },
        {
            "method": "POST",
            "path": "/api/enrichment/by-linkedin",
            "auth": "jwt only",
            "summary": "FLOW 3 legacy: personal LinkedIn URLs only.",
            "body_fields": [
                {"name": "upload_id", "type": "string", "required": True},
                {"name": "linkedin_col", "type": "string", "required": True},
                {"name": "include_company", "type": "bool", "default": "true"},
            ],
        },
        {
            "method": "POST",
            "path": "/api/enrichment/by-domains",
            "auth": "jwt only",
            "summary": "Legacy alias for /jobs — prefer /flows/domain-enrich.",
        },
    ],
    "output_csv_columns": [
        "company_linkedin_url", "company_name", "company_industry", "company_employee_count",
        "dm_first_name", "dm_last_name", "dm_full_name", "dm_title", "dm_job_level", "dm_job_function",
        "dm_linkedin_url", "dm_email", "dm_email_source", "dm_email_verified",
        "mailtester_code", "mailtester_message", "dm_phone", "dm_headline",
        "dm_location_city", "dm_location_country", "dm_icp_tier", "row_status",
        "input_domain", "input_full_name", "input_linkedin_url", "normalized_linkedin_url",
        "source_path", "provider_attempts", "providers_called", "providers_skipped",
        "final_email", "final_email_level",
    ],
    "errors": {
        "400": "Bad request (bad CSV, bad column, invalid provider, too many titles)",
        "401": "Missing/invalid token or API key",
        "403": "Authenticated but not allowed (e.g. another user's job)",
        "404": "Upload/job/domain not found",
        "429": "Daily API quota exceeded (non-admin only; 50K/day)",
        "500": "Internal error (see journalctl -u lead-generation-platform.service)",
        "503": "SQLite database locked (auto-retry with Retry-After header)",
    },
    "rate_limits": {
        "user_facing": "No per-user throttle on the enrichment API itself.",
        "upstream_rates_managed_internally": "Sliding window + exponential backoff",
        "daily_quota": "Non-admin: 50,000 requests/day tracked in daily_api_requests. Admin: unlimited.",
    },
    "examples": {
        "login": 'curl -X POST <base>/api/auth/login -H "Content-Type: application/json" -d \'{"email":"...","password":"..."}\'',
        "upload_csv": 'curl -X POST <base>/api/enrichment/upload -H "Authorization: Bearer $TOKEN" -F "file=@leads.csv"',
        "flow1_minimal": 'curl -X POST <base>/api/enrichment/flows/domain-enrich -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d \'{"upload_id":"<uuid>","domain_col":"domain"}\'',
        "flow1_dental": 'curl -X POST <base>/api/enrichment/flows/domain-enrich -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d \'{"upload_id":"<uuid>","domain_col":"domain","titles":"dentist,orthodontist,dmd,dds","max_results":3,"providers":["blitz","better_enrich"]}\'',
        "single_enrich": 'curl -X POST <base>/api/enrichment/enrich -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d \'{"domain":"google.com","titles":"CEO,CTO"}\'',
        "poll_status": "curl <base>/api/enrichment/jobs/$JOB_ID -H \"Authorization: Bearer $TOKEN\"",
        "download": "curl <base>/api/enrichment/jobs/$JOB_ID/download -H \"Authorization: Bearer $TOKEN\" -o enriched.csv",
        "sse_stream": "curl -N <base>/api/enrichment/jobs/$JOB_ID/stream -H \"Authorization: Bearer $TOKEN\"",
    },
    "support": {
        "logs": "journalctl -u lead-generation-platform.service -f",
        "health": "curl http://localhost:8765/api/health",
        "db_lock_fix": 'cd backend && sqlite3 data/jobs.db "PRAGMA wal_checkpoint(TRUNCATE);"',
        "postgres_companion": "sudo -u postgres psql -p 5433 lead_gen",
        "contact": "arjav@eagleinfoservice.com",
    },
}


@router.get(
    "/flows/help",
    summary="List Building API self-documentation",
    description=(
        "Returns a structured JSON description of every List Building API endpoint: "
        "auth scheme, provider cascade, request/response schemas, defaults, errors, "
        "and ready-to-use curl examples. Mirror of docs/LIST_BUILDING_API_2026-07-05.md. "
        "No authentication required — intentionally public for client discovery."
    ),
)
async def get_list_building_help():
    """Return the in-API documentation payload."""
    return _LIST_BUILDING_HELP_PAYLOAD


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


# Helper functions for response formatting
def _friendly_source(source: str) -> str:
    """Map technical source values to user-friendly names.

    Both `wizleads` and the legacy `wizleads_email` source label map to the
    canonical UI label `wizleads` so historical rows display correctly.
    """
    source_map = {
        "blitz_email": "blitz",
        "blitz_linkedin": "blitz",
        "blitz_contacts": "blitz",
        "contacts_db_linkedin": "contacts_db",
        "contacts_db_name": "contacts_db",
        "contacts_db_email": "contacts_db",
        "better_enrich_company": "better_enrich",
        "better_enrich_person": "better_enrich",
        "wizleads": "wizleads",
        "wizleads_email": "wizleads",
    }
    return source_map.get(source, source or "unknown")


def _map_validation_status(code: str) -> str:
    """Map mailtester codes to validation status.

    Under the default ok-only accept policy, 'mb' is treated as invalid
    (policy-rejected). The raw code is still preserved on the row for audit.
    """
    code_map = {
        "ok": "valid_ok",
        "mb": "invalid",
        "ko": "invalid",
    }
    return code_map.get(code, "unknown")


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
    # Enable email verification with mailtester for Contacts DB emails
    # If True, Contacts DB emails are verified before being returned
    # Invalid emails are marked in Contacts DB and cascade continues to Blitz
    validate_email: bool = True
    # Optional column mappings for high-value identifiers. All optional and
    # backward-compatible: when omitted, the request behaves as before.
    linkedin_url_col: Optional[str] = None
    phone_col: Optional[str] = None
    company_name_col: Optional[str] = None
    existing_email_col: Optional[str] = None


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/default-cascade")
async def get_default_cascade():
    return {"cascade": blitz_client.DEFAULT_CASCADE}


async def _merge_by_company_into_contacts(
    contacts: list[dict[str, Any]],
    domain: str,
    force_provider: Optional[str],
    source: Optional[str] = None,
    limit: int = 25,
) -> list[dict[str, Any]]:
    """Phase 1c (2026-07-21): augment a response contacts list with every
    Contacts DB person filed under the company, emails preserved as stored
    (no mailtester). Shared by the single-row endpoints (POST /enrich, GET
    /enrich, GET /enrich/{domain}). Gated by ENABLE_COMPANY_LOOKUP; skipped
    when force_provider is set. Additive + deduped by email/name; a cascade
    contact is replaced only if its email was stripped/empty (validated
    emails are never overwritten -> no data loss).

    Phase 2 (2026-07-22): optional ``source`` (e.g. "outscraper") narrows the
    internal-DB lookup to contacts tagged with that source only. ``None`` →
    all sources (today's behavior — no regression).
    """
    if force_provider:
        return contacts
    if os.getenv("ENABLE_COMPANY_LOOKUP", "").strip().lower() not in ("1", "true", "yes"):
        return contacts
    try:
        async with httpx.AsyncClient() as bc_http:
            by_company = await contacts_client.company_persons_by_domain(
                bc_http, domain, limit=limit, exclude_source=source
            )
    except Exception as bc_err:
        logger.warning("by-company lookup failed for %s: %s", domain, bc_err)
        return contacts
    if not by_company:
        return contacts
    logger.info("by-company %s: fetched=%d cascade=%d", domain, len(by_company), len(contacts))
    seen_emails = {(c.get("email") or "").strip().lower() for c in contacts if c.get("email")}
    seen_names = {(c.get("full_name") or "").strip().lower() for c in contacts if c.get("full_name")}
    merged = list(contacts)
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
            existing = next((c for c in merged if (c.get("full_name") or "").strip().lower() == key_nm), None)
            if existing and (existing.get("email") or "").strip():
                continue
            merged = [c for c in merged if (c.get("full_name") or "").strip().lower() != key_nm]
        if key_em:
            seen_emails.add(key_em)
        if key_nm:
            seen_names.add(key_nm)
        merged.append({
            "full_name": nm,
            "first_name": bc.get("first_name", "") or "",
            "last_name": bc.get("last_name", "") or "",
            "title": bc.get("title", "") or "",
            "email": em,
            "linkedin_url": bc.get("linkedin_url", "") or "",
            "headline": bc.get("headline", "") or "",
            "location_city": bc.get("city", "") or "",
            "location_country": bc.get("country", "") or "",
            "icp_tier": 0,
            "email_source": "contacts_db",
            "validation_status": "preserved",
            "email_verified": "unverified",
            "verification_message": "",
        })
    return merged


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
    source: Optional[str] = None,
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

    # Normalize domain: strip protocol/www/path/query, reject emails/non-domains.
    # Prevents deep URLs and emails from reaching providers (Blitz 422, contacts 404).
    domain = identifier_utils.normalize_domain(domain)
    if not domain:
        raise HTTPException(status_code=400, detail="Invalid domain format")

    logger.info("Enriching single domain: %s (user: %s)", domain, current_user.get("email"))

    # Prepare input row (just the domain)
    input_row = {"domain": domain}
    rows = [input_row]

    # Run enrichment using the pipeline directly
    # We'll use asyncio directly to call the internal functions
    domain_semaphore = asyncio.Semaphore(pipeline.DOMAIN_CONCURRENCY)
    email_semaphore = asyncio.Semaphore(pipeline.EMAIL_CONCURRENCY)

    await _acquire_enrich_slot()
    blitz_http = _get_blitz_http()
    contacts_http = _get_contacts_http()

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
        # Shared cascade clients are reused across requests — do NOT close here.
        _ENRICH_SEMAPHORE.release()

    # Extract contacts from output rows
    contacts = []
    sources = {
        "company_linkedin": None,
        "contacts": None,
        "emails": None,
    }

    for row in output_rows:
        if row.get("row_status") in (pipeline.STATUS_ENRICHED, pipeline.STATUS_NO_CONTACTS):
            # Track email source with user-friendly names
            email_source = row.get("dm_email_source", "")
            if email_source:
                friendly = _friendly_source(email_source)
                if friendly != "unknown":
                    sources["emails"] = friendly

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
                "email_source": _friendly_source(row.get("dm_email_source", "")),
                "validation_status": _map_validation_status(row.get("mailtester_code", "")),
                "email_verified": row.get("dm_email_verified", "unknown"),
                "verification_message": row.get("mailtester_message", ""),
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
    sync_status = "no_contacts_to_sync"
    if contacts:
        if contacts_writer.is_v2_enabled():
            try:
                sync_result, sync_status = await _run_contacts_writer_v2(
                    contacts, domain
                )
                logger.info("contacts_writer v2 sync result for %s: %s",
                            domain, sync_result)
            except contacts_writer.LoudFailure:
                raise
            except Exception as sync_err:
                logger.error("contacts_writer v2 failed for %s: %s",
                             domain, sync_err)
                sync_result = {"synced": 0, "skipped": 0, "failed": 1,
                               "error": str(sync_err), "records_queued": 0}
                sync_status = "failed"
        else:
            try:
                # Create a temporary CSV with the enriched data for sync
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

    # Determine overall sync status (legacy path only — v2 already set sync_status)
    if not contacts_writer.is_v2_enabled():
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

    # Phase 1 (2026-07-21): by-company lookup — augment the RESPONSE with ALL
    # persons filed under this company in the Contacts DB, with emails preserved
    # AS-STORED (no mailtester re-validation; mailtester strips valid .mil/.gov
    # emails as "No MX"). Runs AFTER the write-back sync on purpose: by-company
    # rows already live in the Contacts DB, so re-syncing them is redundant and
    # slow. Non-disruptive: the cascade + sync above ran unchanged. Same-name
    # cascade contacts (emails possibly stripped) are replaced by the preserved
    # by-company version. Gated by ENABLE_COMPANY_LOOKUP (default off). Skipped
    # when force_provider is set, so a user forcing one provider gets only that.
    if os.getenv("ENABLE_COMPANY_LOOKUP", "").strip().lower() in ("1", "true", "yes") and not force_provider:
        try:
            async with httpx.AsyncClient() as bc_http:
                by_company = await contacts_client.company_persons_by_domain(
                    bc_http, domain, limit=max_results, exclude_source=source
                )
            if by_company:
                logger.info("by-company %s: fetched=%d cascade=%d", domain, len(by_company), len(contacts))
                # Include every by-company row with an email and/or name (the
                # loaded data includes nameless role/group emails — still "data
                # in the table"). Dedup by email (primary) and name (secondary).
                seen_emails: set[str] = set()
                seen_names: set[str] = set()
                merged: list[dict[str, Any]] = []
                for c in contacts:
                    em = (c.get("email") or "").strip().lower()
                    nm = (c.get("full_name") or "").strip().lower()
                    if em:
                        seen_emails.add(em)
                    if nm:
                        seen_names.add(nm)
                    merged.append(c)
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
                        # Only replace a cascade contact if its email was
                        # stripped/empty; if it carries a validated email, keep
                        # it and skip this by-company duplicate (no data loss).
                        existing = next((c for c in merged
                                         if (c.get("full_name") or "").strip().lower() == key_nm), None)
                        if existing and (existing.get("email") or "").strip():
                            continue
                        merged = [c for c in merged
                                  if (c.get("full_name") or "").strip().lower() != key_nm]
                    if key_em:
                        seen_emails.add(key_em)
                    if key_nm:
                        seen_names.add(key_nm)
                    merged.append({
                        "full_name": nm,
                        "first_name": bc.get("first_name", "") or "",
                        "last_name": bc.get("last_name", "") or "",
                        "title": bc.get("title", "") or "",
                        "email": em,
                        "linkedin_url": bc.get("linkedin_url", "") or "",
                        "headline": bc.get("headline", "") or "",
                        "location_city": bc.get("city", "") or "",
                        "location_country": bc.get("country", "") or "",
                        "icp_tier": 0,
                        "email_source": "contacts_db",
                        "validation_status": "preserved",
                        "email_verified": "unverified",
                        "verification_message": "",
                    })
                contacts = merged
                if not sources.get("contacts"):
                    sources["contacts"] = "contacts_db"
        except Exception as bc_err:
            logger.warning("by-company lookup failed for %s: %s", domain, bc_err)

    return _strip_internal_fields_from_response({
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
            "records_queued": sync_result.get("records_queued", 0),
        },
    })


# ---------------------------------------------------------------------------
# Unified Enrichment Endpoint (POST)
# ---------------------------------------------------------------------------

class UnifiedEnrichRequest(BaseModel):
    """
    Request model for unified enrichment endpoint.

    Fields:
        domain: Optional company domain (e.g., "google.com").
        full_name: Optional full name of person.
        first_name: Optional first name (alternative to full_name).
        last_name: Optional last name (alternative to full_name).
        linkedin_url: Optional LinkedIn profile URL.
        phone: Optional phone number.
        company_name: Optional company name.
        existing_email: Optional existing email address.
        max_results: Maximum contacts to return (default: 5).
        cascade: Custom cascade config - list of title filters (each item is a
            dict with include_title, exclude_title, etc.). Advanced usage.
        titles: Optional comma-separated titles for fuzzy search.
            Example: "CEO,CTO,HR" or "dentist,orthodontist,dmd"
            Enables fuzzy matching against LinkedIn headlines.
            Leave empty for default business titles (Owner, CEO, VP, Director).
            Max 50 titles.
            Auto-converts to cascade if cascade is not provided.
        force_provider: Force a specific provider ("contacts_db", "blitz",
            "better_enrich"). If None, uses normal cascade.
        selected_providers: Restrict the cascade to a subset of providers
            (e.g., ["contacts_db", "smartprospect"]). Providers not in this
            list are skipped entirely, so the cascade stops at the last
            allowed provider. contacts_db is always allowed (mandatory first
            step) even if not explicitly listed. Mutually exclusive with
            force_provider. If None, all enabled providers are used.
    """
    domain: Optional[str] = None
    full_name: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    linkedin_url: Optional[str] = None
    company_linkedin_url: Optional[str] = None
    phone: Optional[str] = None
    company_name: Optional[str] = None
    existing_email: Optional[str] = None
    max_results: int = 5
    # Custom cascade: list of title filters (each item is a dict with include_title, exclude_title, etc.)
    cascade: Optional[list[dict]] = None
    # Simple titles: comma-separated list of titles (e.g., "CEO,CTO,HR") - auto-converts to cascade
    titles: Optional[str] = None
    # Force a specific provider: "contacts_db", "blitz", "better_enrich"
    # If None, uses normal cascade
    force_provider: Optional[str] = None
    # Restrict cascade to a subset of providers (e.g., ["contacts_db", "smartprospect"]).
    # Mutually exclusive with force_provider. contacts_db always allowed.
    selected_providers: Optional[list[str]] = None
    # Phase 2 (2026-07-22): optional Contacts DB source filter (e.g.
    # "outscraper"). When set, narrows the by-company internal-DB lookup to
    # contacts tagged with that source only. ``None`` → all sources (today's
    # behavior — no regression). NOT a cascade provider.
    source: Optional[str] = None

    class Config:
        schema_extra = {
            "examples": [
                {"domain": "google.com"},
                {"linkedin_url": "https://linkedin.com/in/johndoe"},
                {"company_linkedin_url": "https://linkedin.com/company/acme"},
                {"domain": "google.com", "full_name": "John Doe"},
                {"linkedin_url": "https://linkedin.com/in/johndoe", "domain": "google.com"},
                {"domain": "google.com", "titles": "CEO,CTO,HR"},
                {"domain": "google.com", "cascade": [{"include_title": ["CEO", "CTO"]}]},
                {"domain": "google.com", "selected_providers": ["contacts_db", "smartprospect"]},
            ]
        }

    def validate_inputs(self):
        """Validate that at least one identifier is provided."""
        if not self.domain and not self.linkedin_url and not self.company_linkedin_url:
            raise ValueError(
                "Either 'domain', 'linkedin_url', or 'company_linkedin_url' must be provided"
            )


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
- `titles`: Optional comma-separated titles for fuzzy search
    - Example: "CEO,CTO,HR" for business titles or "dentist,orthodontist,dmd" for professional titles
    - Enables fuzzy matching against LinkedIn headlines
    - Leave empty for default business titles (Owner, CEO, VP, Director)
    - Max 50 titles supported
- `cascade`: Custom cascade config (advanced - list of title filter dicts)
- `force_provider`: Force specific provider (contacts_db, blitz, better_enrich)

## Response
Returns enriched data with source tracking and sync status.
    """,
    response_description="Enriched domain data with contacts, sources, and sync status",
)
async def unified_enrich(
    req: UnifiedEnrichRequest,
    debug: bool = Query(
        False,
        description=(
            "When true, the response includes the full provider_attempts_json "
            "array (one structured record per provider call) plus latency_ms, "
            "called/skipped flags, and input_type_used. When false, only "
            "source_path and no_email_reason are returned in `routing` to "
            "keep responses compact."
        ),
    ),
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

    # Validate selected_providers (allowlist). Mutually exclusive with
    # force_provider, must reference real provider names, cannot be empty.
    if req.selected_providers is not None:
        if req.force_provider:
            raise HTTPException(
                status_code=400,
                detail="force_provider and selected_providers are mutually exclusive. "
                       "Pick one: force_provider='blitz' (single) or "
                       "selected_providers=['contacts_db','smartprospect'] (subset).",
            )
        if not isinstance(req.selected_providers, list) or len(req.selected_providers) == 0:
            raise HTTPException(
                status_code=400,
                detail="selected_providers must be a non-empty list. "
                       f"Valid providers: {sorted(VALID_PROVIDERS)}",
            )
        invalid = [p for p in req.selected_providers if p not in VALID_PROVIDERS]
        if invalid:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid provider(s) in selected_providers: {invalid}. "
                       f"Valid: {sorted(VALID_PROVIDERS)}",
            )
        logger.info(
            "DEBUG unified_enrich: selected_providers=%s (contacts_db always allowed)",
            req.selected_providers,
        )

    # Convert titles to cascade if provided
    if req.titles and not req.cascade:
        req.cascade = _titles_to_cascade(req.titles)

    # Validate domain format if provided
    domain = ""
    if req.domain:
        domain = identifier_utils.normalize_domain(req.domain)
        if not domain:
            raise HTTPException(status_code=400, detail="Invalid domain format")

    # Resolve full_name from first_name + last_name if provided
    full_name = req.full_name
    if not full_name and (req.first_name or req.last_name):
        full_name = f"{req.first_name or ''} {req.last_name or ''}".strip()

    # Auto-detect LinkedIn URL type — if a /company/ URL was put into the
    # person linkedin_url field, move it to company_linkedin_url (and vice
    # versa for a /in/ URL put into the company field). This makes the API
    # forgiving for callers who don't know which field to use.
    if req.linkedin_url:
        _li_type = list_builder._detect_linkedin_url_type(req.linkedin_url)
        if _li_type == "company" and not req.company_linkedin_url:
            req.company_linkedin_url = req.linkedin_url
            req.linkedin_url = None
            logger.info("Auto-detected company URL in linkedin_url field; rerouted")
    if req.company_linkedin_url:
        _co_type = list_builder._detect_linkedin_url_type(req.company_linkedin_url)
        if _co_type == "personal" and not req.linkedin_url:
            req.linkedin_url = req.company_linkedin_url
            req.company_linkedin_url = None
            logger.info("Auto-detected person URL in company_linkedin_url field; rerouted")

    # Normalize company_linkedin_url if present
    if req.company_linkedin_url:
        req.company_linkedin_url = identifier_utils.normalize_linkedin_url(req.company_linkedin_url)

    # Determine input mode
    # linkedin_only: only person LinkedIn URL provided (no domain)
    # company_linkedin_only: only company LinkedIn URL provided (no domain, no person URL)
    # domain_only: only domain provided (no person/company LinkedIn info)
    # enhanced: domain + person info (name or LinkedIn), or domain + company LinkedIn URL
    if req.linkedin_url and not domain:
        mode = "linkedin_only"
    elif req.company_linkedin_url and not domain and not req.linkedin_url:
        mode = "company_linkedin_only"
    elif not full_name and not req.linkedin_url and not req.company_linkedin_url and domain:
        mode = "domain_only"
    elif domain and req.company_linkedin_url and not full_name and not req.linkedin_url:
        # Domain + company LinkedIn URL but no person info — treat as domain_only
        # and let _enrich_single_domain consume the company LinkedIn URL directly.
        mode = "domain_only"
    else:
        mode = "enhanced"

    logger.info("Unified enrich: domain=%s, linkedin=%s, company_linkedin=%s, mode=%s, user=%s",
                domain, bool(req.linkedin_url), bool(req.company_linkedin_url),
                mode, current_user.get("email"))

    # Create HTTP clients
    await _acquire_enrich_slot()
    blitz_http = _get_blitz_http()
    contacts_http = _get_contacts_http()

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
            if req.linkedin_url and not _should_skip_provider("contacts_db", req.force_provider, req.selected_providers):
                try:
                    person = await contacts_client.person_by_linkedin(contacts_http, linkedin_username)
                    if person:
                        first_bf, last_bf, title_bf = pipeline._backfill_person_identity(person)
                        contacts.append({
                            "full_name": person.get("full_name", ""),
                            "first_name": first_bf,
                            "last_name": last_bf,
                            "title": title_bf,
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

            # Step 2: Try Blitz to get work email if not found.
            # The linkedin endpoint returns a single `email` (no
            # verified/unverified flag in the response shape). Treat any
            # email Blitz returns as provider-acceptable (this matches the
            # pre-fix behavior; the brief's "unverified" gate applies to
            # `person_enrich` where `emails[]` is explicit, not to
            # `person_enrich_by_linkedin`).
            if (not contacts or not any(c.get("email") for c in contacts)) and not _should_skip_provider("blitz", req.force_provider, req.selected_providers):
                try:
                    # Use Blitz to get email from LinkedIn
                    result = await blitz_client.person_enrich_by_linkedin(
                        blitz_http,
                        linkedin_for_blitz,
                    )
                    if result and result.get("email"):
                        candidate_li = result.get("email")
                        if contacts:
                            contacts[0]["email"] = candidate_li
                            contacts[0]["email_source"] = "blitz"
                        else:
                            first_bf, last_bf, title_bf = pipeline._backfill_person_identity(result)
                            contacts.append({
                                "full_name": result.get("full_name", ""),
                                "first_name": first_bf,
                                "last_name": last_bf,
                                "title": title_bf,
                                "email": candidate_li,
                                "linkedin_url": req.linkedin_url,
                                "headline": result.get("headline", ""),
                                "location_city": result.get("location_city", ""),
                                "location_country": result.get("location_country", ""),
                                "icp_tier": 1,
                                "email_source": "blitz",
                            })
                        sources["contacts"] = "blitz"
                        sources["emails"] = "blitz"
                        logger.info("Blitz found email via LinkedIn: %s", candidate_li)

                        # Extract full_name from Blitz result for BetterEnrich fallback
                        if not full_name:
                            full_name = result.get("full_name", "")
                except Exception as e:
                    logger.debug("Blitz LinkedIn email lookup failed: %s", e)

            # Step 3: Try WizLeads as fallback (between Blitz and BetterEnrich
            # to match documented cascade: Contacts DB → Blitz → WizLeads →
            # BetterEnrich). Eligibility: full_name (or first_name) + domain.
            if full_name and domain and (not contacts or not any(c.get("email") for c in contacts)) and not _should_skip_provider("wizleads", req.force_provider, req.selected_providers):
                try:
                    result = await wizleads_client.find_email(
                        blitz_http,
                        first_name=full_name.split(" ")[0],
                        last_name=" ".join(full_name.split(" ")[1:]) if " " in full_name else "",
                        website=domain,
                    )
                    if result and result.get("email"):
                        email = result["email"]
                        if contacts:
                            contacts[0]["email"] = email
                            contacts[0]["email_source"] = "wizleads"
                        else:
                            contacts.append({
                                "full_name": full_name,
                                "first_name": "",
                                "last_name": "",
                                "title": "",
                                "email": email,
                                "linkedin_url": req.linkedin_url or "",
                                "headline": "",
                                "location_city": "",
                                "location_country": "",
                                "icp_tier": 1,
                                "email_source": "wizleads",
                            })
                        sources["contacts"] = "wizleads"
                        sources["emails"] = "wizleads"
                        logger.info("WizLeads found email via LinkedIn: %s", email)
                except Exception as e:
                    logger.debug("WizLeads lookup failed: %s", e)

            # Step 4: Try BetterEnrich V3 as final fallback (requires full_name AND domain)
            # BetterEnrich V3 requires domain, so only try when domain is available
            if full_name and domain and (not contacts or not any(c.get("email") for c in contacts)) and not _should_skip_provider("better_enrich", req.force_provider, req.selected_providers):
                try:
                    be_result = await better_enrich_client.find_work_email_v3(
                        blitz_http,
                        full_name=full_name,
                        company_domain=domain,
                        linkedin_url=req.linkedin_url,
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
                        logger.info("BetterEnrich V3 found email via LinkedIn: %s", be_result.get("email"))
                except Exception as e:
                    logger.debug("BetterEnrich V3 LinkedIn lookup failed: %s", e)

        elif mode == "company_linkedin_only":
            # Company LinkedIn URL only → run title-waterfall directly.
            # Bypasses domain resolution; uses _enrich_by_company_linkedin
            # orchestrator (reuses _enrich_by_company_waterfall + _resolve_person_email).
            logger.info("company_linkedin_only mode: %s", req.company_linkedin_url)
            cascade = req.cascade if req.cascade else blitz_client.DEFAULT_CASCADE
            base_row = {
                "domain": domain,
                "linkedin_url": req.linkedin_url or "",
                "company_linkedin_url": req.company_linkedin_url or "",
            }
            company_rows = await list_builder._enrich_by_company_linkedin(
                blitz_http=blitz_http,
                contacts_http=contacts_http,
                base_row=base_row,
                company_linkedin_url=req.company_linkedin_url,
                domain=domain,
                cascade=cascade,
                max_dms=req.max_results,
                domain_semaphore=domain_semaphore,
                email_semaphore=email_semaphore,
                force_provider=req.force_provider,
                selected_providers=req.selected_providers,
            )

            contacts = []
            sources = {"company_linkedin": "blitz", "contacts": "not_found", "emails": "not_found"}
            for row in company_rows:
                if row.get("dm_email"):
                    contacts.append({
                        "email": row.get("dm_email", ""),
                        "title": row.get("dm_title", ""),
                        "headline": row.get("dm_headline", ""),
                        "icp_tier": int(row.get("dm_icp_tier") or 0) if str(row.get("dm_icp_tier", "")).isdigit() else 0,
                        "full_name": row.get("dm_full_name", ""),
                        "last_name": row.get("dm_last_name", ""),
                        "first_name": row.get("dm_first_name", ""),
                        "email_source": row.get("dm_email_source", ""),
                        "linkedin_url": row.get("dm_linkedin_url", ""),
                        "location_city": row.get("dm_location_city", ""),
                        "location_country": row.get("dm_location_country", ""),
                    })
                    if sources["emails"] == "not_found":
                        sources["emails"] = row.get("dm_email_source", "").split(".")[0] or "blitz"
                        sources["contacts"] = sources["emails"]
            output_rows = company_rows

        elif mode == "domain_only":
            # Domain-only: Use existing pipeline (Contacts DB → Blitz)
            # Use custom cascade if provided, otherwise use default
            # Skip Contacts DB contacts if custom cascade is provided
            logger.info("DEBUG domain_only: force_provider=%s", req.force_provider, req.selected_providers)
            has_custom_cascade = req.cascade is not None and len(req.cascade) > 0
            cascade = req.cascade if has_custom_cascade else blitz_client.DEFAULT_CASCADE
            input_row = {"domain": domain}
            if req.company_linkedin_url:
                input_row["company_linkedin_url"] = req.company_linkedin_url
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
                        "email_source": _friendly_source(row.get("dm_email_source", "")),
                        "validation_status": _map_validation_status(row.get("mailtester_code", "")),
                        "email_verified": row.get("dm_email_verified", "unknown"),
                        "verification_message": row.get("mailtester_message", ""),
                    })

                    # Track sources
                    if row.get("company_linkedin_url"):
                        sources["company_linkedin"] = "contacts_db"
                    if row.get("dm_email"):
                        sources["contacts"] = contacts_source
                        sources["emails"] = _friendly_source(row.get("dm_email_source", "")) if row.get("dm_email_source") else "blitz"

            company_linkedin_url = output_rows[0].get("company_linkedin_url", "") if output_rows else ""

            # Step 2: If no contacts found from Contacts DB/Blitz, try BetterEnrich company email
            # This is a fallback for generic company emails when no decision makers are found
            # Skip if force_provider is set and it's not "better_enrich"
            if not contacts and not _should_skip_provider("better_enrich", req.force_provider, req.selected_providers):
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
                        sources["contacts"] = "better_enrich"
                        sources["emails"] = "better_enrich"
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
            if (full_name or req.linkedin_url) and not _should_skip_provider("contacts_db", req.force_provider, req.selected_providers):
                # Try to find person in Contacts DB
                try:
                    # Priority 1: LinkedIn URL + domain (if provided)
                    if req.linkedin_url:
                        person = await contacts_client.person_by_linkedin(contacts_http, req.linkedin_url)
                        if person and person.get("email"):
                            first_bf, last_bf, title_bf = pipeline._backfill_person_identity(person)
                            contacts.append({
                                "full_name": person.get("full_name", full_name or ""),
                                "first_name": first_bf,
                                "last_name": last_bf,
                                "title": title_bf,
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
                            first_bf, last_bf, title_bf = pipeline._backfill_person_identity(person)
                            contacts.append({
                                "full_name": person.get("full_name", full_name or ""),
                                "first_name": first_bf,
                                "last_name": last_bf,
                                "title": title_bf,
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
            # Per the cascade contract, an unverified Blitz email is NOT a
            # stopping point; the cascade continues to WizLeads and
            # BetterEnrich. We track whether Blitz produced a candidate that
            # we should fall through from.
            blitz_fall_through = False
            if not contacts and not _should_skip_provider("blitz", req.force_provider, req.selected_providers):
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
                    # Treat verified_email as acceptable; treat other emails
                    # as fall-through (do not stop the cascade).
                    if blitz_result and blitz_result.get("found"):
                        email = None
                        email_verified = False

                        if blitz_mode == "linkedin":
                            # Response format: { "found": true, "email": "...", "all_emails": [...] }
                            email = blitz_result.get("email") or (blitz_result.get("all_emails") or [None])[0]
                            # linkedin mode treats as verified (matches original behavior)
                            email_verified = bool(email)
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
                            person_obj = blitz_result.get("person", {})
                            emails = person_obj.get("emails", [])
                            verified_email = person_obj.get("verified_email", "")

                            if verified_email:
                                # Provider-acceptable — stop cascade at Blitz
                                email = verified_email
                                email_verified = True
                            elif emails:
                                # Unverified email — fall through, do NOT
                                # accept as final.
                                email = emails[0]
                                email_verified = False
                                blitz_fall_through = True
                                logger.info("Blitz returned unverified email %s in enhanced mode — cascade continues", email)

                            if email:
                                contacts.append({
                                    "full_name": person_obj.get("full_name", full_name or ""),
                                    "first_name": person_obj.get("first_name", ""),
                                    "last_name": person_obj.get("last_name", ""),
                                    "title": person_obj.get("headline", ""),
                                    "email": email,
                                    "linkedin_url": person_obj.get("linkedin_url", req.linkedin_url or ""),
                                    "headline": person_obj.get("headline", ""),
                                    "location_city": "",
                                    "location_country": "",
                                    "icp_tier": 1,
                                    "email_source": "blitz",
                                })

                        if contacts and email_verified:
                            sources["contacts"] = "blitz"
                            sources["emails"] = "blitz"
                        elif contacts and not email_verified:
                            # Unverified — do not advertise Blitz as the email
                            # source. Mark it as "blitz" tentatively but the
                            # cascade continues and BetterEnrich/WizLeads can
                            # overwrite the contact below.
                            sources["contacts"] = "blitz"

                except Exception as e:
                    logger.debug("Blitz person enrichment failed: %s", e)

            # Step 2.5: If Blitz returned an unverified email, try WizLeads as
            # the next provider in the cascade. WizLeads is eligible when
            # full_name (or first_name) + domain are present.
            # Skip if force_provider is set and it's not "wizleads".
            wizleads_tried = False
            has_email_now = any(c.get("email") for c in contacts)
            should_try_wizleads = (not has_email_now or blitz_fall_through) and (
                (full_name or req.first_name) and domain
            ) and not _should_skip_provider("wizleads", req.force_provider, req.selected_providers)
            if should_try_wizleads:
                wizleads_tried = True
                first_name_wz = (req.first_name or "").strip() or (full_name.split(" ")[0] if full_name else "")
                last_name_wz = (req.last_name or "").strip() or (" ".join(full_name.split(" ")[1:]) if full_name and " " in full_name else "")
                try:
                    wl_result = await wizleads_client.find_email(
                        blitz_http,
                        first_name=first_name_wz,
                        last_name=last_name_wz,
                        website=domain,
                    )
                    if wl_result and wl_result.get("email"):
                        wz_email = wl_result["email"]
                        if contacts:
                            contacts[0]["email"] = wz_email
                            contacts[0]["email_source"] = "wizleads"
                        else:
                            contacts.append({
                                "full_name": full_name or "",
                                "first_name": first_name_wz,
                                "last_name": last_name_wz,
                                "title": "",
                                "email": wz_email,
                                "linkedin_url": req.linkedin_url or "",
                                "headline": "",
                                "location_city": "",
                                "location_country": "",
                                "icp_tier": 1,
                                "email_source": "wizleads",
                            })
                        sources["contacts"] = "wizleads"
                        sources["emails"] = "wizleads"
                        logger.info("WizLeads found email in enhanced mode: %s", wz_email)
                except Exception as e:
                    logger.debug("WizLeads lookup failed in enhanced mode: %s", e)

            # Step 3: If still no email, try BetterEnrich V3 as final fallback
            # BetterEnrich V3 requires both full_name and domain
            # Skip if force_provider is set and it's not "better_enrich"
            if full_name and domain and not _should_skip_provider("better_enrich", req.force_provider, req.selected_providers):
                # If no contacts found at all but we have full_name, try BetterEnrich V3 directly
                if not contacts:
                    try:
                        be_result = await better_enrich_client.find_work_email_v3(
                            blitz_http,
                            full_name=full_name,
                            company_domain=domain,
                            linkedin_url=req.linkedin_url,
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
                            logger.info("BetterEnrich V3 found email for %s: %s", full_name, be_result.get("email"))
                    except Exception as e:
                        logger.debug("BetterEnrich V3 lookup failed: %s", e)

                # Also try to enhance existing contacts without emails
                for contact in contacts:
                    if not contact.get("email") and full_name:
                        try:
                            be_result = await better_enrich_client.find_work_email_v3(
                                blitz_http,
                                full_name=full_name,
                                company_domain=domain,
                                linkedin_url=req.linkedin_url,
                            )
                            if be_result and be_result.get("email"):
                                contact["email"] = be_result.get("email")
                                contact["email_source"] = "better_enrich"
                                sources["emails"] = "better_enrich"
                                logger.info("BetterEnrich V3 found email for %s: %s", full_name, be_result.get("email"))
                        except Exception as e:
                            logger.debug("BetterEnrich V3 lookup failed: %s", e)

    finally:
        # Shared cascade clients are reused across requests — do NOT close here.
        _ENRICH_SEMAPHORE.release()

    # Sync to Contacts DB
    sync_result = {"synced": 0, "skipped": 0, "failed": 0}
    sync_status = "no_contacts_to_sync"
    if contacts:
        if contacts_writer.is_v2_enabled():
            try:
                sync_result, sync_status = await _run_contacts_writer_v2(
                    contacts, domain
                )
                logger.info("contacts_writer v2 sync result for %s: %s",
                            domain, sync_result)
            except contacts_writer.LoudFailure:
                raise
            except Exception as sync_err:
                logger.error("contacts_writer v2 failed for %s: %s",
                             domain, sync_err)
                sync_result = {"synced": 0, "skipped": 0, "failed": 1,
                               "error": str(sync_err), "records_queued": 0}
                sync_status = "failed"
        else:
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

    if not contacts_writer.is_v2_enabled():
        if sync_result.get("failed", 0) > 0:
            sync_status = "failed"
        elif sync_result.get("synced", 0) > 0:
            sync_status = "success"
        else:
            sync_status = "no_contacts_to_sync"

    # Phase 1c (2026-07-21): by-company augment (flag-gated, additive).
    contacts = await _merge_by_company_into_contacts(contacts, domain, req.force_provider, req.source, limit=req.max_results)

    return _strip_internal_fields_from_response({
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
            "records_queued": sync_result.get("records_queued", 0),
        },
    })


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
- `titles`: Optional comma-separated titles for fuzzy search (e.g., "CEO,CTO,HR" or "dentist,orthodontist,dmd")
    - Enables fuzzy matching against LinkedIn headlines
    - Leave empty for default business titles (Owner, CEO, VP, Director)
    - Max 50 titles supported
- `cascade_json`: Custom cascade as JSON string (advanced)
- `force_provider`: Force specific provider (contacts_db, blitz, better_enrich)
- `debug`: When true, returns full provider attempt details

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
    selected_providers: str = Query(
        None,
        description=(
            "Comma-separated allowlist of providers (e.g., 'contacts_db,smartprospect'). "
            "Providers not in this list are skipped entirely. "
            "contacts_db is always allowed. Mutually exclusive with force_provider."
        ),
    ),
    source: str = Query(
        None,
        description=(
            "Optional Contacts DB source filter (e.g., 'outscraper'). When "
            "set, narrows the internal-DB by-company lookup to contacts "
            "tagged with that source only. NOT a cascade provider; ignored "
            "by the paid waterfall."
        ),
    ),
    debug: bool = Query(
        False,
        description=(
            "When true, the response `routing` block includes the full "
            "provider_attempts_json, providers_called, providers_skipped, "
            "final_email_status, and final_email_verification_source."
        ),
    ),
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

    # Parse selected_providers CSV query param into a list.
    # Empty/whitespace entries are dropped so "?selected_providers=" → None
    # (treated as "no filter" rather than "empty list, which would 400").
    selected_providers_list: Optional[list[str]] = None
    if selected_providers:
        parsed = [p.strip() for p in selected_providers.split(",") if p.strip()]
        if parsed:
            selected_providers_list = parsed

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
        selected_providers=selected_providers_list,
        source=source,
    )

    # Call the POST handler logic (reuse by calling unified_enrich internally)
    # We'll manually invoke the same logic flow here to avoid duplication.
    # Response cache: repeat queries (same domain/name/mode) within TTL skip the
    # whole cascade AND the concurrency semaphore (~60% of traffic). Bypassed
    # for debug. See _enrich_cache_key / _enrich_cache_get / _enrich_cache_set.
    if ENRICH_RESPONSE_CACHE and not debug:
        _ckey = _enrich_cache_key(req)
        _cached = _enrich_cache_get(_ckey)
        if _cached is not None:
            _enrich_cache_record_hit()
            return _cached
        _enrich_cache_record_miss()
        _result = await _unified_enrich_logic(req, current_user, debug=debug)
        _enrich_cache_set(_ckey, _result)
        return _result
    return await _unified_enrich_logic(req, current_user, debug=debug)


async def _unified_enrich_logic(req: UnifiedEnrichRequest, current_user: dict, *, debug: bool = False):
    """
    Shared logic for both POST and GET endpoints.

    Args:
        req: UnifiedEnrichRequest with optional force_provider parameter
        current_user: Current authenticated user
        debug: When true, the response `routing` block includes the full
            provider_attempts_json (structured records) and audit fields.
            When false (default), the routing block is compact: only
            source_path, no_email_reason, and the legacy provider_attempts
            string list.

    force_provider: If set, only use that specific provider ("contacts_db", "blitz", "better_enrich")
    """
    # DEBUG: Log force_provider to verify it's being received
    logger.info("DEBUG _unified_enrich_logic: force_provider=%s (type=%s)", req.force_provider, type(req.force_provider).__name__)

    # Validate inputs - must have domain OR linkedin_url
    req.validate_inputs()

    # Validate selected_providers (allowlist). Mutually exclusive with
    # force_provider, must reference real provider names, cannot be empty.
    if req.selected_providers is not None:
        if req.force_provider:
            raise HTTPException(
                status_code=400,
                detail="force_provider and selected_providers are mutually exclusive. "
                       "Pick one: force_provider='blitz' (single) or "
                       "selected_providers=['contacts_db','smartprospect'] (subset).",
            )
        if not isinstance(req.selected_providers, list) or len(req.selected_providers) == 0:
            raise HTTPException(
                status_code=400,
                detail="selected_providers must be a non-empty list. "
                       f"Valid providers: {sorted(VALID_PROVIDERS)}",
            )
        invalid = [p for p in req.selected_providers if p not in VALID_PROVIDERS]
        if invalid:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid provider(s) in selected_providers: {invalid}. "
                       f"Valid: {sorted(VALID_PROVIDERS)}",
            )
        logger.info(
            "DEBUG _unified_enrich_logic: selected_providers=%s (contacts_db always allowed)",
            req.selected_providers,
        )

    # Validate domain format if provided
    domain = ""
    if req.domain:
        domain = identifier_utils.normalize_domain(req.domain)
        if not domain:
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
    await _acquire_enrich_slot()
    blitz_http = _get_blitz_http()
    contacts_http = _get_contacts_http()

    domain_semaphore = asyncio.Semaphore(pipeline.DOMAIN_CONCURRENCY)
    email_semaphore = asyncio.Semaphore(pipeline.EMAIL_CONCURRENCY)

    try:
        if mode in ("linkedin_only", "enhanced"):
            # Single routing function decides provider order. The legacy
            # linkedin_only/enhanced branches in this function used to copy-paste
            # their own contacts_db -> blitz -> better_enrich cascades; both are
            # now driven by pipeline.route_enrichment / run_enrichment_route.
            contacts: list[dict[str, Any]] = []
            sources = {"company_linkedin": "not_found", "contacts": "not_found", "emails": "not_found"}
            company_linkedin_url = ""

            # Enhanced mode used to fetch company LinkedIn via Contacts DB
            # before running the person cascade. Preserve that side effect so
            # downstream consumers still see company_linkedin_url in the
            # response, but only when force_provider doesn't forbid contacts_db.
            if mode == "enhanced" and domain and not _should_skip_provider("contacts_db", req.force_provider, req.selected_providers):
                try:
                    company = await contacts_client.company_by_domain(contacts_http, domain)
                    if company and company.get("linkedin_url"):
                        company_linkedin_url = company.get("linkedin_url", "")
                        sources["company_linkedin"] = "contacts_db"
                except Exception as e:
                    logger.debug("Contacts DB company lookup failed: %s", e)

            # Best-effort name enrichment from Contacts DB so the response
            # contact dict is populated even when no email is found. This
            # mirrors the legacy behaviour of populating name fields before
            # the email cascade. Skipped when force_provider forbids it.
            if not _should_skip_provider("contacts_db", req.force_provider, req.selected_providers):
                try:
                    person = await contacts_client.person_by_linkedin(
                        contacts_http, req.linkedin_url or ""
                    )
                    if person:
                        first_bf, last_bf, title_bf = pipeline._backfill_person_identity(person)
                        contacts.append({
                            "full_name": person.get("full_name", "") or full_name,
                            "first_name": first_bf,
                            "last_name": last_bf,
                            "title": title_bf,
                            "email": person.get("email", "") or "",
                            "linkedin_url": req.linkedin_url or "",
                            "headline": person.get("headline", ""),
                            "location_city": person.get("location_city", ""),
                            "location_country": person.get("location_country", ""),
                            "icp_tier": 1,
                            "email_source": "contacts_db_email" if person.get("email") else "not_found",
                        })
                        if person.get("email"):
                            sources["contacts"] = "contacts_db"
                            sources["emails"] = "contacts_db_email"
                except Exception as e:
                    logger.debug("Contacts DB person lookup failed: %s", e)

            # Single routing decision for the email cascade.
            route = pipeline.route_enrichment(
                linkedin_url=req.linkedin_url or "",
                phone=req.phone or "",
                full_name=full_name,
                first_name=req.first_name or "",
                last_name=req.last_name or "",
                domain=domain,
                company_name=req.company_name or "",
                force_provider=req.force_provider,
                selected_providers=req.selected_providers,
            )
            route_result = await pipeline.run_enrichment_route(
                route,
                blitz_http,
                contacts_http,
                email_semaphore,
                validate_email=True,
                job_id=f"api_{current_user.get('id', 'anon')}_{uuid.uuid4().hex[:8]}",
                row_index=0,
                emit_logs=True,
            )

            # Update the (possibly empty) contact dict with the routing result.
            if route_result.get("email"):
                if not contacts:
                    contacts.append({
                        "full_name": full_name,
                        "first_name": req.first_name or "",
                        "last_name": req.last_name or "",
                        "title": "",
                        "email": "",
                        "linkedin_url": req.linkedin_url or "",
                        "headline": "",
                        "location_city": "",
                        "location_country": "",
                        "icp_tier": 1,
                    })
                contacts[0]["email"] = route_result["email"]
                contacts[0]["email_source"] = route_result.get("source", "not_found")
                sources["emails"] = route_result.get("source", "not_found")
                if sources.get("contacts") in (None, "", "not_found"):
                    sources["contacts"] = route_result.get("source", "not_found")

            # Sync to Contacts DB (enhanced mode only, mirroring legacy behaviour).
            sync_result: dict[str, Any] = {"synced": 0, "skipped": 0, "failed": 0}
            sync_status = "no_contacts_to_sync"
            if mode == "enhanced" and contacts:
                if contacts_writer.is_v2_enabled():
                    try:
                        sync_result, sync_status = await _run_contacts_writer_v2(
                            contacts, domain
                        )
                        logger.info("contacts_writer v2 sync result for %s: %s",
                                    domain, sync_result)
                    except contacts_writer.LoudFailure:
                        raise
                    except Exception as sync_err:
                        logger.warning("contacts_writer v2 failed for %s: %s",
                                       domain, sync_err)
                        sync_result = {"synced": 0, "skipped": 0, "failed": 1,
                                       "error": str(sync_err), "records_queued": 0}
                        sync_status = "failed"
                else:
                    try:
                        with tempfile.NamedTemporaryFile(
                            mode="w", suffix=".csv", delete=False, newline=""
                        ) as tmp:
                            fieldnames = [
                                "domain", "dm_email", "dm_full_name", "dm_first_name",
                                "dm_last_name", "dm_linkedin_url", "dm_title",
                            ]
                            writer = csv.DictWriter(tmp, fieldnames=fieldnames)
                            writer.writeheader()
                            for c in contacts:
                                writer.writerow({
                                    "domain": domain,
                                    "dm_email": c.get("email", ""),
                                    "dm_full_name": c.get("full_name", ""),
                                    "dm_first_name": c.get("first_name", ""),
                                    "dm_last_name": c.get("last_name", ""),
                                    "dm_linkedin_url": c.get("linkedin_url", ""),
                                    "dm_title": c.get("title", ""),
                                })
                            tmp_path = Path(tmp.name)
                        sync_result = sync_contacts.sync_enrichment_to_contacts(tmp_path)
                        sync_status = "success"
                        logger.info("Enhanced mode sync result for %s: %s", domain, sync_result)
                    except Exception as sync_err:
                        logger.warning("Enhanced mode sync failed for %s: %s", domain, sync_err)
                        sync_status = "failed"
                        sync_result = {"synced": 0, "skipped": 0, "failed": 1, "error": str(sync_err)}
                    finally:
                        if "tmp_path" in dir() and tmp_path.exists():
                            tmp_path.unlink(missing_ok=True)
            elif mode == "linkedin_only" and contacts:
                # linkedin_only has no domain — contacts_writer needs domain
                # for the person/company split, so mark as no_contacts_to_sync
                # rather than fabricating success. Fix applies to both v2
                # and legacy paths.
                sync_status = "no_contacts_to_sync"
                sync_result = {"synced": 0, "skipped": len(contacts), "failed": 0, "records_queued": 0}

            # Record source stats for API-only call
            _record_unified_enrich_stats(contacts, domain, current_user)

            return _strip_internal_fields_from_response({
                "domain": domain,
                "mode": mode,
                "company_linkedin_url": company_linkedin_url,
                "contacts": contacts,
                "contact_count": len(contacts),
                "data_sources": sources,
                "routing": _build_routing_response(
                    route, route_result, debug=debug
                ),
                "sync_to_contacts_db": {
                    "status": sync_status,
                    "records_synced": sync_result.get("synced", 0),
                    "records_skipped": sync_result.get("skipped", 0),
                    "records_failed": sync_result.get("failed", 0),
                    "records_queued": sync_result.get("records_queued", 0),
                },
            })


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
            if not has_custom_cascade and not _should_skip_provider("contacts_db", req.force_provider, req.selected_providers):
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
            if company_linkedin_url and not _should_skip_provider("blitz", req.force_provider, req.selected_providers):
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
                            # Backfill first/last from full_name + try alt title field names.
                            first_bf, last_bf, title_bf = pipeline._backfill_person_identity(person)
                            # Extract person fields from nested structure
                            extracted_results.append({
                                "full_name": person.get("full_name", ""),
                                "first_name": first_bf,
                                "last_name": last_bf,
                                "title": title_bf,
                                "headline": person.get("headline", ""),
                                "linkedin_url": person.get("linkedin_url", ""),
                                "location": person.get("location", {}),
                            })
                        return extracted_results, "blitz"
                except Exception as e:
                    logger.warning("Blitz waterfall ICP failed: %s", e)

            return [], "not_found"

        async def find_email_for_person(
            person_linkedin: str,
            person_name: str,
            person_first_name: str = "",
            person_last_name: str = "",
        ):
            """Find email for a person using the unified routing layer.

            Delegates to `pipeline.route_enrichment` /
            `pipeline.run_enrichment_route` so the per-person cascade is
            Contacts DB -> Blitz -> WizLeads -> BetterEnrich, with the
            same eligibility rules and `force_provider` semantics as the
            `linkedin_only` and `enhanced` modes in this function.

            When the contact has both name splits and a domain (the
            normal case for a decision maker returned by
            `company_contacts_enriched` or the Blitz waterfall), the
            route is built from name+domain so the full 4-provider
            cascade is reachable. When the contact only has a
            LinkedIn URL, the LinkedIn-first cascade is used.

            Returns (email, source, route_result) so the caller can also
            surface the routing block in the response.
            """
            # Prefer the name+domain cascade when we have name splits,
            # because `route_enrichment` only includes the WizLeads and
            # BetterEnrich person-email steps in that mode. The
            # LinkedIn-first cascade (used when only linkedin_url is
            # available) stops at BetterEnrich via `find_work_email_v3`
            # and is the right fallback for contacts that have no name.
            has_name_domain = bool(person_first_name and person_last_name and domain)
            route_linkedin = person_linkedin if not has_name_domain else ""
            route = pipeline.route_enrichment(
                linkedin_url=route_linkedin or "",
                full_name=person_name or "",
                first_name=person_first_name or "",
                last_name=person_last_name or "",
                domain=domain or "",
                force_provider=req.force_provider,
                selected_providers=req.selected_providers,
            )
            route_result = await pipeline.run_enrichment_route(
                route,
                blitz_http,
                contacts_http,
                email_semaphore,
                validate_email=True,
                job_id=f"api_{current_user.get('id', 'anon')}_{uuid.uuid4().hex[:8]}",
                row_index=0,
                emit_logs=True,
            )
            email = route_result.get("email", "") or ""
            source = route_result.get("source", "not_found") or "not_found"
            return email, source, route_result

        # Execute the workflow
        company_linkedin_url, company_source = await get_company_linkedin()
        contacts_list, contacts_source = await get_decision_makers()

        # Determine email sources
        email_source_db = contacts_source if contacts_source == "contacts_db" else "not_found"

        # Initialize sources dict - will be updated by BetterEnrich if used
        sources = {"company_linkedin": "not_found", "contacts": "not_found", "emails": "not_found"}

        # Track the per-person routing blocks so the final response can
        # surface a representative `routing` block (matching the format
        # the linkedin_only/enhanced branch already returns).
        last_route: dict[str, Any] = {}
        last_route_result: dict[str, Any] = {}

        # For each contact, try to find email
        enriched_contacts = []
        for contact in contacts_list[:req.max_results]:
            person_name = contact.get("full_name", "")
            person_linkedin = contact.get("linkedin_url", "")
            person_first_name = contact.get("first_name", "")
            person_last_name = contact.get("last_name", "")

            email, email_src, route_result = await find_email_for_person(
                person_linkedin,
                person_name,
                person_first_name=person_first_name,
                person_last_name=person_last_name,
            )
            last_route_result = route_result
            if email:
                last_route = {
                    "mode": route_result.get("mode", ""),
                    "source_path": route_result.get("source_path", ""),
                    "no_email_reason": route_result.get("no_email_reason", ""),
                }

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
        if not enriched_contacts and not _should_skip_provider("better_enrich", req.force_provider, req.selected_providers):
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
                email, email_src, route_result = await find_email_for_person(
                    person_linkedin,
                    full_name,
                    person_first_name=req.first_name or "",
                    person_last_name=req.last_name or "",
                )
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

        # Sync to Contacts DB
        sync_result = {"synced": 0, "skipped": 0, "failed": 0}
        sync_status = "no_contacts_to_sync"
        if enriched_contacts:
            if contacts_writer.is_v2_enabled():
                try:
                    sync_result, sync_status = await _run_contacts_writer_v2(
                        enriched_contacts, domain)
                except contacts_writer.LoudFailure:
                    raise
                except Exception as v2_err:
                    logger.error("contacts_writer v2 sync failed for %s: %s", domain, v2_err)
                    sync_result = {"synced": 0, "skipped": 0, "failed": 1,
                                   "records_queued": 0, "error": str(v2_err)}
                    sync_status = "failed"
            else:
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

        return _strip_internal_fields_from_response({
            "domain": domain,
            "mode": mode,
            "company_linkedin_url": company_linkedin_url,
            "contacts": enriched_contacts,
            "contact_count": len(enriched_contacts),
            "data_sources": sources,
            "routing": _build_routing_response(
                last_route if last_route else {"mode": mode or "", "steps": []},
                last_route_result if last_route_result else (
                    # P1 visibility fix: when domain_only mode found 0 contacts,
                    # surface what was actually attempted instead of returning
                    # an empty routing block. Without this, users see
                    # provider_attempts=[] and reasonably conclude the system
                    # didn't try anything — actually it tried Contacts DB +
                    # Blitz at the company level but couldn't find DMs.
                    _build_domain_only_fallback_route_result(
                        sources, company_linkedin_url
                    ) if mode == "domain_only" else {}
                ),
                debug=debug,
            ),
            "sync_to_contacts_db": {
                "status": sync_status,
                "records_synced": sync_result.get("synced", 0),
                "records_skipped": sync_result.get("skipped", 0),
                "records_failed": sync_result.get("failed", 0),
            },
        })

    finally:
        # Shared cascade clients are reused across requests — do NOT close here.
        _ENRICH_SEMAPHORE.release()


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


def _job_output_exists(job: dict) -> bool:
    """Whether the job's result CSV is still on disk. Drives the UI
    file-available indicator."""
    p = job.get("output_path") or job.get("partial_output_path")
    if p and Path(p).exists():
        return True
    jid = job.get("job_id")
    return bool(jid) and (OUTPUT_DIR / f"{jid}.csv").exists()


@router.get("/jobs")
async def list_enrichment_jobs(
    current_user: dict = Depends(auth.get_current_user),
    limit: int = Query(25, ge=1, le=200),
    offset: int = Query(0, ge=0),
    status: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None, description="Inclusive lower bound (YYYY-MM-DD) on created_at"),
    date_to: Optional[str] = Query(None, description="Inclusive upper bound (YYYY-MM-DD) on created_at"),
    file_ready: bool = Query(False, description="Only return jobs whose result file is on disk (downloadable)"),
    source: Optional[str] = Query(None, description="Filter by origin: google_maps_chain | csv_upload | restart"),
):
    """List enrichment jobs (paginated, optional status + search + date + file-ready + source filter)."""
    store = job_store.get_store()
    user_id = None if current_user.get("is_admin") else current_user["user_id"]

    if file_ready:
        # output_exists is a per-job filesystem check (_job_output_exists), not a
        # DB column, so it can't be a SQL WHERE on list_jobs/count_jobs. Fetch the
        # full filtered candidate set (bounded by FILE_READY_SCAN_CAP), keep only
        # jobs whose file is on disk, then paginate the ready list in Python so
        # the page and the total stay consistent.
        candidates = store.list_jobs(
            user_id=user_id, job_type="enrichment", status=status, search=search,
            date_from=date_from, date_to=date_to, source_type=source,
            limit=FILE_READY_SCAN_CAP, offset=0,
        )
        if len(candidates) >= FILE_READY_SCAN_CAP:
            logger.info(
                "file_ready filter reached scan cap (%d); some ready jobs may be uncounted",
                FILE_READY_SCAN_CAP,
            )
        ready = [j for j in candidates if _job_output_exists(j)]
        total = len(ready)
        jobs = ready[offset:offset + limit]
    else:
        jobs = store.list_jobs(
            user_id=user_id, job_type="enrichment", limit=limit, offset=offset,
            status=status, search=search, date_from=date_from, date_to=date_to,
            source_type=source,
        )
        total = store.count_jobs(
            user_id=user_id, job_type="enrichment", status=status, search=search,
            date_from=date_from, date_to=date_to, source_type=source,
        )

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

    # Attach checkpoint_count per job so the UI can render the 'Resume' button
    # for jobs that have resumable progress. One batched GROUP BY, not N queries.
    try:
        if jobs:
            ids = [j["job_id"] for j in jobs if j.get("job_id")]
            placeholders = ",".join("?" * len(ids))
            ck_rows = store.conn.execute(
                f"SELECT job_id, COUNT(*) AS n FROM job_checkpoints "
                f"WHERE job_id IN ({placeholders}) GROUP BY job_id",
                ids,
            ).fetchall()
            counts = {r["job_id"]: int(r["n"]) for r in ck_rows}
        else:
            counts = {}
        for job in jobs:
            job["checkpoint_count"] = counts.get(job.get("job_id"), 0)
    except Exception as cke:
        logger.warning("checkpoint_count augmentation failed: %s", cke)
        for job in jobs:
            job.setdefault("checkpoint_count", 0)

    # File-availability flag for the UI (cheap stat per job).
    for job in jobs:
        job["output_exists"] = _job_output_exists(job)

    return {"jobs": jobs, "total": total, "limit": limit, "offset": offset}


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
        source_type="csv_upload",
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
        validate_email=req.validate_email,
        linkedin_url_col=req.linkedin_url_col,
        phone_col=req.phone_col,
        company_name_col=req.company_name_col,
        existing_email_col=req.existing_email_col,
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
    token: Optional[str] = Query(default=None),
    current_user: Optional[dict] = Depends(auth.get_current_user_optional),
):
    """SSE stream of enrichment progress events with replay support."""
    # EventSource cannot send custom headers, so accept the JWT via ?token=
    # (mirrors the scraper stream endpoint). Header-based auth still works
    # for non-EventSource clients via get_current_user_optional.
    if current_user is None:
        if token:
            current_user = auth.decode_token(token)
        else:
            raise HTTPException(status_code=401, detail="Authentication required.")
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
    try:
        store = job_store.get_store()
        job_data = store.get_job(job_id)
    except sqlite3.OperationalError as e:
        logger.warning("enrichment download: db lock for job %s: %s", job_id, e)
        raise HTTPException(
            status_code=503,
            detail="The platform database is briefly busy. Please retry in a few seconds.",
            headers={"Retry-After": "3"},
        )
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


# Size of each virtual download shard (rows). Tunable.
SHARD_SIZE = 10_000


def _db_busy() -> HTTPException:
    return HTTPException(
        status_code=503,
        detail="The platform database is briefly busy. Please retry in a few seconds.",
        headers={"Retry-After": "3"},
    )


# Cache for _count_csv_data_rows keyed by (path, size, mtime_ns). While a job is
# actively writing, size/mtime change every batch so the cache naturally
# invalidates (re-scan is correct); for a stable/completed file, repeated
# /shards + /resume-info calls hit the cache instead of re-scanning the CSV.
_CSV_ROW_COUNT_CACHE: dict[tuple[str, int, int], int] = {}


def _count_csv_data_rows(path: Path) -> int:
    """Count data RECORDS in a CSV without loading it (minus the header).
    Uses csv.reader so quoted embedded newlines don't inflate the count.
    Cached by (path, size, mtime_ns) to avoid re-scanning on repeat calls."""
    try:
        st = path.stat()
    except Exception:
        return 0
    key = (str(path), st.st_size, st.st_mtime_ns)
    cached = _CSV_ROW_COUNT_CACHE.get(key)
    if cached is not None:
        return cached
    try:
        with open(path, newline="", encoding="utf-8") as f:
            n = max(0, sum(1 for _ in csv.reader(f)) - 1)
    except Exception:
        return 0
    _CSV_ROW_COUNT_CACHE[key] = n
    if len(_CSV_ROW_COUNT_CACHE) > 1000:  # bound growth across many jobs
        _CSV_ROW_COUNT_CACHE.clear()
    return n


@router.get("/jobs/{job_id}/resume-info")
async def enrichment_resume_info(
    job_id: str,
    current_user: dict = Depends(auth.get_current_user),
):
    """Resume eligibility + partial-CSV status. The frontend's 'Resume' button
    calls this before POSTing /restart. Returns exactly the shape the UI reads."""
    try:
        store = job_store.get_store()
        job_data = store.get_job(job_id)
    except sqlite3.OperationalError:
        raise _db_busy()
    if not job_data or job_data.get("job_type") != "enrichment":
        raise HTTPException(status_code=404, detail="Enrichment job not found.")
    if not _owns_job(job_data, current_user):
        raise HTTPException(status_code=403, detail="Access denied.")
    total = int(job_data.get("total", 0) or 0)
    try:
        checkpoint_count = int(store.get_checkpoint_count(job_id))
    except Exception:
        checkpoint_count = 0
    partial_csv = OUTPUT_DIR / f"{job_id}.csv"
    partial_csv_exists = partial_csv.exists() and partial_csv.stat().st_size > 0
    partial_csv_rows = _count_csv_data_rows(partial_csv) if partial_csv_exists else 0
    emails_found = int(job_data.get("emails_found", 0) or 0)
    unprocessed = max(0, total - checkpoint_count)
    return {
        "filename": job_data.get("original_filename") or job_data.get("filename"),
        "status": job_data["status"],
        "total": total,
        "checkpoint_count": checkpoint_count,
        "partial_csv_exists": partial_csv_exists,
        "partial_csv_rows": partial_csv_rows,
        "emails_found": emails_found,
        "unprocessed": unprocessed,
        "can_resume": (checkpoint_count > 0) or partial_csv_exists,
    }


@router.get("/jobs/{job_id}/recover-partial")
async def enrichment_recover_partial(
    job_id: str,
    current_user: dict = Depends(auth.get_current_user),
):
    """Serve whatever partial CSV exists for a job — works for running/partial/
    failed/cancelled/abandoned (no status guard). The frontend's 'Download Partial'
    button calls this. 404 if no non-empty partial file exists yet."""
    try:
        store = job_store.get_store()
        job_data = store.get_job(job_id)
    except sqlite3.OperationalError:
        raise _db_busy()
    if not job_data or job_data.get("job_type") != "enrichment":
        raise HTTPException(status_code=404, detail="Enrichment job not found.")
    if not _owns_job(job_data, current_user):
        raise HTTPException(status_code=403, detail="Access denied.")
    # Prefer the recorded partial path, then the live output, then a renamed _partial.csv
    candidates = [
        job_data.get("partial_output_path") or "",
        job_data.get("output_path") or "",
        str(OUTPUT_DIR / f"{job_id}.csv"),
        str(OUTPUT_DIR / f"{job_id}_partial.csv"),
    ]
    path = None
    for c in candidates:
        if c and Path(c).exists() and Path(c).stat().st_size > 0:
            path = c
            break
    if not path:
        raise HTTPException(status_code=404, detail="No partial file available yet.")
    original_filename = job_data.get("original_filename") or job_data.get("filename", "results")
    safe_name = (str(original_filename) or "results")[:30].replace(" ", "-").replace("/", "-")
    return FileResponse(
        path=path,
        media_type="text/csv",
        filename=f"partial_{safe_name}_{job_id[:8]}.csv",
    )


@router.get("/jobs/{job_id}/shards")
async def enrichment_shards(
    job_id: str,
    current_user: dict = Depends(auth.get_current_user),
):
    """List virtual 10K-row download shards for a job's live CSV. Works while the
    job is running — each shard becomes downloadable as its rows land on disk."""
    try:
        store = job_store.get_store()
        job_data = store.get_job(job_id)
    except sqlite3.OperationalError:
        raise _db_busy()
    if not job_data or job_data.get("job_type") != "enrichment":
        raise HTTPException(status_code=404, detail="Enrichment job not found.")
    if not _owns_job(job_data, current_user):
        raise HTTPException(status_code=403, detail="Access denied.")
    csv_path = OUTPUT_DIR / f"{job_id}.csv"
    rows_on_disk = _count_csv_data_rows(csv_path) if (csv_path.exists() and csv_path.stat().st_size > 0) else 0
    total = int(job_data.get("total", 0) or 0)
    basis = total if total else rows_on_disk
    num_shards = max(1, (basis + SHARD_SIZE - 1) // SHARD_SIZE) if basis else 0
    shards = []
    for i in range(num_shards):
        start = i * SHARD_SIZE
        end = min(start + SHARD_SIZE, basis)
        ready = max(0, min(rows_on_disk, end) - start)
        shards.append({
            "shard": i,
            "start_row": start,
            "end_row": end,
            "rows_available": ready,
            "complete": (end - start) > 0 and ready >= (end - start),
        })
    return {
        "job_id": job_id,
        "shard_size": SHARD_SIZE,
        "rows_on_disk": rows_on_disk,
        "total": total,
        "shards": shards,
    }


@router.get("/jobs/{job_id}/shard/{shard}")
async def enrichment_shard_download(
    job_id: str,
    shard: int,
    current_user: dict = Depends(auth.get_current_user),
):
    """Stream one 10K-row shard of the live CSV (no status guard — works while
    the job is still running). Reads the file sequentially so a 100K-row job is
    never loaded into memory."""
    try:
        store = job_store.get_store()
        job_data = store.get_job(job_id)
    except sqlite3.OperationalError:
        raise _db_busy()
    if not job_data or job_data.get("job_type") != "enrichment":
        raise HTTPException(status_code=404, detail="Enrichment job not found.")
    if not _owns_job(job_data, current_user):
        raise HTTPException(status_code=403, detail="Access denied.")
    if shard < 0:
        raise HTTPException(status_code=400, detail="Invalid shard index.")
    csv_path = OUTPUT_DIR / f"{job_id}.csv"
    if not csv_path.exists() or csv_path.stat().st_size == 0:
        raise HTTPException(status_code=404, detail="No data written yet.")
    start = shard * SHARD_SIZE

    def iter_shard():
        import csv as _csv
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = _csv.reader(f)
            try:
                header = next(reader)
            except StopIteration:
                return
            buf = _csv.StringIO()
            writer = _csv.writer(buf)
            writer.writerow(header)
            # skip to the shard's start row
            for _ in range(start):
                try:
                    next(reader)
                except StopIteration:
                    break
            count = 0
            for row in reader:
                if count >= SHARD_SIZE:
                    break
                writer.writerow(row)
                count += 1
                if count % 1000 == 0:
                    yield buf.getvalue()
                    buf.seek(0)
                    buf.truncate(0)
            if buf.getvalue():
                yield buf.getvalue()

    original_filename = job_data.get("original_filename") or job_data.get("filename", "results")
    safe_name = (str(original_filename) or "results")[:30].replace(" ", "-").replace("/", "-")
    return StreamingResponse(
        iter_shard(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="shard_{shard}_{safe_name}_{job_id[:8]}.csv"'},
    )


# ---------------------------------------------------------------------------
# Background job runner
# ---------------------------------------------------------------------------

async def _run_background_sync(
    job_id: str,
    output_path: Path,
    collector: Optional[RawContactCollector] = None,
) -> None:
    """
    Background task to sync enrichment results to Contacts DB.
    Runs asynchronously without blocking the API.

    Args:
        job_id: Job identifier.
        output_path: Path to the CSV to sync via the CSV-based path.
        collector: Optional ``RawContactCollector`` populated during the
            job. When provided AND non-empty, its payloads are drained via
            ``contacts_writer.write_enrichment_result_batch`` AFTER the
            CSV-based sync. Phase 1 capture surface.
    """
    try:
        if contacts_writer.is_v2_enabled():
            logger.info("contacts_writer v2 sync for job %s", job_id)
            payloads = _csv_rows_to_payloads(output_path)
            result = await contacts_writer.write_enrichment_result_batch(payloads, job_id=job_id)
            logger.info("contacts_writer v2 sync done for job %s: %s",
                        job_id, result.to_dict())

            # Phase 1: drain the collector (company-level audit captures).
            # Even when empty we skip cleanly — the writer is not invoked.
            if collector is not None:
                extra_payloads = collector.to_payloads()
                if extra_payloads:
                    extra_result = await contacts_writer.write_enrichment_result_batch(
                        extra_payloads, job_id=job_id
                    )
                    logger.info(
                        "Phase 1 collector drain for job %s: %d extra contacts (%s)",
                        job_id, len(extra_payloads), extra_result.to_dict(),
                    )
        else:
            logger.info("Auto-syncing enrichment job %s to contacts DB (person records)", job_id)
            sync_result = sync_contacts.sync_enrichment_to_contacts(output_path)
            logger.info("Auto-sync complete for job %s: %s", job_id, sync_result)
    except contacts_writer.LoudFailure as loud_err:
        # LoudFailure is operator-facing; log at ERROR so journalctl flags it.
        # Outbox retry loop should pick it up; do not re-raise into asyncio.create_task.
        logger.error("LoudFailure during background sync for job %s: %s", job_id, loud_err)
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
    validate_email: bool = True,  # NEW PARAMETER
    linkedin_url_col: Optional[str] = None,
    phone_col: Optional[str] = None,
    company_name_col: Optional[str] = None,
    existing_email_col: Optional[str] = None,
):
    store = job_store.get_store()
    store.set_running(job_id)
    # Set initial heartbeat so cleanup_stale_jobs doesn't mark us as abandoned too soon
    store.heartbeat(job_id)
    seq = [0]

    output_path = OUTPUT_DIR / f"{job_id}.csv"

    # Phase 1: per-job collector for company-level audit captures.
    # Drained by ``_run_background_sync`` at end of job.
    collector = RawContactCollector(job_id=job_id)

    # Start heartbeat task (updates last_heartbeat every 30s)
    async def heartbeat_loop():
        try:
            while True:
                await asyncio.sleep(30)
                try:
                    heartbeat_store = job_store.get_store()
                    heartbeat_store.heartbeat(job_id)
                except Exception as hb_err:
                    logger.warning("Heartbeat failed for %s: %s", job_id, hb_err)
        except asyncio.CancelledError:
            pass

    heartbeat_task = asyncio.create_task(heartbeat_loop())

    async def on_progress(e: dict[str, Any]):
        # Get FRESH store instance for this thread
        # This fixes the progress counter bug where background tasks couldn't commit
        progress_store = job_store.get_store()
        progress_store.append_event(job_id, seq[0], e)
        seq[0] += 1

        # Write checkpoint every 100 rows for incremental resume
        row_index = e.get("index", 0)
        if row_index % 100 == 0:
            progress_store.write_checkpoint(job_id, row_index)

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

        # Track which providers the pipeline actually *attempted* (not just
        # succeeded at). We persist to `jobs.used_providers` so the job
        # summary reflects the real cascade that ran. Without this, CSV
        # uploads running through `run_pipeline` never populated
        # `used_providers` and downstream reporting was incomplete.
        used_providers_set: set[str] = set()
        def record_provider_use(provider: str) -> None:
            if provider not in used_providers_set:
                used_providers_set.add(provider)
                try:
                    record_store = job_store.get_store()
                    record_store.update_used_providers(job_id, provider)
                except Exception as upd_err:
                    logger.warning(
                        "update_used_providers(%s, %s) failed: %s",
                        job_id, provider, upd_err,
                    )

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
            validate_email=validate_email,
            linkedin_url_col=linkedin_url_col,
            phone_col=phone_col,
            company_name_col=company_name_col,
            existing_email_col=existing_email_col,
            record_provider_use=record_provider_use,
            collector=collector,
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

        # Defensive guard: 0 output rows on a non-empty input is always a bug.
        # See csv_jobs_silent_failure_2026-07-13.md for the incident this prevents.
        rows_param_count = len(rows) if 'rows' in dir() else 0
        if len(output_rows) == 0 and rows_param_count > 0:
            raise RuntimeError(
                f"Job produced 0 output rows from {rows_param_count} input rows. "
                f"This is always a bug — check logs for 'Row processing failed' warnings."
            )

        store._mark_done_and_cleanup(job_id, output_path)
        logger.info("Enrichment job %s completed, %d output rows", job_id, len(output_rows))

        # Run auto-sync in the background without blocking the API
        # This prevents the refresh button from getting stuck
        asyncio.create_task(_run_background_sync(job_id, output_path, collector=collector))

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
                store._mark_done_and_cleanup(job_id, output_path)
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
        # Cancel heartbeat task
        if 'heartbeat_task' in locals():
            heartbeat_task.cancel()
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

    if original_job["status"] not in ("failed", "abandoned", "cancelled", "partial"):
        raise HTTPException(status_code=400,
            detail="Only failed, abandoned, cancelled, or partial jobs can be restarted")

    # Prevent duplicate restarts - check if there's already an active restart
    active_statuses = ("running", "queued", "pending")
    existing_restart = store.conn.execute(
        "SELECT job_id, status FROM jobs WHERE parent_job_id = ? AND status IN (?, ?, ?) LIMIT 1",
        (job_id, active_statuses[0], active_statuses[1], active_statuses[2])
    ).fetchone()
    if existing_restart:
        raise HTTPException(
            status_code=409,
            detail=f"A restart for this job is already in progress (job_id: {existing_restart['job_id']}, status: {existing_restart['status']}). Please wait for it to complete or cancel it first."
        )

    # Read the original CSV file
    # For chained jobs (from scraper), the file is in outputs/ from the parent scraper job
    # For uploaded files, the file is in uploads/
    filename = original_job['filename']
    # Defensive: filename may or may not include the .csv extension. Uploaded
    # files are stored as "<uuid>.csv" and the DB stores the uuid without
    # extension, so we append .csv. But scraper outputs and some legacy rows
    # store the full name WITH .csv — appending again produces "<name>.csv.csv"
    # which never matches a real file. Strip the extension first if present.
    clean_filename = filename[:-4] if filename.lower().endswith(".csv") else filename
    upload_path = UPLOAD_DIR / f"{clean_filename}.csv"
    csv_path = upload_path if upload_path.exists() else None

    # If not in uploads, walk up the parent chain looking for a scraper job
    # with an output_path. Chained enrichment jobs may have another enrichment
    # job as their parent (e.g., a restart of a restart) — we need to find the
    # original scraper at the top of the chain that actually has the CSV.
    if csv_path is None and original_job.get('parent_job_id'):
        parent_conn = db.get_db()
        current_parent_id = original_job['parent_job_id']
        # Safety cap on chain depth to prevent infinite loops on cyclic data.
        for _ in range(10):
            if not current_parent_id:
                break
            parent_row = parent_conn.execute(
                "SELECT job_id, output_path, parent_job_id FROM jobs WHERE job_id = ?",
                (current_parent_id,),
            ).fetchone()
            if not parent_row:
                break
            if parent_row['output_path']:
                candidate = Path(parent_row['output_path'])
                if candidate.exists():
                    csv_path = candidate
                    break
            # Walk up another level.
            current_parent_id = parent_row['parent_job_id'] if 'parent_job_id' in parent_row.keys() else None

    if csv_path is None or not csv_path.exists():
        raise HTTPException(status_code=404, detail="Original CSV file not found")

    try:
        df = pd.read_csv(str(csv_path), skipinitialspace=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not read original CSV: {e}")

    # Validate domain column exists
    domain_col = original_job.get('domain_col', '')
    if not domain_col or domain_col not in df.columns:
        raise HTTPException(status_code=400, detail=f"Domain column '{domain_col}' not found in CSV")

    rows = df.fillna("").astype(str).to_dict(orient="records")

    # Carry the previous run's partial output into the resumed job so its CSV ends
    # up complete (prior completed rows + newly processed rows). Safe because the
    # incremental writer checkpoints only AFTER a row is flushed to disk, so every
    # row here maps to a checkpointed index — the unprocessed set below is disjoint
    # from these rows (no duplicates, no dedup needed).
    prepend_rows: list[dict] = []
    prev_output = OUTPUT_DIR / f"{original_job['job_id']}.csv"
    if prev_output.exists() and prev_output.stat().st_size > 0:
        try:
            import csv as _csv
            with open(prev_output, newline="", encoding="utf-8") as _f:
                prepend_rows = [dict(r) for r in _csv.DictReader(_f)]
            logger.info("Job %s: carrying %d partial rows from previous run into resume", job_id, len(prepend_rows))
        except Exception as pre_err:
            logger.warning("Job %s: could not read previous partial for carry-over: %s", job_id, pre_err)
            prepend_rows = []

    # Preserve the previous partial on disk (renamed) and register it as the
    # original job's downloadable partial (UI 'Download Partial' reads partial_output_path).
    if prev_output.exists():
        partial_path = OUTPUT_DIR / f"{original_job['job_id']}_partial.csv"
        prev_output.rename(partial_path)
        try:
            store.set_partial_output_path(job_id, str(partial_path))
        except Exception as sper:
            logger.warning("Job %s: set_partial_output_path failed: %s", job_id, sper)
        logger.info("Renamed previous output to %s", partial_path)

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
    # For old jobs without selected_providers, fall back to all enabled providers
    # so retry preserves the user's expected cascade (or at least defaults to all)
    selected_providers = None
    providers_json = original_job.get('selected_providers', '')
    if providers_json:
        try:
            selected_providers = json.loads(providers_json)
        except Exception as e:
            logger.warning("Failed to parse selected_providers for job %s: %s", job_id, e)

    if not selected_providers:
        # Old job or unparseable - default to all currently enabled providers
        from . import providers as _providers
        selected_providers = [
            name for name, enabled in _providers.ENABLED_PROVIDERS.items() if enabled
        ]
        logger.info("Job %s has no selected_providers, defaulting to enabled providers: %s",
                    job_id, selected_providers)

    # Read pre-processing flags from the original job. Old jobs (pre-migration)
    # default to (1, 1, 0, "") which means normalize on, dedupe on, 0 deduped
    # rows, no audit list — i.e. behavior identical to today.
    orig_normalize = bool(original_job.get("normalize_domains", 1))
    orig_dedupe = bool(original_job.get("dedupe_by_domain", 1))

    # Re-run dedupe deterministically on the freshly loaded CSV rows. This MUST
    # happen BEFORE the unprocessed-row filter: checkpoints are written in
    # DEDUPED-row space (the runner processes deduped_rows), so the filter must
    # also operate in deduped space or resume would duplicate rows.
    if orig_dedupe:
        deduped_all, deduped_count, skipped_domains = identifier_utils.dedupe_rows_by_domain(
            rows, domain_col, orig_normalize
        )
    else:
        deduped_all, deduped_count, skipped_domains = rows, 0, []

    # Filter to unprocessed rows in DEDUPED space (matches checkpoint space).
    total_deduped = len(deduped_all)
    processed = store.get_processed_indices(original_job['job_id'])

    if total_deduped > 0 and len(processed) >= total_deduped:
        # Every row was already processed in the prior run (it crash-landed after
        # checkpointing everything but before set_done). Carry the prior partial as
        # the complete result — do NOT re-process, or prepend_rows would duplicate.
        new_job_id = str(uuid.uuid4())
        store.increment_restart_count(job_id)
        store.create_enrichment_job(
            job_id=new_job_id, user_id=current_user["user_id"], total=total_deduped,
            source_type="restart",
            filename=original_job['filename'], domain_col=original_job['domain_col'],
            original_filename=original_job.get('original_filename', ''),
            parent_job_id=job_id, name_col=original_job.get('name_col'),
            first_name_col=original_job.get('first_name_col'), last_name_col=original_job.get('last_name_col'),
            cascade_config=cascade_json, max_results=original_job.get('max_results', 5),
            selected_providers=selected_providers, linkedin_url_col=original_job.get('linkedin_url_col'),
            phone_col=original_job.get('phone_col'), company_name_col=original_job.get('company_name_col'),
            existing_email_col=original_job.get('existing_email_col'),
            normalize_domains=orig_normalize, dedupe_by_domain=orig_dedupe,
            deduped_rows=deduped_count, dedupe_skipped_domains=json.dumps(skipped_domains),
        )
        prior_partial = OUTPUT_DIR / f"{original_job['job_id']}_partial.csv"
        new_out = OUTPUT_DIR / f"{new_job_id}.csv"
        if prior_partial.exists():
            import shutil as _shutil
            _shutil.copyfile(prior_partial, new_out)
            store.set_done(new_job_id, str(new_out))
            logger.info("Job %s resume: all %d rows already done; carried prior partial to new job %s",
                        job_id, total_deduped, new_job_id)
        else:
            store.set_failed(new_job_id, "Resume: all rows were processed but no partial output was found.")
            logger.warning("Job %s resume: all rows processed but no prior partial found", job_id)
        return {"job_id": new_job_id, "total": total_deduped, "restarted_from": job_id, "deduped_count": deduped_count}

    unprocessed_indices = [i for i in range(total_deduped) if i not in processed]
    if unprocessed_indices:
        deduped_rows = [deduped_all[i] for i in unprocessed_indices]
        logger.info("Job %s resuming: %d/%d deduped rows already done, processing %d remaining",
                    job_id, len(processed), total_deduped, len(deduped_rows))
    else:
        # No checkpoints at all (e.g. old job pre-incremental-writer) -> full re-process.
        deduped_rows = deduped_all
        logger.info("Job %s: no checkpoints found, full re-process (%d rows)", job_id, total_deduped)

    # Update restart count
    store = job_store.get_store()
    new_restart_count = store.increment_restart_count(job_id)

    # Create new job
    new_job_id = str(uuid.uuid4())
    store.create_enrichment_job(
        job_id=new_job_id,
        user_id=current_user["user_id"],
        total=len(deduped_rows),
        filename=original_job['filename'],
        domain_col=original_job['domain_col'],
        original_filename=original_job.get('original_filename', ''),
        parent_job_id=job_id,  # Track original job for restart chain
        source_type="restart",
        name_col=original_job.get('name_col'),
        first_name_col=original_job.get('first_name_col'),
        last_name_col=original_job.get('last_name_col'),
        cascade_config=cascade_json,
        max_results=original_job.get('max_results', 5),
        selected_providers=selected_providers,
        linkedin_url_col=original_job.get('linkedin_url_col'),
        phone_col=original_job.get('phone_col'),
        company_name_col=original_job.get('company_name_col'),
        existing_email_col=original_job.get('existing_email_col'),
        normalize_domains=orig_normalize,
        dedupe_by_domain=orig_dedupe,
        deduped_rows=deduped_count,
        dedupe_skipped_domains=json.dumps(skipped_domains),
    )

    # NOTE: deliberately do NOT pre-seed the new job's checkpoints with original
    # row indices — that was an index-space bug (the new job's total is the filtered
    # count, not the original's). The new job checkpoints its own rows per-batch as it runs.

    # Set up signals and background task
    _job_signals[new_job_id] = asyncio.Event()
    _active_jobs.add(new_job_id)

    # Always use _run_domain_enrich_job for restarts (it now has provider selection support)
    background_tasks.add_task(
        _run_domain_enrich_job,
        job_id=new_job_id,
        rows=deduped_rows,
        domain_col=original_job['domain_col'],
        name_col=original_job.get('name_col'),
        first_name_col=original_job.get('first_name_col'),
        last_name_col=original_job.get('last_name_col'),
        linkedin_url_col=original_job.get('linkedin_url_col'),
        phone_col=original_job.get('phone_col'),
        company_name_col=original_job.get('company_name_col'),
        existing_email_col=original_job.get('existing_email_col'),
        max_results=original_job.get('max_results', 5),
        selected_providers=selected_providers,
        normalize_domains=orig_normalize,
        prepend_rows=prepend_rows,
    )

    logger.info("Restarted enrichment job %s as new job %s with providers %s", job_id, new_job_id, selected_providers)

    return {
        "job_id": new_job_id,
        "total": len(deduped_rows),
        "restarted_from": job_id,
        "deduped_count": deduped_count,
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

    # Persist to database (survives worker restarts)
    store.save_job_state(job_id, "cancelled")

    # Also update database for persistent cancellation
    store.set_cancelled(job_id)

    # Remove from active jobs set and remove from persistence
    _active_jobs.discard(job_id)
    store.remove_job_state(job_id)

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

    Uses heartbeat-based detection: a job is stale only if it has not received
    a heartbeat in the last 2 minutes. This prevents false positives when the
    server is restarted but the underlying job is still healthy.
    """
    store = job_store.get_store()
    stale = store.get_stale_running_jobs_by_heartbeat()
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


class EmployeeSearchRequest(BaseModel):
    """Request model for direct people search (Flow: Find People)."""
    seniority: Optional[list[str]] = None
    function: Optional[list[str]] = None
    geo_country: Optional[list[str]] = None
    industry: Optional[list[str]] = None
    title_keywords: Optional[str] = None
    name_contains: Optional[str] = None
    has_email: Optional[bool] = None
    universe: Optional[str] = None
    limit: int = 50
    offset: int = 0


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


# =============================================================================
# LEGACY ENDPOINTS
# These endpoints are kept for backward compatibility but are NOT used by
# the current frontend. The frontend now uses:
#   - /flows/domain-enrich (with provider selection)
#   - /by-linkedin-v2 (unified LinkedIn enrichment)
#
# To revert: Update frontend to call these endpoints instead of the new ones.
# =============================================================================

# --- Legacy Flow 1: Domain Enrichment (Extended) ---
# FRONTEND NOW USES: /flows/domain-enrich (with provider selection)

@router.post("/by-domains")
async def enrich_by_domains(
    req: StartJobRequest,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(auth.get_current_user),
):
    """
    [LEGACY] Flow 1: Domain → Generic Emails + Decision Makers

    DEPRECATED: Use /flows/domain-enrich instead for provider selection.

    Upload a CSV with domains and get:
    - Generic emails per domain
    - Up to 5 decision makers per company

    This extends the existing enrichment endpoint with additional options.
    Uses _run_job (pipeline-based) instead of _run_domain_enrich_job.
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
    # Build cascade from titles if provided, otherwise use default
    title_cascade = _titles_to_cascade(req.titles or "")
    cascade = title_cascade if title_cascade else blitz_client.DEFAULT_CASCADE

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
        linkedin_url_col=req.linkedin_url_col,
        phone_col=req.phone_col,
        company_name_col=req.company_name_col,
        existing_email_col=req.existing_email_col,
        source_type="csv_upload",
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
        linkedin_url_col=req.linkedin_url_col,
        phone_col=req.phone_col,
        company_name_col=req.company_name_col,
        existing_email_col=req.existing_email_col,
    )

    return {"job_id": job_id, "total": len(rows), "flow": "domain_enrichment"}


class ProviderToggleRequest(BaseModel):
    """
    Request with optional provider selection for Flow 1 using list_builder.

    Fields:
        upload_id: ID of the uploaded CSV file to process.
        domain_col: Name of the column containing company domains.
        name_col: Optional column name for full names.
        first_name_col: Optional column name for first names.
        last_name_col: Optional column name for last names.
        max_results: Maximum number of contacts per domain (default: 5).
        providers: List of providers to use (e.g., ["blitz", "better_enrich"]).
            contacts_db is always used first and cannot be disabled.
            If None, all enabled providers are used.
        linkedin_url_col: Optional column name for LinkedIn URLs.
        phone_col: Optional column name for phone numbers.
        company_name_col: Optional column name for company names.
        existing_email_col: Optional column name for existing emails.
        titles: Optional comma-separated titles for fuzzy search.
            Example: "dentist,orthodontist,dmd"
            Enables fuzzy matching against LinkedIn headlines.
            Leave empty for default business titles (Owner, CEO, VP, Director).
            Max 50 titles.
        normalize_domains: Whether to normalize domain formats (default: True).
        dedupe_by_domain: Whether to deduplicate results by domain (default: True).
    """
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
    # Optional column mappings for high-value identifiers. All optional and
    # backward-compatible: when omitted, the request behaves as before.
    linkedin_url_col: Optional[str] = None
    company_linkedin_col: Optional[str] = None
    phone_col: Optional[str] = None
    company_name_col: Optional[str] = None
    existing_email_col: Optional[str] = None
    # Optional comma-separated titles for fuzzy search
    # Example: "dentist,orthodontist,dmd"
    # Enables fuzzy matching against LinkedIn headlines
    # Leave empty for default business titles (Owner, CEO, VP, Director)
    titles: Optional[str] = None
    # Pre-processing flags. Both default ON to preserve existing behavior.
    normalize_domains: bool = True
    dedupe_by_domain: bool = True
    # Phase 2 (2026-07-22): optional Contacts DB source filter (e.g.
    # "outscraper"). When set, narrows the by-company internal-DB lookup to
    # contacts tagged with that source only. ``None`` → all sources (today's
    # behavior — no regression). NOT a cascade provider; ignored by the paid
    # waterfall.
    source: Optional[str] = None


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

    Title Filtering:
    - Use the 'titles' field for fuzzy search against LinkedIn headlines.
    - Example: "dentist,orthodontist,dmd" finds professionals with those titles.
    - Leave empty for default business titles (Owner, CEO, VP, Director).
    - Max 50 titles supported.
    """
    upload_path = UPLOAD_DIR / f"{req.upload_id}.csv"
    if not upload_path.exists():
        raise HTTPException(status_code=404, detail="Upload not found.")

    df = pd.read_csv(str(upload_path), skipinitialspace=True)
    if req.domain_col not in df.columns:
        raise HTTPException(
            status_code=400, detail=f"Column '{req.domain_col}' not found in CSV."
        )

    # Validate titles if provided
    if req.titles is not None and req.titles != "":
        title_list = [t.strip() for t in req.titles.split(",")]
        if not title_list or all(t == "" for t in title_list):
            raise HTTPException(
                status_code=400,
                detail="Titles cannot be empty"
            )
        if len(title_list) > 50:
            raise HTTPException(
                status_code=400,
                detail="Maximum 50 titles allowed. Contact support for bulk operations."
            )

    # Convert titles to cascade_config if provided
    cascade_json = None
    if req.titles and req.titles.strip():
        cascade = _titles_to_cascade(req.titles)
        if cascade:
            cascade_json = json.dumps(cascade)

    rows = df.fillna("").astype(str).to_dict(orient="records")

    # Pre-processing: dedupe by domain column (default ON). Runs BEFORE
    # job creation so total reflects unique rows. deduped_count and the
    # raw skipped values are persisted for auditability.
    if req.dedupe_by_domain:
        deduped_rows, deduped_count, skipped_domains = identifier_utils.dedupe_rows_by_domain(
            rows, req.domain_col, req.normalize_domains
        )
    else:
        deduped_rows, deduped_count, skipped_domains = rows, 0, []

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
        total=len(deduped_rows),
        filename=str(req.upload_id),
        domain_col=req.domain_col,
        original_filename=original_filename,
        name_col=req.name_col,
        first_name_col=req.first_name_col,
        last_name_col=req.last_name_col,
        cascade_config=cascade_json,
        max_results=req.max_results,
        selected_providers=req.providers,
        linkedin_url_col=req.linkedin_url_col,
        source_type="csv_upload",
        phone_col=req.phone_col,
        company_name_col=req.company_name_col,
        existing_email_col=req.existing_email_col,
        normalize_domains=req.normalize_domains,
        dedupe_by_domain=req.dedupe_by_domain,
        deduped_rows=deduped_count,
        dedupe_skipped_domains=json.dumps(skipped_domains),
    )

    _job_signals[job_id] = asyncio.Event()
    _active_jobs.add(job_id)

    background_tasks.add_task(
        _run_domain_enrich_job,
        job_id=job_id,
        rows=deduped_rows,
        domain_col=req.domain_col,
        name_col=req.name_col,
        first_name_col=req.first_name_col,
        last_name_col=req.last_name_col,
        linkedin_url_col=req.linkedin_url_col,
        company_linkedin_col=req.company_linkedin_col,
        phone_col=req.phone_col,
        company_name_col=req.company_name_col,
        existing_email_col=req.existing_email_col,
        max_results=req.max_results,
        selected_providers=req.providers,
        normalize_domains=req.normalize_domains,
        source=req.source,
    )

    return {
        "job_id": job_id,
        "total": len(deduped_rows),
        "flow": "domain_enrichment",
        "deduped_count": deduped_count,
    }


async def _run_domain_enrich_job(
    job_id: str,
    rows: list[dict[str, Any]],
    domain_col: str,
    name_col: Optional[str],
    first_name_col: Optional[str],
    last_name_col: Optional[str],
    linkedin_url_col: Optional[str] = None,
    company_linkedin_col: Optional[str] = None,
    phone_col: Optional[str] = None,
    company_name_col: Optional[str] = None,
    existing_email_col: Optional[str] = None,
    max_results: int = 5,
    selected_providers: Optional[list[str]] = None,
    normalize_domains: bool = True,
    source: Optional[str] = None,
    prepend_rows: Optional[list[dict]] = None,
):
    """Background task to run domain enrichment using list_builder."""
    store = job_store.get_store()
    store.set_running(job_id)
    # Set initial heartbeat so cleanup_stale_jobs doesn't mark us as abandoned too soon
    store.heartbeat(job_id)
    seq = [0]

    output_path = OUTPUT_DIR / f"{job_id}.csv"

    # Phase 1: per-job collector; drained by ``_run_background_sync``.
    collector = RawContactCollector(job_id=job_id)

    # Start heartbeat task (updates last_heartbeat every 30s)
    async def heartbeat_loop():
        try:
            while True:
                await asyncio.sleep(30)
                try:
                    heartbeat_store = job_store.get_store()
                    heartbeat_store.heartbeat(job_id)
                except Exception as hb_err:
                    logger.warning("Heartbeat failed for %s: %s", job_id, hb_err)
        except asyncio.CancelledError:
            pass

    heartbeat_task = asyncio.create_task(heartbeat_loop())

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

    # Shared progress store for record_provider_use closure below
    progress_store = job_store.get_store()

    try:
        # Check job cancellation
        def check_job_cancelled(jid: str) -> bool:
            check_store = job_store.get_store()
            return check_store.is_job_cancelled_or_abandoned(jid)

        # Track which providers are actually used
        used_providers_set: set[str] = set()
        def record_provider_use(provider: str) -> None:
            if provider not in used_providers_set:
                used_providers_set.add(provider)
                # Also persist to DB periodically
                progress_store.update_used_providers(job_id, provider)

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
            record_provider_use=record_provider_use,
            normalize_domains=normalize_domains,
            collector=collector,
            company_linkedin_col=company_linkedin_col,
            linkedin_url_col=linkedin_url_col,
            source=source,
            output_path=output_path,
            write_incremental=True,
            phone_col=phone_col,
            company_name_col=company_name_col,
            existing_email_col=existing_email_col,
            return_partial_on_cancel=True,
            prepend_rows=prepend_rows,
        )

        # The incremental writer (write_incremental=True above) already flushed
        # every completed batch to output_path WITH input_* columns attached.
        # This block is now a FALLBACK: it runs only if the incremental writer
        # could not initialise (csv_writer came back None) and no file exists —
        # so the job never ends up 'done'/'partial' without a CSV. Dead path on
        # the normal incremental run (file already exists -> skipped).
        if output_rows and not output_path.exists():
            for out_row in output_rows:
                payload = identifier_utils.build_row_identifier_payload(
                    out_row,
                    domain_col=domain_col,
                    name_col=name_col,
                    first_name_col=first_name_col,
                    last_name_col=last_name_col,
                    linkedin_url_col=linkedin_url_col,
                    phone_col=phone_col,
                    company_name_col=company_name_col,
                    existing_email_col=existing_email_col,
                )
                identifier_utils.attach_input_columns(out_row, payload)
            out_df = pd.DataFrame(output_rows)
            input_cols = [c for c in out_df.columns if c not in list_builder.ENRICHED_COLUMNS]
            ordered = input_cols + [c for c in list_builder.ENRICHED_COLUMNS if c in out_df.columns]
            out_df[ordered].to_csv(str(output_path), index=False)
            logger.warning("Job %s: incremental writer was inactive; wrote %d rows end-of-run as fallback", job_id, len(output_rows))
        elif not output_rows and not output_path.exists():
            output_path.write_text("")

        # Cancel/abandon handling. With return_partial_on_cancel=True the
        # pipeline RETURNS whatever completed (it does not raise), and the
        # incremental writer has already flushed those batches to output_path.
        # Mark the job 'partial' (downloadable + resumable), register the partial
        # path so the UI's "Download Partial" button appears, and drain any
        # remaining contacts to the DB as a safety net (idempotent). For a normal
        # completed job this check is False and is skipped.
        _cancelled_mid_run = (job_id in _cancelled_jobs) or check_job_cancelled(job_id)
        if _cancelled_mid_run:
            _cancelled_jobs.discard(job_id)
            _has_partial = output_path.exists() and output_path.stat().st_size > 0
            if output_rows or _has_partial:
                store.set_status(job_id, "partial")
                try:
                    store.set_partial_output_path(job_id, str(output_path))
                except Exception as perr:
                    logger.warning("Job %s: set_partial_output_path failed: %s", job_id, perr)
                logger.info("Domain enrich job %s stopped mid-run: partial CSV at %s (%d output rows)", job_id, output_path, len(output_rows))
            else:
                store.set_failed(job_id, "Job was cancelled by user.")
                logger.info("Domain enrich job %s cancelled before any rows completed", job_id)
            try:
                await _run_background_sync(job_id, output_path, collector=collector)
            except Exception as drain_err:
                logger.error("Partial-job contacts drain failed for %s: %s", job_id, drain_err)
            return

        # Defensive guard: 0 output rows on a non-empty input is always a bug.
        # Phase 1a in list_builder raises when every row fails, but a silent
        # zero (e.g. every row returned [] legitimately, which shouldn't be
        # possible) should still surface as `failed` — not `done` with a
        # 0-byte CSV the user might try to download. See csv_jobs_silent_failure_2026-07-13.md.
        rows_param_count = len(rows) if 'rows' in dir() else 0
        if len(output_rows) == 0 and rows_param_count > 0:
            raise RuntimeError(
                f"Job produced 0 output rows from {rows_param_count} input rows. "
                f"This is always a bug — check logs for 'Row processing failed' warnings."
            )

        store._mark_done_and_cleanup(job_id, output_path)
        logger.info("Domain enrich job %s completed, %d output rows", job_id, len(output_rows))

        # Sync results back to Contacts DB (async, non-blocking)
        asyncio.create_task(_run_background_sync(job_id, output_path, collector=collector))

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
                    try:
                        store.set_partial_output_path(job_id, str(output_path))
                    except Exception as perr:
                        logger.warning("Job %s: set_partial_output_path failed: %s", job_id, perr)
                    try:
                        await _run_background_sync(job_id, output_path, collector=collector)
                    except Exception as drain_err:
                        logger.error("Partial-job contacts drain failed for %s: %s", job_id, drain_err)
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

    finally:
        # Cancel heartbeat task
        if 'heartbeat_task' in locals():
            heartbeat_task.cancel()
        # Clean up state persistence
        try:
            cleanup_store = job_store.get_store()
            cleanup_store.remove_job_state(job_id)
        except Exception as cleanup_err:
            logger.warning("Failed to clean job state for %s: %s", job_id, cleanup_err)


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


@router.post("/search/employees")
async def search_employees(
    req: EmployeeSearchRequest,
    current_user: dict = Depends(auth.get_current_user),
):
    """
    Find People: direct people search against the internal contacts DB by
    role / function / location / industry (index-backed, free — no Blitz cost).
    Returns matching people with one best email each.
    """
    async with httpx.AsyncClient() as client:
        try:
            result = await contacts_client.search_people(
                client,
                seniority=",".join(req.seniority) if req.seniority else None,
                function=",".join(req.function) if req.function else None,
                geo_country=",".join(req.geo_country) if req.geo_country else None,
                industry=",".join(req.industry) if req.industry else None,
                title_keywords=req.title_keywords,
                name_contains=req.name_contains,
                has_email=req.has_email,
                universe=req.universe,
                limit=req.limit,
                offset=req.offset,
            )
            data = result or {}
            return {
                "total": data.get("total", 0),
                "limit": req.limit,
                "offset": req.offset,
                "people": data.get("people", []),
                "flow": "people_search",
            }
        except Exception as e:
            logger.error("People search failed: %s", e)
            raise HTTPException(status_code=500, detail=f"People search failed: {str(e)}")


@router.post("/search/companies/enrich")
async def search_and_enrich(
    req: SearchAndEnrichRequest,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(auth.get_current_user),
):
    """
    [LEGACY] Flow 2b: Search companies + Enrich

    DEPRECATED: This endpoint does not support provider selection.
    Use /flows/domain-enrich instead after extracting domains.

    1. Search for companies matching criteria
    2. Enrich each company with decision makers and emails

    Returns a job_id for tracking the enrichment process.
    Uses _run_job (pipeline-based) instead of _run_domain_enrich_job.
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
        source_type="search",
    )

    _job_signals[job_id] = asyncio.Event()
    _active_jobs.add(job_id)

    # Use _run_domain_enrich_job for provider selection support
    background_tasks.add_task(
        _run_domain_enrich_job,
        job_id=job_id,
        rows=rows,
        domain_col=domain_col,
        name_col=None,
        first_name_col=None,
        last_name_col=None,
        max_results=req.max_decision_makers,
        selected_providers=None,  # None = use all enabled providers
    )

    return {
        "job_id": job_id,
        "total": len(rows),
        "companies_found": len(companies),
        "flow": "search_and_enrich",
    }


# --- Legacy Flow 3: LinkedIn Enrichment ---
# FRONTEND NOW USES: /by-linkedin-v2 (unified personal + company LinkedIn)

@router.post("/by-linkedin")
async def enrich_by_linkedin(
    req: LinkedInEnrichRequest,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(auth.get_current_user),
):
    """
    [LEGACY] Flow 3: LinkedIn URLs → Full Enrichment

    DEPRECATED: Use /by-linkedin-v2 instead for personal + company URL support.

    Upload a CSV with LinkedIn URLs and get fully enriched data:
    - Person details (name, title, company)
    - Work email
    - Phone (if available)
    - Company details

    Uses _run_linkedin_job (pipeline-based) instead of _run_linkedin_v2_job.
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
        source_type="linkedin",
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
    # Set initial heartbeat so cleanup_stale_jobs doesn't mark us as abandoned too soon
    store.heartbeat(job_id)
    seq = [0]

    output_path = OUTPUT_DIR / f"{job_id}.csv"

    # Phase 1: per-job collector. The legacy ``run_linkedin_enrichment``
    # path does not feed the collector (person-level captures come in a
    # later phase), but we still create + drain so the code path is
    # uniform with the other job runners and any future captures land
    # automatically.
    collector = RawContactCollector(job_id=job_id)

    # Start heartbeat task (updates last_heartbeat every 30s)
    async def heartbeat_loop():
        try:
            while True:
                await asyncio.sleep(30)
                try:
                    heartbeat_store = job_store.get_store()
                    heartbeat_store.heartbeat(job_id)
                except Exception as hb_err:
                    logger.warning("Heartbeat failed for %s: %s", job_id, hb_err)
        except asyncio.CancelledError:
            pass

    heartbeat_task = asyncio.create_task(heartbeat_loop())

    async def on_progress(e: dict[str, Any]):
        progress_store = job_store.get_store()
        progress_store.append_event(job_id, seq[0], e)
        seq[0] += 1

        # Write checkpoint every 100 rows for incremental resume
        row_index = e.get("index", 0)
        if row_index % 100 == 0:
            progress_store.write_checkpoint(job_id, row_index)

        sig = _job_signals.get(job_id)
        if sig:
            sig.set()
            sig.clear()

    try:
        # Check job cancellation (mirrors _run_domain_enrich_job)
        def check_job_cancelled(jid: str) -> bool:
            check_store = job_store.get_store()
            return check_store.is_job_cancelled_or_abandoned(jid)

        output_rows = await list_builder.run_linkedin_enrichment(
            rows=rows,
            linkedin_col=linkedin_col,
            on_progress=on_progress,
            record_provider_use=None,
            cancelled_jobs=_cancelled_jobs,
            check_cancelled=check_job_cancelled,
            job_id=job_id,
            output_path=output_path,
            write_incremental=True,
            return_partial_on_cancel=True,
        )

        # The incremental writer (write_incremental=True above) already flushed
        # every completed batch to output_path WITH input_* columns attached.
        # This block is now a FALLBACK: it runs only if the incremental writer
        # could not initialise (csv_writer came back None) and no file exists —
        # so the job never ends up 'done'/'partial' without a CSV. Dead path on
        # the normal incremental run (file already exists -> skipped). Mirrors
        # _run_domain_enrich_job.
        if output_rows and not output_path.exists():
            for out_row in output_rows:
                payload = identifier_utils.build_row_identifier_payload(
                    out_row,
                    linkedin_url_col=linkedin_col,
                )
                identifier_utils.attach_input_columns(out_row, payload)
            out_df = pd.DataFrame(output_rows)
            input_cols = [c for c in out_df.columns if c not in list_builder.ENRICHED_COLUMNS]
            ordered = input_cols + [c for c in list_builder.ENRICHED_COLUMNS if c in out_df.columns]
            out_df[ordered].to_csv(str(output_path), index=False)
            logger.warning("Job %s: incremental writer was inactive; wrote %d rows end-of-run as fallback", job_id, len(output_rows))
        elif not output_rows and not output_path.exists():
            output_path.write_text("")

        # Cancel/abandon handling. With return_partial_on_cancel=True the
        # pipeline RETURNS whatever completed (it does not raise), and the
        # incremental writer has already flushed those batches to output_path.
        # Mark the job 'partial' (downloadable + resumable), register the partial
        # path so the UI's "Download Partial" button appears, and drain any
        # remaining contacts to the DB as a safety net (idempotent). For a normal
        # completed job this check is False and is skipped. Mirrors
        # _run_domain_enrich_job.
        _cancelled_mid_run = (job_id in _cancelled_jobs) or check_job_cancelled(job_id)
        if _cancelled_mid_run:
            _cancelled_jobs.discard(job_id)
            _has_partial = output_path.exists() and output_path.stat().st_size > 0
            if output_rows or _has_partial:
                store.set_status(job_id, "partial")
                try:
                    store.set_partial_output_path(job_id, str(output_path))
                except Exception as perr:
                    logger.warning("Job %s: set_partial_output_path failed: %s", job_id, perr)
                logger.info("LinkedIn enrich job %s stopped mid-run: partial CSV at %s (%d output rows)", job_id, output_path, len(output_rows))
            else:
                store.set_failed(job_id, "Job was cancelled by user.")
                logger.info("LinkedIn enrich job %s cancelled before any rows completed", job_id)
            try:
                await _run_background_sync(job_id, output_path, collector=collector)
            except Exception as drain_err:
                logger.error("Partial-job contacts drain failed for %s: %s", job_id, drain_err)
            return

        store._mark_done_and_cleanup(job_id, output_path)
        logger.info("LinkedIn enrichment job %s completed, %d output rows", job_id, len(output_rows))

        # Sync results back to Contacts DB (async, non-blocking)
        asyncio.create_task(_run_background_sync(job_id, output_path, collector=collector))

    except Exception as e:
        error_msg = str(e)
        # Cancel/abandon exception path: the incremental writer has already
        # flushed completed batches to output_path. Mark 'partial' if a
        # non-empty file exists, drain contacts as a safety net. Mirrors
        # _run_domain_enrich_job.
        if "was cancelled" in error_msg or "was abandoned" in error_msg:
            if "abandoned" in error_msg:
                final_error = "Job was abandoned due to server restart. Please retry from the jobs page."
            elif "cancelled by user" in error_msg:
                final_error = "Job was cancelled by user."
            else:
                final_error = error_msg

            logger.info("LinkedIn enrich job %s stopped: %s", job_id, final_error)
            _cancelled_jobs.discard(job_id)

            if output_path.exists() and output_path.stat().st_size > 0:
                store.set_status(job_id, "partial")
                try:
                    store.set_partial_output_path(job_id, str(output_path))
                except Exception as perr:
                    logger.warning("Job %s: set_partial_output_path failed: %s", job_id, perr)
                try:
                    await _run_background_sync(job_id, output_path, collector=collector)
                except Exception as drain_err:
                    logger.error("Partial-job contacts drain failed for %s: %s", job_id, drain_err)
                logger.info("Job %s has partial output at %s", job_id, output_path)
            else:
                store.set_failed(job_id, final_error)
        else:
            logger.exception("LinkedIn enrichment job %s failed: %s", job_id, e)
            if output_path.exists() and output_path.stat().st_size > 0:
                store._mark_done_and_cleanup(job_id, output_path)
                # Even on partial success, attempt a sync of whatever rows landed.
                asyncio.create_task(_run_background_sync(job_id, output_path, collector=collector))
            else:
                store.set_failed(job_id, str(e))

    finally:
        # Cancel heartbeat task
        if 'heartbeat_task' in locals():
            heartbeat_task.cancel()
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
        source_type="linkedin",
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
    # Set initial heartbeat so cleanup_stale_jobs doesn't mark us as abandoned too soon
    store.heartbeat(job_id)
    seq = [0]

    output_path = OUTPUT_DIR / f"{job_id}.csv"

    # Phase 1: per-job collector; drained by ``_run_background_sync``.
    collector = RawContactCollector(job_id=job_id)

    # Start heartbeat task (updates last_heartbeat every 30s). Mirrors the
    # domain-enrich path. Without it the stale-job reaper marks LinkedIn jobs
    # abandoned after ~3 minutes even while healthy (the 2026-07-15 outage).
    async def heartbeat_loop():
        try:
            while True:
                await asyncio.sleep(30)
                try:
                    heartbeat_store = job_store.get_store()
                    heartbeat_store.heartbeat(job_id)
                except Exception as hb_err:
                    logger.warning("Heartbeat failed for %s: %s", job_id, hb_err)
        except asyncio.CancelledError:
            pass

    heartbeat_task = asyncio.create_task(heartbeat_loop())

    def on_progress(e: dict[str, Any]):
        progress_store = job_store.get_store()
        progress_store.append_event(job_id, seq[0], e)
        seq[0] += 1

        # Write checkpoint every 100 rows for incremental resume
        row_index = e.get("index", 0)
        if row_index % 100 == 0:
            progress_store.write_checkpoint(job_id, row_index)

        sig = _job_signals.get(job_id)
        if sig:
            sig.set()

    # Track which providers are actually used so the job's ``used_providers``
    # tally stays accurate (mirrors the domain-enrich path). Without this the
    # LinkedIn flow always reported only the default ``["contacts_db"]``.
    used_providers_set: set[str] = set()

    def record_provider_use(provider: str) -> None:
        if provider not in used_providers_set:
            used_providers_set.add(provider)
            try:
                usage_store = job_store.get_store()
                usage_store.update_used_providers(job_id, provider)
            except Exception as e:
                logger.warning("update_used_providers(%s, %s) failed: %s", job_id, provider, e)

    try:
        # Check job cancellation (mirrors _run_domain_enrich_job)
        def check_job_cancelled(jid: str) -> bool:
            check_store = job_store.get_store()
            return check_store.is_job_cancelled_or_abandoned(jid)

        output_rows = await list_builder.run_unified_linkedin_enrichment(
            rows=rows,
            personal_col=personal_linkedin_col,
            company_col=company_linkedin_col,
            max_dms=max_dms,
            include_company=include_company,
            on_progress=on_progress,
            collector=collector,
            record_provider_use=record_provider_use,
            cancelled_jobs=_cancelled_jobs,
            check_cancelled=check_job_cancelled,
            job_id=job_id,
            output_path=output_path,
            write_incremental=True,
            return_partial_on_cancel=True,
        )

        # The incremental writer (write_incremental=True above) already flushed
        # every completed batch to output_path WITH input_* columns attached.
        # This block is now a FALLBACK: it runs only if the incremental writer
        # could not initialise (csv_writer came back None) and no file exists —
        # so the job never ends up 'done'/'partial' without a CSV. Dead path on
        # the normal incremental run (file already exists -> skipped). Mirrors
        # _run_domain_enrich_job.
        if output_rows and not output_path.exists():
            for out_row in output_rows:
                payload = identifier_utils.build_row_identifier_payload(
                    out_row,
                    linkedin_url_col=personal_linkedin_col,
                )
                identifier_utils.attach_input_columns(out_row, payload)
            out_df = pd.DataFrame(output_rows)
            input_cols = [c for c in out_df.columns if c not in list_builder.ENRICHED_COLUMNS]
            ordered = input_cols + [c for c in list_builder.ENRICHED_COLUMNS if c in out_df.columns]
            out_df[ordered].to_csv(str(output_path), index=False)
            logger.warning("Job %s: incremental writer was inactive; wrote %d rows end-of-run as fallback", job_id, len(output_rows))
        elif not output_rows and not output_path.exists():
            output_path.write_text("")

        # Cancel/abandon handling. With return_partial_on_cancel=True the
        # pipeline RETURNS whatever completed (it does not raise), and the
        # incremental writer has already flushed those batches to output_path.
        # Mark the job 'partial' (downloadable + resumable), register the partial
        # path so the UI's "Download Partial" button appears, and drain any
        # remaining contacts to the DB as a safety net (idempotent). For a normal
        # completed job this check is False and is skipped. Mirrors
        # _run_domain_enrich_job.
        _cancelled_mid_run = (job_id in _cancelled_jobs) or check_job_cancelled(job_id)
        if _cancelled_mid_run:
            _cancelled_jobs.discard(job_id)
            _has_partial = output_path.exists() and output_path.stat().st_size > 0
            if output_rows or _has_partial:
                store.set_status(job_id, "partial")
                try:
                    store.set_partial_output_path(job_id, str(output_path))
                except Exception as perr:
                    logger.warning("Job %s: set_partial_output_path failed: %s", job_id, perr)
                logger.info("LinkedIn v2 enrich job %s stopped mid-run: partial CSV at %s (%d output rows)", job_id, output_path, len(output_rows))
            else:
                store.set_failed(job_id, "Job was cancelled by user.")
                logger.info("LinkedIn v2 enrich job %s cancelled before any rows completed", job_id)
            try:
                await _run_background_sync(job_id, output_path, collector=collector)
            except Exception as drain_err:
                logger.error("Partial-job contacts drain failed for %s: %s", job_id, drain_err)
            return

        store._mark_done_and_cleanup(job_id, output_path)
        logger.info(
            "LinkedIn v2 enrichment job %s completed, %d output rows",
            job_id,
            len(output_rows),
        )

        # Sync results back to Contacts DB (async, non-blocking)
        asyncio.create_task(_run_background_sync(job_id, output_path, collector=collector))

    except Exception as e:
        error_msg = str(e)
        # Cancel/abandon exception path: the incremental writer has already
        # flushed completed batches to output_path. Mark 'partial' if a
        # non-empty file exists, drain contacts as a safety net. Mirrors
        # _run_domain_enrich_job.
        if "was cancelled" in error_msg or "was abandoned" in error_msg:
            if "abandoned" in error_msg:
                final_error = "Job was abandoned due to server restart. Please retry from the jobs page."
            elif "cancelled by user" in error_msg:
                final_error = "Job was cancelled by user."
            else:
                final_error = error_msg

            logger.info("LinkedIn v2 enrich job %s stopped: %s", job_id, final_error)
            _cancelled_jobs.discard(job_id)

            if output_path.exists() and output_path.stat().st_size > 0:
                store.set_status(job_id, "partial")
                try:
                    store.set_partial_output_path(job_id, str(output_path))
                except Exception as perr:
                    logger.warning("Job %s: set_partial_output_path failed: %s", job_id, perr)
                try:
                    await _run_background_sync(job_id, output_path, collector=collector)
                except Exception as drain_err:
                    logger.error("Partial-job contacts drain failed for %s: %s", job_id, drain_err)
                logger.info("Job %s has partial output at %s", job_id, output_path)
            else:
                store.set_failed(job_id, final_error)
        else:
            logger.exception("LinkedIn v2 enrichment job %s failed: %s", job_id, e)
            if output_path.exists() and output_path.stat().st_size > 0:
                store._mark_done_and_cleanup(job_id, output_path)
                # Even on partial success, attempt a sync of whatever rows landed.
                asyncio.create_task(_run_background_sync(job_id, output_path, collector=collector))
            else:
                store.set_failed(job_id, str(e))

    finally:
        # Cancel heartbeat task
        if 'heartbeat_task' in locals():
            heartbeat_task.cancel()
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
