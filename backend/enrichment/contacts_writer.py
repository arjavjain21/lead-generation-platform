"""
Centralized Contacts DB write-back for enrichment results.

This module is the single entry point for pushing enrichment results into the
internal Contacts DB (leadsdatabase.cc). All enrichment flows (direct API,
CSV jobs, list_builder, LinkedIn, chain-to-enrichment, fallback) MUST route
through `write_enrichment_result()` to enforce idempotency, person/company
email separation, and durable outbox retry semantics.

Design goals:
- Single point of control so future provider additions cannot bypass write-back.
- Idempotent upserts keyed by `email` (person) or `domain` (company placeholder).
- Person emails (dm_email) and company/page emails (company_email) stay
  strictly separate. company_email is NEVER stored as dm_email.
- Failed writes are queued in `contacts_write_outbox` for durable retry.
- Configurable strictness via env flags:
    CONTACTS_WRITEBACK_REQUIRED=true  -> outbox failure fails loudly
    CONTACTS_WRITEBACK_ALLOW_OUTBOX=true -> outbox used on transient errors
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

import httpx


# --- Lead-universe industry classifier -------------------------------------
# Pure Python mirror of the contacts-DB function core.fn_classify_industry:
# same normalize step (lowercase; _ / & -> space) and the same pattern buckets,
# checked in the same precedence order (saas -> local -> ecom -> b2b), so that
# write-back classification agrees with the DB-side backfill. Returns "" for
# ambiguous / missing industry (callers treat "" as "no tag" -> no regression).
_CLASSIFY_SAAS = (
    "software", "saas", "cloud computing", "artificial intelligence",
    "machine learning", "cybersecurity", "data process", "tech platform",
    "computer software", "information and internet",
)
_CLASSIFY_LOCAL = (
    "plumb", "hvac", "electrician", "roof", "dentist", "dental", "salon",
    "barber", " spa", "restaurant", "cafe", "coffee", "gym", "fitness",
    "wellness", "pet groom", "pet store", "moving", "fence", "landscap",
    "tree service", "cleaning", "auto repair", "automotive repair",
    "real estate", "hotel", "hospitality", "hospital", "medical practice",
    "medical clinic", "medical spa", "veterinar", "bakery", "grocery",
    "catering", "photograph", "florist", "pest control", "locksmith",
    "travel", "leisure", "recreation", "food and bev",
)
_CLASSIFY_ECOM = (
    "retail", "apparel", "fashion", "consumer goods", "cosmetic",
    "beauty product", "jewel", "consumer electronic", "ecom", "e-commerce",
    "merchand", "sporting goods", "furniture store", "interior design brand",
)
_CLASSIFY_B2B = (
    "agency", "consulting", "consultant", "manufacturing", "machinery",
    "staffing", "recruit", "financial service", "financial planner", "bank",
    "insurance", "accounting", "engineering", "contractor", "construction",
    "legal", "attorney", "lawyer", "public relations", "advertising",
    "marketing", "information technology", "telecommunication", "semiconductor",
    "chemical", "research", "medical device", "food production",
    "pharmaceutical", "biotech", "logistics", "warehouse", "wholesale",
    "printing", "publishing", "media", "broadcast", "entertainment", "music",
    "website", "design service", "cabinet", "automotive", "events service",
    "training", "energy", "mining", "education",
)


def classify_industry(industry: str) -> str:
    """Classify a company industry into a lead_universe bucket.

    Mirror of core.fn_classify_industry. Returns one of
    'saas' / 'local_business' / 'ecom' / 'b2b_agency', or '' when no rule
    matches (ambiguous / missing). Pure + deterministic.
    """
    if not industry:
        return ""
    norm = re.sub(r"[_/&]", " ", industry).lower()
    for pat in _CLASSIFY_SAAS:
        if pat in norm:
            return "saas"
    for pat in _CLASSIFY_LOCAL:
        if pat in norm:
            return "local_business"
    for pat in _CLASSIFY_ECOM:
        if pat in norm:
            return "ecom"
    for pat in _CLASSIFY_B2B:
        if pat in norm:
            return "b2b_agency"
    return ""


# Phase 2 (full capture): firmographic attributes with no dedicated
# PersonUpsertRequest field travel via custom_fields (merged into
# core.person.custom_fields JSONB by the Contacts DB — the only
# server-persisted catch-all). Tuples are (custom_fields_key, payload_key):
# the FIRST element is the key written into custom_fields, the SECOND the
# payload key read from the enrichment row. Each entry only lands when the
# payload value is non-empty, so existing records are never blanked and a
# caller-supplied custom_fields entry is never overwritten with ''.
# seg_classification / seg_provider (2026-08-25) carry the platform's SEG
# (MX gateway) verdict — belt-and-suspenders for the contacts DB's own 6h
# domain-map cron, which populates the canonical core.email.seg_* columns.
_FIRMOGRAPHIC_CUSTOM_FIELDS: tuple[tuple[str, str], ...] = (
    ("headline", "dm_headline"),
    ("industry", "company_industry"),
    ("employee_count", "company_employee_count"),
    ("revenue", "company_revenue"),
    ("city", "dm_location_city"),
    ("country", "dm_location_country"),
    ("linkedin_connections", "dm_linkedin_connections"),
    ("email_last_verified_at", "dm_email_last_verified_at"),
    ("seg_classification", "seg_classification"),
    ("seg_provider", "seg_provider"),
)


# Local module; keep import lazy in functions to avoid circulars during
# package init (contacts_writer is imported by routes which imports many things).
try:
    from .identifier_utils import normalize_linkedin_url as _normalize_linkedin
except ImportError:  # pragma: no cover - fallback for direct script run
    def _normalize_linkedin(v):  # type: ignore[no-redef]
        if not v:
            return ""
        s = str(v).strip().lower()
        if "linkedin.com" not in s:
            return ""
        for prefix in ("https://", "http://"):
            if s.startswith(prefix):
                s = s[len(prefix):]
                break
        if s.startswith("www."):
            s = s[4:]
        return s.rstrip("/")

from shared import db
from . import contacts_client as _contacts_client
from . import seg

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

CONTACTS_WRITEBACK_REQUIRED = os.getenv("CONTACTS_WRITEBACK_REQUIRED", "true").lower() == "true"
CONTACTS_WRITEBACK_ALLOW_OUTBOX = os.getenv("CONTACTS_WRITEBACK_ALLOW_OUTBOX", "true").lower() == "true"


def is_v2_enabled() -> bool:
    """True if the routes.py sync sites should use the new contacts_writer
    path instead of the legacy sync_contacts path.

    Indirection (function vs constant) so tests can monkeypatch without
    mutating the import-time env value.
    """
    return os.getenv("USE_CONTACTS_WRITER_V2", "false").lower() == "true"


# A row is considered to have an "email" only if it has @ and is not the
# placeholder "no_email" sentinel used in older outputs.
_PLACEHOLDER_EMAILS = {"", "no_email", "n/a", "none"}


def _is_duplicate_response(body: str) -> bool:
    """Detect a 'row already in DB' response from the Contacts API.

    The Contacts API returns a 400/422 for duplicate-key violations. The
    response body has the shape:

        {"detail": {"error": "duplicate key value violates unique constraint ..."}}

    but `detail` can also be a string like "Contact already exists" in
    some cases. We match on multiple substrings to cover both shapes.
    """
    if not body:
        return False
    s = body.lower()
    # String form of detail (some endpoints return this)
    if "already exists" in s:
        return True
    # Nested-dict form of detail (the upsert endpoint returns this)
    if "duplicate key" in s or "duplicate key value" in s:
        return True
    return False


# ---------------------------------------------------------------------------
# Status enum
# ---------------------------------------------------------------------------

class WriteStatus(str, Enum):
    """Outcome of a single write_enrichment_result() call."""
    INSERTED = "inserted"
    UPDATED = "updated"
    SKIPPED = "skipped"
    SYNCED = "synced"                # generic "written" (inserted or updated)
    FAILED = "failed"
    QUEUED = "queued_for_retry"
    NO_DATA = "no_data"              # nothing to write


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------

@dataclass
class WriteResult:
    """Aggregated counters for one or many write_enrichment_result() calls."""
    inserted: int = 0
    updated: int = 0
    skipped: int = 0
    failed: int = 0
    queued: int = 0
    no_data: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def total(self) -> int:
        return self.inserted + self.updated + self.skipped + self.failed + self.queued + self.no_data

    def to_dict(self) -> dict[str, Any]:
        return {
            "inserted": self.inserted,
            "updated": self.updated,
            "skipped": self.skipped,
            "failed": self.failed,
            "queued_for_retry": self.queued,
            "no_data": self.no_data,
            "total": self.total,
        }

    def merge(self, other: "WriteResult") -> None:
        self.inserted += other.inserted
        self.updated += other.updated
        self.skipped += other.skipped
        self.failed += other.failed
        self.queued += other.queued
        self.no_data += other.no_data
        self.errors.extend(other.errors)


# ---------------------------------------------------------------------------
# Outbox table initialization
# ---------------------------------------------------------------------------

def init_outbox_table() -> None:
    """
    Create the contacts_write_outbox table if it does not exist.
    Idempotent — safe to call on every startup.
    """
    conn = db.get_db()
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS contacts_write_outbox (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id          TEXT,
            row_index       INTEGER,
            payload_json    TEXT NOT NULL,
            status          TEXT NOT NULL DEFAULT 'pending',  -- pending|done|failed
            attempt_count   INTEGER NOT NULL DEFAULT 0,
            last_error      TEXT,
            next_retry_at   INTEGER NOT NULL DEFAULT 0,
            created_at      TEXT NOT NULL,
            updated_at      TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_outbox_status
            ON contacts_write_outbox (status, next_retry_at);

        CREATE INDEX IF NOT EXISTS idx_outbox_job
            ON contacts_write_outbox (job_id, row_index);
        """
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_meaningful_email(value: Optional[str]) -> bool:
    """True if value is a real email and not a placeholder."""
    if not value:
        return False
    v = value.strip().lower()
    if v in _PLACEHOLDER_EMAILS:
        return False
    if "@" not in v:
        return False
    return True


def _normalize_domain(value: Optional[str]) -> str:
    """Lowercase + strip a domain. Empty string if not a domain."""
    if not value:
        return ""
    d = value.strip().lower()
    # Strip protocol
    for prefix in ("https://", "http://"):
        if d.startswith(prefix):
            d = d[len(prefix):]
            break
    # Strip path
    d = d.split("/", 1)[0]
    # Strip www.
    if d.startswith("www."):
        d = d[4:]
    return d


def _domain_from_email(email: str) -> str:
    if "@" not in email:
        return ""
    return email.split("@", 1)[1].strip().lower()


def _apply_seg_fields(payload: dict[str, Any]) -> dict[str, Any]:
    """Decorate ``payload`` in place with SEG verdict keys (when derivable).

    Belt-and-suspenders for the contacts DB's own domain-map cron: the
    canonical ``core.email.seg_classification`` columns are populated
    server-side, but a platform-side verdict rides along via custom_fields
    when we already know it. Lookup is cache-only (``domain_seg_cache``) —
    NO network, NO DNS, and a strict no-op when the flag is off, when the
    payload carries no domain, or on a cache miss (the row simply gets no
    seg custom_fields). A payload that already carries a verdict (upstream
    flows may set it) always wins — explicit > derived, same precedence as
    ``lead_universe`` above.
    """
    if payload.get("seg_classification") or payload.get("seg_provider"):
        return payload
    if not seg.is_seg_enabled():
        return payload
    domain = _normalize_domain(
        payload.get("normalized_domain")
        or payload.get("domain")
        or payload.get("website")
        or _domain_from_email(payload.get("dm_email") or payload.get("company_email") or "")
    )
    if not domain:
        return payload
    try:
        entry = seg.get_seg_for_domains_sync([domain]).get(seg.normalize_seg_key(domain))
    except Exception:
        return payload
    if not entry:
        return payload
    classification = str(entry.get("seg_classification") or "").strip()
    provider = str(entry.get("seg_provider") or "").strip()
    if classification:
        payload["seg_classification"] = classification
    if provider:
        payload["seg_provider"] = provider
    return payload


# ---------------------------------------------------------------------------
# Single-row write
# ---------------------------------------------------------------------------

async def write_enrichment_result(
    payload: dict[str, Any],
    *,
    client: Optional[httpx.AsyncClient] = None,
    job_id: Optional[str] = None,
    row_index: Optional[int] = None,
) -> WriteStatus:
    """
    Write one enrichment result to the Contacts DB.

    `payload` may contain any of the following fields (all optional except
    those required for the chosen write path):

        domain, normalized_domain, website
        company_name, company_linkedin_url, company_phone, company_industry,
        company_employee_count, company_revenue
        dm_full_name, dm_first_name, dm_last_name, dm_title, dm_linkedin_url,
        dm_phone, dm_headline, dm_location_city, dm_location_country,
        dm_linkedin_connections, dm_email_last_verified_at
        dm_email, dm_email_source, dm_email_verified
        mailtester_code, mailtester_message
        company_email, company_email_source, company_email_verified,
        company_email_type
        final_email, final_email_level, source_path
        job_id, row_index

    Transport note (verified against the live /v1/persons/upsert
    ``PersonUpsertRequest`` model, 2026-08-14): only email, domain, the
    name fields, title, linkedin_url, company_name/company_domain/
    company_website, email_type, verification_status, phone_number,
    phone_type, lead_universe and custom_fields are persisted; every other
    body key (phone, email_verified, email_source, ...) is silently
    ignored by the server's pydantic model. Firmographic attributes that
    have no dedicated field (headline, industry, employee count, revenue,
    city, country, LinkedIn connections, email last-verified-at) are
    therefore transported via ``custom_fields`` (merged into
    core.person.custom_fields JSONB by the Contacts DB).

    Returns a WriteStatus:
        INSERTED  - new person record created
        UPDATED   - existing person record updated with new fields
        SKIPPED   - record exists and is already current (no change needed)
        QUEUED    - Contacts DB call failed; row queued in outbox
        FAILED    - Contacts DB failed AND outbox disabled; raises LoudFailure
        NO_DATA   - nothing meaningful to write

    Person/company separation: the person email and the company email are
    written as TWO separate upserts so that company_email never replaces
    dm_email on a real person record.
    """
    own_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=30.0)
        own_client = True

    try:
        # SEG (MX gateway) verdict — cache-only lookup, mutates nothing when
        # the flag is off / the cache misses / the payload already carries one.
        _apply_seg_fields(payload)
        person_status = await _write_person_payload(client, payload, job_id, row_index)
        company_status = await _write_company_payload(client, payload, job_id, row_index)
        return _combine_status(person_status, company_status)
    finally:
        if own_client:
            await client.aclose()


def _combine_status(person: WriteStatus, company: WriteStatus) -> WriteStatus:
    """Combine two statuses into a single summary status.

    Priority: FAILED > QUEUED > INSERTED/UPDATED > SKIPPED > NO_DATA.
    """
    for s in (person, company):
        if s in (WriteStatus.FAILED, WriteStatus.QUEUED):
            return s
    for s in (person, company):
        if s in (WriteStatus.INSERTED, WriteStatus.UPDATED, WriteStatus.SYNCED):
            return s
    for s in (person, company):
        if s == WriteStatus.SKIPPED:
            return WriteStatus.SKIPPED
    return WriteStatus.NO_DATA


async def _write_person_payload(
    client: httpx.AsyncClient,
    payload: dict[str, Any],
    job_id: Optional[str],
    row_index: Optional[int],
) -> WriteStatus:
    """Write person-level fields (dm_email, name, title, LinkedIn)."""
    email = (payload.get("dm_email") or "").strip().lower()
    if not _is_meaningful_email(email):
        return WriteStatus.NO_DATA

    domain = _normalize_domain(
        payload.get("normalized_domain")
        or payload.get("domain")
        or payload.get("website")
        or _domain_from_email(email)
    )

    body: dict[str, Any] = {
        "email": email,
        "domain": domain,
        "company_domain": domain,
    }

    # Person fields — only include if non-empty so existing records are not
    # blanked out on update.
    name = (payload.get("dm_full_name") or "").strip()
    if name:
        body["full_name"] = name
    first = (payload.get("dm_first_name") or "").strip()
    if first:
        body["first_name"] = first
    last = (payload.get("dm_last_name") or "").strip()
    if last:
        body["last_name"] = last
    title = (payload.get("dm_title") or "").strip()
    if title:
        body["title"] = title
    linkedin = (payload.get("dm_linkedin_url") or "").strip()
    if linkedin:
        # Normalize LinkedIn URL to canonical form so provider variations
        # (with/without https, trailing slash, etc.) don't trigger the
        # unique-constraint duplicate in the Contacts DB.
        body["linkedin_url"] = _normalize_linkedin(linkedin) or linkedin
    phone = (payload.get("dm_phone") or "").strip()
    if phone:
        body["phone"] = phone
        # phone_number is the field the live PersonUpsertRequest model
        # actually persists (body["phone"] above is ignored server-side but
        # kept for backward compat with older readers). Non-E.164 values are
        # silently skipped by the Contacts DB, so no client-side gate needed.
        body["phone_number"] = phone

    # Company context (we set the company on the person record)
    company_name = (payload.get("company_name") or "").strip()
    if company_name:
        body["company_name"] = company_name
    company_linkedin = (payload.get("company_linkedin_url") or "").strip()
    if company_linkedin:
        body["company_linkedin_url"] = company_linkedin

    # Verification metadata
    verified = (payload.get("dm_email_verified") or "").strip().lower()
    if verified in ("yes", "no", "unknown"):
        body["email_verified"] = verified
    mail_code = (payload.get("mailtester_code") or "").strip()
    if mail_code:
        body["mailtester_code"] = mail_code
    mail_msg = (payload.get("mailtester_message") or "").strip()
    if mail_msg:
        body["mailtester_message"] = mail_msg

    # email_type — REQUIRED by Contacts DB PersonUpsert validation.
    # Contacts DB accepts a fixed enum ('work', 'personal', 'other'). Upstream
    # providers may emit values outside that enum — e.g. BetterEnrich tags
    # generic company emails (info@, contact@) as email_type='generic', which
    # Contacts DB rejects with HTTP 400. We normalize: if the incoming value
    # isn't in the known-allowed set, default to 'work' (all enrichment-sourced
    # emails are business/work emails). Bug observed 2026-07-13: 1,457 stuck
    # outbox rows with email_type='generic'.
    _ALLOWED_EMAIL_TYPES = frozenset({"work", "personal", "other"})
    raw_email_type = (payload.get("email_type") or "").strip().lower()
    email_type = raw_email_type if raw_email_type in _ALLOWED_EMAIL_TYPES else "work"
    body["email_type"] = email_type

    # Source metadata (compact)
    source = (payload.get("dm_email_source") or "").strip()
    if source:
        body["email_source"] = source
    source_path = (payload.get("source_path") or "").strip()
    if source_path:
        body["source_path"] = source_path

    # Universe tag: an explicit origin tag wins (e.g. 'local_business' for
    # scraper.tech-origin); otherwise derive it from the company industry so
    # newly-enriched leads auto-classify (mirror of core.fn_classify_industry).
    lead_universe = (payload.get("lead_universe") or "").strip()
    if not lead_universe:
        lead_universe = classify_industry((payload.get("company_industry") or "").strip())
    if lead_universe:
        body["lead_universe"] = lead_universe

    # Phase 2 (full capture): firmographic attributes with no dedicated
    # PersonUpsertRequest field travel via custom_fields (merged into
    # core.person.custom_fields JSONB by the Contacts DB — the only
    # server-persisted catch-all). Each key only when non-empty so existing
    # records are never blanked, and never overwrites a caller-supplied
    # custom_fields entry with an empty value. Hoisted to module scope
    # (``_FIRMOGRAPHIC_CUSTOM_FIELDS``) so the contract is assertable.
    custom: dict[str, Any] = dict(payload.get("custom_fields") or {})
    for cf_key, payload_key in _FIRMOGRAPHIC_CUSTOM_FIELDS:
        raw_value = payload.get(payload_key)
        value = str(raw_value).strip() if raw_value not in (None, "") else ""
        if value:
            custom[cf_key] = value
    if custom:
        body["custom_fields"] = custom

    # Optional provenance override (contacts-api 2026-08-27): populates
    # core.source_row.source_name so the by-domain source=/exclude_source=
    # lookup filters can match this cohort (e.g. 'website_scrape'). Omitted
    # when the caller doesn't set it — the API defaults to 'api_upsert'.
    payload_source = (payload.get("source_name") or "").strip()
    if payload_source:
        body["source_name"] = payload_source

    # Job lineage (kept compact)
    lineage: dict[str, Any] = {}
    if job_id:
        lineage["job_id"] = job_id
    if row_index is not None:
        lineage["row_index"] = row_index
    if lineage:
        body["provider_metadata"] = json.dumps(lineage)

    return await _do_upsert(
        client, body, payload, job_id=job_id, row_index=row_index, kind="person"
    )


async def _write_company_payload(
    client: httpx.AsyncClient,
    payload: dict[str, Any],
    job_id: Optional[str],
    row_index: Optional[int],
) -> WriteStatus:
    """Write company-level email (company_email). Never merged into dm_email."""
    company_email = (payload.get("company_email") or "").strip().lower()
    if not _is_meaningful_email(company_email):
        return WriteStatus.NO_DATA

    # If a person email is also present, the company_email lives on the
    # company record. The Contacts API exposes /v1/persons/upsert which
    # auto-creates/updates the company for the domain — we use a placeholder
    # email of the company itself so the person record is NOT created with
    # the company email. We send the real company_email via the domain field
    # and the placeholder so the API routes it to the company.
    domain = _normalize_domain(
        payload.get("normalized_domain")
        or payload.get("domain")
        or payload.get("website")
        or _domain_from_email(company_email)
    )

    if not domain:
        return WriteStatus.NO_DATA

    # Use the company email itself as the upsert key. The Contacts API
    # treats this as a record for the company contact.
    body: dict[str, Any] = {
        "email": company_email,
        "domain": domain,
        "company_domain": domain,
        "is_company_email": True,
    }

    # We do NOT set full_name on a company record — the placeholder
    # is the company name (or "Contact"). The Contacts DB does the right
    # thing: it creates/updates a *person placeholder* for this email and
    # links it to the company. The placeholder is harmless because the
    # real person already has a record under their dm_email.
    company_name = (payload.get("company_name") or "").strip()
    if company_name:
        body["full_name"] = company_name
        body["company_name"] = company_name
    else:
        body["full_name"] = "Contact"
    company_linkedin = (payload.get("company_linkedin_url") or "").strip()
    if company_linkedin:
        body["company_linkedin_url"] = company_linkedin

    ce_source = (payload.get("company_email_source") or "").strip()
    if ce_source:
        body["email_source"] = ce_source
    ce_verified = (payload.get("company_email_verified") or "").strip().lower()
    if ce_verified in ("yes", "no", "unknown"):
        body["email_verified"] = ce_verified
    ce_type = (payload.get("company_email_type") or "").strip().lower()
    if ce_type:
        # Same normalization as the person-email path: Contacts DB rejects
        # values outside its enum (e.g. 'generic'). Default invalid → 'work'.
        _ALLOWED_EMAIL_TYPES = frozenset({"work", "personal", "other"})
        body["email_type"] = ce_type if ce_type in _ALLOWED_EMAIL_TYPES else "work"
    else:
        # email_type is REQUIRED by Contacts DB PersonUpsert. When
        # company_email_type isn't set, try the generic email_type field
        # (some upstream payloads use this key), then default to 'work'.
        # Without this, the upsert is rejected with HTTP 400 (bug observed
        # 2026-07-13: 1,457 company-email outbox rows stuck failed).
        _ALLOWED_EMAIL_TYPES = frozenset({"work", "personal", "other"})
        fallback_type = (payload.get("email_type") or "").strip().lower()
        body["email_type"] = fallback_type if fallback_type in _ALLOWED_EMAIL_TYPES else "work"
    ce_source_path = (payload.get("company_email_source_path") or payload.get("source_path") or "").strip()
    if ce_source_path:
        body["source_path"] = ce_source_path

    # Firmographics the PersonUpsertRequest has no first-class field for
    # travel via custom_fields (merged into the record server-side — same
    # catch-all the person path uses ~L569-576). Without this the entire
    # gmaps/phone/location enrichment silently never reached the DB
    # (found in review 2026-08-26).
    custom: dict[str, Any] = dict(payload.get("custom_fields") or {})
    if custom:
        body["custom_fields"] = custom

    # Optional provenance override (contacts-api 2026-08-27) — see the
    # person-path comment above; same semantics for company payloads.
    payload_source = (payload.get("source_name") or "").strip()
    if payload_source:
        body["source_name"] = payload_source

    lineage: dict[str, Any] = {
        "kind": "company_email",
    }
    if job_id:
        lineage["job_id"] = job_id
    if row_index is not None:
        lineage["row_index"] = row_index
    body["provider_metadata"] = json.dumps(lineage)

    return await _do_upsert(
        client, body, payload, job_id=job_id, row_index=row_index, kind="company"
    )


# ---------------------------------------------------------------------------
# Core upsert
# ---------------------------------------------------------------------------

async def _do_upsert(
    client: httpx.AsyncClient,
    body: dict[str, Any],
    original_payload: dict[str, Any],
    *,
    job_id: Optional[str],
    row_index: Optional[int],
    kind: str,
) -> WriteStatus:
    """Call the Contacts API and handle retry/outbox/raise semantics."""
    url = f"{_contacts_client._base_url()}/v1/persons/upsert"
    headers = _contacts_client._headers()

    # Respect the global rate limiter (75 RPS)
    await _contacts_client._acquire_upsert_rate_limit()

    try:
        resp = await client.post(url, headers=headers, json=body, timeout=30.0)
    except Exception as exc:
        # Network failure → outbox
        logger.warning("Contacts DB upsert network error (%s): %s", kind, exc)
        return await _queue_or_fail(body, original_payload, job_id, row_index, str(exc))

    if 200 <= resp.status_code < 300:
        # Heuristic: if response body looks like {"created": true, ...} it's
        # an insert; otherwise treat as update.
        try:
            data = resp.json()
        except Exception:
            data = {}
        if isinstance(data, dict) and data.get("created") in (True, "true"):
            return WriteStatus.INSERTED
        if isinstance(data, dict) and data.get("updated") in (True, "true"):
            return WriteStatus.UPDATED
        return WriteStatus.SYNCED

    if resp.status_code in (400, 404, 422):
        # Check if this is a duplicate constraint violation
        if _is_duplicate_response(resp.text):
            logger.info(
                "Contact already exists in DB, skipping upsert (%s) row=%d",
                kind, row_index,
            )
            return WriteStatus.SKIPPED
        # Bad data (non-duplicate) — do not retry, do not outbox
        logger.warning(
            "Contacts DB upsert permanent failure (%s) status=%d body=%s",
            kind, resp.status_code, resp.text[:200],
        )
        return WriteStatus.FAILED

    # 429, 5xx → outbox eligible
    logger.warning(
        "Contacts DB upsert transient failure (%s) status=%d body=%s",
        kind, resp.status_code, resp.text[:200],
    )
    return await _queue_or_fail(
        body, original_payload, job_id, row_index,
        f"http {resp.status_code}: {resp.text[:200]}",
    )


# ---------------------------------------------------------------------------
# Outbox
# ---------------------------------------------------------------------------

class LoudFailure(RuntimeError):
    """Raised when CONTACTS_WRITEBACK_REQUIRED is set and outbox also fails."""
    pass


async def _queue_or_fail(
    body: dict[str, Any],
    original_payload: dict[str, Any],
    job_id: Optional[str],
    row_index: Optional[int],
    error: str,
) -> WriteStatus:
    """Queue the failed payload in the outbox, or fail loudly if required."""
    if not CONTACTS_WRITEBACK_ALLOW_OUTBOX:
        if CONTACTS_WRITEBACK_REQUIRED:
            raise LoudFailure(
                f"Contacts DB write failed and outbox is disabled: {error}"
            )
        return WriteStatus.FAILED

    try:
        init_outbox_table()
        conn = db.get_db()
        now = _now()
        conn.execute(
            """
            INSERT INTO contacts_write_outbox
                (job_id, row_index, payload_json, status,
                 attempt_count, last_error, next_retry_at, created_at, updated_at)
            VALUES (?, ?, ?, 'pending', 0, ?, strftime('%s','now') + 60, ?, ?)
            """,
            (
                job_id,
                row_index,
                json.dumps(body),
                error,
                now,
                now,
            ),
        )
        conn.commit()
        return WriteStatus.QUEUED
    except Exception as outbox_exc:
        logger.error("Outbox insert also failed: %s", outbox_exc)
        if CONTACTS_WRITEBACK_REQUIRED:
            raise LoudFailure(
                f"Contacts DB write failed and outbox insert failed too: "
                f"original={error} outbox={outbox_exc}"
            ) from outbox_exc
        return WriteStatus.FAILED


# ---------------------------------------------------------------------------
# Outbox retry task
# ---------------------------------------------------------------------------

async def retry_outbox(
    *,
    batch_size: int = 50,
    max_attempts: int = 5,
    backoff_base_seconds: int = 60,
) -> WriteResult:
    """
    Drain the outbox. Picks pending rows whose next_retry_at has passed,
    attempts the upsert, and either deletes (success), updates next_retry_at
    (transient), or marks failed (max attempts reached).
    """
    init_outbox_table()
    conn = db.get_db()

    rows = conn.execute(
        """
        SELECT id, job_id, row_index, payload_json, attempt_count
        FROM contacts_write_outbox
        WHERE status = 'pending' AND next_retry_at <= strftime('%s','now')
        ORDER BY next_retry_at ASC
        LIMIT ?
        """,
        (batch_size,),
    ).fetchall()

    result = WriteResult()
    if not rows:
        return result

    async with httpx.AsyncClient(timeout=30.0) as client:
        for row in rows:
            try:
                body = json.loads(row["payload_json"])
            except Exception as exc:
                conn.execute(
                    "UPDATE contacts_write_outbox SET status='failed', "
                    "last_error=?, updated_at=? WHERE id=?",
                    (f"payload parse error: {exc}", _now(), row["id"]),
                )
                conn.commit()
                result.failed += 1
                continue

            url = f"{_contacts_client._base_url()}/v1/persons/upsert"
            headers = _contacts_client._headers()
            await _contacts_client._acquire_upsert_rate_limit()
            try:
                resp = await client.post(url, headers=headers, json=body, timeout=30.0)
            except Exception as exc:
                _bump_retry(conn, row, max_attempts, backoff_base_seconds,
                            f"network: {exc}", result)
                continue

            if 200 <= resp.status_code < 300:
                conn.execute(
                    "DELETE FROM contacts_write_outbox WHERE id=?",
                    (row["id"],),
                )
                conn.commit()
                result.inserted += 1
            elif resp.status_code in (400, 404, 422):
                # Check if this is a duplicate constraint violation
                if _is_duplicate_response(resp.text):
                    logger.debug("Outbox duplicate already exists: job=%s row=%d",
                                 row["job_id"], row["row_index"])
                    # Duplicate = the row is already in Contacts DB. The outbox
                    # can drop this entry entirely — no further retry needed.
                    conn.execute(
                        "DELETE FROM contacts_write_outbox WHERE id=?",
                        (row["id"],),
                    )
                    conn.commit()
                    result.skipped += 1
                    continue
                conn.execute(
                    "UPDATE contacts_write_outbox SET status='failed', "
                    "last_error=?, updated_at=? WHERE id=?",
                    (f"http {resp.status_code}: {resp.text[:200]}", _now(), row["id"]),
                )
                conn.commit()
                result.failed += 1
            else:
                _bump_retry(conn, row, max_attempts, backoff_base_seconds,
                            f"http {resp.status_code}: {resp.text[:200]}", result)

    return result


async def retry_outbox_loop(*, interval_seconds: int = 60) -> None:
    """
    Long-running background task that drains the outbox on a fixed interval.

    Starts automatically on application startup (main.py).  If the service
    stops, the next restart picks up where it left off — the outbox rows
    are durable in SQLite.
    """
    logger.info("Outbox retry loop started (interval=%ds)", interval_seconds)
    while True:
        try:
            await asyncio.sleep(interval_seconds)
            result = await retry_outbox(batch_size=100, max_attempts=5, backoff_base_seconds=60)
            if result.total:
                logger.info("Outbox drain: %s", result.to_dict())
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.warning("Outbox drain iteration failed: %s", exc)
    logger.info("Outbox retry loop stopped")


def _bump_retry(
    conn,
    row,
    max_attempts: int,
    backoff_base_seconds: int,
    error: str,
    result: WriteResult,
) -> None:
    """Update outbox row: bump attempt, schedule next retry, or mark failed."""
    attempts = (row["attempt_count"] or 0) + 1
    if attempts >= max_attempts:
        conn.execute(
            "UPDATE contacts_write_outbox SET status='failed', "
            "attempt_count=?, last_error=?, updated_at=? WHERE id=?",
            (attempts, error, _now(), row["id"]),
        )
        conn.commit()
        result.failed += 1
    else:
        delay = backoff_base_seconds * (2 ** (attempts - 1))
        conn.execute(
            "UPDATE contacts_write_outbox SET attempt_count=?, "
            "last_error=?, next_retry_at=strftime('%s','now') + ?, updated_at=? "
            "WHERE id=?",
            (attempts, error, delay, _now(), row["id"]),
        )
        conn.commit()
        result.queued += 1


# ---------------------------------------------------------------------------
# Bulk helper (used by routes.py to sync a full output CSV)
# ---------------------------------------------------------------------------

async def write_enrichment_result_batch(
    payloads: list[dict[str, Any]],
    *,
    job_id: Optional[str] = None,
) -> WriteResult:
    """
    Write many enrichment results. Each dict should include at least
    'dm_email' or 'company_email' (plus optional 'row_index' for tracking).

    Returns aggregated WriteResult.
    """
    init_outbox_table()
    overall = WriteResult()
    if not payloads:
        return overall

    # Reuse a single client for the batch
    async with httpx.AsyncClient(timeout=30.0) as client:
        for p in payloads:
            try:
                # Same SEG decoration as the single-row path (defensive
                # read via .get; upstream flows may have set it already).
                _apply_seg_fields(p)
                status = await write_enrichment_result(
                    p, client=client, job_id=job_id, row_index=p.get("row_index")
                )
            except LoudFailure as exc:
                # Loud failure: bubble up but keep aggregating partial counts
                overall.failed += 1
                overall.errors.append(str(exc))
                logger.error("Loud failure during batch write: %s", exc)
                if CONTACTS_WRITEBACK_REQUIRED:
                    raise
                continue
            except Exception as exc:
                overall.failed += 1
                overall.errors.append(str(exc))
                logger.error("Unexpected error during batch write: %s", exc)
                continue

            if status == WriteStatus.INSERTED:
                overall.inserted += 1
            elif status == WriteStatus.UPDATED:
                overall.updated += 1
            elif status == WriteStatus.SYNCED:
                # SYNCED = 2xx success but API didn't echo created/updated
                # (e.g. older API versions, or generic contact upsert).
                # Count as a successful sync rather than no_data.
                overall.inserted += 1
            elif status == WriteStatus.SKIPPED:
                overall.skipped += 1
            elif status == WriteStatus.QUEUED:
                overall.queued += 1
            elif status == WriteStatus.FAILED:
                overall.failed += 1
            else:
                overall.no_data += 1
    return overall


# ---------------------------------------------------------------------------
# Self-test (run with `python -m enrichment.contacts_writer`)
# ---------------------------------------------------------------------------

if __name__ == "__main__":  # pragma: no cover
    import sys

    async def _demo() -> None:
        sample = {
            "dm_email": "jane@example.com",
            "dm_full_name": "Jane Doe",
            "dm_first_name": "Jane",
            "dm_last_name": "Doe",
            "dm_title": "CEO",
            "dm_linkedin_url": "https://linkedin.com/in/janedoe",
            "domain": "example.com",
            "company_name": "Example Inc",
            "company_linkedin_url": "https://linkedin.com/company/example",
            "dm_email_source": "blitz",
            "dm_email_verified": "yes",
            "mailtester_code": "200",
            "source_path": "blitz",
        }
        status = await write_enrichment_result(sample, job_id="demo", row_index=0)
        print(f"person write: {status.value}")
        sample2 = dict(sample)
        sample2["dm_email"] = ""
        sample2["company_email"] = "info@example.com"
        sample2["company_email_source"] = "better_enrich_facebook_email"
        sample2["company_email_verified"] = "yes"
        sample2["company_email_type"] = "generic"
        status2 = await write_enrichment_result(sample2, job_id="demo", row_index=1)
        print(f"company write: {status2.value}")

    try:
        asyncio.run(_demo())
    except Exception as e:
        print(f"demo failed: {e}", file=sys.stderr)
        sys.exit(1)
