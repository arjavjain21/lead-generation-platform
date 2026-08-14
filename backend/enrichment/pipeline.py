"""
Enrichment pipeline orchestrator.

Per-domain workflow:
  1. domain → company LinkedIn URL (Contacts DB PRIMARY, Blitz fallback)
  2. company → decision makers (Contacts DB PRIMARY, Blitz fallback)
  3. For each person (_resolve_email_for_person):
       a. Contacts DB by name + domain
       b. Contacts DB by LinkedIn URL
       c. Blitz by name + domain
       d. Blitz by LinkedIn URL
       e. BetterEnrich person email
  4. If no decision makers found: BetterEnrich company email (final fallback)
  5. If no email from above: return not_found

Supports incremental CSV writes for partial downloads.
"""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Callable, Optional

import httpx

from . import blitz_client
from . import contacts_client
from . import better_enrich_client
from . import prospeo_client
from . import providers
from . import mailtester_client
from . import wizleads_client
from . import smartprospect_client
from . import getleads_client
from . import identifier_utils
from . import company_fallback

logger = logging.getLogger(__name__)

# Directory for per-job JSONL audit sidecars
AUDIT_SIDECAR_DIR = Path(os.environ.get(
    "ENRICHMENT_AUDIT_DIR",
    "/mnt/disk/lead-generation-platform/jobs/audit",
))

# Maximum size of provider_attempts_json in a CSV row before spilling to sidecar.
# Roughly ~80 attempts of ~30 chars each is ~2.4KB; we use 1KB as the threshold
# to keep CSV rows readable.
AUDIT_JSONL_COMPACT_THRESHOLD_BYTES = 1024


# ---------------------------------------------------------------------------
# Provider Error Structure
# ---------------------------------------------------------------------------

class _ProviderError:
    """
    A provider error that evaluates as False but carries error details.

    This allows provider errors to be surfaced to the API response while
    maintaining backward compatibility with the existing cascade behavior
    (which treats falsy values as "not found" and continues to the next provider).

    Usage:
        error = _ProviderError(
            provider="better_enrich",
            method="find_work_email_v3",
            error_type="insufficient_credits",
            message="BetterEnrich: Insufficient credits. Please top up."
        )
        # error is falsy: if not error: # → True, continues cascade
        # But error details can be extracted: error.to_dict()
    """

    def __init__(
        self,
        provider: str,
        method: str,
        error_type: str,
        message: str,
    ):
        self.provider = provider
        self.method = method
        self.error_type = error_type
        self.message = message

    def __bool__(self):
        """Evaluate as False so existing cascade logic continues."""
        return False

    def to_dict(self) -> dict[str, str]:
        """Convert to dict for API response serialization."""
        return {
            "provider": self.provider,
            "method": self.method,
            "error_type": self.error_type,
            "message": self.message,
        }


def _is_provider_error(result: Any) -> bool:
    """Check if a result is a provider error object."""
    return isinstance(result, _ProviderError)


def _extract_provider_error(result: Any) -> Optional[dict[str, str]]:
    """
    If result is a _ProviderError, return the error dict, else None.

    This is the primary way for callers to extract error details from
    provider return values while maintaining backward compatibility.
    """
    if _is_provider_error(result):
        return result.to_dict()
    return None


def _classify_http_error(exc: httpx.HTTPStatusError, provider: str, method: str) -> tuple[str, str]:
    """
    Classify an HTTPStatusError into error_type and user-friendly message.

    Returns:
        Tuple of (error_type, message) where error_type is one of:
        - insufficient_credits: 402, or "insufficient" in response
        - authentication_failed: 401, 403
        - rate_limited: 429
        - service_unavailable: 503, 502, 500+
        - unknown: Other errors
    """
    status = exc.response.status_code
    provider_title = provider.replace("_", " ").title()

    # Try to get more details from response body
    error_detail = ""
    try:
        error_data = exc.response.json()
        if isinstance(error_data, dict):
            error_detail = error_data.get("message") or error_data.get("error") or ""
    except:
        pass

    # Check response body for "insufficient credits" even on non-402 status codes
    detail_lower = error_detail.lower()

    if status == 402 or "insufficient" in detail_lower or "no credits" in detail_lower:
        return "insufficient_credits", f"{provider_title}: Insufficient credits. Please top up to continue."
    elif status == 401 or status == 403:
        return "authentication_failed", f"{provider_title}: Authentication failed. Check API key."
    elif status == 429:
        return "rate_limited", f"{provider_title}: Rate limited. Please try again later."
    elif status == 503:
        return "service_unavailable", f"{provider_title}: Service temporarily unavailable."
    elif status >= 500:
        return "service_unavailable", f"{provider_title}: Server error. Please try again."
    else:
        return "unknown", f"{provider_title}: An error occurred. Status: {status}"


def _build_provider_attempt(
    *,
    job_id: str,
    row_index: int,
    domain: str,
    normalized_linkedin_url: str,
    provider: str,
    method: str,
    input_type_used: str,
    called: bool,
    skipped_reason: str = "",
    status: str = "",
    email_found: bool = False,
    error_type: str = "",
    latency_ms: int = 0,
) -> dict[str, Any]:
    """Build a structured provider_attempt record.

    Each record is a dict with stable keys so callers can JSON-serialize it
    and downstream consumers (CSV columns, JSONL sidecars, API responses) can
    rely on a uniform shape.
    """
    return {
        "job_id": job_id or "",
        "row_index": int(row_index) if row_index is not None else -1,
        "domain": domain or "",
        "normalized_linkedin_url": normalized_linkedin_url or "",
        "provider": provider or "",
        "method": method or "",
        "input_type_used": input_type_used or "",
        "called": bool(called),
        "skipped_reason": skipped_reason or "",
        "status": status or "",
        "email_found": bool(email_found),
        "error_type": error_type or "",
        "latency_ms": int(latency_ms) if latency_ms else 0,
    }


def _log_provider_attempt(attempt: dict[str, Any]) -> None:
    """Emit a structured INFO log for a single provider attempt.

    Uses `extra=` so each key becomes its own log field for downstream log
    aggregators (Loki, ELK, etc.). No secrets, emails, or raw LinkedIn URLs
    beyond the already-normalized form are included.
    """
    if not attempt:
        return
    # Project to a flat dict for the log record. Keep keys short for grep-ability.
    payload = {
        "evt": "provider_attempt",
        "job_id": attempt.get("job_id", ""),
        "row_index": attempt.get("row_index", -1),
        "domain": attempt.get("domain", ""),
        "normalized_linkedin_url": attempt.get("normalized_linkedin_url", ""),
        "provider": attempt.get("provider", ""),
        "method": attempt.get("method", ""),
        "input_type_used": attempt.get("input_type_used", ""),
        "called": attempt.get("called", False),
        "skipped_reason": attempt.get("skipped_reason", "") or None,
        "status": attempt.get("status", "") or None,
        "email_found": attempt.get("email_found", False),
        "error_type": attempt.get("error_type", "") or None,
        "latency_ms": attempt.get("latency_ms", 0),
    }
    # Drop None values so the log line is compact.
    payload = {k: v for k, v in payload.items() if v is not None}
    logger.info("provider_attempt %s", json.dumps(payload, sort_keys=True))


def _compact_attempts_summary(attempts: list[dict[str, Any]]) -> dict[str, Any]:
    """Build a compact summary of attempts for CSV embedding.

    The summary is small (~200 bytes max) so it fits in a CSV cell even when
    many providers were tried. Full per-attempt detail is written to the
    JSONL sidecar.
    """
    providers_called: list[str] = []
    providers_skipped: list[dict[str, str]] = []
    for a in attempts or []:
        provider = a.get("provider", "")
        method = a.get("method", "")
        if a.get("called"):
            providers_called.append(provider)
        else:
            providers_skipped.append({
                "provider": provider,
                "method": method,
                "skipped_reason": a.get("skipped_reason", ""),
            })
    return {
        "providers_called": providers_called,
        "providers_skipped": providers_skipped,
    }


# Valid provider values for force_provider / selected_providers parameter
VALID_PROVIDERS = frozenset({"contacts_db", "blitz", "getleads", "smartprospect", "wizleads", "better_enrich"})


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
      1. Prospeo defensive kill-switch (env flag)
      2. Global ENABLED_PROVIDERS kill switch (beats everything below)
      3. force_provider (if set) — only that provider passes
      4. selected_providers (if set) — only those providers pass; contacts_db
         is always allowed
    """
    # Defensive kill-switch: Prospeo is currently disabled end-to-end.
    # The cascade never invokes prospeo_client today, but this guard ensures
    # any future code path that tries to call Prospeo will be skipped when
    # the ENABLE_PROSPEO env flag is "false" (default). Set ENABLE_PROSPEO=true
    # in backend/.env AND flip ENABLED_PROVIDERS["prospeo"] to re-enable.
    if provider == "prospeo" and os.environ.get("ENABLE_PROSPEO", "false").lower() != "true":
        logger.debug("_should_skip_provider: prospeo skipped (ENABLE_PROSPEO=false)")
        return True

    # Global enablement check — beats force_provider and selected_providers.
    # If a provider is globally disabled, no per-request override re-enables it.
    if not providers.is_provider_enabled(provider):
        logger.debug("_should_skip_provider: %s disabled in ENABLED_PROVIDERS", provider)
        return True

    # force_provider takes precedence over selected_providers - if set, only
    # use that provider.
    if force_provider:
        result = provider != force_provider
        logger.debug("_should_skip_provider(provider=%s, force_provider=%s) = %s", provider, force_provider, result)
        return result

    # selected_providers is an allowlist. contacts_db is always allowed
    # (mandatory first step), matching list_builder.py convention.
    if selected_providers is not None:
        if provider == "contacts_db":
            return False
        return provider not in selected_providers

    return False


def _normalize_source(source: str) -> str:
    """Map raw source value to provider group.

    Args:
        source: Raw source string (e.g., "blitz_email", "contacts_db_email")

    Returns:
        Provider group (e.g., "blitz", "contacts_db") or original source if unknown
    """
    if source.startswith("contacts_db"):
        return "contacts_db"
    elif source.startswith("blitz"):
        return "blitz"
    elif source.startswith("getleads"):
        return "getleads"
    elif source.startswith("smartprospect"):
        return "smartprospect"
    elif source.startswith("better_enrich"):
        return "better_enrich"
    elif source.startswith("wizleads"):
        return "wizleads"
    elif source.startswith("prospeo"):
        return "prospeo"  # Historical: old rows may have prospeo as dm_email_source
    return source


# Max concurrent Blitz calls to avoid hammering the API
# DOMAIN_CONCURRENCY = 25 (standardized with list_builder.py for consistency)
DOMAIN_CONCURRENCY = 25  # Was 5, increased to match list_builder.py
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
SOURCE_BETTER_ENRICH_FACEBOOK = "better_enrich_facebook_email"  # Page email from BetterEnrich Facebook lookup
SOURCE_BETTER_ENRICH_COMPANY_V2 = "better_enrich_company_email"  # Company email from BetterEnrich find-company-email (fallback tier)
SOURCE_WIZLEADS = "wizleads_email"                        # Person email from WizLeads
SOURCE_SMARTPROSPECT = "smartprospect_email"              # Person email from SmartLead Find Emails (self-verifying)
SOURCE_GETLEADS = "getleads_email"                        # Person email from GetLeads (app.getleads.io)

# Routing reasons (no_email_reason values)
# The full standard enum (per LOOP.md acceptance test 2/8):
#   missing_required_input, linkedin_url_not_passed_to_pipeline,
#   linkedin_parse_failed, provider_disabled, force_provider_blocked_provider,
#   provider_rate_limited, provider_circuit_open, provider_auth_failed,
#   provider_timeout, provider_5xx, provider_schema_parse_failed,
#   provider_called_no_match, email_found_but_invalid, verification_unavailable,
#   all_providers_called_no_email
NO_EMAIL_REASON_MISSING_REQUIRED_INPUT = "missing_required_input"
NO_EMAIL_REASON_LINKEDIN_URL_NOT_PASSED = "linkedin_url_not_passed_to_pipeline"
NO_EMAIL_REASON_LINKEDIN_PARSE_FAILED = "linkedin_parse_failed"
NO_EMAIL_REASON_PROVIDER_DISABLED = "provider_disabled"
NO_EMAIL_REASON_FORCE_PROVIDER_BLOCKED = "force_provider_blocked_provider"
NO_EMAIL_REASON_PROVIDER_RATE_LIMITED = "provider_rate_limited"
NO_EMAIL_REASON_PROVIDER_CIRCUIT_OPEN = "provider_circuit_open"
NO_EMAIL_REASON_PROVIDER_AUTH_FAILED = "provider_auth_failed"
NO_EMAIL_REASON_PROVIDER_TIMEOUT = "provider_timeout"
NO_EMAIL_REASON_PROVIDER_5XX = "provider_5xx"
NO_EMAIL_REASON_PROVIDER_SCHEMA_PARSE_FAILED = "provider_schema_parse_failed"
NO_EMAIL_REASON_PROVIDER_CALLED_NO_MATCH = "provider_called_no_match"
NO_EMAIL_REASON_EMAIL_FOUND_BUT_INVALID = "email_found_but_invalid"
NO_EMAIL_REASON_VERIFICATION_UNAVAILABLE = "verification_unavailable"
NO_EMAIL_REASON_ALL_PROVIDERS_CALLED_NO_EMAIL = "all_providers_called_no_email"

# Company / page-level fallback tier (BetterEnrich).
NO_EMAIL_REASON_COMPANY_EMAIL_FOUND_BUT_NOT_ALLOWED = "company_email_found_but_not_allowed"
NO_EMAIL_REASON_GENERIC_COMPANY_EMAIL_REJECTED = "generic_company_email_rejected"
NO_EMAIL_REASON_FACEBOOK_PAGE_MISSING = "facebook_page_missing"
NO_EMAIL_REASON_COMPANY_EMAIL_FALLBACK_DISABLED = "company_email_fallback_disabled"

# Historical aliases (kept for back-compat with prior tests)
NO_EMAIL_REASON_PHONE_REVERSE_UNAVAILABLE = "phone_reverse_unavailable"
NO_EMAIL_REASON_FORCED_PROVIDER_CANNOT_USE_INPUT = "forced_provider_cannot_use_input"
NO_EMAIL_REASON_NO_IDENTIFIERS = "no_identifiers"
NO_EMAIL_REASON_DOMAIN_ONLY_NO_CONTACTS = "domain_only_no_contacts"
NO_EMAIL_REASON_DOMAIN_ONLY_NO_LINKEDIN = "domain_only_no_linkedin"

# Route-identifier values used to describe how the strongest identifier was reached.
# These are appended to source_path in order.
ROUTE_IDENTIFIER_LINKEDIN = "linkedin"
ROUTE_IDENTIFIER_PHONE = "phone"
ROUTE_IDENTIFIER_PHONE_REVERSE = "phone_reverse"
ROUTE_IDENTIFIER_NAME_DOMAIN = "name_domain"
ROUTE_IDENTIFIER_DOMAIN = "domain"

# Provider tokens used in source_path (must match how a provider "found" email).
ROUTE_PROVIDER_CONTACTS_DB = "contacts_db"
ROUTE_PROVIDER_BLITZ = "blitz"
ROUTE_PROVIDER_GETLEADS = "getleads"
ROUTE_PROVIDER_SMARTPROSPECT = "smartprospect"
ROUTE_PROVIDER_WIZLEADS = "wizleads"
ROUTE_PROVIDER_BETTER_ENRICH = "better_enrich"

# Provider methods used in source_path.
ROUTE_METHOD_PERSON_BY_LINKEDIN = "person_by_linkedin"
ROUTE_METHOD_PERSON_ENRICH_BY_LINKEDIN = "person_enrich_by_linkedin"
ROUTE_METHOD_FIND_WORK_EMAIL = "find_work_email"
ROUTE_METHOD_PERSON_BY_NAME_DOMAIN = "person_by_name_and_domain"
ROUTE_METHOD_PERSON_ENRICH = "person_enrich"
ROUTE_METHOD_FIND_EMAIL = "find_email"
ROUTE_METHOD_FIND_EMAIL_SMARTPROSPECT = "find_email_smartprospect"
ROUTE_METHOD_FIND_EMAIL_GETLEADS = "find_email_getleads"
ROUTE_METHOD_FIND_EMAIL_LINKEDIN_GETLEADS = "find_email_linkedin_getleads"
ROUTE_METHOD_FIND_WORK_EMAIL_V3 = "find_work_email_v3"
ROUTE_METHOD_PHONE_REVERSE_LOOKUP = "phone_reverse_lookup"

ENRICHED_COLUMNS = [
    "company_linkedin_url",
    "company_name",
    "company_industry",
    "company_employee_count",
    "company_revenue",
    "dm_first_name",
    "dm_last_name",
    "dm_full_name",
    "dm_title",
    "dm_job_level",
    "dm_job_function",
    "dm_linkedin_url",
    "dm_linkedin_connections",
    "dm_email",
    "dm_email_source",
    "dm_email_verified",
    "dm_email_last_verified_at",
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
    # Routing diagnostics
    "source_path",
    "provider_attempts",
    "provider_attempts_json",
    "providers_called",
    "providers_skipped",
    "no_email_reason",
    "final_email_status",
    "final_email_verification_source",
    # Provider errors (user-facing error information)
    # TODO(Phase 3): populate from cascade error capture. Currently the
    # column exists for schema parity with list_builder.py but is left
    # empty by all row builders below.
    "provider_errors",
    # Company / page-level fallback outputs (BetterEnrich).
    # These are populated only when the person-level waterfall returns no
    # decision-maker email. They MUST never be written into dm_email.
    "company_email",
    "company_email_source",
    "company_email_verified",
    "company_email_type",
    "company_email_source_path",
    # The "best" email for the row, subject to the final-email policy:
    # person email first; only the company/page email if
    # allow_company_email_as_final is set AND (non-generic OR
    # allow_generic_company_email is set).
    "final_email",
    "final_email_level",
    "final_email_source_path",
]


def _provider_label(method: str) -> str:
    """Map a route method to its provider group (used in source_path)."""
    if method in (
        ROUTE_METHOD_PERSON_BY_LINKEDIN,
        ROUTE_METHOD_PERSON_BY_NAME_DOMAIN,
    ):
        return ROUTE_PROVIDER_CONTACTS_DB
    if method in (
        ROUTE_METHOD_PERSON_ENRICH_BY_LINKEDIN,
        ROUTE_METHOD_FIND_WORK_EMAIL,
        ROUTE_METHOD_PERSON_ENRICH,
    ):
        return ROUTE_PROVIDER_BLITZ
    if method == ROUTE_METHOD_FIND_EMAIL:
        return ROUTE_PROVIDER_WIZLEADS
    if method in (ROUTE_METHOD_FIND_EMAIL_GETLEADS, ROUTE_METHOD_FIND_EMAIL_LINKEDIN_GETLEADS):
        return ROUTE_PROVIDER_GETLEADS
    if method == ROUTE_METHOD_FIND_EMAIL_SMARTPROSPECT:
        return ROUTE_PROVIDER_SMARTPROSPECT
    if method == ROUTE_METHOD_FIND_WORK_EMAIL_V3:
        return ROUTE_PROVIDER_BETTER_ENRICH
    if method == ROUTE_METHOD_PHONE_REVERSE_LOOKUP:
        return ROUTE_PROVIDER_BLITZ  # phone reverse is a Blitz concept
    return method


def _can_provider_use_method(method: str, inputs: dict[str, str]) -> bool:
    """Capability gate: does the given inputs dict satisfy the method's required fields?

    Args:
        method: a ROUTE_METHOD_* value
        inputs: dict with at least `linkedin_url`, `phone`, `full_name`, `domain`,
                `first_name`, `last_name`.
    """
    li = inputs.get("linkedin_url", "")
    phone = inputs.get("phone", "")
    fn = inputs.get("full_name", "")
    dom = inputs.get("domain", "")
    first = inputs.get("first_name", "")
    last = inputs.get("last_name", "")

    if method == ROUTE_METHOD_PERSON_BY_LINKEDIN:
        return bool(li)
    if method == ROUTE_METHOD_PERSON_ENRICH_BY_LINKEDIN:
        return bool(li)
    if method == ROUTE_METHOD_FIND_WORK_EMAIL:
        return bool(li)
    if method == ROUTE_METHOD_PERSON_BY_NAME_DOMAIN:
        return bool(fn) and bool(dom)
    if method == ROUTE_METHOD_PERSON_ENRICH:
        return bool(fn) and bool(dom)
    if method == ROUTE_METHOD_FIND_EMAIL:
        return bool(first) and bool(last) and bool(dom)
    if method == ROUTE_METHOD_FIND_EMAIL_GETLEADS:
        return bool(first) and bool(last) and bool(dom)
    if method == ROUTE_METHOD_FIND_EMAIL_LINKEDIN_GETLEADS:
        return bool(li)
    if method == ROUTE_METHOD_FIND_EMAIL_SMARTPROSPECT:
        return bool(first) and bool(last) and bool(dom)
    if method == ROUTE_METHOD_FIND_WORK_EMAIL_V3:
        return bool(fn) and bool(dom)
    if method == ROUTE_METHOD_PHONE_REVERSE_LOOKUP:
        return bool(phone)
    return False


def _method_is_paid(method: str) -> bool:
    """Return True if the method calls a paid provider family."""
    return _provider_label(method) in (ROUTE_PROVIDER_BLITZ, ROUTE_PROVIDER_BETTER_ENRICH, ROUTE_PROVIDER_WIZLEADS, ROUTE_PROVIDER_SMARTPROSPECT)


def _method_is_free(method: str) -> bool:
    return _provider_label(method) == ROUTE_PROVIDER_CONTACTS_DB


def route_enrichment(
    *,
    linkedin_url: str = "",
    phone: str = "",
    full_name: str = "",
    first_name: str = "",
    last_name: str = "",
    domain: str = "",
    company_name: str = "",
    force_provider: Optional[str] = None,
    selected_providers: Optional[list[str]] = None,
) -> dict[str, Any]:
    """Decide provider order based on available identifiers.

    Pure routing function. No I/O. Returns:

        {
            "mode": "linkedin_only" | "phone_then_linkedin"
                   | "name_domain" | "domain_only" | "invalid",
            "steps": [
                {"identifier": "linkedin" | "phone" | "name_domain" | "domain",
                 "method": "<ROUTE_METHOD_*>",
                 "provider": "<ROUTE_PROVIDER_*>"},
                ...
            ],
            "no_email_reason": "" | "linkedin_parse_failed" | ...,
        }

    Routing rules (in priority order):
      1. If linkedin_url is set and not a parseable LinkedIn URL, return
         `mode="invalid"`, `no_email_reason="linkedin_parse_failed"`.
      2. If linkedin_url is set: produce a LinkedIn-first cascade
         (contacts_db -> blitz -> better_enrich). Domain-only fallbacks
         may run after if name+domain are also present.
      3. Else if phone is set: produce a phone -> linkedin cascade.
      4. Else if full_name and domain: produce a name+domain cascade
         (contacts_db -> blitz -> smartprospect -> wizleads -> better_enrich).
      5. Else if domain: produce a domain-only cascade.
      6. Else: return `mode="invalid"`, `no_email_reason="no_identifiers"`.

    force_provider semantics:
      * None: emit all steps that are not blocked by the capability gate.
      * "blitz": keep only blitz steps; if no blitz step can run (e.g. only
        linkedin_url provided but blitz is in the cascade), keep the steps
        that are still valid AND drop free steps. If no blitz step can run
        on the input, return no_email_reason="forced_provider_cannot_use_input".
      * "contacts_db": keep only contacts_db steps; if none can run, return
        no_email_reason="forced_provider_cannot_use_input".
      * "smartprospect": keep only smartprospect steps.
      * "wizleads": keep only wizleads steps.
      * "better_enrich": keep only better_enrich steps.

    selected_providers semantics (mutually exclusive with force_provider):
      * None: no allowlist filtering (all enabled providers considered).
      * ["contacts_db", "smartprospect"]: keep only steps whose provider is
        in the set. contacts_db steps are ALWAYS kept even if not listed
        (mandatory first step). If filtering removes every step, return
        no_email_reason="forced_provider_cannot_use_input" so callers see
        an explicit signal rather than an empty cascade.

    **first_name / last_name auto-derivation:** when the caller supplies
    ``full_name`` but not ``first_name``/``last_name`` (the common case for
    enhanced-mode requests), the function splits ``full_name`` on its first
    space and fills in the missing pieces. This matters because smartprospect
    and WizLeads gate on ``first_name + last_name`` presence — without
    auto-derivation, those providers would be silently dropped from the
    cascade whenever the caller only sends ``full_name``. Single-word
    ``full_name`` values (no space) leave ``last_name`` empty, so the gate
    still correctly excludes those providers (they need both names).
    Explicit non-empty ``first_name``/``last_name`` values are NEVER
    overridden — the derivation only fills empty slots.
    """
    # Auto-derive first_name / last_name from full_name when not provided.
    # See docstring above for rationale. Matches the splitting convention
    # used in the legacy cascade (_resolve_email_for_person Step 5/6).
    if full_name:
        parts = full_name.strip().split(" ", 1)
        if not first_name and parts:
            first_name = parts[0]
        if not last_name and len(parts) > 1:
            last_name = parts[1]

    inputs = {
        "linkedin_url": linkedin_url or "",
        "phone": phone or "",
        "full_name": full_name or "",
        "first_name": first_name or "",
        "last_name": last_name or "",
        "domain": domain or "",
        "company_name": company_name or "",
    }

    has_li = bool(inputs["linkedin_url"])
    has_phone = bool(inputs["phone"])
    has_name_domain = bool(inputs["full_name"]) and bool(inputs["domain"])
    has_domain = bool(inputs["domain"])

    # Malformed-LinkedIn gate. We accept the value iff identifier_utils sees
    # a valid linkedin.com host in it. We use the parser directly so this
    # function remains pure (no I/O, no DB).
    if has_li:
        from . import identifier_utils as _iu
        normalized_li = _iu.normalize_linkedin_url(inputs["linkedin_url"])
        if not normalized_li:
            return {
                "mode": "invalid",
                "steps": [],
                "no_email_reason": NO_EMAIL_REASON_LINKEDIN_PARSE_FAILED,
            }
        # Use the normalized form for downstream calls.
        inputs["linkedin_url"] = normalized_li

    raw_steps: list[dict[str, str]] = []

    if has_li:
        # LinkedIn-first cascade. Domain fallbacks only run if name+domain present.
        raw_steps.extend([
            {
                "identifier": ROUTE_IDENTIFIER_LINKEDIN,
                "method": ROUTE_METHOD_PERSON_BY_LINKEDIN,
                "provider": ROUTE_PROVIDER_CONTACTS_DB,
            },
            {
                "identifier": ROUTE_IDENTIFIER_LINKEDIN,
                "method": ROUTE_METHOD_PERSON_ENRICH_BY_LINKEDIN,
                "provider": ROUTE_PROVIDER_BLITZ,
            },
            {
                "identifier": ROUTE_IDENTIFIER_LINKEDIN,
                "method": ROUTE_METHOD_FIND_WORK_EMAIL,
                "provider": ROUTE_PROVIDER_BLITZ,
            },
            # GetLeads from-linkedin fallback AFTER both Blitz steps miss
            # (only needs the LinkedIn URL). SmartProspect/WizLeads stay out
            # of the LinkedIn arm — they are name+domain gated.
            {
                "identifier": ROUTE_IDENTIFIER_LINKEDIN,
                "method": ROUTE_METHOD_FIND_EMAIL_LINKEDIN_GETLEADS,
                "provider": ROUTE_PROVIDER_GETLEADS,
            },
        ])
        if has_name_domain:
            raw_steps.extend([
                {
                    "identifier": ROUTE_IDENTIFIER_NAME_DOMAIN,
                    "method": ROUTE_METHOD_PERSON_BY_NAME_DOMAIN,
                    "provider": ROUTE_PROVIDER_CONTACTS_DB,
                },
                {
                    "identifier": ROUTE_IDENTIFIER_NAME_DOMAIN,
                    "method": ROUTE_METHOD_PERSON_ENRICH,
                    "provider": ROUTE_PROVIDER_BLITZ,
                },
            ])
            if first_name and last_name:
                # getleads + smartprospect inserted between Blitz and BetterEnrich
                # in the LinkedIn-first name+domain fallback arm too, mirroring the
                # name_domain cascade ordering (blitz -> getleads -> smartprospect).
                raw_steps.append({
                    "identifier": ROUTE_IDENTIFIER_NAME_DOMAIN,
                    "method": ROUTE_METHOD_FIND_EMAIL_GETLEADS,
                    "provider": ROUTE_PROVIDER_GETLEADS,
                })
                raw_steps.append({
                    "identifier": ROUTE_IDENTIFIER_NAME_DOMAIN,
                    "method": ROUTE_METHOD_FIND_EMAIL_SMARTPROSPECT,
                    "provider": ROUTE_PROVIDER_SMARTPROSPECT,
                })
        if has_name_domain:
            raw_steps.append({
                "identifier": ROUTE_IDENTIFIER_NAME_DOMAIN,
                "method": ROUTE_METHOD_FIND_WORK_EMAIL_V3,
                "provider": ROUTE_PROVIDER_BETTER_ENRICH,
            })
        mode = "linkedin_only" if not has_domain else "linkedin_first"
    elif has_phone:
        # Phone -> LinkedIn cascade. The phone reverse step is a stub for now
        # (no provider has a phone reverse endpoint), so the routing function
        # itself returns a clear no_email_reason. Downstream callers can swap
        # in a real reverse lookup without changing the route shape.
        raw_steps.append({
            "identifier": ROUTE_IDENTIFIER_PHONE,
            "method": ROUTE_METHOD_PHONE_REVERSE_LOOKUP,
            "provider": ROUTE_PROVIDER_BLITZ,
        })
        mode = "phone_then_linkedin"
    elif has_name_domain:
        # Name+domain cascade.
        raw_steps.extend([
            {
                "identifier": ROUTE_IDENTIFIER_NAME_DOMAIN,
                "method": ROUTE_METHOD_PERSON_BY_NAME_DOMAIN,
                "provider": ROUTE_PROVIDER_CONTACTS_DB,
            },
            {
                "identifier": ROUTE_IDENTIFIER_NAME_DOMAIN,
                "method": ROUTE_METHOD_PERSON_ENRICH,
                "provider": ROUTE_PROVIDER_BLITZ,
            },
        ])
        if first_name and last_name:
            # getleads runs before SmartProspect — both first+last+domain gated.
            # smartprospect runs before WizLeads — self-verifying, 30 RPS,
            # batch-capable. Same first+last+domain gate as WizLeads.
            raw_steps.append({
                "identifier": ROUTE_IDENTIFIER_NAME_DOMAIN,
                "method": ROUTE_METHOD_FIND_EMAIL_GETLEADS,
                "provider": ROUTE_PROVIDER_GETLEADS,
            })
            raw_steps.append({
                "identifier": ROUTE_IDENTIFIER_NAME_DOMAIN,
                "method": ROUTE_METHOD_FIND_EMAIL_SMARTPROSPECT,
                "provider": ROUTE_PROVIDER_SMARTPROSPECT,
            })
            raw_steps.append({
                "identifier": ROUTE_IDENTIFIER_NAME_DOMAIN,
                "method": ROUTE_METHOD_FIND_EMAIL,
                "provider": ROUTE_PROVIDER_WIZLEADS,
            })
        raw_steps.append({
            "identifier": ROUTE_IDENTIFIER_NAME_DOMAIN,
            "method": ROUTE_METHOD_FIND_WORK_EMAIL_V3,
            "provider": ROUTE_PROVIDER_BETTER_ENRICH,
        })
        mode = "name_domain"
    elif has_domain:
        # Domain-only cascade. The decision-maker cascade is handled
        # separately by `_enrich_domain` (it needs company_linkedin_url),
        # so the route here just notes the mode.
        mode = "domain_only"
        raw_steps = []
    else:
        return {
            "mode": "invalid",
            "steps": [],
            "no_email_reason": NO_EMAIL_REASON_NO_IDENTIFIERS,
        }

    # Apply force_provider.
    if force_provider:
        family_map = {
            "contacts_db": ROUTE_PROVIDER_CONTACTS_DB,
            "blitz": ROUTE_PROVIDER_BLITZ,
            "getleads": ROUTE_PROVIDER_GETLEADS,
            "smartprospect": ROUTE_PROVIDER_SMARTPROSPECT,
            "wizleads": ROUTE_PROVIDER_WIZLEADS,
            "better_enrich": ROUTE_PROVIDER_BETTER_ENRICH,
        }
        forced = family_map.get(force_provider)
        if not forced:
            return {
                "mode": mode,
                "steps": [],
                "no_email_reason": NO_EMAIL_REASON_FORCED_PROVIDER_CANNOT_USE_INPUT,
            }
        filtered = [s for s in raw_steps if s["provider"] == forced]
        if not filtered:
            return {
                "mode": mode,
                "steps": [],
                "no_email_reason": NO_EMAIL_REASON_FORCED_PROVIDER_CANNOT_USE_INPUT,
            }
        raw_steps = filtered

    # Apply selected_providers allowlist. contacts_db steps are always kept
    # (mandatory first step) even when not explicitly listed — mirrors
    # list_builder._should_skip_provider semantics. If filtering removes
    # every step, surface an explicit no_email_reason so the caller sees a
    # clear signal rather than a silent empty cascade.
    if selected_providers is not None:
        allowed = set(selected_providers)
        allowed.add("contacts_db")  # mandatory first step
        filtered = [s for s in raw_steps if s["provider"] in allowed]
        if not filtered:
            return {
                "mode": mode,
                "steps": [],
                "no_email_reason": NO_EMAIL_REASON_FORCED_PROVIDER_CANNOT_USE_INPUT,
            }
        raw_steps = filtered

    # Capability gate: drop any step whose method the inputs do not satisfy.
    filtered_steps: list[dict[str, str]] = []
    for s in raw_steps:
        if _can_provider_use_method(s["method"], inputs):
            filtered_steps.append(s)

    # If force_provider was set and capability-gating removed everything, surface that.
    if force_provider and not filtered_steps:
        return {
            "mode": mode,
            "steps": [],
            "no_email_reason": NO_EMAIL_REASON_FORCED_PROVIDER_CANNOT_USE_INPUT,
        }

    # Same surfacing for selected_providers: if the allowlist + capability
    # gate together removed every step, return an explicit reason.
    if selected_providers is not None and not filtered_steps:
        return {
            "mode": mode,
            "steps": [],
            "no_email_reason": NO_EMAIL_REASON_FORCED_PROVIDER_CANNOT_USE_INPUT,
        }

    return {
        "mode": mode,
        "steps": filtered_steps,
        "no_email_reason": "",
        "inputs": inputs,  # for downstream executor
    }


def _build_source_path(steps_taken: list[dict[str, str]], final_method: str) -> str:
    """Build a `source_path` string from the steps actually executed.

    Format: "<identifier> -> <provider>_<method> -> ... -> <provider>_<final_method>".
    The first step is the identifier (e.g. "linkedin"), each subsequent step
    is "<provider>_<method>". The final step is the one that returned the
    email; earlier steps ran without finding one.
    """
    parts: list[str] = []
    for s in steps_taken:
        ident = s.get("identifier", "")
        method = s.get("method", "")
        provider = _provider_label(method)
        if not parts:
            parts.append(ident)
        else:
            parts.append(f"{provider}_{method}")
    # Final hop: include the provider for the final method too.
    final_provider = _provider_label(final_method)
    if final_method in ("not_found", "phone_reverse_unavailable"):
        parts.append(final_method)
    else:
        parts.append(f"{final_provider}_{final_method}")
    return " -> ".join(parts)


async def _run_route_step(
    method: str,
    inputs: dict[str, str],
    blitz_http: httpx.AsyncClient,
    contacts_http: httpx.AsyncClient,
    email_semaphore: asyncio.Semaphore,
    validate_email: bool = True,
    record_provider_use: Optional[Callable[[str], None]] = None,
    pre_resolved_getleads: Optional[dict[str, Any]] = None,
    pre_resolved_smartprospect: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Execute a single routed step. Returns one of:

        {"email": "x@y.com", "source": "<SOURCE_*>", "verification": {...}}
        {"phone_reverse": "https://linkedin.com/in/..."}  # phone hop
        {"email": "", "source": SOURCE_NOT_FOUND}

    ``pre_resolved_getleads`` / ``pre_resolved_smartprospect`` (Phase 3 batch
    coverage): when the caller ran a ``find_emails_batch`` pre-pass, the
    batch result for THIS person short-circuits the single-call path. Both
    default to None = zero behavior change for every existing caller.
    """
    from . import mailtester_client as _mt
    from . import contacts_client as _cc
    from . import blitz_client as _bc
    from . import wizleads_client as _wl
    from . import better_enrich_client as _be
    from . import smartprospect_client as _sp

    verification: dict[str, Any] = {
        "dm_email_verified": "unknown",
        "mailtester_code": "",
        "mailtester_message": "",
    }

    def _record(provider: str) -> None:
        if record_provider_use is not None:
            try:
                record_provider_use(provider)
            except Exception as e:
                logger.warning("record_provider_use(%s) failed: %s", provider, e)

    async with email_semaphore:
        if method == ROUTE_METHOD_PERSON_BY_LINKEDIN:
            _record("contacts_db")
            try:
                data = await _cc.person_by_linkedin(
                    contacts_http, inputs["linkedin_url"]
                )
            except httpx.HTTPStatusError as e:
                logger.warning("Contacts DB LinkedIn lookup failed: %s", e)
                error_type, message = _classify_http_error(e, "contacts_db", "person_by_linkedin")
                return _ProviderError(
                    provider="contacts_db",
                    method="person_by_linkedin",
                    error_type=error_type,
                    message=message,
                )
            except Exception as e:
                logger.warning("Contacts DB LinkedIn lookup failed: %s", e)
                return {"email": "", "source": SOURCE_NOT_FOUND}
            email = _cc.extract_email_from_contacts_response(data)
            if not email:
                return {"email": "", "source": SOURCE_NOT_FOUND}
            if validate_email:
                try:
                    result = await _mt.verify_email(blitz_http, email)
                    verification["dm_email_verified"] = "yes" if result["valid"] else "no"
                    verification["mailtester_code"] = result["code"]
                    verification["mailtester_message"] = result["message"]
                    if not result["valid"]:
                        try:
                            await _cc.mark_email_invalid(contacts_http, email=email)
                        except Exception:
                            pass
                        return {"email": "", "source": SOURCE_NOT_FOUND}
                except RuntimeError:
                    verification["mailtester_code"] = "unavailable"
            return {"email": email, "source": SOURCE_CONTACTS_DB_EMAIL, "verification": verification}

        if method == ROUTE_METHOD_PERSON_ENRICH_BY_LINKEDIN:
            _record("blitz")
            try:
                result = await _bc.person_enrich_by_linkedin(
                    blitz_http, inputs["linkedin_url"]
                )
            except httpx.HTTPStatusError as e:
                logger.warning("Blitz person_enrich_by_linkedin failed: %s", e)
                error_type, message = _classify_http_error(e, "blitz", "person_enrich_by_linkedin")
                return _ProviderError(
                    provider="blitz",
                    method="person_enrich_by_linkedin",
                    error_type=error_type,
                    message=message,
                )
            except Exception as e:
                logger.warning("Blitz person_enrich_by_linkedin failed: %s", e)
                return {"email": "", "source": SOURCE_NOT_FOUND}
            if result.get("found") and result.get("email"):
                verification["dm_email_verified"] = "yes"
                return {"email": result["email"], "source": SOURCE_BLITZ_EMAIL, "verification": verification}
            return {"email": "", "source": SOURCE_NOT_FOUND}

        if method == ROUTE_METHOD_FIND_WORK_EMAIL:
            _record("blitz")
            try:
                result = await _bc.find_work_email(blitz_http, inputs["linkedin_url"])
            except httpx.HTTPStatusError as e:
                logger.warning("Blitz find_work_email failed: %s", e)
                error_type, message = _classify_http_error(e, "blitz", "find_work_email")
                return _ProviderError(
                    provider="blitz",
                    method="find_work_email",
                    error_type=error_type,
                    message=message,
                )
            except Exception as e:
                logger.warning("Blitz find_work_email failed: %s", e)
                return {"email": "", "source": SOURCE_NOT_FOUND}
            if result.get("found") and result.get("email"):
                verification["dm_email_verified"] = "yes"
                return {"email": result["email"], "source": SOURCE_BLITZ_EMAIL, "verification": verification}
            return {"email": "", "source": SOURCE_NOT_FOUND}

        if method == ROUTE_METHOD_PERSON_BY_NAME_DOMAIN:
            _record("contacts_db")
            try:
                data = await _cc.person_by_name_and_domain(
                    contacts_http, inputs["full_name"], inputs["domain"]
                )
            except httpx.HTTPStatusError as e:
                logger.warning("Contacts DB name+domain lookup failed: %s", e)
                error_type, message = _classify_http_error(e, "contacts_db", "person_by_name_and_domain")
                return _ProviderError(
                    provider="contacts_db",
                    method="person_by_name_and_domain",
                    error_type=error_type,
                    message=message,
                )
            except Exception as e:
                logger.warning("Contacts DB name+domain lookup failed: %s", e)
                return {"email": "", "source": SOURCE_NOT_FOUND}
            email = _cc.extract_email_from_contacts_response(data)
            if not email:
                return {"email": "", "source": SOURCE_NOT_FOUND}
            if validate_email:
                try:
                    result = await _mt.verify_email(blitz_http, email)
                    verification["dm_email_verified"] = "yes" if result["valid"] else "no"
                    verification["mailtester_code"] = result["code"]
                    verification["mailtester_message"] = result["message"]
                    if not result["valid"]:
                        try:
                            await _cc.mark_email_invalid(contacts_http, email=email, domain=inputs["domain"])
                        except Exception:
                            pass
                        return {"email": "", "source": SOURCE_NOT_FOUND}
                except RuntimeError:
                    verification["mailtester_code"] = "unavailable"
            return {"email": email, "source": SOURCE_CONTACTS_DB_EMAIL, "verification": verification}

        if method == ROUTE_METHOD_PERSON_ENRICH:
            _record("blitz")
            try:
                result = await _bc.person_enrich(
                    blitz_http,
                    full_name=inputs["full_name"],
                    domain=inputs["domain"],
                    include_phone=False,
                )
            except httpx.HTTPStatusError as e:
                logger.warning("Blitz person_enrich failed: %s", e)
                error_type, message = _classify_http_error(e, "blitz", "person_enrich")
                return _ProviderError(
                    provider="blitz",
                    method="person_enrich",
                    error_type=error_type,
                    message=message,
                )
            except Exception as e:
                logger.warning("Blitz person_enrich failed: %s", e)
                return {"email": "", "source": SOURCE_NOT_FOUND}
            if result.get("found") and result.get("person"):
                person_data = result.get("person", {})
                verified = person_data.get("verified_email", "")
                if verified:
                    verification["dm_email_verified"] = "yes"
                    return {"email": verified, "source": SOURCE_BLITZ_EMAIL, "verification": verification}
                emails = person_data.get("emails") or []
                if emails:
                    candidate = emails[0].get("email", "")
                    if not candidate:
                        return {"email": "", "source": SOURCE_NOT_FOUND}
                    # Unverified Blitz email is NOT acceptable as final unless
                    # Mailtester confirms it (or fails-open). Otherwise fall
                    # through to WizLeads/BetterEnrich.
                    if validate_email:
                        try:
                            mt_result = await _mt.verify_email(blitz_http, candidate)
                            verification["dm_email_verified"] = "yes" if mt_result["valid"] else "no"
                            verification["mailtester_code"] = mt_result["code"]
                            verification["mailtester_message"] = mt_result["message"]
                            if mt_result["valid"]:
                                return {"email": candidate, "source": SOURCE_BLITZ_EMAIL, "verification": verification}
                            logger.info("Blitz unverified email rejected by Mailtester: %s - falling through", candidate)
                            return {"email": "", "source": SOURCE_NOT_FOUND}
                        except RuntimeError:
                            # Mailtester unavailable — fail-open per cascade policy.
                            logger.warning("Mailtester unavailable for Blitz unverified %s - accepting without verification", candidate)
                            verification["mailtester_code"] = "unavailable"
                            verification["dm_email_verified"] = "unknown"
                            return {"email": candidate, "source": SOURCE_BLITZ_EMAIL, "verification": verification}
                    # validate_email is False — fall through to next provider.
                    logger.info("Blitz returned unverified email %s but cascade continues (validation disabled)", candidate)
                    return {"email": "", "source": SOURCE_NOT_FOUND}
            return {"email": "", "source": SOURCE_NOT_FOUND}

        if method == ROUTE_METHOD_FIND_EMAIL_GETLEADS:
            # Phase 3 (batch coverage): reuse a pre-resolved batch result
            # instead of re-calling the single endpoint. Mirrors the
            # _resolve_email_for_person Step 5 pre-resolved pattern
            # (~pipeline.py:2363). An empty pre-resolved email falls through
            # to the normal single-call path below.
            if pre_resolved_getleads is not None:
                pre_email = pre_resolved_getleads.get("email", "")
                if pre_email:
                    _record("getleads")
                    vs = pre_resolved_getleads.get("verification_status")
                    verification["dm_email_verified"] = "yes" if vs == "Valid" else "unknown"
                    # Phase 2: carry the GetLeads DM attributes through on the
                    # verification dict (same overlay contract as the domain
                    # orchestrator path).
                    verification["getleads_dm"] = _getleads_dm_snapshot(pre_resolved_getleads)
                    logger.info("GetLeads (route batch pre-pass) found email: %s", pre_email)
                    return {"email": pre_email, "source": SOURCE_GETLEADS, "verification": verification}
            _record("getleads")
            try:
                result = await getleads_client.find_email(
                    blitz_http,
                    first_name=inputs["first_name"],
                    last_name=inputs["last_name"],
                    company_domain=inputs["domain"],
                )
            except httpx.HTTPStatusError as e:
                logger.warning("GetLeads find_email failed: %s", e)
                error_type, message = _classify_http_error(e, "getleads", "find_email")
                return _ProviderError(
                    provider="getleads",
                    method="find_email",
                    error_type=error_type,
                    message=message,
                )
            except Exception as e:
                logger.warning("GetLeads find_email failed: %s", e)
                return {"email": "", "source": SOURCE_NOT_FOUND}
            if _is_provider_error(result):
                return result
            if result and result.get("email"):
                # GetLeads returns email_status (VALID observed on hits);
                # trust verification_status="Valid", else accept-unverified.
                vs = result.get("verification_status")
                if vs == "Valid":
                    verification["dm_email_verified"] = "yes"
                else:
                    verification["dm_email_verified"] = "unknown"
                return {"email": result["email"], "source": SOURCE_GETLEADS, "verification": verification}
            return {"email": "", "source": SOURCE_NOT_FOUND}

        if method == ROUTE_METHOD_FIND_EMAIL_LINKEDIN_GETLEADS:
            # LinkedIn-URL-only GetLeads step (from-linkedin endpoint). Same
            # verification / getleads_dm contract as the from-person branch
            # above; only the lookup key differs.
            _record("getleads")
            try:
                result = await getleads_client.find_email_by_linkedin(
                    blitz_http,
                    inputs["linkedin_url"],
                )
            except httpx.HTTPStatusError as e:
                logger.warning("GetLeads find_email_by_linkedin failed: %s", e)
                error_type, message = _classify_http_error(e, "getleads", "find_email_by_linkedin")
                return _ProviderError(
                    provider="getleads",
                    method="find_email_by_linkedin",
                    error_type=error_type,
                    message=message,
                )
            except Exception as e:
                logger.warning("GetLeads find_email_by_linkedin failed: %s", e)
                return {"email": "", "source": SOURCE_NOT_FOUND}
            if _is_provider_error(result):
                return result
            if result and result.get("email"):
                vs = result.get("verification_status")
                if vs == "Valid":
                    verification["dm_email_verified"] = "yes"
                else:
                    verification["dm_email_verified"] = "unknown"
                verification["getleads_dm"] = _getleads_dm_snapshot(result)
                return {"email": result["email"], "source": SOURCE_GETLEADS, "verification": verification}
            return {"email": "", "source": SOURCE_NOT_FOUND}

        if method == ROUTE_METHOD_FIND_EMAIL_SMARTPROSPECT:
            # Phase 3 (batch coverage): reuse a pre-resolved batch result
            # instead of re-calling the single endpoint. Mirrors the
            # _resolve_email_for_person Step 6 pre-resolved pattern
            # (~pipeline.py:2435). An empty pre-resolved email falls through
            # to the normal single-call path below.
            if pre_resolved_smartprospect is not None:
                pre_email = pre_resolved_smartprospect.get("email", "")
                if pre_email:
                    _record("smartprospect")
                    vs = pre_resolved_smartprospect.get("verification_status")
                    verification["dm_email_verified"] = "yes" if vs == "Valid" else "unknown"
                    logger.info("SmartProspect (route batch pre-pass) found email: %s", pre_email)
                    return {"email": pre_email, "source": SOURCE_SMARTPROSPECT, "verification": verification}
            _record("smartprospect")
            try:
                result = await _sp.find_email(
                    blitz_http,
                    first_name=inputs["first_name"],
                    last_name=inputs["last_name"],
                    company_domain=inputs["domain"],
                )
            except httpx.HTTPStatusError as e:
                logger.warning("SmartProspect find_email failed: %s", e)
                error_type, message = _classify_http_error(e, "smartprospect", "find_email")
                return _ProviderError(
                    provider="smartprospect",
                    method="find_email",
                    error_type=error_type,
                    message=message,
                )
            except Exception as e:
                logger.warning("SmartProspect find_email failed: %s", e)
                return {"email": "", "source": SOURCE_NOT_FOUND}
            if _is_provider_error(result):
                return result
            if result and result.get("email"):
                # SmartProspect self-verifies; trust verification_status="Valid".
                # Found+unverified is accepted as dm_email_verified="unknown"
                # (mirrors WizLeads catchall policy).
                vs = result.get("verification_status")
                if vs == "Valid":
                    verification["dm_email_verified"] = "yes"
                else:
                    verification["dm_email_verified"] = "unknown"
                return {"email": result["email"], "source": SOURCE_SMARTPROSPECT, "verification": verification}
            return {"email": "", "source": SOURCE_NOT_FOUND}

        if method == ROUTE_METHOD_FIND_EMAIL:
            _record("wizleads")
            try:
                result = await _wl.find_email(
                    blitz_http,
                    first_name=inputs["first_name"],
                    last_name=inputs["last_name"],
                    website=inputs["domain"],
                )
            except httpx.HTTPStatusError as e:
                logger.warning("WizLeads find_email failed: %s", e)
                error_type, message = _classify_http_error(e, "wizleads", "find_email")
                return _ProviderError(
                    provider="wizleads",
                    method="find_email",
                    error_type=error_type,
                    message=message,
                )
            except Exception as e:
                logger.warning("WizLeads find_email failed: %s", e)
                return {"email": "", "source": SOURCE_NOT_FOUND}
            # Check if result is a provider error
            if _is_provider_error(result):
                return result  # Return the error object directly
            if result and result.get("email"):
                verification["dm_email_verified"] = "yes"
                return {"email": result["email"], "source": SOURCE_WIZLEADS, "verification": verification}
            return {"email": "", "source": SOURCE_NOT_FOUND}

        if method == ROUTE_METHOD_FIND_WORK_EMAIL_V3:
            _record("better_enrich")
            try:
                result = await _be.find_work_email_v3(
                    blitz_http,
                    full_name=inputs["full_name"],
                    company_domain=inputs["domain"],
                    linkedin_url=inputs.get("linkedin_url") or None,
                )
            except httpx.HTTPStatusError as e:
                logger.warning("BetterEnrich V3 failed: %s", e)
                error_type, message = _classify_http_error(e, "better_enrich", "find_work_email_v3")
                return _ProviderError(
                    provider="better_enrich",
                    method="find_work_email_v3",
                    error_type=error_type,
                    message=message,
                )
            except Exception as e:
                logger.warning("BetterEnrich V3 failed: %s", e)
                return {"email": "", "source": SOURCE_NOT_FOUND}
            # Check if result is a provider error
            if _is_provider_error(result):
                return result  # Return the error object directly
            if result and result.get("email"):
                email_status = result.get("email_status", "verified")
                verification["dm_email_verified"] = "yes" if email_status in ("verified", "valid") else "unknown"
                return {"email": result["email"], "source": SOURCE_BETTER_ENRICH_PERSON, "verification": verification}
            return {"email": "", "source": SOURCE_NOT_FOUND}

        if method == ROUTE_METHOD_PHONE_REVERSE_LOOKUP:
            # No provider in this codebase has a phone->LinkedIn reverse
            # endpoint. The router emits this step so callers know the
            # phone->LinkedIn hop was attempted; the executor returns
            # `phone_reverse` so the caller can continue with name/domain
            # or stop with a clear no_email_reason.
            return {"phone_reverse": "", "no_email_reason": NO_EMAIL_REASON_PHONE_REVERSE_UNAVAILABLE}

        return {"email": "", "source": SOURCE_NOT_FOUND}


async def run_enrichment_route(
    route: dict[str, Any],
    blitz_http: httpx.AsyncClient,
    contacts_http: httpx.AsyncClient,
    email_semaphore: asyncio.Semaphore,
    validate_email: bool = True,
    *,
    job_id: str = "",
    row_index: int = -1,
    emit_logs: bool = True,
    record_provider_use: Optional[Callable[[str], None]] = None,
    pre_resolved_getleads: Optional[dict[str, Any]] = None,
    pre_resolved_smartprospect: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Execute a route produced by `route_enrichment`.

    Returns a result dict:
        {
            "email": str,
            "source": str,                  # one of SOURCE_*
            "verification": dict,
            "source_path": str,             # "linkedin -> contacts_db" etc.
            "provider_attempts": [str, ...],  # compact string list (back-compat)
            "provider_attempts_json": [dict, ...],  # structured records
            "providers_called": [str, ...],
            "providers_skipped": [{"provider", "method", "skipped_reason"}, ...],
            "no_email_reason": str,
            "final_email_status": str,
            "final_email_verification_source": str,
        }
    """
    steps = route.get("steps", []) or []
    no_email_reason = route.get("no_email_reason", "") or ""
    inputs = route.get("inputs", {}) or {}
    domain = inputs.get("domain", "")
    normalized_li = inputs.get("linkedin_url", "")

    if no_email_reason:
        # No steps to run; record a single attempt for visibility.
        attempted = _build_provider_attempt(
            job_id=job_id,
            row_index=row_index,
            domain=domain,
            normalized_linkedin_url=normalized_li,
            provider="",
            method="",
            input_type_used="",
            called=False,
            skipped_reason=no_email_reason,
            status="not_run",
            email_found=False,
            error_type="",
            latency_ms=0,
        )
        if emit_logs:
            _log_provider_attempt(attempted)
        return {
            "email": "",
            "source": SOURCE_NOT_FOUND,
            "verification": {
                "dm_email_verified": "unknown",
                "mailtester_code": "",
                "mailtester_message": "",
            },
            "source_path": "",
            "provider_attempts": [],
            "provider_attempts_json": [attempted],
            "providers_called": [],
            "providers_skipped": [],
            "no_email_reason": no_email_reason,
            "final_email_status": "not_run",
            "final_email_verification_source": "",
        }

    attempts: list[str] = []
    attempts_json: list[dict[str, Any]] = []
    steps_taken: list[dict[str, str]] = []
    final_email_status = ""
    final_email_verification_source = ""

    for step in steps:
        method = step["method"]
        identifier = step["identifier"]
        provider = _provider_label(method)
        attempts.append(f"{method}@{identifier}")
        t0 = time.monotonic()
        result = await _run_route_step(
            method,
            inputs,
            blitz_http,
            contacts_http,
            email_semaphore,
            validate_email=validate_email,
            record_provider_use=record_provider_use,
            pre_resolved_getleads=pre_resolved_getleads,
            pre_resolved_smartprospect=pre_resolved_smartprospect,
        )
        latency_ms = int((time.monotonic() - t0) * 1000)
        steps_taken.append(step)

        # Build the structured attempt record for this call.
        # First check if result is a provider error
        provider_error_dict = _extract_provider_error(result)
        if provider_error_dict:
            # Result is a provider error - no email found
            email_found = False
            error_type = provider_error_dict.get("error_type", "")
            status = "no_match"
        else:
            # Result is a normal dict (or None) - extract email info
            email_found = bool(result and result.get("email"))
            error_type = ""
            status = "no_match"

            if result and result.get("phone_reverse") is not None:
                status = "phone_reverse_unavailable"
                error_type = "phone_reverse_unavailable"
            elif email_found:
                status = "email_found"
        attempt = _build_provider_attempt(
            job_id=job_id,
            row_index=row_index,
            domain=domain,
            normalized_linkedin_url=normalized_li,
            provider=provider,
            method=method,
            input_type_used=identifier,
            called=True,
            skipped_reason="",
            status=status,
            email_found=email_found,
            error_type=error_type,
            latency_ms=latency_ms,
        )
        attempts_json.append(attempt)
        if emit_logs:
            _log_provider_attempt(attempt)

        # If this was a provider error, skip result processing and continue cascade
        if provider_error_dict:
            continue

        if result.get("phone_reverse") is not None:
            # Phone reverse hop — append a marker to source_path and stop.
            summary = _compact_attempts_summary(attempts_json)
            return {
                "email": "",
                "source": SOURCE_NOT_FOUND,
                "verification": {
                    "dm_email_verified": "unknown",
                    "mailtester_code": "",
                    "mailtester_message": "",
                },
                "source_path": _build_source_path(steps_taken, "phone_reverse_unavailable"),
                "provider_attempts": attempts,
                "provider_attempts_json": attempts_json,
                "providers_called": summary["providers_called"],
                "providers_skipped": summary["providers_skipped"],
                "no_email_reason": result.get("no_email_reason") or NO_EMAIL_REASON_PHONE_REVERSE_UNAVAILABLE,
                "final_email_status": "phone_reverse_unavailable",
                "final_email_verification_source": "",
            }

        if email_found:
            verif = result.get("verification") or {}
            final_email_status = "enriched"
            final_email_verification_source = "mailtester" if validate_email and verif.get("dm_email_verified") in ("yes", "no") else "provider_self"
            summary = _compact_attempts_summary(attempts_json)
            return {
                "email": result["email"],
                "source": result.get("source", SOURCE_NOT_FOUND),
                "verification": verif,
                "source_path": _build_source_path(steps_taken, method),
                "provider_attempts": attempts,
                "provider_attempts_json": attempts_json,
                "providers_called": summary["providers_called"],
                "providers_skipped": summary["providers_skipped"],
                "no_email_reason": "",
                "final_email_status": final_email_status,
                "final_email_verification_source": final_email_verification_source,
            }

    summary = _compact_attempts_summary(attempts_json)
    return {
        "email": "",
        "source": SOURCE_NOT_FOUND,
        "verification": {
            "dm_email_verified": "unknown",
            "mailtester_code": "",
            "mailtester_message": "",
        },
        "source_path": _build_source_path(steps_taken, "not_found") if steps_taken else "",
        "provider_attempts": attempts,
        "provider_attempts_json": attempts_json,
        "providers_called": summary["providers_called"],
        "providers_skipped": summary["providers_skipped"],
        "no_email_reason": "all_providers_called_no_email" if attempts_json else "no_providers_attempted",
        "final_email_status": "not_found" if attempts_json else "not_run",
        "final_email_verification_source": "",
    }


def _empty_enriched() -> dict[str, Any]:
    return {col: "" for col in ENRICHED_COLUMNS}


async def _maybe_apply_company_fallbacks(
    blitz_http: httpx.AsyncClient,
    result_rows: list[OutputRow],
    *,
    domain: str,
    facebook_url: str,
    source_path_prefix: str,
    validate_email: bool,
    dedupe: "company_fallback.CompanyFallbackDedupe",
    record_provider_use: Optional[Callable[[str], None]] = None,
    collector: Optional[Any] = None,
) -> None:
    """Run the company / page-level fallback tier for a result row and
    mutate it in place.

    If every result row has a decision-maker email, the fallback is
    skipped (per the contract: these are AFTER the person waterfall).
    If at least one row has no dm_email, the fallback runs once with
    the row's domain + facebook_url and the result is applied to every
    row that lacks a person email.

    Mutates result_rows in place. No-ops when both fallback flags are
    off AND no facebook URL is present on the row.
    """
    from . import fallback_config as fb_cfg
    if not fb_cfg.ENABLE_COMPANY_EMAIL_FALLBACK and not fb_cfg.ENABLE_FACEBOOK_EMAIL_FALLBACK:
        return

    # Skip API spend if every row already has a person-level email.
    any_missing = any(not r.get("dm_email", "") for r in result_rows)
    if not any_missing:
        # Just make sure final_email reflects the person email.
        for row in result_rows:
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
        validate_email=validate_email,
        dedupe=dedupe,
        record_provider_use=record_provider_use,
        collector=collector,
        company_linkedin_url=(result_rows[0].get("company_linkedin_url", "") if result_rows else ""),
    )

    for row in result_rows:
        person_email = row.get("dm_email", "")
        person_source_path = row.get("source_path", "")
        # Only apply fallback to rows that lack a person email. If a
        # person email was found, just set final_email to it.
        if not person_email:
            company_fallback.apply_company_fallbacks_to_row(
                row,
                fb_result,
                person_email="",
                person_source_path="",
            )
        else:
            # Person email wins. Make sure final_email reflects it.
            company_fallback.apply_company_fallbacks_to_row(
                row,
                fb_result,
                person_email=person_email,
                person_source_path=person_source_path,
            )
        # Append any company-fallback providers_called entries to the row's
        # providers_called JSON. We only added new "better_enrich*" entries
        # that the person cascade did not record, so this is additive.
        fb_called = fb_result.get("providers_called", []) or []
        if fb_called:
            try:
                existing = json.loads(row.get("providers_called", "") or "[]")
            except Exception:
                existing = []
            if not isinstance(existing, list):
                existing = []
            for p in fb_called:
                if p not in existing:
                    existing.append(p)
            row["providers_called"] = json.dumps(existing, ensure_ascii=False)


def _row_audit_record(
    *,
    job_id: str,
    row_index: int,
    domain: str,
    input_fields_used: str,
    input_payload: dict[str, Any],
    source_path: str,
    no_email_reason: str,
    final_email_status: str,
    final_email_verification_source: str,
    attempts: list[dict[str, Any]],
) -> dict[str, Any]:
    """Build the full audit record for one CSV row.

    Returned dict is the JSONL sidecar payload. It mirrors the columns
    embedded in the CSV plus the structured attempts list.
    """
    return {
        "job_id": job_id or "",
        "row_index": int(row_index) if row_index is not None else -1,
        "domain": domain or "",
        "input_fields_used": input_fields_used or "",
        "input_payload": {k: v for k, v in (input_payload or {}).items() if k.startswith("input_") or k in {"normalized_linkedin_url", "linkedin_username"}},
        "source_path": source_path or "",
        "no_email_reason": no_email_reason or "",
        "final_email_status": final_email_status or "",
        "final_email_verification_source": final_email_verification_source or "",
        "provider_attempts": attempts or [],
    }


def write_audit_sidecar(
    job_id: str,
    records: list[dict[str, Any]],
    *,
    base_dir: Optional[Path] = None,
) -> Optional[Path]:
    """Write a per-job JSONL audit sidecar.

    Each record is one input row. Returns the path written, or None on
    failure (the caller's CSV write is the source of truth; the sidecar is
    an additive observability tool).
    """
    if not job_id or not records:
        return None
    base = Path(base_dir) if base_dir else AUDIT_SIDECAR_DIR
    try:
        base.mkdir(parents=True, exist_ok=True)
        path = base / f"{job_id}_audit.jsonl"
        with path.open("w", encoding="utf-8") as fh:
            for rec in records:
                fh.write(json.dumps(rec, ensure_ascii=False, sort_keys=True))
                fh.write("\n")
        return path
    except Exception as e:  # never let the sidecar break the job
        logger.warning("Failed to write audit sidecar for job %s: %s", job_id, e)
        return None


def _attempts_json_size(attempts: list[dict[str, Any]]) -> int:
    """Return the byte-size of attempts when JSON-serialized (compact form)."""
    try:
        return len(json.dumps(attempts, ensure_ascii=False, separators=(",", ":")))
    except Exception:
        return 0


def _attempts_json_compact(attempts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return a compact copy of attempts for CSV embedding.

    Drops heavy fields (job_id, row_index, normalized_linkedin_url repeats)
    and keeps the unique per-attempt detail.
    """
    compact: list[dict[str, Any]] = []
    for a in attempts or []:
        compact.append({
            "provider": a.get("provider", ""),
            "method": a.get("method", ""),
            "input_type_used": a.get("input_type_used", ""),
            "called": a.get("called", False),
            "skipped_reason": a.get("skipped_reason", ""),
            "status": a.get("status", ""),
            "email_found": a.get("email_found", False),
            "error_type": a.get("error_type", ""),
            "latency_ms": a.get("latency_ms", 0),
        })
    return compact


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


# Field names providers might use for "job title" beyond the canonical ``title``.
# Tried in order; first non-empty value wins. Purely defensive — most providers
# use ``title`` but some historical / future ones may use these alternatives.
_TITLE_ALTERNATIVE_KEYS: tuple[str, ...] = (
    "job_title",
    "position",
    "role",
    "occupation",
    "jobTitle",  # camelCase variant some JSON APIs use
    "current_role",
)


def _backfill_person_identity(person: dict[str, Any]) -> tuple[str, str, str]:
    """Return ``(first_name, last_name, title)`` with backfill applied.

   填补规则:

      * ``first_name`` / ``last_name``: when either is empty and ``full_name``
        is populated, split ``full_name`` on its first space — first token
        becomes ``first_name``, remainder becomes ``last_name``. Mirrors the
        auto-derivation in :func:`route_enrichment` and the per-step splits
        inside :func:`_resolve_email_for_person`.

      * ``title``: when the canonical ``title`` field is empty, try the
        alternative keys in ``_TITLE_ALTERNATIVE_KEYS``. Falls through to
        ``""`` if none are populated.

    Never mutates ``person``. Always returns strings (never ``None``).
    """
    if not isinstance(person, dict):
        return "", "", ""

    first = (person.get("first_name") or "").strip() if isinstance(person.get("first_name"), str) else ""
    last = (person.get("last_name") or "").strip() if isinstance(person.get("last_name"), str) else ""
    full = (person.get("full_name") or "").strip() if isinstance(person.get("full_name"), str) else ""

    # Backfill first/last from full_name (only fills empty slots — never
    # clobbers explicit values).
    if (not first or not last) and full:
        parts = full.split(" ", 1)
        if not first and parts:
            first = parts[0].strip()
        if not last and len(parts) > 1:
            last = parts[1].strip()

    # Title: try canonical key first, then alternatives.
    title_val = person.get("title")
    title = title_val.strip() if isinstance(title_val, str) and title_val.strip() else ""
    if not title:
        for key in _TITLE_ALTERNATIVE_KEYS:
            v = person.get(key)
            if isinstance(v, str) and v.strip():
                title = v.strip()
                break

    return first, last, title


def _getleads_dm_snapshot(item: Optional[dict[str, Any]]) -> dict[str, str]:
    """Extract the GetLeads-sourced DM attributes from a normalized
    ``_normalize_result_item`` result (or a pre-resolved batch result, which
    uses the same shape).

    The snapshot rides on ``verification_info["getleads_dm"]`` from the
    Step 5 return sites into ``_build_person_row``, which overlays it onto
    the row built from the Blitz/contacts_db person. Values are stripped
    strings; missing keys stay "" (the overlay skips empty values).
    """
    if not isinstance(item, dict):
        return {}
    return {
        "title": str(item.get("job_title") or "").strip(),
        "headline": str(item.get("linkedin_headline") or "").strip(),
        "city": str(item.get("city") or "").strip(),
        "country": str(item.get("country") or "").strip(),
        "phone": str(item.get("phone") or "").strip(),
        "job_level": str(item.get("job_level") or "").strip(),
        "job_function": str(item.get("job_function") or "").strip(),
        "revenue": str(item.get("revenue") or "").strip(),
        "employee_count": str(item.get("employee_count") or "").strip(),
        "linkedin_connections": str(item.get("linkedin_connections") or "").strip(),
        "email_last_verified_at": str(item.get("email_last_verified_at") or "").strip(),
    }


def _build_person_row(
    base_row: dict[str, Any],
    company_linkedin_url: str,
    person: dict[str, Any],
    icp_tier: int,
    email: str,
    email_source: str,
    verification_info: Optional[dict[str, Any]] = None,
) -> OutputRow:
    loc = person.get("location") or {}
    experiences = person.get("experiences") or []
    # Backfill first/last from full_name and try alternative title field names.
    # This ensures the output (CSV + API response) has these fields populated
    # even when the provider (e.g., Contacts DB) returns only full_name.
    first_name_bf, last_name_bf, title_bf = _backfill_person_identity(person)
    direct_title = title_bf or person.get("title", "")
    # Pattern A: _empty_enriched() must come FIRST so the company-level
    # fields already stamped on base_row (company_name, company_industry,
    # company_employee_count) take precedence over the empty-string defaults.
    row = {**_empty_enriched(), **base_row}
    row["company_linkedin_url"] = company_linkedin_url
    row["dm_first_name"] = first_name_bf
    row["dm_last_name"] = last_name_bf
    row["dm_full_name"] = person.get("full_name", "")
    row["dm_title"] = _current_title(experiences, direct_title)
    row["dm_linkedin_url"] = person.get("linkedin_url", "")
    row["dm_email"] = email
    row["dm_email_source"] = email_source
    row["dm_headline"] = person.get("headline", "")
    row["dm_location_city"] = loc.get("city", "")
    row["dm_location_country"] = loc.get("country_code", "")
    row["dm_icp_tier"] = icp_tier
    row["row_status"] = STATUS_ENRICHED if email else STATUS_NO_CONTACTS

    # Pattern A: extract seniority / function from the provider person dict.
    # Blitz exposes these as ``job_level`` and ``job_function``; fall back to
    # ``seniority`` / ``function`` aliases for forward-compat with future
    # providers. Empty when absent (fail-open).
    row["dm_job_level"] = (
        person.get("job_level")
        or person.get("seniority")
        or ""
    )
    row["dm_job_function"] = (
        person.get("job_function")
        or person.get("function")
        or ""
    )

    # Add verification status if available
    if verification_info:
        row["dm_email_verified"] = verification_info.get("dm_email_verified", "unknown")
        row["mailtester_code"] = verification_info.get("mailtester_code", "")
        row["mailtester_message"] = verification_info.get("mailtester_message", "")
    else:
        row["dm_email_verified"] = "unknown"
        row["mailtester_code"] = ""
        row["mailtester_message"] = ""

    # Phase 2 (full capture): when GetLeads won the email race, overlay its
    # DM attributes onto the Blitz/contacts_db-derived row. ZERO change when
    # ``getleads_dm`` is absent (every non-GetLeads path) — the overlay only
    # fires on non-empty values, so a GetLeads miss never blanks a field.
    getleads_dm = (verification_info or {}).get("getleads_dm") or {}
    for col, src in (
        ("dm_title", "title"),
        ("dm_headline", "headline"),
        ("dm_location_city", "city"),
        ("dm_location_country", "country"),
        ("dm_phone", "phone"),
        ("dm_job_level", "job_level"),
        ("dm_job_function", "job_function"),
    ):
        v = getleads_dm.get(src)
        if isinstance(v, str) and v.strip():
            row[col] = v.strip()
    # Fill-only columns: company_* are stamped earlier from Contacts DB and
    # must NEVER be overwritten (fallback only); dm_email_last_verified_at /
    # dm_linkedin_connections have no other source, so any value wins.
    if getleads_dm:
        for col, src, fill_only in (
            ("company_revenue", "revenue", True),
            ("company_employee_count", "employee_count", True),
            ("dm_email_last_verified_at", "email_last_verified_at", False),
            ("dm_linkedin_connections", "linkedin_connections", False),
        ):
            v = getleads_dm.get(src)
            if not (isinstance(v, str) and v.strip()):
                continue
            if fill_only and row.get(col):
                continue
            row[col] = v.strip()

    # Pattern C: serialize per-row provider error list to JSON.
    # ``verification_info["provider_errors"]`` is a list[dict] populated by
    # ``_resolve_email_for_person`` on every provider failure. When empty
    # (or when no verification_info), the CSV cell is an empty string so
    # the column stays backward-compatible with consumers that expect a
    # plain string and not "[]". Phase 3 strips this from the JSON response.
    row_errors: list[dict[str, Any]] = []
    if verification_info:
        row_errors = verification_info.get("provider_errors") or []
    if row_errors:
        try:
            row["provider_errors"] = json.dumps(row_errors, ensure_ascii=False)
        except (TypeError, ValueError) as _json_err:
            # Should never happen — dicts are plain primitives — but never
            # let serialization break the row.
            logger.warning("Failed to serialize provider_errors for row: %s", _json_err)
            row["provider_errors"] = ""

    return row


def _stamp_company_errors(row: OutputRow, company_errors: list[dict[str, Any]]) -> None:
    """Pattern C: stamp company-level provider errors onto a non-person row.

    Used for the no_linkedin / no_contacts / error terminal rows where
    ``_build_person_row`` was never called (so no verification_info ever
    existed). Mutates the row in place — never raises.

    Empty list → empty string in the CSV cell (fail-open).
    """
    if not company_errors:
        return
    try:
        row["provider_errors"] = json.dumps(company_errors, ensure_ascii=False)
    except (TypeError, ValueError) as _json_err:
        logger.warning("Failed to serialize company_errors for row: %s", _json_err)
        row["provider_errors"] = ""


def _merge_company_errors_into_row(
    row: OutputRow,
    company_errors: list[dict[str, Any]],
) -> None:
    """Pattern C: merge company-level provider errors into a person row.

    The row's ``provider_errors`` cell may already hold a JSON list of
    person-level errors (set by ``_build_person_row``). Company-level
    errors (e.g. Contacts DB company lookup failed, Blitz waterfall
    failed) are appended, deduped by (provider, method) so a provider
    that failed at both levels doesn't appear twice with the same
    message.

    Mutates the row in place. Never raises. No-op when ``company_errors``
    is empty AND the row already has no provider_errors.
    """
    if not company_errors:
        return

    # Parse existing person-level errors (if any).
    existing_raw = row.get("provider_errors", "") or ""
    existing: list[dict[str, Any]] = []
    if existing_raw:
        try:
            parsed = json.loads(existing_raw)
            if isinstance(parsed, list):
                existing = parsed
        except (TypeError, ValueError):
            existing = []

    # Dedupe by (provider, method) signature. We keep the first occurrence;
    # if a (provider, method) is already present from the person level, we
    # do not duplicate the company-level entry. This keeps the CSV cell
    # readable even when the same provider fails at both stages.
    seen = {
        (e.get("provider", ""), e.get("method", ""))
        for e in existing
        if isinstance(e, dict)
    }
    merged = list(existing)
    for err in company_errors:
        if not isinstance(err, dict):
            continue
        sig = (err.get("provider", ""), err.get("method", ""))
        if sig in seen:
            continue
        seen.add(sig)
        merged.append(err)

    if merged:
        try:
            row["provider_errors"] = json.dumps(merged, ensure_ascii=False)
        except (TypeError, ValueError) as _json_err:
            logger.warning("Failed to serialize merged provider_errors: %s", _json_err)
            # Leave the cell as it was — never make the row worse.


def _no_linkedin_row(base_row: dict[str, Any]) -> OutputRow:
    # Pattern A: _empty_enriched() first so base_row company fields win.
    row = {**_empty_enriched(), **base_row}
    row["row_status"] = STATUS_NO_LINKEDIN
    return row


def _no_contacts_row(base_row: dict[str, Any], company_linkedin_url: str = "") -> OutputRow:
    row = {**_empty_enriched(), **base_row}
    row["company_linkedin_url"] = company_linkedin_url
    row["row_status"] = STATUS_NO_CONTACTS
    return row


def _error_row(base_row: dict[str, Any], company_linkedin_url: str = "") -> OutputRow:
    row = {**_empty_enriched(), **base_row}
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
    row = {**_empty_enriched(), **base_row}
    row["company_linkedin_url"] = company_linkedin_url
    row["dm_email"] = email
    row["dm_email_source"] = email_source
    row["row_status"] = STATUS_ENRICHED
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
    validate_email: bool = True,
    record_provider_use: Optional[Callable[[str], None]] = None,
    collector: Optional[Any] = None,  # RawContactCollector; Phase 2a capture
    company_linkedin_url: str = "",
    pre_resolved_smartprospect: Optional[dict[str, Any]] = None,
    pre_resolved_getleads: Optional[dict[str, Any]] = None,
) -> tuple[str, str, dict[str, Any]]:
    """
    Returns (email, source, verification_info).
    Priority cascade:
      1. Contacts DB by person's name + domain (PRIMARY - avoids stale LinkedIn emails)
      2. Contacts DB by LinkedIn URL (SECONDARY)
      3. Blitz person enrich by name + domain (PRIMARY PAID)
      4. Blitz email from LinkedIn URL (SECONDARY PAID)
      5. GetLeads person email (first + last + domain, batch-capable)
      6. SmartProspect person email (self-verifying, 30 RPS, batch-capable)
      7. WizLeads person email (CATCHALL VERIFIED, 10 RPS)
      8. BetterEnrich work email (person lookup)
      9. Contacts DB by name + domain (name from input row, if different)

    Args:
        force_provider: If set, only use that specific provider.
        validate_email: If True, verify Contacts DB emails with mailtester.
        record_provider_use: Optional callback invoked with provider name
            ("contacts_db" | "blitz" | "wizleads" | "better_enrich") at the
            moment each provider is *attempted* (not gated on success). This
            is what populates `used_providers` on the job.
        collector: Optional ``RawContactCollector``. When provided, every
            provider response that returns a non-None dict is captured
            BEFORE validation/truncation drops it. Phase 2a — no behavior
            change when None.
        company_linkedin_url: Optional company LinkedIn URL included in
            collector payloads for lineage. Defaults to "" for domain-only
            cascades.
        pre_resolved_smartprospect: Phase 5 — when the domain orchestrator
            runs a smartprospect batch pre-pass, this dict carries the
            per-person result so Step 6 can reuse it instead of re-calling
            the API. Shape matches smartprospect_client.find_email output:
            ``{email, status, verification_status, first_name, last_name, domain}``.
            When None (or when its email is empty), Step 6 falls back to the
            normal single-call path. The pre-resolved check ONLY short-circuits
            the smartprospect step — Contacts DB and Blitz still run first so
            free-tier emails win when available.
        pre_resolved_getleads: Phase 5 — when the domain orchestrator runs a
            getleads batch pre-pass, this dict carries the per-person result so
            Step 5 can reuse it instead of re-calling the API. Shape matches
            getleads_client.find_email output: ``{email, first_name, last_name,
            domain, verification_status, linkedin_url, phone}``. When None (or
            when its email is empty), Step 5 falls back to the normal single-call
            path. The pre-resolved check ONLY short-circuits the getleads step.

    verification_info dict contains:
        - dm_email_verified: "yes", "no", "unknown"
        - mailtester_code: "ok", "mb", "ko", "unavailable", ""
        - mailtester_message: Message from mailtester
    """
    linkedin_url = person.get("linkedin_url", "")
    full_name = person.get("full_name", "")
    first_name = person.get("first_name", "")
    last_name = person.get("last_name", "")

    # Initialize verification info
    verification_info: dict[str, Any] = {
        "dm_email_verified": "unknown",
        "mailtester_code": "",
        "mailtester_message": "",
        # Pattern C: per-row provider error list. Every provider call that
        # raises an exception (or fails meaningfully) appends a record here.
        # ``_build_person_row`` serializes this to JSON in the row's
        # ``provider_errors`` CSV cell. Empty list → empty string in CSV.
        "provider_errors": [],
    }

    def _record_error(provider: str, method: str, error: Any, error_type: str = "unknown") -> None:
        """Pattern C: capture a provider error to the per-row list.

        Capped at 300 chars per message so a giant stack trace doesn't
        blow up the CSV cell. Never raises.
        """
        try:
            verification_info["provider_errors"].append({
                "provider": provider,
                "method": method,
                "error_type": error_type,
                "message": str(error)[:300],
            })
        except Exception as cap_err:  # never let capture break the cascade
            logger.debug("provider_errors capture failed: %s", cap_err)

    def _record(provider: str) -> None:
        if record_provider_use is not None:
            try:
                record_provider_use(provider)
            except Exception as e:  # never let accounting break the cascade
                logger.warning("record_provider_use(%s) failed: %s", provider, e)

    def _capture(source: str, contact: dict[str, Any]) -> None:
        """Phase 2a: forward every provider response dict to the collector.

        Captures the RAW provider response (BEFORE mailtester validation
        drops it). No-op when collector is None or contact is falsy. Never
        raises — capture is a side-effect, not part of the cascade contract.
        """
        if collector is None or not contact:
            return
        try:
            collector.capture_company_contact(
                source=source,
                domain=domain,
                company_linkedin_url=company_linkedin_url,
                contact=contact,
            )
        except Exception as cap_err:
            logger.debug(
                "collector.capture_company_contact(%s) failed for %s: %s",
                source, domain, cap_err,
            )

    async with email_semaphore:
        # Step 1: Contacts DB by person's name + domain (PRIMARY - FREE)
        # When both name and LinkedIn URL are available, prefer name+domain to avoid
        # returning stale emails from previous employers via person_by_linkedin
        if full_name and domain and not _should_skip_provider("contacts_db", force_provider):
            _record("contacts_db")
            try:
                contacts_data = await contacts_client.person_by_name_and_domain(
                    contacts_client_inst, full_name, domain
                )
                email = contacts_client.extract_email_from_contacts_response(contacts_data)
                # Phase 2a: capture the Contacts DB response BEFORE validation
                # drops it. Capture even when email is missing — name+title
                # is still a meaningful person contact. Mutate a copy so the
                # downstream extract_email path is unaffected.
                if contacts_data:
                    _cap_contact = dict(contacts_data)
                    _cap_contact.setdefault("full_name", full_name)
                    if email:
                        _cap_contact["email"] = email
                    _capture("contacts_db", _cap_contact)
                if email:
                    # Verify email with mailtester if enabled
                    if validate_email:
                        try:
                            result = await mailtester_client.verify_email(
                                blitz_client_inst, email
                            )
                            verification_info["dm_email_verified"] = "yes" if result["valid"] else "no"
                            verification_info["mailtester_code"] = result["code"]
                            verification_info["mailtester_message"] = result["message"]

                            if result["valid"]:
                                # Email is valid - use it
                                logger.debug("Email verified: %s (code: %s)", email, result["code"])
                                return email, SOURCE_CONTACTS_DB_EMAIL, verification_info
                            else:
                                # Email rejected. Only poison the Contacts DB
                                # row on hard-invalid (ko); policy-rejected codes
                                # (e.g. mb under the ok-only policy) skip the
                                # write so the row can resurface if the policy
                                # is later relaxed.
                                if result["code"] == "ko":
                                    logger.info("Email verification failed: %s (code: %s) - marking invalid", email, result["code"])
                                    await contacts_client.mark_email_invalid(
                                        contacts_client_inst,
                                        email=email,
                                        domain=domain,
                                    )
                                else:
                                    logger.info("Email rejected by policy: %s (code: %s) - skipping mark_email_invalid", email, result["code"])
                                # Continue to next provider
                        except RuntimeError:
                            # Mailtester unavailable - FAIL OPEN
                            logger.warning("Mailtester unavailable for %s - accepting without verification", email)
                            verification_info["mailtester_code"] = "unavailable"
                            verification_info["dm_email_verified"] = "unknown"
                            return email, SOURCE_CONTACTS_DB_EMAIL, verification_info
                    else:
                        # Verification disabled - use email as-is
                        return email, SOURCE_CONTACTS_DB_EMAIL, verification_info
            except Exception as e:
                _record_error("contacts_db", "person_by_name_and_domain", e)
                logger.warning("Contacts DB name+domain lookup failed for %s / %s: %s", full_name, domain, e)

        # Step 2: Contacts DB by LinkedIn URL (SECONDARY - FREE)
        # Fall back to LinkedIn if name+domain didn't find email, or if name not available
        if linkedin_url and not _should_skip_provider("contacts_db", force_provider):
            _record("contacts_db")
            try:
                contacts_data = await contacts_client.person_by_linkedin(
                    contacts_client_inst, linkedin_url
                )
                email = contacts_client.extract_email_from_contacts_response(contacts_data)
                # Phase 2a: capture the Contacts DB LinkedIn-URL lookup result
                # (same shape as Step 1).
                if contacts_data:
                    _cap_contact = dict(contacts_data)
                    _cap_contact.setdefault("linkedin_url", linkedin_url)
                    if email:
                        _cap_contact["email"] = email
                    _capture("contacts_db", _cap_contact)
                if email:
                    # Verify email with mailtester if enabled
                    if validate_email:
                        try:
                            result = await mailtester_client.verify_email(
                                blitz_client_inst, email
                            )
                            verification_info["dm_email_verified"] = "yes" if result["valid"] else "no"
                            verification_info["mailtester_code"] = result["code"]
                            verification_info["mailtester_message"] = result["message"]

                            if result["valid"]:
                                # Email is valid - use it
                                logger.debug("Email verified: %s (code: %s)", email, result["code"])
                                return email, SOURCE_CONTACTS_DB_EMAIL, verification_info
                            else:
                                # Email rejected. Only poison the Contacts DB
                                # row on hard-invalid (ko); policy-rejected codes
                                # (e.g. mb under the ok-only policy) skip the
                                # write so the row can resurface if the policy
                                # is later relaxed.
                                if result["code"] == "ko":
                                    logger.info("Email verification failed: %s (code: %s) - marking invalid", email, result["code"])
                                    await contacts_client.mark_email_invalid(
                                        contacts_client_inst,
                                        email=email,
                                    )
                                else:
                                    logger.info("Email rejected by policy: %s (code: %s) - skipping mark_email_invalid", email, result["code"])
                                # Continue to next provider
                        except RuntimeError:
                            # Mailtester unavailable - FAIL OPEN
                            logger.warning("Mailtester unavailable for %s - accepting without verification", email)
                            verification_info["mailtester_code"] = "unavailable"
                            verification_info["dm_email_verified"] = "unknown"
                            return email, SOURCE_CONTACTS_DB_EMAIL, verification_info
                    else:
                        # Verification disabled - use email as-is
                        return email, SOURCE_CONTACTS_DB_EMAIL, verification_info
            except Exception as e:
                _record_error("contacts_db", "person_by_linkedin", e)
                logger.warning("Contacts DB LinkedIn lookup failed for %s: %s", linkedin_url, e)

        # Step 3: Blitz person enrich by name + domain (PRIMARY PAID)
        # Prioritize name+domain for the same reason as Contacts DB
        if full_name and domain and not _should_skip_provider("blitz", force_provider):
            _record("blitz")
            try:
                result = await blitz_client.person_enrich(
                    blitz_client_inst,
                    full_name=full_name,
                    domain=domain,
                    include_phone=False,
                )
                if result.get("found") and result.get("person"):
                    person_data = result.get("person", {})
                    # Phase 2a: capture the Blitz person dict BEFORE any
                    # mailtester-driven truncation. person_data already
                    # carries full_name, linkedin_url, title, etc.
                    _capture("blitz", person_data)
                    # Check for verified_email field first — provider-acceptable
                    verified_email = person_data.get("verified_email", "")
                    if verified_email:
                        verification_info["dm_email_verified"] = "yes"
                        return verified_email, SOURCE_BLITZ_EMAIL, verification_info
                    # Fall back to unverified emails list. Per the cascade
                    # contract, an unverified Blitz email is NOT acceptable as
                    # the final answer unless Mailtester confirms it (or
                    # Mailtester is unavailable under a fail-open policy).
                    # Otherwise we fall through to WizLeads and BetterEnrich.
                    emails_list = person_data.get("emails", [])
                    if emails_list:
                        candidate = emails_list[0].get("email", "")
                        if candidate and validate_email:
                            try:
                                result_mt = await mailtester_client.verify_email(
                                    blitz_client_inst, candidate
                                )
                                verification_info["dm_email_verified"] = "yes" if result_mt["valid"] else "no"
                                verification_info["mailtester_code"] = result_mt["code"]
                                verification_info["mailtester_message"] = result_mt["message"]
                                if result_mt["valid"]:
                                    logger.debug("Blitz unverified email confirmed by Mailtester: %s", candidate)
                                    return candidate, SOURCE_BLITZ_EMAIL, verification_info
                                # Mailtester rejected it — continue cascade.
                                logger.info("Blitz unverified email rejected by Mailtester: %s (code: %s) - falling through",
                                            candidate, result_mt["code"])
                            except RuntimeError:
                                # Mailtester unavailable — FAIL OPEN at the
                                # candidate level (consistent with Contacts DB
                                # policy above) and accept as final.
                                logger.warning("Mailtester unavailable for Blitz unverified %s - accepting without verification", candidate)
                                verification_info["mailtester_code"] = "unavailable"
                                verification_info["dm_email_verified"] = "unknown"
                                return candidate, SOURCE_BLITZ_EMAIL, verification_info
                        # Either no candidate, validation disabled, or
                        # Mailtester rejected — log and fall through to
                        # WizLeads/BetterEnrich.
                        if candidate:
                            logger.info("Blitz returned unverified email %s but cascade continues to WizLeads/BetterEnrich", candidate)
                            verification_info["dm_email_verified"] = "no"
                        # Fall through to WizLeads step below.
            except Exception as e:
                _record_error("blitz", "person_enrich", e)
                logger.warning("Blitz person enrich failed for %s / %s: %s", full_name, domain, e)

        # Step 4: Blitz email from LinkedIn URL (SECONDARY PAID)
        # Fall back to LinkedIn if name+domain didn't find email, or if name not available
        if linkedin_url and not _should_skip_provider("blitz", force_provider):
            _record("blitz")
            try:
                result = await blitz_client.find_work_email(blitz_client_inst, linkedin_url)
                if result.get("found") and result.get("email"):
                    candidate = result["email"]
                    # Phase 2a: capture the Blitz find_work_email response.
                    # Stitch in identity from the input person so the
                    # payload has full_name + linkeded_url for lineage.
                    _capture("blitz", {
                        **result,
                        "full_name": full_name,
                        "first_name": first_name,
                        "last_name": last_name,
                        "linkedin_url": linkedin_url,
                        "email": candidate,
                    })
                    # Verify with Mailtester; only treat as final if confirmed
                    # or if Mailtester is unavailable. Otherwise fall through.
                    if validate_email:
                        try:
                            result_mt = await mailtester_client.verify_email(
                                blitz_client_inst, candidate
                            )
                            verification_info["dm_email_verified"] = "yes" if result_mt["valid"] else "no"
                            verification_info["mailtester_code"] = result_mt["code"]
                            verification_info["mailtester_message"] = result_mt["message"]
                            if result_mt["valid"]:
                                return candidate, SOURCE_BLITZ_EMAIL, verification_info
                            logger.info("Blitz find_work_email rejected by Mailtester: %s - falling through", candidate)
                        except RuntimeError:
                            verification_info["mailtester_code"] = "unavailable"
                            verification_info["dm_email_verified"] = "unknown"
                            return candidate, SOURCE_BLITZ_EMAIL, verification_info
                    else:
                        verification_info["dm_email_verified"] = "yes"
                        return candidate, SOURCE_BLITZ_EMAIL, verification_info
            except Exception as e:
                _record_error("blitz", "find_work_email", e)
                logger.warning("Blitz email lookup failed for %s: %s", linkedin_url, e)

        # Step 5: GetLeads person email (first + last + domain, batch-capable)
        # Inserted between Blitz and SmartProspect. Gates on firstName + lastName +
        # domain — decoupled from Blitz (mirrors the smartprospect capability
        # gate). The firstName/lastName can come from the input row, Contacts DB,
        # or Blitz.
        if full_name and domain and not _should_skip_provider("getleads", force_provider):
            first_name_gl = person.get("first_name", "") or (full_name.split(" ")[0] if full_name else "")
            last_name_gl = person.get("last_name", "") or (" ".join(full_name.split(" ")[1:]) if " " in full_name else "")
            if first_name_gl and last_name_gl:
                # Phase 5: when the domain orchestrator ran a batch pre-pass,
                # reuse its result instead of re-calling the API. The pre-resolved
                # check short-circuits ONLY the getleads step — Contacts DB
                # and Blitz above already ran, so free-tier emails still win.
                if pre_resolved_getleads is not None:
                    _record("getleads")
                    pre_email = pre_resolved_getleads.get("email", "")
                    if pre_email:
                        _capture("getleads", {
                            **pre_resolved_getleads,
                            "full_name": full_name,
                            "first_name": first_name_gl,
                            "last_name": last_name_gl,
                            "linkedin_url": linkedin_url,
                            "email": pre_email,
                        })
                        vs = pre_resolved_getleads.get("verification_status")
                        verification_info["dm_email_verified"] = "yes" if vs == "Valid" else "unknown"
                        # Phase 2: carry the GetLeads DM attributes through to
                        # _build_person_row (overlay, see getleads_dm there).
                        verification_info["getleads_dm"] = _getleads_dm_snapshot(pre_resolved_getleads)
                        logger.info("GetLeads (batch pre-pass) found email for %s: %s", full_name, pre_email)
                        return pre_email, SOURCE_GETLEADS, verification_info
                    # Batch tried but no email (Not Found / Invalid) — fall through
                    # to SmartProspect without re-calling the single endpoint.
                else:
                    _record("getleads")
                    try:
                        result = await getleads_client.find_email(
                            blitz_client_inst,
                            first_name=first_name_gl,
                            last_name=last_name_gl,
                            company_domain=domain,
                        )
                        if result and result.get("email"):
                            email = result["email"]
                            # Phase 2a: capture the GetLeads response.
                            _capture("getleads", {
                                **result,
                                "full_name": full_name,
                                "first_name": first_name_gl,
                                "last_name": last_name_gl,
                                "linkedin_url": linkedin_url,
                                "email": email,
                            })
                            # GetLeads reports verification_status. Trust "Valid",
                            # otherwise accept as "unknown" (mirrors SmartProspect /
                            # WizLeads catchall policy).
                            vs = result.get("verification_status")
                            if vs == "Valid":
                                verification_info["dm_email_verified"] = "yes"
                            else:
                                verification_info["dm_email_verified"] = "unknown"
                            # Phase 2: carry the GetLeads DM attributes through to
                            # _build_person_row (overlay, see getleads_dm there).
                            verification_info["getleads_dm"] = _getleads_dm_snapshot(result)
                            logger.info("GetLeads found email for %s: %s (verification_status: %s)",
                                        full_name, email, vs)
                            return email, SOURCE_GETLEADS, verification_info
                    except Exception as e:
                        _record_error("getleads", "find_email", e)
                        logger.warning("GetLeads lookup failed for %s / %s: %s", full_name, domain, e)

        # Step 6: SmartProspect person email (self-verifying, 30 RPS, batch-capable)
        # Inserted between GetLeads and WizLeads. Gates on firstName + lastName +
        # domain — decoupled from Blitz (per user requirement: "It does not
        # depend on blitz, it only depends on the input parameters"). The
        # firstName/lastName can come from the input row, Contacts DB, or Blitz.
        if full_name and domain and not _should_skip_provider("smartprospect", force_provider):
            first_name_sp = person.get("first_name", "") or (full_name.split(" ")[0] if full_name else "")
            last_name_sp = person.get("last_name", "") or (" ".join(full_name.split(" ")[1:]) if " " in full_name else "")
            if first_name_sp and last_name_sp:
                # Phase 5: when the domain orchestrator ran a batch pre-pass,
                # reuse its result instead of re-calling the API. The pre-resolved
                # check short-circuits ONLY the smartprospect step — Contacts DB
                # and Blitz above already ran, so free-tier emails still win.
                if pre_resolved_smartprospect is not None:
                    _record("smartprospect")
                    pre_email = pre_resolved_smartprospect.get("email", "")
                    if pre_email:
                        _capture("smartprospect", {
                            **pre_resolved_smartprospect,
                            "full_name": full_name,
                            "first_name": first_name_sp,
                            "last_name": last_name_sp,
                            "linkedin_url": linkedin_url,
                            "email": pre_email,
                        })
                        vs = pre_resolved_smartprospect.get("verification_status")
                        verification_info["dm_email_verified"] = "yes" if vs == "Valid" else "unknown"
                        logger.info("SmartProspect (batch pre-pass) found email for %s: %s", full_name, pre_email)
                        return pre_email, SOURCE_SMARTPROSPECT, verification_info
                    # Batch tried but no email (Not Found / Invalid) — fall through
                    # to WizLeads without re-calling the single endpoint.
                else:
                    _record("smartprospect")
                    try:
                        result = await smartprospect_client.find_email(
                            blitz_client_inst,
                            first_name=first_name_sp,
                            last_name=last_name_sp,
                            company_domain=domain,
                        )
                        if result and result.get("email"):
                            email = result["email"]
                            # Phase 2a: capture the SmartProspect response.
                            _capture("smartprospect", {
                                **result,
                                "full_name": full_name,
                                "first_name": first_name_sp,
                                "last_name": last_name_sp,
                                "linkedin_url": linkedin_url,
                                "email": email,
                            })
                            # SmartProspect self-verifies. Trust "Valid" status,
                            # otherwise accept as "unknown" (mirrors WizLeads catchall
                            # policy per user decision).
                            vs = result.get("verification_status")
                            if vs == "Valid":
                                verification_info["dm_email_verified"] = "yes"
                            else:
                                verification_info["dm_email_verified"] = "unknown"
                            logger.info("SmartProspect found email for %s: %s (verification_status: %s)",
                                        full_name, email, vs)
                            return email, SOURCE_SMARTPROSPECT, verification_info
                    except Exception as e:
                        _record_error("smartprospect", "find_email", e)
                        logger.warning("SmartProspect lookup failed for %s / %s: %s", full_name, domain, e)

        # Step 7: WizLeads person email (catchall verified, 10 RPS)
        # Inserted between SmartProspect and BetterEnrich per user-confirmed cascade order.
        if full_name and domain and not _should_skip_provider("wizleads", force_provider):
            _record("wizleads")
            first_name = person.get("first_name", "") or (full_name.split(" ")[0] if full_name else "")
            last_name = person.get("last_name", "") or (" ".join(full_name.split(" ")[1:]) if " " in full_name else "")
            try:
                result = await wizleads_client.find_email(
                    blitz_client_inst,
                    first_name=first_name,
                    last_name=last_name,
                    website=domain,
                )
                if result and result.get("email"):
                    email = result["email"]
                    # Phase 2a: capture the WizLeads response (already includes
                    # email; stitch in identity for lineage).
                    _capture("wizleads", {
                        **result,
                        "full_name": full_name,
                        "first_name": first_name,
                        "last_name": last_name,
                        "linkedin_url": linkedin_url,
                        "email": email,
                    })
                    verification_info["dm_email_verified"] = "yes"  # catchall verified by WizLeads
                    logger.info("WizLeads found email for %s: %s (catchall: %s)",
                                full_name, email, result.get("catchall"))
                    return email, SOURCE_WIZLEADS, verification_info
            except Exception as e:
                _record_error("wizleads", "find_email", e)
                logger.warning("WizLeads lookup failed for %s / %s: %s", full_name, domain, e)

        # Step 8: BetterEnrich person email (V3 with built-in verification)
        if full_name and domain and not _should_skip_provider("better_enrich", force_provider):
            _record("better_enrich")
            try:
                result = await better_enrich_client.find_work_email_v3(
                    blitz_client_inst, full_name, domain, linkedin_url
                )
                if result and result.get("email"):
                    email = result["email"]
                    # Phase 2a: capture the BetterEnrich V3 person-email
                    # response. Stitch identity for lineage.
                    _capture("better_enrich", {
                        **result,
                        "full_name": full_name,
                        "first_name": first_name,
                        "last_name": last_name,
                        "linkedin_url": linkedin_url,
                        "email": email,
                    })
                    # V3 provides email_status - map to dm_email_verified
                    email_status = result.get("email_status", "verified")
                    if email_status in ("verified", "valid"):
                        verification_info["dm_email_verified"] = "yes"
                    else:
                        verification_info["dm_email_verified"] = "unknown"
                    logger.info("BetterEnrich V3 found email for %s: %s (status: %s)", full_name, email, email_status)
                    return email, SOURCE_BETTER_ENRICH_PERSON, verification_info
            except Exception as e:
                _record_error("better_enrich", "find_work_email_v3", e)
                logger.warning("BetterEnrich V3 person lookup failed for %s / %s: %s", full_name, domain, e)

        # Step 9: Contacts DB by input row name + domain (if different from person name)
        # This handles edge cases where the input name differs from the person's current name
        if input_full_name and input_full_name != full_name and domain and not _should_skip_provider("contacts_db", force_provider):
            _record("contacts_db")
            try:
                contacts_data = await contacts_client.person_by_name_and_domain(
                    contacts_client_inst, input_full_name, domain
                )
                email = contacts_client.extract_email_from_contacts_response(contacts_data)
                # Phase 2a: capture the alternate-name Contacts DB lookup
                # (uses input_full_name rather than person.full_name).
                if contacts_data:
                    _cap_contact = dict(contacts_data)
                    _cap_contact.setdefault("full_name", input_full_name)
                    if email:
                        _cap_contact["email"] = email
                    _capture("contacts_db", _cap_contact)
                if email:
                    # Verify email with mailtester if enabled
                    if validate_email:
                        try:
                            result = await mailtester_client.verify_email(
                                blitz_client_inst, email
                            )
                            verification_info["dm_email_verified"] = "yes" if result["valid"] else "no"
                            verification_info["mailtester_code"] = result["code"]
                            verification_info["mailtester_message"] = result["message"]

                            if result["valid"]:
                                # Email is valid - use it
                                logger.debug("Email verified: %s (code: %s)", email, result["code"])
                                return email, SOURCE_CONTACTS_DB_EMAIL, verification_info
                            else:
                                # Email rejected. Only poison the Contacts DB
                                # row on hard-invalid (ko); policy-rejected codes
                                # (e.g. mb under the ok-only policy) skip the
                                # write so the row can resurface if the policy
                                # is later relaxed.
                                if result["code"] == "ko":
                                    logger.info("Email verification failed: %s (code: %s) - marking invalid", email, result["code"])
                                    await contacts_client.mark_email_invalid(
                                        contacts_client_inst,
                                        email=email,
                                        domain=domain,
                                    )
                                else:
                                    logger.info("Email rejected by policy: %s (code: %s) - skipping mark_email_invalid", email, result["code"])
                                # Continue to not found
                        except RuntimeError:
                            # Mailtester unavailable - FAIL OPEN
                            logger.warning("Mailtester unavailable for %s - accepting without verification", email)
                            verification_info["mailtester_code"] = "unavailable"
                            verification_info["dm_email_verified"] = "unknown"
                            return email, SOURCE_CONTACTS_DB_EMAIL, verification_info
                    else:
                        # Verification disabled - use email as-is
                        return email, SOURCE_CONTACTS_DB_EMAIL, verification_info
            except Exception as e:
                _record_error("contacts_db", "person_by_name_and_domain_input", e)
                logger.warning("Contacts DB input name lookup failed: %s", e)

        return "", SOURCE_NOT_FOUND, verification_info


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
    force_provider: Optional[str] = None,  # "contacts_db", "blitz", "better_enrich"
    validate_email: bool = True,  # NEW PARAMETER
    record_provider_use: Optional[Callable[[str], None]] = None,
    collector: Optional[Any] = None,  # RawContactCollector; Phase 1 capture
) -> list[OutputRow]:
    """
    Enrich a domain with decision-maker contacts.

    Args:
        force_provider: If set, only use that specific provider.
        validate_email: If True, verify Contacts DB emails with mailtester.
        record_provider_use: Optional callback invoked when a provider is
            *attempted* (regardless of success). Used to populate
            `jobs.used_providers` so the job summary shows which providers
            were tried.
        collector: Optional ``RawContactCollector``. When provided, every
            provider response at the company-level lookup step is captured
            BEFORE truncation to the user-facing cap. Phase 1 only.
    """
    logger.info("_enrich_domain called with force_provider=%s for domain=%s", force_provider, domain)
    # Honor a caller-provided company LinkedIn URL (e.g., from the unified API
    # `company_linkedin_url` field or Flow 1 `company_linkedin_col`). When set,
    # the domain → LinkedIn resolution steps below are skipped naturally
    # because the `if not company_linkedin_url:` guards short-circuit.
    company_linkedin_url = base_row.get("company_linkedin_url", "") or ""
    linkedin_source = ""

    # Pattern A: company-level fields extracted from the company lookup.
    # Populated into ``base_row`` so every output row for this domain
    # inherits them. Empty string when the provider returns nothing
    # (fail-open — never block the cascade).
    company_name_for_row = ""
    company_industry_for_row = ""
    company_employee_count_for_row = ""

    # Pattern C: per-row provider error list for company-level lookups.
    # Per-row, not per-job — this list is fresh per domain and gets
    # serialized into the row's ``provider_errors`` CSV column at build
    # time. Empty list → empty string in the CSV.
    company_provider_errors: list[dict[str, Any]] = []

    def _record_company_error(provider: str, method: str, error: str, error_type: str = "unknown") -> None:
        company_provider_errors.append({
            "provider": provider,
            "method": method,
            "error_type": error_type,
            "message": str(error)[:300],
        })

    def _record(provider: str) -> None:
        if record_provider_use is not None:
            try:
                record_provider_use(provider)
            except Exception as e:
                logger.warning("record_provider_use(%s) failed: %s", provider, e)

    # Determine if we should skip Contacts DB for contacts (not for company lookup)
    # Skip Contacts DB if custom cascade is provided (indicated by skip_contacts_db=True)
    # or if cascade is not the default
    use_custom_cascade = skip_contacts_db or cascade != blitz_client.DEFAULT_CASCADE

    async with domain_semaphore:
        # Step 1: domain → company LinkedIn URL (Contacts DB FIRST unless force_provider=blitz)
        if not _should_skip_provider("contacts_db", force_provider):
            _record("contacts_db")
            try:
                contacts_company = await contacts_client.company_by_domain(contacts_http, domain)
                if contacts_company:
                    # Pattern A: extract name / industry / employee_count from
                    # the Contacts DB company response. ``employee_count`` may
                    # be int or str — normalize to str for CSV.
                    company_name_for_row = contacts_company.get("name", "") or ""
                    company_industry_for_row = contacts_company.get("industry", "") or ""
                    _raw_count = contacts_company.get("employee_count", "")
                    company_employee_count_for_row = str(_raw_count) if _raw_count != "" and _raw_count is not None else ""
                    if contacts_company.get("linkedin_url"):
                        company_linkedin_url = contacts_company.get("linkedin_url", "")
                        linkedin_source = SOURCE_CONTACTS_DB_LINKEDIN
                        logger.debug("Found company LinkedIn via Contacts DB for %s: %s", domain, company_linkedin_url)
            except Exception as e:
                _record_company_error("contacts_db", "company_by_domain", str(e))
                logger.warning("Contacts DB company lookup failed for %s: %s", domain, e)

        # Fallback: Blitz API if Contacts DB didn't find it
        if not company_linkedin_url:
            _record("blitz")
            try:
                d2l = await blitz_client.domain_to_linkedin(blitz_http, domain)
                if d2l.get("found"):
                    company_linkedin_url = d2l.get("company_linkedin_url", "")
                    linkedin_source = SOURCE_BLITZ_LINKEDIN
                    logger.debug("Found company LinkedIn via Blitz API for %s: %s", domain, company_linkedin_url)
            except Exception as e:
                _record_company_error("blitz", "domain_to_linkedin", str(e))
                logger.warning("Blitz domain_to_linkedin failed for %s: %s", domain, e)

    # Pattern A: stamp the company-level fields onto base_row so every row
    # built from this domain inherits them. Mutate a copy (immutability
    # rule) so the caller's base_row is unchanged.
    base_row = {
        **base_row,
        "company_name": company_name_for_row,
        "company_industry": company_industry_for_row,
        "company_employee_count": company_employee_count_for_row,
    }

    if not company_linkedin_url:
        # No company LinkedIn found — try fallback if we have a name
        if full_name and not _should_skip_provider("contacts_db", force_provider):
            _record("contacts_db")
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
                _record_company_error("contacts_db", "person_by_name_and_domain_fallback", str(e))
                logger.warning("Contacts DB fallback for no-linkedin failed: %s", e)
        no_linkedin_out = _no_linkedin_row(base_row)
        _stamp_company_errors(no_linkedin_out, company_provider_errors)
        return [no_linkedin_out]

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
            _record("contacts_db")
            try:
                contacts_contacts = await contacts_client.company_contacts_enriched(
                    contacts_http, domain, limit=max_results
                )
                if contacts_contacts and len(contacts_contacts) > 0:
                    # Phase 1: capture every Contacts DB contact BEFORE the
                    # ``[:max_results]`` truncation below. The provider already
                    # capped the response server-side; this audit capture
                    # simply preserves anything that would be dropped on
                    # the way to the user's CSV.
                    if collector is not None:
                        for _cc in contacts_contacts:
                            try:
                                collector.capture_company_contact(
                                    source="contacts_db",
                                    domain=domain,
                                    company_linkedin_url=company_linkedin_url,
                                    contact=_cc,
                                )
                            except Exception as _cap_err:
                                logger.debug(
                                    "collector.capture_company_contact(contacts_db) failed for %s: %s",
                                    domain, _cap_err,
                                )
                    # Quality check: need at least 1 decision maker AND 1 email
                    emails_count = sum(1 for c in contacts_contacts if c.get("email"))
                    if len(contacts_contacts) >= 1 and emails_count >= 1:
                        # Convert Contacts DB format to Blitz format for compatibility
                        persons = []
                        for contact in contacts_contacts[:max_results]:
                            person_dict = {
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
                                "icp": 0,  # Contacts DB doesn't provide ICP scoring
                            }
                            persons.append(person_dict)

                        contacts_db_quality_met = True
                        logger.debug("Using Contacts DB for %d decision makers from %s", len(persons), domain)
            except Exception as e:
                _record_company_error("contacts_db", "company_contacts_enriched", str(e))
                logger.warning("Contacts DB contacts lookup failed for %s: %s", domain, e)

        # Fallback: Blitz API if Contacts DB didn't meet quality threshold
        if not contacts_db_quality_met and not _should_skip_provider("blitz", force_provider):
            _record("blitz")
            try:
                icp_result = await blitz_client.waterfall_icp_search(
                    blitz_http, company_linkedin_url, cascade, max_results
                )
                persons = icp_result.get("results", [])
                # Phase 1: capture every Blitz contact. Blitz already
                # truncated server-side to ``max_results``; we keep all of
                # them in the audit surface even if the cascade later
                # discards some (e.g., email-resolution drops a person).
                if collector is not None and persons:
                    for _blitz_person in persons:
                        try:
                            collector.capture_company_contact(
                                source="blitz",
                                domain=domain,
                                company_linkedin_url=company_linkedin_url,
                                contact=_blitz_person,
                            )
                        except Exception as _cap_err:
                            logger.debug(
                                "collector.capture_company_contact(blitz) failed for %s: %s",
                                domain, _cap_err,
                            )
                logger.debug("Using Blitz API for %d decision makers from %s", len(persons), domain)
            except Exception as e:
                _record_company_error("blitz", "waterfall_icp_search", str(e))
                logger.warning("Blitz waterfall_icp_search failed for %s: %s", company_linkedin_url, e)
                err_out = _error_row(base_row, company_linkedin_url)
                _stamp_company_errors(err_out, company_provider_errors)
                return [err_out]

    if not persons:
        # No decision makers found — try BetterEnrich for generic company email
        if not _should_skip_provider("better_enrich", force_provider):
            _record("better_enrich")
            try:
                be_result = await better_enrich_client.find_company_email(
                    blitz_http,
                    website=domain,
                )
                if be_result and be_result.get("email"):
                    logger.info("BetterEnrich company email found for %s: %s", domain, be_result.get("email"))
                    # Phase 2b: capture company email for Contacts DB write-back.
                    # Uses capture_company_email() so the payload routes to
                    # company_email field (NEVER overwrites a person email).
                    if collector is not None:
                        try:
                            collector.capture_company_email(
                                source="better_enrich",
                                domain=domain,
                                company_linkedin_url=company_linkedin_url,
                                email_data=be_result,
                            )
                        except Exception as _cap_err:
                            logger.debug(
                                "collector.capture_company_email failed for %s: %s",
                                domain, _cap_err,
                            )
                    return [_company_email_row(
                        base_row,
                        company_linkedin_url,
                        be_result.get("email", ""),
                        SOURCE_BETTER_ENRICH_COMPANY,
                    )]
            except Exception as e:
                _record_company_error("better_enrich", "find_company_email", str(e))
                logger.debug("BetterEnrich company email lookup failed for %s: %s", domain, e)

        no_contacts_out = _no_contacts_row(base_row, company_linkedin_url)
        _stamp_company_errors(no_contacts_out, company_provider_errors)
        return [no_contacts_out]

    # Step 2.5: Phase 5 — SmartProspect batch pre-pass.
    #
    # If smartprospect is enabled, no force_provider is set, and we have
    # 2+ decision makers with first+last+domain, resolve them in a single
    # find_emails_batch() call (chunks of 10 internally). Each pre-resolved
    # email is passed down to Step 5 of the per-row cascade so it skips the
    # single-call API hit. Contacts DB and Blitz (free + paid upstream tiers)
    # still run first in the per-row cascade — free-tier emails win when
    # available. Batch only replaces the smartprospect API call.
    #
    # Safety: every guard here is additive. If anything looks off, the batch
    # is skipped and the per-row cascade runs normally for everyone.
    pre_resolved_by_index: dict[int, dict[str, Any]] = {}
    batch_attempted = False
    batch_found = 0
    # Phase 3 (batch coverage): the pre-pass no longer requires a collector —
    # the batch CALL + pre_resolved handoff run for every caller (including
    # POST /enrich domain_only and GET /enrich/{domain}, which pass no
    # collector); only the collector capture below is collector-gated.
    if (
        not force_provider
        and not _should_skip_provider("smartprospect", force_provider)
        and len(persons) >= 2
    ):
        # Build the batch input list — only persons with first+last+domain.
        batch_inputs: list[tuple[int, str, str]] = []  # (person_idx, first, last)
        for idx, item in enumerate(persons):
            p = item.get("person", {}) if isinstance(item, dict) else {}
            if not isinstance(p, dict):
                continue
            p_full = p.get("full_name") or ""
            p_first = p.get("first_name") or (p_full.split(" ")[0] if p_full else "")
            p_last = p.get("last_name") or (
                " ".join(p_full.split(" ")[1:]) if " " in p_full else ""
            )
            if p_first and p_last and domain:
                batch_inputs.append((idx, p_first, p_last))

        if len(batch_inputs) >= 2:
            batch_attempted = True
            contacts_payload = [
                {"firstName": first, "lastName": last, "companyDomain": domain}
                for _, first, last in batch_inputs
            ]
            # record_provider_use fires once for the whole batch (matches the
            # semantic "smartprospect was attempted on this job"). Per-row
            # Step 5 calls _record("smartprospect") only when it actually
            # consumes a pre-resolved result OR falls back to single-call.
            if record_provider_use is not None:
                try:
                    record_provider_use("smartprospect")
                except Exception:
                    pass
            try:
                batch_results = await smartprospect_client.find_emails_batch(
                    blitz_http, contacts_payload
                )
            except Exception as batch_exc:
                logger.warning(
                    "SmartProspect batch pre-pass failed for %s: %s — falling back to per-row cascade",
                    domain, batch_exc,
                )
                batch_results = []

            # Map results back to person indices. find_emails_batch preserves
            # input order and pads short responses with Not Found, so the zip
            # is safe even when the API returns fewer entries than input.
            for (idx, first, last), result in zip(batch_inputs, batch_results):
                if not isinstance(result, dict):
                    continue
                email = result.get("email", "")
                if email:
                    pre_resolved_by_index[idx] = result
                    batch_found += 1
                    # Phase 5 capture: write the batch-resolved email to the
                    # collector so it drains to Contacts DB at job end. Same
                    # shape as the per-row smartprospect capture. No-ops when
                    # the caller wired no collector (Phase 3 relaxed the gate).
                    if collector is not None:
                        try:
                            p = persons[idx].get("person", {}) if isinstance(persons[idx], dict) else {}
                            collector.capture_company_contact(
                                source="smartprospect",
                                domain=domain,
                                company_linkedin_url=company_linkedin_url,
                                contact={
                                    **result,
                                    "full_name": p.get("full_name", ""),
                                    "first_name": first,
                                    "last_name": last,
                                    "linkedin_url": p.get("linkedin_url", ""),
                                    "email": email,
                                },
                            )
                        except Exception as cap_err:
                            logger.debug(
                                "collector.capture_company_contact(smartprospect batch) failed for %s: %s",
                                domain, cap_err,
                            )

            logger.info(
                "SmartProspect batch pre-pass for %s: %d/%d resolved (attempted=%s)",
                domain, batch_found, len(batch_inputs), batch_attempted,
            )

    # Step 2.6: Phase 5 — GetLeads batch pre-pass.
    #
    # Mirror of the SmartProspect pre-pass above, but for GetLeads (runs BEFORE
    # SmartProspect in the per-row cascade). If getleads is enabled, no
    # force_provider is set, and we have 2+ decision makers with first+last+
    # domain, resolve them in a single find_emails_batch() call (chunks of 100
    # internally). Each pre-resolved email is passed down to Step 5 of the
    # per-row cascade so it skips the single-call API hit. Contacts DB and
    # Blitz (free + paid upstream tiers) still run first in the per-row cascade
    # — free-tier emails win when available. Batch only replaces the getleads
    # API call.
    #
    # Safety: every guard here is additive. If anything looks off, the batch
    # is skipped and the per-row cascade runs normally for everyone.
    pre_resolved_getleads_by_index: dict[int, dict[str, Any]] = {}
    getleads_batch_attempted = False
    getleads_batch_found = 0
    # Phase 3 (batch coverage): the pre-pass no longer requires a collector —
    # the batch CALL + pre_resolved handoff run for every caller (including
    # POST /enrich domain_only and GET /enrich/{domain}, which pass no
    # collector); only the collector capture below is collector-gated.
    if (
        not force_provider
        and not _should_skip_provider("getleads", force_provider)
        and len(persons) >= 2
    ):
        # Build the batch input list — only persons with first+last+domain.
        getleads_batch_inputs: list[tuple[int, str, str]] = []  # (person_idx, first, last)
        for idx, item in enumerate(persons):
            p = item.get("person", {}) if isinstance(item, dict) else {}
            if not isinstance(p, dict):
                continue
            p_full = p.get("full_name") or ""
            p_first = p.get("first_name") or (p_full.split(" ")[0] if p_full else "")
            p_last = p.get("last_name") or (
                " ".join(p_full.split(" ")[1:]) if " " in p_full else ""
            )
            if p_first and p_last and domain:
                getleads_batch_inputs.append((idx, p_first, p_last))

        if len(getleads_batch_inputs) >= 2:
            getleads_batch_attempted = True
            getleads_contacts_payload = [
                {"firstName": first, "lastName": last, "companyDomain": domain}
                for _, first, last in getleads_batch_inputs
            ]
            # record_provider_use fires once for the whole batch (matches the
            # semantic "getleads was attempted on this job"). Per-row Step 5
            # calls _record("getleads") only when it actually consumes a
            # pre-resolved result OR falls back to single-call.
            if record_provider_use is not None:
                try:
                    record_provider_use("getleads")
                except Exception:
                    pass
            try:
                getleads_batch_results = await getleads_client.find_emails_batch(
                    blitz_http, getleads_contacts_payload
                )
            except Exception as batch_exc:
                logger.warning(
                    "GetLeads batch pre-pass failed for %s: %s — falling back to per-row cascade",
                    domain, batch_exc,
                )
                getleads_batch_results = []

            # Map results back to person indices. find_emails_batch preserves
            # input order and pads short responses with Not Found, so the zip
            # is safe even when the API returns fewer entries than input.
            for (idx, first, last), result in zip(getleads_batch_inputs, getleads_batch_results):
                if not isinstance(result, dict):
                    continue
                email = result.get("email", "")
                if email:
                    pre_resolved_getleads_by_index[idx] = result
                    getleads_batch_found += 1
                    # Phase 5 capture: write the batch-resolved email to the
                    # collector so it drains to Contacts DB at job end. Same
                    # shape as the per-row getleads capture. No-ops when
                    # the caller wired no collector (Phase 3 relaxed the gate).
                    if collector is not None:
                        try:
                            p = persons[idx].get("person", {}) if isinstance(persons[idx], dict) else {}
                            collector.capture_company_contact(
                                source="getleads",
                                domain=domain,
                                company_linkedin_url=company_linkedin_url,
                                contact={
                                    **result,
                                    "full_name": p.get("full_name", ""),
                                    "first_name": first,
                                    "last_name": last,
                                    "linkedin_url": p.get("linkedin_url", ""),
                                    "email": email,
                                },
                            )
                        except Exception as cap_err:
                            logger.debug(
                                "collector.capture_company_contact(getleads batch) failed for %s: %s",
                                domain, cap_err,
                            )

            logger.info(
                "GetLeads batch pre-pass for %s: %d/%d resolved (attempted=%s)",
                domain, getleads_batch_found, len(getleads_batch_inputs), getleads_batch_attempted,
            )

    # Step 3: resolve email for each person concurrently.
    #
    # Phase 5: pass pre_resolved_smartprospect + pre_resolved_getleads (if any)
    # so Steps 5/6 of the per-row cascade reuse the batch results instead of
    # re-calling the API. Per-row Contacts DB + Blitz still run first (free
    # tier can override).
    tasks = [
        _resolve_email_for_person(
            blitz_http,
            contacts_http,
            item.get("person", {}),
            domain,
            full_name,
            email_semaphore,
            force_provider=force_provider,
            validate_email=validate_email,
            record_provider_use=record_provider_use,
            collector=collector,
            company_linkedin_url=company_linkedin_url,
            pre_resolved_smartprospect=pre_resolved_by_index.get(idx),
            pre_resolved_getleads=pre_resolved_getleads_by_index.get(idx),
        )
        for idx, item in enumerate(persons)
    ]
    email_results = await asyncio.gather(*tasks)

    output_rows: list[OutputRow] = []
    for item, (email, source, verification_info) in zip(persons, email_results):
        person = item.get("person", {})
        icp_tier = item.get("icp", 0)
        row = _build_person_row(
            base_row, company_linkedin_url, person, icp_tier, email, source, verification_info
        )
        # Pattern C: merge company-level errors into the per-row error list.
        # Person-level errors come from ``verification_info["provider_errors"]``
        # (set inside ``_build_person_row``); company-level errors come from
        # ``company_provider_errors``. We dedupe by (provider, method) so a
        # provider that failed both at the company and person level doesn't
        # appear twice unless the messages differ.
        _merge_company_errors_into_row(row, company_provider_errors)
        output_rows.append(row)

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
    check_cancelled: Optional[Callable[[str], bool]] = None,
    validate_email: bool = True,  # NEW PARAMETER
    linkedin_url_col: Optional[str] = None,
    phone_col: Optional[str] = None,
    company_name_col: Optional[str] = None,
    existing_email_col: Optional[str] = None,
    facebook_url_col: Optional[str] = None,
    force_provider: Optional[str] = None,
    record_provider_use: Optional[Callable[[str], None]] = None,
    use_email_cache: bool = True,
    collector: Optional[Any] = None,  # RawContactCollector; Phase 1 capture
) -> list[OutputRow]:
    """
    Runs the full pipeline over all rows.
    Calls on_progress(event_dict) after each domain is processed.
    Returns the list of all output rows.

    If write_incremental=True, writes results to CSV as they are processed
    for partial download support.

    If cancelled_jobs is provided, checks if job_id is in the set and stops processing.
    If check_cancelled is provided, calls it with job_id to check database status.

    The check_cancelled function should return True if job was cancelled or abandoned.
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

    # Per-job dedupe for company / Facebook page fallbacks. The first row
    # that needs the value per normalized_domain / normalized_facebook_url
    # makes the API call; subsequent rows reuse the cached result.
    company_fallback_dedupe = company_fallback.CompanyFallbackDedupe()

    # Persistent email cache on /mnt/disk/ — survives DB wipes and lets a
    # resume recover already-resolved rows even if checkpoints are gone.
    # If we have a parent job_id, open that one instead of starting fresh,
    # so the user benefits from emails found in any previous run.
    # Tests can pass use_email_cache=False to opt out (otherwise the
    # persistent cache will mask their work).
    email_cache_conn = None
    if job_id and use_email_cache:
        try:
            from . import email_cache
            cache_job_id = job_id
            # If this run is a child of a parent that already built up
            # a cache, share the parent's cache.
            try:
                from shared import db as _db
                parent_row = _db.get_db().execute(
                    "SELECT parent_job_id FROM jobs WHERE job_id=?",
                    (job_id,),
                ).fetchone()
                if parent_row and parent_row["parent_job_id"]:
                    parent_cache_path = email_cache.get_cache_path(parent_row["parent_job_id"])
                    if Path(parent_cache_path).exists():
                        cache_job_id = parent_row["parent_job_id"]
            except Exception as e:
                logger.debug("Could not check parent cache: %s", e)
            email_cache_conn = email_cache.open_cache(cache_job_id)
            logger.info("Opened email cache at %s (job_id=%s)",
                        email_cache.get_cache_path(cache_job_id), cache_job_id)
        except Exception as e:
            logger.warning("Could not open email cache: %s", e)
            email_cache_conn = None

    all_output: list[OutputRow] = []
    total = len(rows)

    # Per-job audit collection. Each input row gets one record; written to
    # a JSONL sidecar at the end of the run.
    audit_records: list[dict[str, Any]] = []
    audit_lock = asyncio.Lock()

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
        # Check if job was cancelled (both in-memory set AND database check)
        is_cancelled = False
        cancel_reason = "unknown"
        if cancelled_jobs and job_id and job_id in cancelled_jobs:
            is_cancelled = True
            cancel_reason = "cancelled by user"
        elif check_cancelled and job_id and check_cancelled(job_id):
            is_cancelled = True
            # Determine reason
            from shared import db as shared_db
            from shared.job_store_base import JobStoreBase
            check_store = JobStoreBase(shared_db.get_db())
            job = check_store.get_job(job_id)
            if job:
                if job.get("status") == "abandoned":
                    cancel_reason = "abandoned due to server restart"
                elif job.get("status") == "cancelled":
                    cancel_reason = "cancelled by user"
                else:
                    cancel_reason = f"cancelled (status: {job.get('status')})"
            else:
                cancel_reason = "cancelled"

        if is_cancelled:
            logger.info("Job %s %s, stopping processing at row %d", job_id, cancel_reason, idx)
            if "abandoned" in cancel_reason:
                raise RuntimeError(f"Job {job_id} was abandoned due to server restart. Please retry.")
            elif "user" in cancel_reason:
                raise RuntimeError(f"Job {job_id} was cancelled by user")
            else:
                raise RuntimeError(f"Job {job_id} was {cancel_reason}")

        # Normalize at the batch edge too (deep URLs / emails -> bare domain or "").
        # Same normalize_domain() used by the /enrich handlers; prevents raw URLs
        # from reaching providers (Blitz 422, contacts 404) on CSV batch paths.
        domain = identifier_utils.normalize_domain(str(row.get(domain_col, "")))

        # Resolve full name from available columns
        if name_col and row.get(name_col):
            full_name = str(row[name_col]).strip()
        elif first_name_col or last_name_col:
            first = str(row.get(first_name_col or "", "")).strip()
            last = str(row.get(last_name_col or "", "")).strip()
            full_name = f"{first} {last}".strip()
        else:
            full_name = ""

        # Persistent email cache on /mnt/disk/ — keyed by (domain, name).
        # If a previous run already found an email for this pair, short-
        # circuit the row and reuse the enriched payload. Survives DB wipes
        # so a user can resume even if the jobs table is gone.
        if email_cache_conn is not None and domain:
            cached = email_cache.lookup(
                email_cache_conn, domain, full_name
            )
            if cached is None and not full_name:
                # Try domain-only for company-email fallback
                cached = email_cache.lookup_domain_only(email_cache_conn, domain)
            if cached is not None:
                result_rows = [dict(cached["enriched_row"])]
                # Update row_status to indicate cache hit so the UI can
                # show the user how many rows were recovered.
                result_rows[0]["row_status"] = "cache_hit"
                result_rows[0]["no_email_reason"] = cached.get("no_email_reason", "cache_hit")
                return result_rows

        # Build per-row identifier payload (used downstream for visibility/debug)
        input_payload = identifier_utils.build_row_identifier_payload(
            row,
            domain_col=domain_col,
            name_col=name_col,
            first_name_col=first_name_col,
            last_name_col=last_name_col,
            linkedin_url_col=linkedin_url_col,
            phone_col=phone_col,
            company_name_col=company_name_col,
            existing_email_col=existing_email_col,
            facebook_url_col=facebook_url_col,
        )
        # Prefer normalized full_name we already computed (avoids whitespace-only issue)
        input_payload["full_name"] = full_name or input_payload["full_name"]

        # Use the routing function to decide the enrichment path.
        route = route_enrichment(
            linkedin_url=input_payload.get("normalized_linkedin_url") or "",
            phone=input_payload.get("phone") or "",
            full_name=input_payload.get("full_name") or "",
            first_name=input_payload.get("first_name") or "",
            last_name=input_payload.get("last_name") or "",
            domain=domain,
            company_name=input_payload.get("company_name") or "",
            force_provider=force_provider,
        )

        # If we have a strong identifier (linkedin, phone, or name+domain), route to the single-person path
        # instead of the large decision-maker cascade.
        use_routing = route.get("mode") not in ("invalid", "domain_only", "")
        no_email_reason = route.get("no_email_reason", "")

        if not domain and not use_routing:
            result_rows = [_error_row(row)]
            result_rows[0]["row_status"] = "skipped_no_domain"
            result_rows[0]["no_email_reason"] = (
                route.get("no_email_reason") or NO_EMAIL_REASON_MISSING_REQUIRED_INPUT
            )
            # No providers were attempted at all.
            result_rows[0]["final_email_status"] = "not_run"
            result_rows[0]["final_email_verification_source"] = ""
            result_rows[0]["input_fields_used"] = input_payload.get("input_fields_used", "")
            # Add a synthetic skipped attempt so the row has audit data.
            synth_attempt = _build_provider_attempt(
                job_id=job_id,
                row_index=idx,
                domain=domain,
                normalized_linkedin_url=input_payload.get("normalized_linkedin_url", ""),
                provider="",
                method="",
                input_type_used="",
                called=False,
                skipped_reason=result_rows[0]["no_email_reason"],
                status="not_run",
                email_found=False,
                error_type="",
                latency_ms=0,
            )
            result_rows[0]["provider_attempts"] = ""
            result_rows[0]["provider_attempts_json"] = json.dumps([synth_attempt], ensure_ascii=False)
            result_rows[0]["providers_called"] = ""
            result_rows[0]["providers_skipped"] = json.dumps([{
                "provider": "", "method": "", "skipped_reason": result_rows[0]["no_email_reason"],
            }], ensure_ascii=False)
        elif use_routing and not no_email_reason:
            # Run the routed step.
            route_result = await run_enrichment_route(
                route,
                blitz_http,
                contacts_http,
                email_semaphore,
                validate_email=validate_email,
                job_id=job_id or "",
                row_index=idx,
                emit_logs=True,
                record_provider_use=record_provider_use,
            )
            # Build a person row from the route result.
            row_status = STATUS_ENRICHED if route_result.get("email") else STATUS_NO_CONTACTS
            result_rows = [_empty_enriched()]
            result_rows[0]["input_domain"] = domain
            result_rows[0]["input_full_name"] = input_payload.get("full_name", "")
            result_rows[0]["input_linkedin_url"] = input_payload.get("input_linkedin_url", "")
            result_rows[0]["input_phone"] = input_payload.get("input_phone", "")
            result_rows[0]["input_company_name"] = input_payload.get("input_company_name", "")
            result_rows[0]["input_existing_email"] = input_payload.get("input_existing_email", "")
            result_rows[0]["normalized_linkedin_url"] = input_payload.get("normalized_linkedin_url", "")
            result_rows[0]["linkedin_username"] = input_payload.get("linkedin_username", "")
            result_rows[0]["input_fields_used"] = input_payload.get("input_fields_used", "")
            # Copy routing diagnostics.
            result_rows[0]["dm_email"] = route_result.get("email", "")
            result_rows[0]["dm_email_source"] = route_result.get("source", "")
            result_rows[0]["source_path"] = route_result.get("source_path", "")
            result_rows[0]["provider_attempts"] = ",".join(route_result.get("provider_attempts", []))
            attempts_json = route_result.get("provider_attempts_json") or []
            # If the attempts JSON is too large, embed a compact version and let
            # the JSONL sidecar carry the full record.
            if _attempts_json_size(attempts_json) > AUDIT_JSONL_COMPACT_THRESHOLD_BYTES:
                result_rows[0]["provider_attempts_json"] = json.dumps(
                    _attempts_json_compact(attempts_json), ensure_ascii=False
                )
            else:
                result_rows[0]["provider_attempts_json"] = json.dumps(
                    attempts_json, ensure_ascii=False
                )
            result_rows[0]["providers_called"] = json.dumps(
                route_result.get("providers_called", []), ensure_ascii=False
            )
            result_rows[0]["providers_skipped"] = json.dumps(
                route_result.get("providers_skipped", []), ensure_ascii=False
            )
            result_rows[0]["no_email_reason"] = route_result.get("no_email_reason", "")
            result_rows[0]["final_email_status"] = route_result.get("final_email_status", "")
            result_rows[0]["final_email_verification_source"] = route_result.get(
                "final_email_verification_source", ""
            )
            result_rows[0]["row_status"] = row_status
            verif = route_result.get("verification") or {}
            result_rows[0]["dm_email_verified"] = verif.get("dm_email_verified", "unknown")
            result_rows[0]["mailtester_code"] = verif.get("mailtester_code", "")
            result_rows[0]["mailtester_message"] = verif.get("mailtester_message", "")
        else:
            # Legacy domain-only cascade.
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
                validate_email=validate_email,
                force_provider=force_provider,
                record_provider_use=record_provider_use,
                collector=collector,
            )
            # Add a row-level audit stub for the legacy cascade. Per-DM attempts
            # aren't individually tracked here; we report the row's outcome.
            for r in result_rows:
                if not r.get("source_path"):
                    r["source_path"] = "domain -> decision_maker_cascade"
                if not r.get("provider_attempts_json"):
                    r["provider_attempts_json"] = "[]"
                if not r.get("providers_called"):
                    r["providers_called"] = "[]"
                if not r.get("providers_skipped"):
                    r["providers_skipped"] = "[]"
                if not r.get("final_email_status"):
                    r["final_email_status"] = (
                        "enriched" if r.get("dm_email") else "not_found"
                    )
                if not r.get("final_email_verification_source") and r.get("dm_email"):
                    r["final_email_verification_source"] = (
                        "mailtester" if r.get("dm_email_verified") in ("yes", "no")
                        else "provider_self"
                    )

        # Attach input_* columns to every result row for visibility.
        for r in result_rows:
            identifier_utils.attach_input_columns(r, input_payload)
            # Ensure routing diagnostics are on the result (they were added in the routed case).
            if use_routing and not no_email_reason:
                # If we used routing, we already set these. but in domain-only case,
                # we still need to attach the no_email_reason from routing.
                r["no_email_reason"] = r.get("no_email_reason") or no_email_reason

        # Company / page-level fallbacks (BetterEnrich Facebook + company
        # email). Run only after the person-level cascade has had its turn.
        # Mutates each result row in place to add company_email* / final_email*.
        facebook_url = (input_payload.get("facebook_url") or "").strip()
        await _maybe_apply_company_fallbacks(
            blitz_http,
            result_rows,
            domain=domain,
            facebook_url=facebook_url,
            source_path_prefix=(result_rows[0].get("source_path", "") if result_rows else ""),
            validate_email=validate_email,
            dedupe=company_fallback_dedupe,
            record_provider_use=record_provider_use,
            collector=collector,
        )

        # Collect source counts from this row's results
        source_counts: dict[str, int] = {}
        for r in result_rows:
            source = r.get("dm_email_source", "")
            if source:
                provider = _normalize_source(source)
                source_counts[provider] = source_counts.get(provider, 0) + 1

        emails_found = sum(1 for r in result_rows if r.get("dm_email"))
        await on_progress(
            {
                "index": idx,
                "total": total,
                "domain": domain,
                "status": result_rows[0].get("row_status", STATUS_ERROR),
                "contacts_found": len(result_rows),
                "emails_found": emails_found,
                "source_counts": source_counts,
            }
        )

        # Build and collect the audit record for this input row.
        # Use the first result row's diagnostic fields; if multi-row (legacy
        # cascade), the sidecar contains one audit per input row.
        primary = result_rows[0] if result_rows else {}
        try:
            attempts_list = json.loads(primary.get("provider_attempts_json", "") or "[]")
        except Exception:
            attempts_list = []
        audit = _row_audit_record(
            job_id=job_id or "",
            row_index=idx,
            domain=domain,
            input_fields_used=input_payload.get("input_fields_used", ""),
            input_payload=input_payload,
            source_path=primary.get("source_path", ""),
            no_email_reason=primary.get("no_email_reason", ""),
            final_email_status=primary.get("final_email_status", ""),
            final_email_verification_source=primary.get("final_email_verification_source", ""),
            attempts=attempts_list,
        )
        async with audit_lock:
            audit_records.append(audit)

        # Persist the successful row to the email cache so future runs can
        # skip it. We do this *after* the audit so the audit log always
        # reflects work that actually happened, never cache hits.
        if email_cache_conn is not None and domain:
            for r in result_rows:
                try:
                    email_cache.store(
                        email_cache_conn,
                        domain,
                        r,
                        full_name=r.get("input_full_name", "") or full_name,
                    )
                except Exception as e:
                    logger.debug("Email cache write failed for %s: %s", domain, e)

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

    # Close email cache (flush + commit)
    if email_cache_conn is not None:
        try:
            from . import email_cache as _email_cache
            _email_cache.close_cache()
        except Exception:
            pass

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
            error_row["no_email_reason"] = "exception"
            error_row["final_email_status"] = "error"
            error_row["provider_attempts_json"] = "[]"
            error_row["providers_called"] = "[]"
            error_row["providers_skipped"] = "[]"
            all_output.append(error_row)
        else:
            # Normal case: result is a list of OutputRow objects
            all_output.extend(result)

    # Write the per-job JSONL audit sidecar (one record per input row).
    if audit_records:
        try:
            write_audit_sidecar(job_id or "", audit_records)
        except Exception as e:
            logger.warning("Failed to write audit sidecar for job %s: %s", job_id, e)

    if exception_count > 0:
        logger.warning(
            "Pipeline completed with %d row errors out of %d total rows (%.1f%% success rate)",
            exception_count, len(rows), (len(rows) - exception_count) / len(rows) * 100
        )

    return all_output
