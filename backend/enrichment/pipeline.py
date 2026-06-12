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
import logging
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
from . import identifier_utils

logger = logging.getLogger(__name__)


# Valid provider values for force_provider parameter
VALID_PROVIDERS = frozenset({"contacts_db", "blitz", "wizleads", "better_enrich"})


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
        result = provider != force_provider
        logger.debug("_should_skip_provider(provider=%s, force_provider=%s) = %s", provider, force_provider, result)
        return result

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
SOURCE_WIZLEADS = "wizleads_email"                        # Person email from WizLeads

# Routing reasons (no_email_reason values)
NO_EMAIL_REASON_LINKEDIN_PARSE_FAILED = "linkedin_parse_failed"
NO_EMAIL_REASON_PHONE_REVERSE_UNAVAILABLE = "phone_reverse_unavailable"
NO_EMAIL_REASON_FORCED_PROVIDER_CANNOT_USE_INPUT = "forced_provider_cannot_use_input"
NO_EMAIL_REASON_NO_IDENTIFIERS = "no_identifiers"
NO_EMAIL_REASON_DOMAIN_ONLY_NO_CONTACTS = "domain_only_no_contacts"

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
ROUTE_PROVIDER_WIZLEADS = "wizleads"
ROUTE_PROVIDER_BETTER_ENRICH = "better_enrich"

# Provider methods used in source_path.
ROUTE_METHOD_PERSON_BY_LINKEDIN = "person_by_linkedin"
ROUTE_METHOD_PERSON_ENRICH_BY_LINKEDIN = "person_enrich_by_linkedin"
ROUTE_METHOD_FIND_WORK_EMAIL = "find_work_email"
ROUTE_METHOD_PERSON_BY_NAME_DOMAIN = "person_by_name_and_domain"
ROUTE_METHOD_PERSON_ENRICH = "person_enrich"
ROUTE_METHOD_FIND_EMAIL = "find_email"
ROUTE_METHOD_FIND_WORK_EMAIL_V3 = "find_work_email_v3"
ROUTE_METHOD_PHONE_REVERSE_LOOKUP = "phone_reverse_lookup"

ENRICHED_COLUMNS = [
    "company_linkedin_url",
    "dm_first_name",
    "dm_last_name",
    "dm_full_name",
    "dm_title",
    "dm_linkedin_url",
    "dm_email",
    "dm_email_source",
    "dm_email_verified",
    "mailtester_code",
    "mailtester_message",
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
    "normalized_linkedin_url",
    "linkedin_username",
    "input_fields_used",
    # Routing diagnostics
    "source_path",
    "provider_attempts",
    "no_email_reason",
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
    if method == ROUTE_METHOD_FIND_WORK_EMAIL_V3:
        return bool(fn) and bool(dom)
    if method == ROUTE_METHOD_PHONE_REVERSE_LOOKUP:
        return bool(phone)
    return False


def _method_is_paid(method: str) -> bool:
    """Return True if the method calls a paid provider family."""
    return _provider_label(method) in (ROUTE_PROVIDER_BLITZ, ROUTE_PROVIDER_BETTER_ENRICH, ROUTE_PROVIDER_WIZLEADS)


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
         (contacts_db -> blitz -> wizleads -> better_enrich).
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
      * "wizleads": keep only wizleads steps.
      * "better_enrich": keep only better_enrich steps.
    """
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
) -> dict[str, Any]:
    """Execute a single routed step. Returns one of:

        {"email": "x@y.com", "source": "<SOURCE_*>", "verification": {...}}
        {"phone_reverse": "https://linkedin.com/in/..."}  # phone hop
        {"email": "", "source": SOURCE_NOT_FOUND}
    """
    from . import mailtester_client as _mt
    from . import contacts_client as _cc
    from . import blitz_client as _bc
    from . import wizleads_client as _wl
    from . import better_enrich_client as _be

    verification: dict[str, Any] = {
        "dm_email_verified": "unknown",
        "mailtester_code": "",
        "mailtester_message": "",
    }

    async with email_semaphore:
        if method == ROUTE_METHOD_PERSON_BY_LINKEDIN:
            try:
                data = await _cc.person_by_linkedin(
                    contacts_http, inputs["linkedin_url"]
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
            try:
                result = await _bc.person_enrich_by_linkedin(
                    blitz_http, inputs["linkedin_url"]
                )
            except Exception as e:
                logger.warning("Blitz person_enrich_by_linkedin failed: %s", e)
                return {"email": "", "source": SOURCE_NOT_FOUND}
            if result.get("found") and result.get("email"):
                verification["dm_email_verified"] = "yes"
                return {"email": result["email"], "source": SOURCE_BLITZ_EMAIL, "verification": verification}
            return {"email": "", "source": SOURCE_NOT_FOUND}

        if method == ROUTE_METHOD_FIND_WORK_EMAIL:
            try:
                result = await _bc.find_work_email(blitz_http, inputs["linkedin_url"])
            except Exception as e:
                logger.warning("Blitz find_work_email failed: %s", e)
                return {"email": "", "source": SOURCE_NOT_FOUND}
            if result.get("found") and result.get("email"):
                verification["dm_email_verified"] = "yes"
                return {"email": result["email"], "source": SOURCE_BLITZ_EMAIL, "verification": verification}
            return {"email": "", "source": SOURCE_NOT_FOUND}

        if method == ROUTE_METHOD_PERSON_BY_NAME_DOMAIN:
            try:
                data = await _cc.person_by_name_and_domain(
                    contacts_http, inputs["full_name"], inputs["domain"]
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
            try:
                result = await _bc.person_enrich(
                    blitz_http,
                    full_name=inputs["full_name"],
                    domain=inputs["domain"],
                    include_phone=False,
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
                    verification["dm_email_verified"] = "no"
                    return {"email": emails[0].get("email", ""), "source": SOURCE_BLITZ_EMAIL, "verification": verification}
            return {"email": "", "source": SOURCE_NOT_FOUND}

        if method == ROUTE_METHOD_FIND_EMAIL:
            try:
                result = await _wl.find_email(
                    blitz_http,
                    first_name=inputs["first_name"],
                    last_name=inputs["last_name"],
                    website=inputs["domain"],
                )
            except Exception as e:
                logger.warning("WizLeads find_email failed: %s", e)
                return {"email": "", "source": SOURCE_NOT_FOUND}
            if result and result.get("email"):
                verification["dm_email_verified"] = "yes"
                return {"email": result["email"], "source": SOURCE_WIZLEADS, "verification": verification}
            return {"email": "", "source": SOURCE_NOT_FOUND}

        if method == ROUTE_METHOD_FIND_WORK_EMAIL_V3:
            try:
                result = await _be.find_work_email_v3(
                    blitz_http,
                    full_name=inputs["full_name"],
                    company_domain=inputs["domain"],
                    linkedin_url=inputs.get("linkedin_url") or None,
                )
            except Exception as e:
                logger.warning("BetterEnrich V3 failed: %s", e)
                return {"email": "", "source": SOURCE_NOT_FOUND}
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
) -> dict[str, Any]:
    """Execute a route produced by `route_enrichment`.

    Returns a result dict:
        {
            "email": str,
            "source": str,                  # one of SOURCE_*
            "verification": dict,
            "source_path": str,             # "linkedin -> contacts_db" etc.
            "provider_attempts": [str, ...],
            "no_email_reason": str,
        }
    """
    steps = route.get("steps", []) or []
    no_email_reason = route.get("no_email_reason", "") or ""
    if no_email_reason:
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
            "no_email_reason": no_email_reason,
        }

    inputs = route.get("inputs", {}) or {}
    attempts: list[str] = []
    steps_taken: list[dict[str, str]] = []
    for step in steps:
        attempts.append(f"{step['method']}@{step['identifier']}")
        result = await _run_route_step(
            step["method"],
            inputs,
            blitz_http,
            contacts_http,
            email_semaphore,
            validate_email=validate_email,
        )
        steps_taken.append(step)
        if result.get("phone_reverse") is not None:
            # Phone reverse hop — append a marker to source_path and stop.
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
                "no_email_reason": result.get("no_email_reason") or NO_EMAIL_REASON_PHONE_REVERSE_UNAVAILABLE,
            }
        if result.get("email"):
            return {
                "email": result["email"],
                "source": result.get("source", SOURCE_NOT_FOUND),
                "verification": result.get("verification") or {
                    "dm_email_verified": "unknown",
                    "mailtester_code": "",
                    "mailtester_message": "",
                },
                "source_path": _build_source_path(steps_taken, step["method"]),
                "provider_attempts": attempts,
                "no_email_reason": "",
            }

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
        "no_email_reason": "",
    }


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
    verification_info: Optional[dict[str, Any]] = None,
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

    # Add verification status if available
    if verification_info:
        row["dm_email_verified"] = verification_info.get("dm_email_verified", "unknown")
        row["mailtester_code"] = verification_info.get("mailtester_code", "")
        row["mailtester_message"] = verification_info.get("mailtester_message", "")
    else:
        row["dm_email_verified"] = "unknown"
        row["mailtester_code"] = ""
        row["mailtester_message"] = ""

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
) -> tuple[str, str, dict[str, Any]]:
    """
    Returns (email, source, verification_info).
    Priority cascade:
      1. Contacts DB by person's name + domain (PRIMARY - avoids stale LinkedIn emails)
      2. Contacts DB by LinkedIn URL (SECONDARY)
      3. Blitz person enrich by name + domain (PRIMARY PAID)
      4. Blitz email from LinkedIn URL (SECONDARY PAID)
      5. BetterEnrich work email (person lookup)
      6. Contacts DB by name + domain (name from input row, if different)

    Args:
        force_provider: If set, only use that specific provider.
        validate_email: If True, verify Contacts DB emails with mailtester.

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
    }

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
                                # Email is invalid - mark in Contacts DB and continue
                                logger.info("Email verification failed: %s (code: %s) - marking invalid", email, result["code"])
                                await contacts_client.mark_email_invalid(
                                    contacts_client_inst,
                                    email=email,
                                    domain=domain,
                                )
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
                                # Email is invalid - mark in Contacts DB and continue
                                logger.info("Email verification failed: %s (code: %s) - marking invalid", email, result["code"])
                                await contacts_client.mark_email_invalid(
                                    contacts_client_inst,
                                    email=email,
                                )
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
                        verification_info["dm_email_verified"] = "yes"
                        return verified_email, SOURCE_BLITZ_EMAIL, verification_info
                    # Fall back to unverified emails list
                    emails_list = person_data.get("emails", [])
                    if emails_list:
                        verification_info["dm_email_verified"] = "no"
                        return emails_list[0].get("email", ""), SOURCE_BLITZ_EMAIL, verification_info
            except Exception as e:
                logger.warning("Blitz person enrich failed for %s / %s: %s", full_name, domain, e)

        # Step 4: Blitz email from LinkedIn URL (SECONDARY PAID)
        # Fall back to LinkedIn if name+domain didn't find email, or if name not available
        if linkedin_url and not _should_skip_provider("blitz", force_provider):
            try:
                result = await blitz_client.find_work_email(blitz_client_inst, linkedin_url)
                if result.get("found") and result.get("email"):
                    verification_info["dm_email_verified"] = "yes"  # Blitz emails are verified
                    return result["email"], SOURCE_BLITZ_EMAIL, verification_info
            except Exception as e:
                logger.warning("Blitz email lookup failed for %s: %s", linkedin_url, e)

        # Step 5: WizLeads person email (catchall verified, 10 RPS)
        # Inserted between Blitz and BetterEnrich per user-confirmed cascade order.
        if full_name and domain and not _should_skip_provider("wizleads", force_provider):
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
                    verification_info["dm_email_verified"] = "yes"  # catchall verified by WizLeads
                    logger.info("WizLeads found email for %s: %s (catchall: %s)",
                                full_name, email, result.get("catchall"))
                    return email, SOURCE_WIZLEADS, verification_info
            except Exception as e:
                logger.warning("WizLeads lookup failed for %s / %s: %s", full_name, domain, e)

        # Step 6: BetterEnrich person email (V3 with built-in verification)
        if full_name and domain and not _should_skip_provider("better_enrich", force_provider):
            try:
                result = await better_enrich_client.find_work_email_v3(
                    blitz_client_inst, full_name, domain, linkedin_url
                )
                if result and result.get("email"):
                    email = result["email"]
                    # V3 provides email_status - map to dm_email_verified
                    email_status = result.get("email_status", "verified")
                    if email_status in ("verified", "valid"):
                        verification_info["dm_email_verified"] = "yes"
                    else:
                        verification_info["dm_email_verified"] = "unknown"
                    logger.info("BetterEnrich V3 found email for %s: %s (status: %s)", full_name, email, email_status)
                    return email, SOURCE_BETTER_ENRICH_PERSON, verification_info
            except Exception as e:
                logger.warning("BetterEnrich V3 person lookup failed for %s / %s: %s", full_name, domain, e)

        # Step 7: Contacts DB by input row name + domain (if different from person name)
        # This handles edge cases where the input name differs from the person's current name
        if input_full_name and input_full_name != full_name and domain and not _should_skip_provider("contacts_db", force_provider):
            try:
                contacts_data = await contacts_client.person_by_name_and_domain(
                    contacts_client_inst, input_full_name, domain
                )
                email = contacts_client.extract_email_from_contacts_response(contacts_data)
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
                                # Email is invalid - mark in Contacts DB and continue
                                logger.info("Email verification failed: %s (code: %s) - marking invalid", email, result["code"])
                                await contacts_client.mark_email_invalid(
                                    contacts_client_inst,
                                    email=email,
                                    domain=domain,
                                )
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
) -> list[OutputRow]:
    """
    Enrich a domain with decision-maker contacts.

    Args:
        force_provider: If set, only use that specific provider.
        validate_email: If True, verify Contacts DB emails with mailtester.
    """
    logger.info("_enrich_domain called with force_provider=%s for domain=%s", force_provider, domain)
    company_linkedin_url = ""
    linkedin_source = ""

    # Determine if we should skip Contacts DB for contacts (not for company lookup)
    # Skip Contacts DB if custom cascade is provided (indicated by skip_contacts_db=True)
    # or if cascade is not the default
    use_custom_cascade = skip_contacts_db or cascade != blitz_client.DEFAULT_CASCADE

    async with domain_semaphore:
        # Step 1: domain → company LinkedIn URL (Contacts DB FIRST unless force_provider=blitz)
        if not _should_skip_provider("contacts_db", force_provider):
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
            validate_email=validate_email,
        )
        for item in persons
    ]
    email_results = await asyncio.gather(*tasks)

    output_rows: list[OutputRow] = []
    for item, (email, source, verification_info) in zip(persons, email_results):
        person = item.get("person", {})
        icp_tier = item.get("icp", 0)
        output_rows.append(
            _build_person_row(base_row, company_linkedin_url, person, icp_tier, email, source, verification_info)
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
    check_cancelled: Optional[Callable[[str], bool]] = None,
    validate_email: bool = True,  # NEW PARAMETER
    linkedin_url_col: Optional[str] = None,
    phone_col: Optional[str] = None,
    company_name_col: Optional[str] = None,
    existing_email_col: Optional[str] = None,
    force_provider: Optional[str] = None,
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
        elif use_routing and not no_email_reason:
            # Run the routed step.
            route_result = await run_enrichment_route(
                route,
                blitz_http,
                contacts_http,
                email_semaphore,
                validate_email=validate_email,
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
            result_rows[0]["no_email_reason"] = route_result.get("no_email_reason", "")
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
            )

        # Attach input_* columns to every result row for visibility.
        for r in result_rows:
            identifier_utils.attach_input_columns(r, input_payload)
            # Ensure routing diagnostics are on the result (they were added in the routed case).
            if use_routing and not no_email_reason:
                # If we used routing, we already set these. but in domain-only case,
                # we still need to attach the no_email_reason from routing.
                r["no_email_reason"] = r.get("no_email_reason") or no_email_reason

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
