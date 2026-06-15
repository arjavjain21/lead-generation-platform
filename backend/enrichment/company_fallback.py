"""
Company / page-level email fallbacks for the enrichment pipeline.

These run AFTER the person-level waterfall has returned no decision-maker
email. They produce a *company- or page-level* email (e.g. contact@,
info@, sales@, hello@, or a Facebook page email) that is never
written into dm_email. Instead, it lands in dedicated
company_email/final_email columns and is subject to the policy in
`fallback_config.py` (ALLOW_GENERIC_COMPANY_EMAIL,
ALLOW_COMPANY_EMAIL_AS_FINAL).

Dedupe:
  * BetterEnrich find-company-email: keyed by `normalized_domain` and
    reused across rows in the same job. The first row that needs the
    value makes the API call; subsequent rows with the same domain
    reuse the cached result (hit or miss).
  * BetterEnrich find-email-from-facebook-page: keyed by
    `normalized_facebook_url` and reused across rows in the same job.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable, Optional

import httpx

from . import better_enrich_client
from . import fallback_config as fb_cfg
from . import identifier_utils
from . import mailtester_client
from . import pipeline as pipeline_mod

logger = logging.getLogger(__name__)


def normalize_domain_key(domain: str) -> str:
    """Return a dedupe key for a website/domain. '' if no domain.

    Thin alias for `identifier_utils.normalize_domain` so the company
    fallback dedupe keys match the per-row identifier payload's
    `domain` value exactly (no provider will see two different
    strings for the same site).
    """
    return identifier_utils.normalize_domain(domain)


def _is_paid_facebook() -> bool:
    """Whether the Facebook page lookup should run at all. Always runs when
    ENABLE_FACEBOOK_EMAIL_FALLBACK is True AND a normalized facebook URL is
    present on the row.
    """
    return fb_cfg.ENABLE_FACEBOOK_EMAIL_FALLBACK


def _is_paid_company() -> bool:
    """Whether the company-email lookup should run at all. Always runs when
    ENABLE_COMPANY_EMAIL_FALLBACK is True AND a domain is present on the row.
    """
    return fb_cfg.ENABLE_COMPANY_EMAIL_FALLBACK


async def _verify_with_mailtester(
    client: httpx.AsyncClient,
    email: str,
    *,
    validate_email: bool,
) -> tuple[bool, str, str]:
    """Verify `email` with Mailtester (or pass-through if disabled).

    Returns (verified_yes, code, message) where verified_yes is True when
    Mailtester said OK, or when validation is disabled (treated as
    pass-through so the caller can still mark verified='unknown' if it
    wants to be explicit).

    The function never raises: on Mailtester error, verified_yes=False.
    """
    if not email or "@" not in email:
        return False, "", "invalid_format"
    if not validate_email:
        return True, "validation_disabled", "validation disabled"
    try:
        result = await mailtester_client.verify_email(client, email)
        return result["valid"], result.get("code", ""), result.get("message", "")
    except RuntimeError as e:
        logger.warning("Mailtester unavailable for company email %s: %s", email, e)
        return False, "unavailable", str(e)


def _classify_email_type(email: str) -> str:
    """Return 'generic' if the local part matches GENERIC_EMAIL_PREFIXES,
    else 'specific'. Empty emails return ''.
    """
    if not email:
        return ""
    return "generic" if fb_cfg.is_generic_email(email) else "specific"


def _build_source_path(parts: list[str]) -> str:
    """Join non-empty source_path parts with ' -> '. Strips empties."""
    return " -> ".join(p for p in parts if p)


class CompanyFallbackDedupe:
    """Per-job cache so repeated domains/facebook pages only hit the API once.

    Stored on the calling code's run_pipeline/run_domain_enrichment state.
    Methods are async-safe (lock-guarded) so the concurrent row workers
    in the pipeline don't double-call. Uses an in-flight Future so two
    coroutines that race for the same key share the same API call.
    """

    def __init__(self) -> None:
        self._company: dict[str, Optional[dict]] = {}
        self._facebook: dict[str, Optional[dict]] = {}
        self._company_inflight: dict[str, asyncio.Future] = {}
        self._facebook_inflight: dict[str, asyncio.Future] = {}
        self._lock = asyncio.Lock()

    async def get_or_fetch_company(
        self,
        fetch: Callable[[], Awaitable[Optional[dict]]],
        domain: str,
    ) -> Optional[dict]:
        """Run `fetch` at most once per normalized_domain. Subsequent
        callers for the same domain get the cached value (which may be
        None on miss)."""
        key = normalize_domain_key(domain)
        if not key:
            return None
        own_call = False
        inflight: Optional[asyncio.Future] = None
        async with self._lock:
            if key in self._company:
                return self._company[key]
            if key in self._company_inflight:
                inflight = self._company_inflight[key]
            else:
                inflight = asyncio.get_event_loop().create_future()
                self._company_inflight[key] = inflight
                own_call = True
        if not own_call:
            assert inflight is not None
            return await inflight
        try:
            result = await fetch()
        except Exception as e:
            inflight.set_exception(e)
            async with self._lock:
                self._company_inflight.pop(key, None)
            raise
        async with self._lock:
            self._company[key] = result
            self._company_inflight.pop(key, None)
        inflight.set_result(result)
        return result

    async def get_or_fetch_facebook(
        self,
        fetch: Callable[[], Awaitable[Optional[dict]]],
        facebook_url: str,
    ) -> Optional[dict]:
        """Run `fetch` at most once per normalized_facebook_url."""
        key = identifier_utils.normalize_facebook_url(facebook_url)
        if not key:
            return None
        own_call = False
        inflight: Optional[asyncio.Future] = None
        async with self._lock:
            if key in self._facebook:
                return self._facebook[key]
            if key in self._facebook_inflight:
                inflight = self._facebook_inflight[key]
            else:
                inflight = asyncio.get_event_loop().create_future()
                self._facebook_inflight[key] = inflight
                own_call = True
        if not own_call:
            assert inflight is not None
            return await inflight
        try:
            result = await fetch()
        except Exception as e:
            inflight.set_exception(e)
            async with self._lock:
                self._facebook_inflight.pop(key, None)
            raise
        async with self._lock:
            self._facebook[key] = result
            self._facebook_inflight.pop(key, None)
        inflight.set_result(result)
        return result


async def run_company_fallbacks(
    blitz_http: httpx.AsyncClient,
    *,
    domain: str,
    facebook_url: str,
    source_path_prefix: str = "",
    validate_email: bool = True,
    dedupe: Optional[CompanyFallbackDedupe] = None,
    record_provider_use: Optional[Callable[[str], None]] = None,
) -> dict[str, Any]:
    """Run the company / page-level email fallbacks for a single row.

    Args:
        blitz_http: shared httpx client (we reuse the same one the
            person-level cascade uses).
        domain: row's website/domain (e.g. "acme.com"). Empty -> skipped.
        facebook_url: row's facebook page URL. Empty -> skipped.
        source_path_prefix: source_path value to prefix the result
            with, so the row's source_path can show "contacts_db -> ... ->
            better_enrich_company" rather than only "better_enrich_company".
        validate_email: if True, verify the returned company/page email
            with Mailtester. If False, accept as-is.
        dedupe: shared per-job dedupe cache. If None, a fresh one is
            created (so this function is safe to call directly in tests).
        record_provider_use: optional callback for the provider-attempts
            accounting (records "better_enrich" attempts).

    Returns a dict with the new company/final columns:
        {
            "company_email": str,
            "company_email_source": str,        # "better_enrich_facebook_email" | "better_enrich_company_email" | ""
            "company_email_verified": str,     # "yes" | "no" | "unknown" | ""
            "company_email_type": str,         # "generic" | "specific" | ""
            "company_email_source_path": str,
            "final_email": str,
            "final_email_level": str,          # "person" | "company" | ""
            "final_email_source_path": str,
            "no_email_reason": str,            # one of the new *_NOT_ALLOWED / *_REJECTED / *_DISABLED / *_MISSING
            "providers_called": [str, ...],    # attempted fallbacks
        }

    Notes:
      * This function NEVER raises; all provider errors are caught and
        surfaced as "no_email_reason" / "providers_called" entries.
      * The function does NOT mutate dm_email. The caller is responsible
        for assembling the full OutputRow using _apply_company_fallbacks_to_row.
    """
    if dedupe is None:
        dedupe = CompanyFallbackDedupe()

    providers_called: list[str] = []
    company_email = ""
    company_email_source = ""
    company_email_verified = ""
    company_email_type = ""
    company_email_source_path = ""
    company_email_provider_status = ""  # what BetterEnrich told us about the email
    no_email_reason = ""

    # ---- Facebook page email fallback ----
    normalized_fb = identifier_utils.normalize_facebook_url(facebook_url)
    if not _is_paid_facebook():
        if facebook_url and not normalized_fb:
            # facebook URL was given but not parseable.
            no_email_reason = pipeline_mod.NO_EMAIL_REASON_FACEBOOK_PAGE_MISSING
    elif not normalized_fb:
        no_email_reason = pipeline_mod.NO_EMAIL_REASON_FACEBOOK_PAGE_MISSING
    else:
        async def _do_facebook() -> Optional[dict]:
            providers_called.append("better_enrich")
            if record_provider_use:
                try:
                    record_provider_use("better_enrich")
                except Exception as e:
                    logger.warning("record_provider_use failed: %s", e)
            return await better_enrich_client.find_email_from_facebook_page(
                blitz_http, normalized_fb,
            )

        fb_result = await dedupe.get_or_fetch_facebook(_do_facebook, normalized_fb)
        if fb_result and fb_result.get("email"):
            providers_called.append("better_enrich_facebook_email")
            company_email = fb_result["email"]
            company_email_source = pipeline_mod.SOURCE_BETTER_ENRICH_FACEBOOK
            company_email_provider_status = fb_result.get("email_status", "unknown")
            company_email_type = _classify_email_type(company_email)
            if company_email_provider_status in ("verified", "valid"):
                company_email_verified = "yes"
            else:
                # Fall through to Mailtester below.
                ok, code, msg = await _verify_with_mailtester(
                    blitz_http, company_email, validate_email=validate_email,
                )
                company_email_verified = "yes" if ok else "no"
            company_email_source_path = _build_source_path([
                source_path_prefix,
                "better_enrich_facebook",
            ])

    # ---- Company email fallback (only if facebook didn't yield) ----
    if not company_email:
        domain_key = normalize_domain_key(domain)
        if not _is_paid_company():
            if domain and not no_email_reason:
                no_email_reason = pipeline_mod.NO_EMAIL_REASON_COMPANY_EMAIL_FALLBACK_DISABLED
        elif not domain_key:
            if not no_email_reason:
                no_email_reason = pipeline_mod.NO_EMAIL_REASON_COMPANY_EMAIL_FALLBACK_DISABLED
        else:
            async def _do_company() -> Optional[dict]:
                providers_called.append("better_enrich")
                if record_provider_use:
                    try:
                        record_provider_use("better_enrich")
                    except Exception as e:
                        logger.warning("record_provider_use failed: %s", e)
                return await better_enrich_client.find_company_email(
                    blitz_http, website=domain_key,
                )

            be_result = await dedupe.get_or_fetch_company(_do_company, domain_key)
            if be_result and be_result.get("email"):
                providers_called.append("better_enrich_company_email")
                company_email = be_result["email"]
                company_email_source = pipeline_mod.SOURCE_BETTER_ENRICH_COMPANY_V2
                company_email_provider_status = be_result.get("email_status", "unknown")
                company_email_type = _classify_email_type(company_email)
                if company_email_provider_status in ("verified", "valid"):
                    company_email_verified = "yes"
                else:
                    ok, code, msg = await _verify_with_mailtester(
                        blitz_http, company_email, validate_email=validate_email,
                    )
                    company_email_verified = "yes" if ok else "no"
                company_email_source_path = _build_source_path([
                    source_path_prefix,
                    "better_enrich_company",
                ])

    # ---- Apply generic-email + final-email policy ----
    final_email = ""
    final_email_level = ""
    final_email_source_path = ""

    if company_email and company_email_type == "generic" and not fb_cfg.ALLOW_GENERIC_COMPANY_EMAIL:
        # Reject the email entirely (do not even keep it in company_email? —
        # per spec, company_email is still populated, but final_email stays
        # blank and no_email_reason is set).
        no_email_reason = pipeline_mod.NO_EMAIL_REASON_GENERIC_COMPANY_EMAIL_REJECTED
        company_email = ""  # Spec: "final_email blank, no_email_reason=..."
        company_email_source = ""
        company_email_verified = ""
        company_email_type = ""
        company_email_source_path = ""

    if company_email:
        if fb_cfg.ALLOW_COMPANY_EMAIL_AS_FINAL:
            final_email = company_email
            final_email_level = "company"
            final_email_source_path = company_email_source_path or _build_source_path([
                source_path_prefix,
                "company_fallback",
            ])
        else:
            if not no_email_reason:
                no_email_reason = pipeline_mod.NO_EMAIL_REASON_COMPANY_EMAIL_FOUND_BUT_NOT_ALLOWED

    return {
        "company_email": company_email,
        "company_email_source": company_email_source,
        "company_email_verified": company_email_verified,
        "company_email_type": company_email_type,
        "company_email_source_path": company_email_source_path,
        "final_email": final_email,
        "final_email_level": final_email_level,
        "final_email_source_path": final_email_source_path,
        "no_email_reason": no_email_reason,
        "providers_called": providers_called,
    }


def apply_company_fallbacks_to_row(
    row: dict[str, Any],
    result: dict[str, Any],
    *,
    person_email: str = "",
    person_source_path: str = "",
) -> dict[str, Any]:
    """Mutate `row` in place to add the company/final columns.

    Computes `final_email` honoring the person-first policy: if a
    decision-maker email was already found by the person-level cascade,
    final_email is the person email and the company email is recorded
    separately. If no person email, the company fallback decides
    final_email (subject to ALLOW_COMPANY_EMAIL_AS_FINAL).
    """
    # Person email wins.
    if person_email:
        row["final_email"] = person_email
        row["final_email_level"] = "person"
        row["final_email_source_path"] = person_source_path
    else:
        row["final_email"] = result.get("final_email", "")
        row["final_email_level"] = result.get("final_email_level", "")
        row["final_email_source_path"] = result.get("final_email_source_path", "")

    row["company_email"] = result.get("company_email", "")
    row["company_email_source"] = result.get("company_email_source", "")
    row["company_email_verified"] = result.get("company_email_verified", "")
    row["company_email_type"] = result.get("company_email_type", "")
    row["company_email_source_path"] = result.get("company_email_source_path", "")

    # If the person-level cascade left no reason, don't overwrite; but if
    # the company fallback set a reason (e.g. rejected, disabled), keep it
    # so the user knows why a fallback didn't return an email.
    fb_reason = result.get("no_email_reason", "")
    if fb_reason and not row.get("no_email_reason"):
        row["no_email_reason"] = fb_reason

    return row
