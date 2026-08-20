"""
RawContactCollector — accumulates EVERY contact seen during an enrichment
cascade (per provider, per row), then drains to Contacts DB at job end.

Design rationale
================
The enrichment cascade currently truncates provider responses to a small
subset (typically the "best" decision maker per row) before reaching the
user's CSV. That truncation discards audit data the business wants: we
pay for 1000s of contacts across Contacts DB / Blitz / BetterEnrich /
WizLeads and only persist a handful.

This module sits at the provider call site. For every contact returned
by every provider (BEFORE truncation), the cascade calls
``capture_company_contact`` / ``capture_company_email``. The collector
applies the same junk filter as the rest of the system
(``response_normalizer.normalize_provider_contact``), accumulates the
kept contacts in memory, and at job end ``to_payloads()`` returns a list
ready for ``contacts_writer.write_enrichment_result_batch``.

Non-goals
---------
* No deduplication. The downstream contacts_writer upsert is keyed by
  email, so capturing the same contact twice (e.g. Contacts DB then
  Blitz) is harmless AND surfaces overlap signal we want to measure.
* No I/O. Synchronous, pure, in-memory. Draining is the caller's job.
* No thread safety. Used from async tasks in a single thread only.
* No disk persistence. Per-job, drained at end. A crashed job loses
  the in-flight captures — acceptable for this audit surface.

Payload schema
--------------
The output payloads use the EXACT keys documented in
``contacts_writer.write_enrichment_result`` (line 243 onwards). The
writer tolerates missing fields gracefully; we set every documented key
to either a real value or "" so the schema is uniform.
"""

from __future__ import annotations

from typing import Any, Optional

from .response_normalizer import (
    normalize_domain as _normalize_domain,
    normalize_provider_contact,
)


# ---------------------------------------------------------------------------
# Verified-status extraction
# ---------------------------------------------------------------------------
#
# response_normalizer intentionally does NOT surface verification status
# (its normalized dict only carries identity fields). The collector pulls
# the status directly from the raw provider dict, defending against the
# many key names providers use.
#
# Recognised values are normalised to the {"yes", "no", "unknown"} set
# that contacts_writer accepts. Anything else collapses to "" (let the
# writer treat it as missing).

_VERIFIED_TRUTHY: frozenset[str] = frozenset({
    "yes", "true", "verified", "valid", "ok", "1",
})

_VERIFIED_FALSY: frozenset[str] = frozenset({
    "no", "false", "unverified", "invalid", "bounced", "0",
})

_VERIFIED_UNKNOWN: frozenset[str] = frozenset({
    "unknown", "pending", "catchall", "catch_all", "unverifiable",
    "grey", "greylist",
})

_VERIFIED_KEY_CANDIDATES: tuple[str, ...] = (
    "email_verified",
    "verified",
    "verification_status",
    "email_status",
    "status",  # BetterEnrich uses {"status": "verified"} as the email verdict
    "is_verified",
)


def _extract_verified(raw: dict[str, Any]) -> str:
    """Best-effort extraction of an email-verified verdict from a raw
    provider contact dict.

    Returns one of {"yes", "no", "unknown", ""}. Never raises.
    """
    if not isinstance(raw, dict):
        return ""
    for key in _VERIFIED_KEY_CANDIDATES:
        if key not in raw:
            continue
        value = raw.get(key)
        if value is None:
            continue
        if isinstance(value, bool):
            return "yes" if value else "no"
        if not isinstance(value, str):
            try:
                value = str(value)
            except Exception:
                continue
        v = value.strip().lower()
        if not v:
            continue
        if v in _VERIFIED_TRUTHY:
            return "yes"
        if v in _VERIFIED_FALSY:
            return "no"
        if v in _VERIFIED_UNKNOWN:
            return "unknown"
    return ""


def _extract_company_name(raw: dict[str, Any]) -> str:
    """Best-effort company-name pull from a raw contact dict."""
    if not isinstance(raw, dict):
        return ""
    for key in ("company_name", "company", "organization", "organization_name"):
        v = raw.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def _extract_company_industry(raw: dict[str, Any]) -> str:
    """Best-effort company-industry pull from a raw contact dict.

    Used so newly-captured leads carry their company industry into the
    contacts_writer payload, where the write-back hook auto-classifies them
    into a lead_universe bucket. Returns "" when not present (graceful).
    """
    if not isinstance(raw, dict):
        return ""
    for key in ("company_industry", "industry"):
        v = raw.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


# ---------------------------------------------------------------------------
# Collector
# ---------------------------------------------------------------------------


class RawContactCollector:
    """Accumulates every contact seen during an enrichment cascade.

    Provider call sites call ``capture_company_contact`` for every contact
    returned (BEFORE truncation). At end of job, ``to_payloads()`` returns
    the full list ready for
    ``contacts_writer.write_enrichment_result_batch()``.

    The collector is intentionally minimal:
      * Junk filter via ``response_normalizer.normalize_provider_contact``.
      * No dedup (downstream contacts_writer handles that via email upsert).
      * No I/O, no thread safety, no disk persistence.

    See module docstring for the full design rationale.
    """

    def __init__(self, job_id: Optional[str] = None) -> None:
        """Initialize an empty collector.

        Args:
            job_id: Optional job identifier. If provided, included in each
                emitted payload as ``payload["job_id"]`` for lineage.
        """
        self.job_id: Optional[str] = job_id
        self._payloads: list[dict[str, Any]] = []
        self._captured_by_source: dict[str, int] = {}
        self._filtered_by_source: dict[str, int] = {}

    # ------------------------------------------------------------------
    # Person contacts
    # ------------------------------------------------------------------

    def capture_company_contact(
        self,
        *,
        source: str,
        domain: str,
        company_linkedin_url: str,
        contact: dict[str, Any],
    ) -> bool:
        """Capture one person-level contact.

        Args:
            source: Provider name (``"contacts_db"``, ``"blitz"``,
                ``"better_enrich"``, ``"wizleads"``). Unknown sources are
                treated as junk by the normalizer.
            domain: The queried domain (the input to the cascade).
            company_linkedin_url: The queried company LinkedIn URL
                (may be ``""`` if the cascade was domain-only).
            contact: Raw provider response dict.

        Returns:
            True if the contact was captured, False if filtered as junk
            (no meaningful identifier after normalization, or unknown
            provider).
        """
        normalized = normalize_provider_contact(source, contact)
        if normalized is None:
            self._bump(self._filtered_by_source, source)
            return False

        # Normalize the queried domain via the shared helper so payloads
        # use the same canonical form as the rest of the pipeline.
        norm_domain = _normalize_domain(domain) if domain else ""

        payload: dict[str, Any] = {
            # Required email
            "dm_email": normalized.get("email", ""),
            # Person identity
            "dm_full_name": normalized.get("full_name", ""),
            "dm_first_name": normalized.get("first_name", ""),
            "dm_last_name": normalized.get("last_name", ""),
            "dm_title": normalized.get("title", ""),
            "dm_linkedin_url": normalized.get("linkedin_url", ""),
            # Company context
            "domain": domain or "",
            "normalized_domain": norm_domain,
            "company_linkedin_url": company_linkedin_url or "",
            # Source metadata
            "dm_email_source": source,
            "source_path": f"{source}.company_cascade",
            # Verification — defensive pull from raw, see _extract_verified
            "dm_email_verified": _extract_verified(contact),
            # Sequence number for downstream tracking
            "row_index": len(self._payloads),
        }

        # Optional company name if available in the raw contact
        company_name = _extract_company_name(contact)
        if company_name:
            payload["company_name"] = company_name

        # Optional company industry (powers write-back universe classification)
        company_industry = _extract_company_industry(contact)
        if company_industry:
            payload["company_industry"] = company_industry

        # Job lineage
        if self.job_id is not None:
            payload["job_id"] = self.job_id

        self._payloads.append(payload)
        self._bump(self._captured_by_source, source)
        return True

    # ------------------------------------------------------------------
    # Company / generic emails
    # ------------------------------------------------------------------

    def capture_company_email(
        self,
        *,
        source: str,
        domain: str,
        company_linkedin_url: str,
        email_data: dict[str, Any],
    ) -> bool:
        """Capture a generic / company email (info@, contact@, etc.).

        Same junk filter as ``capture_company_contact``: the email is run
        through ``response_normalizer.normalize_provider_contact`` so
        placeholders (``no_email``, ``n/a`` etc.) are dropped. If the
        raw dict has no meaningful email, returns False.

        The captured payload uses ``company_email`` (NOT ``dm_email``) so
        it routes to the company-record write path in contacts_writer
        and never overwrites a real person email.

        Args:
            source: Provider name.
            domain: The queried domain.
            company_linkedin_url: The queried company LinkedIn URL.
            email_data: Raw provider response. Expected to contain an
                ``email`` (or alias) field.

        Returns:
            True if captured, False if filtered.
        """
        normalized = normalize_provider_contact(source, email_data)
        if normalized is None:
            self._bump(self._filtered_by_source, source)
            return False

        # For the company-email path we require a meaningful email —
        # a name-only or linkedin-only record has no business being
        # written as a "company email". response_normalizer's junk filter
        # already accepts name-only or linkedin-only contacts, so we
        # tighten the check here.
        email = normalized.get("email", "")
        if not email:
            self._bump(self._filtered_by_source, source)
            return False

        norm_domain = _normalize_domain(domain) if domain else ""

        payload: dict[str, Any] = {
            "company_email": email,
            "company_email_source": f"{source}.company_email",
            "company_email_verified": _extract_verified(email_data),
            "company_email_type": "generic",
            "domain": domain or "",
            "normalized_domain": norm_domain,
            "company_linkedin_url": company_linkedin_url or "",
            "source_path": f"{source}.company_email",
            "row_index": len(self._payloads),
        }

        company_name = _extract_company_name(email_data)
        if company_name:
            payload["company_name"] = company_name

        if self.job_id is not None:
            payload["job_id"] = self.job_id

        self._payloads.append(payload)
        self._bump(self._captured_by_source, source)
        return True

    # ------------------------------------------------------------------
    # Output / introspection
    # ------------------------------------------------------------------

    def to_payloads(self) -> list[dict[str, Any]]:
        """Return a fresh list of payloads for contacts_writer.

        The returned list is a shallow copy of the internal list — callers
        can sort, filter, or mutate the list itself without affecting the
        collector's internal state. The individual payload dicts are NOT
        copied (they are immutable-in-practice: callers should not mutate
        them), which keeps memory bounded for large jobs.
        """
        return list(self._payloads)

    def stats(self) -> dict[str, Any]:
        """Return a stats dict.

        Keys:
          * ``total_captured``: total contacts stored.
          * ``total_filtered``: total contacts dropped by the junk filter.
          * ``by_source_captured``: per-source captured counts.
          * ``by_source_filtered``: per-source filtered counts.
        """
        return {
            "total_captured": len(self._payloads),
            "total_filtered": sum(self._filtered_by_source.values()),
            "by_source_captured": dict(self._captured_by_source),
            "by_source_filtered": dict(self._filtered_by_source),
        }

    def __len__(self) -> int:
        """Number of captured contacts (person + company-email)."""
        return len(self._payloads)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _bump(counter: dict[str, int], source: str) -> None:
        """Increment ``counter[source]`` by 1, defaulting missing to 0."""
        counter[source] = counter.get(source, 0) + 1


__all__ = ["RawContactCollector"]
