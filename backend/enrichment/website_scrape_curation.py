"""Curation for the website-scrape nightly sync.

Turns a raw ``email_enrichment`` row (from webscraper.eagleinfoservice.com)
plus its optional joined ``gmaps_places`` record into either ``None`` (junk —
do not import) or a ``CuratedRow``, and builds the exact payload dicts that
``contacts_writer.write_enrichment_result_batch`` consumes.

Rules are pinned by enrichment/tests/test_website_scrape_curation.py and by
docs/WEBSITE_SCRAPE_INTEGRATION_PLAN.md §4 (Curation policy):

* status must be terminal ``completed`` (in-flight rows re-pull later)
* email_class in {own_domain, role_service} — everything else is junk
* email_shared_nd <= cap (default 20) — hosting/aggregator junk
* junk local-parts (abuse@, postmaster@, ...) never import
* domain normalization goes through the canonical
  ``identifier_utils.normalize_domain`` — the same function the pipeline uses
  (a historical bug was a normalization mismatch: 18 emails from 96K rows)

The module is pure (no I/O) so it is cheap to unit test.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Optional

from .identifier_utils import normalize_domain

SOURCE_TAG = "website_scrape"

# Classes safe to import. role_service = generic role address (info@, ...) —
# imported flagged generic; own_domain = a named address on the business's own
# domain. Everything else (freemail, off_domain, placeholder, vendor_*,
# artifact, pubsec_mismatch, role_technical) is junk for lead purposes.
ALLOWED_EMAIL_CLASSES = frozenset({"own_domain", "role_service"})

# Role local-parts that are never lead-worthy. Case-insensitive.
JUNK_LOCAL_PARTS = frozenset(
    {
        "abuse",
        "postmaster",
        "webmaster",
        "hostmaster",
        "noc",
        "security",
        "privacy",
        "admin",
        "administrator",
        "support-form",
        "compliance",
        "legal",
        "dmca",
    }
)

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

# gmaps_places columns worth keeping as firmographics (plan §4 mapping).
_GMAPS_CUSTOM_FIELDS = (
    "rating",
    "reviews_count",
    "gmaps_types",
    "google_maps_url",
    "address",
    "postal_code",
    "country",
    "latitude",
    "longitude",
)


@dataclass(frozen=True)
class CurationPolicy:
    """Import filter knobs. Defaults match the plan; env overrides live in
    the sync module (kept here pure for testability)."""

    shared_nd_cap: int = 20

    def allows_class(self, email_class: Optional[str]) -> bool:
        return email_class in ALLOWED_EMAIL_CLASSES

    def allows_shared_nd(self, shared_nd: Optional[int]) -> bool:
        # NULL shared_nd means "computed for exactly this domain" — singleton.
        effective = 1 if shared_nd is None else shared_nd
        return effective <= self.shared_nd_cap


@dataclass(frozen=True)
class CuratedRow:
    """A website-scrape row that passed curation."""

    domain: str
    email: str
    email_class: str
    email_type: str
    email_confidence: Optional[float]
    is_generic: bool
    business_name: str
    industry: Optional[str]
    metadata: dict[str, Any] = field(default_factory=dict)
    gmaps: Optional[dict[str, Any]] = None
    named_contacts: tuple[dict[str, str], ...] = ()


def _is_meaningful_email(value: Optional[str]) -> bool:
    if not value or not isinstance(value, str):
        return False
    candidate = value.strip().lower()
    if candidate in {"", "no_email", "n/a", "none", "null"}:
        return False
    return bool(_EMAIL_RE.match(candidate))


def _is_junk_local_part(email: str) -> bool:
    local = email.split("@", 1)[0]
    return local in JUNK_LOCAL_PARTS


def _extract_named_contacts(metadata: dict[str, Any]) -> tuple[dict[str, str], ...]:
    """Pull {"e","n","t"} dicts out of metadata.email_contacts, dropping
    entries without a usable email. No title gating (titles too sparse — by
    design; see plan §5.10)."""
    raw_contacts = metadata.get("email_contacts") or []
    if not isinstance(raw_contacts, list):
        return ()
    contacts: list[dict[str, str]] = []
    for entry in raw_contacts:
        if not isinstance(entry, dict):
            continue
        email = str(entry.get("e") or "").strip().lower()
        if not _is_meaningful_email(email) or _is_junk_local_part(email):
            continue
        contacts.append(
            {
                "email": email,
                "name": str(entry.get("n") or "").strip(),
                "title": str(entry.get("t") or "").strip(),
            }
        )
    return tuple(contacts)


def curate_row(row: dict[str, Any], policy: CurationPolicy) -> Optional[CuratedRow]:
    """Return a CuratedRow if the row should be imported, else None.

    Never mutates ``row``.
    """
    if row.get("status") != "completed":
        return None

    email = str(row.get("email") or "").strip().lower()
    if not _is_meaningful_email(email):
        return None

    email_class = row.get("email_class")
    if not policy.allows_class(email_class):
        return None

    if not policy.allows_shared_nd(row.get("email_shared_nd")):
        return None

    if _is_junk_local_part(email):
        return None

    domain = normalize_domain(row.get("domain"))
    if not domain:
        return None

    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    gmaps = row.get("gmaps") if isinstance(row.get("gmaps"), dict) else None

    business_name = str(row.get("business_name") or "").strip() or str(row.get("page_title") or "").strip()

    confidence = row.get("email_confidence")
    if isinstance(confidence, str):
        try:
            confidence = float(confidence)
        except ValueError:
            confidence = None

    return CuratedRow(
        domain=domain,
        email=email,
        email_class=str(email_class),
        email_type=str(row.get("email_type") or ""),
        email_confidence=confidence,
        is_generic=(email_class == "role_service"),
        business_name=business_name,
        industry=(str(row["industry"]).strip() if row.get("industry") else None),
        metadata=dict(metadata),
        gmaps=dict(gmaps) if gmaps else None,
        named_contacts=_extract_named_contacts(metadata),
    )


def _company_custom_fields(curated: CuratedRow) -> dict[str, Any]:
    """Firmographics the writer cannot carry as first-class keys go here."""
    custom: dict[str, Any] = {
        "email_class": curated.email_class,
        "is_generic_email": curated.is_generic,
        "source": SOURCE_TAG,
    }
    if curated.email_confidence is not None:
        custom["email_confidence"] = curated.email_confidence
    for key in ("phone", "phone_source", "meta_description", "page_title"):
        value = curated.metadata.get(key)
        if value:
            custom[key] = value
    if curated.industry:
        custom["industry"] = curated.industry
    if curated.gmaps:
        for key in ("city", "state"):
            value = curated.gmaps.get(key)
            if value:
                custom[key] = value
        gmaps_payload = {k: curated.gmaps[k] for k in _GMAPS_CUSTOM_FIELDS if curated.gmaps.get(k) is not None}
        if gmaps_payload:
            custom["gmaps"] = gmaps_payload
    return custom


def build_company_payload(curated: CuratedRow, job_id: str) -> dict[str, Any]:
    """Build the writer payload for the domain's (generic or named) email.

    One payload covers company info + the winning email — the sanctioned
    ``_write_company_payload`` path auto-creates/updates the company from the
    domain server-side.
    """
    return {
        "domain": curated.domain,
        "company_name": curated.business_name or curated.domain,
        "company_email": curated.email,
        "company_email_source": SOURCE_TAG,
        "company_email_verified": "no",
        "company_email_type": "work",
        "source_path": "website_scrape_sync",
        "custom_fields": _company_custom_fields(curated),
        "provider_metadata": {"job_id": job_id, "kind": "website_scrape"},
        "row_index": 0,
    }


def build_named_contact_payloads(curated: CuratedRow, job_id: str) -> list[dict[str, Any]]:
    """Build writer payloads for metadata.email_contacts entries.

    A named contact is imported only when its email is on the scraped domain —
    otherwise we cannot tie the person to the company (off-domain contacts in
    scraped page HTML are frequently vendors/footers). Duplicates of the
    winning company email are dropped (the company payload carries it).
    """
    payloads: list[dict[str, Any]] = []
    for index, contact in enumerate(curated.named_contacts):
        email = contact["email"]
        if not email.endswith(f"@{curated.domain}"):
            continue
        if email == curated.email:
            continue
        full_name = contact["name"]
        first, last = _split_name(full_name)
        payloads.append(
            {
                "normalized_domain": curated.domain,
                "company_name": curated.business_name or curated.domain,
                "dm_email": email,
                "dm_email_source": SOURCE_TAG,
                "dm_email_verified": "no",
                "dm_full_name": full_name,
                "dm_first_name": first,
                "dm_last_name": last,
                "dm_title": contact["title"],
                "email_type": "work",
                "source_path": "website_scrape_sync",
                # company_industry feeds lead_universe classification in
                # _write_person_payload (classify_industry) — the plan's
                # "industry → lead universe" mapping (review 2026-08-26).
                "company_industry": curated.industry or "",
                "custom_fields": {"source": SOURCE_TAG},
                "provider_metadata": {"job_id": job_id, "kind": "website_scrape"},
                "row_index": index + 1,
            }
        )
    return payloads


def _split_name(full_name: str) -> tuple[str, str]:
    """Split 'First Last' conservatively; anything odd stays in full_name."""
    parts = full_name.split()
    if len(parts) == 0:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])
