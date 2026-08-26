"""Tests for enrichment.website_scrape_curation — import curation for the
website-scrape nightly sync (docs/WEBSITE_SCRAPE_INTEGRATION_PLAN.md).

Pins the plan's curation rules:
1. Email-class filter — keep own_domain + role_service; drop freemail,
   off_domain, placeholder, vendor_*, artifact, pubsec_mismatch, blank.
2. email_shared_nd cap (default 20) — hosting/aggregator junk out.
3. Junk local-parts (abuse@, postmaster@...) never import.
4. Email syntax gate + domain normalization via identifier_utils.normalize_domain
   (the canonical implementation — a past 18-emails-from-96K bug was a
   normalization mismatch).
5. Payload builders emit only keys contacts_writer consumes: company payloads
   (company_email + company_name + firmographics via custom_fields) and
   named-contact dm_* payloads.
6. Immutability — curation never mutates the input row dict.
"""

from __future__ import annotations

import sys
from pathlib import Path

_BACKEND = Path(__file__).resolve().parents[2]
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

from enrichment.website_scrape_curation import (  # noqa: E402
    CuratedRow,
    CurationPolicy,
    build_company_payload,
    build_named_contact_payloads,
    curate_row,
)

# Exact key sets contacts_writer reads (from contacts_writer.py
# _write_person_payload / _write_company_payload). Payload builders must not
# emit anything outside these — keys the writer ignores are dead weight and a
# contract-drift smell.
_WRITER_COMPANY_KEYS = {
    "company_email",
    "company_email_source",
    "company_email_source_path",
    "company_email_verified",
    "company_email_type",
    "company_name",
    "company_linkedin_url",
    "domain",
    "company_domain",
    "website",
    "normalized_domain",
    "email_type",
    "email_source",
    "source_path",
    "custom_fields",
    "row_index",
    "provider_metadata",
}

_WRITER_PERSON_KEYS = {
    "dm_email",
    "dm_full_name",
    "dm_first_name",
    "dm_last_name",
    "dm_title",
    "dm_headline",
    "dm_linkedin_url",
    "dm_phone",
    "dm_email_verified",
    "dm_email_source",
    "dm_location_city",
    "dm_location_country",
    "company_name",
    "company_linkedin_url",
    "company_industry",
    "mailtester_code",
    "mailtester_message",
    "email_type",
    "email_source",
    "source_path",
    "lead_universe",
    "normalized_domain",
    "domain",
    "website",
    "custom_fields",
    "row_index",
    "provider_metadata",
}


# ---------------------------------------------------------------------------
# Email-class filter
# ---------------------------------------------------------------------------


class TestEmailClassFilter:
    def test_own_domain_allowed(self):
        curated = curate_row(_row(), CurationPolicy())
        assert curated is not None
        assert curated.email == "david@handy237.com"

    def test_role_service_allowed_as_generic(self):
        curated = curate_row(_row(email_class="role_service"), CurationPolicy())
        assert curated is not None
        assert curated.is_generic is True

    def test_dropped_classes(self):
        policy = CurationPolicy()
        for klass in (
            "freemail",
            "off_domain",
            "placeholder",
            "vendor_platform",
            "vendor_signature",
            "artifact",
            "pubsec_mismatch",
            "role_technical",
            None,
            "",
        ):
            assert curate_row(_row(email_class=klass), policy) is None, klass


# ---------------------------------------------------------------------------
# shared_nd cap
# ---------------------------------------------------------------------------


class TestSharedNdCap:
    def test_under_cap_passes(self):
        assert curate_row(_row(email_shared_nd=5), CurationPolicy()) is not None

    def test_at_cap_passes(self):
        assert curate_row(_row(email_shared_nd=20), CurationPolicy()) is not None

    def test_over_cap_dropped(self):
        assert curate_row(_row(email_shared_nd=21), CurationPolicy()) is None

    def test_none_treated_as_singleton(self):
        assert curate_row(_row(email_shared_nd=None), CurationPolicy()) is not None

    def test_cap_env_override(self):
        policy = CurationPolicy(shared_nd_cap=2)
        assert curate_row(_row(email_shared_nd=3), policy) is None


# ---------------------------------------------------------------------------
# Junk local-parts + syntax
# ---------------------------------------------------------------------------


class TestJunkLocalParts:
    def test_junk_local_parts_dropped(self):
        policy = CurationPolicy()
        for local in ("abuse", "postmaster", "webmaster", "noc", "hostmaster"):
            row = _row(email=f"{local}@acme.com")
            assert curate_row(row, policy) is None, local

    def test_junk_case_insensitive(self):
        assert curate_row(_row(email="ABUSE@acme.com"), CurationPolicy()) is None


class TestSyntaxGate:
    def test_bad_syntax_dropped(self):
        policy = CurationPolicy()
        for bad in ("no-at-sign", "two@@at.com", "@nonlocal.com", "x@y@z.com", "", " ", None):
            assert curate_row(_row(email=bad), policy) is None, repr(bad)

    def test_valid_email_passes(self):
        assert curate_row(_row(), CurationPolicy()) is not None


# ---------------------------------------------------------------------------
# Status gate + normalization
# ---------------------------------------------------------------------------


class TestStatusGate:
    def test_only_terminal_completed_imports(self):
        policy = CurationPolicy()
        for status in ("pending", "processing", "browser_queued", "failed", "no_email"):
            assert curate_row(_row(status=status), policy) is None, status


class TestNormalization:
    def test_www_stripped(self):
        curated = curate_row(_row(domain="www.acme.com"), CurationPolicy())
        assert curated.domain == "acme.com"

    def test_deep_url_trimmed(self):
        curated = curate_row(_row(domain="https://acme.com/path?q=1"), CurationPolicy())
        assert curated.domain == "acme.com"

    def test_email_lowercased(self):
        curated = curate_row(_row(email="David@Handy237.com"), CurationPolicy())
        assert curated.email == "david@handy237.com"

    def test_unnormalizable_domain_dropped(self):
        assert curate_row(_row(domain="not a domain"), CurationPolicy()) is None

    def test_email_domain_mismatch_still_imports(self):
        # own_domain class is trusted from the scraper; we do not re-derive.
        curated = curate_row(_row(email="david@other.com"), CurationPolicy())
        assert curated is not None


# ---------------------------------------------------------------------------
# Immutability
# ---------------------------------------------------------------------------


class TestImmutability:
    def test_curate_row_does_not_mutate_input(self):
        row = _row()
        snapshot = dict(row)
        curate_row(row, CurationPolicy())
        assert row == snapshot


# ---------------------------------------------------------------------------
# Company payload builder
# ---------------------------------------------------------------------------


class TestBuildCompanyPayload:
    def test_minimal_payload_keys(self):
        curated = curate_row(_row(), CurationPolicy())
        payload = build_company_payload(curated, job_id="j1")
        assert payload["company_email"] == "david@handy237.com"
        assert payload["company_email_source"] == "website_scrape"
        assert payload["company_email_verified"] == "no"
        assert payload["company_email_type"] == "work"
        assert payload["domain"] == "handy237.com"
        assert payload["company_name"] == "Handy 237"
        payload.pop("row_index")
        assert set(payload) <= _WRITER_COMPANY_KEYS, set(payload) - _WRITER_COMPANY_KEYS

    def test_generic_flag_not_in_payload(self):
        # Generic emails import with type work; genericness rides in custom_fields.
        curated = curate_row(_row(email_class="role_service"), CurationPolicy())
        payload = build_company_payload(curated, job_id="j1")
        assert payload["custom_fields"]["is_generic_email"] is True

    def test_metadata_phone_in_custom_fields(self):
        row = _row(metadata={"phone": "+1 555 0100", "meta_description": "We fix things"})
        curated = curate_row(row, CurationPolicy())
        cf = build_company_payload(curated, job_id="j1")["custom_fields"]
        assert cf["phone"] == "+1 555 0100"
        assert cf["meta_description"] == "We fix things"

    def test_gmaps_fields_in_custom_fields(self):
        row = _row_with_gmaps()
        curated = curate_row(row, CurationPolicy())
        cf = build_company_payload(curated, job_id="j1")["custom_fields"]
        assert cf["city"] == "Austin"
        assert cf["state"] == "TX"
        assert cf["gmaps"]["rating"] == 4.8
        assert cf["gmaps"]["reviews_count"] == 210
        assert cf["gmaps"]["gmaps_types"] == ["plumber"]
        assert cf["gmaps"]["google_maps_url"] == "https://maps.google.com/?cid=1"

    def test_confidence_and_class_in_custom_fields(self):
        curated = curate_row(_row(email_confidence=0.75), CurationPolicy())
        cf = build_company_payload(curated, job_id="j1")["custom_fields"]
        assert cf["email_confidence"] == 0.75
        assert cf["email_class"] == "own_domain"

    def test_no_name_falls_back_to_domain(self):
        curated = curate_row(_row(business_name=None, page_title=None), CurationPolicy())
        payload = build_company_payload(curated, job_id="j1")
        assert payload["company_name"] == "handy237.com"


# ---------------------------------------------------------------------------
# Named-contact payload builder
# ---------------------------------------------------------------------------


class TestBuildNamedContactPayloads:
    def test_builds_one_payload_per_contact(self):
        row = _row(
            metadata={
                "email_contacts": [
                    {"e": "sarah@handy237.com", "n": "Sarah Jones", "t": "Manager"},
                    {"e": "bob@handy237.com"},
                ]
            }
        )
        curated = curate_row(row, CurationPolicy())
        payloads = build_named_contact_payloads(curated, job_id="j1")
        assert len(payloads) == 2
        first = payloads[0]
        first.pop("row_index")
        assert set(first) <= _WRITER_PERSON_KEYS, set(first) - _WRITER_PERSON_KEYS
        assert first["dm_email"] == "sarah@handy237.com"
        assert first["dm_full_name"] == "Sarah Jones"
        assert first["dm_title"] == "Manager"
        assert first["dm_email_source"] == "website_scrape"
        assert first["dm_email_verified"] == "no"
        assert first["email_type"] == "work"
        assert payloads[1]["dm_full_name"] == ""

    def test_named_contact_split_first_last(self):
        row = _row(metadata={"email_contacts": [{"e": "sarah@handy237.com", "n": "Sarah Jones"}]})
        curated = curate_row(row, CurationPolicy())
        payload = build_named_contact_payloads(curated, job_id="j1")[0]
        assert payload["dm_first_name"] == "Sarah"
        assert payload["dm_last_name"] == "Jones"

    def test_named_contact_on_domain_email_only(self):
        # A named contact whose email lives on a different domain is dropped —
        # we only import contacts verifiably tied to the scraped domain.
        row = _row(metadata={"email_contacts": [{"e": "sarah@elsewhere.com", "n": "Sarah"}]})
        curated = curate_row(row, CurationPolicy())
        assert build_named_contact_payloads(curated, job_id="j1") == []

    def test_named_contact_junk_local_part_dropped(self):
        row = _row(metadata={"email_contacts": [{"e": "postmaster@handy237.com", "n": "PM"}]})
        curated = curate_row(row, CurationPolicy())
        assert build_named_contact_payloads(curated, job_id="j1") == []

    def test_named_contact_bad_syntax_dropped(self):
        row = _row(metadata={"email_contacts": [{"e": "not-an-email", "n": "X"}]})
        curated = curate_row(row, CurationPolicy())
        assert build_named_contact_payloads(curated, job_id="j1") == []

    def test_no_contacts_returns_empty(self):
        curated = curate_row(_row(), CurationPolicy())
        assert build_named_contact_payloads(curated, job_id="j1") == []

    def test_contacts_deduped_against_company_email(self):
        row = _row(
            metadata={"email_contacts": [{"e": "david@handy237.com", "n": "David Dupe"}]}
        )
        curated = curate_row(row, CurationPolicy())
        assert build_named_contact_payloads(curated, job_id="j1") == []


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _row(**over):
    row = {
        "domain": "handy237.com",
        "email": "david@handy237.com",
        "email_class": "own_domain",
        "email_type": "domain_named",
        "email_confidence": 0.9,
        "email_shared_nd": 1,
        "status": "completed",
        "business_name": "Handy 237",
        "page_title": "Handy 237 — Plumbing",
        "industry": "plumber",
        "metadata": {},
        "gmaps": None,
    }
    row.update(over)
    return row


def _row_with_gmaps(**over):
    row = _row()
    row["gmaps"] = {
        "city": "Austin",
        "state": "TX",
        "rating": 4.8,
        "reviews_count": 210,
        "gmaps_types": ["plumber"],
        "google_maps_url": "https://maps.google.com/?cid=1",
    }
    row.update(over)
    return row


# CuratedRow import is exercised above; re-export guard for lint friendliness.
_ = CuratedRow
