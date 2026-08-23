"""
Unit tests for ``normalize_getleads_contact`` in
``enrichment.response_normalizer``.

GetLeads (app.getleads.io) has two verified response shapes (see
``/tmp/getleads_live_shapes.md``, the authoritative spec):

  (a) Enrich ``results[]`` item — top-level ``email``/``profileUrl``/
      ``linkedinUrl`` + nested ``data`` object. ``/v1/enrich/from-person``
      echoes ``profileUrl``; ``/v1/enrich/from-linkedin`` echoes ``linkedinUrl``.
  (b) Decision-makers ``contacts[]`` flat record — no ``data`` wrapper;
      ``org_*`` aliases + ``email_address``.

The API never returns ``success:false``; a no-result is ``success:true`` with
``email:null`` + ``data:null`` (not-found) or ``data`` populated but no
``email_address`` (partial). Both must normalize to None (email required).

Fixtures below are drawn verbatim from the live shape spec so the tests
document the real contract.

Pure unit tests — no async, no HTTP, no I/O, no mocks.
"""

from __future__ import annotations

import os
import sys

import pytest

# Make sure the backend root is on sys.path so `enrichment` is importable
# regardless of where pytest is invoked from.
_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from enrichment import response_normalizer as rn  # noqa: E402


# ---------------------------------------------------------------------------
# Canonical output keys (every record has exactly these keys).
#
# Phase 2 (full capture, 2026-08-14) widened the canonical record: the 9
# original identity keys plus 11 passthrough firmographic fields. GetLeads
# records additionally carry the ``_raw_getleads`` blob when the raw dict
# had one (nested ``data`` or a stitched ``_raw_getleads`` passthrough).
# ---------------------------------------------------------------------------

EXPECTED_KEYS = {
    "email", "first_name", "last_name", "full_name",
    "title", "headline", "linkedin_url", "domain", "source",
    "phone", "city", "country", "company_name", "company_industry",
    "employee_count", "revenue", "linkedin_connections",
    "email_last_verified_at", "job_level", "job_function",
}

EXPECTED_KEYS_WITH_RAW = EXPECTED_KEYS | {"_raw_getleads"}


# ---------------------------------------------------------------------------
# Verbatim fixtures from /tmp/getleads_live_shapes.md
# ---------------------------------------------------------------------------

# (a) Enrich /v1/enrich/from-person SUCCESS item — Zac Chaffin (idx 2).
ENRICH_FROM_PERSON_SUCCESS = {
    "first_name": "Zac", "last_name": "Chaffin", "email_domain": "earth.works",
    "success": True,
    "email": "zac@earth.works",
    "profileUrl": "https://www.linkedin.com/in/zac-chaffin-0475a023",
    "data": {
        "first_name": "Zac", "last_name": "Chaffin",
        "person_full_name": "Zac Chaffin",
        "job_title": "Chief Financial Officer",
        "org_company_name": "Earthworks Inc",
        "job_is_current": True,
        "job_function": "Finance & Accounting", "job_level": "C-Team",
        "person_linkedin_url": "https://www.linkedin.com/in/zac-chaffin-0475a023",
        "linkedin_url_org": "https://www.linkedin.com/company/earthworks-inc-",
        "linkedin_connections_count": 215,
        "linkedin_headline": "CFO at Earthworks, Inc.",
        "country_name_org": "United States", "city_org": "Alvarado",
        # Phase 2: person-level geo keys (present in the live shape; the
        # doc fixture had omitted them). They feed city/country passthrough.
        "person_city": "Alvarado", "person_country_name": "United States",
        "domain_org": "earth.works", "website_org": "https://www.earth.works",
        "email_address": "zac@earth.works", "email_domain": "earth.works",
        "email_status": "VALID",
        "email_last_verified_at": "2026-06-05T18:32:42",
    },
}

# (a) Enrich /v1/enrich/from-linkedin SUCCESS item — brian-mccord-pg (idx 0).
# Note: from-linkedin echoes `linkedinUrl` (NOT `profileUrl`).
ENRICH_FROM_LINKEDIN_SUCCESS = {
    "linkedinUrl": "https://www.linkedin.com/in/brian-mccord-pg",
    "success": True,
    "email": "bmccord@greenwaste.com",
    "data": {
        "first_name": "Brian", "last_name": "McCord",
        "person_full_name": "Brian McCord",
        "job_title": "Vice President of Safety",
        "org_company_name": "Greenwaste", "job_is_current": True,
        "job_function": "Public Administration & Safety", "job_level": "VP",
        "person_linkedin_url": "https://www.linkedin.com/in/brian-mccord-pg",
        "linkedin_url_org": "https://www.linkedin.com/company/greenwaste-recovery",
        "linkedin_headline": "Vice President of Safety at Greenwaste",
        "domain_org": "greenwaste.com", "website_org": "https://www.greenwaste.com",
        "email_address": "bmccord@greenwaste.com", "email_domain": "greenwaste.com",
        "email_status": "VALID",
    },
}

# (b) Decision-makers /v1/contacts/lookup/decision-makers contact — Brad Bobenrieth.
DECISION_MAKER_CONTACT = {
    "first_name": "Brad", "last_name": "Bobenrieth",
    "email_address": "bbobenrieth@meadowsfarms.com", "email_status": "VALID",
    "job_title": "Vice President",
    "job_function": "General Business & Management", "job_level": "C-Team",
    "org_company_name": "Meadows Farms Nurseries & Landscaping",
    "org_domain": "meadowsfarms.com",
    "person_country_name": "United States",
    "org_industry_linkedin": "Retail",
    "employee_count_range": "501 to 1000",
    "person_linkedin_url": "https://www.linkedin.com/in/brad-bobenrieth-84a45695",
    "email_domain": "meadowsfarms.com",
}

# Not-found shape — Ryan Verba (idx 0 of from-person). success:true, no email/data.
ENRICH_NOT_FOUND = {
    "first_name": "Ryan", "last_name": "Verba",
    "email_domain": "form.jotform.com", "success": True,
    "email": None, "profileUrl": None, "data": None,
}

# Partial shape — person matched, data populated, but NO email_address.
# (cf. idx 17 Troy Nelson / idx 6 manuel-vera in the spec). Crucially the
# data object carries identity (name + LinkedIn) but omits email_address —
# this must still normalize to None (email required).
ENRICH_PARTIAL_NO_EMAIL = {
    "first_name": "Troy", "last_name": "Nelson",
    "email_domain": "example.com", "success": True,
    "email": None,
    "profileUrl": "https://www.linkedin.com/in/troy-nelson",
    "data": {
        "first_name": "Troy", "last_name": "Nelson",
        "person_full_name": "Troy Nelson",
        "job_title": "Operations Manager",
        "person_linkedin_url": "https://www.linkedin.com/in/troy-nelson",
        "domain_org": "example.com", "email_domain": "example.com",
        # NOTE: email_address / email_status / email_last_verified_at OMITTED.
    },
}


# ---------------------------------------------------------------------------
# Happy paths — the three accepted shapes
# ---------------------------------------------------------------------------

class TestGetLeadsHappyPath:
    def test_enrich_from_person_full_data_record(self):
        """(1) Full enrich data.* record -> canonical dict source='getleads'."""
        result = rn.normalize_getleads_contact(ENRICH_FROM_PERSON_SUCCESS)
        assert result is not None
        assert result["source"] == "getleads"
        assert result["email"] == "zac@earth.works"
        assert result["first_name"] == "Zac"
        assert result["last_name"] == "Chaffin"
        assert result["full_name"] == "Zac Chaffin"
        assert result["title"] == "Chief Financial Officer"
        assert result["headline"] == "CFO at Earthworks, Inc."
        assert result["domain"] == "earth.works"
        # LinkedIn canonicalized to bare host + path.
        assert result["linkedin_url"] == "linkedin.com/in/zac-chaffin-0475a023"
        # Phase 2: the raw data blob rides along for forward-compat.
        assert set(result.keys()) == EXPECTED_KEYS_WITH_RAW
        assert result["_raw_getleads"] == ENRICH_FROM_PERSON_SUCCESS["data"]
        # Phase 2: firmographic passthroughs populated from data.*.
        assert result["job_level"] == "C-Team"
        assert result["job_function"] == "Finance & Accounting"
        assert result["linkedin_connections"] == "215"
        assert result["email_last_verified_at"] == "2026-06-05T18:32:42"
        assert result["company_name"] == "Earthworks Inc"
        assert result["country"] == "United States"
        assert result["city"] == "Alvarado"

    def test_decision_makers_org_record(self):
        """(2) Decision-makers org_* record (flat, no data wrapper)."""
        result = rn.normalize_getleads_contact(DECISION_MAKER_CONTACT)
        assert result is not None
        assert result["source"] == "getleads"
        assert result["email"] == "bbobenrieth@meadowsfarms.com"
        assert result["first_name"] == "Brad"
        assert result["last_name"] == "Bobenrieth"
        assert result["full_name"] == "Brad Bobenrieth"
        assert result["title"] == "Vice President"
        # org_domain alias must be picked up on the flat shape.
        assert result["domain"] == "meadowsfarms.com"
        assert result["linkedin_url"] == "linkedin.com/in/brad-bobenrieth-84a45695"

    def test_item_with_nested_data_from_linkedin(self):
        """(3) Item-with-nested-data shape (from-linkedin echoes linkedinUrl)."""
        result = rn.normalize_getleads_contact(ENRICH_FROM_LINKEDIN_SUCCESS)
        assert result is not None
        assert result["source"] == "getleads"
        assert result["email"] == "bmccord@greenwaste.com"
        assert result["full_name"] == "Brian McCord"
        assert result["title"] == "Vice President of Safety"
        assert result["domain"] == "greenwaste.com"
        # data.person_linkedin_url wins over the echoed linkedinUrl.
        assert result["linkedin_url"] == "linkedin.com/in/brian-mccord-pg"

    def test_bare_data_dict_passed_directly(self):
        """A caller may pass the bare `data` dict with no item wrapper."""
        bare_data = {
            "first_name": "Karin", "last_name": "Reber",
            "person_full_name": "Karin Reber",
            "job_title": "Landscape Design Sales Administrative Assistant",
            "person_linkedin_url": "https://www.linkedin.com/in/karin-reber-77b39b3a",
            "domain_org": "campbellferrara.com",
            "email_address": "kreber@campbellferrara.com",
            "email_status": "VALID",
        }
        result = rn.normalize_getleads_contact(bare_data)
        assert result is not None
        assert result["email"] == "kreber@campbellferrara.com"
        assert result["domain"] == "campbellferrara.com"
        assert result["title"] == "Landscape Design Sales Administrative Assistant"


# ---------------------------------------------------------------------------
# No-result shapes — both must return None (email required)
# ---------------------------------------------------------------------------

class TestGetLeadsNoResult:
    def test_not_found_email_null_data_null(self):
        """(4) not-found (email:null, data:null) -> None.

        Top-level first_name/last_name are INPUT ECHOES, not a confirmed
        identity, so they must NOT rescue the record.
        """
        assert rn.normalize_getleads_contact(ENRICH_NOT_FOUND) is None

    def test_partial_data_populated_no_email_address(self):
        """(5) partial (data populated, no email_address) -> None.

        Data carries a full person identity (name + LinkedIn) but omits
        email_address — GetLeads is an email-enrichment provider, so a
        record with no deliverable email is skipped (shapes spec §5–6).
        """
        assert rn.normalize_getleads_contact(ENRICH_PARTIAL_NO_EMAIL) is None

    def test_not_found_from_linkedin_shape(self):
        """from-linkedin not-found: linkedinUrl echo, email:null, data:null."""
        raw = {
            "linkedinUrl": "http://www.linkedin.com/in/brian-helgoe-81821726",
            "success": True, "email": None, "data": None,
        }
        assert rn.normalize_getleads_contact(raw) is None


# ---------------------------------------------------------------------------
# Junk / defensive inputs
# ---------------------------------------------------------------------------

class TestGetLeadsJunkAndDefensive:
    def test_none_input(self):
        assert rn.normalize_getleads_contact(None) is None  # type: ignore[arg-type]

    def test_empty_dict(self):
        """(8) junk (all empty) -> None."""
        assert rn.normalize_getleads_contact({}) is None

    def test_all_empty_values(self):
        raw = {
            "first_name": "", "last_name": "", "email": "",
            "data": {}, "success": True,
        }
        assert rn.normalize_getleads_contact(raw) is None

    def test_data_present_but_email_placeholder(self):
        raw = {
            "email": "no_email",
            "data": {"email_address": "no-email", "first_name": "John"},
        }
        assert rn.normalize_getleads_contact(raw) is None

    @pytest.mark.parametrize("bad", [
        "string",
        ["a", "b"],
        42,
        3.14,
        [],
        (),
    ])
    def test_non_dict_input(self, bad):
        assert rn.normalize_getleads_contact(bad) is None  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Field-extraction edge cases
# ---------------------------------------------------------------------------

class TestGetLeadsFieldExtraction:
    def test_domain_alias_domain_org_wins(self):
        raw = {
            "email": "x@acme.com",
            "data": {"domain_org": "acme.com", "email_domain": "wrong.com"},
            "email_domain": "alsowrong.com",
        }
        result = rn.normalize_getleads_contact(raw)
        assert result is not None
        assert result["domain"] == "acme.com"

    def test_domain_alias_email_domain_when_no_domain_org(self):
        raw = {"email": "x@acme.com", "data": {"email_domain": "acme.com"}}
        result = rn.normalize_getleads_contact(raw)
        assert result is not None
        assert result["domain"] == "acme.com"

    def test_domain_alias_org_domain_on_flat_shape(self):
        raw = {"email_address": "x@acme.com", "org_domain": "acme.com"}
        result = rn.normalize_getleads_contact(raw)
        assert result is not None
        assert result["domain"] == "acme.com"

    def test_linkedin_prefers_data_person_linkedin_url(self):
        raw = {
            "email": "x@acme.com",
            "profileUrl": "https://linkedin.com/in/wrong",
            "data": {"person_linkedin_url": "https://linkedin.com/in/right"},
        }
        result = rn.normalize_getleads_contact(raw)
        assert result is not None
        assert result["linkedin_url"] == "linkedin.com/in/right"

    def test_linkedin_falls_back_to_profile_url(self):
        # from-person echoes profileUrl at the top level; data has no linkedin.
        raw = {
            "email": "x@acme.com",
            "profileUrl": "https://www.linkedin.com/in/someone",
            "data": {"first_name": "Pat"},
        }
        result = rn.normalize_getleads_contact(raw)
        assert result is not None
        assert result["linkedin_url"] == "linkedin.com/in/someone"

    def test_linkedin_falls_back_to_linkedin_url(self):
        # from-linkedin echoes linkedinUrl at the top level.
        raw = {
            "email": "x@acme.com",
            "linkedinUrl": "https://www.linkedin.com/in/echoed",
        }
        result = rn.normalize_getleads_contact(raw)
        assert result is not None
        assert result["linkedin_url"] == "linkedin.com/in/echoed"

    def test_email_prefers_top_level_email_over_data(self):
        raw = {
            "email": "top@acme.com",
            "data": {"email_address": "nested@acme.com"},
        }
        result = rn.normalize_getleads_contact(raw)
        assert result is not None
        assert result["email"] == "top@acme.com"

    def test_full_name_synthesized_when_only_first_last(self):
        raw = {
            "email_address": "x@acme.com",
            "first_name": "Jesse", "last_name": "Pinkman",
        }
        result = rn.normalize_getleads_contact(raw)
        assert result is not None
        assert result["full_name"] == "Jesse Pinkman"

    def test_domain_with_protocol_stripped(self):
        raw = {
            "email": "x@acme.com",
            "data": {"domain_org": "https://www.acme.com/?ref=foo"},
        }
        result = rn.normalize_getleads_contact(raw)
        assert result is not None
        assert result["domain"] == "acme.com"

    def test_client_flat_output_preserves_domain_and_linkedin(self):
        """Regression (adversarial review 2026-08-13): the cascade captures the
        CLIENT's flat output dict — keys email/first_name/last_name/domain/
        verification_status/linkedin_url/phone, NO nested "data". The
        normalizer must recover domain + linkedin_url from these flat keys,
        not silently drop them. (Previously the alias chains only read the raw
        API keys domain_org/person_linkedin_url/etc., so both became "".)
        """
        client_output = {
            "email": "john.doe@example.com",
            "first_name": "John",
            "last_name": "Doe",
            "domain": "example.com",
            "verification_status": "Valid",
            "linkedin_url": "https://www.linkedin.com/in/john-doe",
            "phone": "+1 555-0100",
        }
        result = rn.normalize_getleads_contact(client_output)
        assert result is not None
        assert result["email"] == "john.doe@example.com"
        assert result["domain"] == "example.com"
        assert result["linkedin_url"] == "linkedin.com/in/john-doe"
        assert result["first_name"] == "John"
        assert result["last_name"] == "Doe"


# ---------------------------------------------------------------------------
# Dispatch via normalize_provider_contact
# ---------------------------------------------------------------------------

class TestGetLeadsDispatch:
    def _sample_raw(self):
        return ENRICH_FROM_PERSON_SUCCESS

    def test_dispatch_lowercase_matches_direct_call(self):
        raw = self._sample_raw()
        via_dispatch = rn.normalize_provider_contact("getleads", raw)
        direct = rn.normalize_getleads_contact(raw)
        assert via_dispatch is not None
        assert direct is not None
        assert via_dispatch == direct
        assert via_dispatch["source"] == "getleads"

    @pytest.mark.parametrize("variant", [
        "getleads",
        "GetLeads",
        "GETLEADS",
        "get_leads",
        "get-leads",
    ])
    def test_case_and_separator_variants_dispatch(self, variant):
        """(6) dispatch resolves getleads/get_leads/get-leads."""
        raw = self._sample_raw()
        result = rn.normalize_provider_contact(variant, raw)
        assert result is not None
        assert result["source"] == "getleads"
        assert result["email"] == "zac@earth.works"

    def test_unknown_provider_returns_none(self):
        """(7) unknown source -> None."""
        raw = self._sample_raw()
        assert rn.normalize_provider_contact("unknown_provider", raw) is None

    def test_non_dict_raw_returns_none(self):
        assert rn.normalize_provider_contact("getleads", None) is None  # type: ignore[arg-type]
        assert rn.normalize_provider_contact("getleads", []) is None  # type: ignore[arg-type]
        assert rn.normalize_provider_contact("getleads", "string") is None  # type: ignore[arg-type]

    def test_non_string_source_returns_none(self):
        raw = self._sample_raw()
        assert rn.normalize_provider_contact(None, raw) is None  # type: ignore[arg-type]
        assert rn.normalize_provider_contact(42, raw) is None  # type: ignore[arg-type]

    def test_dispatch_not_found_returns_none(self):
        # Dispatch must also honour the email-required gate.
        assert rn.normalize_provider_contact("getleads", ENRICH_NOT_FOUND) is None


# ---------------------------------------------------------------------------
# Output-shape invariants
# ---------------------------------------------------------------------------

class TestGetLeadsOutputShape:
    def test_keys_are_exactly_expected(self):
        result = rn.normalize_getleads_contact(ENRICH_FROM_PERSON_SUCCESS)
        assert result is not None
        # Canonical keys + passthrough keys + the _raw_getleads blob.
        assert set(result.keys()) == EXPECTED_KEYS_WITH_RAW

    def test_all_values_are_strings(self):
        result = rn.normalize_getleads_contact(ENRICH_FROM_PERSON_SUCCESS)
        assert result is not None
        for k, v in result.items():
            if k == "_raw_getleads":
                # The raw passthrough blob is the raw provider dict.
                assert isinstance(v, dict)
            else:
                assert isinstance(v, str), k

    def test_no_raw_blob_when_no_data(self):
        """A raw dict with no data/_raw_getleads yields the plain key set."""
        result = rn.normalize_getleads_contact(DECISION_MAKER_CONTACT)
        assert result is not None
        assert set(result.keys()) == EXPECTED_KEYS

    def test_no_getleads_specific_keys_leak(self):
        result = rn.normalize_getleads_contact(DECISION_MAKER_CONTACT)
        assert result is not None
        # status / verification / org_company_name must NOT be present.
        assert "email_status" not in result
        assert "success" not in result
        assert "org_domain" not in result
        assert "data" not in result
