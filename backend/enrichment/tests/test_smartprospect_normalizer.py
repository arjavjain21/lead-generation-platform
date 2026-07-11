"""
Unit tests for ``normalize_smartprospect_contact`` in
``enrichment.response_normalizer``.

Covers:
  * Happy path (Found + Valid, Found + null verification, Not Found).
  * Junk / defensive inputs (None, empty dict, non-dict, all-empty).
  * Field extraction edge cases (first only, last only, empty email,
    protocol stripping on domain).
  * Dispatch via ``normalize_provider_contact`` (case variants,
    underscore / dash aliases, unknown provider).
  * Confirmation that ``status`` / ``verification_status`` are NOT
    carried into the canonical record.

Pure unit tests — no async, no HTTP, no I/O, no mocks. The normalizer
is a pure function.
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
# Canonical output keys (every record has exactly these 9 keys).
# ---------------------------------------------------------------------------

EXPECTED_KEYS = {
    "email", "first_name", "last_name", "full_name",
    "title", "headline", "linkedin_url", "domain", "source",
}


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

class TestSmartProspectHappyPath:
    def test_found_and_valid(self):
        raw = {
            "firstName": "John",
            "lastName": "Doe",
            "companyDomain": "example.com",
            "email_id": "john.doe@example.com",
            "status": "Found",
            "verification_status": "Valid",
        }
        result = rn.normalize_smartprospect_contact(raw)
        assert result is not None
        assert result["source"] == "smartprospect"
        assert result["email"] == "john.doe@example.com"
        assert result["first_name"] == "John"
        assert result["last_name"] == "Doe"
        assert result["full_name"] == "John Doe"
        assert result["domain"] == "example.com"
        # SmartProspect responses never carry these three.
        assert result["title"] == ""
        assert result["headline"] == ""
        assert result["linkedin_url"] == ""

    def test_found_with_null_verification_status(self):
        # Email found but unverified.
        raw = {
            "firstName": "John",
            "lastName": "Doe",
            "companyDomain": "example.com",
            "email_id": "john.doe@example.com",
            "status": "Found",
            "verification_status": None,
        }
        result = rn.normalize_smartprospect_contact(raw)
        assert result is not None
        assert result["email"] == "john.doe@example.com"
        assert result["first_name"] == "John"
        assert result["last_name"] == "Doe"
        assert result["domain"] == "example.com"

    def test_not_found_still_has_name(self):
        # "Not Found" contact has empty email_id but keeps firstName + lastName.
        # _build_record keeps it because full_name is non-empty.
        raw = {
            "firstName": "John",
            "lastName": "Doe",
            "companyDomain": "example.com",
            "email_id": "",
            "status": "Not Found",
            "verification_status": None,
        }
        result = rn.normalize_smartprospect_contact(raw)
        assert result is not None
        # Email is empty (treated as missing).
        assert result["email"] == ""
        # Name is preserved.
        assert result["first_name"] == "John"
        assert result["last_name"] == "Doe"
        # full_name is synthesized from first + last.
        assert result["full_name"] == "John Doe"
        assert result["domain"] == "example.com"


# ---------------------------------------------------------------------------
# Junk / defensive inputs
# ---------------------------------------------------------------------------

class TestSmartProspectJunkAndDefensive:
    def test_none_input(self):
        assert rn.normalize_smartprospect_contact(None) is None  # type: ignore[arg-type]

    def test_empty_dict(self):
        # No email, no name, no linkedin -> junk.
        assert rn.normalize_smartprospect_contact({}) is None

    @pytest.mark.parametrize("bad", [
        "string",
        ["a", "b"],
        42,
        3.14,
        [],
        (),
    ])
    def test_non_dict_input(self, bad):
        assert rn.normalize_smartprospect_contact(bad) is None  # type: ignore[arg-type]

    def test_all_empty_values(self):
        raw = {
            "firstName": "",
            "lastName": "",
            "companyDomain": "",
            "email_id": "",
            "status": "",
            "verification_status": None,
        }
        assert rn.normalize_smartprospect_contact(raw) is None


# ---------------------------------------------------------------------------
# Field extraction edge cases
# ---------------------------------------------------------------------------

class TestSmartProspectFieldExtraction:
    def test_first_name_only(self):
        raw = {
            "firstName": "John",
            "email_id": "john@example.com",
        }
        result = rn.normalize_smartprospect_contact(raw)
        assert result is not None
        assert result["first_name"] == "John"
        assert result["last_name"] == ""
        # full_name synthesized from first only.
        assert result["full_name"] == "John"

    def test_last_name_only(self):
        raw = {
            "lastName": "Doe",
            "email_id": "doe@example.com",
        }
        result = rn.normalize_smartprospect_contact(raw)
        assert result is not None
        assert result["first_name"] == ""
        assert result["last_name"] == "Doe"
        assert result["full_name"] == "Doe"

    def test_email_id_empty_string_treated_as_missing(self):
        raw = {
            "firstName": "John",
            "lastName": "Doe",
            "email_id": "",
            "companyDomain": "example.com",
        }
        result = rn.normalize_smartprospect_contact(raw)
        assert result is not None
        assert result["email"] == ""
        # Kept because full_name is non-empty.
        assert result["full_name"] == "John Doe"

    def test_company_domain_protocol_stripped(self):
        # normalize_domain strips https://, www., paths, queries.
        raw = {
            "firstName": "John",
            "lastName": "Doe",
            "email_id": "john.doe@example.com",
            "companyDomain": "https://example.com",
        }
        result = rn.normalize_smartprospect_contact(raw)
        assert result is not None
        assert result["domain"] == "example.com"

    def test_company_domain_with_www_and_path(self):
        raw = {
            "firstName": "John",
            "lastName": "Doe",
            "email_id": "john.doe@example.com",
            "companyDomain": "https://www.example.com/?utm_source=gmb",
        }
        result = rn.normalize_smartprospect_contact(raw)
        assert result is not None
        assert result["domain"] == "example.com"

    def test_company_domain_missing(self):
        raw = {
            "firstName": "John",
            "lastName": "Doe",
            "email_id": "john.doe@example.com",
        }
        result = rn.normalize_smartprospect_contact(raw)
        assert result is not None
        assert result["domain"] == ""


# ---------------------------------------------------------------------------
# Dispatch via normalize_provider_contact
# ---------------------------------------------------------------------------

class TestSmartProspectDispatch:
    def _sample_raw(self):
        return {
            "firstName": "John",
            "lastName": "Doe",
            "companyDomain": "example.com",
            "email_id": "john.doe@example.com",
            "status": "Found",
            "verification_status": "Valid",
        }

    def test_dispatch_lowercase_matches_direct_call(self):
        raw = self._sample_raw()
        via_dispatch = rn.normalize_provider_contact("smartprospect", raw)
        direct = rn.normalize_smartprospect_contact(raw)
        assert via_dispatch is not None
        assert direct is not None
        assert via_dispatch == direct
        assert via_dispatch["source"] == "smartprospect"

    @pytest.mark.parametrize("variant", [
        "smartprospect",
        "SmartProspect",
        "SMARTPROSPECT",
        "smart_prospect",
        "smart-prospect",
    ])
    def test_case_and_separator_variants_dispatch(self, variant):
        raw = self._sample_raw()
        result = rn.normalize_provider_contact(variant, raw)
        assert result is not None
        assert result["source"] == "smartprospect"
        assert result["email"] == "john.doe@example.com"

    def test_unknown_provider_returns_none(self):
        raw = self._sample_raw()
        assert rn.normalize_provider_contact("unknown_provider", raw) is None

    def test_non_dict_raw_returns_none(self):
        assert rn.normalize_provider_contact("smartprospect", None) is None  # type: ignore[arg-type]
        assert rn.normalize_provider_contact("smartprospect", []) is None  # type: ignore[arg-type]
        assert rn.normalize_provider_contact("smartprospect", "string") is None  # type: ignore[arg-type]

    def test_non_string_source_returns_none(self):
        raw = self._sample_raw()
        assert rn.normalize_provider_contact(None, raw) is None  # type: ignore[arg-type]
        assert rn.normalize_provider_contact(42, raw) is None  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Verification status is NOT a normalized field
# ---------------------------------------------------------------------------

class TestSmartProspectStatusFieldsNotLeaked:
    def test_status_and_verification_status_not_in_keys(self):
        raw = {
            "firstName": "John",
            "lastName": "Doe",
            "companyDomain": "example.com",
            "email_id": "john.doe@example.com",
            "status": "Found",
            "verification_status": "Valid",
        }
        result = rn.normalize_smartprospect_contact(raw)
        assert result is not None
        # The canonical record has exactly these 9 keys.
        assert set(result.keys()) == EXPECTED_KEYS
        # Specifically, status and verification_status must NOT be present.
        assert "status" not in result
        assert "verification_status" not in result

    def test_all_values_are_strings(self):
        raw = {
            "firstName": "John",
            "lastName": "Doe",
            "companyDomain": "example.com",
            "email_id": "john.doe@example.com",
            "status": "Found",
            "verification_status": "Valid",
        }
        result = rn.normalize_smartprospect_contact(raw)
        assert result is not None
        for v in result.values():
            assert isinstance(v, str)
