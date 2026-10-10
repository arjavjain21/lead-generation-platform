"""
Tests for ``response_normalizer.normalize_quickenrich_contact`` — the
QuickEnrich contact normalizer + dispatcher registry coverage.

Both input shapes are covered:
  (a) ``quickenrich_client._parse_employee_record`` output (client-canonical)
  (b) raw vendor EmployeeRecord (employee_linkedin / N/A sentinels)

Run:
    cd backend && python -m pytest enrichment/tests/test_quickenrich_normalizer.py -v
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from enrichment import response_normalizer as rn  # noqa: E402


class TestClientCanonicalShape:
    def test_full_record(self):
        rec = rn.normalize_provider_contact(
            "quickenrich",
            {
                "found": True,
                "email": "jane@acme.com",
                "first_name": "Jane",
                "last_name": "Doe",
                "full_name": "Jane Doe",
                "title": "CEO",
                "linkedin_url": "https://www.linkedin.com/in/janedoe",
                "company_url": "acme.com",
                "email_domain": "acme.com",
                "company_name": "Acme Inc",
                "industry": "Software",
                "employee_count": "51-200",
                "revenue": "$10M-$50M",
                "city": "Austin",
                "country_code": "US",
                "email_verification_date": "2026-09-01",
            },
        )
        assert rec is not None
        assert rec["source"] == "quickenrich"
        assert rec["email"] == "jane@acme.com"
        assert rec["domain"] == "acme.com"
        assert rec["title"] == "CEO"
        assert "linkedin.com/in/janedoe" in rec["linkedin_url"]
        assert rec["email_last_verified_at"] == "2026-09-01"
        assert rec["company_industry"] == "Software"
        assert rec["country"] == "US"

    def test_missing_optional_fields_default_empty(self):
        rec = rn.normalize_provider_contact(
            "quickenrich", {"email": "x@y.io", "first_name": "X"}
        )
        assert rec is not None
        assert rec["last_name"] == ""
        assert rec["title"] == ""


class TestRawVendorShape:
    def test_employee_linkedin_alias(self):
        rec = rn.normalize_provider_contact(
            "quickenrich",
            {
                "email": "bob@corp.io",
                "first_name": "Bob",
                "last_name": "Smith",
                "employee_linkedin": "https://www.linkedin.com/in/bob",
                "email_domain": "corp.io",
            },
        )
        assert rec is not None
        assert "linkedin.com/in/bob" in rec["linkedin_url"]
        assert rec["domain"] == "corp.io"

    def test_na_sentinels_treated_as_missing(self):
        rec = rn.normalize_provider_contact(
            "quickenrich",
            {"email": "a@b.co", "first_name": "A", "employee_phone": "N/A", "city": " n/a "},
        )
        assert rec is not None
        assert rec["phone"] in ("", None)
        assert rec["city"] in ("", None)


class TestJunkFilter:
    def test_email_less_record_skipped(self):
        assert rn.normalize_provider_contact("quickenrich", {"first_name": "X"}) is None

    def test_non_dict_skipped(self):
        assert rn.normalize_provider_contact("quickenrich", "not-a-dict") is None

    def test_placeholder_email_skipped(self):
        assert rn.normalize_provider_contact("quickenrich", {"email": "N/A"}) is None


class TestRegistryVariants:
    @staticmethod
    def _hit(source: str) -> bool:
        return (
            rn.normalize_provider_contact(source, {"email": "a@b.co", "first_name": "A"})
            is not None
        )

    def test_all_spelling_variants(self):
        for variant in ("quickenrich", "QuickEnrich", "QUICKENRICH", "quick_enrich", "quick-enrich", "qe"):
            assert self._hit(variant), f"variant {variant!r} not registered"

    def test_unknown_source_returns_none(self):
        assert rn.normalize_provider_contact("quickenrichx", {"email": "a@b.co"}) is None

    def test_exported_in_all(self):
        assert "normalize_quickenrich_contact" in rn.__all__
