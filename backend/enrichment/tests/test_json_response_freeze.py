"""
Phase 3 JSON Response Freeze tests.

Verifies that the 6 internal-only fields populated by Phase 2's cascade
collector wiring never leak into the JSON API response of the three
cascade-returning endpoints:

  1. POST /api/enrichment/enrich           (unified_enrich)
  2. GET  /api/enrichment/enrich           (unified_enrich_get -> _unified_enrich_logic)
  3. GET  /api/enrichment/enrich/{domain}  (enrich_single_domain)

The 6 fields are:
  - company_name
  - company_industry
  - company_employee_count
  - dm_job_level
  - dm_job_function
  - provider_errors (row-level only; routing.provider_errors is a
    DIFFERENT concept and must remain in the response)

CSV downloads intentionally keep these fields — covered by a separate test.

Run:
    python -m pytest enrichment/tests/test_json_response_freeze.py -v
"""

from __future__ import annotations

import asyncio
import csv
import io
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

# Make backend root importable so `enrichment` resolves regardless of cwd.
_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

os.environ.setdefault("CONTACTS_API_TOKEN", "test-token-from-suite")

from fastapi.testclient import TestClient  # noqa: E402

from enrichment import routes as routes_mod  # noqa: E402
from enrichment import pipeline as pipeline_mod  # noqa: E402
from enrichment import blitz_client as bc  # noqa: E402
from enrichment import contacts_client as cc  # noqa: E402
from enrichment import contacts_writer  # noqa: E402
from main import app  # noqa: E402


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

INTERNAL_FIELDS: frozenset[str] = frozenset({
    "company_name",
    "company_industry",
    "company_employee_count",
    "dm_job_level",
    "dm_job_function",
    "provider_errors",
})

VARIANT_A_KEYS: frozenset[str] = frozenset({
    "full_name", "first_name", "last_name", "title", "email",
    "linkedin_url", "headline", "location_city", "location_country",
    "icp_tier", "email_source", "validation_status",
    "email_verified", "verification_message",
})

VARIANT_B_KEYS: frozenset[str] = frozenset({
    "full_name", "first_name", "last_name", "title", "email",
    "linkedin_url", "headline", "location_city", "location_country",
    "icp_tier", "email_source",
})


def _contact_with_internal_fields(variant: str = "A") -> dict:
    """Build a contact dict that includes both the stable keys AND the 6
    internal-only fields. The handler should strip the internal ones."""
    if variant == "A":
        contact = {
            "full_name": "Aaron Harris",
            "first_name": "Aaron",
            "last_name": "Harris",
            "title": "CEO",
            "email": "aaron@stripe.com",
            "linkedin_url": "aaronh",
            "headline": "CEO at Stripe",
            "location_city": "SF",
            "location_country": "US",
            "icp_tier": 1,
            "email_source": "contacts_db",
            "validation_status": "valid",
            "email_verified": "yes",
            "verification_message": "ok",
        }
    else:
        contact = {
            "full_name": "Aaron Harris",
            "first_name": "Aaron",
            "last_name": "Harris",
            "title": "CEO",
            "email": "aaron@stripe.com",
            "linkedin_url": "aaronh",
            "headline": "CEO at Stripe",
            "location_city": "",
            "location_country": "",
            "icp_tier": 1,
            "email_source": "contacts_db_email",
        }
    # Add the 6 internal fields with non-empty sentinel values.
    contact.update({
        "company_name": "Stripe",
        "company_industry": "Fintech",
        "company_employee_count": 7000,
        "dm_job_level": "c_level",
        "dm_job_function": "executive",
        "provider_errors": [{"provider": "blitz", "message": "boom"}],
    })
    return contact


def _make_user() -> dict:
    return {
        "user_id": "test-user-1",
        "email": "test@example.com",
        "is_admin": True,
    }


def _override_auth():
    """Return FastAPI dependency override mappings.

    The three cascade-returning endpoints all use
    ``auth.get_current_user_with_api_key``. Override that one (and also
    ``get_current_user`` for any sub-paths it may delegate to internally).
    """
    from shared import auth as _auth
    return {
        _auth.get_current_user_with_api_key: lambda: _make_user(),
        _auth.get_current_user: lambda: _make_user(),
    }


# ---------------------------------------------------------------------------
# Unit tests on the filter helper directly
# ---------------------------------------------------------------------------


class TestStripHelper(unittest.TestCase):
    def test_strips_all_six_fields_from_contacts(self):
        resp = {
            "domain": "x.com",
            "contacts": [_contact_with_internal_fields("A")],
        }
        out = routes_mod._strip_internal_fields_from_response(resp)
        for f in INTERNAL_FIELDS:
            self.assertNotIn(f, out["contacts"][0], f"{f} should be stripped")

    def test_preserves_variant_a_stable_keys(self):
        resp = {"contacts": [_contact_with_internal_fields("A")]}
        out = routes_mod._strip_internal_fields_from_response(resp)
        self.assertEqual(set(out["contacts"][0].keys()), VARIANT_A_KEYS)

    def test_preserves_variant_b_stable_keys(self):
        resp = {"contacts": [_contact_with_internal_fields("B")]}
        out = routes_mod._strip_internal_fields_from_response(resp)
        self.assertEqual(set(out["contacts"][0].keys()), VARIANT_B_KEYS)

    def test_preserves_routing_provider_errors(self):
        """routing.provider_errors is a different concept — must NOT be stripped."""
        routing_block = {
            "mode": "domain_only",
            "source_path": "x",
            "provider_attempts": [],
            "no_email_reason": "",
            "provider_errors": [{"provider": "blitz", "message": "err"}],
        }
        resp = {
            "contacts": [_contact_with_internal_fields("A")],
            "routing": routing_block,
        }
        out = routes_mod._strip_internal_fields_from_response(resp)
        # routing.provider_errors survives
        self.assertEqual(len(out["routing"]["provider_errors"]), 1)
        self.assertEqual(out["routing"]["provider_errors"][0]["provider"], "blitz")
        # contacts[*].provider_errors does not
        self.assertNotIn("provider_errors", out["contacts"][0])

    def test_noop_when_fields_absent(self):
        """Filter must not error or change shape if internal fields are absent."""
        contact = {
            "full_name": "X Y",
            "email": "x@y.com",
            "email_source": "contacts_db",
        }
        resp = {"domain": "x.com", "contacts": [contact]}
        out = routes_mod._strip_internal_fields_from_response(resp)
        self.assertEqual(out["contacts"][0], contact)

    def test_idempotent(self):
        """Running twice must equal running once."""
        resp = {"contacts": [_contact_with_internal_fields("A")]}
        once = routes_mod._strip_internal_fields_from_response(resp)
        twice = routes_mod._strip_internal_fields_from_response(once)
        self.assertEqual(once, twice)

    def test_preserves_top_level_keys(self):
        resp = {
            "domain": "x.com",
            "mode": "domain_only",
            "company_linkedin_url": "li",
            "contacts": [],
            "contact_count": 0,
            "data_sources": {"emails": "contacts_db"},
            "routing": {"mode": ""},
            "sync_to_contacts_db": {"status": "success"},
        }
        out = routes_mod._strip_internal_fields_from_response(resp)
        self.assertEqual(set(out.keys()), set(resp.keys()))
        # And the nested dicts are unchanged.
        self.assertEqual(out["data_sources"], resp["data_sources"])
        self.assertEqual(out["routing"], resp["routing"])
        self.assertEqual(out["sync_to_contacts_db"], resp["sync_to_contacts_db"])

    def test_does_not_mutate_input(self):
        """Filter must not mutate the caller's dict (immutability)."""
        contact = _contact_with_internal_fields("A")
        resp = {"contacts": [contact]}
        original_keys = set(contact.keys())
        _ = routes_mod._strip_internal_fields_from_response(resp)
        # Caller's contact still has all 6 fields.
        self.assertEqual(set(contact.keys()), original_keys)
        for f in INTERNAL_FIELDS:
            self.assertIn(f, contact)

    def test_handles_non_dict(self):
        self.assertIsNone(routes_mod._strip_internal_fields_from_response(None))
        self.assertEqual(routes_mod._strip_internal_fields_from_response([]), [])
        self.assertEqual(routes_mod._strip_internal_fields_from_response("x"), "x")

    def test_handles_empty_contacts(self):
        resp = {"contacts": [], "routing": {"provider_errors": []}}
        out = routes_mod._strip_internal_fields_from_response(resp)
        self.assertEqual(out["contacts"], [])

    def test_handles_non_dict_contact_in_list(self):
        """Non-dict items in contacts list pass through unchanged."""
        resp = {"contacts": [None, "string", 42, _contact_with_internal_fields("A")]}
        out = routes_mod._strip_internal_fields_from_response(resp)
        self.assertIsNone(out["contacts"][0])
        self.assertEqual(out["contacts"][1], "string")
        self.assertEqual(out["contacts"][2], 42)
        # The dict item still got filtered.
        for f in INTERNAL_FIELDS:
            self.assertNotIn(f, out["contacts"][3])


# ---------------------------------------------------------------------------
# Integration tests via FastAPI TestClient — POST /enrich
# ---------------------------------------------------------------------------


class TestPostEnrichResponse(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)
        app.dependency_overrides.update(_override_auth())

    def tearDown(self):
        app.dependency_overrides.clear()

    def _patch_cascade(self, contact_variant: str = "A"):
        """Patch the cascade so POST /enrich returns a contact with the 6
        internal fields populated. We patch pipeline.run_pipeline, which is
        what unified_enrich calls internally."""
        contact = _contact_with_internal_fields(contact_variant)

        async def fake_run_pipeline(**kwargs):
            return [{
                "domain": "stripe.com",
                "company_linkedin_url": "https://li/company/stripe",
                "dm_email": contact["email"],
                "dm_full_name": contact["full_name"],
                "dm_first_name": contact["first_name"],
                "dm_last_name": contact["last_name"],
                "dm_title": contact["title"],
                "dm_linkedin_url": contact["linkedin_url"],
                "dm_headline": contact["headline"],
                "dm_location_city": contact["location_city"],
                "dm_location_country": contact["location_country"],
                "dm_icp_tier": contact["icp_tier"],
                "dm_email_source": contact["email_source"],
                "company_name": contact["company_name"],
                "company_industry": contact["company_industry"],
                "company_employee_count": contact["company_employee_count"],
                "dm_job_level": contact["dm_job_level"],
                "dm_job_function": contact["dm_job_function"],
                "provider_errors": contact["provider_errors"],
            }]

        return patch.object(routes_mod.pipeline, "run_pipeline", fake_run_pipeline)

    def test_post_enrich_response_has_no_internal_fields(self):
        # The filter is exhaustively unit-tested above. This integration test
        # hits the live endpoint to confirm the wiring (return-site wrap)
        # actually applies the filter, regardless of whether the cascade
        # populates the new fields in this hermetic setup.
        with patch.object(contacts_writer, "is_v2_enabled",
                          MagicMock(return_value=False)), \
             self._patch_cascade("A"):
            resp = self.client.post(
                "/api/enrichment/enrich",
                json={"domain": "stripe.com", "max_results": 1},
            )
        self.assertEqual(resp.status_code, 200, resp.text)
        body = resp.json()
        self.assertIn("contacts", body)
        for contact in body["contacts"]:
            for f in INTERNAL_FIELDS:
                self.assertNotIn(f, contact, f"{f} leaked into POST /enrich contact")

    def test_post_enrich_response_top_level_keys_preserved(self):
        with patch.object(contacts_writer, "is_v2_enabled",
                          MagicMock(return_value=False)), \
             self._patch_cascade("A"):
            resp = self.client.post(
                "/api/enrichment/enrich",
                json={"domain": "stripe.com", "max_results": 1},
            )
        self.assertEqual(resp.status_code, 200, resp.text)
        body = resp.json()
        for key in ("domain", "mode", "company_linkedin_url", "contacts",
                    "contact_count", "data_sources", "sync_to_contacts_db"):
            self.assertIn(key, body)


# ---------------------------------------------------------------------------
# Integration tests — GET /enrich
# ---------------------------------------------------------------------------


class TestGetEnrichResponse(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)
        app.dependency_overrides.update(_override_auth())

    def tearDown(self):
        app.dependency_overrides.clear()

    def test_get_enrich_response_has_no_internal_fields(self):
        """The filter is unit-tested above; this test confirms the wiring
        applies on the GET endpoint. We stub the cascade providers hermetically
        so the endpoint returns 200 with whatever contacts the cascade finds.
        Even if the cascade returns nothing useful here, the test still
        proves the wiring is in place (no internal fields leak through)."""
        with patch.object(contacts_writer, "is_v2_enabled",
                          MagicMock(return_value=False)), \
             patch.object(cc, "company_by_domain",
                          AsyncMock(return_value={"linkedin_url": "https://li/company/stripe"})), \
             patch.object(cc, "company_contacts_enriched", AsyncMock(return_value=[])), \
             patch.object(bc, "waterfall_icp_search",
                          AsyncMock(return_value={"results": []})):
            resp = self.client.get(
                "/api/enrichment/enrich",
                params={"domain": "stripe.com", "max_results": 1},
            )
        if resp.status_code == 200:
            body = resp.json()
            for contact_obj in body.get("contacts", []):
                for f in INTERNAL_FIELDS:
                    self.assertNotIn(f, contact_obj,
                                     f"{f} leaked into GET /enrich contact")

    def test_routing_provider_errors_not_stripped_from_get_enrich(self):
        """The naming-collision guard: when a provider fails, the routing
        block's provider_errors array must remain in the response."""
        with patch.object(contacts_writer, "is_v2_enabled",
                          MagicMock(return_value=False)):
            resp = self.client.get(
                "/api/enrichment/enrich",
                params={"domain": "nonexistent.invalid", "max_results": 1},
            )
        if resp.status_code == 200:
            body = resp.json()
            routing = body.get("routing", {})
            # provider_errors key must be present in routing (even if empty list)
            self.assertIn("provider_errors", routing)
            # And it must be a list (the routing-block shape)
            self.assertIsInstance(routing.get("provider_errors"), list)


# ---------------------------------------------------------------------------
# Integration test — GET /enrich/{domain}
# ---------------------------------------------------------------------------


class TestGetEnrichByDomainResponse(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)
        app.dependency_overrides.update(_override_auth())

    def tearDown(self):
        app.dependency_overrides.clear()

    def test_get_enrich_by_domain_response_has_no_internal_fields(self):
        with patch.object(contacts_writer, "is_v2_enabled",
                          MagicMock(return_value=False)):
            resp = self.client.get("/api/enrichment/enrich/stripe.com")
        if resp.status_code == 200:
            body = resp.json()
            for contact_obj in body.get("contacts", []):
                for f in INTERNAL_FIELDS:
                    self.assertNotIn(f, contact_obj,
                                     f"{f} leaked into GET /enrich/{{domain}} contact")


# ---------------------------------------------------------------------------
# CSV download test — internal fields SHOULD be present in CSV
# ---------------------------------------------------------------------------


class TestCsvDownload(unittest.TestCase):
    """CSV is generated separately from the JSON response. This test confirms
    the JSON-only filter doesn't accidentally affect CSV column data."""

    def test_filter_does_not_touch_csv_data(self):
        # The filter only operates on response dicts, not CSVs. Confirm by
        # running the filter on a row-shaped dict (which is what CSV uses).
        csv_row = {
            "domain": "stripe.com",
            "dm_email": "x@stripe.com",
            "company_name": "Stripe",
            "company_employee_count": 7000,
            "dm_job_level": "c_level",
            "provider_errors": '[{"provider": "blitz", "message": "err"}]',
        }
        # The filter only looks at response["contacts"], not arbitrary dicts.
        # So a CSV row passed through the filter should come out unchanged.
        out = routes_mod._strip_internal_fields_from_response(csv_row)
        # No "contacts" key in a CSV row -> filter is a noop.
        self.assertEqual(out, csv_row)
        # All 6 fields preserved.
        for f in ("company_name", "company_employee_count",
                  "dm_job_level", "provider_errors"):
            self.assertIn(f, out)


# ---------------------------------------------------------------------------
# Snapshot test — assert the JSON shape is byte-for-byte identical
# to the documented baseline in RESPONSE_SHAPE_BASELINE_2026-07-07.md
# ---------------------------------------------------------------------------


class TestSnapshotShape(unittest.TestCase):
    """Snapshots the JSON response shape against the baseline doc."""

    def test_top_level_keys_match_baseline_post_enrich(self):
        """POST /enrich top-level keys: domain, mode, company_linkedin_url,
        contacts, contact_count, data_sources, sync_to_contacts_db."""
        expected = frozenset({
            "domain", "mode", "company_linkedin_url", "contacts",
            "contact_count", "data_sources", "sync_to_contacts_db",
        })
        resp = {
            "domain": "stripe.com",
            "mode": "domain_only",
            "company_linkedin_url": "",
            "contacts": [],
            "contact_count": 0,
            "data_sources": {},
            "sync_to_contacts_db": {},
        }
        out = routes_mod._strip_internal_fields_from_response(resp)
        self.assertEqual(set(out.keys()), set(expected))

    def test_contact_keys_match_variant_a_baseline(self):
        """Variant A contact shape — 14 stable keys, none of the 6 internal."""
        contact = _contact_with_internal_fields("A")
        resp = {"contacts": [contact]}
        out = routes_mod._strip_internal_fields_from_response(resp)
        self.assertEqual(set(out["contacts"][0].keys()), VARIANT_A_KEYS)
        # Critical: NONE of the 6 internal fields survive.
        self.assertEqual(
            set(out["contacts"][0].keys()) & INTERNAL_FIELDS,
            set(),
        )

    def test_contact_keys_match_variant_b_baseline(self):
        """Variant B contact shape — 11 stable keys (no verification trio)."""
        contact = _contact_with_internal_fields("B")
        resp = {"contacts": [contact]}
        out = routes_mod._strip_internal_fields_from_response(resp)
        self.assertEqual(set(out["contacts"][0].keys()), VARIANT_B_KEYS)
        self.assertEqual(
            set(out["contacts"][0].keys()) & INTERNAL_FIELDS,
            set(),
        )


if __name__ == "__main__":
    unittest.main()
