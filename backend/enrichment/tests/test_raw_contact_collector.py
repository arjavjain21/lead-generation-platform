"""
Comprehensive unit tests for ``enrichment.raw_contact_collector``.

Covers:
  * Construction (empty state, default job_id, optional job_id)
  * ``capture_company_contact`` for Contacts DB, Blitz, BetterEnrich,
    WizLeads shapes — both valid and junk contacts
  * Junk filter integration with response_normalizer (unknown provider,
    placeholders, no-meaningful-identifier)
  * ``capture_company_email`` (generic company-email path) — keeps
    meaningful emails, drops placeholders, never routes to dm_email
  * ``to_payloads`` returns a fresh list, all required keys present
  * ``stats`` shape and counter accuracy
  * ``__len__`` consistency
  * row_index sequencing across mixed captures
  * job_id lineage propagation
  * Integration with realistic provider waterfall shapes (mocked)
  * Payload compatibility with ``contacts_writer._write_person_payload``
    (mocked httpx client) — proves the schema is accepted end-to-end

Run:
    python -m pytest enrichment/tests/test_raw_contact_collector.py -v
"""

from __future__ import annotations

import asyncio
import os
import sys
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

# Make sure the backend root is on sys.path so `enrichment` is importable
# regardless of where pytest is invoked from.
_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

# Ensure CONTACTS_API_TOKEN is set even when not running via service
os.environ.setdefault("CONTACTS_API_TOKEN", "test-token-from-suite")

from enrichment import contacts_writer as cw  # noqa: E402
from enrichment.raw_contact_collector import RawContactCollector  # noqa: E402


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


class TestConstruction(unittest.TestCase):
    def test_empty_len_zero(self):
        c = RawContactCollector()
        self.assertEqual(len(c), 0)

    def test_empty_to_payloads_returns_empty_list(self):
        c = RawContactCollector()
        # Must be a list, not a generator or None
        self.assertEqual(c.to_payloads(), [])
        self.assertIsInstance(c.to_payloads(), list)

    def test_empty_stats_zero_counts(self):
        c = RawContactCollector()
        stats = c.stats()
        self.assertEqual(stats["total_captured"], 0)
        self.assertEqual(stats["total_filtered"], 0)
        self.assertEqual(stats["by_source_captured"], {})
        self.assertEqual(stats["by_source_filtered"], {})

    def test_job_id_default_none(self):
        c = RawContactCollector()
        self.assertIsNone(c.job_id)

    def test_job_id_stored(self):
        c = RawContactCollector(job_id="job-123")
        self.assertEqual(c.job_id, "job-123")

    def test_stats_returns_all_four_keys(self):
        c = RawContactCollector()
        stats = c.stats()
        for key in ("total_captured", "total_filtered", "by_source_captured", "by_source_filtered"):
            self.assertIn(key, stats)


# ---------------------------------------------------------------------------
# capture_company_contact — valid contacts kept
# ---------------------------------------------------------------------------


class TestCaptureCompanyContactValid(unittest.TestCase):
    def test_contacts_db_full_contact(self):
        c = RawContactCollector()
        captured = c.capture_company_contact(
            source="contacts_db",
            domain="acme.com",
            company_linkedin_url="https://linkedin.com/company/acme",
            contact={
                "full_name": "Alice Anderson",
                "email": "alice@acme.com",
                "linkedin_url": "https://linkedin.com/in/alice",
                "title": "CEO",
            },
        )
        self.assertTrue(captured)
        self.assertEqual(len(c), 1)
        p = c.to_payloads()[0]
        self.assertEqual(p["dm_email"], "alice@acme.com")
        self.assertEqual(p["dm_full_name"], "Alice Anderson")
        self.assertEqual(p["dm_first_name"], "")  # not provided
        self.assertEqual(p["dm_title"], "CEO")
        self.assertEqual(p["dm_linkedin_url"], "linkedin.com/in/alice")
        self.assertEqual(p["domain"], "acme.com")
        self.assertEqual(p["normalized_domain"], "acme.com")
        self.assertEqual(p["company_linkedin_url"], "https://linkedin.com/company/acme")
        self.assertEqual(p["dm_email_source"], "contacts_db")
        self.assertEqual(p["source_path"], "contacts_db.company_cascade")

    def test_blitz_wrapped_person(self):
        """Blitz waterfall responses wrap the person in {'person': {...}}."""
        c = RawContactCollector()
        captured = c.capture_company_contact(
            source="blitz",
            domain="acme.com",
            company_linkedin_url="",
            contact={
                "icp": 1,
                "ranking": 1,
                "person": {
                    "first_name": "Bob",
                    "last_name": "Baker",
                    "verified_email": "bob@acme.com",
                    "linkedin_url": "https://linkedin.com/in/bob",
                    "experiences": [{"job_is_current": True, "job_title": "CTO"}],
                },
            },
        )
        self.assertTrue(captured)
        p = c.to_payloads()[0]
        self.assertEqual(p["dm_email"], "bob@acme.com")
        self.assertEqual(p["dm_full_name"], "Bob Baker")  # derived from first/last
        self.assertEqual(p["dm_title"], "CTO")  # pulled from experiences
        # Blitz person dict doesn't carry company_name — payload omits it
        self.assertNotIn("company_name", p)

    def test_better_enrich_minimal_email(self):
        c = RawContactCollector()
        captured = c.capture_company_contact(
            source="better_enrich",
            domain="acme.com",
            company_linkedin_url="",
            contact={
                "email": "carol@acme.com",
                "email_status": "verified",
            },
        )
        self.assertTrue(captured)
        p = c.to_payloads()[0]
        self.assertEqual(p["dm_email"], "carol@acme.com")
        # BetterEnrich responses use 'status'/'email_status' for verification
        self.assertEqual(p["dm_email_verified"], "yes")

    def test_wizleads_contact(self):
        c = RawContactCollector()
        captured = c.capture_company_contact(
            source="wizleads",
            domain="acme.com",
            company_linkedin_url="",
            contact={
                "email": "dan@acme.com",
                "normalized_fname": "Dan",
                "normalized_lname": "Davis",
                "catchall": "YES",
            },
        )
        self.assertTrue(captured)
        p = c.to_payloads()[0]
        self.assertEqual(p["dm_email"], "dan@acme.com")
        self.assertEqual(p["dm_first_name"], "Dan")
        self.assertEqual(p["dm_last_name"], "Davis")

    def test_only_email_kept(self):
        """Junk filter keeps a contact that has only an email."""
        c = RawContactCollector()
        captured = c.capture_company_contact(
            source="contacts_db",
            domain="x.com",
            company_linkedin_url="",
            contact={"email": "someone@x.com"},
        )
        self.assertTrue(captured)
        p = c.to_payloads()[0]
        self.assertEqual(p["dm_email"], "someone@x.com")
        self.assertEqual(p["dm_full_name"], "")

    def test_only_name_kept(self):
        """Junk filter keeps a contact that has only a name."""
        c = RawContactCollector()
        captured = c.capture_company_contact(
            source="contacts_db",
            domain="x.com",
            company_linkedin_url="",
            contact={"full_name": "Lone Stranger"},
        )
        self.assertTrue(captured)
        p = c.to_payloads()[0]
        self.assertEqual(p["dm_full_name"], "Lone Stranger")
        self.assertEqual(p["dm_email"], "")

    def test_only_linkedin_kept(self):
        """Junk filter keeps a contact that has only a LinkedIn URL."""
        c = RawContactCollector()
        captured = c.capture_company_contact(
            source="contacts_db",
            domain="x.com",
            company_linkedin_url="",
            contact={"linkedin_url": "https://linkedin.com/in/lonely"},
        )
        self.assertTrue(captured)
        p = c.to_payloads()[0]
        self.assertEqual(p["dm_linkedin_url"], "linkedin.com/in/lonely")
        self.assertEqual(p["dm_email"], "")


# ---------------------------------------------------------------------------
# capture_company_contact — junk filtered
# ---------------------------------------------------------------------------


class TestCaptureCompanyContactJunk(unittest.TestCase):
    def test_no_meaningful_identifier_filtered(self):
        """Contact with no email, no name, no linkedin is junk."""
        c = RawContactCollector()
        captured = c.capture_company_contact(
            source="contacts_db",
            domain="x.com",
            company_linkedin_url="",
            contact={"title": "CEO"},  # title alone is NOT enough
        )
        self.assertFalse(captured)
        self.assertEqual(len(c), 0)
        self.assertEqual(c.stats()["total_filtered"], 1)

    def test_unknown_provider_filtered(self):
        """prospeo is not registered in response_normalizer."""
        c = RawContactCollector()
        captured = c.capture_company_contact(
            source="prospeo",
            domain="x.com",
            company_linkedin_url="",
            contact={"email": "real@x.com", "full_name": "Real Person"},
        )
        self.assertFalse(captured)
        self.assertEqual(c.stats()["by_source_filtered"].get("prospeo"), 1)

    def test_email_placeholder_with_placeholder_name_filtered(self):
        """Contact whose email AND name are both placeholders is junk.

        'placeholder' is NOT in NAME_PLACEHOLDERS (only template values like
        'john doe' / 'first last' are), so we use a known placeholder here.
        """
        c = RawContactCollector()
        captured = c.capture_company_contact(
            source="contacts_db",
            domain="x.com",
            company_linkedin_url="",
            contact={"email": "no_email", "full_name": "unknown"},
        )
        # 'no_email' and 'unknown' are both placeholders -> junk
        self.assertFalse(captured)

    def test_email_placeholder_with_real_name_kept(self):
        """If only email is placeholder but name is real, the contact is kept.

        response_normalizer's junk filter keeps any contact with at least
        ONE meaningful identifier (email OR name OR linkedin). A real name
        is enough — the dm_email field will be "" but dm_full_name set.
        """
        c = RawContactCollector()
        captured = c.capture_company_contact(
            source="contacts_db",
            domain="x.com",
            company_linkedin_url="",
            contact={"email": "no_email", "full_name": "Real Person"},
        )
        self.assertTrue(captured)
        p = c.to_payloads()[0]
        self.assertEqual(p["dm_email"], "")  # email placeholder cleared
        self.assertEqual(p["dm_full_name"], "Real Person")  # name kept

    def test_all_placeholder_contact_filtered(self):
        c = RawContactCollector()
        captured = c.capture_company_contact(
            source="contacts_db",
            domain="x.com",
            company_linkedin_url="",
            contact={"email": "n/a", "full_name": "unknown", "linkedin_url": "none"},
        )
        self.assertFalse(captured)
        self.assertEqual(c.stats()["total_filtered"], 1)
        self.assertEqual(c.stats()["total_captured"], 0)

    def test_non_dict_contact_filtered(self):
        """A non-dict contact should not crash; treated as junk."""
        c = RawContactCollector()
        # response_normalizer returns None for non-dict input
        captured = c.capture_company_contact(
            source="contacts_db",
            domain="x.com",
            company_linkedin_url="",
            contact="not a dict",  # type: ignore[arg-type]
        )
        self.assertFalse(captured)

    def test_empty_dict_filtered(self):
        c = RawContactCollector()
        captured = c.capture_company_contact(
            source="contacts_db",
            domain="x.com",
            company_linkedin_url="",
            contact={},
        )
        self.assertFalse(captured)


# ---------------------------------------------------------------------------
# Counters / stats
# ---------------------------------------------------------------------------


class TestCounters(unittest.TestCase):
    def test_captured_counter_increments(self):
        c = RawContactCollector()
        for i in range(3):
            c.capture_company_contact(
                source="contacts_db",
                domain="x.com",
                company_linkedin_url="",
                contact={"email": f"u{i}@x.com", "full_name": f"User {i}"},
            )
        self.assertEqual(c.stats()["total_captured"], 3)
        self.assertEqual(c.stats()["by_source_captured"]["contacts_db"], 3)

    def test_filtered_counter_increments(self):
        c = RawContactCollector()
        for _ in range(4):
            c.capture_company_contact(
                source="blitz",
                domain="x.com",
                company_linkedin_url="",
                contact={},
            )
        self.assertEqual(c.stats()["total_filtered"], 4)
        self.assertEqual(c.stats()["by_source_filtered"]["blitz"], 4)
        self.assertEqual(c.stats()["total_captured"], 0)

    def test_mixed_sources_separate_counts(self):
        c = RawContactCollector()
        c.capture_company_contact(
            source="contacts_db", domain="x.com", company_linkedin_url="",
            contact={"email": "a@x.com"},
        )
        c.capture_company_contact(
            source="blitz", domain="x.com", company_linkedin_url="",
            contact={"email": "b@x.com"},
        )
        c.capture_company_contact(
            source="blitz", domain="x.com", company_linkedin_url="",
            contact={},
        )
        s = c.stats()
        self.assertEqual(s["by_source_captured"], {"contacts_db": 1, "blitz": 1})
        self.assertEqual(s["by_source_filtered"], {"blitz": 1})
        self.assertEqual(s["total_captured"], 2)
        self.assertEqual(s["total_filtered"], 1)

    def test_len_matches_captured(self):
        c = RawContactCollector()
        c.capture_company_contact(
            source="contacts_db", domain="x.com", company_linkedin_url="",
            contact={"email": "a@x.com"},
        )
        c.capture_company_contact(
            source="contacts_db", domain="x.com", company_linkedin_url="",
            contact={},  # junk
        )
        c.capture_company_email(
            source="blitz", domain="x.com", company_linkedin_url="",
            email_data={"email": "info@x.com"},
        )
        # 2 captured (1 person + 1 company), 1 filtered
        self.assertEqual(len(c), 2)
        self.assertEqual(c.stats()["total_captured"], 2)
        self.assertEqual(c.stats()["total_filtered"], 1)


# ---------------------------------------------------------------------------
# row_index sequencing
# ---------------------------------------------------------------------------


class TestRowIndex(unittest.TestCase):
    def test_row_index_sequential_person_only(self):
        c = RawContactCollector()
        for i in range(5):
            c.capture_company_contact(
                source="contacts_db", domain="x.com", company_linkedin_url="",
                contact={"email": f"u{i}@x.com"},
            )
        indices = [p["row_index"] for p in c.to_payloads()]
        self.assertEqual(indices, [0, 1, 2, 3, 4])

    def test_row_index_sequential_mixed_captures(self):
        """Person and company-email captures share the same row_index space."""
        c = RawContactCollector()
        c.capture_company_contact(
            source="contacts_db", domain="x.com", company_linkedin_url="",
            contact={"email": "p1@x.com"},
        )  # row_index 0
        c.capture_company_email(
            source="blitz", domain="x.com", company_linkedin_url="",
            email_data={"email": "info@x.com"},
        )  # row_index 1
        c.capture_company_contact(
            source="blitz", domain="x.com", company_linkedin_url="",
            contact={"email": "p2@x.com"},
        )  # row_index 2
        indices = [p["row_index"] for p in c.to_payloads()]
        self.assertEqual(indices, [0, 1, 2])


# ---------------------------------------------------------------------------
# job_id lineage
# ---------------------------------------------------------------------------


class TestJobIdLineage(unittest.TestCase):
    def test_job_id_propagated_to_payload(self):
        c = RawContactCollector(job_id="job-abc-123")
        c.capture_company_contact(
            source="contacts_db", domain="x.com", company_linkedin_url="",
            contact={"email": "a@x.com"},
        )
        c.capture_company_email(
            source="blitz", domain="x.com", company_linkedin_url="",
            email_data={"email": "info@x.com"},
        )
        for p in c.to_payloads():
            self.assertEqual(p["job_id"], "job-abc-123")

    def test_no_job_id_omitted_from_payload(self):
        """When job_id is None, the payload omits the key entirely."""
        c = RawContactCollector()
        c.capture_company_contact(
            source="contacts_db", domain="x.com", company_linkedin_url="",
            contact={"email": "a@x.com"},
        )
        p = c.to_payloads()[0]
        self.assertNotIn("job_id", p)


# ---------------------------------------------------------------------------
# capture_company_email
# ---------------------------------------------------------------------------


class TestCaptureCompanyEmail(unittest.TestCase):
    def test_captures_generic_email(self):
        c = RawContactCollector()
        captured = c.capture_company_email(
            source="blitz",
            domain="acme.com",
            company_linkedin_url="",
            email_data={"email": "info@acme.com"},
        )
        self.assertTrue(captured)
        p = c.to_payloads()[0]
        # CRITICAL: routed to company_email, NEVER dm_email
        self.assertEqual(p["company_email"], "info@acme.com")
        self.assertNotIn("dm_email", p)
        self.assertEqual(p["company_email_source"], "blitz.company_email")
        self.assertEqual(p["company_email_type"], "generic")
        self.assertEqual(p["source_path"], "blitz.company_email")

    def test_routes_to_company_email_not_dm_email(self):
        """The CRITICAL invariant: company emails never enter the dm_email path."""
        c = RawContactCollector()
        c.capture_company_email(
            source="contacts_db", domain="x.com", company_linkedin_url="",
            email_data={"email": "support@x.com"},
        )
        p = c.to_payloads()[0]
        self.assertIn("company_email", p)
        self.assertNotIn("dm_email", p)
        self.assertEqual(p["company_email"], "support@x.com")

    def test_empty_email_filtered(self):
        c = RawContactCollector()
        captured = c.capture_company_email(
            source="blitz", domain="x.com", company_linkedin_url="",
            email_data={"email": ""},
        )
        self.assertFalse(captured)

    def test_placeholder_email_filtered(self):
        c = RawContactCollector()
        captured = c.capture_company_email(
            source="blitz", domain="x.com", company_linkedin_url="",
            email_data={"email": "no_email"},
        )
        self.assertFalse(captured)

    def test_na_email_filtered(self):
        c = RawContactCollector()
        captured = c.capture_company_email(
            source="blitz", domain="x.com", company_linkedin_url="",
            email_data={"email": "n/a"},
        )
        self.assertFalse(captured)
        self.assertEqual(c.stats()["total_filtered"], 1)

    def test_name_only_contact_filtered_for_company_email(self):
        """For the company-email path, name-only is NOT enough — we need an email."""
        c = RawContactCollector()
        captured = c.capture_company_email(
            source="contacts_db", domain="x.com", company_linkedin_url="",
            email_data={"full_name": "John Doe"},  # passes normalizer junk filter
        )
        self.assertFalse(captured)
        self.assertEqual(c.stats()["total_filtered"], 1)

    def test_verified_propagates(self):
        c = RawContactCollector()
        c.capture_company_email(
            source="better_enrich", domain="x.com", company_linkedin_url="",
            email_data={"email": "info@x.com", "email_status": "verified"},
        )
        p = c.to_payloads()[0]
        self.assertEqual(p["company_email_verified"], "yes")

    def test_company_name_propagates_when_present(self):
        c = RawContactCollector()
        c.capture_company_email(
            source="contacts_db", domain="x.com", company_linkedin_url="",
            email_data={"email": "info@x.com", "company_name": "ACME Inc"},
        )
        p = c.to_payloads()[0]
        self.assertEqual(p["company_name"], "ACME Inc")


# ---------------------------------------------------------------------------
# to_payloads semantics
# ---------------------------------------------------------------------------


class TestToPayloads(unittest.TestCase):
    def test_returns_list_not_generator(self):
        c = RawContactCollector()
        c.capture_company_contact(
            source="contacts_db", domain="x.com", company_linkedin_url="",
            contact={"email": "a@x.com"},
        )
        out = c.to_payloads()
        self.assertIsInstance(out, list)
        # Calling next() on it would fail — confirms it's not a generator
        self.assertEqual(len(out), 1)

    def test_returns_fresh_list_each_call(self):
        """Mutating the returned list must not affect internal state."""
        c = RawContactCollector()
        c.capture_company_contact(
            source="contacts_db", domain="x.com", company_linkedin_url="",
            contact={"email": "a@x.com"},
        )
        out1 = c.to_payloads()
        out1.clear()  # mutate
        out1.append({"junk": True})
        out2 = c.to_payloads()
        # Internal state preserved
        self.assertEqual(len(out2), 1)
        self.assertEqual(out2[0]["dm_email"], "a@x.com")

    def test_all_required_keys_present_person(self):
        c = RawContactCollector()
        c.capture_company_contact(
            source="contacts_db", domain="x.com", company_linkedin_url="",
            contact={"email": "a@x.com"},
        )
        p = c.to_payloads()[0]
        required = {
            "dm_email", "dm_full_name", "dm_first_name", "dm_last_name",
            "dm_title", "dm_linkedin_url", "domain", "normalized_domain",
            "company_linkedin_url", "dm_email_source", "source_path",
            "dm_email_verified", "row_index",
        }
        for key in required:
            self.assertIn(key, p, f"missing required key: {key}")

    def test_all_required_keys_present_company(self):
        c = RawContactCollector()
        c.capture_company_email(
            source="blitz", domain="x.com", company_linkedin_url="",
            email_data={"email": "info@x.com"},
        )
        p = c.to_payloads()[0]
        required = {
            "company_email", "company_email_source", "company_email_verified",
            "company_email_type", "domain", "normalized_domain",
            "company_linkedin_url", "source_path", "row_index",
        }
        for key in required:
            self.assertIn(key, p, f"missing required key: {key}")

    def test_normalized_domain_canonicalized(self):
        """normalized_domain strips protocol/www."""
        c = RawContactCollector()
        c.capture_company_contact(
            source="contacts_db",
            domain="https://www.acme.com/about",
            company_linkedin_url="",
            contact={"email": "a@acme.com"},
        )
        p = c.to_payloads()[0]
        # domain field keeps the queried value verbatim...
        self.assertEqual(p["domain"], "https://www.acme.com/about")
        # ...normalized_domain is canonical
        self.assertEqual(p["normalized_domain"], "acme.com")


# ---------------------------------------------------------------------------
# Integration: realistic provider waterfall shapes
# ---------------------------------------------------------------------------


class TestRealisticProviderShapes(unittest.TestCase):
    """Mock realistic provider responses and verify end-to-end capture."""

    def test_contacts_db_waterfall_5_contacts(self):
        """A Contacts DB company_contacts_enriched response with 5 contacts."""
        c = RawContactCollector(job_id="job-1")
        # Realistic shape from contacts_client.company_contacts_enriched
        contacts = [
            {"full_name": "CEO Person", "email": "ceo@acme.com", "title": "CEO"},
            {"full_name": "CTO Person", "email": "cto@acme.com", "title": "CTO"},
            {"full_name": "CFO Person", "email": "cfo@acme.com", "title": "CFO"},
            {"full_name": "VP Sales", "email": "vpsales@acme.com"},
            {"full_name": "VP Marketing", "email": "vpmarketing@acme.com"},
        ]
        for contact in contacts:
            c.capture_company_contact(
                source="contacts_db",
                domain="acme.com",
                company_linkedin_url="https://linkedin.com/company/acme",
                contact=contact,
            )
        self.assertEqual(len(c), 5)
        emails = [p["dm_email"] for p in c.to_payloads()]
        self.assertEqual(
            sorted(emails),
            ["ceo@acme.com", "cfo@acme.com", "cto@acme.com",
             "vpmarketing@acme.com", "vpsales@acme.com"],
        )
        self.assertEqual(c.stats()["by_source_captured"]["contacts_db"], 5)

    def test_blitz_waterfall_3_results(self):
        """A Blitz waterfall_icp_search response with 3 ranked results."""
        c = RawContactCollector(job_id="job-2")
        # Realistic shape from blitz_client.waterfall_icp_search
        waterfall = [
            {
                "icp": 1, "ranking": 1,
                "person": {
                    "first_name": "Alice", "last_name": "Anderson",
                    "verified_email": "alice@acme.com",
                    "linkedin_url": "https://linkedin.com/in/alice",
                    "experiences": [{"job_is_current": True, "job_title": "CEO"}],
                },
            },
            {
                "icp": 1, "ranking": 2,
                "person": {
                    "first_name": "Bob", "last_name": "Baker",
                    "verified_email": "bob@acme.com",
                    "experiences": [{"job_is_current": True, "job_title": "CTO"}],
                },
            },
            {
                "icp": 1, "ranking": 3,
                "person": {
                    "first_name": "Carol", "last_name": "Chen",
                    "emails": [{"email": "carol@acme.com"}],
                },
            },
        ]
        for entry in waterfall:
            c.capture_company_contact(
                source="blitz",
                domain="acme.com",
                company_linkedin_url="",
                contact=entry,
            )
        self.assertEqual(len(c), 3)
        emails = [p["dm_email"] for p in c.to_payloads()]
        self.assertEqual(
            sorted(emails),
            ["alice@acme.com", "bob@acme.com", "carol@acme.com"],
        )
        # All three came from blitz
        self.assertEqual(c.stats()["by_source_captured"]["blitz"], 3)

    def test_mixed_providers_capture(self):
        """A cascade hits multiple providers — verify by_source counts."""
        c = RawContactCollector()
        # Contacts DB finds 2
        for n in ("ceo", "cto"):
            c.capture_company_contact(
                source="contacts_db", domain="acme.com", company_linkedin_url="",
                contact={"email": f"{n}@acme.com", "full_name": n.upper()},
            )
        # Blitz finds 1 more (different person)
        c.capture_company_contact(
            source="blitz", domain="acme.com", company_linkedin_url="",
            contact={"person": {"verified_email": "vp@acme.com",
                                "first_name": "V", "last_name": "P"}},
        )
        # WizLeads finds 1 more
        c.capture_company_contact(
            source="wizleads", domain="acme.com", company_linkedin_url="",
            contact={"email": "founder@acme.com",
                     "normalized_fname": "Founder"},
        )
        # BetterEnrich finds 1 more
        c.capture_company_contact(
            source="better_enrich", domain="acme.com", company_linkedin_url="",
            contact={"email": "cfo@acme.com"},
        )
        s = c.stats()
        self.assertEqual(s["by_source_captured"],
                         {"contacts_db": 2, "blitz": 1, "wizleads": 1, "better_enrich": 1})
        self.assertEqual(s["total_captured"], 5)

    def test_mixed_capture_with_junk_filtered(self):
        """Realistic cascade: some providers return junk that must be filtered."""
        c = RawContactCollector()
        # Contacts DB returns valid + junk
        c.capture_company_contact(
            source="contacts_db", domain="x.com", company_linkedin_url="",
            contact={"email": "ceo@x.com"},
        )
        c.capture_company_contact(
            source="contacts_db", domain="x.com", company_linkedin_url="",
            contact={"email": "n/a", "full_name": "unknown"},  # junk
        )
        # Blitz returns only junk
        c.capture_company_contact(
            source="blitz", domain="x.com", company_linkedin_url="",
            contact={"person": {"title": "CEO"}},  # no email/name/linkedin
        )
        s = c.stats()
        self.assertEqual(s["total_captured"], 1)
        self.assertEqual(s["total_filtered"], 2)
        self.assertEqual(s["by_source_captured"], {"contacts_db": 1})
        self.assertEqual(s["by_source_filtered"], {"contacts_db": 1, "blitz": 1})


# ---------------------------------------------------------------------------
# Payload compatibility with contacts_writer
# ---------------------------------------------------------------------------


class TestContactsWriterCompatibility(unittest.TestCase):
    """Verify that collector payloads are accepted by contacts_writer."""

    def test_person_payload_accepted_by_write_enrichment_result(self):
        """End-to-end: collector -> contacts_writer._write_person_payload.

        We mock _do_upsert so no real HTTP is made, then verify the writer
        accepts our payload shape (no missing required fields, no type errors,
        and the writer routes it to the person path because dm_email is set).
        """
        c = RawContactCollector(job_id="job-compat-1")
        c.capture_company_contact(
            source="contacts_db",
            domain="acme.com",
            company_linkedin_url="https://linkedin.com/company/acme",
            contact={
                "full_name": "Compat Test",
                "email": "compat@acme.com",
                "title": "CEO",
            },
        )
        payload = c.to_payloads()[0]

        async def runner():
            with patch.object(cw, "_do_upsert", new=AsyncMock(
                    return_value=cw.WriteStatus.INSERTED)):
                status = await cw.write_enrichment_result(
                    payload, job_id=payload.get("job_id"),
                    row_index=payload.get("row_index"),
                )
                return status

        status = asyncio.run(runner())
        # INSERTED confirms the writer accepted the payload and routed it
        # to the person path (a company-only payload would have returned
        # NO_DATA because dm_email is set AND company_email is not).
        self.assertEqual(status, cw.WriteStatus.INSERTED)

    def test_company_email_payload_accepted_by_write_enrichment_result(self):
        """End-to-end: collector -> company-email path in contacts_writer."""
        c = RawContactCollector()
        c.capture_company_email(
            source="blitz",
            domain="acme.com",
            company_linkedin_url="",
            email_data={"email": "info@acme.com"},
        )
        payload = c.to_payloads()[0]

        async def runner():
            with patch.object(cw, "_do_upsert", new=AsyncMock(
                    return_value=cw.WriteStatus.UPDATED)):
                status = await cw.write_enrichment_result(
                    payload, job_id=None, row_index=payload["row_index"],
                )
                return status

        status = asyncio.run(runner())
        # UPDATED confirms the writer accepted the company_email payload
        # and routed it to the company path (the person path returns NO_DATA
        # because dm_email is empty).
        self.assertEqual(status, cw.WriteStatus.UPDATED)

    def test_batch_writer_accepts_collector_payloads(self):
        """Collector.to_payloads() feeds write_enrichment_result_batch."""
        c = RawContactCollector(job_id="job-batch")
        for i in range(3):
            c.capture_company_contact(
                source="contacts_db", domain="acme.com", company_linkedin_url="",
                contact={"email": f"u{i}@acme.com"},
            )
        payloads = c.to_payloads()

        async def runner():
            with patch.object(cw, "_do_upsert", new=AsyncMock(
                    return_value=cw.WriteStatus.INSERTED)):
                result = await cw.write_enrichment_result_batch(
                    payloads, job_id="job-batch")
                return result

        result = asyncio.run(runner())
        self.assertEqual(result.inserted, 3)
        self.assertEqual(result.failed, 0)

    def test_payload_does_not_crash_writer_with_missing_optional_fields(self):
        """Writer tolerates missing optional fields; we set them all anyway."""
        c = RawContactCollector()
        c.capture_company_contact(
            source="contacts_db", domain="x.com", company_linkedin_url="",
            contact={"email": "a@x.com"},
        )
        payload = c.to_payloads()[0]
        # All required and optional keys are present, values may be ""
        for key in ("dm_full_name", "dm_first_name", "dm_last_name",
                    "dm_title", "dm_linkedin_url", "dm_email_verified"):
            self.assertIn(key, payload)


# ---------------------------------------------------------------------------
# Verified-status extraction (defensive)
# ---------------------------------------------------------------------------


class TestVerifiedExtraction(unittest.TestCase):
    def test_contacts_db_email_verified_yes(self):
        c = RawContactCollector()
        c.capture_company_contact(
            source="contacts_db", domain="x.com", company_linkedin_url="",
            contact={"email": "a@x.com", "email_verified": "yes"},
        )
        self.assertEqual(c.to_payloads()[0]["dm_email_verified"], "yes")

    def test_better_enrich_status_verified(self):
        c = RawContactCollector()
        c.capture_company_contact(
            source="better_enrich", domain="x.com", company_linkedin_url="",
            contact={"email": "a@x.com", "status": "verified"},
        )
        self.assertEqual(c.to_payloads()[0]["dm_email_verified"], "yes")

    def test_blitz_verified_email_no_status_field(self):
        """Blitz extracts email from verified_email but has no separate status."""
        c = RawContactCollector()
        c.capture_company_contact(
            source="blitz", domain="x.com", company_linkedin_url="",
            contact={"person": {"verified_email": "a@x.com",
                                "first_name": "A"}},
        )
        # No status key present -> empty string is correct
        self.assertEqual(c.to_payloads()[0]["dm_email_verified"], "")

    def test_invalid_value_returns_empty(self):
        c = RawContactCollector()
        c.capture_company_contact(
            source="contacts_db", domain="x.com", company_linkedin_url="",
            contact={"email": "a@x.com", "email_verified": "garbage_value"},
        )
        self.assertEqual(c.to_payloads()[0]["dm_email_verified"], "")


# ---------------------------------------------------------------------------
# Idempotency: same contact captured twice is NOT deduped
# ---------------------------------------------------------------------------


class TestIdempotency(unittest.TestCase):
    def test_same_contact_twice_kept_both(self):
        """Collector does NOT dedup — both captures are stored.

        This is intentional: we want overlap signal (e.g. Contacts DB
        AND Blitz both found the same person). The downstream contacts_writer
        handles actual dedup via email-keyed upsert.
        """
        c = RawContactCollector()
        contact = {"email": "dup@x.com", "full_name": "Dup Name"}
        c.capture_company_contact(
            source="contacts_db", domain="x.com", company_linkedin_url="",
            contact=contact,
        )
        c.capture_company_contact(
            source="blitz", domain="x.com", company_linkedin_url="",
            contact=contact,
        )
        self.assertEqual(len(c), 2)
        # Both payloads reference the same email but different sources
        payloads = c.to_payloads()
        self.assertEqual(payloads[0]["dm_email"], "dup@x.com")
        self.assertEqual(payloads[1]["dm_email"], "dup@x.com")
        self.assertEqual(payloads[0]["dm_email_source"], "contacts_db")
        self.assertEqual(payloads[1]["dm_email_source"], "blitz")


if __name__ == "__main__":
    unittest.main()
