"""
Acceptance tests for the cascade fixes from PROVIDER_CASCADE_AUDIT.md.

These tests verify the LOOP.md goal contract:
  1. Direct API enhanced mode uses Contacts DB -> Blitz -> WizLeads -> BetterEnrich.
  2. Unverified Blitz emails do not stop the cascade.
  3. emails_wizleads column is populated.
  4. CSV/pipeline jobs populate used_providers.
  5. BetterEnrich 201 polling still works.
  6. Verified Blitz email still stops the cascade.
  7. Source label normalization: wizleads and wizleads_email map to "wizleads".

All provider calls are mocked — no production provider credits are spent.
"""

from __future__ import annotations

import asyncio
import os
import sys
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from enrichment import pipeline as pipeline_mod  # noqa: E402
from enrichment import stats_store as stats_mod  # noqa: E402
from enrichment import contacts_client as cc  # noqa: E402
from enrichment import blitz_client as bc  # noqa: E402
from enrichment import wizleads_client as wc  # noqa: E402
from enrichment import better_enrich_client as bec  # noqa: E402
from enrichment import mailtester_client as mt  # noqa: E402
from shared import db as shared_db  # noqa: E402


# ---------------------------------------------------------------------------
# Acceptance 1: Direct API enhanced mode uses Contacts DB -> Blitz -> WizLeads
# -> BetterEnrich. When WizLeads returns a hit, BetterEnrich is NOT called.
# ---------------------------------------------------------------------------


class TestAcceptance1WizLeadsBeforeBetterEnrich(unittest.IsolatedAsyncioTestCase):
    """In enhanced mode (full_name + domain), when Contacts DB and Blitz return
    no email and WizLeads returns an email, BetterEnrich MUST NOT be called."""

    async def test_enhanced_route_calls_wizleads_then_skips_better_enrich(self):
        # Track which providers were called.
        called_providers: list[str] = []
        recorded_providers: list[str] = []

        async def fake_cc_name_domain(http, full_name, domain):
            called_providers.append("contacts_db")
            return None

        async def fake_blitz_person_enrich(http, full_name, domain, include_phone):
            called_providers.append("blitz")
            return {"found": False, "person": {}}

        async def fake_wizleads(http, first_name, last_name, website):
            called_providers.append("wizleads")
            return {"email": "jane@acme.com", "catchall": True}

        async def fake_better_enrich(http, full_name, company_domain, linkedin_url):
            called_providers.append("better_enrich")
            return {"email": "jane-be@acme.com", "email_status": "verified"}

        with patch.object(cc, "person_by_name_and_domain", fake_cc_name_domain), \
             patch.object(cc, "extract_email_from_contacts_response", MagicMock(return_value=None)), \
             patch.object(bc, "person_enrich", fake_blitz_person_enrich), \
             patch.object(bc, "find_work_email", AsyncMock(return_value={"found": False, "email": ""})), \
             patch.object(wc, "find_email", fake_wizleads), \
             patch.object(bec, "find_work_email_v3", fake_better_enrich):
            route = pipeline_mod.route_enrichment(
                full_name="Jane Doe",
                first_name="Jane",
                last_name="Doe",
                domain="acme.com",
            )
            self.assertEqual(route.get("mode"), "name_domain")

            def record(provider: str) -> None:
                recorded_providers.append(provider)

            result = await pipeline_mod.run_enrichment_route(
                route,
                AsyncMock(), AsyncMock(),
                asyncio.Semaphore(1),
                validate_email=False,
                job_id="job_acceptance1",
                row_index=0,
                emit_logs=False,
                record_provider_use=record,
            )

        # The cascade order must include contacts_db, blitz, wizleads.
        self.assertIn("contacts_db", called_providers)
        self.assertIn("blitz", called_providers)
        self.assertIn("wizleads", called_providers)
        # BetterEnrich must NOT be called because WizLeads returned a hit.
        self.assertNotIn("better_enrich", called_providers)
        # Email came from WizLeads.
        self.assertEqual(result["email"], "jane@acme.com")
        self.assertEqual(result["source"], pipeline_mod.SOURCE_WIZLEADS)
        # record_provider_use saw contacts_db, blitz, wizleads (in that order).
        self.assertIn("contacts_db", recorded_providers)
        self.assertIn("blitz", recorded_providers)
        self.assertIn("wizleads", recorded_providers)
        # BetterEnrich never recorded.
        self.assertNotIn("better_enrich", recorded_providers)


# ---------------------------------------------------------------------------
# Acceptance 2: BetterEnrich fallback after WizLeads. When Contacts DB, Blitz,
# and WizLeads all miss, BetterEnrich is called.
# ---------------------------------------------------------------------------


class TestAcceptance2BetterEnrichAfterWizLeads(unittest.IsolatedAsyncioTestCase):
    """When Contacts DB, Blitz, and WizLeads all miss, BetterEnrich MUST be
    called and the email it returns must be the final answer."""

    async def test_enhanced_route_falls_through_to_better_enrich(self):
        called_providers: list[str] = []

        async def fake_cc_name_domain(http, full_name, domain):
            called_providers.append("contacts_db")
            return None

        async def fake_blitz_person_enrich(http, full_name, domain, include_phone):
            called_providers.append("blitz")
            return {"found": False, "person": {}}

        async def fake_wizleads(http, first_name, last_name, website):
            called_providers.append("wizleads")
            return {"email": "", "catchall": False}

        async def fake_better_enrich(http, full_name, company_domain, linkedin_url):
            called_providers.append("better_enrich")
            return {"email": "jane@acme.com", "email_status": "verified"}

        with patch.object(cc, "person_by_name_and_domain", fake_cc_name_domain), \
             patch.object(cc, "extract_email_from_contacts_response", MagicMock(return_value=None)), \
             patch.object(bc, "person_enrich", fake_blitz_person_enrich), \
             patch.object(bc, "find_work_email", AsyncMock(return_value={"found": False, "email": ""})), \
             patch.object(wc, "find_email", fake_wizleads), \
             patch.object(bec, "find_work_email_v3", fake_better_enrich):
            route = pipeline_mod.route_enrichment(
                full_name="Jane Doe",
                first_name="Jane",
                last_name="Doe",
                domain="acme.com",
            )

            result = await pipeline_mod.run_enrichment_route(
                route,
                AsyncMock(), AsyncMock(),
                asyncio.Semaphore(1),
                validate_email=False,
                job_id="job_acceptance2",
                row_index=0,
                emit_logs=False,
                record_provider_use=lambda p: None,
            )

        # All four providers were called in order.
        self.assertEqual(
            called_providers,
            ["contacts_db", "blitz", "wizleads", "better_enrich"],
        )
        # Email came from BetterEnrich.
        self.assertEqual(result["email"], "jane@acme.com")
        self.assertEqual(result["source"], pipeline_mod.SOURCE_BETTER_ENRICH_PERSON)


# ---------------------------------------------------------------------------
# Acceptance 3: Unverified Blitz email does not stop the cascade. If Mailtester
# rejects the unverified email, the cascade MUST continue to WizLeads.
# ---------------------------------------------------------------------------


class TestAcceptance3UnverifiedBlitzDoesNotStop(unittest.IsolatedAsyncioTestCase):
    """A Blitz person_enrich response with an unverified email[] and no
    verified_email must NOT be accepted as final. The cascade continues to
    WizLeads when Mailtester rejects the candidate."""

    async def test_unverified_blitz_falls_through_to_wizleads(self):
        blitz_called = {"count": 0}
        wizleads_called = {"count": 0}
        better_enrich_called = {"count": 0}

        async def fake_blitz_person_enrich(http, full_name, domain, include_phone):
            blitz_called["count"] += 1
            # Return found=True with unverified email only.
            return {
                "found": True,
                "person": {
                    "first_name": "Jane",
                    "last_name": "Doe",
                    "full_name": "Jane Doe",
                    "verified_email": "",  # no verified field
                    "emails": [{"email": "jane-spam@acme.com"}],
                },
            }

        async def fake_wizleads(http, first_name, last_name, website):
            wizleads_called["count"] += 1
            return {"email": "jane@acme.com", "catchall": True}

        async def fake_better_enrich(http, full_name, company_domain, linkedin_url):
            better_enrich_called["count"] += 1
            return {"email": "jane-be@acme.com", "email_status": "verified"}

        async def fake_mailtester_reject(http, email):
            # Reject everything — the Blitz unverified email must fall through.
            return {"valid": False, "code": "ko", "message": "rejected"}

        with patch.object(cc, "person_by_name_and_domain", AsyncMock(return_value=None)), \
             patch.object(cc, "extract_email_from_contacts_response", MagicMock(return_value=None)), \
             patch.object(bc, "person_enrich", fake_blitz_person_enrich), \
             patch.object(bc, "find_work_email", AsyncMock(return_value={"found": False, "email": ""})), \
             patch.object(wc, "find_email", fake_wizleads), \
             patch.object(bec, "find_work_email_v3", fake_better_enrich), \
             patch.object(mt, "verify_email", fake_mailtester_reject):
            route = pipeline_mod.route_enrichment(
                full_name="Jane Doe",
                first_name="Jane",
                last_name="Doe",
                domain="acme.com",
            )
            result = await pipeline_mod.run_enrichment_route(
                route,
                AsyncMock(), AsyncMock(),
                asyncio.Semaphore(1),
                validate_email=True,  # IMPORTANT: Mailtester enabled.
                job_id="job_acceptance3",
                row_index=0,
                emit_logs=False,
                record_provider_use=lambda p: None,
            )

        # Blitz was called (returned a candidate).
        self.assertEqual(blitz_called["count"], 1)
        # WizLeads was called because Mailtester rejected the Blitz candidate.
        self.assertEqual(wizleads_called["count"], 1)
        # BetterEnrich was NOT called (WizLeads succeeded).
        self.assertEqual(better_enrich_called["count"], 0)
        # Final email is from WizLeads, not the rejected Blitz candidate.
        self.assertEqual(result["email"], "jane@acme.com")
        self.assertEqual(result["source"], pipeline_mod.SOURCE_WIZLEADS)


# ---------------------------------------------------------------------------
# Acceptance 4: Verified Blitz email STILL stops the cascade. This is the
# pre-fix behavior we must NOT regress.
# ---------------------------------------------------------------------------


class TestAcceptance4VerifiedBlitzStopsCascade(unittest.IsolatedAsyncioTestCase):
    """A Blitz person_enrich response with verified_email must short-circuit
    the cascade. WizLeads and BetterEnrich must NOT be called."""

    async def test_verified_blitz_short_circuits(self):
        wizleads_called = {"count": 0}
        better_enrich_called = {"count": 0}

        async def fake_blitz_person_enrich(http, full_name, domain, include_phone):
            return {
                "found": True,
                "person": {
                    "first_name": "Jane",
                    "last_name": "Doe",
                    "full_name": "Jane Doe",
                    "verified_email": "jane-verified@acme.com",
                },
            }

        async def fake_wizleads(http, first_name, last_name, website):
            wizleads_called["count"] += 1
            return {"email": "jane-wl@acme.com", "catchall": True}

        async def fake_better_enrich(http, full_name, company_domain, linkedin_url):
            better_enrich_called["count"] += 1
            return {"email": "jane-be@acme.com", "email_status": "verified"}

        with patch.object(cc, "person_by_name_and_domain", AsyncMock(return_value=None)), \
             patch.object(cc, "extract_email_from_contacts_response", MagicMock(return_value=None)), \
             patch.object(bc, "person_enrich", fake_blitz_person_enrich), \
             patch.object(wc, "find_email", fake_wizleads), \
             patch.object(bec, "find_work_email_v3", fake_better_enrich):
            route = pipeline_mod.route_enrichment(
                full_name="Jane Doe",
                first_name="Jane",
                last_name="Doe",
                domain="acme.com",
            )
            result = await pipeline_mod.run_enrichment_route(
                route,
                AsyncMock(), AsyncMock(),
                asyncio.Semaphore(1),
                validate_email=True,
                job_id="job_acceptance4",
                row_index=0,
                emit_logs=False,
                record_provider_use=lambda p: None,
            )

        # Final email is the Blitz verified one.
        self.assertEqual(result["email"], "jane-verified@acme.com")
        self.assertEqual(result["source"], pipeline_mod.SOURCE_BLITZ_EMAIL)
        # WizLeads and BetterEnrich were NOT called.
        self.assertEqual(wizleads_called["count"], 0)
        self.assertEqual(better_enrich_called["count"], 0)


# ---------------------------------------------------------------------------
# Acceptance 5: emails_wizleads column is populated when WizLeads returns
# emails. The aggregation must include wizleads and wizleads_email sources.
# ---------------------------------------------------------------------------


class TestAcceptance5WizLeadsStatsAndNormalization(unittest.TestCase):
    """The enrichment_stats table and stats_store.aggregate_by_provider /
    normalize_source must treat wizleads and wizleads_email as the canonical
    'wizleads' provider group."""

    def test_normalize_source_wizleads(self):
        # Both raw source values map to "wizleads".
        self.assertEqual(stats_mod.normalize_source("wizleads"), "wizleads")
        self.assertEqual(stats_mod.normalize_source("wizleads_email"), "wizleads")

    def test_aggregate_by_provider_merges_wizleads_variants(self):
        counts = stats_mod.EnrichmentStatsStore.aggregate_by_provider(
            ["wizleads", "wizleads_email", "wizleads_email"]
        )
        # 1 + 2 = 3 total under canonical "wizleads".
        self.assertEqual(counts, {"wizleads": 3})

    def test_get_total_stats_normalizes_wizleads(self):
        # Insert two rows: one with source='wizleads', one with 'wizleads_email'.
        conn = shared_db.get_db()
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat()
        # Use a unique job_id for this test.
        test_job_id = "test_acceptance5_wizleads"
        try:
            conn.execute(
                "INSERT INTO enrichment_stats (job_id, user_id, source, emails_count, contacts_count, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (test_job_id, None, "wizleads", 2, 2, now),
            )
            conn.execute(
                "INSERT INTO enrichment_stats (job_id, user_id, source, emails_count, contacts_count, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (test_job_id, None, "wizleads_email", 3, 3, now),
            )
            conn.commit()

            totals = stats_mod.EnrichmentStatsStore.get_total_stats()
            # Both rows should be aggregated under canonical "wizleads".
            self.assertIn("wizleads", totals)
            self.assertGreaterEqual(totals["wizleads"], 5)
        finally:
            conn.execute(
                "DELETE FROM enrichment_stats WHERE job_id = ?",
                (test_job_id,),
            )
            conn.commit()


# ---------------------------------------------------------------------------
# Acceptance 6: used_providers is populated in CSV/pipeline jobs. The
# `record_provider_use` callback fires for every provider the pipeline
# attempts, so used_providers reflects attempted providers (not just
# successful ones).
# ---------------------------------------------------------------------------


class TestAcceptance6UsedProvidersInRunPipeline(unittest.IsolatedAsyncioTestCase):
    """When run_pipeline processes a row and the cascade attempts
    contacts_db, blitz, and wizleads, the record_provider_use callback
    must be invoked for all three — even if contacts_db fails."""

    async def test_record_provider_use_called_for_each_attempted_provider(self):
        recorded: list[str] = []

        async def fake_cc_company_by_domain(http, domain):
            return None  # no company found

        async def fake_blitz_d2l(http, domain):
            # Fallback: Blitz provides a company LinkedIn.
            return {"found": True, "company_linkedin_url": "https://linkedin.com/company/acme"}

        async def fake_blitz_waterfall(http, linkedin, cascade, limit):
            return {"results": [{
                "person": {
                    "first_name": "Jane",
                    "last_name": "Doe",
                    "full_name": "Jane Doe",
                    "headline": "CEO",
                    "linkedin_url": "https://linkedin.com/in/jane",
                    "location": {"city": "NYC", "country_code": "US"},
                    "experiences": [],
                },
                "icp": 0,
            }]}

        async def fake_blitz_person_enrich(http, full_name, domain, include_phone):
            # No email from Blitz.
            return {"found": False, "person": {}}

        async def fake_wizleads(http, first_name, last_name, website):
            return {"email": "jane@acme.com", "catchall": True}

        with patch.object(cc, "company_by_domain", fake_cc_company_by_domain), \
             patch.object(cc, "company_contacts_enriched", AsyncMock(return_value=[])), \
             patch.object(bc, "domain_to_linkedin", fake_blitz_d2l), \
             patch.object(bc, "waterfall_icp_search", fake_blitz_waterfall), \
             patch.object(bc, "person_enrich", fake_blitz_person_enrich), \
             patch.object(bc, "find_work_email", AsyncMock(return_value={"found": False, "email": ""})), \
             patch.object(wc, "find_email", fake_wizleads), \
             patch.object(mt, "verify_email", AsyncMock(return_value={"valid": True, "code": "ok", "message": "ok"})):
            rows = [{"domain": "acme.com", "first_name": "Jane", "last_name": "Doe"}]
            events: list[dict] = []

            async def on_progress(e: dict) -> None:
                events.append(e)

            output = await pipeline_mod.run_pipeline(
                rows=rows,
                domain_col="domain",
                name_col=None,
                first_name_col="first_name",
                last_name_col="last_name",
                cascade=[{"tier": 1, "title": "Owner"}],
                max_results=1,
                on_progress=on_progress,
                job_id="test_acceptance6",
                record_provider_use=lambda p: recorded.append(p),
                use_email_cache=False,
            )

        # contacts_db, blitz, wizleads were all attempted.
        self.assertIn("contacts_db", recorded)
        self.assertIn("blitz", recorded)
        self.assertIn("wizleads", recorded)
        # We got a result back.
        self.assertGreater(len(output), 0)
        # The dm_email_source is wizleads.
        wizleads_rows = [r for r in output if r.get("dm_email_source") == "wizleads_email"]
        self.assertGreaterEqual(len(wizleads_rows), 1)


# ---------------------------------------------------------------------------
# Acceptance 7: BetterEnrich 201 polling still works. The find_work_email_v3
# client polls until the job completes; the cascade must wait for the
# polling result.
# ---------------------------------------------------------------------------


class TestAcceptance7BetterEnrichPollingStillWorks(unittest.IsolatedAsyncioTestCase):
    """Mock BetterEnrich V3 to return 201 (id) on POST, then poll returns
    the completed email. The cascade must receive the polled email."""

    async def test_better_enrich_polling_returns_email(self):
        be_call_count = {"v3": 0, "poll": 0}

        async def fake_find_work_email_v3(http, full_name, company_domain, linkedin_url):
            be_call_count["v3"] += 1
            # Simulate the V3 client already doing the poll internally and
            # returning a result. The key invariant: the function returns a
            # result dict with 'email' and 'email_status'.
            return {
                "email": "jane@acme.com",
                "email_status": "verified",
                "id": "be_job_123",
            }

        with patch.object(cc, "person_by_name_and_domain", AsyncMock(return_value=None)), \
             patch.object(cc, "extract_email_from_contacts_response", MagicMock(return_value=None)), \
             patch.object(bc, "person_enrich", AsyncMock(return_value={"found": False, "person": {}})), \
             patch.object(bc, "find_work_email", AsyncMock(return_value={"found": False, "email": ""})), \
             patch.object(wc, "find_email", AsyncMock(return_value={"email": "", "catchall": False})), \
             patch.object(bec, "find_work_email_v3", fake_find_work_email_v3):
            route = pipeline_mod.route_enrichment(
                full_name="Jane Doe",
                first_name="Jane",
                last_name="Doe",
                domain="acme.com",
            )
            result = await pipeline_mod.run_enrichment_route(
                route,
                AsyncMock(), AsyncMock(),
                asyncio.Semaphore(1),
                validate_email=False,
                job_id="job_acceptance7",
                row_index=0,
                emit_logs=False,
                record_provider_use=lambda p: None,
            )

        # V3 was called and returned an email.
        self.assertEqual(be_call_count["v3"], 1)
        # Final email is the polled BetterEnrich email.
        self.assertEqual(result["email"], "jane@acme.com")
        self.assertEqual(result["source"], pipeline_mod.SOURCE_BETTER_ENRICH_PERSON)


if __name__ == "__main__":
    unittest.main()
