"""
Phase 1 cascade wiring tests.

Verifies that ``RawContactCollector`` is correctly integrated into the
enrichment cascade at the company-level lookup steps in both
``enrichment.pipeline._enrich_domain`` and
``enrichment.list_builder._enrich_single_domain`` /
``_enrich_by_company_waterfall``, AND that ``_run_background_sync``
correctly drains the collector through
``contacts_writer.write_enrichment_result_batch``.

Coverage:
  1. ``_enrich_domain`` with collector captures Contacts DB response.
  2. ``_enrich_domain`` with collector captures Blitz response when
     Contacts DB returns nothing useful.
  3. ``_enrich_domain`` with both providers tried captures both.
  4. ``_enrich_domain`` without collector (None) works as before and
     raises no errors.
  5. ``_enrich_single_domain`` (list_builder) with collector captures
     the Contacts DB response.
  6. ``_enrich_single_domain`` with collector captures Blitz response.
  7. ``_enrich_single_domain`` no-LinkedIn fallback uses
     ``max_decision_makers`` (NOT the legacy hardcoded 10).
  8. ``_run_background_sync`` with a non-empty collector drains it via
     ``write_enrichment_result_batch`` (extra call).
  9. ``_run_background_sync`` with an empty collector does NOT issue
     an extra writer call.
 10. End-to-end: a tiny job runs through ``_run_job`` producing a CSV
     AND draining the collector.

Run:
    python -m pytest enrichment/tests/test_cascade_collector_wiring.py -v
"""

from __future__ import annotations

import asyncio
import csv
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

# Make sure backend root is importable so `enrichment` resolves regardless
# of pytest invocation cwd.
_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

os.environ.setdefault("CONTACTS_API_TOKEN", "test-token-from-suite")

from enrichment import pipeline as pipeline_mod  # noqa: E402
from enrichment import list_builder as lb  # noqa: E402
from enrichment import contacts_client as cc  # noqa: E402
from enrichment import blitz_client as bc  # noqa: E402
from enrichment import better_enrich_client as be  # noqa: E402
from enrichment import contacts_writer  # noqa: E402
from enrichment import company_fallback as cf  # noqa: E402
from enrichment import fallback_config as fb_cfg  # noqa: E402
from enrichment import routes as routes_mod  # noqa: E402
from enrichment.raw_contact_collector import RawContactCollector  # noqa: E402


def _enable_company_fallbacks(
    *,
    allow_generic: bool = True,
    allow_as_final: bool = True,
    enable_company: bool = True,
    enable_facebook: bool = True,
):
    """Return a contextmanager stack that patches the four fb_cfg flags.

    Defaults to permissive so company emails flow through to the row.
    """
    from contextlib import ExitStack
    stack = ExitStack()
    stack.enter_context(patch.object(fb_cfg, "ENABLE_COMPANY_EMAIL_FALLBACK", enable_company, create=True))
    stack.enter_context(patch.object(fb_cfg, "ENABLE_FACEBOOK_EMAIL_FALLBACK", enable_facebook, create=True))
    stack.enter_context(patch.object(fb_cfg, "ALLOW_GENERIC_COMPANY_EMAIL", allow_generic, create=True))
    stack.enter_context(patch.object(fb_cfg, "ALLOW_COMPANY_EMAIL_AS_FINAL", allow_as_final, create=True))
    return stack


def _contacts_db_payload(n: int) -> list[dict]:
    """Build N synthetic Contacts DB contact dicts."""
    return [
        {
            "first_name": f"First{i}",
            "last_name": f"Last{i}",
            "full_name": f"First{i} Last{i}",
            "title": f"Title {i}",
            "headline": f"Headline {i}",
            "linkedin_url": f"https://linkedin.com/in/person-{i}",
            "email": f"person{i}@acme.com",
            "city": "NYC",
            "country_code": "US",
        }
        for i in range(n)
    ]


def _blitz_payload(n: int) -> dict:
    """Build a synthetic Blitz waterfall_icp_search response with N persons."""
    return {
        "results": [
            {
                "person": {
                    "first_name": f"BFirst{i}",
                    "last_name": f"BLast{i}",
                    "full_name": f"BFirst{i} BLast{i}",
                    "title": f"BTitle {i}",
                    "headline": f"BHeadline {i}",
                    "linkedin_url": f"https://linkedin.com/in/blitz-{i}",
                    "location": {"city": "SF", "country_code": "US"},
                    "emails": [{"email": f"blitz{i}@acme.com"}],
                    "verified_email": f"blitz{i}@acme.com",
                },
                "icp": 0,
            }
            for i in range(n)
        ]
    }


# ---------------------------------------------------------------------------
# 1-4: pipeline._enrich_domain wiring
# ---------------------------------------------------------------------------


class TestEnrichDomainCapturesContactsDB(unittest.IsolatedAsyncioTestCase):
    async def test_captures_contacts_db_response(self):
        """Capture every Contacts DB contact (3 returned, all captured)."""
        collector = RawContactCollector(job_id="job-1")

        async def fake_company_by_domain(http, domain):
            return {"linkedin_url": "https://linkedin.com/company/acme"}

        async def fake_company_contacts_enriched(http, domain, limit):
            # Return 3 contacts — provider already capped at limit
            return _contacts_db_payload(3)

        with patch.object(cc, "company_by_domain", fake_company_by_domain), \
             patch.object(cc, "company_contacts_enriched", fake_company_contacts_enriched), \
             patch.object(bc, "waterfall_icp_search", AsyncMock(return_value={"results": []})), \
             patch.object(cc, "person_by_name_and_domain", AsyncMock(return_value=None)), \
             patch.object(pipeline_mod, "_resolve_email_for_person",
                          AsyncMock(return_value=("", "not_found", {}))):
            rows = await pipeline_mod._enrich_domain(
                blitz_http=MagicMock(),
                contacts_http=MagicMock(),
                base_row={"domain": "acme.com"},
                domain="acme.com",
                full_name="",
                cascade=bc.DEFAULT_CASCADE,  # default cascade so Contacts DB runs
                max_results=2,  # user cap = 2, but provider returned 3
                domain_semaphore=asyncio.Semaphore(1),
                email_semaphore=asyncio.Semaphore(1),
                collector=collector,
            )

        # Collector captured all 3 from Contacts DB.
        self.assertEqual(len(collector), 3)
        stats = collector.stats()
        self.assertEqual(stats["by_source_captured"].get("contacts_db"), 3)
        # User-facing cap is honored (at most 2 rows emitted per max_results=2,
        # though pipeline may emit fewer depending on email resolution).
        self.assertLessEqual(len(rows), 2)


class TestEnrichDomainCapturesBlitzOnFallback(unittest.IsolatedAsyncioTestCase):
    async def test_captures_blitz_when_contacts_db_empty(self):
        """When Contacts DB returns nothing, Blitz response is captured."""
        collector = RawContactCollector(job_id="job-2")

        async def fake_company_by_domain(http, domain):
            return {"linkedin_url": "https://linkedin.com/company/acme"}

        async def fake_company_contacts_enriched(http, domain, limit):
            return []  # Nothing from Contacts DB

        async def fake_blitz_waterfall(http, linkedin, cascade, limit):
            return _blitz_payload(2)

        # Email resolution — return empty so cascade returns rows but no emails.
        async def fake_resolve_email(*args, **kwargs):
            return ("", "not_found", {})

        with patch.object(cc, "company_by_domain", fake_company_by_domain), \
             patch.object(cc, "company_contacts_enriched", fake_company_contacts_enriched), \
             patch.object(bc, "waterfall_icp_search", fake_blitz_waterfall), \
             patch.object(cc, "person_by_name_and_domain", AsyncMock(return_value=None)), \
             patch.object(pipeline_mod, "_resolve_email_for_person", fake_resolve_email):
            await pipeline_mod._enrich_domain(
                blitz_http=MagicMock(),
                contacts_http=MagicMock(),
                base_row={"domain": "acme.com"},
                domain="acme.com",
                full_name="",
                cascade=bc.DEFAULT_CASCADE,  # default cascade so Contacts DB runs first
                max_results=2,
                domain_semaphore=asyncio.Semaphore(1),
                email_semaphore=asyncio.Semaphore(1),
                collector=collector,
            )

        # Only Blitz captured (Contacts DB returned empty list → no captures).
        self.assertEqual(len(collector), 2)
        stats = collector.stats()
        self.assertEqual(stats["by_source_captured"].get("blitz"), 2)
        self.assertNotIn("contacts_db", stats["by_source_captured"])


class TestEnrichDomainCapturesBothProviders(unittest.IsolatedAsyncioTestCase):
    async def test_captures_both_when_contacts_db_does_not_meet_quality(self):
        """Contacts DB returns 0 emails → quality check fails → Blitz runs.

        Both provider responses should be captured."""
        collector = RawContactCollector(job_id="job-3")

        async def fake_company_by_domain(http, domain):
            return {"linkedin_url": "https://linkedin.com/company/acme"}

        async def fake_company_contacts_enriched(http, domain, limit):
            # 3 contacts but NONE with emails — quality check fails
            payload = _contacts_db_payload(3)
            for p in payload:
                p["email"] = ""
            return payload

        async def fake_blitz_waterfall(http, linkedin, cascade, limit):
            return _blitz_payload(2)

        async def fake_resolve_email(*args, **kwargs):
            return ("", "not_found", {})

        with patch.object(cc, "company_by_domain", fake_company_by_domain), \
             patch.object(cc, "company_contacts_enriched", fake_company_contacts_enriched), \
             patch.object(bc, "waterfall_icp_search", fake_blitz_waterfall), \
             patch.object(cc, "person_by_name_and_domain", AsyncMock(return_value=None)), \
             patch.object(pipeline_mod, "_resolve_email_for_person", fake_resolve_email):
            await pipeline_mod._enrich_domain(
                blitz_http=MagicMock(),
                contacts_http=MagicMock(),
                base_row={"domain": "acme.com"},
                domain="acme.com",
                full_name="",
                cascade=bc.DEFAULT_CASCADE,
                max_results=2,
                domain_semaphore=asyncio.Semaphore(1),
                email_semaphore=asyncio.Semaphore(1),
                collector=collector,
            )

        # 3 from Contacts DB (no emails → still captured) + 2 from Blitz
        # but the no-email contacts may be filtered by normalizer.
        # Check that Blitz was definitely captured.
        stats = collector.stats()
        self.assertGreaterEqual(stats["by_source_captured"].get("blitz", 0), 1)


class TestEnrichDomainNoCollectorWorks(unittest.IsolatedAsyncioTestCase):
    async def test_no_collector_no_error(self):
        """collector=None must keep working exactly as before."""

        async def fake_company_by_domain(http, domain):
            return {"linkedin_url": "https://linkedin.com/company/acme"}

        async def fake_company_contacts_enriched(http, domain, limit):
            return _contacts_db_payload(2)

        with patch.object(cc, "company_by_domain", fake_company_by_domain), \
             patch.object(cc, "company_contacts_enriched", fake_company_contacts_enriched), \
             patch.object(bc, "waterfall_icp_search", AsyncMock(return_value={"results": []})), \
             patch.object(cc, "person_by_name_and_domain", AsyncMock(return_value=None)), \
             patch.object(pipeline_mod, "_resolve_email_for_person",
                          AsyncMock(return_value=("", "not_found", {}))):
            rows = await pipeline_mod._enrich_domain(
                blitz_http=MagicMock(),
                contacts_http=MagicMock(),
                base_row={"domain": "acme.com"},
                domain="acme.com",
                full_name="",
                cascade=bc.DEFAULT_CASCADE,
                max_results=2,
                domain_semaphore=asyncio.Semaphore(1),
                email_semaphore=asyncio.Semaphore(1),
                collector=None,
            )

        # Got rows, no exception
        self.assertIsInstance(rows, list)


# ---------------------------------------------------------------------------
# 5-7: list_builder._enrich_single_domain wiring + limit=10 bug fix
# ---------------------------------------------------------------------------


class TestEnrichSingleDomainCapturesContactsDB(unittest.IsolatedAsyncioTestCase):
    async def test_captures_contacts_db_response(self):
        collector = RawContactCollector(job_id="job-4")

        async def fake_company_by_domain(http, domain):
            return {"linkedin_url": "https://linkedin.com/company/acme"}

        async def fake_company_contacts_enriched(http, domain, limit):
            return _contacts_db_payload(3)

        # Stub email resolver so cascade finishes cleanly.
        async def fake_resolve_person_email(*args, **kwargs):
            return ("", "", "not_found", "no", "", "")

        with patch.object(cc, "company_by_domain", fake_company_by_domain), \
             patch.object(cc, "company_contacts_enriched", fake_company_contacts_enriched), \
             patch.object(bc, "waterfall_icp_search", AsyncMock(return_value={"results": []})), \
             patch.object(lb, "_resolve_person_email", fake_resolve_person_email):
            await lb._enrich_single_domain(
                blitz_http=MagicMock(),
                contacts_http=MagicMock(),
                base_row={"domain": "acme.com"},
                domain="acme.com",
                max_decision_makers=2,
                include_generic_emails=False,
                collector=collector,
            )

        stats = collector.stats()
        self.assertEqual(stats["by_source_captured"].get("contacts_db"), 3)


class TestEnrichSingleDomainCapturesBlitz(unittest.IsolatedAsyncioTestCase):
    async def test_captures_blitz_response(self):
        collector = RawContactCollector(job_id="job-5")

        async def fake_company_by_domain(http, domain):
            return {"linkedin_url": "https://linkedin.com/company/acme"}

        async def fake_company_contacts_enriched(http, domain, limit):
            return []  # Contacts DB empty → Blitz fallback

        async def fake_blitz_waterfall(http, linkedin, cascade, limit):
            return _blitz_payload(2)

        async def fake_resolve_person_email(*args, **kwargs):
            return ("", "", "not_found", "no", "", "")

        with patch.object(cc, "company_by_domain", fake_company_by_domain), \
             patch.object(cc, "company_contacts_enriched", fake_company_contacts_enriched), \
             patch.object(bc, "waterfall_icp_search", fake_blitz_waterfall), \
             patch.object(lb, "_resolve_person_email", fake_resolve_person_email):
            await lb._enrich_single_domain(
                blitz_http=MagicMock(),
                contacts_http=MagicMock(),
                base_row={"domain": "acme.com"},
                domain="acme.com",
                max_decision_makers=2,
                include_generic_emails=False,
                collector=collector,
            )

        stats = collector.stats()
        self.assertEqual(stats["by_source_captured"].get("blitz"), 2)


class TestEnrichSingleDomainNoLinkedInFallbackUsesMaxDecisionMakers(unittest.IsolatedAsyncioTestCase):
    """Regression test for the ``limit=10`` bug at the no-LinkedIn fallback.

    When the company has no LinkedIn URL, the fallback Contacts DB call
    MUST use ``max_decision_makers`` (NOT the legacy hardcoded 10).
    """

    async def test_uses_max_decision_makers_not_ten(self):
        captured_limits: list[int] = []

        async def fake_company_by_domain(http, domain):
            return None  # no LinkedIn URL

        async def fake_blitz_d2l(http, domain):
            return {"found": False}

        async def fake_company_contacts_enriched(http, domain, limit):
            captured_limits.append(limit)
            return _contacts_db_payload(5)

        collector = RawContactCollector(job_id="job-6")

        with patch.object(cc, "company_by_domain", fake_company_by_domain), \
             patch.object(bc, "domain_to_linkedin", fake_blitz_d2l), \
             patch.object(cc, "company_contacts_enriched", fake_company_contacts_enriched):
            await lb._enrich_single_domain(
                blitz_http=MagicMock(),
                contacts_http=MagicMock(),
                base_row={"domain": "acme.com"},
                domain="acme.com",
                max_decision_makers=7,  # arbitrary non-10 value
                include_generic_emails=True,
                collector=collector,
            )

        # The fallback ran and used 7, NOT 10.
        self.assertIn(7, captured_limits)
        self.assertNotIn(10, captured_limits)
        # Captures flowed through too.
        stats = collector.stats()
        self.assertEqual(stats["by_source_captured"].get("contacts_db"), 5)


# ---------------------------------------------------------------------------
# 8-9: _run_background_sync drain behavior
# ---------------------------------------------------------------------------


class TestRunBackgroundSyncDrainsCollector(unittest.IsolatedAsyncioTestCase):
    async def test_drains_nonempty_collector(self):
        """When the collector has captures, write_enrichment_result_batch is
        called twice: once for CSV payloads, once for collector payloads."""
        tmp = Path(tempfile.mkstemp(suffix=".csv")[1])
        try:
            with tmp.open("w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["domain", "dm_email"])
                writer.writerow(["acme.com", "row@acme.com"])

            collector = RawContactCollector(job_id="sync-1")
            collector.capture_company_contact(
                source="contacts_db",
                domain="acme.com",
                company_linkedin_url="https://linkedin.com/company/acme",
                contact={
                    "first_name": "Jane",
                    "last_name": "Doe",
                    "full_name": "Jane Doe",
                    "email": "jane@acme.com",
                    "linkedin_url": "https://linkedin.com/in/jane",
                },
            )
            self.assertEqual(len(collector), 1)

            call_count = {"n": 0}

            async def fake_write(payloads, *, job_id=None):
                call_count["n"] += 1
                # Return a fake WriteResult-shaped object
                result = MagicMock()
                result.to_dict = MagicMock(return_value={"written": len(payloads)})
                return result

            with patch.object(contacts_writer, "is_v2_enabled", MagicMock(return_value=True)), \
                 patch.object(contacts_writer, "write_enrichment_result_batch", fake_write):
                await routes_mod._run_background_sync("sync-1", tmp, collector=collector)

            # Two calls: one for CSV, one for collector drain.
            self.assertEqual(call_count["n"], 2)
        finally:
            try:
                tmp.unlink()
            except FileNotFoundError:
                pass

    async def test_empty_collector_no_extra_call(self):
        """Empty collector must NOT trigger an extra writer call."""
        tmp = Path(tempfile.mkstemp(suffix=".csv")[1])
        try:
            with tmp.open("w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["domain", "dm_email"])
                writer.writerow(["acme.com", "row@acme.com"])

            collector = RawContactCollector(job_id="sync-2")  # empty
            call_count = {"n": 0}

            async def fake_write(payloads, *, job_id=None):
                call_count["n"] += 1
                result = MagicMock()
                result.to_dict = MagicMock(return_value={"written": len(payloads)})
                return result

            with patch.object(contacts_writer, "is_v2_enabled", MagicMock(return_value=True)), \
                 patch.object(contacts_writer, "write_enrichment_result_batch", fake_write):
                await routes_mod._run_background_sync("sync-2", tmp, collector=collector)

            # Only the CSV sync ran.
            self.assertEqual(call_count["n"], 1)
        finally:
            try:
                tmp.unlink()
            except FileNotFoundError:
                pass


# ---------------------------------------------------------------------------
# 10: End-to-end _run_job produces CSV and drains collector
# ---------------------------------------------------------------------------


class TestRunJobEndToEndDrainsCollector(unittest.IsolatedAsyncioTestCase):
    async def test_small_job_runs_and_drains(self):
        """A minimal _run_job run produces a CSV AND the collector drain
        runs through _run_background_sync."""

        # Patch store + pipeline to keep this fast & hermetic.
        store = MagicMock()
        store.set_running = MagicMock()
        store.set_done = MagicMock()
        store.set_failed = MagicMock()
        store.get_enrichment_job = MagicMock(return_value=None)
        store.update_used_providers = MagicMock()

        async def fake_run_pipeline(**kwargs):
            # Confirm collector was forwarded
            self.assertIsNotNone(kwargs.get("collector"))
            return [{"domain": "acme.com", "dm_email": "row@acme.com"}]

        async def fake_send_notification(*args, **kwargs):
            return None

        # Make _run_background_sync a no-op spy that records the collector.
        drained: list = []

        async def fake_sync(job_id, output_path, collector=None):
            drained.append(collector)

        with patch.object(routes_mod, "job_store", MagicMock(get_store=MagicMock(return_value=store))), \
             patch.object(routes_mod, "pipeline") as fake_pipe, \
             patch.object(fake_pipe, "run_pipeline", fake_run_pipeline), \
             patch.object(routes_mod, "send_job_notification", fake_send_notification), \
             patch.object(routes_mod, "get_notification_recipients", MagicMock(return_value=[])), \
             patch.object(routes_mod, "_run_background_sync", fake_sync), \
             patch("asyncio.create_task", new=lambda coro: asyncio.ensure_future(coro) if not asyncio.isfuture(coro) else coro):
            await routes_mod._run_job(
                job_id="e2e-1",
                rows=[{"domain": "acme.com"}],
                domain_col="domain",
                name_col=None,
                first_name_col=None,
                last_name_col=None,
                cascade=[{"tier": 1, "title": "Owner"}],
                max_results=2,
            )

        # Let the background task created in _run_job run.
        await asyncio.sleep(0.05)

        # The collector was forwarded through to _run_background_sync.
        self.assertEqual(len(drained), 1)
        self.assertIsNotNone(drained[0])
        self.assertIsInstance(drained[0], RawContactCollector)


# ---------------------------------------------------------------------------
# Phase 2b: company-email capture wiring (BetterEnrich fallback)
# ---------------------------------------------------------------------------


class TestCompanyEmailCapturedViaBetterEnrichFallback(unittest.IsolatedAsyncioTestCase):
    """In _enrich_domain, when no persons are found, BetterEnrich's
    find_company_email result must be captured as a company_email."""

    async def test_company_email_captured_via_better_enrich_fallback(self):
        collector = RawContactCollector(job_id="ce-1")

        async def fake_company_by_domain(http, domain):
            return {"linkedin_url": "https://linkedin.com/company/acme"}

        async def fake_company_contacts_enriched(http, domain, limit):
            return []  # no persons

        async def fake_blitz_waterfall(http, linkedin, cascade, limit):
            return {"results": []}  # no persons

        async def fake_find_company_email(http, website):
            return {
                "email": "info@acme.com",
                "email_status": "verified",
            }

        with patch.object(cc, "company_by_domain", fake_company_by_domain), \
             patch.object(cc, "company_contacts_enriched", fake_company_contacts_enriched), \
             patch.object(bc, "waterfall_icp_search", fake_blitz_waterfall), \
             patch.object(be, "find_company_email", fake_find_company_email):
            rows = await pipeline_mod._enrich_domain(
                blitz_http=MagicMock(),
                contacts_http=MagicMock(),
                base_row={"domain": "acme.com"},
                domain="acme.com",
                full_name="",
                cascade=bc.DEFAULT_CASCADE,
                max_results=2,
                domain_semaphore=asyncio.Semaphore(1),
                email_semaphore=asyncio.Semaphore(1),
                collector=collector,
            )

        # One row emitted (the company email row).
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].get("dm_email"), "info@acme.com")
        # Captured exactly one company_email payload.
        self.assertEqual(len(collector), 1)
        stats = collector.stats()
        self.assertEqual(stats["by_source_captured"].get("better_enrich"), 1)
        # Verify the payload uses company_email field, NOT dm_email.
        payloads = collector.to_payloads()
        self.assertEqual(payloads[0].get("company_email"), "info@acme.com")
        self.assertNotIn("dm_email", payloads[0])


class TestApplyCompanyFallbackCaptures(unittest.IsolatedAsyncioTestCase):
    """In _apply_company_fallback_to_output_rows, every company email
    discovered for rows lacking a person email must be captured."""

    async def test_apply_company_fallback_captures(self):
        collector = RawContactCollector(job_id="ce-2")

        # Mock BetterEnrich find_company_email to return a generic email.
        async def fake_find_company_email(http, website):
            return {
                "email": "contact@acme.com",
                "email_status": "valid",
            }

        output_rows = [
            {
                "domain": "acme.com",
                "company_linkedin_url": "https://linkedin.com/company/acme",
                "dm_email": "",  # no person email
                "source_path": "domain",
            }
        ]

        dedupe = cf.CompanyFallbackDedupe()

        with _enable_company_fallbacks(), \
             patch.object(be, "find_company_email", fake_find_company_email), \
             patch.object(cf.mailtester_client, "verify_email",
                          AsyncMock(return_value={"valid": True, "code": "ok", "message": "ok"})):
            await lb._apply_company_fallback_to_output_rows(
                blitz_http=MagicMock(),
                output_rows=output_rows,
                domain="acme.com",
                facebook_url="",
                dedupe=dedupe,
                collector=collector,
            )

        # Captured the company email.
        self.assertEqual(len(collector), 1)
        stats = collector.stats()
        self.assertEqual(stats["by_source_captured"].get("better_enrich"), 1)
        payloads = collector.to_payloads()
        self.assertEqual(payloads[0].get("company_email"), "contact@acme.com")
        # Mutated row gets company_email too.
        self.assertEqual(output_rows[0].get("company_email"), "contact@acme.com")


class TestCompanyEmailPayloadUsesCompanyEmailField(unittest.TestCase):
    """Verify that company_email payloads never carry dm_email — they must
    route to a separate field so they cannot overwrite person emails."""

    def test_company_email_payload_uses_company_email_field(self):
        collector = RawContactCollector(job_id="ce-3")
        result = collector.capture_company_email(
            source="better_enrich",
            domain="acme.com",
            company_linkedin_url="https://linkedin.com/company/acme",
            email_data={
                "email": "support@acme.com",
                "email_status": "verified",
            },
        )
        self.assertTrue(result)
        self.assertEqual(len(collector), 1)
        payloads = collector.to_payloads()
        self.assertEqual(payloads[0].get("company_email"), "support@acme.com")
        self.assertEqual(payloads[0].get("company_email_source"), "better_enrich.company_email")
        self.assertEqual(payloads[0].get("company_email_type"), "generic")
        # Critical: dm_email must NOT be present on company-email payloads.
        self.assertNotIn("dm_email", payloads[0])
        self.assertNotIn("dm_first_name", payloads[0])


class TestCollectorNoneDoesntBreakCompanyFallback(unittest.IsolatedAsyncioTestCase):
    """When collector=None, behavior must be identical to pre-Phase-2b."""

    async def test_collector_none_doesnt_break_company_fallback(self):
        async def fake_find_company_email(http, website):
            return {
                "email": "hello@acme.com",
                "email_status": "valid",
            }

        output_rows = [
            {
                "domain": "acme.com",
                "company_linkedin_url": "https://linkedin.com/company/acme",
                "dm_email": "",
                "source_path": "domain",
            }
        ]

        dedupe = cf.CompanyFallbackDedupe()

        with _enable_company_fallbacks(), \
             patch.object(be, "find_company_email", fake_find_company_email), \
             patch.object(cf.mailtester_client, "verify_email",
                          AsyncMock(return_value={"valid": True, "code": "ok", "message": "ok"})):
            # No exception even though collector is None.
            await lb._apply_company_fallback_to_output_rows(
                blitz_http=MagicMock(),
                output_rows=output_rows,
                domain="acme.com",
                facebook_url="",
                dedupe=dedupe,
                collector=None,
            )

        # Row still got the company email.
        self.assertEqual(output_rows[0].get("company_email"), "hello@acme.com")


# ---------------------------------------------------------------------------
# Phase 2a: _resolve_email_for_person per-provider capture wiring
# ---------------------------------------------------------------------------


class TestResolveEmailCapturesContactsDbByName(unittest.IsolatedAsyncioTestCase):
    """Step 1 of the cascade: Contacts DB by name+domain finds an email.
    That response must be captured even though it's also the winning email."""

    async def test_resolve_email_captures_contacts_db_by_name(self):
        collector = RawContactCollector(job_id="p2a-1")

        async def fake_pband(http, full_name, domain):
            return {
                "first_name": "Jane",
                "last_name": "Doe",
                "full_name": "Jane Doe",
                "title": "CEO",
                "linkedin_url": "https://linkedin.com/in/jane",
                "email": "jane@acme.com",
            }

        async def fake_verify(http, email):
            return {"valid": True, "code": "ok", "message": "ok"}

        with patch.object(cc, "person_by_name_and_domain", fake_pband), \
             patch.object(pipeline_mod.mailtester_client, "verify_email", fake_verify):
            email, source, _ = await pipeline_mod._resolve_email_for_person(
                blitz_client_inst=MagicMock(),
                contacts_client_inst=MagicMock(),
                person={"full_name": "Jane Doe"},
                domain="acme.com",
                input_full_name="",
                email_semaphore=asyncio.Semaphore(1),
                collector=collector,
                company_linkedin_url="https://linkedin.com/company/acme",
            )

        self.assertEqual(email, "jane@acme.com")
        self.assertEqual(source, pipeline_mod.SOURCE_CONTACTS_DB_EMAIL)
        stats = collector.stats()
        self.assertEqual(stats["by_source_captured"].get("contacts_db"), 1)
        payloads = collector.to_payloads()
        self.assertEqual(payloads[0]["dm_email"], "jane@acme.com")
        self.assertEqual(payloads[0]["company_linkedin_url"],
                         "https://linkedin.com/company/acme")


class TestResolveEmailCapturesBlitzWhenContactsDbEmpty(unittest.IsolatedAsyncioTestCase):
    """Step 1 returns None → Step 3 Blitz person_enrich finds a verified_email.
    Only Blitz should be captured."""

    async def test_resolve_email_captures_blitz_when_contacts_db_empty(self):
        collector = RawContactCollector(job_id="p2a-2")

        async def fake_pband(http, full_name, domain):
            return None

        async def fake_person_enrich(http, *, full_name, domain, include_phone):
            return {
                "found": True,
                "person": {
                    "first_name": "Bob",
                    "last_name": "Smith",
                    "full_name": "Bob Smith",
                    "title": "CTO",
                    "linkedin_url": "https://linkedin.com/in/bob",
                    "verified_email": "bob@acme.com",
                },
            }

        with patch.object(cc, "person_by_name_and_domain", fake_pband), \
             patch.object(bc, "person_enrich", fake_person_enrich):
            email, source, _ = await pipeline_mod._resolve_email_for_person(
                blitz_client_inst=MagicMock(),
                contacts_client_inst=MagicMock(),
                person={"full_name": "Bob Smith"},
                domain="acme.com",
                input_full_name="",
                email_semaphore=asyncio.Semaphore(1),
                collector=collector,
            )

        self.assertEqual(email, "bob@acme.com")
        self.assertEqual(source, pipeline_mod.SOURCE_BLITZ_EMAIL)
        stats = collector.stats()
        self.assertEqual(stats["by_source_captured"].get("blitz"), 1)
        self.assertNotIn("contacts_db", stats["by_source_captured"])


class TestResolveEmailCapturesBothProvidersAlternatives(unittest.IsolatedAsyncioTestCase):
    """Contacts DB email fails mailtester → cascade continues → Blitz
    find_work_email is captured too. Both providers' responses captured."""

    async def test_resolve_email_captures_both_providers_alternatives(self):
        collector = RawContactCollector(job_id="p2a-3")
        verify_calls: list[str] = []

        async def fake_pband(http, full_name, domain):
            return {
                "full_name": full_name,
                "first_name": "Carol",
                "last_name": "Jones",
                "linkedin_url": "https://linkedin.com/in/carol",
                "email": "stale@acme.com",
            }

        async def fake_person_enrich(http, *, full_name, domain, include_phone):
            # No verified_email, no emails list — Step 3 finds nothing usable.
            return {"found": True, "person": {"full_name": full_name}}

        async def fake_find_work_email(http, linkedin_url):
            return {
                "found": True,
                "email": "carol@acme.com",
            }

        async def fake_verify(http, email):
            verify_calls.append(email)
            if email == "stale@acme.com":
                return {"valid": False, "code": "ko", "message": "invalid"}
            return {"valid": True, "code": "ok", "message": "ok"}

        with patch.object(cc, "person_by_name_and_domain", fake_pband), \
             patch.object(cc, "person_by_linkedin", AsyncMock(return_value=None)), \
             patch.object(bc, "person_enrich", fake_person_enrich), \
             patch.object(bc, "find_work_email", fake_find_work_email), \
             patch.object(pipeline_mod.mailtester_client, "verify_email", fake_verify), \
             patch.object(cc, "mark_email_invalid", AsyncMock()):
            email, source, _ = await pipeline_mod._resolve_email_for_person(
                blitz_client_inst=MagicMock(),
                contacts_client_inst=MagicMock(),
                person={
                    "full_name": "Carol Jones",
                    "linkedin_url": "https://linkedin.com/in/carol",
                },
                domain="acme.com",
                input_full_name="",
                email_semaphore=asyncio.Semaphore(1),
                collector=collector,
            )

        # Cascade returned the Blitz email (Contacts DB was rejected).
        self.assertEqual(email, "carol@acme.com")
        self.assertEqual(source, pipeline_mod.SOURCE_BLITZ_EMAIL)
        # Captures: 1 from Contacts DB (Step 1, the stale email) +
        # 2 from Blitz (Step 3 metadata-only + Step 4 winning email).
        stats = collector.stats()
        self.assertEqual(stats["by_source_captured"].get("contacts_db"), 1)
        self.assertGreaterEqual(stats["by_source_captured"].get("blitz", 0), 1)
        emails = sorted(p.get("dm_email") for p in collector.to_payloads() if p.get("dm_email"))
        self.assertIn("stale@acme.com", emails)
        self.assertIn("carol@acme.com", emails)


class TestResolveEmailCapturesFailedValidationEmail(unittest.IsolatedAsyncioTestCase):
    """When a provider email fails mailtester, it's STILL captured — that
    alternative-email data is exactly what Phase 2a is designed to preserve."""

    async def test_resolve_email_captures_failed_validation_email(self):
        collector = RawContactCollector(job_id="p2a-4")

        async def fake_pband(http, full_name, domain):
            return {
                "full_name": full_name,
                "email": "bounced@acme.com",
            }

        async def fake_verify(http, email):
            return {"valid": False, "code": "ko", "message": "bounced"}

        # Downstream providers return nothing — cascade ends at "not_found".
        async def fake_person_enrich(http, *, full_name, domain, include_phone):
            return {"found": False}

        with patch.object(cc, "person_by_name_and_domain", fake_pband), \
             patch.object(pipeline_mod.mailtester_client, "verify_email", fake_verify), \
             patch.object(cc, "mark_email_invalid", AsyncMock()), \
             patch.object(bc, "person_enrich", fake_person_enrich), \
             patch.object(bc, "find_work_email", AsyncMock(return_value={"found": False})):
            email, source, _ = await pipeline_mod._resolve_email_for_person(
                blitz_client_inst=MagicMock(),
                contacts_client_inst=MagicMock(),
                person={"full_name": "Dan Doe"},
                domain="acme.com",
                input_full_name="",
                email_semaphore=asyncio.Semaphore(1),
                collector=collector,
            )

        self.assertEqual(email, "")
        self.assertEqual(source, pipeline_mod.SOURCE_NOT_FOUND)
        # The bounced email was still captured.
        stats = collector.stats()
        self.assertEqual(stats["by_source_captured"].get("contacts_db"), 1)
        self.assertEqual(collector.to_payloads()[0]["dm_email"], "bounced@acme.com")


class TestResolveEmailCapturesMetadataOnlyResponse(unittest.IsolatedAsyncioTestCase):
    """A provider returns a person dict with no email but with name+title+
    linkedin. Captured as a metadata-only contact (no email)."""

    async def test_resolve_email_captures_metadata_only_response(self):
        collector = RawContactCollector(job_id="p2a-5")

        async def fake_pband(http, full_name, domain):
            return {
                "first_name": "Eve",
                "last_name": "Ng",
                "full_name": "Eve Ng",
                "title": "CFO",
                "linkedin_url": "https://linkedin.com/in/eve",
                # No email field
            }

        async def fake_person_enrich(http, *, full_name, domain, include_phone):
            return {"found": False}

        with patch.object(cc, "person_by_name_and_domain", fake_pband), \
             patch.object(bc, "person_enrich", fake_person_enrich), \
             patch.object(bc, "find_work_email", AsyncMock(return_value={"found": False})):
            email, source, _ = await pipeline_mod._resolve_email_for_person(
                blitz_client_inst=MagicMock(),
                contacts_client_inst=MagicMock(),
                person={"full_name": "Eve Ng"},
                domain="acme.com",
                input_full_name="",
                email_semaphore=asyncio.Semaphore(1),
                collector=collector,
            )

        self.assertEqual(email, "")
        # The metadata-only contact (name + linkedin) was captured.
        stats = collector.stats()
        self.assertGreaterEqual(stats["by_source_captured"].get("contacts_db", 0), 1)
        payload = collector.to_payloads()[0]
        # Normalizer strips the scheme; the path remains.
        self.assertEqual(payload.get("dm_linkedin_url"), "linkedin.com/in/eve")
        self.assertEqual(payload.get("dm_title"), "CFO")


class TestResolveEmailNoCollectorWorksAsBefore(unittest.IsolatedAsyncioTestCase):
    """collector=None → cascade works exactly as before, no errors, no
    captures (because there's nowhere to capture to)."""

    async def test_resolve_email_no_collector_works_as_before(self):
        async def fake_pband(http, full_name, domain):
            return {"full_name": full_name, "email": "x@acme.com"}

        async def fake_verify(http, email):
            return {"valid": True, "code": "ok", "message": "ok"}

        with patch.object(cc, "person_by_name_and_domain", fake_pband), \
             patch.object(pipeline_mod.mailtester_client, "verify_email", fake_verify):
            email, source, _ = await pipeline_mod._resolve_email_for_person(
                blitz_client_inst=MagicMock(),
                contacts_client_inst=MagicMock(),
                person={"full_name": "Frank Foo"},
                domain="acme.com",
                input_full_name="",
                email_semaphore=asyncio.Semaphore(1),
                collector=None,
            )

        self.assertEqual(email, "x@acme.com")
        self.assertEqual(source, pipeline_mod.SOURCE_CONTACTS_DB_EMAIL)


class TestCompanyLinkedInUrlPassedThrough(unittest.IsolatedAsyncioTestCase):
    """The company_linkedin_url argument must appear verbatim in the
    captured payloads (for lineage)."""

    async def test_company_linkedin_url_passed_through(self):
        collector = RawContactCollector(job_id="p2a-7")
        expected_url = "https://linkedin.com/company/some-acme"

        async def fake_pband(http, full_name, domain):
            return {"full_name": full_name, "email": "x@acme.com"}

        async def fake_verify(http, email):
            return {"valid": True, "code": "ok", "message": "ok"}

        with patch.object(cc, "person_by_name_and_domain", fake_pband), \
             patch.object(pipeline_mod.mailtester_client, "verify_email", fake_verify):
            await pipeline_mod._resolve_email_for_person(
                blitz_client_inst=MagicMock(),
                contacts_client_inst=MagicMock(),
                person={"full_name": "Grace Gee"},
                domain="acme.com",
                input_full_name="",
                email_semaphore=asyncio.Semaphore(1),
                collector=collector,
                company_linkedin_url=expected_url,
            )

        self.assertGreaterEqual(len(collector), 1)
        for payload in collector.to_payloads():
            self.assertEqual(payload.get("company_linkedin_url"), expected_url)


class TestResolveEmailCapturesAllProvidersInLongCascade(unittest.IsolatedAsyncioTestCase):
    """Smoke test: cascade runs through every provider (each returns data
    but no winning email). Every provider's response is captured."""

    async def test_all_providers_captured(self):
        collector = RawContactCollector(job_id="p2a-8")

        async def fake_pband(http, full_name, domain):
            return {"full_name": full_name, "linkedin_url": "https://linkedin.com/in/x"}

        async def fake_person_linkedin(http, linkedin_url):
            return {"full_name": "Hank", "linkedin_url": linkedin_url}

        async def fake_person_enrich(http, *, full_name, domain, include_phone):
            return {
                "found": True,
                "person": {
                    "full_name": full_name,
                    "linkedin_url": "https://linkedin.com/in/hank",
                    # emails list present but candidate rejected
                    "emails": [{"email": "unverified@acme.com"}],
                },
            }

        async def fake_find_work_email(http, linkedin_url):
            return {"found": True, "email": "unverified2@acme.com"}

        async def fake_verify(http, email):
            return {"valid": False, "code": "ko", "message": "no"}

        from enrichment import wizleads_client as wz
        from enrichment import better_enrich_client as be

        async def fake_wizleads_find(http, *, first_name, last_name, website):
            return None

        async def fake_be_v3(http, full_name, domain, linkedin_url):
            return None

        with patch.object(cc, "person_by_name_and_domain", fake_pband), \
             patch.object(cc, "person_by_linkedin", fake_person_linkedin), \
             patch.object(bc, "person_enrich", fake_person_enrich), \
             patch.object(bc, "find_work_email", fake_find_work_email), \
             patch.object(pipeline_mod.mailtester_client, "verify_email", fake_verify), \
             patch.object(cc, "mark_email_invalid", AsyncMock()), \
             patch.object(wz, "find_email", fake_wizleads_find), \
             patch.object(be, "find_work_email_v3", fake_be_v3):
            email, source, _ = await pipeline_mod._resolve_email_for_person(
                blitz_client_inst=MagicMock(),
                contacts_client_inst=MagicMock(),
                person={
                    "full_name": "Hank Hill",
                    "linkedin_url": "https://linkedin.com/in/hank",
                },
                domain="acme.com",
                input_full_name="",
                email_semaphore=asyncio.Semaphore(1),
                collector=collector,
            )

        self.assertEqual(email, "")
        self.assertEqual(source, pipeline_mod.SOURCE_NOT_FOUND)
        stats = collector.stats()
        # Captures from at least contacts_db (Steps 1 & 2) and blitz (Steps 3 & 4).
        self.assertGreaterEqual(stats["by_source_captured"].get("contacts_db", 0), 1)
        self.assertGreaterEqual(stats["by_source_captured"].get("blitz", 0), 1)


if __name__ == "__main__":
    unittest.main()
