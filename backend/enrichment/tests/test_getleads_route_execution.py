"""
Regression test: GetLeads must EXECUTE (not merely be listed) on the
``run_enrichment_route`` path — the executor backing the unified
POST/GET ``/api/enrichment/enrich`` endpoint (routes.py:2895, 3144).

Background: the initial mirror wiring added GetLeads to the ``route_enrichment``
*plan* and to ``_resolve_email_for_person`` / ``_resolve_person_email``, but
missed the ``_run_route_step`` dispatch that ``run_enrichment_route`` uses to
actually invoke each provider. Without a getleads branch in ``_run_route_step``,
GetLeads was silently skipped (fell through to SOURCE_NOT_FOUND) on the /enrich
path even though it appeared in the cascade plan. This test locks the fix in and
verifies the cascade order (Blitz -> GetLeads -> SmartProspect).

All provider calls are mocked — no credits spent.
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
from enrichment import contacts_client as cc  # noqa: E402
from enrichment import blitz_client as bc  # noqa: E402
from enrichment import getleads_client as gl  # noqa: E402
from enrichment import smartprospect_client as sc  # noqa: E402
from enrichment import wizleads_client as wc  # noqa: E402
from enrichment import better_enrich_client as bec  # noqa: E402


class TestGetLeadsExecutesInRunEnrichmentRoute(unittest.IsolatedAsyncioTestCase):
    """GetLeads fires after Blitz and before SmartProspect on the /enrich path."""

    async def test_getleads_fires_after_blitz_before_smartprospect(self):
        called: list[str] = []

        async def fake_cc_name_domain(http, full_name, domain):
            called.append("contacts_db")
            return None

        async def fake_blitz_person_enrich(http, full_name, domain, include_phone):
            called.append("blitz")
            return {"found": False, "person": {}}

        async def fake_getleads(http, first_name, last_name, company_domain):
            called.append("getleads")
            return {
                "email": "jane@acme.com",
                "first_name": "Jane",
                "last_name": "Doe",
                "domain": "acme.com",
                "verification_status": "Valid",
                "linkedin_url": "https://www.linkedin.com/in/jane-doe",
                "phone": "",
            }

        async def fake_smartprospect(http, first_name, last_name, company_domain):
            called.append("smartprospect")
            return None

        async def fake_wizleads(http, first_name, last_name, website):
            called.append("wizleads")
            return None

        async def fake_better_enrich(http, full_name, company_domain, linkedin_url):
            called.append("better_enrich")
            return None

        with patch.object(cc, "person_by_name_and_domain", fake_cc_name_domain), \
             patch.object(cc, "extract_email_from_contacts_response", MagicMock(return_value=None)), \
             patch.object(bc, "person_enrich", fake_blitz_person_enrich), \
             patch.object(bc, "find_work_email", AsyncMock(return_value={"found": False, "email": ""})), \
             patch.object(gl, "find_email", fake_getleads), \
             patch.object(sc, "find_email", fake_smartprospect), \
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
                pass

            result = await pipeline_mod.run_enrichment_route(
                route,
                AsyncMock(), AsyncMock(),
                asyncio.Semaphore(1),
                validate_email=False,
                job_id="job_getleads_route",
                row_index=0,
                emit_logs=False,
                record_provider_use=record,
            )

        # GetLeads EXECUTED (not skipped) and produced the email.
        self.assertIn("getleads", called)
        self.assertEqual(result["email"], "jane@acme.com")
        self.assertEqual(result["source"], pipeline_mod.SOURCE_GETLEADS)
        # Cascade order: GetLeads AFTER Blitz.
        self.assertIn("blitz", called)
        self.assertLess(called.index("blitz"), called.index("getleads"))
        # A GetLeads hit short-circuits -> later providers never run.
        self.assertNotIn("smartprospect", called)
        self.assertNotIn("wizleads", called)
        self.assertNotIn("better_enrich", called)


if __name__ == "__main__":
    unittest.main()
