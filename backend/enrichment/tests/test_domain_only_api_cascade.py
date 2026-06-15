"""
Acceptance tests for the GET /api/enrichment/enrich/ domain_only branch.

The user reported that when Clay calls
  GET https://listbuilding.eagleinfoservice.com/api/enrichment/enrich/?domain=...
it only ever returns `contacts_db` and `blitz` as sources — never
`wizleads` and never `better_enrich`. The expected cascade per LOOP.md
is Contacts DB -> Blitz -> WizLeads -> BetterEnrich.

Root cause: the `domain_only` branch in
`backend/enrichment/routes.py::_unified_enrich_logic` re-implements the
cascade inline (its `find_email_for_person()` only calls contacts_db
and blitz) instead of delegating to `pipeline.route_enrichment` /
`pipeline.run_enrichment_route` like `linkedin_only` and `enhanced`
modes do.

These tests assert the desired cascade order and the `force_provider`
semantics for that branch. All provider calls are mocked — no
production provider credits are spent.
"""

from __future__ import annotations

import asyncio
import os
import sys
import unittest
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from enrichment import routes as routes_mod  # noqa: E402
from enrichment import contacts_client as cc  # noqa: E402
from enrichment import blitz_client as bc  # noqa: E402
from enrichment import wizleads_client as wc  # noqa: E402
from enrichment import better_enrich_client as bec  # noqa: E402
from enrichment import mailtester_client as mt  # noqa: E402
from enrichment import contacts_writer as contacts_writer_mod  # noqa: E402


def _fake_user() -> dict[str, Any]:
    return {"id": 1, "user_id": 1, "email": "test@example.com", "is_admin": True}


def _build_decision_maker_contact() -> list[dict[str, Any]]:
    """A single decision-maker contact with name splits (so WizLeads and
    BetterEnrich are eligible). The contact comes from
    contacts_db.company_contacts_enriched when Clay's GET request hits
    the domain_only branch.

    `linkedin_url` is intentionally empty: the new per-person cascade
    prefers the name+domain route when first_name + last_name + domain
    are all present (route_enrichment then produces contacts_db ->
    blitz -> wizleads -> better_enrich). With a linkedin URL set, the
    routing layer takes a LinkedIn-first path that doesn't include
    WizLeads."""
    return [
        {
            "full_name": "Jane Doe",
            "first_name": "Jane",
            "last_name": "Doe",
            "title": "CEO",
            "linkedin_url": "",
            "headline": "CEO at Acme",
            "location_city": "NY",
            "location_country": "US",
            "icp_tier": 1,
            "email_source": "blitz",
        }
    ]


def _build_req(domain: str = "acme.com", force_provider: str | None = None) -> Any:
    """Build a domain-only request — the shape Clay sends.

    Critically: no `full_name`, `first_name`, `last_name`, or
    `linkedin_url` are passed. The mode detection at
    routes.py:1595-1600 will classify this as `domain_only`."""
    return routes_mod.UnifiedEnrichRequest(
        domain=domain,
        max_results=5,
        force_provider=force_provider,
    )


# Mock factories — flexible signatures because the cascade calls each
# provider with a slightly different positional/keyword argument shape.
def _flex_async(return_value: Any):
    async def _fn(*args, **kwargs):
        return return_value
    return _fn


# ---------------------------------------------------------------------------
# Helpers shared across tests
# ---------------------------------------------------------------------------


def _patch_all_providers(mocks: dict[str, Any]):
    """Patch all 4 provider clients + writer + mailtester with the
    given mock callables. `mocks` is a dict of
    `module.func_name -> AsyncMock`."""
    patches = []
    for mod_attr, fn in mocks.items():
        mod_name, attr = mod_attr.split(".", 1)
        mod = {
            "cc": cc,
            "bc": bc,
            "wc": wc,
            "bec": bec,
            "mt": mt,
            "contacts_writer_mod": contacts_writer_mod,
        }[mod_name]
        patches.append(patch.object(mod, attr, fn))
    return patches


# ---------------------------------------------------------------------------
# Acceptance A: domain_only -> all 4 providers reached
# ---------------------------------------------------------------------------


class TestDomainOnlyCallsAllFourProviders(unittest.IsolatedAsyncioTestCase):
    """GET /api/enrichment/enrich/?domain=... (no name) must call
    Contacts DB, Blitz, WizLeads, and BetterEnrich when the per-person
    loop runs with name splits."""

    async def test_domain_only_falls_through_to_wizleads_and_better_enrich(self):
        called_providers: list[str] = []

        def make_recorder(name):
            async def _fn(*args, **kwargs):
                called_providers.append(name)
                if name == "wizleads__find_email":
                    return {"email": "", "catchall": False}
                if name == "better_enrich__find_work_email_v3":
                    return {"email": "jane@acme.com", "email_status": "verified"}
                if name.startswith("blitz__person_enrich") or name == "blitz__person_enrich":
                    return {"found": False, "person": {}}
                if name == "blitz__waterfall":
                    return {"results": []}
                if name == "blitz__domain_to_linkedin":
                    return {"company_linkedin_url": ""}
                if name == "blitz__find_work_email":
                    return {"found": False, "email": ""}
                if name == "blitz__person_enrich_by_linkedin":
                    return {"found": False, "email": ""}
                if name.startswith("contacts_db__"):
                    return None
                if name == "better_enrich__company_email":
                    return {"email": ""}
                return None
            return _fn

        mocks = {
            "cc.company_by_domain": make_recorder("contacts_db__company_by_domain"),
            "cc.company_contacts_enriched": make_recorder("contacts_db__company_contacts"),
            "cc.person_by_linkedin": make_recorder("contacts_db__person_by_linkedin"),
            "cc.person_by_name_and_domain": make_recorder("contacts_db__name_domain"),
            "bc.waterfall_icp_search": make_recorder("blitz__waterfall"),
            "bc.domain_to_linkedin": make_recorder("blitz__domain_to_linkedin"),
            "bc.person_enrich": make_recorder("blitz__person_enrich"),
            "bc.person_enrich_by_linkedin": make_recorder("blitz__person_enrich_by_linkedin"),
            "bc.find_work_email": make_recorder("blitz__find_work_email"),
            "wc.find_email": make_recorder("wizleads__find_email"),
            "bec.find_work_email_v3": make_recorder("better_enrich__find_work_email_v3"),
            "bec.find_company_email": make_recorder("better_enrich__company_email"),
            "mt.verify_email": _flex_async({"valid": True, "code": "valid", "message": "ok"}),
            "contacts_writer_mod.write_enrichment_result": _flex_async(
                contacts_writer_mod.WriteStatus.SKIPPED
            ),
        }

        # Force the contact-list path to return a real decision maker.
        # We patch the inner function after import to keep test simple.
        async def fake_company_contacts(http, dom, max_results):
            called_providers.append("contacts_db__company_contacts")
            return _build_decision_maker_contact()

        mocks["cc.company_contacts_enriched"] = fake_company_contacts

        patches = _patch_all_providers(mocks)
        for p in patches:
            p.start()
        try:
            # cc.extract_email_from_contacts_response returns None for None input.
            with patch.object(
                cc, "extract_email_from_contacts_response", MagicMock(return_value=None)
            ):
                response = await routes_mod._unified_enrich_logic(
                    _build_req(), _fake_user(), debug=True
                )
        finally:
            for p in patches:
                p.stop()

        # WizLeads and BetterEnrich must have been called in the
        # per-person email loop.
        self.assertIn("wizleads__find_email", called_providers,
                      f"wizleads not called. called: {called_providers}")
        self.assertIn("better_enrich__find_work_email_v3", called_providers,
                      f"better_enrich not called. called: {called_providers}")
        # Final email must come from BetterEnrich.
        self.assertEqual(response["contacts"][0]["email"], "jane@acme.com")
        self.assertEqual(response["contacts"][0]["email_source"], "better_enrich_person")
        # The routing block must show all 4 providers were considered.
        called = response["routing"]["providers_called"]
        for p in ("contacts_db", "blitz", "wizleads", "better_enrich"):
            self.assertIn(p, called, f"{p} missing from providers_called: {called}")

    async def test_domain_only_uses_wizleads_when_others_miss(self):
        """When Contacts DB and Blitz miss, but WizLeads finds an email,
        BetterEnrich must NOT be called."""

        called_providers: list[str] = []

        async def fake_company_by_domain(http, dom):
            return {"linkedin_url": "https://linkedin.com/company/acme"}

        async def fake_company_contacts(http, dom, max_results):
            return _build_decision_maker_contact()

        async def fake_wizleads(*args, **kwargs):
            called_providers.append("wizleads")
            return {"email": "jane@acme.com", "catchall": True}

        async def fake_better_enrich(*args, **kwargs):
            called_providers.append("better_enrich")
            return {"email": "should-not-be-used@acme.com", "email_status": "verified"}

        mocks = {
            "cc.company_by_domain": fake_company_by_domain,
            "cc.company_contacts_enriched": fake_company_contacts,
            "cc.person_by_linkedin": _flex_async(None),
            "cc.person_by_name_and_domain": _flex_async(None),
            "bc.waterfall_icp_search": _flex_async({"results": []}),
            "bc.domain_to_linkedin": _flex_async({"company_linkedin_url": ""}),
            "bc.person_enrich": _flex_async({"found": False, "person": {}}),
            "bc.person_enrich_by_linkedin": _flex_async({"found": False, "email": ""}),
            "bc.find_work_email": _flex_async({"found": False, "email": ""}),
            "wc.find_email": fake_wizleads,
            "bec.find_work_email_v3": fake_better_enrich,
            "bec.find_company_email": _flex_async({"email": ""}),
            "mt.verify_email": _flex_async({"valid": True, "code": "valid", "message": "ok"}),
            "contacts_writer_mod.write_enrichment_result": _flex_async(
                contacts_writer_mod.WriteStatus.SKIPPED
            ),
        }
        patches = _patch_all_providers(mocks)
        for p in patches:
            p.start()
        try:
            with patch.object(
                cc, "extract_email_from_contacts_response", MagicMock(return_value=None)
            ):
                response = await routes_mod._unified_enrich_logic(
                    _build_req(), _fake_user(), debug=True
                )
        finally:
            for p in patches:
                p.stop()

        # WizLeads reached; BetterEnrich not reached.
        self.assertIn("wizleads", called_providers)
        self.assertNotIn("better_enrich", called_providers)
        # Final email came from WizLeads.
        self.assertEqual(response["contacts"][0]["email"], "jane@acme.com")
        self.assertEqual(response["contacts"][0]["email_source"], "wizleads_email")


# ---------------------------------------------------------------------------
# Acceptance B: force_provider=contacts_db skips WizLeads and BetterEnrich
# ---------------------------------------------------------------------------


class TestDomainOnlyForceProviderContactsDb(unittest.IsolatedAsyncioTestCase):
    async def test_force_provider_contacts_db_skips_paid(self):
        called_providers: list[str] = []

        async def fake_company_by_domain(http, dom):
            return {"linkedin_url": "https://linkedin.com/company/acme"}

        async def fake_company_contacts(http, dom, max_results):
            return _build_decision_maker_contact()

        async def fake_blitz(*args, **kwargs):
            called_providers.append("blitz")
            return {"found": False, "person": {}}

        async def fake_wizleads(*args, **kwargs):
            called_providers.append("wizleads")
            return {"email": "should-not-be-used@acme.com", "catchall": True}

        async def fake_better_enrich(*args, **kwargs):
            called_providers.append("better_enrich")
            return {"email": "should-not-be-used@acme.com", "email_status": "verified"}

        mocks = {
            "cc.company_by_domain": fake_company_by_domain,
            "cc.company_contacts_enriched": fake_company_contacts,
            "cc.person_by_linkedin": _flex_async(None),
            "cc.person_by_name_and_domain": _flex_async(None),
            "bc.waterfall_icp_search": _flex_async({"results": []}),
            "bc.domain_to_linkedin": _flex_async({"company_linkedin_url": ""}),
            "bc.person_enrich": fake_blitz,
            "bc.person_enrich_by_linkedin": _flex_async({"found": False, "email": ""}),
            "bc.find_work_email": _flex_async({"found": False, "email": ""}),
            "wc.find_email": fake_wizleads,
            "bec.find_work_email_v3": fake_better_enrich,
            "bec.find_company_email": _flex_async({"email": ""}),
            "mt.verify_email": _flex_async({"valid": True, "code": "valid", "message": "ok"}),
            "contacts_writer_mod.write_enrichment_result": _flex_async(
                contacts_writer_mod.WriteStatus.SKIPPED
            ),
        }
        patches = _patch_all_providers(mocks)
        for p in patches:
            p.start()
        try:
            with patch.object(
                cc, "extract_email_from_contacts_response", MagicMock(return_value=None)
            ):
                await routes_mod._unified_enrich_logic(
                    _build_req(force_provider="contacts_db"), _fake_user(), debug=True
                )
        finally:
            for p in patches:
                p.stop()

        self.assertNotIn("wizleads", called_providers)
        self.assertNotIn("better_enrich", called_providers)


# ---------------------------------------------------------------------------
# Acceptance C: force_provider=wizleads in domain_only mode
# ---------------------------------------------------------------------------
#
# In the domain_only branch, the inline `get_decision_makers()` skips
# both contacts_db AND blitz when `force_provider=wizleads` (since
# `_should_skip_provider("blitz", "wizleads")` is True), so the
# per-person loop has no contacts to enrich. This is a pre-existing
# limitation of the `domain_only` contact-list population: a
# wizleads-only or better_enrich-only decision-maker lookup requires
# the caller to use `linkedin_only` or `enhanced` mode (which use
# `pipeline.route_enrichment` directly and produce their own contacts).
# The wizleads force_provider semantics for the per-person cascade
# itself are covered in `test_cascade_fixes.py` (TestAcceptance6).
#
# What we DO assert here is that the per-person routing layer, when
# reached, honors `force_provider=wizleads` and only calls WizLeads.
# This is tested by directly invoking the same `route_enrichment` +
# `run_enrichment_route` flow the per-person loop uses.


class TestPerPersonRouteEnrichmentHonorsForceProvider(unittest.IsolatedAsyncioTestCase):
    async def test_per_person_route_with_wizleads_only_calls_wizleads(self):
        """Mirrors the per-person cascade in `_unified_enrich_logic`'s
        domain_only branch. When the per-person loop runs with
        `force_provider=wizleads`, only WizLeads is called."""
        from enrichment import pipeline as pipeline_mod
        called_providers: list[str] = []

        async def fake_wizleads(*args, **kwargs):
            called_providers.append("wizleads")
            return {"email": "jane@acme.com", "catchall": True}

        async def fake_better_enrich(*args, **kwargs):
            called_providers.append("better_enrich")
            return {"email": "should-not-be-used@acme.com", "email_status": "verified"}

        async def fake_blitz(*args, **kwargs):
            called_providers.append("blitz")
            return {"found": False, "person": {}}

        async def fake_cc_name_domain(*args, **kwargs):
            called_providers.append("contacts_db")
            return None

        async def fake_mailtester(*args, **kwargs):
            return {"valid": True, "code": "valid", "message": "ok"}

        with patch.object(wc, "find_email", fake_wizleads), \
             patch.object(bec, "find_work_email_v3", fake_better_enrich), \
             patch.object(bc, "person_enrich", fake_blitz), \
             patch.object(cc, "person_by_name_and_domain", fake_cc_name_domain), \
             patch.object(mt, "verify_email", fake_mailtester), \
             patch.object(cc, "extract_email_from_contacts_response", MagicMock(return_value=None)):
            route = pipeline_mod.route_enrichment(
                first_name="Jane",
                last_name="Doe",
                full_name="Jane Doe",
                domain="acme.com",
                force_provider="wizleads",
            )
            route_result = await pipeline_mod.run_enrichment_route(
                route,
                AsyncMock(), AsyncMock(),
                asyncio.Semaphore(1),
                validate_email=False,
                job_id="per_person_test",
                row_index=0,
                emit_logs=False,
            )

        # Only wizleads was called.
        self.assertEqual(called_providers, ["wizleads"])
        self.assertEqual(route_result["email"], "jane@acme.com")
        self.assertEqual(route_result["source"], "wizleads_email")


if __name__ == "__main__":
    unittest.main()
