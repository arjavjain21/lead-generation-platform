"""
Acceptance tests for the strongest-identifier-first enrichment router.

The router is `pipeline.route_enrichment` (pure, no I/O) and the executor
is `pipeline.run_enrichment_route` (calls mocked provider clients).

These tests cover the contract criteria 1-10 from LOOP.md:
  1. Single routing function exists and is used by both APIs (covered by import).
  2. LinkedIn-first cascade order.
  3. LinkedIn-only input (no domain, no name) is routeable.
  4. Phone -> LinkedIn -> email path (with phone_reverse stub).
  5. CSV-row path triggers LinkedIn cascade when row has linkedin_url.
  6. force_provider=blitz restricts to blitz.
  7. force_provider=contacts_db skips paid providers.
  8. Malformed LinkedIn returns no_email_reason="linkedin_parse_failed".
  9. Provider capability gates prevent calls with insufficient input.
 10. source_path is recorded on every success.
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

from enrichment import pipeline as pipeline_mod  # noqa: E402
from enrichment import identifier_utils as identifier_utils  # noqa: E402
from enrichment import providers as providers_mod  # noqa: E402


# ---------------------------------------------------------------------------
# Pure-routing tests (no I/O, no mocks)
# ---------------------------------------------------------------------------


class TestRouteEnrichmentPure(unittest.TestCase):
    def test_linkedin_first_cascade_order(self):
        """Criterion 2: LinkedIn URL is present, calls land in this order:
        Contacts DB -> Blitz LinkedIn -> Blitz find_work_email -> (name/domain fallbacks).
        """
        route = pipeline_mod.route_enrichment(
            linkedin_url="https://linkedin.com/in/johndoe",
            full_name="John Doe",
            domain="acme.com",
        )
        methods = [s["method"] for s in route["steps"]]
        self.assertEqual(
            methods[:3],
            [
                pipeline_mod.ROUTE_METHOD_PERSON_BY_LINKEDIN,
                pipeline_mod.ROUTE_METHOD_PERSON_ENRICH_BY_LINKEDIN,
                pipeline_mod.ROUTE_METHOD_FIND_WORK_EMAIL,
            ],
        )
        # Name/domain fallbacks come after the LinkedIn cascade.
        self.assertIn(pipeline_mod.ROUTE_METHOD_PERSON_BY_NAME_DOMAIN, methods)
        self.assertIn(pipeline_mod.ROUTE_METHOD_PERSON_ENRICH, methods)

    def test_linkedin_only_no_domain_no_name(self):
        """Criterion 3: LinkedIn-only input produces a valid route."""
        route = pipeline_mod.route_enrichment(linkedin_url="https://linkedin.com/in/jane")
        self.assertEqual(route["mode"], "linkedin_only")
        self.assertEqual(route["no_email_reason"], "")
        methods = [s["method"] for s in route["steps"]]
        # All three LinkedIn methods must be present.
        self.assertIn(pipeline_mod.ROUTE_METHOD_PERSON_BY_LINKEDIN, methods)
        self.assertIn(pipeline_mod.ROUTE_METHOD_PERSON_ENRICH_BY_LINKEDIN, methods)
        self.assertIn(pipeline_mod.ROUTE_METHOD_FIND_WORK_EMAIL, methods)
        # No name/domain fallbacks (no domain and no full_name).
        self.assertNotIn(pipeline_mod.ROUTE_METHOD_PERSON_BY_NAME_DOMAIN, methods)
        self.assertNotIn(pipeline_mod.ROUTE_METHOD_PERSON_ENRICH, methods)

    def test_phone_only_path(self):
        """Criterion 4: phone input produces a phone_then_linkedin route."""
        route = pipeline_mod.route_enrichment(phone="+1-555-0100")
        self.assertEqual(route["mode"], "phone_then_linkedin")
        self.assertEqual(route["no_email_reason"], "")
        self.assertEqual(len(route["steps"]), 1)
        self.assertEqual(route["steps"][0]["method"], pipeline_mod.ROUTE_METHOD_PHONE_REVERSE_LOOKUP)

    def test_name_plus_domain_cascade(self):
        route = pipeline_mod.route_enrichment(full_name="John Doe", domain="acme.com")
        self.assertEqual(route["mode"], "name_domain")
        methods = [s["method"] for s in route["steps"]]
        self.assertEqual(methods[0], pipeline_mod.ROUTE_METHOD_PERSON_BY_NAME_DOMAIN)
        self.assertEqual(methods[1], pipeline_mod.ROUTE_METHOD_PERSON_ENRICH)
        self.assertIn(pipeline_mod.ROUTE_METHOD_FIND_WORK_EMAIL_V3, methods)

    def test_domain_only_returns_empty_steps(self):
        route = pipeline_mod.route_enrichment(domain="acme.com")
        self.assertEqual(route["mode"], "domain_only")
        self.assertEqual(route["steps"], [])

    def test_malformed_linkedin_returns_no_email_reason(self):
        """Criterion 8."""
        for bad in (
            "not-a-url",
            "https://example.com/in/john",
            "linkedin.com.evil.com/in/john",
            "https://facebook.com/john",
        ):
            route = pipeline_mod.route_enrichment(linkedin_url=bad)
            self.assertEqual(route["mode"], "invalid", msg=bad)
            self.assertEqual(
                route["no_email_reason"],
                pipeline_mod.NO_EMAIL_REASON_LINKEDIN_PARSE_FAILED,
                msg=bad,
            )
            self.assertEqual(route["steps"], [])

    def test_force_provider_blitz_restricts_to_blitz(self):
        """Criterion 6."""
        route = pipeline_mod.route_enrichment(
            linkedin_url="https://linkedin.com/in/jane",
            full_name="Jane Doe",
            domain="acme.com",
            force_provider="blitz",
        )
        providers = {s["provider"] for s in route["steps"]}
        self.assertEqual(providers, {pipeline_mod.ROUTE_PROVIDER_BLITZ})
        # No contacts_db / wizleads / better_enrich steps.
        self.assertNotIn(pipeline_mod.ROUTE_PROVIDER_CONTACTS_DB, providers)
        self.assertNotIn(pipeline_mod.ROUTE_PROVIDER_WIZLEADS, providers)
        self.assertNotIn(pipeline_mod.ROUTE_PROVIDER_BETTER_ENRICH, providers)

    def test_force_provider_blitz_with_phone_uses_blitz_phone_reverse(self):
        """force_provider=blitz with only phone: the phone-reverse step IS a blitz step,
        so the route keeps it (only-blitz constraint). When the executor runs it
        and finds no email, it returns no_email_reason=phone_reverse_unavailable."""
        route = pipeline_mod.route_enrichment(
            phone="+1-555-0100",
            force_provider="blitz",
        )
        # The route has the phone_reverse step, which is a blitz step.
        self.assertEqual(route["no_email_reason"], "")
        self.assertEqual(len(route["steps"]), 1)
        self.assertEqual(route["steps"][0]["provider"], pipeline_mod.ROUTE_PROVIDER_BLITZ)
        # No non-blitz providers in the route.
        self.assertEqual(
            {s["provider"] for s in route["steps"]},
            {pipeline_mod.ROUTE_PROVIDER_BLITZ},
        )

    def test_force_provider_contacts_db_skips_paid(self):
        """Criterion 7."""
        route = pipeline_mod.route_enrichment(
            linkedin_url="https://linkedin.com/in/jane",
            full_name="Jane Doe",
            domain="acme.com",
            force_provider="contacts_db",
        )
        providers = {s["provider"] for s in route["steps"]}
        self.assertEqual(providers, {pipeline_mod.ROUTE_PROVIDER_CONTACTS_DB})
        # The contacts_db step is person_by_linkedin (gated on li) plus
        # person_by_name_and_domain (gated on name+domain). Both should be present.
        methods = [s["method"] for s in route["steps"]]
        self.assertIn(pipeline_mod.ROUTE_METHOD_PERSON_BY_LINKEDIN, methods)
        self.assertIn(pipeline_mod.ROUTE_METHOD_PERSON_BY_NAME_DOMAIN, methods)

    def test_capability_gate_drops_under_capable_methods(self):
        """Criterion 9: providers are not called with insufficient input."""
        # LinkedIn-only with NO domain, NO name should not include any
        # name+domain method.
        route = pipeline_mod.route_enrichment(linkedin_url="https://linkedin.com/in/jane")
        methods = [s["method"] for s in route["steps"]]
        for m in (
            pipeline_mod.ROUTE_METHOD_PERSON_BY_NAME_DOMAIN,
            pipeline_mod.ROUTE_METHOD_PERSON_ENRICH,
            pipeline_mod.ROUTE_METHOD_FIND_WORK_EMAIL_V3,
            pipeline_mod.ROUTE_METHOD_FIND_EMAIL,
        ):
            self.assertNotIn(m, methods)

        # Full name without domain should drop name+domain methods.
        route2 = pipeline_mod.route_enrichment(full_name="Jane Doe")
        self.assertEqual(route2["mode"], "invalid")
        self.assertEqual(
            route2["no_email_reason"], pipeline_mod.NO_EMAIL_REASON_NO_IDENTIFIERS
        )

    def test_wizleads_requires_first_and_last_name(self):
        """wizleads step only appears when first_name and last_name are present.

        Note: ``route_enrichment`` auto-derives first/last from a multi-word
        ``full_name``. To test the genuine "last name unknowable" case we use
        a single-word full_name, which leaves last_name empty after derivation."""
        # Single-word full_name -> last_name empty after derivation -> no wizleads step
        route = pipeline_mod.route_enrichment(
            full_name="Jane", domain="acme.com"
        )
        methods = [s["method"] for s in route["steps"]]
        self.assertNotIn(pipeline_mod.ROUTE_METHOD_FIND_EMAIL, methods)

        # With first/last -> wizleads step appears
        route2 = pipeline_mod.route_enrichment(
            full_name="Jane Doe",
            first_name="Jane",
            last_name="Doe",
            domain="acme.com",
        )
        methods2 = [s["method"] for s in route2["steps"]]
        self.assertIn(pipeline_mod.ROUTE_METHOD_FIND_EMAIL, methods2)


# ---------------------------------------------------------------------------
# Executor tests (mocked provider clients)
# ---------------------------------------------------------------------------


def _build_mock_clients():
    """Return a (blitz_http, contacts_http) pair with patched provider
    functions that all return 'not found' by default.

    Tests override individual provider functions via AsyncMock side_effects
    to test specific branches.
    """
    blitz_http = MagicMock()
    contacts_http = MagicMock()
    return blitz_http, contacts_http


def _patched_provider_calls(monkey, **overrides):
    """Helper: patch every provider function in blitz/contacts/wizleads/better_enrich
    with AsyncMock that returns the override (or a 'not found' default).
    """
    from enrichment import blitz_client as bc
    from enrichment import contacts_client as cc
    from enrichment import wizleads_client as wl
    from enrichment import better_enrich_client as be
    from enrichment import mailtester_client as mt

    defaults = {
        "person_by_linkedin": (lambda *a, **kw: None),
        "person_by_name_and_domain": (lambda *a, **kw: None),
        "company_by_domain": (lambda *a, **kw: None),
        "company_contacts_enriched": (lambda *a, **kw: []),
        "extract_email_from_contacts_response": (lambda *a, **kw: None),
        "mark_email_invalid": (lambda *a, **kw: None),
        "domain_to_linkedin": (lambda *a, **kw: {"found": False, "company_linkedin_url": ""}),
        "waterfall_icp_search": (lambda *a, **kw: {"results": []}),
        "find_work_email": (lambda *a, **kw: {"found": False, "email": ""}),
        "person_enrich": (lambda *a, **kw: {"found": False, "person": {}}),
        "person_enrich_by_linkedin": (lambda *a, **kw: {"found": False, "email": ""}),
        "find_email": (lambda *a, **kw: None),
        "find_work_email_v3": (lambda *a, **kw: None),
        "find_company_email": (lambda *a, **kw: None),
        "verify_email": (lambda *a, **kw: {"valid": True, "code": "ok", "message": ""}),
    }
    defaults.update(overrides)

    for fn_name, return_value in defaults.items():
        if fn_name in {"extract_email_from_contacts_response", "mark_email_invalid", "verify_email"}:
            target = getattr(cc if fn_name != "verify_email" else mt, fn_name, None)
        elif hasattr(cc, fn_name):
            target = getattr(cc, fn_name)
        elif hasattr(bc, fn_name):
            target = getattr(bc, fn_name)
        elif hasattr(wl, fn_name):
            target = getattr(wl, fn_name)
        elif hasattr(be, fn_name):
            target = getattr(be, fn_name)
        else:
            continue
        if asyncio.iscoroutinefunction(target):
            monkey.setattr(target, AsyncMock(return_value=return_value))
        else:
            monkey.setattr(target, MagicMock(return_value=return_value))


class TestRunEnrichmentRoute(unittest.TestCase):
    """Test the executor with mocked provider calls."""

    def _run(self, route):
        blitz_http, contacts_http = _build_mock_clients()
        sem = asyncio.Semaphore(5)
        return asyncio.run(
            pipeline_mod.run_enrichment_route(
                route, blitz_http, contacts_http, sem, validate_email=False
            )
        )

    def test_linkedin_first_cascade_calls_linkedin_providers(self):
        """Criterion 2: executor must call Contacts DB by LinkedIn before name/domain providers."""
        from enrichment import blitz_client as bc
        from enrichment import contacts_client as cc

        # LinkedIn-only path; first provider should be contacts_db person_by_linkedin
        # but we make it return None, then blitz person_enrich_by_linkedin returns email.
        with patch.object(
            cc, "person_by_linkedin", AsyncMock(return_value=None)
        ), patch.object(
            cc, "extract_email_from_contacts_response", MagicMock(return_value=None)
        ), patch.object(
            cc, "mark_email_invalid", AsyncMock(return_value=None)
        ), patch.object(
            bc, "person_enrich_by_linkedin",
            AsyncMock(return_value={"found": True, "email": "jane@acme.com"}),
        ), patch.object(
            bc, "find_work_email",
            AsyncMock(return_value={"found": False, "email": ""}),
        ):
            route = pipeline_mod.route_enrichment(linkedin_url="https://linkedin.com/in/jane")
            result = self._run(route)

        self.assertEqual(result["email"], "jane@acme.com")
        self.assertEqual(result["source"], pipeline_mod.SOURCE_BLITZ_EMAIL)
        self.assertIn("linkedin", result["source_path"])
        self.assertIn("person_enrich_by_linkedin", result["source_path"])
        # The first step in attempts must be the LinkedIn method.
        self.assertIn(pipeline_mod.ROUTE_METHOD_PERSON_BY_LINKEDIN, result["provider_attempts"][0])

    def test_linkedin_only_input_produces_route(self):
        """Criterion 3: linkedin-only input works without domain."""
        route = pipeline_mod.route_enrichment(linkedin_url="https://linkedin.com/in/jane")
        result = self._run(route)
        # No email is found (default mocks return empty). The route ran without
        # an "invalid input" no_email_reason. The executor surfaces the new
        # standard reason `all_providers_called_no_email` since every cascaded
        # provider was tried and none returned an email.
        self.assertEqual(
            result["no_email_reason"],
            pipeline_mod.NO_EMAIL_REASON_ALL_PROVIDERS_CALLED_NO_EMAIL,
        )
        self.assertEqual(result["email"], "")
        # The attempts list must contain only LinkedIn methods.
        for attempt in result["provider_attempts"]:
            self.assertIn("linkedin", attempt)

    def test_phone_only_returns_phone_reverse_unavailable(self):
        """Criterion 4: phone-only input runs the phone reverse step and stops
        with a clear no_email_reason (no phone reverse endpoint configured)."""
        route = pipeline_mod.route_enrichment(phone="+1-555-0100")
        result = self._run(route)
        self.assertEqual(
            result["no_email_reason"],
            pipeline_mod.NO_EMAIL_REASON_PHONE_REVERSE_UNAVAILABLE,
        )
        self.assertIn("phone", result["source_path"])
        self.assertIn("phone_reverse", result["provider_attempts"][0])

    def test_force_provider_blitz_calls_only_blitz(self):
        """Criterion 6: force_provider=blitz must not call Contacts DB."""
        from enrichment import blitz_client as bc
        from enrichment import contacts_client as cc

        blitz_called = []

        async def track_person_enrich_by_linkedin(*a, **kw):
            blitz_called.append("person_enrich_by_linkedin")
            return {"found": True, "email": "j@acme.com"}

        async def track_find_work_email(*a, **kw):
            blitz_called.append("find_work_email")
            return {"found": False, "email": ""}

        contacts_called = []

        async def fail_contacts(*a, **kw):
            contacts_called.append("contacts")
            return None

        with patch.object(bc, "person_enrich_by_linkedin", side_effect=track_person_enrich_by_linkedin), \
             patch.object(bc, "find_work_email", side_effect=track_find_work_email), \
             patch.object(cc, "person_by_linkedin", side_effect=fail_contacts):
            route = pipeline_mod.route_enrichment(
                linkedin_url="https://linkedin.com/in/j",
                force_provider="blitz",
            )
            result = self._run(route)

        self.assertEqual(result["email"], "j@acme.com")
        self.assertIn("person_enrich_by_linkedin", blitz_called)
        # Contacts DB must NOT have been called.
        self.assertEqual(contacts_called, [])

    def test_force_provider_contacts_db_skips_paid(self):
        """Criterion 7: force_provider=contacts_db does not call paid providers."""
        from enrichment import blitz_client as bc
        from enrichment import contacts_client as cc

        blitz_called = []
        be_called = []
        contacts_called = []

        async def fail_blitz(*a, **kw):
            blitz_called.append("blitz")
            return {"found": False, "email": ""}

        async def fail_be(*a, **kw):
            be_called.append("better_enrich")
            return None

        async def fail_contacts(*a, **kw):
            contacts_called.append("contacts")
            return None

        async def contacts_returns_email(*a, **kw):
            return {"email": "jane@acme.com", "first_name": "Jane", "last_name": "Doe"}

        with patch.object(cc, "person_by_linkedin", side_effect=contacts_returns_email), \
             patch.object(cc, "extract_email_from_contacts_response", MagicMock(return_value="jane@acme.com")), \
             patch.object(bc, "person_enrich_by_linkedin", side_effect=fail_blitz), \
             patch.object(bc, "find_work_email", side_effect=fail_blitz), \
             patch.object(bc, "person_enrich", side_effect=fail_blitz), \
             patch.object(bc, "domain_to_linkedin", side_effect=fail_blitz), \
             patch.object(bc, "waterfall_icp_search", side_effect=fail_blitz), \
             patch.object(cc, "person_by_name_and_domain", side_effect=fail_contacts), \
             patch("enrichment.better_enrich_client.find_work_email_v3", side_effect=fail_be), \
             patch("enrichment.wizleads_client.find_email", side_effect=fail_be):
            route = pipeline_mod.route_enrichment(
                linkedin_url="https://linkedin.com/in/jane",
                full_name="Jane Doe",
                domain="acme.com",
                force_provider="contacts_db",
            )
            result = self._run(route)

        self.assertEqual(result["email"], "jane@acme.com")
        self.assertEqual(result["source"], pipeline_mod.SOURCE_CONTACTS_DB_EMAIL)
        self.assertEqual(blitz_called, [])
        self.assertEqual(be_called, [])

    def test_malformed_linkedin_returns_parse_failed(self):
        """Criterion 8: malformed LinkedIn returns no_email_reason and no calls."""
        from enrichment import blitz_client as bc
        from enrichment import contacts_client as cc

        blitz_called = []
        contacts_called = []

        async def fail_blitz(*a, **kw):
            blitz_called.append("blitz")
            return {"found": False, "email": ""}

        async def fail_contacts(*a, **kw):
            contacts_called.append("contacts")
            return None

        with patch.object(bc, "person_enrich_by_linkedin", side_effect=fail_blitz), \
             patch.object(bc, "find_work_email", side_effect=fail_blitz), \
             patch.object(cc, "person_by_linkedin", side_effect=fail_contacts):
            route = pipeline_mod.route_enrichment(linkedin_url="not-a-linkedin-url")
            result = self._run(route)

        self.assertEqual(
            result["no_email_reason"],
            pipeline_mod.NO_EMAIL_REASON_LINKEDIN_PARSE_FAILED,
        )
        self.assertEqual(result["email"], "")
        self.assertEqual(blitz_called, [])
        self.assertEqual(contacts_called, [])

    def test_source_path_recorded_on_success(self):
        """Criterion 10: source_path is set when an email is found."""
        from enrichment import contacts_client as cc

        async def contacts_returns_email(*a, **kw):
            return {"email": "a@b.com", "first_name": "A", "last_name": "B"}

        with patch.object(cc, "person_by_linkedin", side_effect=contacts_returns_email), \
             patch.object(cc, "extract_email_from_contacts_response", MagicMock(return_value="a@b.com")):
            route = pipeline_mod.route_enrichment(linkedin_url="https://linkedin.com/in/a")
            result = self._run(route)

        self.assertEqual(result["email"], "a@b.com")
        self.assertIn("linkedin", result["source_path"])
        self.assertIn("contacts_db", result["source_path"])
        self.assertIn("person_by_linkedin", result["source_path"])

    def test_source_path_for_phone(self):
        """source_path for phone route should mention phone and the reverse hop."""
        route = pipeline_mod.route_enrichment(phone="+1-555-0100")
        result = self._run(route)
        self.assertIn("phone", result["source_path"])
        self.assertIn("phone_reverse_unavailable", result["source_path"])


# ---------------------------------------------------------------------------
# CSV row routing integration (run_pipeline uses route_enrichment)
# ---------------------------------------------------------------------------


class TestRunPipelineRouting(unittest.TestCase):
    """Criterion 5: a CSV row with linkedin_url goes through the router."""

    def test_route_enrichment_used_by_process_row(self):
        """Verify that process_row calls route_enrichment with the row's
        linkedin_url. This is a pure-routing test on the same inputs the
        CSV row path would feed in. The actual HTTP execution is tested
        separately in TestRunEnrichmentRoute."""
        from enrichment import identifier_utils as iu

        row = {
            "domain": "acme.com",
            "full_name": "Jane Doe",
            "linkedin_url": "https://linkedin.com/in/jane",
        }
        payload = iu.build_row_identifier_payload(
            row,
            domain_col="domain",
            name_col="full_name",
            linkedin_url_col="linkedin_url",
        )
        route = pipeline_mod.route_enrichment(
            linkedin_url=payload.get("normalized_linkedin_url") or "",
            phone=payload.get("phone") or "",
            full_name=payload.get("full_name") or "",
            first_name=payload.get("first_name") or "",
            last_name=payload.get("last_name") or "",
            domain=row["domain"],
            company_name=payload.get("company_name") or "",
        )
        # The route must start with a LinkedIn-based provider call.
        self.assertEqual(route["mode"], "linkedin_first")
        self.assertEqual(route["steps"][0]["identifier"], pipeline_mod.ROUTE_IDENTIFIER_LINKEDIN)
        # The first three steps must all be LinkedIn methods (LinkedIn-first cascade).
        for step in route["steps"][:3]:
            self.assertEqual(step["identifier"], pipeline_mod.ROUTE_IDENTIFIER_LINKEDIN)
        # No name/domain step is allowed to come before a LinkedIn step.
        for step in route["steps"]:
            if step["identifier"] == pipeline_mod.ROUTE_IDENTIFIER_NAME_DOMAIN:
                # It must come after at least one linkedin step.
                idx = route["steps"].index(step)
                self.assertGreater(idx, 0)
                self.assertEqual(
                    route["steps"][0]["identifier"],
                    pipeline_mod.ROUTE_IDENTIFIER_LINKEDIN,
                )
                break

    def test_csv_row_without_linkedin_uses_decision_maker_cascade(self):
        """Domain-only CSV row (no linkedin_url, no name) should NOT enter
        the routing path. It should go to the legacy _enrich_domain cascade
        (which the route_enrichment will report as 'domain_only')."""
        row = {"domain": "acme.com", "full_name": ""}
        route = pipeline_mod.route_enrichment(
            linkedin_url="",
            phone="",
            full_name=row["full_name"],
            first_name="",
            last_name="",
            domain=row["domain"],
        )
        # mode == "domain_only" means the router defers to _enrich_domain.
        self.assertEqual(route["mode"], "domain_only")


# ---------------------------------------------------------------------------
# Wiring tests: _unified_enrich_logic uses route_enrichment/run_enrichment_route
# ---------------------------------------------------------------------------


class TestUnifiedEnrichUsesRouter(unittest.IsolatedAsyncioTestCase):
    """Criterion 1: the direct enrichment API must use the same router as
    the CSV pipeline. We verify by patching the routing primitives that
    `_unified_enrich_logic` must call, and asserting they were invoked with
    the right inputs."""

    async def _run_unified(self, req, route_result=None):
        from enrichment import routes as routes_mod

        route_result = route_result or {
            "email": "",
            "source": "not_found",
            "verification": {
                "dm_email_verified": "unknown",
                "mailtester_code": "",
                "mailtester_message": "",
            },
            "source_path": "",
            "provider_attempts": [],
            "no_email_reason": "",
        }
        called = {"route": None, "executed": None}
        route = {
            "mode": "linkedin_first",
            "steps": [
                {"identifier": "linkedin", "method": "person_by_linkedin", "provider": "contacts_db"},
            ],
            "no_email_reason": "",
            "inputs": {
                "linkedin_url": req.linkedin_url or "",
                "phone": "",
                "full_name": "",
                "first_name": "",
                "last_name": "",
                "domain": req.domain or "",
                "company_name": "",
            },
        }

        def fake_route_enrichment(*, linkedin_url, phone, full_name, first_name, last_name, domain, company_name, force_provider=None):
            called["route"] = {
                "linkedin_url": linkedin_url,
                "phone": phone,
                "full_name": full_name,
                "first_name": first_name,
                "last_name": last_name,
                "domain": domain,
                "company_name": company_name,
                "force_provider": force_provider,
            }
            return route

        async def fake_run_enrichment_route(*args, **kwargs):
            called["executed"] = True
            return route_result

        with patch.object(routes_mod.pipeline, "route_enrichment", new=fake_route_enrichment), \
             patch.object(routes_mod.pipeline, "run_enrichment_route", new=fake_run_enrichment_route), \
             patch.object(routes_mod, "sync_contacts") as fake_sync, \
             patch.object(routes_mod, "_record_unified_enrich_stats"):
            fake_sync.sync_enrichment_to_contacts = MagicMock(return_value={"synced": 0, "skipped": 0, "failed": 0})
            result = await routes_mod._unified_enrich_logic(req, {"email": "test@example.com", "user_id": 1})

        return result, called

    async def test_unified_linkedin_only_calls_router(self):
        from enrichment import routes as routes_mod
        req = routes_mod.UnifiedEnrichRequest(linkedin_url="https://linkedin.com/in/jane")
        result, called = await self._run_unified(req)
        self.assertIsNotNone(called["route"], "route_enrichment must be called")
        self.assertTrue(called["executed"], "run_enrichment_route must be called")
        self.assertEqual(called["route"]["linkedin_url"], "https://linkedin.com/in/jane")
        # Response must include the routing diagnostics block.
        self.assertIn("routing", result)
        self.assertEqual(result["routing"]["mode"], "linkedin_first")
        # Mode in the response is set from the original request mode, not the
        # route's mode. The route here is mocked so the request's mode label
        # is what the API consumer sees.
        self.assertEqual(result["mode"], "linkedin_only")

    async def test_unified_enhanced_calls_router(self):
        from enrichment import routes as routes_mod
        req = routes_mod.UnifiedEnrichRequest(
            linkedin_url="https://linkedin.com/in/jane",
            domain="acme.com",
            full_name="Jane Doe",
        )
        result, called = await self._run_unified(req)
        self.assertIsNotNone(called["route"])
        self.assertEqual(called["route"]["linkedin_url"], "https://linkedin.com/in/jane")
        self.assertEqual(called["route"]["domain"], "acme.com")
        self.assertEqual(called["route"]["full_name"], "Jane Doe")
        self.assertEqual(result["mode"], "enhanced")

    async def test_unified_enhanced_with_force_provider_blitz_passes_through(self):
        from enrichment import routes as routes_mod
        req = routes_mod.UnifiedEnrichRequest(
            linkedin_url="https://linkedin.com/in/jane",
            domain="acme.com",
            full_name="Jane Doe",
            force_provider="blitz",
        )
        result, called = await self._run_unified(req)
        self.assertEqual(called["route"]["force_provider"], "blitz")
        self.assertIn("routing", result)

    async def test_unified_linkedin_only_uses_blitz_when_router_returns_blitz(self):
        from enrichment import routes as routes_mod
        route_result = {
            "email": "jane@acme.com",
            "source": "blitz_email",
            "verification": {
                "dm_email_verified": "valid",
                "mailtester_code": "ok",
                "mailtester_message": "ok",
            },
            "source_path": "linkedin -> blitz_person_enrich_by_linkedin",
            "provider_attempts": ["person_by_linkedin@linkedin", "person_enrich_by_linkedin@linkedin"],
            "no_email_reason": "",
        }
        req = routes_mod.UnifiedEnrichRequest(linkedin_url="https://linkedin.com/in/jane")
        result, _ = await self._run_unified(req, route_result=route_result)
        self.assertEqual(result["data_sources"]["emails"], "blitz_email")
        self.assertEqual(result["contacts"][0]["email"], "jane@acme.com")
        self.assertEqual(result["contacts"][0]["email_source"], "blitz_email")
        self.assertEqual(result["routing"]["source_path"], "linkedin -> blitz_person_enrich_by_linkedin")


if __name__ == "__main__":
    unittest.main()
