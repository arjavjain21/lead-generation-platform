"""
Phase 3 (batch coverage) tests: GetLeads multi-DM paths collapse N single
``find_email`` calls into 1 ``find_emails_batch`` call.

Scope (mirrors the approved plan, Phase 3):
  1. ``pipeline._enrich_domain`` batch pre-pass fires WITHOUT a collector
     (the Phase-3 gate relaxation) — POST /enrich domain_only and
     GET /enrich/{domain} get the pre-pass for free. Only the collector
     capture is collector-gated.
  2. ``pipeline.run_enrichment_route`` / ``_run_route_step`` accept
     ``pre_resolved_getleads`` / ``pre_resolved_smartprospect`` kwargs
     (default None = zero behavior change). A hit short-circuits the
     single-call path; an empty pre-resolved result falls through to the
     normal single call.
  3. ``routes._unified_enrich_logic`` domain_only per-DM loop runs ONE
     batch call for 5 decision makers and ZERO single getleads calls.
  4. ``list_builder``: ``_enrich_single_domain`` runs ONE batch call for
     3 persons; ``_resolve_person_email`` Strategy 5 consumes the
     pre-resolved result; the GetLeads DM fields land in the output row.
  5. Existing ``run_enrichment_route`` callers (no new kwargs) are
     unaffected.

All provider calls are mocked — no credits spent, no HTTP.

Run:
    python -m pytest enrichment/tests/test_getleads_batch.py -v
"""

from __future__ import annotations

import asyncio
import inspect
import os
import sys
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

os.environ.setdefault("CONTACTS_API_TOKEN", "test-token-from-suite")

from enrichment import pipeline as pipeline_mod  # noqa: E402
from enrichment import list_builder as lb  # noqa: E402
from enrichment import routes as routes_mod  # noqa: E402
from enrichment import contacts_client as cc  # noqa: E402
from enrichment import blitz_client as bc  # noqa: E402
from enrichment import getleads_client as gl  # noqa: E402
from enrichment import smartprospect_client as sp  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _gl_result(
    email: str = "john.doe@example.com",
    first: str = "John",
    last: str = "Doe",
    domain: str = "example.com",
    verification: Any = "Valid",
    **extra: Any,
) -> dict[str, Any]:
    """Build a normalized GetLeads result dict (find_email /
    find_emails_batch per-item shape, post Phase 2)."""
    return {
        "email": email,
        "status": "Found" if email else "Not Found",
        "verification_status": verification,
        "first_name": first,
        "last_name": last,
        "domain": domain,
        **extra,
    }


def _make_person(first: str, last: str, title: str = "CEO") -> dict[str, Any]:
    """Synthetic {person, icp} dict consumed by ``_enrich_domain``."""
    return {
        "person": {
            "first_name": first,
            "last_name": last,
            "full_name": f"{first} {last}",
            "title": title,
            "headline": f"{title} at Acme",
            "linkedin_url": "",
            "location": {"city": "NYC", "country_code": "US"},
            "emails": [],
        },
        "icp": 0,
    }


def _wire_minimal_company(monkeypatch: pytest.MonkeyPatch, persons: list[dict[str, Any]]) -> None:
    """Patch Contacts DB so ``_enrich_domain`` reaches the pre-pass with the
    given persons (Contacts DB quality gate met via emails)."""
    contacts_payload = [
        {
            "first_name": p["person"]["first_name"],
            "last_name": p["person"]["last_name"],
            "full_name": p["person"]["full_name"],
            "title": p["person"].get("title", ""),
            "headline": p["person"].get("headline", ""),
            "linkedin_url": "",
            "email": f"{p['person']['first_name'].lower()}@acme.com",
            "city": "NYC",
            "country_code": "US",
        }
        for p in persons
    ]

    async def fake_company_by_domain(http, domain):
        return {"linkedin_url": "https://linkedin.com/company/acme", "name": "Acme"}

    async def fake_company_contacts_enriched(http, domain, limit):
        return contacts_payload[:limit]

    async def fake_person_by_name_and_domain(http, full_name, domain):
        return None

    monkeypatch.setattr(cc, "company_by_domain", fake_company_by_domain)
    monkeypatch.setattr(cc, "company_contacts_enriched", fake_company_contacts_enriched)
    monkeypatch.setattr(cc, "person_by_name_and_domain", fake_person_by_name_and_domain)


def _run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# 1. _enrich_domain pre-pass fires WITHOUT a collector
# ---------------------------------------------------------------------------


class TestEnrichDomainPrepassWithoutCollector:
    def test_getleads_batch_fires_with_collector_none(self, monkeypatch):
        """collector=None must NOT suppress the GetLeads batch pre-pass
        anymore (Phase 3) — the batch call fires once and the pre_resolved
        handoff reaches the per-row resolver."""
        _wire_minimal_company(
            monkeypatch,
            [_make_person("Alice", "Alpha"), _make_person("Bob", "Beta"),
             _make_person("Carol", "Gamma")],
        )
        batch_calls = {"n": 0}

        async def fake_batch(http, contacts):
            batch_calls["n"] += 1
            return [
                _gl_result(email=f"{c['firstName'].lower()}@acme.com",
                           first=c["firstName"], last=c["lastName"])
                for c in contacts
            ]

        monkeypatch.setattr(gl, "find_emails_batch", fake_batch)

        seen: list[Any] = []

        async def fake_resolve(*args, **kwargs):
            seen.append(kwargs.get("pre_resolved_getleads"))
            return ("", "not_found", {"dm_email_verified": "unknown"})

        def go():
            async def _go():
                with patch.object(pipeline_mod, "_resolve_email_for_person", fake_resolve):
                    return await pipeline_mod._enrich_domain(
                        blitz_http=MagicMock(),
                        contacts_http=MagicMock(),
                        base_row={"domain": "acme.com"},
                        domain="acme.com",
                        full_name="",
                        cascade=bc.DEFAULT_CASCADE,
                        max_results=10,
                        domain_semaphore=asyncio.Semaphore(1),
                        email_semaphore=asyncio.Semaphore(1),
                        collector=None,  # KEY: Phase 3 relaxation
                    )
            return asyncio.run(_go())

        rows = go()
        assert batch_calls["n"] == 1, (
            f"Phase 3: batch must fire without a collector, got {batch_calls['n']}"
        )
        assert len(rows) == 3
        for pre in seen:
            assert pre is not None and pre.get("email"), (
                "pre_resolved_getleads handoff must fire without a collector"
            )

    def test_capture_skipped_but_handoff_fires_when_no_collector(self, monkeypatch):
        """Without a collector the capture is skipped (no crash) — verified by
        running the REAL pre-pass body with a collector spy set to None."""
        _wire_minimal_company(
            monkeypatch,
            [_make_person("Alice", "Alpha"), _make_person("Bob", "Beta")],
        )

        async def fake_batch(http, contacts):
            return [
                _gl_result(email=f"{c['firstName'].lower()}@acme.com",
                           first=c["firstName"], last=c["lastName"])
                for c in contacts
            ]

        monkeypatch.setattr(gl, "find_emails_batch", fake_batch)

        # Real resolver would call providers; stub it (we only care that the
        # pre-pass completes without a collector).
        with patch.object(
            pipeline_mod, "_resolve_email_for_person",
            AsyncMock(return_value=("", "not_found", {"dm_email_verified": "unknown"})),
        ):
            rows = _run(pipeline_mod._enrich_domain(
                blitz_http=MagicMock(),
                contacts_http=MagicMock(),
                base_row={"domain": "acme.com"},
                domain="acme.com",
                full_name="",
                cascade=bc.DEFAULT_CASCADE,
                max_results=10,
                domain_semaphore=asyncio.Semaphore(1),
                email_semaphore=asyncio.Semaphore(1),
                collector=None,
            ))
        assert len(rows) == 2, "no-collector run must not crash and must return rows"

    def test_getleads_batch_still_skipped_when_force_provider(self, monkeypatch):
        """The other gates (force_provider / <2 persons) still suppress the
        batch even after the collector gate was relaxed."""
        _wire_minimal_company(
            monkeypatch,
            [_make_person("Alice", "Alpha"), _make_person("Bob", "Beta")],
        )
        batch_calls = {"n": 0}

        async def fake_batch(http, contacts):
            batch_calls["n"] += 1
            return []

        monkeypatch.setattr(gl, "find_emails_batch", fake_batch)

        with patch.object(
            pipeline_mod, "_resolve_email_for_person",
            AsyncMock(return_value=("", "not_found", {})),
        ):
            _run(pipeline_mod._enrich_domain(
                blitz_http=MagicMock(),
                contacts_http=MagicMock(),
                base_row={"domain": "acme.com"},
                domain="acme.com",
                full_name="",
                cascade=bc.DEFAULT_CASCADE,
                max_results=10,
                domain_semaphore=asyncio.Semaphore(1),
                email_semaphore=asyncio.Semaphore(1),
                force_provider="blitz",
                collector=None,
            ))
        assert batch_calls["n"] == 0, "force_provider must still suppress the batch"


# ---------------------------------------------------------------------------
# 2. run_enrichment_route pre-resolved kwargs
# ---------------------------------------------------------------------------


class TestRunEnrichmentRoutePreResolved:
    def _route(self):
        return pipeline_mod.route_enrichment(
            full_name="Jane Doe",
            first_name="Jane",
            last_name="Doe",
            domain="acme.com",
        )

    def _patch_upstream(self):
        """Patch contacts_db + blitz so the route reaches the getleads step."""
        from contextlib import ExitStack
        stack = ExitStack()
        stack.enter_context(patch.object(
            cc, "person_by_name_and_domain", AsyncMock(return_value=None)))
        stack.enter_context(patch.object(
            cc, "extract_email_from_contacts_response", MagicMock(return_value="")))
        stack.enter_context(patch.object(
            bc, "person_enrich", AsyncMock(return_value={"found": False, "person": {}})))
        return stack

    def test_pre_resolved_getleads_hit_no_single_call(self):
        """A pre-resolved getleads email short-circuits the single call and
        returns SOURCE_GETLEADS with the Phase-2 DM snapshot."""
        single_calls = {"n": 0}

        async def fake_single(*a, **kw):
            single_calls["n"] += 1
            return _gl_result()

        with self._patch_upstream(), patch.object(gl, "find_email", fake_single):
            result = _run(pipeline_mod.run_enrichment_route(
                self._route(),
                AsyncMock(), AsyncMock(),
                asyncio.Semaphore(1),
                validate_email=False,
                job_id="job-gl-pre-hit",
                row_index=0,
                emit_logs=False,
                pre_resolved_getleads=_gl_result(
                    email="pre.jane@acme.com", verification="Valid",
                    job_title="CEO", city="Austin", country="US",
                ),
            ))

        assert single_calls["n"] == 0, "pre-resolved hit must NOT call find_email"
        assert result["email"] == "pre.jane@acme.com"
        assert result["source"] == pipeline_mod.SOURCE_GETLEADS
        assert result["verification"]["dm_email_verified"] == "yes"
        gl_dm = result["verification"].get("getleads_dm") or {}
        assert gl_dm.get("title") == "CEO", "Phase-2 DM snapshot must ride the verification dict"
        assert gl_dm.get("city") == "Austin"

    def test_pre_resolved_getleads_empty_falls_through_to_single(self):
        """An empty pre-resolved email must fall through to the normal
        single-call path (the step is NOT skipped)."""
        single_calls = {"n": 0}

        async def fake_single(*a, **kw):
            single_calls["n"] += 1
            return _gl_result(email="single.jane@acme.com")

        with self._patch_upstream(), patch.object(gl, "find_email", fake_single):
            result = _run(pipeline_mod.run_enrichment_route(
                self._route(),
                AsyncMock(), AsyncMock(),
                asyncio.Semaphore(1),
                validate_email=False,
                job_id="job-gl-pre-miss",
                row_index=0,
                emit_logs=False,
                pre_resolved_getleads=_gl_result(email=""),
            ))

        assert single_calls["n"] == 1, "empty pre-resolved must fall through to one single call"
        assert result["email"] == "single.jane@acme.com"
        assert result["source"] == pipeline_mod.SOURCE_GETLEADS

    def test_pre_resolved_smartprospect_hit_no_single_call(self):
        """Mirror of the getleads test for the smartprospect kwarg."""
        single_calls = {"n": 0}

        async def fake_single(*a, **kw):
            single_calls["n"] += 1
            return {"email": "should-not-happen@acme.com"}

        with self._patch_upstream(), patch.object(gl, "find_email", AsyncMock(return_value=None)), \
             patch.object(sp, "find_email", fake_single):
            result = _run(pipeline_mod.run_enrichment_route(
                self._route(),
                AsyncMock(), AsyncMock(),
                asyncio.Semaphore(1),
                validate_email=False,
                job_id="job-sp-pre-hit",
                row_index=0,
                emit_logs=False,
                pre_resolved_smartprospect={
                    "email": "pre.jane@acme.com",
                    "verification_status": "Valid",
                },
            ))

        assert single_calls["n"] == 0
        assert result["email"] == "pre.jane@acme.com"
        assert result["source"] == pipeline_mod.SOURCE_SMARTPROSPECT
        assert result["verification"]["dm_email_verified"] == "yes"

    def test_default_none_kwargs_zero_behavior_change(self):
        """Regression: existing callers (no new kwargs) still hit the normal
        single-call path."""
        single_calls = {"n": 0}

        async def fake_single(*a, **kw):
            single_calls["n"] += 1
            return _gl_result(email="plain.jane@acme.com")

        with self._patch_upstream(), patch.object(gl, "find_email", fake_single):
            result = _run(pipeline_mod.run_enrichment_route(
                self._route(),
                AsyncMock(), AsyncMock(),
                asyncio.Semaphore(1),
                validate_email=False,
                job_id="job-gl-no-kwarg",
                row_index=0,
                emit_logs=False,
            ))

        assert single_calls["n"] == 1
        assert result["email"] == "plain.jane@acme.com"
        assert result["source"] == pipeline_mod.SOURCE_GETLEADS

    def test_new_kwargs_default_to_none(self):
        """Mechanical: both kwargs exist and default to None."""
        for fn in (pipeline_mod.run_enrichment_route, pipeline_mod._run_route_step):
            sig = inspect.signature(fn)
            for name in ("pre_resolved_getleads", "pre_resolved_smartprospect"):
                assert name in sig.parameters, f"{fn.__name__} missing {name}"
                assert sig.parameters[name].default is None


# ---------------------------------------------------------------------------
# 3. routes domain_only per-DM loop: 1 batch, 0 singles
# ---------------------------------------------------------------------------


def _dm_contacts(n: int = 5) -> list[dict[str, Any]]:
    return [
        {
            "full_name": f"First{i} Last{i}",
            "first_name": f"First{i}",
            "last_name": f"Last{i}",
            "title": "CEO",
            "linkedin_url": "",
            "headline": "",
            "location_city": "",
            "location_country": "",
            "icp_tier": 1,
        }
        for i in range(n)
    ]


class TestDomainOnlyLoopBatch:
    def _patch_routes_stack(self, contacts: list[dict[str, Any]]):
        """Patch everything the domain_only branch touches; return dict of
        counters."""
        counters = {"batch": 0, "single": 0}

        async def fake_batch(http, payload):
            counters["batch"] += 1
            return [
                _gl_result(email=f"{c['firstName'].lower()}.{c['lastName'].lower()}@acme.com",
                           first=c["firstName"], last=c["lastName"])
                for c in payload
            ]

        async def fake_single(*a, **kw):
            counters["single"] += 1
            return _gl_result(email="should-not-be-used@acme.com")

        async def fake_v2_writer(contacts, domain):
            return {"synced": 0, "skipped": 0, "failed": 0, "records_queued": 0}, "skipped"

        async def fake_slot():
            return None

        patches = [
            patch.object(routes_mod, "_acquire_enrich_slot", fake_slot),
            patch.object(routes_mod, "_get_blitz_http", MagicMock(return_value=MagicMock())),
            patch.object(routes_mod, "_get_contacts_http", MagicMock(return_value=MagicMock())),
            patch.object(routes_mod, "_record_unified_enrich_stats", MagicMock()),
            patch.object(routes_mod, "_run_contacts_writer_v2", fake_v2_writer),
            patch.object(routes_mod.contacts_writer, "is_v2_enabled", MagicMock(return_value=True)),
            patch.object(cc, "company_by_domain",
                         AsyncMock(return_value={"linkedin_url": "https://linkedin.com/company/acme"})),
            patch.object(cc, "company_contacts_enriched", AsyncMock(return_value=contacts)),
            patch.object(cc, "person_by_name_and_domain", AsyncMock(return_value=None)),
            patch.object(cc, "extract_email_from_contacts_response", MagicMock(return_value="")),
            patch.object(bc, "domain_to_linkedin",
                         AsyncMock(return_value={"company_linkedin_url": ""})),
            patch.object(bc, "waterfall_icp_search", AsyncMock(return_value={"results": []})),
            patch.object(bc, "person_enrich", AsyncMock(return_value={"found": False, "person": {}})),
            patch.object(gl, "find_emails_batch", fake_batch),
            patch.object(gl, "find_email", fake_single),
        ]
        return counters, patches

    def test_five_dms_one_batch_zero_singles(self):
        counters, patches = self._patch_routes_stack(_dm_contacts(5))
        for p in patches:
            p.start()
        try:
            req = routes_mod.UnifiedEnrichRequest(domain="acme.com", max_results=5)
            response = _run(routes_mod._unified_enrich_logic(
                req, {"email": "t@e.com", "user_id": 1, "id": 1}, debug=False
            ))
        finally:
            for p in patches:
                p.stop()

        assert counters["batch"] == 1, (
            f"expected exactly 1 getleads batch call, got {counters['batch']}"
        )
        assert counters["single"] == 0, (
            f"expected 0 single getleads calls (all pre-resolved), got {counters['single']}"
        )
        emails = [c["email"] for c in response["contacts"]]
        assert emails == [f"first{i}.last{i}@acme.com" for i in range(5)], emails
        assert all(c["email_source"] == "getleads_email" for c in response["contacts"])

    def test_batch_failure_falls_back_to_singles(self):
        counters, patches = self._patch_routes_stack(_dm_contacts(2))

        # Override the batch mock to raise; singles must take over.
        async def failing_batch(http, payload):
            counters["batch"] += 1
            raise RuntimeError("simulated outage")

        async def fallback_single(*a, **kw):
            counters["single"] += 1
            return _gl_result(email="")  # Not Found → cascade continues harmlessly

        patches[-2] = patch.object(gl, "find_emails_batch", failing_batch)
        patches[-1] = patch.object(gl, "find_email", fallback_single)
        # Downstream providers after a getleads miss (smartprospect etc.):
        patches.append(patch.object(sp, "find_email", AsyncMock(return_value=None)))

        for p in patches:
            p.start()
        try:
            req = routes_mod.UnifiedEnrichRequest(domain="acme.com", max_results=5)
            response = _run(routes_mod._unified_enrich_logic(
                req, {"email": "t@e.com", "user_id": 1, "id": 1}, debug=False
            ))
        finally:
            for p in patches:
                p.stop()

        assert counters["batch"] == 1
        assert counters["single"] == 2, (
            f"batch failure must fall back to per-DM singles, got {counters['single']}"
        )
        assert isinstance(response["contacts"], list)

    def test_single_dm_no_batch(self):
        counters, patches = self._patch_routes_stack(_dm_contacts(1))

        async def fallback_single(*a, **kw):
            counters["single"] += 1
            return _gl_result(email="")

        patches[-1] = patch.object(gl, "find_email", fallback_single)
        patches.append(patch.object(sp, "find_email", AsyncMock(return_value=None)))

        for p in patches:
            p.start()
        try:
            req = routes_mod.UnifiedEnrichRequest(domain="acme.com", max_results=5)
            _run(routes_mod._unified_enrich_logic(
                req, {"email": "t@e.com", "user_id": 1, "id": 1}, debug=False
            ))
        finally:
            for p in patches:
                p.stop()

        assert counters["batch"] == 0, "1 DM must not trigger the batch"


# ---------------------------------------------------------------------------
# 4. list_builder: batch pre-pass + Strategy 5 bypass + DM fields in row
# ---------------------------------------------------------------------------


_BLITZ_PERSONS = {
    "results": [
        {
            "person": {
                "first_name": f"PFirst{i}",
                "last_name": f"PLast{i}",
                "full_name": f"PFirst{i} PLast{i}",
                "title": f"Title {i}",
                "headline": f"Headline {i}",
                "linkedin_url": "",
                "location": {"city": "SF", "country_code": "US"},
                "emails": [],
            },
            "icp": 0,
        }
        for i in range(3)
    ]
}


class TestListBuilderBatch:
    def test_three_persons_one_batch_dm_fields_in_row(self, monkeypatch):
        monkeypatch.setattr(cc, "company_by_domain",
                            AsyncMock(return_value={"linkedin_url": "https://linkedin.com/company/acme"}))
        monkeypatch.setattr(cc, "company_contacts_enriched", AsyncMock(return_value=[]))
        monkeypatch.setattr(bc, "waterfall_icp_search", AsyncMock(return_value=_BLITZ_PERSONS))
        monkeypatch.setattr(cc, "person_by_name_and_domain", AsyncMock(return_value=None))
        monkeypatch.setattr(cc, "extract_email_from_contacts_response", MagicMock(return_value=""))
        monkeypatch.setattr(bc, "person_enrich",
                            AsyncMock(return_value={"found": False, "person": {}}))
        monkeypatch.setattr(sp, "find_email", AsyncMock(return_value=None))

        counters = {"batch": 0, "single": 0}

        async def fake_batch(http, payload):
            counters["batch"] += 1
            return [
                _gl_result(
                    email=f"{c['firstName'].lower()}@acme.com",
                    first=c["firstName"], last=c["lastName"],
                    job_title="Chief Officer", linkedin_headline="CO at Acme",
                    city="Austin", country="US", phone="+1-555-0100",
                    job_level="c_suite", job_function="operations",
                    revenue="$10M-$50M", employee_count="50-200",
                )
                for c in payload
            ]

        async def fake_single(*a, **kw):
            counters["single"] += 1
            return _gl_result()

        monkeypatch.setattr(gl, "find_emails_batch", fake_batch)
        monkeypatch.setattr(gl, "find_email", fake_single)

        rows = _run(lb._enrich_single_domain(
            blitz_http=MagicMock(),
            contacts_http=MagicMock(),
            base_row={"domain": "acme.com"},
            domain="acme.com",
            max_decision_makers=5,
            include_generic_emails=False,
            collector=None,
        ))

        assert counters["batch"] == 1, (
            f"expected exactly 1 getleads batch call, got {counters['batch']}"
        )
        assert counters["single"] == 0, (
            f"expected 0 single getleads calls, got {counters['single']}"
        )
        assert len(rows) == 3
        for i, row in enumerate(rows):
            assert row["dm_email"] == f"pfirst{i}@acme.com"
            assert row["dm_email_source"] == lb.SOURCE_GETLEADS
            # Phase-2 DM overlay lands in the row (non-empty values only).
            assert row["dm_title"] == "Chief Officer", row["dm_title"]
            assert row["dm_headline"] == "CO at Acme"
            assert row["dm_location_city"] == "Austin"
            assert row["dm_phone"] == "+1-555-0100"
            assert row["dm_job_level"] == "c_suite"
            assert row["dm_job_function"] == "operations"
            assert row["company_revenue"] == "$10M-$50M"
            assert row["dm_linkedin_connections"] == ""

    def test_resolve_person_email_pre_resolved_returns_early(self, monkeypatch):
        """Strategy 5 consumes the pre-resolved result without calling the
        single endpoint and returns the 7-tuple with the DM snapshot."""
        async def fail_single(*a, **kw):
            raise AssertionError("pre-resolved hit must not call find_email")

        monkeypatch.setattr(cc, "person_by_name_and_domain", AsyncMock(return_value=None))
        monkeypatch.setattr(cc, "extract_email_from_contacts_response", MagicMock(return_value=""))
        monkeypatch.setattr(bc, "person_enrich",
                            AsyncMock(return_value={"found": False, "person": {}}))
        monkeypatch.setattr(gl, "find_email", fail_single)

        result = _run(lb._resolve_person_email(
            MagicMock(),
            MagicMock(),
            {"full_name": "Jane Doe", "first_name": "Jane", "last_name": "Doe",
             "linkedin_url": ""},
            "acme.com",
            validate_email=False,
            pre_resolved_getleads=_gl_result(
                email="pre.jane@acme.com", verification="Valid", job_title="CEO",
            ),
        ))

        email, phone, source, verified, mc, mm, gl_dm = result
        assert email == "pre.jane@acme.com"
        assert source == lb.SOURCE_GETLEADS
        assert verified == "yes"
        assert gl_dm.get("title") == "CEO"

    def test_resolve_person_email_empty_pre_resolved_falls_through(self, monkeypatch):
        monkeypatch.setattr(cc, "person_by_name_and_domain", AsyncMock(return_value=None))
        monkeypatch.setattr(cc, "extract_email_from_contacts_response", MagicMock(return_value=""))
        monkeypatch.setattr(bc, "person_enrich",
                            AsyncMock(return_value={"found": False, "person": {}}))

        single_calls = {"n": 0}

        async def fake_single(*a, **kw):
            single_calls["n"] += 1
            return _gl_result(email="single.jane@acme.com", job_title="CTO")

        monkeypatch.setattr(gl, "find_email", fake_single)

        result = _run(lb._resolve_person_email(
            MagicMock(),
            MagicMock(),
            {"full_name": "Jane Doe", "first_name": "Jane", "last_name": "Doe",
             "linkedin_url": ""},
            "acme.com",
            validate_email=False,
            pre_resolved_getleads=_gl_result(email=""),
        ))

        email, _, source, _, _, _, gl_dm = result
        assert single_calls["n"] == 1, "empty pre-resolved must fall through to the single call"
        assert email == "single.jane@acme.com"
        assert source == lb.SOURCE_GETLEADS
        assert gl_dm.get("title") == "CTO", "single-call path must also carry the DM snapshot"

    def test_batch_failure_falls_back_to_singles(self, monkeypatch):
        monkeypatch.setattr(cc, "company_by_domain",
                            AsyncMock(return_value={"linkedin_url": "https://linkedin.com/company/acme"}))
        monkeypatch.setattr(cc, "company_contacts_enriched", AsyncMock(return_value=[]))
        monkeypatch.setattr(bc, "waterfall_icp_search", AsyncMock(return_value=_BLITZ_PERSONS))
        monkeypatch.setattr(cc, "person_by_name_and_domain", AsyncMock(return_value=None))
        monkeypatch.setattr(cc, "extract_email_from_contacts_response", MagicMock(return_value=""))
        monkeypatch.setattr(bc, "person_enrich",
                            AsyncMock(return_value={"found": False, "person": {}}))

        counters = {"batch": 0, "single": 0}

        async def failing_batch(http, payload):
            counters["batch"] += 1
            raise RuntimeError("boom")

        async def fake_single(*a, **kw):
            counters["single"] += 1
            return _gl_result(email=f"single{counters['single']}@acme.com")

        monkeypatch.setattr(gl, "find_emails_batch", failing_batch)
        monkeypatch.setattr(gl, "find_email", fake_single)

        rows = _run(lb._enrich_single_domain(
            blitz_http=MagicMock(),
            contacts_http=MagicMock(),
            base_row={"domain": "acme.com"},
            domain="acme.com",
            max_decision_makers=5,
            include_generic_emails=False,
            collector=None,
        ))

        assert counters["batch"] == 1
        assert counters["single"] == 3, "batch failure must fall back to 3 singles"
        assert len(rows) == 3


# ---------------------------------------------------------------------------
# 5. Mirror invariant (must stay green alongside this phase)
# ---------------------------------------------------------------------------


def test_valid_providers_mirror_still_holds():
    """The three VALID_PROVIDERS sets must stay identical (invariant also
    asserted in test_smartprospect_cascade.py)."""
    assert set(pipeline_mod.VALID_PROVIDERS) == set(routes_mod.VALID_PROVIDERS)
    assert set(pipeline_mod.VALID_PROVIDERS) == set(lb.VALID_PROVIDERS)
