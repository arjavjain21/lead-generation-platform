"""
GetLeads from-linkedin fallback wiring tests (2026-08-14).

Locks in:
  1. Route plan: the LinkedIn-first arm includes the GetLeads
     ``find_email_linkedin_getleads`` step AFTER both Blitz steps
     (person_enrich_by_linkedin + find_work_email) and BEFORE the
     name+domain fallback block.
  2. Executor: ``run_enrichment_route`` on a linkedin_only route calls
     GetLeads ``find_email_by_linkedin`` ONLY after both Blitz steps miss,
     and returns SOURCE_GETLEADS on a hit (with the getleads_dm snapshot).
  3. Blitz hit short-circuits — GetLeads is NEVER called.
  4. list_builder Flow 3: N URLs -> exactly ceil(N/100) batch calls; a
     blitz miss + batch hit resolves via GetLeads; a batch MISS is never
     re-fired as a single call.
  5. Mirror invariants: the 3 VALID_PROVIDERS sets + 2 ENRICHED_COLUMNS
     lists stay identical (re-asserted here since this change touches the
     same modules).

All provider calls are mocked — no credits spent, no HTTP.
"""
from __future__ import annotations

import asyncio
import math
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


def _run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# 1. Route plan: getleads step position in the LinkedIn-first arm
# ---------------------------------------------------------------------------


class TestRoutePlanGetLeadsAfterBlitz:
    def test_linkedin_only_step_order(self):
        route = pipeline_mod.route_enrichment(
            linkedin_url="https://linkedin.com/in/jane"
        )
        assert route["mode"] == "linkedin_only"
        methods = [s["method"] for s in route["steps"]]
        assert methods == [
            pipeline_mod.ROUTE_METHOD_PERSON_BY_LINKEDIN,
            pipeline_mod.ROUTE_METHOD_PERSON_ENRICH_BY_LINKEDIN,
            pipeline_mod.ROUTE_METHOD_FIND_WORK_EMAIL,
            pipeline_mod.ROUTE_METHOD_FIND_EMAIL_LINKEDIN_GETLEADS,
        ], methods

    def test_linkedin_first_with_name_domain_getleads_before_fallbacks(self):
        """With linkedin + name + domain, the getleads LinkedIn step stays in
        the LinkedIn block (after Blitz) and BEFORE the name+domain block."""
        route = pipeline_mod.route_enrichment(
            linkedin_url="https://linkedin.com/in/jane",
            full_name="Jane Doe",
            first_name="Jane",
            last_name="Doe",
            domain="acme.com",
        )
        methods = [s["method"] for s in route["steps"]]
        gl_idx = methods.index(pipeline_mod.ROUTE_METHOD_FIND_EMAIL_LINKEDIN_GETLEADS)
        blitz_fwe_idx = methods.index(pipeline_mod.ROUTE_METHOD_FIND_WORK_EMAIL)
        nd_idx = methods.index(pipeline_mod.ROUTE_METHOD_PERSON_BY_NAME_DOMAIN)
        assert blitz_fwe_idx < gl_idx < nd_idx

    def test_capability_gate_requires_linkedin_url(self):
        assert pipeline_mod._can_provider_use_method(
            pipeline_mod.ROUTE_METHOD_FIND_EMAIL_LINKEDIN_GETLEADS,
            {"linkedin_url": "https://linkedin.com/in/jane", "full_name": "",
             "domain": "", "first_name": "", "last_name": "", "phone": ""},
        ) is True
        assert pipeline_mod._can_provider_use_method(
            pipeline_mod.ROUTE_METHOD_FIND_EMAIL_LINKEDIN_GETLEADS,
            {"linkedin_url": "", "full_name": "Jane Doe",
             "domain": "acme.com", "first_name": "Jane", "last_name": "Doe",
             "phone": ""},
        ) is False

    def test_provider_label(self):
        assert (
            pipeline_mod._provider_label(
                pipeline_mod.ROUTE_METHOD_FIND_EMAIL_LINKEDIN_GETLEADS
            )
            == pipeline_mod.ROUTE_PROVIDER_GETLEADS
        )


# ---------------------------------------------------------------------------
# 2/3. Executor: getleads fires ONLY after blitz; blitz hit short-circuits
# ---------------------------------------------------------------------------


def _linkedin_route():
    return pipeline_mod.route_enrichment(
        linkedin_url="https://linkedin.com/in/jane"
    )


class TestRunEnrichmentRouteGetLeadsLinkedin:
    def test_blitz_miss_getleads_hit_returns_source_getleads(self):
        called: list[str] = []

        async def fake_cc_person_by_linkedin(http, url):
            called.append("contacts_db")
            return None

        async def fake_blitz_pebl(http, url, **kw):
            called.append("blitz")
            return {"found": False, "email": ""}

        async def fake_blitz_fwe(http, url):
            called.append("blitz")
            return {"found": False, "email": ""}

        async def fake_gl(http, url):
            called.append("getleads")
            assert url == "https://linkedin.com/in/jane"
            return {
                "email": "jane@acme.com",
                "first_name": "Jane",
                "last_name": "Doe",
                "domain": "acme.com",
                "verification_status": "Valid",
                "linkedin_url": "https://linkedin.com/in/jane",
                "phone": "+1-555-0100",
                "job_title": "CEO",
                "city": "Austin",
            }

        with patch.object(cc, "person_by_linkedin", fake_cc_person_by_linkedin), \
             patch.object(cc, "extract_email_from_contacts_response",
                          MagicMock(return_value="")), \
             patch.object(bc, "person_enrich_by_linkedin", fake_blitz_pebl), \
             patch.object(bc, "find_work_email", fake_blitz_fwe), \
             patch.object(gl, "find_email_by_linkedin", fake_gl):
            result = _run(pipeline_mod.run_enrichment_route(
                _linkedin_route(),
                AsyncMock(), AsyncMock(),
                asyncio.Semaphore(1),
                validate_email=False,
                job_id="job-gl-li",
                row_index=0,
                emit_logs=False,
            ))

        assert result["email"] == "jane@acme.com"
        assert result["source"] == pipeline_mod.SOURCE_GETLEADS
        assert result["verification"]["dm_email_verified"] == "yes"
        gl_dm = result["verification"].get("getleads_dm") or {}
        assert gl_dm.get("title") == "CEO"
        assert gl_dm.get("city") == "Austin"
        # ORDER PROOF: getleads strictly AFTER every blitz call.
        assert "getleads" in called
        assert all(
            called.index("getleads") > i
            for i, p in enumerate(called) if p == "blitz"
        ), called

    def test_blitz_hit_short_circuits_getleads(self):
        called: list[str] = []

        async def fake_cc_person_by_linkedin(http, url):
            called.append("contacts_db")
            return None

        async def fake_blitz_pebl(http, url, **kw):
            called.append("blitz")
            return {"found": True, "email": "jane@blitz.com", "all_emails": []}

        async def fail_gl(http, url):
            raise AssertionError("GetLeads must NOT be called when Blitz hits")

        with patch.object(cc, "person_by_linkedin", fake_cc_person_by_linkedin), \
             patch.object(cc, "extract_email_from_contacts_response",
                          MagicMock(return_value="")), \
             patch.object(bc, "person_enrich_by_linkedin", fake_blitz_pebl), \
             patch.object(gl, "find_email_by_linkedin", fail_gl):
            result = _run(pipeline_mod.run_enrichment_route(
                _linkedin_route(),
                AsyncMock(), AsyncMock(),
                asyncio.Semaphore(1),
                validate_email=False,
                job_id="job-gl-li-short",
                row_index=0,
                emit_logs=False,
            ))

        assert result["email"] == "jane@blitz.com"
        assert result["source"] == pipeline_mod.SOURCE_BLITZ_EMAIL
        assert "getleads" not in called

    def test_getleads_miss_returns_not_found(self):
        async def fake_cc(http, url):
            return None

        async def fake_blitz(http, url, **kw):
            return {"found": False, "email": ""}

        async def fake_gl(http, url):
            return None

        with patch.object(cc, "person_by_linkedin", fake_cc), \
             patch.object(cc, "extract_email_from_contacts_response",
                          MagicMock(return_value="")), \
             patch.object(bc, "person_enrich_by_linkedin", fake_blitz), \
             patch.object(bc, "find_work_email", fake_blitz), \
             patch.object(gl, "find_email_by_linkedin", fake_gl):
            result = _run(pipeline_mod.run_enrichment_route(
                _linkedin_route(),
                AsyncMock(), AsyncMock(),
                asyncio.Semaphore(1),
                validate_email=False,
                job_id="job-gl-li-miss",
                row_index=0,
                emit_logs=False,
            ))

        assert result["email"] == ""
        assert result["source"] == pipeline_mod.SOURCE_NOT_FOUND

    def test_force_provider_getleads_keeps_only_getleads(self):
        """force_provider=getleads on a linkedin_only route drops contacts_db
        and blitz (contacts_db survives only via the allowlist rule for
        selected_providers, not force_provider)."""
        route = pipeline_mod.route_enrichment(
            linkedin_url="https://linkedin.com/in/jane",
            force_provider="getleads",
        )
        providers = [s["provider"] for s in route["steps"]]
        assert providers == ["getleads"], providers

    def test_selected_providers_excludes_getleads_drops_step(self):
        route = pipeline_mod.route_enrichment(
            linkedin_url="https://linkedin.com/in/jane",
            selected_providers=["blitz"],
        )
        providers = [s["provider"] for s in route["steps"]]
        assert "getleads" not in providers
        assert providers == ["contacts_db", "blitz", "blitz"], providers


# ---------------------------------------------------------------------------
# 4. list_builder Flow 3: batch pre-pass + no single refire
# ---------------------------------------------------------------------------


def _gl_li_result(url: str, email: str) -> dict[str, Any]:
    return {
        "email": email,
        "first_name": "First",
        "last_name": "Last",
        "person_full_name": "First Last",
        "domain": "acme.com",
        "verification_status": "Valid",
        "linkedin_url": url,
        "phone": "",
        "job_title": "CEO",
        "linkedin_headline": "CEO at Acme",
        "city": "Austin",
        "country": "US",
        "job_level": "C-Team",
        "job_function": "Operations",
        "revenue": "$10M-$50M",
        "employee_count": "50-200",
    }


_GL_MISS = {
    "email": "", "first_name": "", "last_name": "", "domain": "",
    "verification_status": "unknown", "linkedin_url": "", "phone": "",
}


class TestFlow3GetLeadsBatchPrepass:
    def _patch_steps(self, monkeypatch):
        """Contacts DB + Blitz always miss so every URL reaches GetLeads."""
        monkeypatch.setattr(
            cc, "person_by_linkedin", AsyncMock(return_value=None))
        monkeypatch.setattr(
            cc, "extract_email_from_contacts_response",
            MagicMock(return_value=""))
        monkeypatch.setattr(
            lb.blitz_client, "person_enrich_by_linkedin",
            AsyncMock(return_value={"found": False, "email": ""}))

    def test_batch_called_once_per_100_no_singles(self, monkeypatch):
        self._patch_steps(monkeypatch)
        n = 150
        counters = {"batch": 0, "single": 0}
        seen_urls: list[str] = []

        async def fake_batch(http, urls):
            counters["batch"] += 1
            seen_urls.extend(urls)
            return [_gl_li_result(u, f"hit@{u}.com") for u in urls]

        async def fail_single(http, url):
            counters["single"] += 1
            raise AssertionError("single find_email_by_linkedin must not run")

        monkeypatch.setattr(gl, "find_emails_by_linkedin_batch", fake_batch)
        monkeypatch.setattr(gl, "find_email_by_linkedin", fail_single)

        rows = [{"linkedin_url": f"https://linkedin.com/in/u{i}"} for i in range(n)]
        out = _run(lb.run_linkedin_enrichment(rows=rows, linkedin_col="linkedin_url"))

        # One batch invocation carrying ALL unique URLs (the CLIENT chunks
        # to <=100 internally — covered by test_getleads_client.py).
        assert counters["batch"] == 1, counters
        assert len(seen_urls) == n
        assert counters["single"] == 0
        assert len(out) == n
        for row in out:
            assert row["dm_email"] == f"hit@{row['dm_linkedin_url']}.com"
            assert row["dm_email_source"] == lb.SOURCE_GETLEADS
            assert row["row_status"] == lb.STATUS_ENRICHED

    def test_batch_miss_does_not_refire_single(self, monkeypatch):
        self._patch_steps(monkeypatch)
        counters = {"batch": 0, "single": 0}

        async def fake_batch(http, urls):
            counters["batch"] += 1
            return [dict(_GL_MISS, linkedin_url=u) for u in urls]

        async def fail_single(http, url):
            counters["single"] += 1
            raise AssertionError(
                "a URL the batch already answered (miss) must NOT be re-fired"
            )

        monkeypatch.setattr(gl, "find_emails_by_linkedin_batch", fake_batch)
        monkeypatch.setattr(gl, "find_email_by_linkedin", fail_single)

        rows = [{"linkedin_url": f"https://linkedin.com/in/u{i}"} for i in range(3)]
        out = _run(lb.run_linkedin_enrichment(rows=rows, linkedin_col="linkedin_url"))

        assert counters["batch"] == 1
        assert counters["single"] == 0
        assert all(r["row_status"] == lb.STATUS_NOT_FOUND for r in out)

    def test_prepass_skipped_single_url_uses_single_call(self, monkeypatch):
        """A single URL (pre-pass gate needs >=2) falls back to ONE
        find_email_by_linkedin call per URL."""
        self._patch_steps(monkeypatch)
        counters = {"batch": 0, "single": 0}

        async def fail_batch(http, urls):
            counters["batch"] += 1
            raise AssertionError("batch must not run for a single URL")

        async def fake_single(http, url):
            counters["single"] += 1
            return _gl_li_result(url, "single-hit@acme.com")

        monkeypatch.setattr(gl, "find_emails_by_linkedin_batch", fail_batch)
        monkeypatch.setattr(gl, "find_email_by_linkedin", fake_single)

        rows = [{"linkedin_url": "https://linkedin.com/in/solo"}]
        out = _run(lb.run_linkedin_enrichment(rows=rows, linkedin_col="linkedin_url"))

        assert counters["batch"] == 0
        assert counters["single"] == 1
        assert out[0]["dm_email"] == "single-hit@acme.com"
        assert out[0]["dm_email_source"] == lb.SOURCE_GETLEADS

    def test_contacts_db_hit_wins_over_getleads(self, monkeypatch):
        """Contacts DB priority untouched: the batch pre-pass may run up-front
        (mirroring the smartprospect pre-pass design), but a Contacts DB hit
        means GetLeads never WINS — every row is contacts_db-sourced."""
        counters = {"batch": 0, "single": 0}

        async def fake_cc(http, url):
            return {"full_name": "Jane Doe", "email": "jane@contacts.db"}

        async def fake_batch(http, urls):
            counters["batch"] += 1
            # Even a GetLeads "hit" must lose to Contacts DB.
            return [_gl_li_result(u, "gl-should-lose@acme.com") for u in urls]

        async def fail_single(http, url):
            counters["single"] += 1
            raise AssertionError("single getleads call must not run")

        monkeypatch.setattr(cc, "person_by_linkedin", fake_cc)
        monkeypatch.setattr(
            cc, "extract_email_from_contacts_response",
            MagicMock(return_value="jane@contacts.db"))
        monkeypatch.setattr(gl, "find_emails_by_linkedin_batch", fake_batch)
        monkeypatch.setattr(gl, "find_email_by_linkedin", fail_single)

        rows = [
            {"linkedin_url": "https://linkedin.com/in/a"},
            {"linkedin_url": "https://linkedin.com/in/b"},
        ]
        out = _run(lb.run_linkedin_enrichment(rows=rows, linkedin_col="linkedin_url"))

        assert counters["single"] == 0
        assert all(
            r["dm_email_source"] == lb.SOURCE_CONTACTS_DB_LINKEDIN for r in out
        )
        assert all(r["dm_email"] == "jane@contacts.db" for r in out)

    def test_getleads_win_carries_dm_overlay(self, monkeypatch):
        """A GetLeads win on the Flow 3 path overlays the DM attributes
        onto the output row (title/city/phone/level etc.)."""
        self._patch_steps(monkeypatch)

        async def fake_batch(http, urls):
            return [_gl_li_result(u, "gl@acme.com") for u in urls]

        monkeypatch.setattr(gl, "find_emails_by_linkedin_batch", fake_batch)

        rows = [
            {"linkedin_url": "https://linkedin.com/in/jane"},
            {"linkedin_url": "https://linkedin.com/in/john"},
        ]
        out = _run(lb.run_linkedin_enrichment(rows=rows, linkedin_col="linkedin_url"))

        row = out[0]
        assert row["dm_email"] == "gl@acme.com"
        assert row["dm_email_verified"] == "yes"
        assert row["dm_title"] == "CEO"
        assert row["dm_headline"] == "CEO at Acme"
        assert row["dm_location_city"] == "Austin"
        assert row["dm_job_level"] == "C-Team"
        assert row["dm_job_function"] == "Operations"
        assert row["dm_full_name"] == "First Last"


# ---------------------------------------------------------------------------
# 5. Mirror invariants (re-asserted at runtime)
# ---------------------------------------------------------------------------


def test_valid_providers_mirror_x3():
    assert set(pipeline_mod.VALID_PROVIDERS) == set(routes_mod.VALID_PROVIDERS)
    assert set(pipeline_mod.VALID_PROVIDERS) == set(lb.VALID_PROVIDERS)
    assert set(pipeline_mod.VALID_PROVIDERS) == {
        "contacts_db", "blitz", "getleads", "smartprospect",
        "wizleads", "better_enrich",
    }


def test_enriched_columns_mirror_x2():
    assert list(pipeline_mod.ENRICHED_COLUMNS) == list(lb.ENRICHED_COLUMNS)
