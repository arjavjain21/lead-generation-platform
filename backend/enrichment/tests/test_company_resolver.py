"""Company-name resolver tests (PR-2.2, 2026-11-11).

Ambiguity matrix, kill switch, cache TTL, blitz gating, and the runner-level
guarantee: an ambiguous name NEVER auto-joins (skipped with reviewable
candidates); a high-confidence name routes through the EXISTING domain
cascade (no duplicate waterfall).
"""
from __future__ import annotations

import asyncio
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from enrichment import company_resolver as cr  # noqa: E402


def _companies(*rows):
    return [{"name": n, "website": d, "country": c} for n, d, c in rows]


class TestNormalize(unittest.TestCase):
    def test_suffix_and_case_normalization(self):
        self.assertEqual(cr.normalize_company_name("Acme, Inc."), "acme")
        self.assertEqual(cr.normalize_company_name("THE Acme Corp"), "acme")
        self.assertEqual(cr.normalize_company_name("Acme Holdings Ltd"), "acme")
        self.assertEqual(cr.normalize_company_name(""), "")


class _TempDb(unittest.TestCase):
    def setUp(self):
        import tempfile
        from shared import db as shared_db
        self._tmp = tempfile.TemporaryDirectory()
        self._orig = shared_db.DB_PATH
        shared_db.DB_PATH = Path(self._tmp.name) / "jobs.db"
        shared_db._local.conn = None
        shared_db.init_db()

    def tearDown(self):
        from shared import db as shared_db
        conn = getattr(shared_db._local, "conn", None)
        if conn is not None:
            conn.close()
            shared_db._local.conn = None
        shared_db.DB_PATH = self._orig
        self._tmp.cleanup()


class TestResolveMatrix(_TempDb):
    def _resolve(self, companies, name="Acme", country_hint="", blitz_results=None):
        async def go():
            with patch.dict(os.environ, {"ENABLE_NAME_RESOLVER": "true"}), \
                 patch("enrichment.contacts_client.search_companies_by_name",
                       new=AsyncMock(return_value=companies)), \
                 patch("enrichment.blitz_client.company_search",
                       new=AsyncMock(return_value={"results": blitz_results or []})):
                return await cr.resolve_company_name(
                    object(), name, country_hint=country_hint,
                    conn=self.conn(), blitz_http=object())
        return asyncio.run(go())

    def conn(self):
        from shared import db as shared_db
        return shared_db.get_db()

    def test_unique_exact_match_is_high(self):
        r = self._resolve(_companies(("Acme Inc", "acme.com", "us")))
        self.assertEqual(r.status, "high")
        self.assertEqual(r.domain, "acme.com")

    def test_multiple_with_unique_country_hint_is_medium(self):
        rows = _companies(("Acme", "acme.us", "us"), ("Acme", "acme.de", "germany"))
        r = self._resolve(rows, country_hint="us")
        self.assertEqual(r.status, "medium")
        self.assertEqual(r.domain, "acme.us")

    def test_ambiguous_never_autojoins_and_is_reviewable(self):
        rows = _companies(("Acme", "acme.us", "us"), ("Acme", "acme.de", "germany"))
        r = self._resolve(rows)  # no hint → no discriminator
        self.assertEqual(r.status, "ambiguous")
        self.assertEqual(r.domain, "")  # NEVER joined
        self.assertEqual(len(r.candidates), 2)
        self.assertTrue(all("name" in c and "domain" in c for c in r.candidates))

    def test_none_when_no_strong_match(self):
        r = self._resolve(_companies(("Beta LLC", "beta.com", "us")), name="Acme")
        self.assertEqual(r.status, "none")

    def test_kill_switch_off_returns_none_without_calls(self):
        async def go():
            with patch.dict(os.environ, {"ENABLE_NAME_RESOLVER": "false"}), \
                 patch("enrichment.contacts_client.search_companies_by_name",
                       new=AsyncMock(side_effect=AssertionError("must not call"))):
                return await cr.resolve_company_name(object(), "Acme", conn=self.conn())
        r = asyncio.run(go())
        self.assertEqual(r.status, "none")

    def test_blitz_fallback_gated_off_by_default(self):
        async def go():
            with patch.dict(os.environ, {"ENABLE_NAME_RESOLVER": "true",
                                         "ENABLE_NAME_RESOLVER_BLITZ": "false"}), \
                 patch("enrichment.contacts_client.search_companies_by_name",
                       new=AsyncMock(return_value=[])), \
                 patch("enrichment.blitz_client.company_search",
                       new=AsyncMock(side_effect=AssertionError("blitz must stay off"))):
                return await cr.resolve_company_name(
                    object(), "Acme", conn=self.conn(), blitz_http=object())
        r = asyncio.run(go())
        self.assertEqual(r.status, "none")

    def test_cache_hit_second_call_zero_lookups(self):
        calls = {"n": 0}
        rows = _companies(("Acme Inc", "acme.com", "us"))

        async def counting(*_a, **_k):
            calls["n"] += 1
            return rows

        async def go():
            with patch.dict(os.environ, {"ENABLE_NAME_RESOLVER": "true"}), \
                 patch("enrichment.contacts_client.search_companies_by_name",
                       new=counting):
                a = await cr.resolve_company_name(object(), "Acme Inc", conn=self.conn())
                b = await cr.resolve_company_name(object(), "Acme Inc", conn=self.conn())
                return a, b
        a, b = asyncio.run(go())
        self.assertEqual((a.status, b.status), ("high", "high"))
        self.assertEqual(calls["n"], 1, "second resolve must be a cache hit")


class TestRunnerNameOnly(_TempDb):
    """Runner-level: ambiguous never joins; high routes the domain cascade."""

    def _run_rows(self, resolver_result):
        from enrichment import list_builder
        async def fake_resolve(_http, name, **_k):
            return resolver_result

        stubs = [
            patch("enrichment.list_builder._enrich_single_domain",
                  new=AsyncMock(return_value=[{"row_status": "no_contacts"}])),
            patch("enrichment.list_builder._attach_seg_columns",
                  new=AsyncMock(return_value=None)),
            patch("enrichment.list_builder.company_fallback.run_company_fallbacks",
                  new=AsyncMock(return_value={})),
        ]
        with patch.dict(os.environ, {"ENABLE_NAME_RESOLVER": "true"}), \
             patch("enrichment.list_builder.company_resolver.resolver_enabled",
                   return_value=True), \
             patch("enrichment.list_builder.company_resolver.resolve_company_name",
                   new=fake_resolve):
            for p in stubs:
                p.start()
            try:
                return asyncio.run(list_builder.run_domain_enrichment(
                    rows=[{"company_name": "Acme"}],
                    domain_col="", company_name_col="company_name"))
            finally:
                for p in stubs:
                    p.stop()

    def test_high_confidence_routes_domain_cascade(self):
        from enrichment.company_resolver import ResolveResult
        recorder = AsyncMock(return_value=[{"row_status": "no_contacts"}])
        self._run_rows_with_domain_recorder(
            ResolveResult(status="high", domain="acme.com", country="us"), recorder)
        recorder.assert_awaited_once()
        self.assertEqual(recorder.await_args.args[3], "acme.com",
                         "the RESOLVED domain is passed to the existing cascade")

    def _run_rows_with_domain_recorder(self, resolver_result, domain_recorder):
        from enrichment import list_builder
        async def fake_resolve(_http, name, **_k):
            return resolver_result

        stubs = [
            patch("enrichment.list_builder._enrich_single_domain", new=domain_recorder),
            patch("enrichment.list_builder._attach_seg_columns",
                  new=AsyncMock(return_value=None)),
            patch("enrichment.list_builder.company_fallback.run_company_fallbacks",
                  new=AsyncMock(return_value={})),
        ]
        with patch.dict(os.environ, {"ENABLE_NAME_RESOLVER": "true"}), \
             patch("enrichment.list_builder.company_resolver.resolver_enabled",
                   return_value=True), \
             patch("enrichment.list_builder.company_resolver.resolve_company_name",
                   new=fake_resolve):
            for p in stubs:
                p.start()
            try:
                return asyncio.run(list_builder.run_domain_enrichment(
                    rows=[{"company_name": "Acme"}],
                    domain_col="", company_name_col="company_name"))
            finally:
                for p in stubs:
                    p.stop()

    def test_ambiguous_skips_with_candidates(self):
        from enrichment.company_resolver import ResolveResult
        rows = self._run_rows(ResolveResult(
            status="ambiguous",
            candidates=[{"name": "Acme US", "domain": "acme.us", "score": 1.0},
                        {"name": "Acme DE", "domain": "acme.de", "score": 1.0}]))
        self.assertEqual(rows[0].get("row_status"), "skipped")
        self.assertEqual(rows[0].get("no_email_reason"), "ambiguous_company_name")
        self.assertIn("acme.us", rows[0].get("name_candidates", ""))


if __name__ == "__main__":
    unittest.main()
