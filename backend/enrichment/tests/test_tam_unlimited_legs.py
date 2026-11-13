"""QuickEnrich + LeadOcean TAM discovery legs (2026-11-11 wiring).

Covers: client payload construction + response normalization, leg dedupe and
caps, untargeted-LO refusal, flow-level source acceptance, and the UI source
map pins. Mocked transports only — no vendor calls.
"""
from __future__ import annotations

import asyncio
import os
import sys
import unittest
from unittest.mock import AsyncMock, patch

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from enrichment import tam_legs  # noqa: E402
from enrichment import tam_flow  # noqa: E402


def _qe_company(page, cursor=None, more=False):
    return {
        "companies": [{
            "company_name": "Acme " + str(page),
            "final_email_domain": f"acme{page}.com",
            "linkedin_url": f"https://linkedin.com/company/acme{page}",
            "industry": "Software",
            "employee_count": "51-200",
        }],
        "next_cursor": cursor,
        "has_more": more,
    }


class TestQEClient(unittest.TestCase):
    def test_company_finder_payload_and_parse(self):
        from enrichment import quickenrich_client as qe
        captured = {}

        class FakeResp:
            status_code = 200
            def json(self):
                return {"data": [{"company_name": "X", "final_email_domain": "x.com"}],
                        "meta": {"next_cursor": "c2", "has_more": True}}

        async def fake_request(client, method, path, *, json_body=None, **_k):
            captured["payload"] = json_body
            return FakeResp()

        with patch.dict(os.environ, {"QUICKENRICH_API_KEY": "k"}), \
             patch.object(qe, "_request_with_retries", new=fake_request), \
             patch.object(qe, "is_configured", return_value=True), \
             patch.object(qe, "_is_enabled", return_value=True):
            out = asyncio.run(qe.company_finder(
                object(), industry=["Software"], country_code=["US"], cursor="c1"))
        self.assertEqual(out["next_cursor"], "c2")
        self.assertTrue(out["has_more"])
        payload = captured["payload"]
        self.assertEqual(payload["industry"], {"include": ["Software"]})
        self.assertEqual(payload["country_code"], {"include": ["US"]})
        self.assertEqual(payload["cursor"], "c1")
        self.assertEqual(payload["per_page"], 100)

    def test_filter_none_when_empty(self):
        from enrichment import quickenrich_client as qe
        self.assertIsNone(qe._finder_filter())
        self.assertEqual(qe._finder_filter(include=[], exclude=["X"]), {"exclude": ["X"]})


class TestLOClient(unittest.TestCase):
    def test_search_companies_normalizes_and_counts(self):
        from enrichment import leadocean_client as lo
        captured = {}

        class FakeResp:
            status_code = 200
            def json(self):
                return {"data": [{"company_data": {
                    "company_name": "Notion", "company_domain": "NOTION.so",
                    "company_linkedin_url": "https://linkedin.com/company/notionlabs",
                    "industry": "Software", "employee_range": "51-200"}}],
                        "meta": {"nextCursor": None, "records": 1}}

        async def fake_post(client, path, body, label):
            captured["body"] = body
            return 200, {"data": FakeResp().json()["data"], "meta": {"nextCursor": None, "records": 1}}

        with patch.object(lo, "_post_json", new=fake_post), \
             patch.object(lo, "_is_enabled", return_value=True), \
             patch.object(lo, "API_KEY", "k"):
            out = asyncio.run(lo.search_companies(object(), {"industry": ["Software"]}))
        row = out["companies"][0]
        self.assertEqual(row["domain"], "notion.so")
        self.assertEqual(row["name"], "Notion")
        self.assertIn("notionlabs", row["linkedin_url"])
        self.assertEqual(out["records_used"], 1)
        self.assertEqual(captured["body"]["industry"], ["Software"])
        self.assertEqual(captured["body"]["limit"], 100)


class TestLegs(unittest.TestCase):
    def test_qe_pull_dedupes_against_existing(self):
        async def fake_cf(client, **kw):
            return {"contacts": [{
                "company_name": "Acme", "email_domain": "acme.com",
                "company_linkedin_url": "https://linkedin.com/company/acme"}],
                "next_cursor": None, "has_more": False}

        from enrichment import quickenrich_client as qe
        with patch.object(qe, "contact_finder", new=fake_cf), \
             patch.object(qe, "is_configured", return_value=True), \
             patch.object(qe, "_is_enabled", return_value=True), \
             patch.dict(os.environ, {"QUICKENRICH_API_KEY": "k"}):
            rows, pages = asyncio.run(tam_legs.quickenrich_company_pull(
                object(), {}, {"job_title_include": ["CEO"]}, needed=10,
                existing_keys={"acme.com"}, exclude_domains=set()))
        self.assertEqual(rows, [], "existing-domain contact deduped away")

    def test_qe_pull_caps_rows(self):
        async def fake_cf(client, **kw):
            i = kw.get("cursor") or 0
            return {"contacts": [{"company_name": f"C{i}", "email_domain": f"c{i}.com"}],
                    "next_cursor": i + 1, "has_more": True}

        from enrichment import quickenrich_client as qe
        with patch.object(qe, "contact_finder", new=fake_cf), \
             patch.object(qe, "is_configured", return_value=True), \
             patch.object(qe, "_is_enabled", return_value=True), \
             patch.dict(os.environ, {"QUICKENRICH_API_KEY": "k",
                                     "TAM_QUICKENRICH_MAX_ROWS": "5",
                                     "TAM_QUICKENRICH_MAX_PAGES": "3"}):
            rows, pages = asyncio.run(tam_legs.quickenrich_company_pull(
                object(), {}, {"job_title_include": ["CEO"]}, needed=100,
                existing_keys=set(), exclude_domains=set()))
        self.assertLessEqual(len(rows), 5)
        self.assertLessEqual(pages, 3)

    def test_lo_pull_refuses_untargeted(self):
        rows, pages = asyncio.run(tam_legs.leadocean_company_pull(
            object(), {}, needed=10, existing_keys=set(), exclude_domains=set()))
        self.assertEqual((rows, pages), ([], 0),
                         "empty filters must not burn search records")

    def test_lo_pull_dedupes_and_labels(self):
        from enrichment import leadocean_client as lo
        async def fake_sc(client, params, *, cursor=None, **kw):
            return {"companies": [{"name": "Notion", "domain": "notion.so",
                                   "linkedin_url": "", "industry": "Software",
                                   "size": "51-200", "revenue": "", "country": ""}],
                    "next_cursor": None, "rows": 1, "records_used": 1}
        with patch.object(lo, "search_companies", new=fake_sc):
            rows, _ = asyncio.run(tam_legs.leadocean_company_pull(
                object(), {"industry_include": ["Software"]}, needed=5,
                existing_keys={"notion.so"}, exclude_domains=set()))
            self.assertEqual(rows, [], "existing key deduped")
            rows2, _ = asyncio.run(tam_legs.leadocean_company_pull(
                object(), {"type_include": ["Privately Held"],
                           "keywords_include": ["dental"]}, needed=5,
                existing_keys=set(), exclude_domains=set()))
        self.assertEqual(rows2[0]["source"], "leadocean")
        self.assertEqual(rows2[0]["matched_people"], 1)


class TestFlowSources(unittest.TestCase):
    def test_route_accepts_new_sources(self):
        import re
        src = open(os.path.join(_BACKEND_DIR, "enrichment", "tam_routes.py")).read()
        self.assertIn('"blitz", "getleads", "quickenrich", "leadocean"', src)

    def test_flow_runs_qe_and_lo_legs(self):
        """A sources=['quickenrich','leadocean'] run invokes both pulls."""
        import tempfile
        from pathlib import Path
        tmp = tempfile.mkdtemp()
        orig = tam_flow.OUTPUT_DIR
        tam_flow.OUTPUT_DIR = Path(tmp)
        calls = {"qe": 0, "lo": 0}

        async def fake_qe(http, cf, pf, **kw):
            calls["qe"] += 1
            return [{"name": "QE Co", "domain": "qeco.com", "linkedin_url": "",
                      "industry": "", "size": "", "revenue": "",
                      "matched_people": 1, "source": "quickenrich"}], 1

        async def fake_lo(http, cf, **kw):
            calls["lo"] += 1
            return [], 0

        params = {"sources": ["quickenrich", "leadocean"], "max_companies": 50,
                  "user_id": "u1", "company_filters": {"industry_include": ["Software"]},
                  "people": {"job_title_include": ["CEO"]}}
        try:
            with patch.object(tam_legs, "quickenrich_company_pull", new=fake_qe), \
                 patch.object(tam_legs, "leadocean_company_pull", new=fake_lo), \
                 patch.object(tam_flow, "_backup_companies_to_contacts_db",
                              new=AsyncMock(return_value={})), \
                 patch.object(tam_flow, "_emit_progress", new=AsyncMock()):
                summary = asyncio.run(tam_flow.run_tam_flow("job-qe-lo-1", params))
        finally:
            tam_flow.OUTPUT_DIR = orig
        self.assertEqual(calls, {"qe": 1, "lo": 1})
        self.assertEqual(summary["companies_found"], 1)
        self.assertIn("quickenrich", summary["sources"])
        self.assertIn("leadocean", summary["sources"])


if __name__ == "__main__":
    unittest.main()
