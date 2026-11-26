"""TAM V2 filter semantics tests (2026-11-26).

Acceptance matrix from the requirements: non-contiguous employee bands stay
disjoint (no broadening), multi-country OR, keyword include-ANY / exclude-ANY,
unknown-data policy (missing description ≠ confirmed match), legacy
single-value API compatibility, and the provider translation layer.
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

from enrichment import tam_flow  # noqa: E402
from enrichment import tam_legs  # noqa: E402
from enrichment import tam_routes  # noqa: E402


# ---------------------------------------------------------------------------
# Model / payload (Blitz arm)
# ---------------------------------------------------------------------------

class TestTamRequestModel(unittest.TestCase):
    def _req(self, **company):
        return tam_routes.TamCompanyFilters(**company)

    def test_noncontiguous_ranges_preserved_as_list(self):
        f = self._req(employee_range=["11-50", "201-500"])
        payload = f.to_payload()
        self.assertEqual(payload["employee_range"], ["11-50", "201-500"],
                         "both bands forwarded — no broadening to 11-500")

    def test_contiguous_ranges_not_merged_away_silently(self):
        # 11-50 + 51-200 stay two OR bands (equivalent to 11-200 in effect,
        # but the payload keeps what the user chose).
        f = self._req(employee_range=["11-50", "51-200"])
        self.assertEqual(f.to_payload()["employee_range"], ["11-50", "51-200"])

    def test_multi_country_list(self):
        f = self._req(hq_country_code=["US", "CA", "GB", "AU"])
        self.assertEqual(f.to_payload()["hq"]["country_code"],
                         ["US", "CA", "GB", "AU"])

    def test_keywords_include_exclude_lists(self):
        f = self._req(keywords_include=["jewelry", "jewellery"],
                      keywords_exclude=["manufacturing equipment"])
        p = f.to_payload()
        self.assertEqual(p["keywords"]["include"], ["jewelry", "jewellery"])
        self.assertEqual(p["keywords"]["exclude"], ["manufacturing equipment"])

    def test_legacy_single_value_still_accepted(self):
        f = self._req(employee_range=["11-50"], hq_country_code=["US"])
        p = f.to_payload()
        self.assertEqual(p["employee_range"], ["11-50"])
        self.assertEqual(p["hq"]["country_code"], ["US"])


# ---------------------------------------------------------------------------
# Provider translation
# ---------------------------------------------------------------------------

class TestProviderTranslation(unittest.TestCase):
    def test_gl_bands_and_countries_arrays(self):
        from enrichment.tam_routes import TamCompanyFilters, TamPeopleFilters, _getleads_filters

        class R:  # minimal TamRequest stand-in
            company = TamCompanyFilters(
                employee_range=["11-50", "201-500"],
                industry_include=["Apparel and Fashion"],
                hq_country_code=["US", "CA"],
                keywords_include=["jewelry"],
            )
            people = TamPeopleFilters(job_title_include=["CEO"])
        gl = _getleads_filters(R(), {})
        self.assertEqual(gl["company_size"], ["11 to 50", "201 to 500"])
        self.assertEqual(gl["countries"], ["US", "CA"])
        self.assertNotIn("keywords", gl, "GetLeads has no company-keyword filter")

    def test_qe_leg_payload_carries_bands_countries_keywords(self):
        captured = {}

        async def fake_cf(client, **kw):
            captured.update(kw)
            return {"contacts": [], "next_cursor": None, "has_more": False}

        from enrichment import quickenrich_client as qe
        cf = {"employee_range": ["11-50", "201-500"], "hq": {"country_code": ["US", "CA"]},
              "keywords": {"include": ["jewelry"], "exclude": ["wholesale"]}}
        with patch.object(qe, "contact_finder", new=fake_cf), \
             patch.object(qe, "is_configured", return_value=True), \
             patch.object(qe, "_is_enabled", return_value=True), \
             patch.dict(os.environ, {"QUICKENRICH_API_KEY": "k"}):
            asyncio.run(tam_legs.quickenrich_company_pull(
                object(), cf, {"job_title_include": ["CEO"]}, needed=10,
                existing_keys=set(), exclude_domains=set()))
        self.assertEqual(captured.get("employee_count"), ["11-50", "201-500"])
        self.assertEqual(captured.get("country_code"), ["US", "CA"])
        # contact-finder mode: company keywords NOT passed (its bio_li is an
        # employee-bio field — company keywords must never ride it).
        self.assertIsNone(captured.get("industry"))
        self.assertNotIn("keywords", captured)

    def test_qe_company_finder_mode_carries_keywords_and_exclude(self):
        captured = {}

        async def fake_finder(client, **kw):
            captured.update(kw)
            return {"companies": [], "next_cursor": None, "has_more": False}

        from enrichment import quickenrich_client as qe
        cf = {"keywords": {"include": ["jewelry"], "exclude": ["wholesale"]},
              "employee_range": ["11-50", "201-500"]}
        with patch.object(qe, "company_finder", new=fake_finder), \
             patch.object(qe, "is_configured", return_value=True), \
             patch.object(qe, "_is_enabled", return_value=True), \
             patch.dict(os.environ, {"QUICKENRICH_API_KEY": "k"}):
            asyncio.run(tam_legs.quickenrich_company_pull(
                object(), cf, {}, needed=10,
                existing_keys=set(), exclude_domains=set()))
        self.assertEqual(captured.get("keywords"), ["jewelry"])
        self.assertEqual(captured.get("exclude_keywords"), ["wholesale"])
        self.assertEqual(captured.get("employee_count"), ["11-50", "201-500"])

    def test_lo_leg_params_arrays_and_exclude_keywords(self):
        captured = {}

        async def fake_sc(client, params, *, cursor=None, **kw):
            captured.update(params)
            return {"companies": [], "next_cursor": None, "rows": 0, "records_used": 0}

        from enrichment import leadocean_client as lo
        cf = {"employee_range": ["11-50", "201-500"], "hq": {"country_code": ["US", "CA"]},
              "keywords": {"include": ["jewewelry"], "exclude": ["wholesale"]},
              "industry": {"include": ["Apparel and Fashion"]}}
        with patch.object(lo, "search_companies", new=fake_sc):
            asyncio.run(tam_legs.leadocean_company_pull(
                object(), cf, needed=10, existing_keys=set(), exclude_domains=set()))
        self.assertEqual(captured.get("employeeRange"), ["11-50", "201-500"])
        self.assertEqual(captured.get("hqCountry"), ["US", "CA"])
        self.assertEqual(captured.get("excludeKeywords"), ["wholesale"])


# ---------------------------------------------------------------------------
# Keyword provenance / unknown-data policy
# ---------------------------------------------------------------------------

class TestKeywordProvenance(unittest.TestCase):
    def _row(self, name, source, industry="", slogan=""):
        return {"name": name, "domain": name.lower().replace(" ", "") + ".com",
                "source": source, "industry": industry, "slogan": slogan}

    def test_native_sources_confirmed(self):
        rows = [self._row("Jewelry Co", "leadocean"), self._row("Blitz Jewels", "blitz")]
        stats = tam_flow._apply_keyword_provenance(
            rows, ["jewelry"], [], {"blitz", "leadocean", "quickenrich"})
        self.assertEqual(stats["confirmed"], 2)
        self.assertTrue(all(r["keyword_match"] == "confirmed" for r in rows))

    def test_unsupported_source_local_match_confirmed(self):
        rows = [self._row("Gemstone Traders", "getleads", industry="Jewelry")]
        stats = tam_flow._apply_keyword_provenance(
            rows, ["gemstone"], [], {"blitz"})
        self.assertEqual(rows[0]["keyword_match"], "confirmed")

    def test_missing_description_is_unverified_not_confirmed(self):
        rows = [self._row("Acme Corp", "getleads", industry="Apparel")]
        stats = tam_flow._apply_keyword_provenance(
            rows, ["jewelry"], [], set())
        self.assertEqual(rows[0]["keyword_match"], "unverified")
        self.assertEqual(stats["unverified"], 1)

    def test_exclusion_keyword_drops_unsupported_source_row(self):
        rows = [self._row("Recruitment Agency Plus", "getleads")]
        stats = tam_flow._apply_keyword_provenance(
            rows, ["jewelry"], ["recruitment agency"], set())
        self.assertEqual(rows, [])
        self.assertEqual(stats["excluded_local"], 1)

    def test_native_source_not_locally_dropped(self):
        # blitz/leadocean rows already had exclusion applied provider-side —
        # the local pass must not second-guess sparse local text.
        rows = [self._row("Acme", "blitz")]
        tam_flow._apply_keyword_provenance(rows, ["jewelry"], ["wholesale"], {"blitz"})
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["keyword_match"], "confirmed")

    def test_qe_native_only_in_company_finder_mode(self):
        self.assertIn("quickenrich", tam_flow._native_keyword_sources(
            ["quickenrich"], {}))
        self.assertNotIn("quickenrich", tam_flow._native_keyword_sources(
            ["quickenrich"], {"job_title_include": ["CEO"]}),
            "contact-finder mode has no company-description filter")


# ---------------------------------------------------------------------------
# Flow-level: provenance lands in the summary + CSV
# ---------------------------------------------------------------------------

class TestFlowKeywordStats(unittest.TestCase):
    def test_summary_carries_keyword_stats_and_notes(self):
        import tempfile
        from pathlib import Path
        tmp = tempfile.mkdtemp()
        orig = tam_flow.OUTPUT_DIR
        tam_flow.OUTPUT_DIR = Path(tmp)
        params = {
            "sources": ["quickenrich", "leadocean", "getleads"],
            "max_companies": 50, "user_id": "u1",
            "company_filters": {
                "industry": {"include": ["Apparel and Fashion"]},
                "employee_range": ["11-50", "51-200"],
                "hq": {"country_code": ["US", "CA"]},
                "keywords": {"include": ["jewelry"], "exclude": ["recruitment agency"]},
            },
            "people_filters": {"job_title": {"include": ["CEO"]}},
            "getleads_filters": {"industries": ["Apparel and Fashion"]},
        }

        async def fake_qe(http, cf, pf, **kw):
            return [{"name": "QE Jewel", "domain": "qejewel.com", "industry": "",
                      "size": "", "revenue": "", "linkedin_url": "",
                      "matched_people": 1, "source": "quickenrich"}], 1

        async def fake_lo(http, cf, **kw):
            return [{"name": "LO Jewel", "domain": "lojewel.com", "industry": "",
                      "size": "", "revenue": "", "linkedin_url": "",
                      "matched_people": 1, "source": "leadocean"}], 1

        async def fake_gl(http, getleads_filters, **kw):
            return ([{"name": "GL NoText", "domain": "glnotext.com", "industry": "Apparel",
                      "size": "", "revenue": "", "source": "getleads"}], 0)

        try:
            with patch.object(tam_legs, "quickenrich_company_pull", new=fake_qe),                  patch.object(tam_legs, "leadocean_company_pull", new=fake_lo),                  patch.object(tam_flow, "_getleads_company_pull", new=fake_gl),                  patch.object(tam_flow, "_backup_companies_to_contacts_db",
                              new=AsyncMock(return_value={})),                  patch.object(tam_flow, "_emit_progress", new=AsyncMock()):
                summary = asyncio.run(tam_flow.run_tam_flow("job-v2-1", params))
        finally:
            tam_flow.OUTPUT_DIR = orig
        ks = summary.get("keyword_stats") or {}
        self.assertEqual(ks.get("confirmed"), 1)
        self.assertEqual(ks.get("unverified"), 2)
        notes = " ".join(summary.get("filter_notes") or [])
        self.assertIn("quickenrich", notes)
        self.assertIn("unverified", notes)
        csv_path = Path(summary["csv_path"])
        import csv as _csv
        with open(csv_path, newline="", encoding="utf-8") as f:
            rows = list(_csv.DictReader(f))
        marks = {r["name"]: r["keyword_match"] for r in rows}
        self.assertEqual(marks["LO Jewel"], "confirmed")
        self.assertEqual(marks["QE Jewel"], "unverified")
        self.assertEqual(marks["GL NoText"], "unverified")


if __name__ == "__main__":
    unittest.main()
