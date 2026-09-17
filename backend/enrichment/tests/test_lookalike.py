"""Tests for the lookalike mode (enrichment/lookalike.py + tam_routes seeds
+ tam_flow GetLeads leg).

Network is fully mocked: GetLeads search_contacts_companies, Blitz
domain_to_linkedin and blitz_search.company_enrich are patched per test.
"""

from __future__ import annotations

import asyncio
import os
import sys
import unittest
from pathlib import Path
from typing import Any, Optional
from unittest.mock import patch

import httpx

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from enrichment import lookalike
from enrichment import tam_flow


def _seed(
    domain: str, *, industry: Optional[str] = None, band: Optional[str] = None,
    specialties: Optional[list[str]] = None, country: Optional[str] = None,
    resolved: bool = True, source: str = "getleads", name: Optional[str] = None,
) -> dict[str, Any]:
    return {
        "input": domain, "kind": "domain", "resolved": resolved, "source": source,
        "name": name or domain.split(".")[0].title(), "domain": domain if resolved else None,
        "linkedin_url": None, "industry": industry, "size_band": band,
        "specialties": specialties or [], "country": country,
    }


class TestBandHelpers(unittest.TestCase):
    def test_normalize_getleads_form(self):
        self.assertEqual(lookalike.normalize_band("11 to 50"), "11-50")
        self.assertEqual(lookalike.normalize_band("5001 to 10000"), "5001-10000")
        self.assertEqual(lookalike.normalize_band("10001+"), "10001+")
        self.assertIsNone(lookalike.normalize_band("weird-size"))
        self.assertIsNone(lookalike.normalize_band(None))

    def test_to_getleads_band(self):
        self.assertEqual(lookalike.to_getleads_band("11-50"), "11 to 50")
        self.assertEqual(lookalike.to_getleads_band("10001+"), "10001+")
        self.assertIsNone(lookalike.to_getleads_band(None))


class TestParseSeed(unittest.TestCase):
    def test_domain_forms(self):
        self.assertEqual(lookalike.parse_seed("acme.com"),
                         {"kind": "domain", "value": "acme.com"})
        self.assertEqual(lookalike.parse_seed("https://www.acme.com/about"),
                         {"kind": "domain", "value": "acme.com"})
        self.assertEqual(
            lookalike.parse_seed("https://www.linkedin.com/company/acme/"),
            {"kind": "linkedin_url",
             "value": "https://www.linkedin.com/company/acme"})

    def test_invalid(self):
        self.assertEqual(lookalike.parse_seed("not a domain !!"),
                         {"kind": "invalid", "value": "not a domain !!"})
        self.assertEqual(lookalike.parse_seed(""),
                         {"kind": "invalid", "value": ""})


class TestSynthesizeProfile(unittest.TestCase):
    def test_majority_industry_and_shared_keywords(self):
        profile = lookalike.synthesize_profile([
            _seed("a.com", industry="Software Development", band="11-50",
                  specialties=["saas", "b2b marketing", "analytics"], country="US"),
            _seed("b.com", industry="Software Development", band="51-200",
                  specialties=["saas", "analytics"], country="US"),
            _seed("c.com", industry="Advertising", band="11-50",
                  specialties=["saas"], country="GB"),
        ])
        self.assertEqual(profile["industries"], ["Software Development"])
        self.assertIn("saas", profile["keywords"])
        self.assertIn("analytics", profile["keywords"])
        self.assertNotIn("b2b marketing", profile["keywords"])  # only 1 seed
        self.assertEqual(profile["size_band"], "11-50")  # median of 11-50/51-200/11-50
        self.assertEqual(profile["countries"], ["US"])
        self.assertFalse(profile["conflicting_profiles"])

    def test_conflicting_seeds_flag(self):
        profile = lookalike.synthesize_profile([
            _seed("a.com", industry="Software Development"),
            _seed("b.com", industry="Dental Care"),
        ])
        self.assertTrue(profile["conflicting_profiles"])
        self.assertEqual(profile["industries"], [])
        self.assertEqual(profile["keywords"], [])

    def test_unresolved_seeds_ignored(self):
        profile = lookalike.synthesize_profile([
            _seed("a.com", industry="Software Development"),
            _seed("b.com", resolved=False),
        ])
        self.assertEqual(profile["seeds_resolved"], 1)
        # Single resolved seed: its own traits become the suggestions.
        self.assertEqual(profile["industries"], ["Software Development"])


class TestResolveSeed(unittest.TestCase):
    def test_blitz_hit_skips_getleads(self):
        """Blitz profiles first; a Blitz hit must not spend a GetLeads credit."""
        async def fake_gl(client, *, domains=None, limit=1, **kw):
            raise AssertionError("getleads must not be called when Blitz profiles")
            return {"ok": True, "contacts": [{
                "org_company_name": "Acme", "org_domain": "acme.com",
                "org_industry_linkedin": "Software Development",
                "employee_count_range": "11 to 50",
            }]}

        async def fake_d2l(client, domain):
            return {"found": True,
                    "company_linkedin_url": "https://linkedin.com/company/acme"}

        async def fake_enrich(client, url):
            return {"company": {"name": "Acme", "domain": "acme.com",
                                "industry": "Software Development",
                                "size": "11-50", "specialties": [],
                                "hq": {"country_code": "US"}}}

        async def run():
            async with httpx.AsyncClient() as c:
                with patch.object(lookalike.getleads_client,
                                  "search_contacts_companies", fake_gl), \
                     patch.object(lookalike.blitz_client,
                                  "domain_to_linkedin", fake_d2l), \
                     patch.object(lookalike.blitz_search,
                                  "company_enrich", fake_enrich):
                    return await lookalike.resolve_seed(
                        c, lookalike.parse_seed("acme.com"))

        result = asyncio.run(run())
        self.assertTrue(result["resolved"])
        self.assertEqual(result["source"], "blitz")
        self.assertEqual(result["size_band"], "11-50")

    def test_getleads_domain_mismatch_treated_as_miss(self):
        """Live-seen bug (2026-09-18): GetLeads returned a mismatched company
        for notion.so. The org_domain trust guard must discard it and fall
        through to Blitz."""
        async def fake_gl(client, *, domains, limit=1, **kw):
            return {"ok": True, "contacts": [{
                "org_company_name": "Wrong Co", "org_domain": "other.com",
                "org_industry_linkedin": "Pet Care",
                "employee_count_range": "11 to 50",
            }]}

        async def fake_d2l(client, domain):
            return {"found": True,
                    "company_linkedin_url": "https://linkedin.com/company/acme"}

        async def fake_enrich(client, url):
            return {"company": {"name": "Acme", "domain": "acme.com",
                                "industry": "Software Development",
                                "size": "51-200", "specialties": ["saas"],
                                "hq": {"country_code": "US"}}}

        async def run():
            async with httpx.AsyncClient() as c:
                with patch.object(lookalike.getleads_client,
                                  "search_contacts_companies", fake_gl), \
                     patch.object(lookalike.blitz_client,
                                  "domain_to_linkedin", fake_d2l), \
                     patch.object(lookalike.blitz_search,
                                  "company_enrich", fake_enrich):
                    return await lookalike.resolve_seed(
                        c, lookalike.parse_seed("acme.com"))

        result = asyncio.run(run())
        self.assertEqual(result["source"], "blitz")  # GL only as fallback
        self.assertEqual(result["industry"], "Software Development")

    def test_blitz_fallback_on_getleads_miss(self):
        async def fake_gl(client, *, domains, limit=1, **kw):
            return {"ok": True, "contacts": []}

        async def fake_d2l(client, domain):
            return {"found": True,
                    "company_linkedin_url": "https://linkedin.com/company/acme"}

        async def fake_enrich(client, url):
            return {"company": {
                "name": "Acme", "domain": "acme.com", "industry": "Advertising",
                "size": "51-200", "specialties": ["branding"],
                "hq": {"country_code": "GB"},
            }}

        async def run():
            async with httpx.AsyncClient() as c:
                with patch.object(lookalike.getleads_client,
                                  "search_contacts_companies", fake_gl), \
                     patch.object(lookalike.blitz_client,
                                  "domain_to_linkedin", fake_d2l), \
                     patch.object(lookalike.blitz_search,
                                  "company_enrich", fake_enrich):
                    return await lookalike.resolve_seed(
                        c, lookalike.parse_seed("acme.com"))

        result = asyncio.run(run())
        self.assertTrue(result["resolved"])
        self.assertEqual(result["source"], "blitz")
        self.assertEqual(result["specialties"], ["branding"])
        self.assertEqual(result["country"], "GB")

    def test_never_raises(self):
        async def boom(client, *, domains, limit=1, **kw):
            raise RuntimeError("network down")

        async def run():
            async with httpx.AsyncClient() as c:
                with patch.object(lookalike.getleads_client,
                                  "search_contacts_companies", boom):
                    # d2l/enrich also fail -> unresolved, no exception
                    with patch.object(lookalike.blitz_client,
                                      "domain_to_linkedin", boom), \
                         patch.object(lookalike.blitz_search,
                                      "company_enrich", boom):
                        return await lookalike.resolve_seed(
                            c, lookalike.parse_seed("acme.com"))

        result = asyncio.run(run())
        self.assertFalse(result["resolved"])


class TestSeedsDisplayList(unittest.TestCase):
    def test_label(self):
        self.assertEqual(lookalike.seeds_display_list(["a.com", "b.com"]),
                         "a.com, b.com")
        self.assertEqual(lookalike.seeds_display_list(["a.com", "b.com", "c.com"]),
                         "a.com, b.com +1")


class TestGetleadsPullLeg(unittest.TestCase):
    @staticmethod
    def _page(contacts, *, credits, has_more, next_offset):
        return {"ok": True, "contacts": contacts, "has_more": has_more,
                "next_offset": next_offset, "query_credits_used": credits}

    def test_dedupe_and_credit_cap(self):
        pages = [
            self._page([
                {"org_company_name": "A", "org_domain": "a.com",
                 "org_industry_linkedin": "Software", "employee_count_range": "11 to 50"},
                {"org_company_name": "A dupe", "org_domain": "a.com"},
                {"org_company_name": "NoDomain Co", "org_domain": ""},
            ], credits=3, has_more=True, next_offset=3),
            self._page([
                {"org_company_name": "B", "org_domain": "b.com"},
            ], credits=1, has_more=False, next_offset=4),
        ]
        state = {"i": 0}

        async def fake_gl(client, *, limit=100, offset=0, **kw):
            page = pages[state["i"]]
            state["i"] += 1
            return page

        async def run():
            async with httpx.AsyncClient() as c:
                with patch.object(tam_flow.getleads_client,
                                  "search_contacts_companies", fake_gl):
                    return await tam_flow._getleads_company_pull(
                        c, getleads_filters={}, needed=10,
                        existing_domains=set(), exclude_domains={"seed.com"},
                        credits_cap=500,
                    )

        rows, credits = asyncio.run(run())
        self.assertEqual(credits, 4)
        domains = [r["domain"] for r in rows]
        self.assertIn("a.com", domains)
        self.assertIn("b.com", domains)
        self.assertIn(None, domains)  # domainless company kept
        self.assertEqual(len([d for d in domains if d == "a.com"]), 1)
        self.assertTrue(all(r["source"] == "getleads" for r in rows))
        self.assertTrue(all(r["size"] in (None, "11-50") for r in rows))

    def test_credit_cap_stops_pull(self):
        calls = {"n": 0}

        async def fake_gl(client, *, limit=100, offset=0, **kw):
            calls["n"] += 1
            return self._page([
                {"org_company_name": f"C{calls['n']}", "org_domain": f"c{calls['n']}.com"},
            ], credits=400, has_more=True, next_offset=calls["n"])

        async def run():
            async with httpx.AsyncClient() as c:
                with patch.object(tam_flow.getleads_client,
                                  "search_contacts_companies", fake_gl):
                    return await tam_flow._getleads_company_pull(
                        c, getleads_filters={}, needed=100,
                        existing_domains=set(), exclude_domains=set(),
                        credits_cap=500,
                    )

        rows, credits = asyncio.run(run())
        self.assertEqual(calls["n"], 2)  # 400 + 400 >= 500 cap
        self.assertEqual(credits, 800)
        self.assertEqual(len(rows), 2)

    def test_failed_leg_returns_empty(self):
        async def fake_gl(client, **kw):
            return {"ok": False, "contacts": [], "error": "insufficient_credits"}

        async def run():
            async with httpx.AsyncClient() as c:
                with patch.object(tam_flow.getleads_client,
                                  "search_contacts_companies", fake_gl):
                    return await tam_flow._getleads_company_pull(
                        c, getleads_filters={}, needed=10,
                        existing_domains=set(), exclude_domains=set(),
                        credits_cap=100,
                    )

        rows, credits = asyncio.run(run())
        self.assertEqual((rows, credits), ([], 0))


if __name__ == "__main__":
    unittest.main()
