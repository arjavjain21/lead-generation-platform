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
from enrichment import tam_legs


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


class TestSeedTextHelpers(unittest.TestCase):
    """Lookalike 2.0: homepage-text fetch/attach around resolve_seed."""

    def test_fetch_seed_homepage_texts_aligned_and_parallel(self):
        seeds = [
            _seed("a.com", resolved=True),
            _seed("b.com", resolved=False),  # unresolved -> "" without fetch
            _seed("c.com", resolved=True),
        ]

        async def fake_fetch(client, domain):
            return f"text for {domain}"

        async def run():
            async with httpx.AsyncClient() as c:
                with patch.object(lookalike.seed_text, "fetch_homepage_text",
                                  fake_fetch):
                    texts = await lookalike.fetch_seed_homepage_texts(c, seeds)
                    return texts, lookalike.attach_seed_texts(seeds, texts)

        texts, enriched = asyncio.run(run())
        self.assertEqual(texts, ["text for a.com", "", "text for c.com"])
        self.assertEqual(enriched[0]["website_text"], "text for a.com")
        # "for"/"a.com" drop out (<4 chars); "text" survives tokenization.
        self.assertEqual(enriched[0]["website_keywords"], ["text"])
        self.assertEqual(enriched[1]["website_text"], "")
        # Inputs untouched (immutable merge).
        self.assertNotIn("website_text", seeds[0])

    def test_rank_seed_payloads_use_website_text_then_about(self):
        seeds = [
            {**_seed("a.com", industry="Gambling", band="51-200"),
             "website_text": "prediction markets", "about": "about fallback"},
            {"resolved": False},  # dropped
            {**_seed("b.com", industry="Entertainment", band="51-200"),
             "about": "about only"},
        ]
        payloads = lookalike.rank_seed_payloads(seeds)
        self.assertEqual(len(payloads), 2)
        self.assertEqual(payloads[0]["text"], "prediction markets")
        self.assertEqual(payloads[1]["text"], "about only")
        self.assertEqual(payloads[0]["domain"], "a.com")

    def test_merge_profile_keywords_adds_website_keywords_and_sha(self):
        profile = {"keywords": ["saas"], "industries": ["Software Development"]}
        merged = lookalike.merge_profile_keywords(
            profile,
            ["prediction markets platform", "prediction markets app"],
        )
        self.assertIn("saas", merged["keywords"])
        self.assertIn("prediction", merged["keywords"])
        self.assertIn("markets", merged["keywords"])
        self.assertNotIn("platform", merged["keywords"])  # stopword
        self.assertEqual(len(merged["website_text_sha"]), 12)
        # Original profile untouched.
        self.assertNotIn("website_text_sha", profile)


class TestBuildQueryPlan(unittest.TestCase):
    """Lookalike 2.0 fan-out plan: ALL distinct industries (the fix for the
    majority-industry collapse), niche keywords from homepage texts + the
    niche box, median size band."""

    def _kalshi_style_seeds(self):
        # Three seeds, three DIFFERENT LinkedIn industries, no specialties —
        # the exact shape that used to produce zero chips -> 400.
        return [
            _seed("kalshi.com", industry="Financial Services", band="51-200"),
            _seed("polymarket.com", industry="Gambling", band="51-200"),
            _seed("novig.com", industry="Entertainment", band="201-500"),
        ]

    def test_all_distinct_industries_fan_out(self):
        plan = lookalike.build_query_plan(self._kalshi_style_seeds(), ["", "", ""])
        self.assertEqual(
            plan["industries"],
            ["Financial Services", "Gambling", "Entertainment"],
        )
        self.assertEqual(plan["size_band"], "51-200")  # median of 51-200/51-200/201-500
        self.assertEqual(plan["keywords"], [])

    def test_keywords_from_shared_homepage_tokens_plus_niche_box(self):
        texts = [
            "prediction markets trading sports app",
            "prediction markets sports trading exchange",
            "prediction sports markets trading events",
        ]
        plan = lookalike.build_query_plan(
            self._kalshi_style_seeds(), texts,
            extra_keywords=["Fantasy Sports ", "prediction"],
        )
        for expected in ("prediction", "markets", "sports", "trading"):
            self.assertIn(expected, plan["keywords"])
        self.assertIn("fantasy sports", plan["keywords"])  # niche box, lowered
        self.assertEqual(
            len(plan["keywords"]), len(set(plan["keywords"])), "deduped"
        )

    def test_unresolved_seeds_contribute_nothing(self):
        plan = lookalike.build_query_plan(
            [_seed("a.com", resolved=False), _seed("b.com", resolved=False)],
            ["", ""],
        )
        self.assertEqual(plan, {"industries": [], "keywords": [], "size_band": None})


class TestRankedScoring(unittest.TestCase):
    """tam_ranker.rank_rows: deterministic order + why_matched content."""

    SEEDS = [
        {"name": "Kalshi", "industry": "Gambling", "size_band": "51-200",
         "domain": "kalshi.com", "text": "prediction markets trading sports events"},
        {"name": "Polymarket", "industry": "Gambling", "size_band": "51-200",
         "domain": "polymarket.com", "text": "prediction markets sports trading"},
    ]
    PLAN = {"industries": ["Gambling"], "size_band": "51-200",
            "keywords": ["prediction", "markets", "sports"]}

    @staticmethod
    def _row(name, industry, size, domain, slogan=None):
        return {"name": name, "industry": industry, "size": size,
                "domain": domain, "slogan": slogan}

    def test_perfect_match_outranks_partial_outranks_none(self):
        rows = [
            self._row("Dental Clinic", "Dental Care", "11-50", "dental.example"),
            self._row("Prediction Sports Exchange", "Gambling", "51-200",
                      "pse.example", "prediction markets for sports"),
            self._row("Prediction Markets Co", "Gambling", "51-200",
                      "pmc.example", "prediction markets sports trading"),
        ]
        ranked = tam_flow.tam_ranker.rank_rows(rows, self.SEEDS, self.PLAN)
        self.assertEqual([r["name"] for r in ranked],
                         ["Prediction Markets Co",
                          "Prediction Sports Exchange",
                          "Dental Clinic"])
        scores = [r["match_score"] for r in ranked]
        self.assertEqual(scores, sorted(scores, reverse=True))
        self.assertGreater(scores[0], scores[-1])
        self.assertTrue(all(0 <= s <= 100 for s in scores))

    def test_why_matched_signals_present(self):
        rows = [self._row("Prediction Markets Co", "Gambling", "51-200",
                          "pmc.example", "prediction markets")]
        ranked = tam_flow.tam_ranker.rank_rows(rows, self.SEEDS, self.PLAN)
        why = ranked[0]["why_matched"]
        self.assertIn("prediction", why)          # keyword hits named
        self.assertIn("industry match", why)
        self.assertIn("size 51-200", why)
        self.assertIn(" · ", why)                 # signal separator

    def test_no_signals_says_broad_match(self):
        rows = [self._row("Generic Corp", "Construction", None, "generic.example")]
        ranked = tam_flow.tam_ranker.rank_rows(rows, self.SEEDS, self.PLAN)
        self.assertEqual(ranked[0]["why_matched"], "broad match")

    def test_inputs_not_mutated_and_columns_appended(self):
        rows = [self._row("Prediction Markets Co", "Gambling", "51-200",
                          "pmc.example")]
        ranked = tam_flow.tam_ranker.rank_rows(rows, self.SEEDS, self.PLAN)
        self.assertIn("match_score", ranked[0])
        self.assertIn("why_matched", ranked[0])
        self.assertNotIn("match_score", rows[0], "input rows stay untouched")

    def test_equal_scores_break_tie_by_name(self):
        rows = [
            self._row("Zeta Corp", "Construction", "11-50", "z.example"),
            self._row("Alpha Corp", "Construction", "11-50", "a.example"),
        ]
        ranked = tam_flow.tam_ranker.rank_rows(rows, self.SEEDS, self.PLAN)
        self.assertEqual([r["name"] for r in ranked], ["Alpha Corp", "Zeta Corp"])


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
                with patch.object(tam_legs.getleads_client,
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
                with patch.object(tam_legs.getleads_client,
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
                with patch.object(tam_legs.getleads_client,
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
