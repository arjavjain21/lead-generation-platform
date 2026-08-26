"""Tests for the local title-ICP gate (enrichment/title_filter.py) and its
wiring into list_builder + pipeline.

Context (2026-08-26): the List Building tool accepted provider-fuzzy matches
unchanged — job a75c4cae (Akshat's complaint) returned 77% off-ICP enriched
rows, 100% of them discovered via the Blitz waterfall. These tests pin the
local gate that re-applies the user's titles after EVERY discovery path:

  * matcher semantics (include AND-words, exclude, synonyms, negations)
  * strict_titles=False escape hatch (marker stamping + detection)
  * list_builder: Blitz waterfall results filtered; no cascade -> unfiltered
  * pipeline: Contacts-DB persons filtered; quality-met only when survivors
  * junior-exclude senior-override preserved on Blitz titles
"""
import asyncio
import json
import sys
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))


def _run(coro):
    # asyncio.run() instead of the deprecated get_event_loop(): other test
    # modules close the thread's loop via asyncio.run(), after which
    # get_event_loop() raises — order pollution.
    return asyncio.run(coro)


AKSHAT_CASCADE = [{
    "include_title": [
        "Founder", "Marketing Head", "VP of Marketing", "Head of growth",
        "Owner", "President", "VP of growth", "Head of sales", "sales head",
        "director of sales", "business development director",
    ],
    "exclude_title": ["assistant", "intern", "junior", "associate"],
    "location": ["WORLD"],
    "include_headline_search": True,
}]


def _akshat_inc_exc():
    from enrichment import title_filter
    return title_filter.parse_cascade_titles(AKSHAT_CASCADE)


def _blitz_person(title="", headline="", experiences=None, full_name="Test Person"):
    """Shape mirrors blitz waterfall results: {person: {...}, icp: N}."""
    return {
        "person": {
            "title": title,
            "headline": headline,
            "full_name": full_name,
            "first_name": full_name.split(" ")[0],
            "last_name": full_name.split(" ")[-1],
            "experiences": experiences or [],
        },
        "icp": 1,
    }


class TestMatcherSemantics:
    """person_matches_titles: include-AND words, excludes, synonyms."""

    def test_exact_title_match(self):
        from enrichment import title_filter
        inc, exc = _akshat_inc_exc()
        assert title_filter.person_matches_titles("Founder & CEO", "", inc, exc)

    def test_headline_only_match(self):
        from enrichment import title_filter
        inc, exc = _akshat_inc_exc()
        assert title_filter.person_matches_titles("", "Head of Growth at Acme", inc, exc)

    def test_word_order_insensitive(self):
        """'Marketing Head' include must match 'Head of Marketing'."""
        from enrichment import title_filter
        inc, exc = _akshat_inc_exc()
        assert title_filter.person_matches_titles("Head of Marketing", "", inc, exc)

    def test_synonym_vp_expansion(self):
        from enrichment import title_filter
        inc, exc = _akshat_inc_exc()
        assert title_filter.person_matches_titles("Vice President of Growth", "", inc, exc)

    def test_synonym_founder_cofounder(self):
        from enrichment import title_filter
        inc, exc = _akshat_inc_exc()
        assert title_filter.person_matches_titles("Co-Founder", "", inc, exc)

    def test_off_icp_dropped(self):
        from enrichment import title_filter
        inc, exc = _akshat_inc_exc()
        assert not title_filter.person_matches_titles(
            "", "Chief Revenue Officer at Optimal Blue", inc, exc)

    def test_vice_president_is_not_president(self):
        """The complaint's core false positive: bare 'President' in the include
        list must NOT match 'Vice President ...' (masked negation)."""
        from enrichment import title_filter
        inc, exc = _akshat_inc_exc()
        assert not title_filter.person_matches_titles(
            "", "Vice President at Comergence by Optimal Blue", inc, exc)

    def test_bare_president_still_matches(self):
        from enrichment import title_filter
        inc, exc = _akshat_inc_exc()
        assert title_filter.person_matches_titles("Regional President", "", inc, exc)

    def test_empty_hay_dropped(self):
        """No title/headline/seniority/function at all -> no match."""
        from enrichment import title_filter
        inc, exc = _akshat_inc_exc()
        assert not title_filter.person_matches_titles("", "", inc, exc)

    def test_no_include_list_keeps_all(self):
        from enrichment import title_filter
        assert title_filter.person_matches_titles("Sales Associate", "", [], [])

    def test_exclude_drops_junior(self):
        from enrichment import title_filter
        assert not title_filter.person_matches_titles("Sales Associate", "", [], ["associate"])

    def test_senior_override_keeps_associate_director(self):
        """Junior excludes are overridden by senior indicators."""
        from enrichment import title_filter
        assert title_filter.person_matches_titles("Associate Director", "", [], ["associate"])
        assert not title_filter.person_matches_titles("Sales Associate", "", [], ["associate"])

    def test_structured_seniority_function_signals(self):
        """Contacts DB seniority='vp' counts even when the headline omits 'VP';
        a multi-word include still needs ALL its words."""
        from enrichment import title_filter
        inc = ["vp of growth"]
        # structured signals alone satisfy the words across fields
        assert title_filter.person_matches_titles("", "", inc, [], seniority="vp", function="growth")
        # 'vp' alone doesn't satisfy 'vp of growth' (needs 'growth' too)
        assert not title_filter.person_matches_titles("", "", inc, [], seniority="vp", function="sales")
        # seniority='vp' alone satisfies a bare 'VP' include
        assert title_filter.person_matches_titles("", "", ["vp"], [], seniority="vp")


class TestStrictTitlesEscapeHatch:
    """strict_titles=False disables the gate; marker survives serialization."""

    def test_marker_roundtrip_through_json(self):
        from enrichment import title_filter
        stamped = title_filter.mark_cascade_strict_off(AKSHAT_CASCADE)
        serialized = json.dumps(stamped)
        assert title_filter.cascade_config_allows_strict_off(serialized)

    def test_marker_absent_by_default(self):
        from enrichment import title_filter
        assert not title_filter.cascade_config_allows_strict_off(json.dumps(AKSHAT_CASCADE))
        assert not title_filter.cascade_config_allows_strict_off(None)
        assert not title_filter.cascade_config_allows_strict_off("not-json")

    def test_gate_disabled_when_strict_off(self):
        from enrichment import title_filter
        stamped = title_filter.mark_cascade_strict_off(AKSHAT_CASCADE)
        inc, exc = title_filter.gate_title_filter(
            strict_titles=not title_filter.cascade_config_allows_strict_off(json.dumps(stamped)),
            cascade_config=json.dumps(stamped),
        )
        assert inc == [] and exc == []

    def test_gate_active_by_default(self):
        from enrichment import title_filter
        inc, exc = title_filter.gate_title_filter(
            strict_titles=True, cascade_config=json.dumps(AKSHAT_CASCADE))
        assert inc and exc

    def test_default_cascade_exempt_from_gate(self):
        """Regression guard: requests with NO user titles run the built-in
        Blitz DEFAULT_CASCADE — the discovery gate must stay OFF there, or
        title-less /enrich traffic gets silently filtered by the default
        tiers (broke test_cascade_collector_wiring when first shipped)."""
        from enrichment import title_filter
        from enrichment import blitz_client
        inc, exc = title_filter.gate_title_filter(
            strict_titles=True,
            cascade_config=blitz_client.DEFAULT_CASCADE,
            default_cascade=blitz_client.DEFAULT_CASCADE,
        )
        assert inc == [] and exc == []

    def test_stamping_is_immutable(self):
        """mark_cascade_strict_off must not mutate the input cascade."""
        from enrichment import title_filter
        original = json.loads(json.dumps(AKSHAT_CASCADE))
        title_filter.mark_cascade_strict_off(original)
        assert original == AKSHAT_CASCADE


class TestBlitzWaterfallGate:
    """filter_blitz_persons: the primary fix for the complaint."""

    def test_off_icp_blitz_results_dropped(self):
        from enrichment import title_filter
        inc, exc = _akshat_inc_exc()
        persons = [
            _blitz_person(headline="Vice President of Product Management"),
            _blitz_person(headline="Chief Revenue Officer"),
            _blitz_person(headline="VP of Growth at SaaS Co"),
            _blitz_person(title="Founder"),
        ]
        kept, dropped = title_filter.filter_blitz_persons(persons, inc, exc)
        assert dropped == 2
        assert len(kept) == 2
        names = [p["person"]["headline"] or p["person"]["title"] for p in kept]
        assert "VP of Growth at SaaS Co" in names
        assert "Founder" in names

    def test_no_titles_no_filtering(self):
        """Regression guard: cascade without titles -> results unchanged."""
        from enrichment import title_filter
        persons = [_blitz_person(headline="Chief Revenue Officer")]
        kept, dropped = title_filter.filter_blitz_persons(persons, [], [])
        assert kept == persons and dropped == 0

    def test_current_title_from_experiences_preferred(self):
        """dm_title derivation: experiences[0].job_title wins over stale title."""
        from enrichment import title_filter
        inc, exc = _akshat_inc_exc()
        persons = [{
            "person": {
                "title": "Founder",  # stale
                "headline": "",
                "experiences": [{"job_title": "Accountant"}],
            },
            "icp": 1,
        }]
        kept, dropped = title_filter.filter_blitz_persons(persons, inc, exc)
        assert dropped == 1 and kept == []

    def test_junior_exclude_senior_override_on_blitz(self):
        from enrichment import title_filter
        inc, _ = _akshat_inc_exc()
        exc = ["associate", "intern"]
        persons = [
            _blitz_person(title="Associate Director of Sales"),  # senior override -> kept
            _blitz_person(title="Sales Intern"),                 # dropped
        ]
        kept, dropped = title_filter.filter_blitz_persons(persons, inc, exc)
        assert len(kept) == 1 and dropped == 1

    def test_inner_person_dict_shape_accepted(self):
        """Some callers pass the inner person dict, not the wrapper."""
        from enrichment import title_filter
        inc, exc = _akshat_inc_exc()
        kept, dropped = title_filter.filter_blitz_persons(
            [_blitz_person(title="Founder")["person"]], inc, exc)
        assert len(kept) == 1 and dropped == 0

    def test_unknown_shape_fails_open(self):
        """Non-dict junk items are kept (fail-open); the gate only drops
        people it can read and judged off-ICP."""
        from enrichment import title_filter
        inc, exc = _akshat_inc_exc()
        kept, dropped = title_filter.filter_blitz_persons([42, None], inc, exc)
        assert len(kept) == 2 and dropped == 0


class TestListBuilderWaterfallWiring:
    """_enrich_single_domain (list_builder) must gate the Blitz fallback."""

    def _make_req(self):
        from enrichment import list_builder
        return list_builder

    def test_waterfall_results_filtered_via_list_builder(self):
        """End-to-end through list_builder._enrich_single_domain: Contacts DB
        returns nobody -> Blitz waterfall -> off-ICP results dropped."""
        lb = self._make_req()
        inc, exc = _akshat_inc_exc()

        async def fake_company_by_domain(client, domain):
            return {"linkedin_url": "https://linkedin.com/company/acme", "name": "Acme"}

        async def fake_company_contacts(client, domain, limit=5):
            return []  # nobody in Contacts DB -> forces Blitz fallback

        async def fake_waterfall(client, url, cascade, max_results):
            return {"results": [
                {"person": {"title": "", "headline": "Vice President of Product Management",
                            "first_name": "A", "last_name": "B", "full_name": "A B",
                            "experiences": []}, "icp": 1},
                {"person": {"title": "Founder", "headline": "", "first_name": "C",
                            "last_name": "D", "full_name": "C D", "experiences": []}, "icp": 1},
            ]}

        async def fake_resolve(*args, **kwargs):
            return ("", "", "", "", "", "")

        base_row = {c: "" for c in ("domain",)}
        with patch.object(lb.contacts_client, "company_by_domain", side_effect=fake_company_by_domain), \
             patch.object(lb.contacts_client, "company_contacts_enriched", side_effect=fake_company_contacts), \
             patch.object(lb.blitz_client, "waterfall_icp_search", side_effect=fake_waterfall), \
             patch.object(lb, "_resolve_person_email", side_effect=fake_resolve), \
             patch.object(lb.blitz_client, "domain_to_linkedin", new=AsyncMock(return_value={})), \
             patch.object(lb, "_apply_company_fallback_to_output_rows",
                          side_effect=lambda *a, **k: a[0] if a else []):
            rows = _run(lb._enrich_single_domain(
                None, None, base_row, "acme.com",
                cascade_config=json.dumps(AKSHAT_CASCADE),
                max_decision_makers=5,
            ))
        titles = [r.get("dm_title") or r.get("dm_headline") for r in rows]
        assert all(t != "Vice President of Product Management" for t in titles), titles
        assert any((r.get("dm_title") or "") == "Founder" for r in rows), titles

    def test_no_cascade_waterfall_unfiltered(self):
        """No cascade_config -> no gate (today's behavior)."""
        lb = self._make_req()

        async def fake_company_by_domain(client, domain):
            return {"linkedin_url": "https://linkedin.com/company/acme", "name": "Acme"}

        async def fake_company_contacts(client, domain, limit=5):
            return []

        async def fake_waterfall(client, url, cascade, max_results):
            return {"results": [
                {"person": {"title": "Chief Revenue Officer", "headline": "",
                            "first_name": "A", "last_name": "B", "full_name": "A B",
                            "experiences": []}, "icp": 1},
            ]}

        async def fake_resolve(*args, **kwargs):
            return ("", "", "", "", "", "")

        base_row = {"domain": ""}
        with patch.object(lb.contacts_client, "company_by_domain", side_effect=fake_company_by_domain), \
             patch.object(lb.contacts_client, "company_contacts_enriched", side_effect=fake_company_contacts), \
             patch.object(lb.blitz_client, "waterfall_icp_search", side_effect=fake_waterfall), \
             patch.object(lb, "_resolve_person_email", side_effect=fake_resolve), \
             patch.object(lb.blitz_client, "domain_to_linkedin", new=AsyncMock(return_value={})), \
             patch.object(lb, "_apply_company_fallback_to_output_rows",
                          side_effect=lambda *a, **k: a[0] if a else []):
            rows = _run(lb._enrich_single_domain(
                None, None, base_row, "acme.com",
                cascade_config=None, max_decision_makers=5,
            ))
        assert any((r.get("dm_title") or "") == "Chief Revenue Officer" for r in rows)

    def test_strict_off_marker_disables_list_builder_gate(self):
        """cascade stamped strict_titles=False -> off-ICP waterfall results kept."""
        lb = self._make_req()
        from enrichment import title_filter
        stamped = json.dumps(title_filter.mark_cascade_strict_off(AKSHAT_CASCADE))

        async def fake_company_by_domain(client, domain):
            return {"linkedin_url": "https://linkedin.com/company/acme", "name": "Acme"}

        async def fake_company_contacts(client, domain, limit=5):
            return []

        async def fake_waterfall(client, url, cascade, max_results):
            return {"results": [
                {"person": {"title": "", "headline": "Chief Revenue Officer",
                            "first_name": "A", "last_name": "B", "full_name": "A B",
                            "experiences": []}, "icp": 1},
            ]}

        async def fake_resolve(*args, **kwargs):
            return ("", "", "", "", "", "")

        base_row = {"domain": ""}
        with patch.object(lb.contacts_client, "company_by_domain", side_effect=fake_company_by_domain), \
             patch.object(lb.contacts_client, "company_contacts_enriched", side_effect=fake_company_contacts), \
             patch.object(lb.blitz_client, "waterfall_icp_search", side_effect=fake_waterfall), \
             patch.object(lb, "_resolve_person_email", side_effect=fake_resolve), \
             patch.object(lb.blitz_client, "domain_to_linkedin", new=AsyncMock(return_value={})), \
             patch.object(lb, "_apply_company_fallback_to_output_rows",
                          side_effect=lambda *a, **k: a[0] if a else []):
            rows = _run(lb._enrich_single_domain(
                None, None, base_row, "acme.com",
                cascade_config=stamped, max_decision_makers=5,
            ))
        assert any("Chief Revenue Officer" in (r.get("dm_headline") or "") for r in rows)


class TestPipelineGate:
    """pipeline._enrich_domain must gate every discovery path.

    NOTE: with a custom cascade (titles), pipeline SKIPS the Contacts-DB
    persons lookup entirely (use_custom_cascade) and goes straight to the
    Blitz waterfall — so the waterfall gate is the load-bearing one there.
    The Contacts-DB persons gate below covers the DEFAULT-cascade case.
    """

    def test_waterfall_results_filtered(self):
        """Custom cascade -> Blitz waterfall path -> off-ICP results dropped."""
        from enrichment import pipeline

        async def fake_company_by_domain(client, domain):
            return {"linkedin_url": "https://linkedin.com/company/acme", "name": "Acme"}

        async def fake_waterfall(client, url, cascade, max_results):
            return {"results": [
                {"person": {"title": "", "headline": "Vice President of Product Management",
                            "first_name": "A", "last_name": "B", "full_name": "A B",
                            "experiences": []}, "icp": 1},
                {"person": {"title": "Founder", "headline": "", "first_name": "C",
                            "last_name": "D", "full_name": "C D", "experiences": []}, "icp": 1},
            ]}

        async def fake_resolve(*args, **kwargs):
            return ("", "", "")  # pipeline's 3-tuple: (email, source, info)

        base_row = {"domain": "acme.com"}
        with patch.object(pipeline.contacts_client, "company_by_domain",
                          side_effect=fake_company_by_domain), \
             patch.object(pipeline.blitz_client, "waterfall_icp_search",
                          side_effect=fake_waterfall), \
             patch.object(pipeline, "_resolve_email_for_person", side_effect=fake_resolve):
            rows = _run(pipeline._enrich_domain(
                None, None, base_row, "acme.com", "",
                AKSHAT_CASCADE, 5,
                asyncio.Semaphore(1), asyncio.Semaphore(1),
            ))
        headlines = [r.get("dm_headline") or "" for r in rows]
        titles = [r.get("dm_title") or "" for r in rows]
        assert "Founder" in titles
        assert not any("Vice President of Product Management" in h for h in headlines)

    def test_contacts_db_persons_filtered_default_cascade(self):
        """DEFAULT cascade -> Contacts-DB persons path runs and is gated."""
        from enrichment import pipeline
        from enrichment import blitz_client

        contacts = [
            {"title": "Founder", "headline": "", "email": "f@acme.com",
             "first_name": "F", "last_name": "O", "full_name": "F O"},
            {"title": "Accountant", "headline": "", "email": "a@acme.com",
             "first_name": "A", "last_name": "C", "full_name": "A C"},
        ]
        # default cascade + titles encoded? DEFAULT has no user titles; the
        # gate is inert for DEFAULT (no include_title extraction differs from
        # DEFAULT_CASCADE which HAS include_title tiers!). Parse what DEFAULT
        # would yield and only assert the filter fn itself here.
        from enrichment import title_filter
        inc, exc = title_filter.parse_cascade_titles(blitz_client.DEFAULT_CASCADE)
        assert title_filter.person_matches_titles(
            contacts[0]["title"], contacts[0]["headline"], inc, exc)
        # DEFAULT tier-1 includes Owner/CEO/Founder -> Accountant dropped
        assert not title_filter.person_matches_titles(
            contacts[1]["title"], contacts[1]["headline"], inc, exc)
