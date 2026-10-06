"""Strict-title enforcement on paths that historically bypassed it (2026-10-06).

RCA (Akshat complaint, jobs 144ee780/939136b6): Flow-1 CSVs with a populated
Company LinkedIn column routed ~80% of rows to ``_enrich_by_company_linkedin``,
which (a) never received the job's cascade (Blitz waterfall searched the
DEFAULT Owner/CEO/Founder/President tiers, fuzzy) and (b) applied NO local
title gate — 97.4% / 38.6% of delivered contacts were off-ICP while the job
card badged "Strict titles ON".

These tests pin the closure:
  * company-URL orchestrator: user cascade reaches the waterfall; off-ICP
    persons dropped BEFORE email resolution; gate_stats counted
  * Step 2.4 GetLeads decision-makers fallback gated in list_builder AND
    pipeline
  * matcher precision: role-segment + contiguous function phrase + word
    boundaries (company-name words can't satisfy an include token)
"""
import asyncio
import json
import sys
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

PS_CASCADE = [{
    "include_title": [
        "VP Professional Services", "VP of Professional Services",
        "Head of Professional Services", "Director of Professional Services",
        "VP Customer Success", "Head of Customer Success",
        "Director of Implementation", "Head of Data",
    ],
    "exclude_title": ["assistant", "intern", "junior", "associate"],
    "location": ["WORLD"],
    "include_headline_search": True,
}]


def _run(coro):
    return asyncio.run(coro)


def _flat_person(headline="", title="", full_name="Test Person"):
    """Shape mirrors _enrich_by_company_waterfall's flat person dict."""
    return {
        "first_name": full_name.split(" ")[0],
        "last_name": full_name.split(" ")[-1],
        "full_name": full_name,
        "title": title,
        "job_level": "",
        "linkedin_url": f"https://linkedin.com/in/{full_name.replace(' ', '').lower()}",
        "email": "",
        "verified_email": "",
        "headline": headline,
        "location_city": "",
        "location_country": "",
        "icp_tier": 1,
        "ranking": 1,
        "previous_companies": "",
        "previous_titles": "",
    }


class TestCompanyUrlGate:
    """_enrich_by_company_linkedin must search the user cascade AND gate."""

    def test_off_icp_persons_dropped_and_on_icp_kept(self):
        from enrichment import list_builder
        persons = [
            _flat_person(headline="Vice President - Sales | @Informed.IQ", full_name="Sales VP"),
            _flat_person(headline="Co-Founder | @Informed.IQ", full_name="Cofounder"),
            _flat_person(headline="Vice President of Professional Services", full_name="PS VP"),
        ]
        resolved = ("ps@example.com", "", "blitz", "yes", "", "")

        with patch.object(list_builder, "_enrich_by_company_waterfall",
                          return_value=persons) as mock_wf, \
             patch.object(list_builder, "_resolve_person_email",
                          return_value=resolved) as mock_resolve:
            rows = _run(list_builder._enrich_by_company_linkedin(
                blitz_http=None, contacts_http=None,
                base_row={"domain": "informediq.com"},
                company_linkedin_url="https://linkedin.com/company/informediq",
                domain="informediq.com",
                cascade_config=json.dumps(PS_CASCADE),
            ))

        # Waterfall searched the USER's cascade, not the default tiers
        _, wf_kwargs = mock_wf.call_args
        assert wf_kwargs["cascade"][0]["include_title"] == PS_CASCADE[0]["include_title"]
        # Email resolution only spent on the survivor
        assert mock_resolve.await_count == 1
        assert len(rows) == 1
        assert rows[0]["dm_full_name"] == "PS VP"

    def test_gate_stats_counts_drops(self):
        from enrichment import list_builder
        persons = [
            _flat_person(headline="Vice President - Sales | @Informed.IQ"),
            _flat_person(headline="Vice President Finance | @Informed.IQ"),
            _flat_person(headline="Head of Professional Services"),
        ]
        gate_stats = {}
        with patch.object(list_builder, "_enrich_by_company_waterfall",
                          return_value=persons), \
             patch.object(list_builder, "_resolve_person_email",
                          return_value=("", "", "", "unknown", "", "")):
            _run(list_builder._enrich_by_company_linkedin(
                blitz_http=None, contacts_http=None,
                base_row={"domain": "x.com"},
                company_linkedin_url="https://linkedin.com/company/x",
                cascade_config=json.dumps(PS_CASCADE),
                gate_stats=gate_stats,
            ))
        assert gate_stats.get("dropped") == 2

    def test_no_cascade_stays_ungated(self):
        """No titles → DEFAULT cascade → gate exempt (title-less traffic)."""
        from enrichment import list_builder
        from enrichment import blitz_client
        persons = [_flat_person(headline="Chief Revenue Officer")]
        with patch.object(list_builder, "_enrich_by_company_waterfall",
                          return_value=persons) as mock_wf, \
             patch.object(list_builder, "_resolve_person_email",
                          return_value=("c@example.com", "", "blitz", "yes", "", "")):
            rows = _run(list_builder._enrich_by_company_linkedin(
                blitz_http=None, contacts_http=None,
                base_row={"domain": "x.com"},
                company_linkedin_url="https://linkedin.com/company/x",
            ))
        _, wf_kwargs = mock_wf.call_args
        assert wf_kwargs["cascade"] == blitz_client.DEFAULT_CASCADE
        assert len(rows) == 1  # CRO kept — nobody asked for titles

    def test_strict_off_marker_disables_company_gate(self):
        from enrichment import list_builder
        from enrichment import title_filter
        stamped = json.dumps(title_filter.mark_cascade_strict_off(PS_CASCADE))
        persons = [_flat_person(headline="Chief Revenue Officer")]
        with patch.object(list_builder, "_enrich_by_company_waterfall",
                          return_value=persons), \
             patch.object(list_builder, "_resolve_person_email",
                          return_value=("c@example.com", "", "blitz", "yes", "", "")):
            rows = _run(list_builder._enrich_by_company_linkedin(
                blitz_http=None, contacts_http=None,
                base_row={"domain": "x.com"},
                company_linkedin_url="https://linkedin.com/company/x",
                cascade_config=stamped,
            ))
        assert len(rows) == 1  # escape hatch honoured on the company path too

    def test_explicit_cascade_list_gates_too(self):
        """The unified /enrich caller passes a cascade LIST — gate derives
        from it the same way."""
        from enrichment import list_builder
        persons = [_flat_person(headline="VP of Sales | @Admiral Consulting Group")]
        with patch.object(list_builder, "_enrich_by_company_waterfall",
                          return_value=persons), \
             patch.object(list_builder, "_resolve_person_email",
                          return_value=("v@example.com", "", "blitz", "yes", "", "")) as mock_resolve:
            rows = _run(list_builder._enrich_by_company_linkedin(
                blitz_http=None, contacts_http=None,
                base_row={"domain": "admiral.com"},
                company_linkedin_url="https://linkedin.com/company/admiral",
                cascade=PS_CASCADE,
            ))
        assert rows and rows[0]["row_status"] != list_builder.STATUS_ENRICHED
        assert mock_resolve.await_count == 0  # dropped before email spend


class TestGetLeadsDmFallbackGate:
    """Step 2.4 (list_builder + pipeline): generic C-Team/VP/Head people
    from the decision-makers layer must pass the gate under strict titles."""

    def _dm_contacts(self):
        return [
            {"person_full_name": "A CTO", "first_name": "A", "last_name": "CTO",
             "linkedin_url": "https://linkedin.com/in/a", "job_title": "Chief Technology Officer",
             "email": "a@x.com"},
            {"person_full_name": "B CFO", "first_name": "B", "last_name": "CFO",
             "linkedin_url": "https://linkedin.com/in/b", "job_title": "VP of Finance",
             "email": "b@x.com"},
            {"person_full_name": "C PS", "first_name": "C", "last_name": "PS",
             "linkedin_url": "https://linkedin.com/in/c", "job_title": "Director of Professional Services",
             "email": "c@x.com"},
        ]

    def test_list_builder_dm_fallback_gated(self):
        from enrichment import list_builder
        lb = list_builder

        async def fake_company_by_domain(client, domain):
            return {"linkedin_url": "https://linkedin.com/company/x", "name": "X"}

        async def fake_company_contacts(client, domain, limit=5):
            return []

        async def fake_waterfall(client, url, cascade, max_results):
            return {"results": []}  # empty → Step 2.4 fires

        async def fake_dm(blitz_http, domain, limit=25):
            return self._dm_contacts()

        async def fake_resolve(*args, **kwargs):
            return ("", "", "", "unknown", "", "")

        base_row = {"domain": ""}
        with patch.object(lb.contacts_client, "company_by_domain",
                          side_effect=fake_company_by_domain), \
             patch.object(lb.contacts_client, "company_contacts_enriched",
                          side_effect=fake_company_contacts), \
             patch.object(lb.blitz_client, "waterfall_icp_search",
                          side_effect=fake_waterfall), \
             patch.object(lb.getleads_client, "lookup_decision_makers",
                          side_effect=fake_dm), \
             patch.object(lb, "_resolve_person_email", side_effect=fake_resolve), \
             patch.object(lb.blitz_client, "domain_to_linkedin",
                          new=AsyncMock(return_value={})), \
             patch.object(lb, "_apply_company_fallback_to_output_rows",
                          side_effect=lambda *a, **k: a[0] if a else []):
            gate_stats = {}
            rows = _run(lb._enrich_single_domain(
                None, None, base_row, "x.com",
                cascade_config=json.dumps(PS_CASCADE),
                max_decision_makers=5,
                gate_stats=gate_stats,
            ))
        names = [r.get("dm_full_name") for r in rows if r.get("dm_full_name")]
        assert names == ["C PS"]  # CTO + VP Finance dropped
        assert gate_stats.get("dropped") == 2

    def test_list_builder_dm_fallback_ungated_without_titles(self):
        from enrichment import list_builder
        lb = list_builder

        async def fake_company_by_domain(client, domain):
            return {"linkedin_url": "https://linkedin.com/company/x", "name": "X"}

        async def fake_company_contacts(client, domain, limit=5):
            return []

        async def fake_waterfall(client, url, cascade, max_results):
            return {"results": []}

        async def fake_dm(blitz_http, domain, limit=25):
            return self._dm_contacts()

        async def fake_resolve(*args, **kwargs):
            return ("", "", "", "unknown", "", "")

        with patch.object(lb.contacts_client, "company_by_domain",
                          side_effect=fake_company_by_domain), \
             patch.object(lb.contacts_client, "company_contacts_enriched",
                          side_effect=fake_company_contacts), \
             patch.object(lb.blitz_client, "waterfall_icp_search",
                          side_effect=fake_waterfall), \
             patch.object(lb.getleads_client, "lookup_decision_makers",
                          side_effect=fake_dm), \
             patch.object(lb, "_resolve_person_email", side_effect=fake_resolve), \
             patch.object(lb.blitz_client, "domain_to_linkedin",
                          new=AsyncMock(return_value={})), \
             patch.object(lb, "_apply_company_fallback_to_output_rows",
                          side_effect=lambda *a, **k: a[0] if a else []):
            rows = _run(lb._enrich_single_domain(
                None, None, {"domain": ""}, "x.com",
                cascade_config=None, max_decision_makers=5,
            ))
        names = [r.get("dm_full_name") for r in rows if r.get("dm_full_name")]
        assert len(names) == 3  # no titles → decision-makers layer untouched

    def test_pipeline_dm_fallback_gated(self):
        from enrichment import pipeline

        async def fake_company_by_domain(client, domain):
            return {"linkedin_url": "https://linkedin.com/company/x", "name": "X"}

        async def fake_waterfall(client, url, cascade, max_results):
            return {"results": []}

        async def fake_dm(blitz_http, domain, limit=25):
            return self._dm_contacts()

        async def fake_resolve(*args, **kwargs):
            return ("", "", "")  # pipeline 3-tuple

        with patch.object(pipeline.contacts_client, "company_by_domain",
                          side_effect=fake_company_by_domain), \
             patch.object(pipeline.blitz_client, "waterfall_icp_search",
                          side_effect=fake_waterfall), \
             patch.object(pipeline.getleads_client, "lookup_decision_makers",
                          side_effect=fake_dm), \
             patch.object(pipeline, "_resolve_email_for_person",
                          side_effect=fake_resolve):
            rows = _run(pipeline._enrich_domain(
                None, None, {"domain": "x.com"}, "x.com", "",
                PS_CASCADE, 5,
                asyncio.Semaphore(1), asyncio.Semaphore(1),
            ))
        names = [r.get("dm_full_name") for r in rows if r.get("dm_full_name")]
        assert names == ["C PS"]


class TestMatcherPrecision:
    """2026-10-06 matcher: role segment, contiguous function phrase,
    word boundaries."""

    INC = PS_CASCADE[0]["include_title"]
    EXC = PS_CASCADE[0]["exclude_title"]

    def _match(self, title="", headline=""):
        from enrichment import title_filter
        return title_filter.person_matches_titles(title, headline, self.INC, self.EXC)

    def test_company_name_in_headline_cannot_satisfy_function(self):
        assert not self._match(headline="VP of Sales | @Admiral Consulting Group")

    def test_company_segment_after_pipe_ignored(self):
        assert not self._match(headline="Vice President - Sales | @Informed.IQ")
        assert not self._match(headline="Co-Founder | @Informed.IQ")

    def test_company_segment_after_at_ignored(self):
        assert not self._match(headline="VP of Sales at Acme Professional Services Group")
        assert self._match(headline="Head of Professional Services at Acme Corp")

    def test_word_boundary_kills_headquarters_leak(self):
        assert not self._match(headline="VP of Ops at Acme Data Headquarters")

    def test_genuine_function_titles_still_match(self):
        assert self._match(headline="Vice President of Customer Success | @Tesorio")
        assert self._match(title="Director of Professional Services")
        assert self._match(headline="Senior Director, Professional Services")

    def test_seniority_word_required(self):
        # function phrase present but wrong seniority → no match
        assert not self._match(headline="Manager of Professional Services")
        assert self._match(headline="Head of Professional Services")

    def test_plural_tolerance(self):
        assert self._match(title="VP, Professional Services")
        assert self._match(title="Vice President, Professional Services")
