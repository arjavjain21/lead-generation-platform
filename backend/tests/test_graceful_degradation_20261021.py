"""Graceful-degradation tests (2026-10-21 RCA: silent 0-email jobs).

Akshat's `qe-msp-netnew-final.csv` ran 4× identically: 0 emails at 01:42 /
01:55 / 12:37 and 1,888 at 12:26. Root cause stack:
  1. contacts_db circuit breaker OPEN (500s under load) — every tier skipped,
     indistinguishable from "no data" (empty provider_errors column)
  2. Blitz 402 (FUP exhausted) — d2l/prepass/waterfall dead, breaker open
  3. QE no-LI rescue disabled under a title cascade (``not cascade_config``)
  4. GetLeads DM fallback unreachable for no-LinkedIn rows (early return)

These tests pin the three fixes:
  * Fix #1 — breaker-open/errors stamped into provider_errors +
    no_email_reason=contacts_db_unavailable on terminal rows
  * Fix #2 — GetLeads DM fallback runs for no-LinkedIn rows (list_builder
    domain path + company-URL path)
  * Fix #3 — QE no-LI rescue + Step-2 QE tier run under a title cascade
"""
import asyncio
import json
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from shared.circuit_breaker import CircuitBreakerError


def _run(coro):
    # asyncio.run() — see test_list_builder_normalize_branch for the
    # get_event_loop() order-pollution note.
    return asyncio.run(coro)


_GL_DM_CONTACT = {
    "first_name": "Jane",
    "last_name": "Doe",
    "person_full_name": "Jane Doe",
    "job_title": "CEO",
    "linkedin_url": "https://www.linkedin.com/in/jane-doe",
    "email": "jane@acme.com",
}


class TestStampUnavailability:
    """Fix #1 unit level: the terminal-row stamp helper."""

    def test_breaker_open_sets_reason_and_errors(self):
        from enrichment import list_builder

        row = {"provider_errors": "", "no_email_reason": ""}
        errors = [
            {"provider": "contacts_db", "method": "company_by_domain",
             "error_type": "breaker_open", "message": "circuit breaker OPEN"},
        ]
        list_builder._stamp_unavailability(row, errors)
        assert row["no_email_reason"] == "contacts_db_unavailable"
        parsed = json.loads(row["provider_errors"])
        assert parsed[0]["provider"] == "contacts_db"
        assert parsed[0]["error_type"] == "breaker_open"

    def test_existing_reason_preserved(self):
        from enrichment import list_builder

        row = {"provider_errors": "", "no_email_reason": "facebook_page_missing"}
        errors = [
            {"provider": "contacts_db", "method": "company_by_domain",
             "error_type": "breaker_open", "message": "circuit breaker OPEN"},
        ]
        list_builder._stamp_unavailability(row, errors)
        assert row["no_email_reason"] == "facebook_page_missing"

    def test_no_contacts_reason_without_breaker(self):
        from enrichment import list_builder

        row = {"provider_errors": "", "no_email_reason": ""}
        errors = [
            {"provider": "blitz", "method": "domain_to_linkedin",
             "error_type": "exception", "message": "boom"},
        ]
        list_builder._stamp_unavailability(row, errors)
        assert row["no_email_reason"] == ""
        assert json.loads(row["provider_errors"])[0]["provider"] == "blitz"

    def test_noop_without_errors(self):
        from enrichment import list_builder

        row = {"provider_errors": "", "no_email_reason": ""}
        list_builder._stamp_unavailability(row, [])
        assert row == {"provider_errors": "", "no_email_reason": ""}

    def test_breaker_state_helpers_exist(self):
        from enrichment import contacts_client, blitz_client

        assert contacts_client.breaker_state() in ("closed", "open", "half_open")
        assert blitz_client.breaker_state() in ("closed", "open", "half_open")


def _mocks_for_dead_world(lb, gl_contacts=None):
    """Both breakers open / Blitz 402-dead world — the RCA conditions."""
    mocks = {
        "company_by_domain": AsyncMock(return_value=None),
        "company_contacts_enriched": AsyncMock(return_value=[]),
        "breaker_state": MagicMock(return_value="open"),
        "blitz_breaker": MagicMock(return_value="open"),
        "d2l": AsyncMock(side_effect=CircuitBreakerError("blitz_api")),
        "qe": AsyncMock(return_value=([], {})),
        "gl_dm": AsyncMock(return_value=list(gl_contacts or [])),
    }
    return mocks


class TestListBuilderNoLinkedInVisibility:
    """Fix #1 wiring: the RCA scenario — contacts_db breaker open, Blitz
    402-dead — must produce a terminal row that SAYS so."""

    def test_no_linkedin_row_carries_breaker_reasons(self):
        from enrichment import list_builder

        m = _mocks_for_dead_world(list_builder)
        with patch.object(list_builder.contacts_client, "company_by_domain", m["company_by_domain"]), \
             patch.object(list_builder.contacts_client, "company_contacts_enriched", m["company_contacts_enriched"]), \
             patch.object(list_builder.contacts_client, "breaker_state", m["breaker_state"]), \
             patch.object(list_builder.blitz_client, "breaker_state", m["blitz_breaker"]), \
             patch.object(list_builder.blitz_client, "domain_to_linkedin", m["d2l"]), \
             patch.object(list_builder, "_quickenrich_domain_discovery", m["qe"]), \
             patch.object(list_builder.getleads_client, "lookup_decision_makers", m["gl_dm"]), \
             patch.object(list_builder, "_should_skip_provider", return_value=False):
            rows = _run(list_builder._enrich_single_domain(
                MagicMock(), MagicMock(), {"domain": "acme.com"}, "acme.com",
            ))

        assert len(rows) == 1
        row = rows[0]
        assert row["row_status"] == "no_linkedin"
        assert row["no_email_reason"] == "contacts_db_unavailable"
        errors = json.loads(row["provider_errors"])
        by_provider = {(e["provider"], e["error_type"]) for e in errors}
        assert ("contacts_db", "breaker_open") in by_provider
        assert ("blitz", "breaker_open") in by_provider


class TestGetleadsNoLiRescue:
    """Fix #2: GetLeads DM fallback must be reachable for no-LI rows.

    cascade_config is set (title job) so this simultaneously proves Fix #3 —
    the QE rescue is consulted even under a title cascade.
    """

    def _run_rescue(self, gl_contacts):
        from enrichment import list_builder

        m = _mocks_for_dead_world(list_builder, gl_contacts=gl_contacts)
        resolver = AsyncMock(return_value=(
            "jane@acme.com", "", "getleads_email", "yes", "", "",
            {"mx_provider": "google"},
        ))
        cascade = json.dumps([{"include_title": ["CEO", "Founder"], "exclude_title": ["intern"]}])
        with patch.object(list_builder.contacts_client, "company_by_domain", m["company_by_domain"]), \
             patch.object(list_builder.contacts_client, "company_contacts_enriched", m["company_contacts_enriched"]), \
             patch.object(list_builder.contacts_client, "breaker_state", m["breaker_state"]), \
             patch.object(list_builder.blitz_client, "breaker_state", m["blitz_breaker"]), \
             patch.object(list_builder.blitz_client, "domain_to_linkedin", m["d2l"]), \
             patch.object(list_builder, "_quickenrich_domain_discovery", m["qe"]), \
             patch.object(list_builder.getleads_client, "lookup_decision_makers", m["gl_dm"]), \
             patch.object(list_builder, "_resolve_person_email", resolver), \
             patch.object(list_builder, "_should_skip_provider", return_value=False):
            rows = _run(list_builder._enrich_single_domain(
                MagicMock(), MagicMock(), {"domain": "acme.com"}, "acme.com",
                cascade_config=cascade,
            ))
        return rows, m, resolver

    def test_gl_rescue_finds_person_under_title_cascade(self):
        rows, m, resolver = self._run_rescue([dict(_GL_DM_CONTACT)])
        # QE rescue consulted despite cascade_config (Fix #3)...
        m["qe"].assert_called_once()
        # ...GetLeads DM fallback reached for the no-LI row (Fix #2)...
        m["gl_dm"].assert_called_once()
        # ...and the rescued person flows to email resolution instead of a
        # bare no_linkedin return.
        resolver.assert_called_once()
        assert rows and rows[0]["row_status"] == "enriched"
        assert rows[0]["dm_email"] == "jane@acme.com"
        assert rows[0]["dm_full_name"] == "Jane Doe"

    def test_gl_rescue_empty_still_explains_itself(self):
        rows, m, _ = self._run_rescue([])
        m["gl_dm"].assert_called_once()
        assert len(rows) == 1
        assert rows[0]["row_status"] == "no_linkedin"
        assert rows[0]["no_email_reason"] == "contacts_db_unavailable"


class TestCompanyUrlPathRescue:
    """Fix #1/#2 on the path job e0581b86 actually took: company-URL rows
    with the Blitz breaker open (waterfall dead)."""

    def _run_company(self, gl_contacts):
        from enrichment import list_builder

        waterfall = AsyncMock(return_value=[])
        gl_dm = AsyncMock(return_value=list(gl_contacts))
        resolver = AsyncMock(return_value=(
            "jane@acme.com", "", "getleads_email", "yes", "", "", {},
        ))
        with patch.object(list_builder, "_enrich_by_company_waterfall", waterfall), \
             patch.object(list_builder.blitz_client, "breaker_state", MagicMock(return_value="open")), \
             patch.object(list_builder.getleads_client, "lookup_decision_makers", gl_dm), \
             patch.object(list_builder, "_resolve_person_email", resolver), \
             patch.object(list_builder, "_should_skip_provider", return_value=False):
            rows = _run(list_builder._enrich_by_company_linkedin(
                MagicMock(), MagicMock(),
                {"domain": "acme.com"},
                "https://www.linkedin.com/company/acme",
                domain="acme.com",
                max_dms=5,
            ))
        return rows, waterfall, gl_dm

    def test_waterfall_dead_gl_rescues(self):
        rows, waterfall, gl_dm = self._run_company([dict(_GL_DM_CONTACT)])
        waterfall.assert_called_once()
        gl_dm.assert_called_once()
        assert rows[0]["row_status"] == "enriched"
        assert rows[0]["dm_email"] == "jane@acme.com"

    def test_waterfall_dead_no_gl_stamps_breaker(self):
        rows, waterfall, gl_dm = self._run_company([])
        gl_dm.assert_called_once()
        assert len(rows) == 1
        assert rows[0]["row_status"] == "no_contacts"
        errors = json.loads(rows[0]["provider_errors"])
        assert errors and errors[0]["provider"] == "blitz"
        assert errors[0]["error_type"] == "breaker_open"


class TestPipelineHelpersAndParity:
    """Pipeline side: the extracted DM helper + the de-gated QE rescue."""

    def test_dm_fallback_helper_title_gate_and_prekeyed(self):
        from enrichment import pipeline

        contacts = [
            dict(_GL_DM_CONTACT),                                  # CEO — passes
            {**_GL_DM_CONTACT, "first_name": "Bob", "job_title": "Intern"},  # dropped
        ]
        gl = AsyncMock(return_value=contacts)
        with patch.object(pipeline.getleads_client, "lookup_decision_makers", gl):
            persons, pre = _run(pipeline._getleads_dm_fallback_dm(
                MagicMock(), "acme.com",
                include_titles=["CEO"], exclude_titles=["Intern"],
                title_filter_active=True,
            ))
        assert len(persons) == 1
        assert persons[0]["person"]["full_name"] == "Jane Doe"
        # pre-resolved keyed by person index, only email-carriers
        assert 0 in pre and pre[0]["email"] == "jane@acme.com"

    def test_dm_fallback_helper_never_raises(self):
        from enrichment import pipeline

        recorded = []
        gl = AsyncMock(side_effect=RuntimeError("getleads down"))
        with patch.object(pipeline.getleads_client, "lookup_decision_makers", gl):
            persons, pre = _run(pipeline._getleads_dm_fallback_dm(
                MagicMock(), "acme.com",
                record_company_error=lambda p, mth, e, *a: recorded.append((p, mth)),
            ))
        assert persons == [] and pre == {}
        assert recorded and recorded[0][0] == "getleads"

    def test_qe_rescue_gate_no_longer_blocks_custom_cascade(self):
        """Source-level pin: neither QE rescue site keys on the custom
        cascade anymore (regression guard for the gate removal)."""
        import inspect
        from enrichment import pipeline, list_builder

        pipe_src = inspect.getsource(pipeline._enrich_domain)
        lb_src = inspect.getsource(list_builder._enrich_single_domain)
        # The QE rescue calls must not be guarded by the cascade gates.
        assert "not use_custom_cascade\n            and not _should_skip_provider(\"quickenrich\"" not in pipe_src
        assert "and not cascade_config\n            and not _should_skip_provider(\"quickenrich\"" not in lb_src
