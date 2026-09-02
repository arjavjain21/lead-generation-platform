"""Tests for website_only serving mode (Flow 1) — docs/WEBSITE_SCRAPE_INTEGRATION_PLAN.md §Phase 2.

Pins:
1. `_merge_by_company_contacts(source_include=...)` — include-side filter
   (source=website_scrape) without disturbing the legacy exclude-side
   `source` param semantics (today's UI behavior).
2. `run_domain_enrichment(website_only=True)`:
   - by-company lookup ONLY, include-filtered to website_scrape
   - ZERO provider calls (blitz/getleads/smartprospect/wizleads/better_enrich)
   - ZERO company-fallback calls (BetterEnrich company email)
   - ZERO mailtester validations (emails preserved as stored)
   - title filter still respected (user-entered titles)
   - normal mode (website_only=False) unchanged: no behavior difference
3. Status payload contract: /api/enrichment/website-scrape/status reads the
   sync state table (as-of watermark, last run) — plain dict shape.

NOTE: pytest-asyncio is NOT installed in this repo (root cause of the
pre-existing wizleads failures) — async work goes through asyncio.run(),
the established convention in enrichment/tests/.
"""

from __future__ import annotations

import asyncio
import sqlite3
import sys
from pathlib import Path
from unittest import mock

_BACKEND = Path(__file__).resolve().parents[2]
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

import httpx  # noqa: E402
import pytest  # noqa: E402

from enrichment import list_builder  # noqa: E402


def run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# _merge_by_company_contacts include-side filter
# ---------------------------------------------------------------------------


def _base_row():
    return {"domain": "acme.com", "dm_email": "", "dm_full_name": ""}


class TestMergeByCompanySourceInclude:
    def test_include_filter_passes_source_param(self, monkeypatch):
        captured: dict = {}

        async def fake_by_domain(client, domain, limit=100, source=None, exclude_source=None):
            captured["source"] = source
            captured["exclude_source"] = exclude_source
            return [
                {
                    "person_id": "p1",
                    "full_name": "Web Contact",
                    "email": "orders@acme.com",
                    "title": "Owner",
                }
            ]

        monkeypatch.setattr(list_builder.contacts_client, "company_persons_by_domain", fake_by_domain)
        monkeypatch.setenv("ENABLE_COMPANY_LOOKUP", "true")

        rows = run(
            list_builder._merge_by_company_contacts(
                httpx.AsyncClient(), [], "acme.com", _base_row(),
                force_provider=None, source_include="website_scrape",
            )
        )
        assert captured["source"] == "website_scrape"
        assert captured["exclude_source"] is None
        assert len(rows) == 1
        assert rows[0]["dm_email"] == "orders@acme.com"
        assert rows[0]["dm_email_source"] == "contacts_db_by_company"

    def test_legacy_source_still_excludes(self, monkeypatch):
        """Today's UI semantics (source → exclude_source) must not change."""
        captured: dict = {}

        async def fake_by_domain(client, domain, limit=100, source=None, exclude_source=None):
            captured["source"] = source
            captured["exclude_source"] = exclude_source
            return []

        monkeypatch.setattr(list_builder.contacts_client, "company_persons_by_domain", fake_by_domain)
        monkeypatch.setenv("ENABLE_COMPANY_LOOKUP", "true")

        run(
            list_builder._merge_by_company_contacts(
                httpx.AsyncClient(), [], "acme.com", _base_row(),
                force_provider=None, source="outscraper",
            )
        )
        assert captured["exclude_source"] == "outscraper"
        assert captured["source"] is None

    def test_include_and_exclude_mutually_exclusive(self, monkeypatch):
        async def fake_by_domain(client, domain, limit=100, source=None, exclude_source=None):
            raise AssertionError("should not be called when both filters set")

        monkeypatch.setattr(list_builder.contacts_client, "company_persons_by_domain", fake_by_domain)
        monkeypatch.setenv("ENABLE_COMPANY_LOOKUP", "true")

        coro = list_builder._merge_by_company_contacts(
            httpx.AsyncClient(), [], "acme.com", _base_row(),
            force_provider=None, source="outscraper", source_include="website_scrape",
        )
        with pytest.raises(ValueError):
            run(coro)


# ---------------------------------------------------------------------------
# website_only flow — zero paid calls
# ---------------------------------------------------------------------------


def _flow_rows(n=2):
    return [{"domain": f"site{i}.com"} for i in range(n)]


class TestWebsiteOnlyFlow:
    def test_website_only_makes_zero_paid_calls(self, monkeypatch):
        """The core promise: no provider, no fallback, no mailtester."""
        paid_calls: list[str] = []

        async def fake_by_domain(client, domain, limit=100, source=None, exclude_source=None):
            assert source == "website_scrape", f"website_only must include-filter, got {source}"
            return [
                {
                    "person_id": "p1",
                    "full_name": f"Owner of {domain}",
                    "email": f"owner@{domain}",
                    "title": "Owner",
                }
            ]

        monkeypatch.setattr(list_builder.contacts_client, "company_persons_by_domain", fake_by_domain)

        async def fail_call(client, *a, **k):
            paid_calls.append("contacts_lookup")
            raise AssertionError("website_only must not use the enriched/company lookups")

        monkeypatch.setattr(list_builder.contacts_client, "company_contacts_enriched", fail_call)
        monkeypatch.setattr(list_builder.contacts_client, "company_by_domain", fail_call)

        for client_name, attr in (
            ("blitz_client", "domain_to_linkedin"),
            ("blitz_client", "waterfall_icp_search"),
            ("blitz_client", "person_enrich"),
            ("blitz_client", "find_work_email"),
            ("better_enrich_client", "find_company_email"),
            ("better_enrich_client", "find_work_email_v3"),
            ("wizleads_client", "find_email"),
            ("smartprospect_client", "find_email"),
        ):
            async def fail_paid(*a, _n=attr, **k):
                paid_calls.append(_n)
                raise AssertionError(f"website_only must not call {_n}")

            monkeypatch.setattr(getattr(list_builder, client_name), attr, fail_paid)

        async def fail_mailtester(*a, **k):
            paid_calls.append("mailtester")
            raise AssertionError("website_only must not run mailtester")

        monkeypatch.setattr(list_builder.mailtester_client, "verify_email", fail_mailtester)

        async def fail_fallback(*a, **k):
            paid_calls.append("company_fallback")
            raise AssertionError("website_only must not run company fallback")

        monkeypatch.setattr(list_builder.company_fallback, "run_company_fallbacks", fail_fallback)
        monkeypatch.setenv("ENABLE_COMPANY_LOOKUP", "true")

        output = run(
            list_builder.run_domain_enrichment(
                _flow_rows(2),
                domain_col="domain",
                website_only=True,
            )
        )
        assert paid_calls == []
        assert len(output) >= 2
        sources = {r.get("dm_email_source") for r in output}
        assert sources == {"contacts_db_by_company"}

    def test_website_only_no_contacts_yields_empty(self, monkeypatch):
        async def fake_by_domain(client, domain, limit=100, source=None, exclude_source=None):
            return []

        monkeypatch.setattr(list_builder.contacts_client, "company_persons_by_domain", fake_by_domain)
        monkeypatch.setenv("ENABLE_COMPANY_LOOKUP", "true")

        async def fail_call(*a, **k):
            raise AssertionError("website_only must not fall back to providers")

        monkeypatch.setattr(list_builder.contacts_client, "company_contacts_enriched", fail_call)
        monkeypatch.setattr(list_builder.blitz_client, "waterfall_icp_search", fail_call)

        output = run(
            list_builder.run_domain_enrichment(
                _flow_rows(1), domain_col="domain", website_only=True,
            )
        )
        assert output == []

    def test_website_only_respects_title_filter(self, monkeypatch):
        async def fake_by_domain(client, domain, limit=100, source=None, exclude_source=None):
            return [
                {"person_id": "p1", "full_name": "A", "email": "a@site0.com", "title": "Office Manager"},
                {"person_id": "p2", "full_name": "B", "email": "b@site0.com", "title": "Owner"},
            ]

        monkeypatch.setattr(list_builder.contacts_client, "company_persons_by_domain", fake_by_domain)
        monkeypatch.setenv("ENABLE_COMPANY_LOOKUP", "true")

        # cascade_config is a JSON list of tier objects (routes._titles_to_cascade shape)
        import json as _json

        cascade_json = _json.dumps([
            {
                "include_title": ["Owner", "Founder"],
                "exclude_title": ["assistant", "intern"],
                "location": ["WORLD"],
            }
        ])
        output = run(
            list_builder.run_domain_enrichment(
                [{"domain": "site0.com"}],
                domain_col="domain",
                website_only=True,
                cascade_config=cascade_json,
            )
        )
        assert len(output) == 1
        assert output[0]["dm_full_name"] == "B"

    def test_default_mode_unchanged_without_website_only(self, monkeypatch):
        """website_only=False must flow through today's machinery untouched."""
        async def fake_by_domain(client, domain, limit=100, source=None, exclude_source=None):
            return []

        monkeypatch.setattr(list_builder.contacts_client, "company_persons_by_domain", fake_by_domain)
        monkeypatch.setenv("ENABLE_COMPANY_LOOKUP", "true")

        async def fake_enriched(client, domain, limit=100):
            return []  # quality gate fails → normal flow proceeds to providers

        monkeypatch.setattr(list_builder.contacts_client, "company_contacts_enriched", fake_enriched)

        async def noop(*a, **k):
            return None

        monkeypatch.setattr(list_builder.contacts_client, "company_by_domain", noop)
        monkeypatch.setattr(list_builder.blitz_client, "domain_to_linkedin", noop)
        monkeypatch.setattr(list_builder.blitz_client, "waterfall_icp_search", noop)
        monkeypatch.setattr(
            list_builder.company_fallback, "run_company_fallbacks", mock.AsyncMock(return_value={})
        )

        output = run(
            list_builder.run_domain_enrichment([{"domain": "site0.com"}], domain_col="domain")
        )
        assert isinstance(output, list)


# ---------------------------------------------------------------------------
# Status payload contract
# ---------------------------------------------------------------------------


class TestWebsiteScrapeStatus:
    def test_status_payload_shape(self, tmp_path):
        from enrichment import website_scrape_sync as wss
        from enrichment.routes import _website_scrape_status_payload

        db = tmp_path / "jobs.db"
        conn = sqlite3.connect(db)
        wss.init_state_table(conn)
        store = wss.SyncStateStore(conn)
        store.set_watermark("2026-08-27 03:37:00+00", 999, rows_pulled=500, rows_pushed=422)
        store.record_run(status="success", rows_pulled=500, rows_pushed=422, skipped_junk=78, errors=0)

        payload = _website_scrape_status_payload(store)
        assert payload["enabled"] is False  # env unset in tests
        assert payload["watermark"]["completed_at"] == "2026-08-27 03:37:00+00"
        assert payload["watermark"]["row_id"] == 999
        assert payload["last_run"]["status"] == "success"
        assert payload["last_run"]["rows_pushed"] == 422
        assert isinstance(payload["synced_within_hours"], bool)

    def test_status_payload_never_run(self, tmp_path):
        from enrichment import website_scrape_sync as wss
        from enrichment.routes import _website_scrape_status_payload

        db = tmp_path / "jobs.db"
        conn = sqlite3.connect(db)
        wss.init_state_table(conn)

        payload = _website_scrape_status_payload(wss.SyncStateStore(conn))
        assert payload["watermark"] is None
        assert payload["last_run"]["status"] is None
        assert payload["synced_within_hours"] is False
