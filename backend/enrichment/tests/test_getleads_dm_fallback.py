"""GetLeads decision-makers fallback layer — cascade wiring.

Domain-level coverage: when contacts_db AND the Blitz waterfall find no
decision makers, the domain flows now ask GetLeads'
POST /api/v1/contacts/lookup/decision-makers before giving up (pipeline
_enrich_domain Step 2.4 + list_builder _enrich_single_domain mirror).

Pinned behavior:
- fallback fires exactly once for an empty-discovery domain, and its
  email-bearing persons flow through as normal getleads rows
- persons resolved by the DM layer are NOT re-asked by the from-person
  batch (no find_emails_batch / find_email calls for them)
- force_provider / selected_providers gating respected
- Blitz finding persons suppresses the fallback entirely
"""
from __future__ import annotations

import asyncio
import os
import sys
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

os.environ.setdefault("CONTACTS_API_TOKEN", "test-token-from-suite")

from enrichment import blitz_client as bc  # noqa: E402
from enrichment import contacts_client as cc  # noqa: E402
from enrichment import getleads_client as gl  # noqa: E402
from enrichment import list_builder as lb  # noqa: E402
from enrichment import pipeline as pipeline_mod  # noqa: E402


def _dm_contact(
    first: str,
    last: str,
    email: str,
    title: str = "Chief Operating Officer",
    linkedin: str = "",
    phone: str = "",
) -> dict[str, Any]:
    """Canonical lookup_decision_makers output (client-normalized shape)."""
    return {
        "email": email,
        "first_name": first,
        "last_name": last,
        "domain": "acme.com",
        "verification_status": "Valid" if email else "unknown",
        "linkedin_url": linkedin,
        "phone": phone,
        "job_title": title,
        "linkedin_headline": "",
        "person_full_name": f"{first} {last}",
        "company_name": "Acme",
        "company_industry": "Software Development",
        "employee_count": "51 to 200",
        "revenue": "",
        "city": "NYC",
        "country": "United States",
        "linkedin_connections": "",
        "email_last_verified_at": "",
        "job_level": "C-Team",
        "job_function": "Operations",
    }


def _wire_empty_discovery(monkeypatch: pytest.MonkeyPatch, *, getleads_dm=None) -> dict[str, int]:
    """Patch contacts_db + blitz so NO decision makers are discovered; count
    getleads calls. Returns counters."""
    counters = {"dm": 0, "gl_batch": 0, "gl_single": 0}

    async def fake_dm(http, domain, limit=25):
        counters["dm"] += 1
        return getleads_dm or []

    async def fake_gl_batch(http, payload):
        counters["gl_batch"] += 1
        return []

    async def fake_gl_single(*a, **kw):
        counters["gl_single"] += 1
        return {"email": ""}

    monkeypatch.setattr(cc, "company_by_domain",
                        AsyncMock(return_value={"linkedin_url": "https://linkedin.com/company/acme"}))
    monkeypatch.setattr(cc, "company_contacts_enriched", AsyncMock(return_value=[]))
    monkeypatch.setattr(cc, "person_by_name_and_domain", AsyncMock(return_value=None))
    monkeypatch.setattr(cc, "extract_email_from_contacts_response", MagicMock(return_value=""))
    monkeypatch.setattr(bc, "domain_to_linkedin",
                        AsyncMock(return_value={"company_linkedin_url": "https://linkedin.com/company/acme"}))
    monkeypatch.setattr(bc, "waterfall_icp_search", AsyncMock(return_value={"results": []}))
    monkeypatch.setattr(bc, "person_enrich", AsyncMock(return_value={"found": False, "person": {}}))
    monkeypatch.setattr(gl, "lookup_decision_makers", fake_dm)
    monkeypatch.setattr(gl, "find_emails_batch", fake_gl_batch)
    monkeypatch.setattr(gl, "find_email", fake_gl_single)
    return counters


class TestPipelineDomainFallback:
    def _run(self, **kwargs):
        async def _go():
            return await pipeline_mod._enrich_domain(
                blitz_http=MagicMock(),
                contacts_http=MagicMock(),
                base_row={"domain": "acme.com"},
                domain="acme.com",
                full_name="",
                cascade=bc.DEFAULT_CASCADE,
                max_results=10,
                domain_semaphore=asyncio.Semaphore(1),
                email_semaphore=asyncio.Semaphore(1),
                validate_email=False,
                **kwargs,
            )
        return asyncio.run(_go())

    def test_dm_fallback_populates_rows_without_re_asking(self, monkeypatch):
        counters = _wire_empty_discovery(
            monkeypatch,
            getleads_dm=[_dm_contact("Alice", "Alpha", "alice@acme.com"),
                          _dm_contact("Bob", "Beta", "bob@acme.com")],
        )
        rows = self._run()
        assert counters["dm"] == 1, "decision-makers lookup must fire exactly once"
        assert counters["gl_batch"] == 0, "DM-resolved persons must not re-enter the from-person batch"
        assert counters["gl_single"] == 0, "DM-resolved persons must not re-fire singles"
        emailed = [r for r in rows if r.get("dm_email")]
        assert len(emailed) == 2, f"expected 2 DM rows, got {len(emailed)} of {len(rows)}"
        assert {r["dm_email"] for r in emailed} == {"alice@acme.com", "bob@acme.com"}
        assert all(r.get("dm_email_source") == pipeline_mod.SOURCE_GETLEADS for r in emailed)

    def test_dm_fallback_skipped_when_force_provider(self, monkeypatch):
        counters = _wire_empty_discovery(monkeypatch, getleads_dm=[_dm_contact("A", "B", "a@acme.com")])
        rows = self._run(force_provider="blitz")
        assert counters["dm"] == 0
        assert rows and rows[0].get("row_status") in ("no_contacts", "not_found", "error") or not rows[0].get("dm_email")

    def test_dm_fallback_suppressed_when_blitz_finds_persons(self, monkeypatch):
        counters = _wire_empty_discovery(monkeypatch, getleads_dm=[_dm_contact("A", "B", "a@acme.com")])
        # Blitz waterfall returns a person -> DM layer must not run.
        monkeypatch.setattr(bc, "waterfall_icp_search", AsyncMock(return_value={
            "results": [{"person": {"full_name": "Carol Gamma", "first_name": "Carol",
                                    "last_name": "Gamma", "linkedin_url": "", "title": "CEO"},
                          "icp": 1}],
        }))
        self._run()
        assert counters["dm"] == 0

    def test_dm_empty_returns_no_contacts(self, monkeypatch):
        counters = _wire_empty_discovery(monkeypatch, getleads_dm=[])
        rows = self._run()
        assert counters["dm"] == 1
        assert all(not r.get("dm_email") for r in rows)


class TestListBuilderDomainFallback:
    def _run(self, **kwargs):
        async def _go():
            return await lb._enrich_single_domain(
                MagicMock(), MagicMock(),
                {"domain": "acme.com"},
                "acme.com",
                max_decision_makers=5,
                domain_semaphore=asyncio.Semaphore(1),
                email_semaphore=asyncio.Semaphore(1),
                validate_email=False,
                **kwargs,
            )
        return asyncio.run(_go())

    def test_dm_fallback_populates_rows(self, monkeypatch):
        counters = _wire_empty_discovery(
            monkeypatch,
            getleads_dm=[_dm_contact("Alice", "Alpha", "alice@acme.com"),
                          _dm_contact("Bob", "Beta", "bob@acme.com")],
        )
        rows = self._run()
        assert counters["dm"] == 1
        assert counters["gl_batch"] == 0, "DM-resolved persons must not re-enter the batch"
        assert counters["gl_single"] == 0
        emailed = [r for r in rows if r.get("dm_email")]
        assert len(emailed) == 2
        assert {r["dm_email"] for r in emailed} == {"alice@acme.com", "bob@acme.com"}
        assert all(r.get("dm_email_source") == lb.SOURCE_GETLEADS for r in emailed)

    def test_dm_fallback_skipped_when_getleads_not_selected(self, monkeypatch):
        counters = _wire_empty_discovery(monkeypatch, getleads_dm=[_dm_contact("A", "B", "a@acme.com")])
        rows = self._run(selected_providers=["blitz"])
        assert counters["dm"] == 0
        assert all(not r.get("dm_email") for r in rows)
