"""Email-quality scoreboard — recorder fix + structured statuses + read API.

ROOT CAUSE (found 2026-09-13, live-probed): httpx fires async response hooks
BEFORE the body stream is read on real network responses, so
``response.json()`` inside the hook raised ``httpx.ResponseNotRead`` and the
swallowing ``except: return`` silently dropped EVERY real response —
provider_email_ledger has recorded nothing but MockTransport test artifacts
(john.doe@example.com ×1700) since 2026-07-11. Same bug also silently killed
the fair_usage meter.

These tests pin:
- the aread() fix: a STREAMED (unread-body) response still records
- structured email_status capture: getleads from-person/from-linkedin
  (results[].data.email_status), getleads decision-makers
  (contacts[].Email + "Email Verification Status"), smartprospect
  (data[].email_id + verification_status) — others fall back to UNKNOWN
- per-(day, provider, endpoint, status) counters + metadata in the ledger
- GET /api/enrichment/stats/email-quality aggregation math
"""
from __future__ import annotations

import asyncio
import json
import sqlite3
from typing import Any

import httpx
import pytest

from enrichment import call_tracker
from enrichment.tests.test_call_tracker import installed, temp_db  # noqa: F401


def _unread_stream_response(host: str, path: str, body: dict) -> httpx.Response:
    """Build a response whose body is NOT yet read (the prod condition).

    Mirrors what the hook receives on a live network response:
    is_stream_consumed=False until aread() is awaited.
    """
    import io

    raw = json.dumps(body).encode("utf-8")

    class _Stream(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield raw

    request = httpx.Request("POST", f"https://{host}{path}")
    return httpx.Response(200, headers={"content-length": str(len(raw))},
                          stream=_Stream(), request=request)


def _read(table: str, cols: str) -> list[tuple]:
    with sqlite3.connect(str(call_tracker.DB_PATH)) as conn:
        return conn.execute(f"SELECT {cols} FROM {table}").fetchall()


# ---------------------------------------------------------------------------
# The bug: unread-body responses
# ---------------------------------------------------------------------------

class TestStreamBodyFix:
    def test_unread_stream_response_records_ledger_and_counters(self, installed) -> None:
        resp = _unread_stream_response(
            "api.blitz-api.ai", "/v2/enrichment/email",
            {"found": True, "email": "ceo@acme.com",
             "all_emails": [{"email": "ceo@acme.com"}]},
        )
        assert resp.is_stream_consumed is False
        asyncio.run(call_tracker._on_response(resp))
        ledger = _read("provider_email_ledger", "provider, email")
        assert ("blitz", "ceo@acme.com") in ledger, "unread-stream response must still record"
        counters = _read("provider_email_quality_daily", "provider, endpoint, email_status, emails")
        assert ("blitz", "/v2/enrichment/email", "UNKNOWN", 1) in counters


# ---------------------------------------------------------------------------
# Structured statuses
# ---------------------------------------------------------------------------

class TestStructuredStatuses:
    def _fire(self, host, path, body):
        resp = _unread_stream_response(host, path, body)
        asyncio.run(call_tracker._on_response(resp))

    def test_getleads_from_person_statuses(self, installed) -> None:
        self._fire("app.getleads.io", "/api/v1/enrich/from-person", {
            "ok": True,
            "results": [
                {"success": True, "data": {"email_address": "a@acme.com", "email_status": "VALID"}},
                {"success": True, "data": {"email_address": "b@acme.com", "email_status": "CATCH_ALL"}},
                {"success": True, "data": {"email_address": "c@acme.com", "email_status": "INVALID"}},
                {"success": True, "data": {"email_address": None}},
            ],
        })
        led = dict(_read("provider_email_ledger", "email, metadata"))
        assert "a@acme.com" in led and "VALID" in led["a@acme.com"]
        assert "b@acme.com" in led and "CATCH_ALL" in led["b@acme.com"]
        counters = {(p, s): n for p, s, n in
                    _read("provider_email_quality_daily", "provider, email_status, emails")}
        assert counters[("getleads", "VALID")] == 1
        assert counters[("getleads", "CATCH_ALL")] == 1
        assert counters[("getleads", "INVALID")] == 1

    def test_getleads_decision_makers_shape(self, installed) -> None:
        self._fire("app.getleads.io", "/api/v1/contacts/lookup/decision-makers", {
            "ok": True,
            "contacts": [
                {"First Name": "A", "Last Name": "B", "Email": "dm1@linear.app",
                 "Email Verification Status": "VALID"},
                {"First Name": "C", "Last Name": "D", "Email": "dm2@linear.app",
                 "Email Verification Status": "VALID"},
                {"First Name": "E", "Last Name": "F", "Email": "", },
            ],
        })
        led = [r[0] for r in _read("provider_email_ledger", "email")]
        assert "dm1@linear.app" in led and "dm2@linear.app" in led
        counters = {(p, s): n for p, s, n in
                    _read("provider_email_quality_daily", "provider, email_status, emails")}
        assert counters[("getleads", "VALID")] == 2

    def test_smartprospect_email_id_and_verification(self, installed) -> None:
        self._fire("prospect-api.smartlead.ai",
                   "/api/v1/search-email-leads/search-contacts/find-emails",
                   {"success": True, "data": [
                       {"firstName": "J", "lastName": "D", "companyDomain": "x.com",
                        "email_id": "s1@x.com", "status": "Found", "verification_status": "Valid"},
                       {"firstName": "K", "lastName": "L", "companyDomain": "y.com",
                        "email_id": "s2@y.com", "status": "Found"},
                   ]})
        counters = {(p, s): n for p, s, n in
                    _read("provider_email_quality_daily", "provider, email_status, emails")}
        assert counters[("smartprospect", "VALID")] == 1
        assert counters[("smartprospect", "UNKNOWN")] == 1

    def test_blitz_regex_fallback_unknown(self, installed) -> None:
        self._fire("api.blitz-api.ai", "/v2/enrichment/email",
                   {"found": True, "email": "z@acme.com"})
        counters = {(p, s): n for p, s, n in
                    _read("provider_email_quality_daily", "provider, email_status, emails")}
        assert counters[("blitz", "UNKNOWN")] == 1

    def test_provider_own_domain_still_filtered(self, installed) -> None:
        self._fire("app.getleads.io", "/api/v1/enrich/from-person",
                   {"ok": True, "results": [
                       {"success": True, "data": {"email_address": "support@getleads.io",
                                                  "email_status": "VALID"}}]})
        assert _read("provider_email_ledger", "email") == []


# ---------------------------------------------------------------------------
# Read API
# ---------------------------------------------------------------------------

def test_email_quality_endpoint_aggregates(tmp_path, monkeypatch) -> None:
    from shared import db as shared_db

    monkeypatch.setattr(shared_db, "DB_PATH", tmp_path / "eq_jobs.db")
    monkeypatch.setattr(shared_db._local, "conn", None, raising=False)
    shared_db.init_db()
    conn = shared_db.get_db()
    try:
        today = "2026-09-13"
        rows = [
            (today, "getleads", "/from-person", "VALID", 10, 10),
            (today, "getleads", "/from-person", "CATCH_ALL", 5, 5),
            (today, "getleads", "/from-person", "INVALID", 5, 5),
            (today, "blitz", "/v2/enrichment/email", "UNKNOWN", 80, 60),
        ]
        for r in rows:
            conn.execute(
                "INSERT INTO provider_email_quality_daily"
                " (day, provider, endpoint, email_status, responses, emails, updated_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)", (*r, "2026-09-13T00:00:00"))
        conn.commit()
    finally:
        conn.close()
        # Reset the thread-local cached connection — leaving it cached-closed
        # poisons later tests that reuse shared_db.get_db() on this thread.
        shared_db._local.conn = None

    from fastapi.testclient import TestClient
    from main import app
    from shared import auth as _auth

    app.dependency_overrides[_auth.get_current_user_with_api_key] = lambda: {
        "user_id": "u1", "email": "t@t", "is_admin": False
    }
    try:
        with TestClient(app) as tc:
            resp = tc.get("/api/enrichment/stats/email-quality?days=30")
            assert resp.status_code == 200
            by_provider = resp.json()["providers"]
            gl = by_provider["getleads"]
            assert gl["emails"] == 20
            assert gl["status_breakdown"]["VALID"]["emails"] == 10
            assert gl["status_breakdown"]["VALID"]["pct"] == 50.0
            assert gl["status_breakdown"]["CATCH_ALL"]["pct"] == 25.0
            assert by_provider["blitz"]["status_breakdown"]["UNKNOWN"]["emails"] == 60
            assert by_provider["blitz"]["status_breakdown"]["UNKNOWN"]["responses"] == 80
    finally:
        app.dependency_overrides.clear()
