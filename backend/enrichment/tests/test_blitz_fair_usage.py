"""Blitz fair_usage meter — FUP observability.

Blitz responses carry a top-level ``fair_usage`` object (verified live
2026-09-13: ``records_used`` per response, ``records_remaining`` gauge of the
15M/month cap, ``next_reset_at``). The platform previously dropped it — the
FUP burn was invisible. These tests pin the capture contract in
``call_tracker._on_response``:

- a 2xx blitz response with ``fair_usage`` upserts the snapshot row
- repeated responses UPDATE the single row (never duplicate)
- a daily rollup row is written once per UTC day
- non-blitz providers and fair_usage-less blitz bodies write nothing
- failures never propagate into the HTTP call path
"""
from __future__ import annotations

import asyncio
import sqlite3
from unittest import mock

import httpx

from enrichment import call_tracker
from enrichment.tests.test_call_tracker import installed, temp_db  # noqa: F401  (fixtures)


def _make_json_response(host: str, path: str, body: dict, status: int = 200) -> httpx.Response:
    request = httpx.Request("POST", f"https://{host}{path}")
    return httpx.Response(status_code=status, json=body, request=request)


_FAIR_USAGE = {
    "records_used": 3,
    "records_remaining": 10930270,
    "next_reset_at": "2026-09-26T03:10:25.000Z",
    "rate_limit": {"requests_per_second": 50, "remaining_this_second": 49},
    "request_id": "req-abc",
}


def _read(table: str, cols: str) -> list[tuple]:
    with sqlite3.connect(str(call_tracker.DB_PATH)) as conn:
        return conn.execute(f"SELECT {cols} FROM {table}").fetchall()


def test_blitz_fair_usage_upserts_snapshot(installed) -> None:
    asyncio.run(call_tracker._on_response(_make_json_response(
        "api.blitz-api.ai", "/v2/enrichment/email",
        {"found": True, "fair_usage": _FAIR_USAGE},
    )))
    rows = _read("blitz_fair_use", "endpoint, records_used, records_remaining, next_reset_at")
    assert rows == [("/v2/enrichment/email", 3, 10930270, "2026-09-26T03:10:25.000Z")]


def test_blitz_fair_usage_snapshot_updates_not_duplicates(installed) -> None:
    for remaining in (10930270, 10930200):
        body = {"found": True, "fair_usage": {**_FAIR_USAGE, "records_remaining": remaining}}
        asyncio.run(call_tracker._on_response(_make_json_response(
            "api.blitz-api.ai", "/v2/enrichment/email", body,
        )))
    rows = _read("blitz_fair_use", "records_remaining")
    assert rows == [(10930200,)]


def test_blitz_fair_usage_daily_rollup_one_per_day(installed) -> None:
    call_tracker._last_fair_use_daily_day = ""
    for _ in range(3):
        asyncio.run(call_tracker._on_response(_make_json_response(
            "api.blitz-api.ai", "/v2/search/waterfall-icp-keyword",
            {"results": [], "fair_usage": _FAIR_USAGE},
        )))
    rows = _read("blitz_fair_use_daily", "records_remaining")
    assert len(rows) == 1


def test_no_fair_usage_no_write(installed) -> None:
    asyncio.run(call_tracker._on_response(_make_json_response(
        "api.blitz-api.ai", "/v2/enrichment/email", {"found": False},
    )))
    asyncio.run(call_tracker._on_response(_make_json_response(
        "app.getleads.io", "/api/v1/enrich/from-person",
        {"ok": True, "fair_usage": _FAIR_USAGE},
    )))
    assert _read("blitz_fair_use", "id") == []


def test_fair_usage_db_error_never_propagates(installed, monkeypatch) -> None:
    def boom(*a, **k):
        raise sqlite3.OperationalError("locked")

    monkeypatch.setattr(call_tracker, "_upsert_blitz_fair_usage", boom)
    # Must not raise — the hook swallows everything.
    asyncio.run(call_tracker._on_response(_make_json_response(
        "api.blitz-api.ai", "/v2/enrichment/email",
        {"found": True, "fair_usage": _FAIR_USAGE},
    )))


# ---------------------------------------------------------------------------
# GET /api/enrichment/blitz/fair-use (thin read endpoint)
# ---------------------------------------------------------------------------

def test_fair_use_endpoint_returns_snapshot(tmp_path, monkeypatch) -> None:
    from shared import db as shared_db

    tmp_db = tmp_path / "ep_jobs.db"
    monkeypatch.setattr(shared_db, "DB_PATH", tmp_db)
    # Reset the thread-local cached connection so get_db() opens the temp file.
    monkeypatch.setattr(shared_db._local, "conn", None, raising=False)

    shared_db.init_db()
    conn = shared_db.get_db()
    try:
        conn.execute(
            """
            INSERT INTO blitz_fair_use
                (id, endpoint, records_used, records_remaining, next_reset_at, updated_at)
            VALUES (1, '/v2/enrichment/email', 3, 10930200, '2026-09-26T03:10:25.000Z', '2026-09-13T00:00:00')
            """
        )
        conn.commit()
    finally:
        conn.close()

    from fastapi.testclient import TestClient
    from main import app
    from shared import auth as _auth

    app.dependency_overrides[_auth.get_current_user_with_api_key] = lambda: {
        "user_id": "u1", "email": "t@t", "is_admin": False
    }
    try:
        with TestClient(app) as tc:
            resp = tc.get("/api/enrichment/blitz/fair-use")
            assert resp.status_code == 200
            body = resp.json()
            assert body["enabled"] is True
            assert body["snapshot"]["records_remaining"] == 10930200
            assert body["snapshot"]["next_reset_at"].startswith("2026-09-26")
    finally:
        app.dependency_overrides.clear()
