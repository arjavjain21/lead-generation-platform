"""
Tests for /api/external/scraper/* — the API-key surface.

Covers the auth matrix (401 no-creds / X-API-Key 200 / 403 non-owner /
admin bypass / non-scraper 404), the envelope contract on success AND error,
the prefer_cache short-circuit, guardrails (task cap 422, quota 429), results
pagination + 409 not_ready, cancel paths, and two regression guards:
- old routes still return bare {"detail": ...} (the path-scoped exception
  handler must not leak outside /api/external/*);
- the ENABLE_EXTERNAL_SCRAPER_API kill-switch removes the router.

Uses the same TestClient + dependency-override + real-DB insert/cleanup
pattern as tests/test_scraper_partial_download.py.
"""
from __future__ import annotations

import csv as _csv
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import pytest
from fastapi.testclient import TestClient

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

import main  # noqa: E402
from scraper import cache as cache_module  # noqa: E402
from scraper import external_helpers as ext  # noqa: E402
from shared import auth as _auth, db  # noqa: E402

OWNER = "ext-api-owner"
OTHER = "ext-api-other"
ADMIN = "ext-api-admin"


def _make_user(user_id, is_admin=False):
    return {"user_id": user_id, "email": f"{user_id}@test.example", "is_admin": is_admin}


def _override(user_id=OWNER, is_admin=False):
    return {_auth.get_current_user_with_api_key: lambda: _make_user(user_id, is_admin)}


@pytest.fixture()
def client():
    c = TestClient(main.app)
    yield c


@pytest.fixture(autouse=True)
def _env():
    """SCRAPER_TECH_KEY present for create paths; restored after each test."""
    with mock.patch.dict(os.environ, {"SCRAPER_TECH_KEY": "test-key"}):
        yield


def _mk_user_row(conn, user_id, is_admin=0):
    conn.execute(
        "INSERT OR REPLACE INTO users (user_id, email, password_hash, is_admin, created_at) "
        "VALUES (?, ?, 'x', ?, ?)",
        (user_id, f"{user_id}@test.example", is_admin, datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()


def _mk_job(conn, job_id, user_id, status="running", total_tasks=9, done_tasks=3,
            result_count=2, output_path=None):
    iso = datetime.now(timezone.utc).isoformat()
    regions = '{"mode":"cities","country":"us","cities":["Austin"],"states":[],"zips":[],"center_ids":[]}'
    conn.execute(
        "INSERT INTO jobs (job_id, user_id, job_type, status, query, regions, total_tasks, "
        "done_tasks, result_count, output_path, created_at, updated_at) "
        "VALUES (?, ?, 'scraper', ?, 'coffee shop', ?, ?, ?, ?, ?, ?, ?)",
        (job_id, user_id, status, regions, total_tasks, done_tasks, result_count,
         output_path, iso, iso),
    )
    conn.commit()


def _cleanup(conn, *job_ids, user_ids=()):
    try:
        for jid in job_ids:
            conn.execute("DELETE FROM job_events WHERE job_id = ?", (jid,))
            conn.execute("DELETE FROM task_checkpoints WHERE job_id = ?", (jid,))
            conn.execute("DELETE FROM jobs WHERE job_id = ?", (jid,))
        for uid in user_ids:
            conn.execute("DELETE FROM users WHERE user_id = ?", (uid,))
        conn.commit()
    except Exception:
        conn.rollback()


def _mk_cache_row(conn, cache_id, result_path, total_results=5, query="coffee shop",
                  is_partial=0, pct=100.0):
    now = datetime.now(timezone.utc)
    conn.execute(
        "INSERT OR REPLACE INTO scraped_cache "
        "(cache_id, job_id, user_id, query, region_signature, regions, zoom_signature, "
        "expected_types_signature, status, result_file_path, checksum, total_results, "
        "created_at, updated_at, expires_at, last_accessed_at, access_count, "
        "is_partial, percentage_complete) "
        "VALUES (?, 'j', 'u', ?, 'sig', '{}', 'zsig', 'tsig', 'active', ?, 'chk', ?, "
        "?, ?, ?, ?, 3, ?, ?)",
        (cache_id, query, str(result_path), total_results, now.isoformat(), now.isoformat(),
         (now + timedelta(days=60)).isoformat(), now.isoformat(), is_partial, pct),
    )
    conn.commit()


def _write_csv(path, n, cols=("name", "phone")):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = _csv.writer(f)
        w.writerow(cols)
        for i in range(n):
            w.writerow([f"Biz {i}", "555"])


REGIONS_AUSTIN = {"mode": "cities", "country": "us", "states": [], "cities": ["Austin"],
                  "zips": [], "center_ids": []}


def _austin_cache_id():
    return cache_module.generate_cache_id(
        "coffee shop",
        cache_module.generate_region_signature(REGIONS_AUSTIN),
        cache_module.generate_zoom_signature([10, 11, 12]),
        cache_module.generate_expected_types_signature(None),
    )


# ---------------------------------------------------------------------------
# 1. Auth matrix
# ---------------------------------------------------------------------------

class TestAuthMatrix:
    def test_401_without_credentials(self, client):
        r = client.get("/api/external/scraper/quota")
        assert r.status_code == 401
        body = r.json()
        assert body["success"] is False
        assert body["error"]["status"] == 401

    def test_200_with_api_key_dependency(self, client):
        # Dependency override simulates a valid X-API-Key resolution.
        main.app.dependency_overrides[_auth.get_current_user_with_api_key] = \
            lambda: _make_user(OWNER)
        try:
            r = client.get("/api/external/scraper/quota")
            assert r.status_code == 200
            assert r.json()["success"] is True
        finally:
            main.app.dependency_overrides.clear()

    def test_403_non_owner_on_status(self, client, ):
        conn = db.get_db()
        jid = str(uuid.uuid4())
        _mk_user_row(conn, OWNER)
        _mk_user_row(conn, OTHER)
        _mk_job(conn, jid, OWNER)
        main.app.dependency_overrides.update(_override(OTHER))
        try:
            r = client.get(f"/api/external/scraper/jobs/{jid}")
            assert r.status_code == 403
            assert r.json()["error"]["code"] == "access_denied"
        finally:
            main.app.dependency_overrides.clear()
            _cleanup(conn, jid, user_ids=(OWNER, OTHER))

    def test_admin_bypasses_ownership(self, client):
        conn = db.get_db()
        jid = str(uuid.uuid4())
        _mk_user_row(conn, OWNER)
        _mk_user_row(conn, ADMIN, is_admin=1)
        _mk_job(conn, jid, OWNER)
        main.app.dependency_overrides.update(_override(ADMIN, is_admin=True))
        try:
            r = client.get(f"/api/external/scraper/jobs/{jid}")
            assert r.status_code == 200
            assert r.json()["data"]["job_id"] == jid
        finally:
            main.app.dependency_overrides.clear()
            _cleanup(conn, jid, user_ids=(OWNER, ADMIN))

    def test_non_scraper_job_404(self, client):
        conn = db.get_db()
        jid = str(uuid.uuid4())
        _mk_user_row(conn, OWNER)
        iso = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT INTO jobs (job_id, user_id, job_type, status, query, created_at, updated_at) "
            "VALUES (?, ?, 'enrichment', 'done', 'q', ?, ?)", (jid, OWNER, iso, iso))
        conn.commit()
        main.app.dependency_overrides.update(_override(OWNER))
        try:
            r = client.get(f"/api/external/scraper/jobs/{jid}")
            assert r.status_code == 404
            assert r.json()["error"]["code"] == "not_found"
        finally:
            main.app.dependency_overrides.clear()
            _cleanup(conn, jid, user_ids=(OWNER,))


# ---------------------------------------------------------------------------
# 2. Envelope contract + regression guards
# ---------------------------------------------------------------------------

class TestEnvelope:
    def test_success_envelope_shape(self, client):
        main.app.dependency_overrides.update(_override(OWNER))
        try:
            r = client.get("/api/external/scraper/quota")
            assert r.status_code == 200
            body = r.json()
            assert set(body.keys()) == {"success", "data", "error", "meta"}
            assert body["success"] is True and body["error"] is None
            assert "external_task_limit" in body["data"]
        finally:
            main.app.dependency_overrides.clear()

    def test_error_envelope_includes_code_and_status(self, client):
        main.app.dependency_overrides.update(_override(OWNER))
        try:
            r = client.get("/api/external/scraper/jobs/nope")
            assert r.status_code == 404
            body = r.json()
            assert body["success"] is False
            assert body["error"]["code"] == "not_found"
            assert body["error"]["status"] == 404
            assert body["data"] is None
        finally:
            main.app.dependency_overrides.clear()

    def test_validation_error_enveloped_422(self, client):
        main.app.dependency_overrides.update(_override(OWNER))
        try:
            r = client.post("/api/external/scraper/jobs", json={"query": ""})
            assert r.status_code == 422
            body = r.json()
            assert body["success"] is False
            assert body["error"]["code"] == "validation_error"
        finally:
            main.app.dependency_overrides.clear()

    def test_old_routes_keep_detail_body(self, client):
        """REGRESSION GUARD: the path-scoped envelope handler must not leak —
        existing routes keep FastAPI's default {"detail": ...} body."""
        r = client.get("/api/scraper/jobs")
        assert r.status_code == 401
        assert "detail" in r.json()  # NOT {"success": ...}

    def test_old_route_404_keeps_detail_body(self, client):
        main.app.dependency_overrides[_auth.get_current_user] = lambda: _make_user(OWNER)
        try:
            r = client.get("/api/scraper/jobs/definitely-not-a-job")
            assert r.status_code == 404
            assert set(r.json().keys()) == {"detail"}
        finally:
            main.app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# 3. POST /jobs — create flow, cache short-circuit, guardrails
# ---------------------------------------------------------------------------

class TestCreateJob:
    def test_create_success_payload(self, client):
        conn = db.get_db()
        _mk_user_row(conn, OWNER)
        main.app.dependency_overrides.update(_override(OWNER))
        job_id = None
        try:
            r = client.post("/api/external/scraper/jobs", json={
                "query": "coffee shop", "mode": "cities", "country": "us",
                "cities": ["Austin"], "prefer_cache": False,
            })
            assert r.status_code == 200
            data = r.json()["data"]
            assert data["created"] is True
            job_id = data["job_id"]
            assert data["status"] == "queued"
            assert data["display_name"].startswith("[API] ")
            assert data["total_tasks"] == data["center_count"] * 3
            assert data["links"]["results"].endswith(f"/jobs/{job_id}/results")
        finally:
            main.app.dependency_overrides.clear()
            if job_id:
                _cleanup(conn, job_id, user_ids=(OWNER,))
            else:
                _cleanup(conn, user_ids=(OWNER,))

    def test_full_cache_hit_short_circuits_no_job(self, client, tmp_path):
        conn = db.get_db()
        _mk_user_row(conn, OWNER)
        csv_path = tmp_path / "hit.csv"
        _write_csv(csv_path, 5)
        _mk_cache_row(conn, _austin_cache_id(), csv_path, total_results=5)
        main.app.dependency_overrides.update(_override(OWNER))
        try:
            before = conn.execute(
                "SELECT COUNT(*) AS n FROM jobs WHERE job_type='scraper'").fetchone()["n"]
            r = client.post("/api/external/scraper/jobs", json={
                "query": "coffee shop", "mode": "cities", "country": "us",
                "cities": ["Austin"], "prefer_cache": True,
            })
            after = conn.execute(
                "SELECT COUNT(*) AS n FROM jobs WHERE job_type='scraper'").fetchone()["n"]
            assert r.status_code == 200
            data = r.json()["data"]
            assert data["created"] is False
            assert data["served_from_cache"] is True
            assert data["rows_available"] == 5
            assert after == before  # no row inserted
        finally:
            main.app.dependency_overrides.clear()
            conn.execute("DELETE FROM scraped_cache WHERE cache_id = ?", (_austin_cache_id(),))
            conn.commit()
            _cleanup(conn, user_ids=(OWNER,))

    def test_task_cap_422(self, client):
        conn = db.get_db()
        _mk_user_row(conn, OWNER)
        main.app.dependency_overrides.update(_override(OWNER))
        try:
            with mock.patch.dict(os.environ, {"MAX_EXTERNAL_SCRAPER_TASKS": "1"}):
                r = client.post("/api/external/scraper/jobs", json={
                    "query": "coffee shop", "mode": "cities", "country": "us",
                    "cities": ["Austin"], "prefer_cache": False,
                })
            assert r.status_code == 422
            body = r.json()
            assert body["error"]["code"] == "task_limit_exceeded"
            assert body["error"]["limit"] == 1
        finally:
            main.app.dependency_overrides.clear()
            _cleanup(conn, user_ids=(OWNER,))

    def test_quota_denied_429(self, client):
        conn = db.get_db()
        _mk_user_row(conn, OWNER)
        main.app.dependency_overrides.update(_override(OWNER))
        try:
            with mock.patch.object(ext.db, "check_daily_request_limit",
                                   return_value=(False, "Daily limit exceeded.")):
                r = client.post("/api/external/scraper/jobs", json={
                    "query": "coffee shop", "mode": "cities", "country": "us",
                    "cities": ["Austin"], "prefer_cache": False,
                })
            assert r.status_code == 429
            body = r.json()
            assert body["error"]["code"] == "quota_exceeded"
            assert "resets_at" in body["error"]
        finally:
            main.app.dependency_overrides.clear()
            _cleanup(conn, user_ids=(OWNER,))

    def test_missing_scraper_key_500(self, client):
        conn = db.get_db()
        _mk_user_row(conn, OWNER)
        main.app.dependency_overrides.update(_override(OWNER))
        try:
            with mock.patch.dict(os.environ, {"SCRAPER_TECH_KEY": ""}):
                r = client.post("/api/external/scraper/jobs", json={
                    "query": "coffee shop", "mode": "cities", "country": "us",
                    "cities": ["Austin"], "prefer_cache": False,
                })
            assert r.status_code == 500
            assert r.json()["error"]["code"] == "scraper_not_configured"
        finally:
            main.app.dependency_overrides.clear()
            _cleanup(conn, user_ids=(OWNER,))


# ---------------------------------------------------------------------------
# 4. GET /jobs + GET /jobs/{id}
# ---------------------------------------------------------------------------

class TestJobListAndStatus:
    def test_list_only_own_jobs_with_meta(self, client):
        conn = db.get_db()
        _mk_user_row(conn, OWNER)
        _mk_user_row(conn, OTHER)
        mine = str(uuid.uuid4())
        theirs = str(uuid.uuid4())
        _mk_job(conn, mine, OWNER, status="done")
        _mk_job(conn, theirs, OTHER, status="done")
        main.app.dependency_overrides.update(_override(OWNER))
        try:
            r = client.get("/api/external/scraper/jobs?limit=10")
            assert r.status_code == 200
            body = r.json()
            ids = [j["job_id"] for j in body["data"]]
            assert mine in ids and theirs not in ids
            assert body["meta"]["limit"] == 10
        finally:
            main.app.dependency_overrides.clear()
            _cleanup(conn, mine, theirs, user_ids=(OWNER, OTHER))

    def test_status_detail_projection(self, client):
        conn = db.get_db()
        _mk_user_row(conn, OWNER)
        jid = str(uuid.uuid4())
        _mk_job(conn, jid, OWNER, status="running", total_tasks=9, done_tasks=3)
        main.app.dependency_overrides.update(_override(OWNER))
        try:
            r = client.get(f"/api/external/scraper/jobs/{jid}")
            assert r.status_code == 200
            data = r.json()["data"]
            assert data["progress"]["pct_complete"] == 33.3
            assert data["regions"]["cities"] == ["Austin"]
            assert "user_id" not in data and "output_path" not in data
            assert data["suggested_poll_seconds"] == 10
        finally:
            main.app.dependency_overrides.clear()
            _cleanup(conn, jid, user_ids=(OWNER,))

    def test_status_filter(self, client):
        conn = db.get_db()
        _mk_user_row(conn, OWNER)
        done_j = str(uuid.uuid4())
        run_j = str(uuid.uuid4())
        _mk_job(conn, done_j, OWNER, status="done")
        _mk_job(conn, run_j, OWNER, status="running")
        main.app.dependency_overrides.update(_override(OWNER))
        try:
            r = client.get("/api/external/scraper/jobs?status=done")
            ids = [j["job_id"] for j in r.json()["data"]]
            assert done_j in ids and run_j not in ids
        finally:
            main.app.dependency_overrides.clear()
            _cleanup(conn, done_j, run_j, user_ids=(OWNER,))


# ---------------------------------------------------------------------------
# 5. GET /jobs/{id}/results
# ---------------------------------------------------------------------------

class TestJobResults:
    def test_running_409_with_retry_after_header(self, client):
        conn = db.get_db()
        _mk_user_row(conn, OWNER)
        jid = str(uuid.uuid4())
        _mk_job(conn, jid, OWNER, status="running", total_tasks=9, done_tasks=3)
        main.app.dependency_overrides.update(_override(OWNER))
        try:
            r = client.get(f"/api/external/scraper/jobs/{jid}/results")
            assert r.status_code == 409
            body = r.json()
            assert body["error"]["code"] == "not_ready"
            assert body["error"]["job_status"] == "running"
            assert r.headers.get("Retry-After") == "10"
        finally:
            main.app.dependency_overrides.clear()
            _cleanup(conn, jid, user_ids=(OWNER,))

    def test_done_returns_rows_and_meta(self, client, tmp_path):
        conn = db.get_db()
        _mk_user_row(conn, OWNER)
        jid = str(uuid.uuid4())
        csv_path = tmp_path / "out.csv"
        _write_csv(csv_path, 7, cols=("name", "phone", "website"))
        _mk_job(conn, jid, OWNER, status="done", output_path=str(csv_path))
        main.app.dependency_overrides.update(_override(OWNER))
        try:
            r = client.get(f"/api/external/scraper/jobs/{jid}/results?limit=3&offset=1")
            assert r.status_code == 200
            body = r.json()
            assert body["data"]["total_rows"] == 7
            assert len(body["data"]["rows"]) == 3
            assert body["meta"] == {"total": 7, "limit": 3, "offset": 1}
            assert body["data"]["rows"][0]["name"] == "Biz 1"
        finally:
            main.app.dependency_overrides.clear()
            _cleanup(conn, jid, user_ids=(OWNER,))

    def test_fields_projection_query_param(self, client, tmp_path):
        conn = db.get_db()
        _mk_user_row(conn, OWNER)
        jid = str(uuid.uuid4())
        csv_path = tmp_path / "proj.csv"
        _write_csv(csv_path, 2, cols=("name", "phone", "website"))
        _mk_job(conn, jid, OWNER, status="done", output_path=str(csv_path))
        main.app.dependency_overrides.update(_override(OWNER))
        try:
            r = client.get(f"/api/external/scraper/jobs/{jid}/results?fields=name")
            rows = r.json()["data"]["rows"]
            assert rows[0].keys() == {"name"}
        finally:
            main.app.dependency_overrides.clear()
            _cleanup(conn, jid, user_ids=(OWNER,))

    def test_compact_fields(self, client, tmp_path):
        conn = db.get_db()
        _mk_user_row(conn, OWNER)
        jid = str(uuid.uuid4())
        csv_path = tmp_path / "compact.csv"
        _write_csv(csv_path, 1, cols=list(ext.COMPACT_FIELDS))
        _mk_job(conn, jid, OWNER, status="done", output_path=str(csv_path))
        main.app.dependency_overrides.update(_override(OWNER))
        try:
            r = client.get(f"/api/external/scraper/jobs/{jid}/results?fields=compact")
            assert r.status_code == 200
            assert r.json()["data"]["fields"] == list(ext.COMPACT_FIELDS)
        finally:
            main.app.dependency_overrides.clear()
            _cleanup(conn, jid, user_ids=(OWNER,))

    def test_invalid_field_400(self, client, tmp_path):
        conn = db.get_db()
        _mk_user_row(conn, OWNER)
        jid = str(uuid.uuid4())
        csv_path = tmp_path / "inv.csv"
        _write_csv(csv_path, 1)
        _mk_job(conn, jid, OWNER, status="done", output_path=str(csv_path))
        main.app.dependency_overrides.update(_override(OWNER))
        try:
            r = client.get(f"/api/external/scraper/jobs/{jid}/results?fields=name,bogus")
            assert r.status_code == 400
            assert r.json()["error"]["code"] == "invalid_fields"
        finally:
            main.app.dependency_overrides.clear()
            _cleanup(conn, jid, user_ids=(OWNER,))

    def test_done_no_file_404(self, client):
        conn = db.get_db()
        _mk_user_row(conn, OWNER)
        jid = str(uuid.uuid4())
        _mk_job(conn, jid, OWNER, status="done", output_path="/nonexistent/x.csv")
        main.app.dependency_overrides.update(_override(OWNER))
        try:
            r = client.get(f"/api/external/scraper/jobs/{jid}/results")
            assert r.status_code == 404
            assert r.json()["error"]["code"] == "results_not_available"
        finally:
            main.app.dependency_overrides.clear()
            _cleanup(conn, jid, user_ids=(OWNER,))

    def test_limit_above_max_422(self, client, tmp_path):
        conn = db.get_db()
        _mk_user_row(conn, OWNER)
        jid = str(uuid.uuid4())
        csv_path = tmp_path / "lim.csv"
        _write_csv(csv_path, 2)
        _mk_job(conn, jid, OWNER, status="done", output_path=str(csv_path))
        main.app.dependency_overrides.update(_override(OWNER))
        try:
            r = client.get(f"/api/external/scraper/jobs/{jid}/results?limit=1001")
            assert r.status_code == 422
        finally:
            main.app.dependency_overrides.clear()
            _cleanup(conn, jid, user_ids=(OWNER,))


# ---------------------------------------------------------------------------
# 6. POST /jobs/{id}/cancel
# ---------------------------------------------------------------------------

class TestCancel:
    def test_cancel_queued_job(self, client):
        conn = db.get_db()
        _mk_user_row(conn, OWNER)
        jid = str(uuid.uuid4())
        _mk_job(conn, jid, OWNER, status="queued")
        main.app.dependency_overrides.update(_override(OWNER))
        try:
            r = client.post(f"/api/external/scraper/jobs/{jid}/cancel")
            assert r.status_code == 200
            body = r.json()["data"]
            assert body["ok"] is True and body["status"] == "cancelled"
            row = conn.execute("SELECT status FROM jobs WHERE job_id = ?", (jid,)).fetchone()
            assert row["status"] == "cancelled"
        finally:
            main.app.dependency_overrides.clear()
            _cleanup(conn, jid, user_ids=(OWNER,))

    def test_cancel_done_400(self, client):
        conn = db.get_db()
        _mk_user_row(conn, OWNER)
        jid = str(uuid.uuid4())
        _mk_job(conn, jid, OWNER, status="done")
        main.app.dependency_overrides.update(_override(OWNER))
        try:
            r = client.post(f"/api/external/scraper/jobs/{jid}/cancel")
            assert r.status_code == 400
            assert r.json()["error"]["code"] == "not_cancellable"
        finally:
            main.app.dependency_overrides.clear()
            _cleanup(conn, jid, user_ids=(OWNER,))

    def test_cancel_non_owner_403(self, client):
        conn = db.get_db()
        _mk_user_row(conn, OWNER)
        _mk_user_row(conn, OTHER)
        jid = str(uuid.uuid4())
        _mk_job(conn, jid, OWNER, status="running")
        main.app.dependency_overrides.update(_override(OTHER))
        try:
            r = client.post(f"/api/external/scraper/jobs/{jid}/cancel")
            assert r.status_code == 403
        finally:
            main.app.dependency_overrides.clear()
            _cleanup(conn, jid, user_ids=(OWNER, OTHER))


# ---------------------------------------------------------------------------
# 7. POST /estimate + POST /cache
# ---------------------------------------------------------------------------

class TestEstimateAndCache:
    def test_estimate_payload(self, client):
        conn = db.get_db()
        _mk_user_row(conn, OWNER)
        main.app.dependency_overrides.update(_override(OWNER))
        try:
            r = client.post("/api/external/scraper/estimate", json={
                "query": "coffee shop", "mode": "cities", "country": "us",
                "cities": ["Austin"],
            })
            assert r.status_code == 200
            data = r.json()["data"]
            assert data["task_count"] == data["center_count"] * 3
            assert data["cache"]["cached"] is False
            assert data["can_proceed"] is True
        finally:
            main.app.dependency_overrides.clear()
            _cleanup(conn, user_ids=(OWNER,))

    def test_cache_hit_returns_inline_rows(self, client, tmp_path):
        conn = db.get_db()
        _mk_user_row(conn, OWNER)
        csv_path = tmp_path / "c.csv"
        _write_csv(csv_path, 5)
        _mk_cache_row(conn, _austin_cache_id(), csv_path, total_results=5)
        main.app.dependency_overrides.update(_override(OWNER))
        try:
            r = client.post("/api/external/scraper/cache?limit=2", json={
                "query": "coffee shop", "mode": "cities", "country": "us",
                "cities": ["Austin"],
            })
            assert r.status_code == 200
            data = r.json()["data"]
            assert data["cached"] is True
            assert data["file_available"] is True
            assert data["rows_available"] == 5
            assert len(data["rows"]) == 2
            assert r.json()["meta"]["total"] == 5
        finally:
            main.app.dependency_overrides.clear()
            conn.execute("DELETE FROM scraped_cache WHERE cache_id = ?", (_austin_cache_id(),))
            conn.commit()
            _cleanup(conn, user_ids=(OWNER,))

    def test_cache_hit_missing_file_not_404(self, client, tmp_path):
        conn = db.get_db()
        _mk_user_row(conn, OWNER)
        _mk_cache_row(conn, _austin_cache_id(), tmp_path / "gone.csv", total_results=9)
        main.app.dependency_overrides.update(_override(OWNER))
        try:
            r = client.post("/api/external/scraper/cache", json={
                "query": "coffee shop", "mode": "cities", "country": "us",
                "cities": ["Austin"],
            })
            assert r.status_code == 200
            data = r.json()["data"]
            assert data["cached"] is True
            assert data["file_available"] is False
            assert data["rows"] == []
        finally:
            main.app.dependency_overrides.clear()
            conn.execute("DELETE FROM scraped_cache WHERE cache_id = ?", (_austin_cache_id(),))
            conn.commit()
            _cleanup(conn, user_ids=(OWNER,))

    def test_cache_miss_shape(self, client):
        conn = db.get_db()
        _mk_user_row(conn, OWNER)
        main.app.dependency_overrides.update(_override(OWNER))
        try:
            r = client.post("/api/external/scraper/cache", json={
                "query": "definitely-not-cached-xyz", "mode": "cities", "country": "us",
                "cities": ["Austin"],
            })
            assert r.status_code == 200
            data = r.json()["data"]
            assert data["cached"] is False
            assert data["fresh_task_estimate"] > 0
        finally:
            main.app.dependency_overrides.clear()
            _cleanup(conn, user_ids=(OWNER,))


# ---------------------------------------------------------------------------
# 8. Quota + kill-switch
# ---------------------------------------------------------------------------

class TestQuotaAndKillSwitch:
    def test_quota_non_admin(self, client):
        conn = db.get_db()
        _mk_user_row(conn, OWNER)
        main.app.dependency_overrides.update(_override(OWNER))
        try:
            r = client.get("/api/external/scraper/quota")
            data = r.json()["data"]
            assert data["limit"] == 50_000
            assert data["external_task_limit"] > 0
            assert data["resets_at"]
        finally:
            main.app.dependency_overrides.clear()
            _cleanup(conn, user_ids=(OWNER,))

    def test_quota_admin_nulls(self, client):
        conn = db.get_db()
        _mk_user_row(conn, ADMIN, is_admin=1)
        main.app.dependency_overrides.update(_override(ADMIN, is_admin=True))
        try:
            r = client.get("/api/external/scraper/quota")
            data = r.json()["data"]
            assert data["limit"] is None and data["remaining"] is None
        finally:
            main.app.dependency_overrides.clear()
            _cleanup(conn, user_ids=(ADMIN,))

    def test_kill_switch_removes_router(self):
        """ENABLE_EXTERNAL_SCRAPER_API=false must omit the whole namespace."""
        import importlib
        with mock.patch.dict(os.environ, {"ENABLE_EXTERNAL_SCRAPER_API": "false"}):
            fresh = importlib.reload(main)
            try:
                paths = {r.path for r in fresh.app.routes if hasattr(r, "path")}
                assert not any("/api/external/" in p for p in paths)
            finally:
                # restore the real module for other tests
                importlib.reload(main)
