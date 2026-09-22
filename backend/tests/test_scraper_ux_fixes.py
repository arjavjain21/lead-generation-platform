"""
Tests for the 2026-09-20 scraper UX / reliability fixes (queue-stall RCA).

Covers:
- ``GET /api/scraper/jobs`` slim projection: fat columns (filename,
  cascade_config, ...) no longer ship to the UI; card fields survive.
- ``GET /api/scraper/estimate``: cheap centers-math estimate for the search
  form (200 with center_count/total_tasks, 422 on an unknown region).
- SSE ``/jobs/{id}/stream``: an immediate snapshot event (with queue_position
  for queued jobs) and stream termination for cancelled jobs (previously only
  done/failed closed the stream).
- ``scraper.dispatch.trim_process_memory``: returns freed bytes, never raises,
  and ``dispatch_loop`` runs it after a hosted job finishes.

Fixture wiring mirrors tests/test_scraper_partial_download.py: the store is
patched onto routes.job_store so endpoints never touch the live jobs.db.
"""
from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from scraper import routes  # noqa: E402
from shared import auth as _auth  # noqa: E402

OWNER_UID = "scraper-ux-owner"


def _make_user(user_id=OWNER_UID, is_admin=True):
    return {"user_id": user_id, "email": f"{user_id}@test.example", "is_admin": is_admin}


def _make_test_user(conn, user_id):
    import hashlib
    conn.execute(
        "INSERT OR IGNORE INTO users (user_id, email, password_hash, created_at) "
        "VALUES (?, ?, ?, ?)",
        (user_id, f"{user_id}@test.example",
         hashlib.sha256(b"x").hexdigest(), datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()


def _insert_scraper_job(conn, job_id, status="queued", fat_cols=True):
    iso = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO jobs (job_id, user_id, job_type, status, query, display_name, "
        "regions, total_tasks, done_tasks, result_count, filename, cascade_config, "
        "created_at, updated_at) "
        "VALUES (?, ?, 'scraper', ?, 'pharmacy', 'pharmacy (US)', '{}', 88638, 0, 0, ?, ?, ?, ?)",
        (
            job_id, OWNER_UID, status,
            "x" * 5000 if fat_cols else None,        # filename
            '{"big":"blob"}' if fat_cols else None,  # cascade_config
            iso, iso,
        ),
    )
    conn.commit()


@pytest.fixture
def temp_db():
    """Isolated temp SQLite DB with every table the touched endpoints use."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE jobs (
            job_id TEXT PRIMARY KEY, user_id TEXT, job_type TEXT, status TEXT,
            parent_job_id TEXT, query TEXT, display_name TEXT, regions TEXT,
            output_path TEXT, total_tasks INTEGER, done_tasks INTEGER,
            result_count INTEGER, filename TEXT, cascade_config TEXT,
            checkpoint_count INTEGER DEFAULT 0, is_resumable INTEGER DEFAULT 1,
            last_heartbeat TEXT, error TEXT, created_at TEXT, updated_at TEXT,
            hidden_from_ui INTEGER DEFAULT 0
        );
        CREATE TABLE users (
            user_id TEXT PRIMARY KEY, email TEXT, password_hash TEXT,
            is_admin INTEGER DEFAULT 0, created_at TEXT
        );
        CREATE TABLE task_checkpoints (
            job_id TEXT, center_name TEXT, center_state TEXT, zoom INTEGER,
            completed_at TEXT, result_count INTEGER,
            PRIMARY KEY (job_id, center_name, center_state, zoom)
        );
        CREATE TABLE job_events (
            job_id TEXT, seq INTEGER, payload TEXT,
            PRIMARY KEY (job_id, seq)
        );
        """
    )
    yield conn, db_path
    conn.close()
    Path(db_path).unlink(missing_ok=True)


@pytest.fixture
def scraper_store(temp_db, monkeypatch):
    """Store + db.get_db patched at the temp DB (queue-position path included)."""
    from scraper.job_store import ScraperJobStore

    conn, _db_path = temp_db
    monkeypatch.setattr(routes.job_store, "get_store", lambda: ScraperJobStore(conn))
    monkeypatch.setattr(routes.db, "get_db", lambda: conn)
    _make_test_user(conn, OWNER_UID)
    yield conn


@pytest.fixture
def client(scraper_store):
    """TestClient with auth overridden to an admin user owning the test jobs."""
    from fastapi.testclient import TestClient
    from main import app

    app.dependency_overrides[_auth.get_current_user] = lambda: _make_user()
    app.dependency_overrides[_auth.get_current_user_optional] = lambda: _make_user()
    try:
        with TestClient(app) as c:
            yield c
    finally:
        app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# 1. Slim jobs list
# ---------------------------------------------------------------------------

class TestSlimJobsList:
    def test_fat_columns_stay_server_side(self, client, scraper_store):
        _insert_scraper_job(scraper_store, "ux-slim-1", status="queued")
        r = client.get("/api/scraper/jobs")
        assert r.status_code == 200
        jobs = r.json()["jobs"]
        mine = [j for j in jobs if j["job_id"] == "ux-slim-1"]
        assert len(mine) == 1
        job = mine[0]
        # Card fields the UI renders must survive...
        for field in ("job_id", "status", "query", "display_name", "regions",
                      "created_at", "done_tasks", "total_tasks", "result_count",
                      "output_exists", "location_display"):
            assert field in job, f"missing UI field: {field}"
        # ...and fat columns must NOT ship.
        for field in ("filename", "cascade_config", "user_id", "output_path",
                      "last_heartbeat"):
            assert field not in job, f"fat field leaked to UI: {field}"

    def test_payload_no_longer_scales_with_row_width(self, client, scraper_store):
        _insert_scraper_job(scraper_store, "ux-slim-2", status="done", fat_cols=True)
        body = client.get("/api/scraper/jobs").content
        # 5,000 filler chars in `filename` must not reach the wire.
        assert b"x" * 100 not in body

    def test_huge_regions_blob_compacted_with_accurate_display(self, client, scraper_store):
        iso = datetime.now(timezone.utc).isoformat()
        fat_zips = [f"1{i:04d}" for i in range(5000)]  # 5,000 zips ≈ 30 KB blob
        import json as _json
        scraper_store.execute(
            "INSERT INTO jobs (job_id, user_id, job_type, status, query, regions, "
            "total_tasks, done_tasks, result_count, created_at, updated_at) "
            "VALUES (?, ?, 'scraper', 'done', 'vaporizer store', ?, 15000, 15000, 5, ?, ?)",
            ("ux-slim-3", OWNER_UID, _json.dumps(
                {"mode": "zips", "country": "us", "states": [], "cities": [],
                 "zips": fat_zips, "center_ids": [], "expected_types": []}
            ), iso, iso),
        )
        scraper_store.commit()
        r = client.get("/api/scraper/jobs")
        mine = [j for j in r.json()["jobs"] if j["job_id"] == "ux-slim-3"][0]
        # The blob itself is capped…
        assert len(mine["regions"]) < 500
        # …but the display keeps the ACCURATE count from the full blob.
        assert mine["location_display"] == "5000 zip/postal codes (US)"


# ---------------------------------------------------------------------------
# 2. Estimate endpoint
# ---------------------------------------------------------------------------

class TestEstimateEndpoint:
    def test_us_all_estimate(self, client):
        r = client.get("/api/scraper/estimate", params={"mode": "all", "country": "us"})
        assert r.status_code == 200
        data = r.json()
        assert data["center_count"] > 0
        assert data["total_tasks"] == data["center_count"] * 3  # zooms 10, 11, 12

    def test_unknown_region_is_422(self, client):
        r = client.get("/api/scraper/estimate", params={"mode": "all", "country": "xx"})
        assert r.status_code == 422
        assert "detail" in r.json()

    def test_states_mode_narrows_scope(self, client):
        r = client.get("/api/scraper/estimate", params={
            "mode": "states", "country": "us", "states": "California",
        })
        assert r.status_code == 200
        data = r.json()
        assert 0 < data["total_tasks"] < 88638  # strictly narrower than US-all


# ---------------------------------------------------------------------------
# 3. SSE snapshot + terminal statuses
# ---------------------------------------------------------------------------

class TestSseStreamFixes:
    """Invokes the route function directly and drives its body_iterator —
    TestClient's sync streaming never returns the first chunk while the
    generator is still looping (queued jobs poll forever), which hangs."""

    @staticmethod
    def _first_event(job_id):
        async def run():
            resp = await routes.stream_scraper_job(
                job_id=job_id, token=None, current_user=_make_user()
            )
            async for chunk in resp.body_iterator:
                for line in chunk.splitlines():
                    if line.startswith("data: "):
                        return json.loads(line.removeprefix("data: "))
            return None

        return asyncio.run(run())

    @staticmethod
    def _all_events(job_id):
        async def run():
            resp = await routes.stream_scraper_job(
                job_id=job_id, token=None, current_user=_make_user()
            )
            events = []
            async for chunk in resp.body_iterator:
                for line in chunk.splitlines():
                    if line.startswith("data: "):
                        events.append(json.loads(line.removeprefix("data: ")))
            return events

        return asyncio.run(run())

    def test_queued_job_emits_snapshot_with_position(self, scraper_store):
        _insert_scraper_job(scraper_store, "ux-sse-queued", status="queued")
        data = self._first_event("ux-sse-queued")
        assert data is not None
        assert data["snapshot"] is True
        assert data["status"] == "queued"
        assert data["queue_position"] >= 1

    def test_cancelled_job_stream_terminates(self, scraper_store):
        _insert_scraper_job(scraper_store, "ux-sse-cancelled", status="cancelled")
        events = self._all_events("ux-sse-cancelled")
        statuses = [e.get("status") for e in events]
        assert "cancelled" in statuses
        assert any(e.get("done") for e in events)  # final event emitted, generator exited


# ---------------------------------------------------------------------------
# 4. ?token= query auth on download endpoints (browser-native downloads)
# ---------------------------------------------------------------------------

class TestDownloadTokenAuth:
    """The download endpoints accept ?token= JWT so the browser's own download
    manager can fetch mega-job CSVs (no Authorization header possible)."""

    @pytest.fixture
    def running_job_with_csv(self, scraper_store, tmp_path):
        from unittest import mock as _mock

        _insert_scraper_job(scraper_store, "ux-dl-1", status="running")
        csv_path = tmp_path / "ux-dl-1.csv"
        csv_path.write_text("name,phone\nAcme,555\n", encoding="utf-8")
        with _mock.patch.object(routes, "OUTPUT_DIR", tmp_path):
            yield "ux-dl-1"

    def test_no_auth_is_401(self, client, running_job_with_csv):
        r = client.get(f"/api/scraper/jobs/{running_job_with_csv}/partial-download")
        assert r.status_code == 401

    def test_api_key_branch(self, running_job_with_csv, monkeypatch):
        monkeypatch.setattr(
            routes.auth, "verify_api_key", lambda k: {"user_id": OWNER_UID, "is_admin": True}
        )
        user = asyncio.run(routes._user_or_token_fallback(
            x_api_key="lgp_test", authorization=None, token=None))
        assert user["user_id"] == OWNER_UID

    def test_bearer_branch(self, running_job_with_csv, monkeypatch):
        decoded = []
        monkeypatch.setattr(
            routes.auth, "decode_token",
            lambda t: decoded.append(t) or {"user_id": OWNER_UID},
        )
        user = asyncio.run(routes._user_or_token_fallback(
            x_api_key=None, authorization="Bearer abc.def", token=None))
        assert user["user_id"] == OWNER_UID
        assert decoded == ["abc.def"]  # "Bearer " prefix stripped

    def test_token_branch(self, running_job_with_csv, monkeypatch):
        decoded = []
        monkeypatch.setattr(
            routes.auth, "decode_token",
            lambda t: decoded.append(t) or {"user_id": OWNER_UID},
        )
        user = asyncio.run(routes._user_or_token_fallback(
            x_api_key=None, authorization=None, token="q.jwt.token"))
        assert user["user_id"] == OWNER_UID
        assert decoded == ["q.jwt.token"]


# ---------------------------------------------------------------------------
# 5. Dispatch memory trim (standalone — needs only a jobs table)
# ---------------------------------------------------------------------------

@pytest.fixture
def dispatch_db(tmp_path, monkeypatch):
    db_path = tmp_path / "jobs.db"
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE jobs (
            job_id TEXT PRIMARY KEY, user_id TEXT, job_type TEXT,
            status TEXT, parent_job_id TEXT, query TEXT, regions TEXT,
            total_tasks INTEGER, done_tasks INTEGER, result_count INTEGER,
            restart_count INTEGER DEFAULT 0, is_resumable INTEGER DEFAULT 1,
            last_heartbeat TEXT, error TEXT, output_path TEXT, cancelled_at TEXT,
            created_at TEXT NOT NULL, updated_at TEXT NOT NULL
        );
        """
    )
    conn.commit()
    import shared.db as shared_db
    monkeypatch.setattr(shared_db, "get_db", lambda: conn, raising=True)
    yield conn
    conn.close()


class TestMemoryTrim:
    def test_trim_returns_freed_bytes_and_never_raises(self, monkeypatch):
        from scraper import dispatch
        rss = [10_000_000, 4_000_000]
        monkeypatch.setattr(dispatch, "_rss_bytes", lambda: rss.pop(0))
        freed = dispatch.trim_process_memory()
        assert freed == 6_000_000

    def test_trim_survives_unreadable_proc(self, monkeypatch):
        from scraper import dispatch
        monkeypatch.setattr(dispatch, "_rss_bytes", lambda: 0)
        assert dispatch.trim_process_memory() == 0

    def test_dispatch_loop_trims_after_job_finishes(self, dispatch_db, monkeypatch):
        from scraper import dispatch
        monkeypatch.setattr(dispatch, "MAX_CONCURRENT_SCRAPER_JOBS", 10)
        iso = datetime.now(timezone.utc).isoformat()
        dispatch_db.execute(
            "INSERT INTO jobs (job_id, user_id, job_type, status, query, regions, "
            "total_tasks, created_at, updated_at) "
            "VALUES ('ux-trim-1', 'u1', 'scraper', 'queued', 'q', '{}', 100, ?, ?)",
            (iso, iso),
        )
        dispatch_db.commit()

        trim_calls: list[str] = []
        monkeypatch.setattr(
            dispatch, "trim_process_memory", lambda: trim_calls.append("trim") or 0
        )

        async def quick_launch(job_id):
            await asyncio.sleep(0.02)

        async def run():
            task = asyncio.create_task(
                dispatch.dispatch_loop(quick_launch, poll_seconds=0.01, per_worker_cap=2)
            )
            await asyncio.sleep(0.2)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(run())
        # The hosted job finished during the window — a trim must have run.
        assert len(trim_calls) >= 1
