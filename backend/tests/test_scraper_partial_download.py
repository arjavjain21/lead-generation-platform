"""
Tests for scraper incremental/segmented partial downloads.

Covers:
- ``_count_csv_data_rows`` counts RECORDS (not physical lines) and its
  (size, mtime) cache invalidates as the writer appends rows.
- The new endpoints ``/partial-progress``, ``/shards``, ``/shard/{n}`` and the
  hardened ``/partial-download``: shard math uses rows_on_disk (NOT total_tasks),
  streaming returns the right row slice, no status gate on running jobs (while
  ``/download`` stays gated -> 202), 503+Retry-After on a DB lock, 403 for a
  non-owner, 404 for a non-scraper job / when no CSV has flushed yet.

Mirrors the enrichment shard pattern (``enrichment/routes.py:3759-3982``) adapted
for the scraper (task-based %, rows_on_disk shard basis).
"""
from __future__ import annotations

import csv as _csv
import io
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

import pytest

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from scraper import routes  # noqa: E402
from shared import auth as _auth, db  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

OWNER_UID = "scraper-pd-owner"


def _make_user(user_id=OWNER_UID, is_admin=False):
    return {"user_id": user_id, "email": f"{user_id}@test.example", "is_admin": is_admin}


def _override_auth(user_id=OWNER_UID, is_admin=False):
    return {_auth.get_current_user_with_api_key: lambda: _make_user(user_id, is_admin)}


def _make_test_user(conn, user_id):
    import hashlib
    conn.execute(
        "INSERT OR IGNORE INTO users (user_id, email, password_hash, created_at) "
        "VALUES (?, ?, ?, ?)",
        (user_id, f"{user_id}@test.example",
         hashlib.sha256(b"x").hexdigest(), datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()


def _insert_scraper_job(conn, job_id, user_id, status="running",
                        total_tasks=88638, done_tasks=1000, result_count=0):
    iso = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO jobs (job_id, user_id, job_type, status, query, total_tasks, "
        "done_tasks, result_count, created_at, updated_at) "
        "VALUES (?, ?, 'scraper', ?, 'test query', ?, ?, ?, ?, ?)",
        (job_id, user_id, status, total_tasks, done_tasks, result_count, iso, iso),
    )
    conn.commit()


def _cleanup(conn, job_id, user_id=None):
    try:
        conn.execute("DELETE FROM job_events WHERE job_id = ?", (job_id,))
        conn.execute("DELETE FROM task_checkpoints WHERE job_id = ?", (job_id,))
        conn.execute("DELETE FROM jobs WHERE job_id = ?", (job_id,))
        if user_id:
            conn.execute("DELETE FROM users WHERE user_id = ?", (user_id,))
        conn.commit()
    except Exception:
        conn.rollback()


def _write_rows(path, n, cols=("a", "b")):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = _csv.writer(f)
        w.writerow(cols)
        for i in range(n):
            w.writerow([i, f"row{i}"])


# ---------------------------------------------------------------------------
# 1. _count_csv_data_rows
# ---------------------------------------------------------------------------

class TestCountCsvDataRows:
    def test_counts_records_not_physical_lines(self, tmp_path):
        # Header + 2 data RECORDS, but the 2nd record's cell has an embedded
        # newline inside quotes (4 physical lines). A naive line count = 3.
        path = tmp_path / "weird.csv"
        path.write_text(
            "name,company\nalice,Acme\nbob,\"Smith\nCo\"\n", encoding="utf-8"
        )
        assert routes._count_csv_data_rows(path) == 2

    def test_cache_invalidates_as_file_grows(self, tmp_path):
        path = tmp_path / "growing.csv"
        _write_rows(path, 5)
        assert routes._count_csv_data_rows(path) == 5
        _write_rows(path, 10)  # overwrite -> new size + mtime -> cache must miss
        assert routes._count_csv_data_rows(path) == 10

    def test_missing_file_returns_zero(self, tmp_path):
        assert routes._count_csv_data_rows(tmp_path / "absent.csv") == 0


# ---------------------------------------------------------------------------
# 2. Endpoint tests
# ---------------------------------------------------------------------------

@pytest.fixture
def client():
    """TestClient with auth dependency overridden to the job owner."""
    from fastapi.testclient import TestClient
    from main import app

    app.dependency_overrides.update(_override_auth())
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


@pytest.fixture
def temp_db():
    """Isolated temp SQLite DB (NOT the live jobs.db) with the tables the scraper
    endpoints touch. Mirrors tests/test_scraper_resume_restart_fixes.py so endpoint
    tests never read/write the production database."""
    import tempfile

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    # check_same_thread=False: the connection is created in the test thread but
    # TestClient runs endpoints in a portal thread. Sequential access only.
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE jobs (
            job_id TEXT PRIMARY KEY, user_id TEXT, job_type TEXT, status TEXT,
            parent_job_id TEXT, query TEXT, regions TEXT, output_path TEXT,
            total_tasks INTEGER, done_tasks INTEGER, result_count INTEGER,
            total INTEGER, created_at TEXT, updated_at TEXT,
            is_resumable INTEGER DEFAULT 1, last_heartbeat TEXT
        );
        CREATE TABLE users (
            user_id TEXT PRIMARY KEY, email TEXT, password_hash TEXT, created_at TEXT
        );
        CREATE TABLE task_checkpoints (
            job_id TEXT, center_name TEXT, center_state TEXT, zoom INTEGER,
            completed_at TEXT, result_count INTEGER,
            PRIMARY KEY (job_id, center_name, center_state, zoom)
        );
        """
    )
    yield conn, db_path
    conn.close()
    Path(db_path).unlink(missing_ok=True)


@pytest.fixture
def scraper_store(temp_db, monkeypatch):
    """Patch routes.job_store.get_store -> a store over the temp DB, so scraper
    endpoint tests never touch the live jobs.db. Patches get_store (not db.get_db)
    so background lifespan loops keep using the real DB untouched."""
    from scraper.job_store import ScraperJobStore

    conn, _db_path = temp_db
    monkeypatch.setattr(routes.job_store, "get_store", lambda: ScraperJobStore(conn))
    yield conn


@pytest.fixture
def running_job(scraper_store, tmp_path):
    """A 'running' scraper job owned by OWNER_UID; OUTPUT_DIR patched to tmp_path
    so test CSVs never touch the real outputs dir."""
    conn = scraper_store
    job_id = f"scraper-pd-job-{int(time.time() * 1000)}"
    _make_test_user(conn, OWNER_UID)
    _insert_scraper_job(conn, job_id, OWNER_UID)
    csv_path = tmp_path / f"{job_id}.csv"
    with mock.patch.object(routes, "OUTPUT_DIR", tmp_path):
        yield job_id, csv_path, conn
    _cleanup(conn, job_id, OWNER_UID)


class TestPartialProgress:
    def test_shape_and_task_based_pct(self, client, running_job):
        job_id, csv_path, _ = running_job
        _write_rows(csv_path, 12345)
        r = client.get(f"/api/scraper/jobs/{job_id}/partial-progress")
        assert r.status_code == 200
        b = r.json()
        assert b["status"] == "running"
        assert b["total_tasks"] == 88638
        assert b["done_tasks"] == 1000
        assert b["rows_on_disk"] == 12345
        assert b["pct_complete"] == round(1000 / 88638 * 100, 1)  # task-based
        assert b["partial_available"] is True
        assert b["shard_size"] == 10_000

    def test_no_file_yet_returns_partial_available_false(self, client, running_job):
        job_id, _, _ = running_job
        r = client.get(f"/api/scraper/jobs/{job_id}/partial-progress")
        assert r.status_code == 200
        assert r.json()["partial_available"] is False
        assert r.json()["rows_on_disk"] == 0

    def test_pct_capped_at_100_when_done_exceeds_total(self, client, running_job):
        # Resumed/legacy jobs can accumulate done_tasks > total_tasks; the
        # displayed percentage must cap at 100, not show e.g. 2330%.
        job_id, csv_path, conn = running_job
        conn.execute("UPDATE jobs SET done_tasks = 200000 WHERE job_id = ?", (job_id,))
        conn.commit()
        _write_rows(csv_path, 5)
        r = client.get(f"/api/scraper/jobs/{job_id}/partial-progress")
        assert r.status_code == 200
        assert r.json()["pct_complete"] == 100.0


class TestShards:
    def test_shard_basis_is_rows_on_disk_not_total_tasks(self, client, running_job):
        job_id, csv_path, _ = running_job
        _write_rows(csv_path, 25_000)
        r = client.get(f"/api/scraper/jobs/{job_id}/shards")
        assert r.status_code == 200
        b = r.json()
        assert b["rows_on_disk"] == 25_000
        assert b["total_tasks"] == 88638  # present but NOT the shard basis
        assert len(b["shards"]) == 3  # 25k / 10k -> 3 shards (10k, 10k, 5k)
        assert b["shards"][0] == {
            "shard": 0, "start_row": 0, "end_row": 10_000,
            "rows_available": 10_000, "complete": True,
        }
        assert b["shards"][2]["end_row"] == 25_000
        assert b["shards"][2]["rows_available"] == 5_000

    def test_zero_rows_returns_no_shards(self, client, running_job):
        job_id, _, _ = running_job
        r = client.get(f"/api/scraper/jobs/{job_id}/shards")
        assert r.status_code == 200
        assert r.json()["shards"] == []


class TestShardDownload:
    def test_streams_correct_slice(self, client, running_job):
        job_id, csv_path, _ = running_job
        _write_rows(csv_path, 12_000)
        r = client.get(f"/api/scraper/jobs/{job_id}/shard/1")
        assert r.status_code == 200
        rows = list(_csv.reader(io.StringIO(r.text)))
        assert rows[0] == ["a", "b"]              # header preserved
        assert len(rows) - 1 == 2_000             # shard 1 = rows 10000..11999
        assert rows[1] == ["10000", "row10000"]   # first data row of shard 1

    def test_404_when_no_data_yet(self, client, running_job):
        job_id, _, _ = running_job
        assert client.get(f"/api/scraper/jobs/{job_id}/shard/0").status_code == 404

    def test_400_on_negative_shard(self, client, running_job):
        job_id, csv_path, _ = running_job
        _write_rows(csv_path, 5)
        assert client.get(f"/api/scraper/jobs/{job_id}/shard/-1").status_code == 400


class TestStatusGateAndAuth:
    def test_no_status_gate_on_running_but_download_still_gated(self, client, running_job):
        """Regression guard: the new endpoints serve a running job, but the main
        /download endpoint must still return 202 for a running job."""
        job_id, csv_path, _ = running_job
        _write_rows(csv_path, 50)
        for ep in ("partial-progress", "shards", "partial-download"):
            assert client.get(f"/api/scraper/jobs/{job_id}/{ep}").status_code == 200
        assert client.get(f"/api/scraper/jobs/{job_id}/shard/0").status_code == 200
        assert client.get(f"/api/scraper/jobs/{job_id}/download").status_code == 202

    def test_partial_download_404_when_no_file(self, client, running_job):
        job_id, _, _ = running_job
        assert client.get(f"/api/scraper/jobs/{job_id}/partial-download").status_code == 404

    def test_non_owner_gets_403(self, running_job):
        from fastapi.testclient import TestClient
        from main import app

        job_id, csv_path, _ = running_job
        _write_rows(csv_path, 50)
        app.dependency_overrides.update(
            {_auth.get_current_user_with_api_key: lambda: _make_user("someone-else")}
        )
        try:
            with TestClient(app) as c:
                for ep in ("partial-progress", "shards", "partial-download"):
                    assert c.get(f"/api/scraper/jobs/{job_id}/{ep}").status_code == 403
                assert c.get(f"/api/scraper/jobs/{job_id}/shard/0").status_code == 403
        finally:
            app.dependency_overrides.clear()

    def test_non_scraper_job_type_404(self, client, scraper_store):
        conn = scraper_store
        job_id = f"enrich-job-{int(time.time() * 1000)}"
        _make_test_user(conn, OWNER_UID)
        iso = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT INTO jobs (job_id, user_id, job_type, status, total, "
            "created_at, updated_at) VALUES (?, ?, 'enrichment', 'running', 10, ?, ?)",
            (job_id, OWNER_UID, iso, iso),
        )
        conn.commit()
        try:
            for ep in ("partial-progress", "shards", "partial-download"):
                assert client.get(f"/api/scraper/jobs/{job_id}/{ep}").status_code == 404
        finally:
            _cleanup(conn, job_id, OWNER_UID)


class TestLockHandling:
    def test_503_retry_after_on_db_lock(self, client, running_job):
        job_id, _, _ = running_job
        with mock.patch.object(
            routes.job_store, "get_store",
            side_effect=sqlite3.OperationalError("database is locked"),
        ):
            for ep in ("partial-progress", "shards", "partial-download"):
                r = client.get(f"/api/scraper/jobs/{job_id}/{ep}")
                assert r.status_code == 503
                assert r.headers.get("retry-after") == "3"
            r = client.get(f"/api/scraper/jobs/{job_id}/shard/0")
            assert r.status_code == 503
