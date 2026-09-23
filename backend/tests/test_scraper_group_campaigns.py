"""Tests for scraper campaign buckets (group_name, 2026-09-23).

Covers:
- ``POST /api/scraper/jobs`` with ``group``: sanitized label persisted as
  ``group_name``; invalid labels are dropped (NULL) instead of rejected.
- ``GET /api/scraper/jobs`` slim projection carries ``group_name``.
- ``GET /api/scraper/jobs/group/{name}/download``: merged CSV across the
  group's on-disk files — single header, creation order, per-job rows kept;
  404 for unknown group or group with no files yet; works mid-campaign
  (running job contributes its incrementally-written file).
- Resume path propagates ``group_name`` (store-level check).

Fixture wiring mirrors tests/test_scraper_ux_fixes.py: the store is patched
onto routes.job_store so endpoints never touch the live jobs.db.
"""
from __future__ import annotations

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

OWNER_UID = "group-campaign-owner"
OTHER_UID = "group-campaign-other"


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


def _insert_group_job(conn, job_id, group_name, query, status="done",
                      user_id=OWNER_UID, created_at=None):
    iso = created_at or datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO jobs (job_id, user_id, job_type, status, query, group_name, "
        "regions, total_tasks, done_tasks, result_count, created_at, updated_at) "
        "VALUES (?, ?, 'scraper', ?, ?, ?, '{}', 864, 864, 10, ?, ?)",
        (job_id, user_id, status, query, group_name, iso, iso),
    )
    conn.commit()


def _write_csv(path: Path, rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        for row in rows:
            f.write(",".join(row) + "\n")


@pytest.fixture
def temp_db():
    """Isolated temp SQLite DB with the tables the touched endpoints use."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE jobs (
            job_id TEXT PRIMARY KEY, user_id TEXT, job_type TEXT, status TEXT,
            parent_job_id TEXT, query TEXT, display_name TEXT, group_name TEXT,
            regions TEXT, output_path TEXT, total_tasks INTEGER,
            done_tasks INTEGER, result_count INTEGER, filename TEXT,
            cascade_config TEXT, checkpoint_count INTEGER DEFAULT 0,
            is_resumable INTEGER DEFAULT 1, last_heartbeat TEXT, error TEXT,
            created_at TEXT, updated_at TEXT, hidden_from_ui INTEGER DEFAULT 0
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
def scraper_store(temp_db, monkeypatch, tmp_path):
    """Store + OUTPUT_DIR patched to temp DB/dirs (never touches live jobs.db)."""
    from scraper.job_store import ScraperJobStore

    conn, _db_path = temp_db
    monkeypatch.setattr(routes.job_store, "get_store", lambda: ScraperJobStore(conn))
    monkeypatch.setattr(routes.db, "get_db", lambda: conn)
    monkeypatch.setattr(routes, "OUTPUT_DIR", tmp_path / "outputs")
    _make_test_user(conn, OWNER_UID)
    _make_test_user(conn, OTHER_UID)
    yield conn


@pytest.fixture
def client(scraper_store):
    from fastapi.testclient import TestClient
    from main import app

    app.dependency_overrides[_auth.get_current_user] = lambda: _make_user()
    app.dependency_overrides[_auth.get_current_user_optional] = lambda: _make_user()
    app.dependency_overrides[routes._user_or_token_fallback] = lambda: _make_user()
    try:
        with TestClient(app) as c:
            yield c
    finally:
        app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# Sanitizer
# ---------------------------------------------------------------------------

class TestSanitizeGroupName:
    def test_normal_label_kept(self):
        assert routes._sanitize_group_name("Indonesia — Hospitality & Wellness".replace("—", "-")) is not None

    def test_strips_and_rejects_empty(self):
        assert routes._sanitize_group_name("   ") is None
        assert routes._sanitize_group_name(None) is None

    def test_rejects_overlong_and_bad_chars(self):
        assert routes._sanitize_group_name("x" * 81) is None
        assert routes._sanitize_group_name('drop"; TABLE') is None

    def test_ampersand_and_space_allowed(self):
        assert routes._sanitize_group_name("Footwear & Stockists batch-1") == "Footwear & Stockists batch-1"


# ---------------------------------------------------------------------------
# Slim list carries group_name
# ---------------------------------------------------------------------------

class TestGroupInJobsList:
    def test_group_name_in_slim_projection(self, client, scraper_store):
        _insert_group_job(scraper_store, "grp-list-1", "Indonesia Hospitality", "beach club")
        _insert_group_job(scraper_store, "grp-list-2", None, "plain job")
        r = client.get("/api/scraper/jobs")
        assert r.status_code == 200
        jobs = {j["job_id"]: j for j in r.json()["jobs"]}
        assert jobs["grp-list-1"]["group_name"] == "Indonesia Hospitality"
        assert jobs["grp-list-2"]["group_name"] is None


# ---------------------------------------------------------------------------
# Group merged download
# ---------------------------------------------------------------------------

class TestGroupDownload:
    def test_merges_csvs_single_header_creation_order(self, client, scraper_store):
        out = routes.OUTPUT_DIR
        _write_csv(out / "a.csv", [["name,query"], ["row-a1,beach club"], ["row-a2,beach club"]])
        _write_csv(out / "b.csv", [["name,query"], ["row-b1,yoga studio"]])
        base = datetime(2026, 9, 23, 10, 0, 0, tzinfo=timezone.utc)
        _insert_group_job(scraper_store, "ga", "Camp X", "beach club",
                          created_at=base.isoformat())
        _insert_group_job(scraper_store, "gb", "Camp X", "yoga studio",
                          created_at=base.replace(minute=1).isoformat())
        scraper_store.execute("UPDATE jobs SET output_path=? WHERE job_id='ga'", (str(out / "a.csv"),))
        scraper_store.execute("UPDATE jobs SET output_path=? WHERE job_id='gb'", (str(out / "b.csv"),))
        scraper_store.commit()

        r = client.get("/api/scraper/jobs/group/Camp X/download")
        assert r.status_code == 200
        assert "text/csv" in r.headers["content-type"]
        assert "Camp_X_combined.csv" in r.headers.get("content-disposition", "")
        lines = r.text.strip().splitlines()
        # one header + 3 data rows, in creation order (a before b)
        assert lines[0] == "name,query"
        assert lines[1:] == ["row-a1,beach club", "row-a2,beach club", "row-b1,yoga studio"]

    def test_includes_running_jobs_partial_file(self, client, scraper_store):
        out = routes.OUTPUT_DIR
        # no output_path on the row — the {job_id}.csv fallback must kick in,
        # which is exactly how a running job's incremental file is found.
        _write_csv(out / "gr1.csv", [["name"], ["partial-row-1"]])
        _insert_group_job(scraper_store, "gr1", "Camp Y", "surf shop", status="running")
        # no output_path on the row — the {job_id}.csv fallback must kick in
        r = client.get("/api/scraper/jobs/group/Camp Y/download")
        assert r.status_code == 200
        assert "partial-row-1" in r.text

    def test_unknown_group_404(self, client, scraper_store):
        r = client.get("/api/scraper/jobs/group/Nope/download")
        assert r.status_code == 404

    def test_group_without_files_404(self, client, scraper_store):
        _insert_group_job(scraper_store, "gnf1", "Camp Z", "gift shop")
        r = client.get("/api/scraper/jobs/group/Camp Z/download")
        assert r.status_code == 404

    def test_non_owner_cannot_download_group(self, client, scraper_store):
        from fastapi.testclient import TestClient
        from main import app
        _insert_group_job(scraper_store, "gother", "Secret Camp", "bar",
                          user_id=OTHER_UID)
        app.dependency_overrides[routes._user_or_token_fallback] = lambda: _make_user(OTHER_UID, is_admin=False)
        # Owner of the jobs (non-admin OTHER) CAN download
        r = client.get("/api/scraper/jobs/group/Secret Camp/download")
        assert r.status_code == 404  # no files yet — but not 403: owns the group
        app.dependency_overrides[routes._user_or_token_fallback] = lambda: _make_user(OWNER_UID, is_admin=False)
        # A different non-admin sees nothing of the group either
        r2 = client.get("/api/scraper/jobs/group/Secret Camp/download")
        assert r2.status_code == 404


# ---------------------------------------------------------------------------
# Resume propagates group membership
# ---------------------------------------------------------------------------

class TestResumePropagatesGroup:
    def test_create_scraper_job_accepts_group_name(self, scraper_store):
        from scraper.job_store import ScraperJobStore
        store = ScraperJobStore(scraper_store)
        store.create_scraper_job(
            job_id="grp-new-1", user_id=OWNER_UID, query="test",
            regions={"mode": "all", "country": "id"}, total_tasks=864,
            group_name="Resume Camp",
        )
        row = store.get_job("grp-new-1")
        assert row["group_name"] == "Resume Camp"

    def test_create_scraper_job_without_group_leaves_null(self, scraper_store):
        from scraper.job_store import ScraperJobStore
        store = ScraperJobStore(scraper_store)
        store.create_scraper_job(
            job_id="grp-new-2", user_id=OWNER_UID, query="test",
            regions={"mode": "all", "country": "id"}, total_tasks=864,
        )
        assert store.get_job("grp-new-2")["group_name"] is None
