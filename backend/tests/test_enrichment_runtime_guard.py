"""Tests for the enrichment runtime watchdog (2026-09-11 stuck-job gap).

The boot reaper only runs at worker startup; a murdered runner whose job
missed the reaper window stays 'running' forever. The guard loop reaps
heartbeat-stale running enrichment jobs (>10 min) and feeds them to the same
atomic-claim auto-resume. These tests pin:
  * staleness threshold + job_type scoping of the SELECT
  * reap → resume_one wiring (only for the claim winner)
  * fresh jobs / other job types / kill-switch behavior
"""
import asyncio
import sqlite3
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))


SCHEMA = """
CREATE TABLE jobs (
    job_id TEXT PRIMARY KEY,
    user_id TEXT,
    job_type TEXT,
    status TEXT,
    parent_job_id TEXT,
    restart_count INTEGER DEFAULT 0,
    is_resumable INTEGER DEFAULT 1,
    resume_claimed_at TEXT,
    last_heartbeat TEXT,
    created_at TEXT,
    updated_at TEXT,
    error TEXT
);
"""


@pytest.fixture
def temp_db():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    conn.commit()
    yield db_path
    conn.close()
    Path(db_path).unlink(missing_ok=True)


def _iso(dt: datetime) -> str:
    return dt.isoformat()


def _insert_job(conn, job_id, job_type="enrichment", status="running",
                heartbeat_age_min=1, age_min=60):
    now = datetime.now(timezone.utc)
    conn.execute(
        """INSERT INTO jobs (job_id, user_id, job_type, status, parent_job_id,
             restart_count, is_resumable, last_heartbeat, created_at, updated_at)
           VALUES (?, 'u1', ?, ?, NULL, 0, 1, ?, ?, ?)""",
        (job_id, job_type, status,
         _iso(now - timedelta(minutes=heartbeat_age_min)),
         _iso(now - timedelta(minutes=age_min)), _iso(now)),
    )
    conn.commit()


def _run(coro):
    return asyncio.run(coro)


class TestStaleQuery:
    def test_stale_running_enrichment_found(self, temp_db):
        conn = sqlite3.connect(temp_db)
        conn.row_factory = sqlite3.Row
        _insert_job(conn, "stale-1", heartbeat_age_min=30)
        from shared import auto_resume
        with patch("shared.auto_resume.db") as mock_db:
            mock_db.get_db.return_value = conn
            jobs = auto_resume.get_heartbeat_stale_running_enrichment_jobs(10)
        assert [j["job_id"] for j in jobs] == ["stale-1"]

    def test_fresh_job_not_stale(self, temp_db):
        conn = sqlite3.connect(temp_db)
        conn.row_factory = sqlite3.Row
        _insert_job(conn, "fresh-1", heartbeat_age_min=2)
        from shared import auto_resume
        with patch("shared.auto_resume.db") as mock_db:
            mock_db.get_db.return_value = conn
            jobs = auto_resume.get_heartbeat_stale_running_enrichment_jobs(10)
        assert jobs == []

    def test_scraper_jobs_excluded(self, temp_db):
        conn = sqlite3.connect(temp_db)
        conn.row_factory = sqlite3.Row
        _insert_job(conn, "scr-1", job_type="scraper", heartbeat_age_min=30)
        from shared import auto_resume
        with patch("shared.auto_resume.db") as mock_db:
            mock_db.get_db.return_value = conn
            jobs = auto_resume.get_heartbeat_stale_running_enrichment_jobs(10)
        assert jobs == []

    def test_young_job_with_null_heartbeat_excluded(self, temp_db):
        """A job created <threshold minutes ago may legitimately not have its
        first heartbeat yet (CSV load / dedupe) — never reap it."""
        conn = sqlite3.connect(temp_db)
        conn.row_factory = sqlite3.Row
        now = datetime.now(timezone.utc)
        conn.execute(
            """INSERT INTO jobs (job_id, user_id, job_type, status, last_heartbeat, created_at, updated_at)
               VALUES ('young-1', 'u1', 'enrichment', 'running', NULL, ?, ?)""",
            (_iso(now - timedelta(minutes=5)), _iso(now)),
        )
        conn.commit()
        from shared import auto_resume
        with patch("shared.auto_resume.db") as mock_db:
            mock_db.get_db.return_value = conn
            jobs = auto_resume.get_heartbeat_stale_running_enrichment_jobs(10)
        assert jobs == []


class TestGuardOnce:
    def test_reaps_stale_and_resumes(self, temp_db):
        conn = sqlite3.connect(temp_db)
        conn.row_factory = sqlite3.Row
        _insert_job(conn, "stale-1", heartbeat_age_min=30)
        _insert_job(conn, "fresh-1", heartbeat_age_min=2)
        from shared import auto_resume
        resumed = []
        # Force the switch constants: sibling tests reload this module with
        # mutated env, leaving stale constants behind (suite-order pollution).
        with patch("shared.auto_resume.db") as mock_db, patch.object(
            auto_resume, "AUTO_RESUME_ENABLED", True
        ), patch.object(
            auto_resume, "ENABLE_ENRICHMENT_RUNTIME_GUARD", True
        ), patch.object(
            auto_resume, "resume_one", side_effect=lambda jid, uid: resumed.append(jid)
        ):
            mock_db.get_db.return_value = conn
            reaped = _run(auto_resume.enrichment_runtime_guard_once())
        assert reaped == 1
        assert resumed == ["stale-1"]
        row = conn.execute("SELECT status FROM jobs WHERE job_id='stale-1'").fetchone()
        assert row[0] == "abandoned"
        row = conn.execute("SELECT status FROM jobs WHERE job_id='fresh-1'").fetchone()
        assert row[0] == "running"

    def test_lost_claim_skips_resume(self, temp_db):
        """Another worker flipped the row first (or it finished in the gap):
        no resume attempt from THIS pass."""
        conn = sqlite3.connect(temp_db)
        conn.row_factory = sqlite3.Row
        _insert_job(conn, "stale-1", heartbeat_age_min=30)
        from shared import auto_resume
        with patch("shared.auto_resume.db") as mock_db, patch.object(
            auto_resume, "AUTO_RESUME_ENABLED", True
        ), patch.object(
            auto_resume, "ENABLE_ENRICHMENT_RUNTIME_GUARD", True
        ), patch.object(
            auto_resume, "try_claim_abandoned", return_value=False
        ), patch.object(
            auto_resume, "resume_one", side_effect=AssertionError("must not resume")
        ):
            mock_db.get_db.return_value = conn
            reaped = _run(auto_resume.enrichment_runtime_guard_once())
        assert reaped == 0

    def test_kill_switch_off_does_nothing(self, temp_db):
        from shared import auto_resume
        with patch.object(auto_resume, "ENABLE_ENRICHMENT_RUNTIME_GUARD", False), patch.object(
            auto_resume,
            "get_heartbeat_stale_running_enrichment_jobs",
            side_effect=AssertionError("must not query"),
        ):
            reaped = _run(auto_resume.enrichment_runtime_guard_once())
        assert reaped == 0

    def test_auto_resume_disabled_disables_guard_too(self):
        """AUTO_RESUME_ENABLED=false (global kill-switch) must silence the
        guard: reaping without resume would straddle the two-switch contract."""
        from shared import auto_resume
        with patch.object(auto_resume, "AUTO_RESUME_ENABLED", False), patch.object(
            auto_resume,
            "get_heartbeat_stale_running_enrichment_jobs",
            side_effect=AssertionError("must not query"),
        ):
            reaped = _run(auto_resume.enrichment_runtime_guard_once())
        assert reaped == 0
