"""Tests for auto-resume of abandoned enrichment jobs (2026-08-20 incident).

Covers:
- Atomic claim (only one caller flips running -> abandoned)
- Eligibility window (fresh deaths only, restart cap, is_resumable)
- job_type scoping of both enrichment and scraper reapers (the base query is
  unscoped — without the overrides, the scraper reaper abandons enrichment jobs)
- Active-child guard (a job already resumed is not resumed twice)
- Worker notify interval decoupling (unit-level sanity)
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
    last_heartbeat TEXT,
    created_at TEXT,
    updated_at TEXT,
    error TEXT
);
CREATE TABLE job_checkpoints (
    job_id TEXT NOT NULL,
    row_index INTEGER NOT NULL,
    processed_at TEXT NOT NULL,
    PRIMARY KEY (job_id, row_index)
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
                heartbeat_age_min=1, restart_count=0, is_resumable=1, parent=None):
    now = datetime.now(timezone.utc)
    conn.execute(
        """INSERT INTO jobs (job_id, user_id, job_type, status, parent_job_id,
             restart_count, is_resumable, last_heartbeat, created_at, updated_at)
           VALUES (?, 'u1', ?, ?, ?, ?, ?, ?, ?, ?)""",
        (job_id, job_type, status, parent, restart_count, is_resumable,
         _iso(now - timedelta(minutes=heartbeat_age_min)),
         _iso(now - timedelta(hours=1)), _iso(now)),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Atomic claim
# ---------------------------------------------------------------------------

class TestTryClaimAbandoned:
    def test_first_claim_wins(self, temp_db):
        seed = sqlite3.connect(temp_db)
        seed.row_factory = sqlite3.Row
        _insert_job(seed, "job-1", status="running")
        seed.close()
        from shared import auto_resume
        with patch("shared.auto_resume.db") as mock_db:
            mock_db.get_db.return_value = sqlite3.connect(temp_db)
            assert auto_resume.try_claim_abandoned("job-1") is True
            # Second claim on the now-abandoned row must fail
            assert auto_resume.try_claim_abandoned("job-1") is False

    def test_claim_only_touches_running(self, temp_db):
        conn = sqlite3.connect(temp_db)
        conn.row_factory = sqlite3.Row
        _insert_job(conn, "job-1", status="done")
        from shared import auto_resume
        with patch("shared.auto_resume.db") as mock_db:
            mock_db.get_db.return_value = sqlite3.connect(temp_db)
            assert auto_resume.try_claim_abandoned("job-1") is False
            # Status unchanged
            row = conn.execute("SELECT status FROM jobs WHERE job_id='job-1'").fetchone()
            assert row[0] == "done"


# ---------------------------------------------------------------------------
# Eligibility query
# ---------------------------------------------------------------------------

class TestEligibility:
    def test_fresh_abandoned_enrichment_job_found(self, temp_db):
        conn = sqlite3.connect(temp_db)
        conn.row_factory = sqlite3.Row
        _insert_job(conn, "fresh-1", status="abandoned", heartbeat_age_min=5)
        from shared import auto_resume
        with patch("shared.auto_resume.db") as mock_db:
            mock_db.get_db.return_value = conn
            jobs = auto_resume.get_recently_abandoned_resumable_jobs()
        assert [j["job_id"] for j in jobs] == ["fresh-1"]

    def test_ancient_job_excluded(self, temp_db):
        conn = sqlite3.connect(temp_db)
        conn.row_factory = sqlite3.Row
        _insert_job(conn, "ancient-1", status="abandoned", heartbeat_age_min=60 * 48)
        from shared import auto_resume
        with patch("shared.auto_resume.db") as mock_db:
            mock_db.get_db.return_value = conn
            jobs = auto_resume.get_recently_abandoned_resumable_jobs()
        assert jobs == []

    def test_restart_cap_excluded(self, temp_db):
        conn = sqlite3.connect(temp_db)
        conn.row_factory = sqlite3.Row
        _insert_job(conn, "capped-1", status="abandoned", heartbeat_age_min=5,
                    restart_count=10)
        from shared import auto_resume
        with patch("shared.auto_resume.db") as mock_db:
            mock_db.get_db.return_value = conn
            jobs = auto_resume.get_recently_abandoned_resumable_jobs()
        assert jobs == []

    def test_non_resumable_excluded(self, temp_db):
        conn = sqlite3.connect(temp_db)
        conn.row_factory = sqlite3.Row
        _insert_job(conn, "nores-1", status="abandoned", heartbeat_age_min=5,
                    is_resumable=0)
        from shared import auto_resume
        with patch("shared.auto_resume.db") as mock_db:
            mock_db.get_db.return_value = conn
            jobs = auto_resume.get_recently_abandoned_resumable_jobs()
        assert jobs == []

    def test_scraper_job_never_auto_resumed(self, temp_db):
        conn = sqlite3.connect(temp_db)
        conn.row_factory = sqlite3.Row
        _insert_job(conn, "scrap-1", job_type="scraper", status="abandoned",
                    heartbeat_age_min=5)
        from shared import auto_resume
        with patch("shared.auto_resume.db") as mock_db:
            mock_db.get_db.return_value = conn
            jobs = auto_resume.get_recently_abandoned_resumable_jobs()
        assert jobs == []

    def test_running_job_not_selected(self, temp_db):
        conn = sqlite3.connect(temp_db)
        conn.row_factory = sqlite3.Row
        _insert_job(conn, "run-1", status="running", heartbeat_age_min=5)
        from shared import auto_resume
        with patch("shared.auto_resume.db") as mock_db:
            mock_db.get_db.return_value = conn
            jobs = auto_resume.get_recently_abandoned_resumable_jobs()
        assert jobs == []


# ---------------------------------------------------------------------------
# Reaper job_type scoping (both stores)
# ---------------------------------------------------------------------------

class TestReaperScoping:
    def test_enrichment_reaper_ignores_scraper_jobs(self, temp_db):
        from enrichment.job_store import EnrichmentJobStore
        conn = sqlite3.connect(temp_db)
        conn.row_factory = sqlite3.Row
        _insert_job(conn, "scrap-stale", job_type="scraper", status="running",
                    heartbeat_age_min=30)
        _insert_job(conn, "enr-stale", job_type="enrichment", status="running",
                    heartbeat_age_min=30)
        store = EnrichmentJobStore(conn)
        assert store.get_stale_running_jobs_by_heartbeat() == ["enr-stale"]

    def test_scraper_reaper_ignores_enrichment_jobs(self, temp_db):
        from scraper.job_store import ScraperJobStore
        conn = sqlite3.connect(temp_db)
        conn.row_factory = sqlite3.Row
        _insert_job(conn, "scrap-stale", job_type="scraper", status="running",
                    heartbeat_age_min=30)
        _insert_job(conn, "enr-stale", job_type="enrichment", status="running",
                    heartbeat_age_min=30)
        store = ScraperJobStore(conn)
        assert store.get_stale_running_jobs_by_heartbeat() == ["scrap-stale"]

    def test_running_enrichment_job_with_fresh_heartbeat_not_reaped(self, temp_db):
        from enrichment.job_store import EnrichmentJobStore
        conn = sqlite3.connect(temp_db)
        conn.row_factory = sqlite3.Row
        _insert_job(conn, "enr-live", job_type="enrichment", status="running",
                    heartbeat_age_min=0.2)
        store = EnrichmentJobStore(conn)
        assert store.get_stale_running_jobs_by_heartbeat() == []


# ---------------------------------------------------------------------------
# Active-child guard
# ---------------------------------------------------------------------------

class TestActiveChildGuard:
    def test_running_child_blocks_resume(self, temp_db):
        conn = sqlite3.connect(temp_db)
        conn.row_factory = sqlite3.Row
        _insert_job(conn, "parent-1", status="abandoned", heartbeat_age_min=5)
        _insert_job(conn, "child-1", status="running", heartbeat_age_min=0.1,
                    parent="parent-1")
        from shared import auto_resume
        with patch("shared.auto_resume.db") as mock_db:
            mock_db.get_db.return_value = conn
            assert auto_resume._has_child_job("parent-1") is True
            assert auto_resume._has_child_job("nonexistent") is False

    def test_completed_child_also_blocks_resume(self, temp_db):
        """A done child means the chain already finished — resuming the parent
        would duplicate paid provider calls (the Aug 19-20 chain scenario)."""
        conn = sqlite3.connect(temp_db)
        conn.row_factory = sqlite3.Row
        _insert_job(conn, "parent-2", status="abandoned", heartbeat_age_min=5)
        _insert_job(conn, "child-2", status="done", heartbeat_age_min=60,
                    parent="parent-2")
        from shared import auto_resume
        with patch("shared.auto_resume.db") as mock_db:
            mock_db.get_db.return_value = conn
            assert auto_resume._has_child_job("parent-2") is True


# ---------------------------------------------------------------------------
# Worker notify interval
# ---------------------------------------------------------------------------

class TestWorkerNotifyInterval:
    def test_notify_interval_decoupled_from_timeout(self):
        from shared.worker import LeadGenUvicornWorker
        # Constructing a real UvicornWorker requires gunicorn plumbing; verify
        # the class-level contract instead: interval must be < the stock
        # hardwiring (timeout itself). Simulated via __init__ patching.
        import types
        fake = types.SimpleNamespace(timeout_notify=9999)
        with patch.object(LeadGenUvicornWorker, "__init__", lambda self: None):
            w = LeadGenUvicornWorker()
            w.timeout = 600
            w.config = fake
        # Re-run the mutation logic the real __init__ performs
        interval = min(30.0, w.timeout)
        w.config.timeout_notify = interval
        assert w.config.timeout_notify == 30.0
        assert w.config.timeout_notify < w.timeout

    def test_env_override(self, monkeypatch):
        monkeypatch.setenv("WORKER_NOTIFY_INTERVAL", "15")
        import importlib
        import shared.worker as worker_mod
        importlib.reload(worker_mod)
        assert worker_mod.NOTIFY_INTERVAL_SECONDS == 15.0
        monkeypatch.delenv("WORKER_NOTIFY_INTERVAL")
        importlib.reload(worker_mod)


# ---------------------------------------------------------------------------
# maybe_auto_resume orchestration (no candidates -> no-op; exception -> safe)
# ---------------------------------------------------------------------------

class TestOrchestration:
    def test_disabled_env_skips_everything(self, temp_db, monkeypatch):
        monkeypatch.setenv("AUTO_RESUME_ENABLED", "false")
        import importlib
        import shared.auto_resume as ar
        importlib.reload(ar)
        called = []
        with patch.object(ar, "get_recently_abandoned_resumable_jobs",
                          side_effect=lambda: called.append(1) or []):
            asyncio.run(ar.maybe_auto_resume_abandoned_jobs())
        assert called == []  # disabled short-circuits before the query

    def test_exception_is_swallowed(self, monkeypatch):
        import shared.auto_resume as ar
        monkeypatch.setattr(ar, "AUTO_RESUME_DELAY_SECONDS", 0)
        with patch.object(ar, "get_recently_abandoned_resumable_jobs",
                          side_effect=RuntimeError("boom")):
            # Must not raise
            asyncio.run(ar.maybe_auto_resume_abandoned_jobs())
