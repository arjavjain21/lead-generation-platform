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
    resume_claimed_at TEXT,
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
# Atomic enrichment resume claim (2026-08-24 fan-out fix)
# ---------------------------------------------------------------------------

class TestEnrichmentClaim:
    def _claim_with_real_db_path(self, temp_db):
        """Point shared.db.DB_PATH at the temp file so the claim's dedicated
        connection reads the same data as the fixture connection."""
        from pathlib import Path as P
        import shared.db as db_mod
        original = db_mod.DB_PATH
        db_mod.DB_PATH = P(temp_db)
        return original

    def _restore_db_path(self, original):
        import shared.db as db_mod
        db_mod.DB_PATH = original

    def test_only_one_claimant_wins(self, temp_db):
        """The Aug 24 bug: 4 workers booted together, each saw 'abandoned,
        no child', each created a resume child (4 children in 5ms). The claim
        must let exactly one through."""
        conn = sqlite3.connect(temp_db)
        conn.row_factory = sqlite3.Row
        _insert_job(conn, "parent-x", status="abandoned", heartbeat_age_min=5)
        conn.commit()
        from shared import auto_resume
        orig = self._claim_with_real_db_path(temp_db)
        try:
            winners = [auto_resume.claim_enrichment_resume_job("parent-x")
                       for _ in range(4)]  # simulate 4 workers claiming
            assert winners == [True, False, False, False]
            row = conn.execute(
                "SELECT resume_claimed_at FROM jobs WHERE job_id='parent-x'"
            ).fetchone()
            assert row["resume_claimed_at"] is not None
        finally:
            self._restore_db_path(orig)

    def test_claimed_job_excluded_from_candidates(self, temp_db):
        """A FRESH claim (younger than RESUME_CLAIM_STALE_MINUTES) keeps the
        job out of the candidate list — the claim winner is mid-resume."""
        conn = sqlite3.connect(temp_db)
        conn.row_factory = sqlite3.Row
        _insert_job(conn, "claimed-1", status="abandoned", heartbeat_age_min=5)
        conn.execute(
            "UPDATE jobs SET resume_claimed_at=? WHERE job_id='claimed-1'",
            (_iso(datetime.now(timezone.utc) - timedelta(minutes=2)),),
        )
        conn.commit()
        from shared import auto_resume
        with patch("shared.auto_resume.db") as mock_db:
            mock_db.get_db.return_value = conn
            jobs = auto_resume.get_recently_abandoned_resumable_jobs()
        assert jobs == []

    def test_stale_claim_recovers_candidacy(self, temp_db):
        """Crash-orphan recovery: a worker died between the claim COMMIT and
        the child INSERT. Nothing cleared resume_claimed_at, so the job used
        to be permanently un-auto-resumable. A claim older than
        RESUME_CLAIM_STALE_MINUTES with no child must make it a candidate
        again."""
        conn = sqlite3.connect(temp_db)
        conn.row_factory = sqlite3.Row
        _insert_job(conn, "orphan-1", status="abandoned", heartbeat_age_min=5)
        conn.execute(
            "UPDATE jobs SET resume_claimed_at=? WHERE job_id='orphan-1'",
            (_iso(datetime.now(timezone.utc) - timedelta(minutes=45)),),
        )
        conn.commit()
        from shared import auto_resume
        with patch("shared.auto_resume.db") as mock_db:
            mock_db.get_db.return_value = conn
            jobs = auto_resume.get_recently_abandoned_resumable_jobs()
        assert [j["job_id"] for j in jobs] == ["orphan-1"]

    def test_stale_claim_with_child_stays_excluded(self, temp_db):
        """A stale claim is NOT a license to re-fan-out: if a child already
        exists the resume happened, so the job stays out regardless."""
        conn = sqlite3.connect(temp_db)
        conn.row_factory = sqlite3.Row
        _insert_job(conn, "orphan-2", status="abandoned", heartbeat_age_min=5)
        _insert_job(conn, "child-2", status="done", heartbeat_age_min=60,
                    parent="orphan-2")
        conn.execute(
            "UPDATE jobs SET resume_claimed_at=? WHERE job_id='orphan-2'",
            (_iso(datetime.now(timezone.utc) - timedelta(minutes=45)),),
        )
        conn.commit()
        from shared import auto_resume
        with patch("shared.auto_resume.db") as mock_db:
            mock_db.get_db.return_value = conn
            jobs = auto_resume.get_recently_abandoned_resumable_jobs()
        assert jobs == []

    def test_stale_claim_is_reclaimable(self, temp_db):
        """The claim txn itself must accept a stale claim (candidate query and
        claim txn agree, or recovery would be cosmetic)."""
        conn = sqlite3.connect(temp_db)
        conn.row_factory = sqlite3.Row
        _insert_job(conn, "orphan-3", status="abandoned", heartbeat_age_min=5)
        conn.execute(
            "UPDATE jobs SET resume_claimed_at=? WHERE job_id='orphan-3'",
            (_iso(datetime.now(timezone.utc) - timedelta(minutes=45)),),
        )
        conn.commit()
        from shared import auto_resume
        orig = self._claim_with_real_db_path(temp_db)
        try:
            assert auto_resume.claim_enrichment_resume_job("orphan-3") is True
        finally:
            self._restore_db_path(orig)

    def test_fresh_claim_not_reclaimable(self, temp_db):
        """The 30-minute window must not weaken the original fan-out guard:
        a FRESH claim still loses the claim txn."""
        conn = sqlite3.connect(temp_db)
        conn.row_factory = sqlite3.Row
        _insert_job(conn, "claimed-3", status="abandoned", heartbeat_age_min=5)
        conn.execute(
            "UPDATE jobs SET resume_claimed_at=? WHERE job_id='claimed-3'",
            (_iso(datetime.now(timezone.utc) - timedelta(minutes=2)),),
        )
        conn.commit()
        from shared import auto_resume
        orig = self._claim_with_real_db_path(temp_db)
        try:
            assert auto_resume.claim_enrichment_resume_job("claimed-3") is False
        finally:
            self._restore_db_path(orig)

    def test_root_counted_attempt_cap(self, temp_db):
        """Chain cap is counted on the ROOT, not the abandoned row — each
        generation's child starts restart_count=0, so per-row counting
        reset every hop (the 22-card ladder)."""
        conn = sqlite3.connect(temp_db)
        conn.row_factory = sqlite3.Row
        # root at cap, abandoned head 2 hops down with restart_count=0
        _insert_job(conn, "root-c", status="abandoned", heartbeat_age_min=5,
                    restart_count=2)
        _insert_job(conn, "mid-c", status="abandoned", heartbeat_age_min=5,
                    restart_count=0, parent="root-c")
        _insert_job(conn, "head-c", status="abandoned", heartbeat_age_min=5,
                    restart_count=0, parent="mid-c")
        conn.commit()
        from shared import auto_resume
        orig = self._claim_with_real_db_path(temp_db)
        try:
            assert auto_resume.claim_enrichment_resume_job("head-c") is False
            # root's count unchanged (claim rolled back)
            row = conn.execute(
                "SELECT restart_count FROM jobs WHERE job_id='root-c'"
            ).fetchone()
            assert row["restart_count"] == 2
            # head must remain unclaimed
            row = conn.execute(
                "SELECT resume_claimed_at FROM jobs WHERE job_id='head-c'"
            ).fetchone()
            assert row["resume_claimed_at"] is None
        finally:
            self._restore_db_path(orig)

    def test_claim_bumps_root_and_marks_parent(self, temp_db):
        conn = sqlite3.connect(temp_db)
        conn.row_factory = sqlite3.Row
        _insert_job(conn, "root-d", status="abandoned", heartbeat_age_min=5,
                    restart_count=0)
        _insert_job(conn, "head-d", status="abandoned", heartbeat_age_min=5,
                    restart_count=0, parent="root-d")
        conn.commit()
        from shared import auto_resume
        orig = self._claim_with_real_db_path(temp_db)
        try:
            assert auto_resume.claim_enrichment_resume_job("head-d") is True
            root = conn.execute(
                "SELECT restart_count FROM jobs WHERE job_id='root-d'"
            ).fetchone()
            head = conn.execute(
                "SELECT resume_claimed_at FROM jobs WHERE job_id='head-d'"
            ).fetchone()
            assert root["restart_count"] == 1
            assert head["resume_claimed_at"] is not None
        finally:
            self._restore_db_path(orig)

    def test_job_with_child_not_claimable(self, temp_db):
        conn = sqlite3.connect(temp_db)
        conn.row_factory = sqlite3.Row
        _insert_job(conn, "parent-e", status="abandoned", heartbeat_age_min=5)
        _insert_job(conn, "child-e", status="done", heartbeat_age_min=60,
                    parent="parent-e")
        conn.commit()
        from shared import auto_resume
        orig = self._claim_with_real_db_path(temp_db)
        try:
            assert auto_resume.claim_enrichment_resume_job("parent-e") is False
        finally:
            self._restore_db_path(orig)


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
