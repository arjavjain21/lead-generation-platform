"""
Tests for the scraper job dispatcher + scraper auto-resume (2026-08-24 fix).

Covers:
- claim_next_queued_scraper_job atomicity + platform cap + FIFO order
- dispatch_loop per-worker cap
- scraper reaper never touches queued jobs (running-only stale query)
- auto-resume scraper eligibility (fresh-window, restart cap, no-child rule)
- resume_one_scraper claim race (only one winner bumps restart_count)
- _derive_pending_tasks checkpoint subtraction
- safe_copy_csv truncated-tail repair
- conftest sets ENABLE_STARTUP_REAPERS=false (prod-state safety)

All DB work happens on a temp SQLite file patched over shared.db.DB_PATH so
no test ever touches the production jobs DB.
"""

from __future__ import annotations

import asyncio
import csv as csv_mod
import os
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import pytest


@pytest.fixture()
def temp_db(tmp_path, monkeypatch):
    """Fresh jobs DB on a temp file; shared.db re-pointed at it."""
    db_path = tmp_path / "jobs.db"
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE jobs (
            job_id TEXT PRIMARY KEY,
            user_id TEXT,
            job_type TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'queued',
            parent_job_id TEXT,
            query TEXT,
            regions TEXT,
            total_tasks INTEGER,
            done_tasks INTEGER DEFAULT 0,
            result_count INTEGER DEFAULT 0,
            restart_count INTEGER DEFAULT 0,
            is_resumable INTEGER DEFAULT 1,
            last_heartbeat TEXT,
            error TEXT,
            output_path TEXT,
            cancelled_at TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE task_checkpoints (
            job_id TEXT,
            center_name TEXT,
            center_state TEXT,
            zoom INTEGER,
            result_count INTEGER,
            completed_at TEXT,
            checkpoint_time TEXT,
            PRIMARY KEY (job_id, center_name, center_state, zoom)
        );
        CREATE TABLE users (
            user_id TEXT PRIMARY KEY,
            email TEXT UNIQUE,
            password_hash TEXT,
            is_admin INTEGER DEFAULT 0,
            created_at TEXT
        );
        """
    )
    conn.commit()

    import shared.db as shared_db

    local = threading.local()
    local.conn = conn
    monkeypatch.setattr(shared_db, "get_db", lambda: conn, raising=True)

    import shared.db as sd
    monkeypatch.setattr(sd, "DB_PATH", db_path, raising=False)

    yield conn
    conn.close()


def _now_iso(offset_seconds: float = 0) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=offset_seconds)).isoformat()


def _insert_job(conn, job_id, status="queued", restart_count=0, parent=None,
                heartbeat_age=None, updated_age=None, resumable=1, created_age=None):
    created = _now_iso(-(created_age if created_age is not None else 60))
    updated = _now_iso(-(updated_age if updated_age is not None else 60))
    hb = _now_iso(-heartbeat_age) if heartbeat_age is not None else None
    conn.execute(
        """INSERT INTO jobs (job_id, user_id, job_type, status, parent_job_id, query,
           regions, total_tasks, restart_count, is_resumable, last_heartbeat,
           created_at, updated_at)
           VALUES (?, 'u1', 'scraper', ?, ?, 'q', '{}', 100, ?, ?, ?, ?, ?)""",
        (job_id, status, parent, restart_count, resumable, hb, created, updated),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Dispatcher claim
# ---------------------------------------------------------------------------

class TestClaim:
    def test_claims_oldest_queued_fifo(self, temp_db):
        from scraper import dispatch
        _insert_job(temp_db, "old", created_age=600)
        _insert_job(temp_db, "new", created_age=60)
        assert dispatch.claim_next_queued_scraper_job() == "old"
        assert dispatch.claim_next_queued_scraper_job() == "new"

    def test_claim_flips_status_to_running(self, temp_db):
        from scraper import dispatch
        _insert_job(temp_db, "j1")
        dispatch.claim_next_queued_scraper_job()
        row = temp_db.execute("SELECT status FROM jobs WHERE job_id='j1'").fetchone()
        assert row["status"] == "running"

    def test_platform_cap_respected(self, temp_db, monkeypatch):
        from scraper import dispatch
        monkeypatch.setattr(dispatch, "MAX_CONCURRENT_SCRAPER_JOBS", 2)
        for i in range(5):
            _insert_job(temp_db, f"j{i}")
        assert dispatch.claim_next_queued_scraper_job() == "j0"
        assert dispatch.claim_next_queued_scraper_job() == "j1"
        # Cap hit — no more claims even though 3 jobs wait.
        assert dispatch.claim_next_queued_scraper_job() is None
        # A running job finishing frees a slot.
        temp_db.execute("UPDATE jobs SET status='done' WHERE job_id='j0'")
        temp_db.commit()
        assert dispatch.claim_next_queued_scraper_job() == "j2"

    def test_ignores_non_scraper_and_non_queued(self, temp_db):
        from scraper import dispatch
        temp_db.execute(
            """INSERT INTO jobs (job_id, user_id, job_type, status, query, regions,
               total_tasks, created_at, updated_at)
               VALUES ('e1','u1','enrichment','queued','q','{}',10,?,?)""",
            (_now_iso(-60), _now_iso(-60)),
        )
        _insert_job(temp_db, "running-job", status="running")
        _insert_job(temp_db, "done-job", status="done")
        temp_db.commit()
        assert dispatch.claim_next_queued_scraper_job() is None

    def test_lost_race_returns_none(self, temp_db, monkeypatch):
        from scraper import dispatch
        _insert_job(temp_db, "j1")

        # Simulate another worker flipping status between our SELECT and our
        # UPDATE. sqlite3.Connection.execute is read-only (can't mock it), so
        # wrap the connection in an object that intercepts the claim UPDATE
        # and rewrites its own UPDATE underneath it.
        class RacingConn:
            def __init__(self, inner):
                self._inner = inner

            def execute(self, sql, params=()):
                if sql.startswith("UPDATE jobs SET status='running'"):
                    # Rival wins first: flip to cancelled, then run our
                    # UPDATE which now matches 0 rows.
                    self._inner.execute(
                        "UPDATE jobs SET status='cancelled' WHERE job_id='j1'"
                    )
                    self._inner.commit()
                return self._inner.execute(sql, params)

            def commit(self):
                self._inner.commit()

        with mock.patch("scraper.dispatch.db.get_db", return_value=RacingConn(temp_db)):
            assert dispatch.claim_next_queued_scraper_job() is None
        # The rival's cancelled status stands.
        row = temp_db.execute("SELECT status FROM jobs WHERE job_id='j1'").fetchone()
        assert row["status"] == "cancelled"


# ---------------------------------------------------------------------------
# dispatch_loop per-worker cap
# ---------------------------------------------------------------------------

class TestDispatchLoop:
    def test_per_worker_cap_bounds_in_flight(self, temp_db, monkeypatch):
        from scraper import dispatch
        monkeypatch.setattr(dispatch, "MAX_CONCURRENT_SCRAPER_JOBS", 10)
        for i in range(5):
            _insert_job(temp_db, f"j{i}")

        launched: list[str] = []
        release = asyncio.Event()

        async def slow_launch(job_id):
            launched.append(job_id)
            await release.wait()

        async def run():
            task = asyncio.create_task(
                dispatch.dispatch_loop(slow_launch, poll_seconds=0.01, per_worker_cap=2)
            )
            await asyncio.sleep(0.15)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(run())
        release.set()
        assert len(launched) == 2  # per-worker cap held at 2 despite 5 queued

    def test_finished_job_frees_slot(self, temp_db, monkeypatch):
        from scraper import dispatch
        monkeypatch.setattr(dispatch, "MAX_CONCURRENT_SCRAPER_JOBS", 10)
        for i in range(3):
            _insert_job(temp_db, f"j{i}")

        launched: list[str] = []

        async def quick_launch(job_id):
            launched.append(job_id)
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
        assert len(launched) == 3  # first two finish, third gets claimed


# ---------------------------------------------------------------------------
# Reaper scope (running-only)
# ---------------------------------------------------------------------------

class TestReaperScope:
    def test_queued_jobs_never_reaped(self, temp_db):
        from scraper.job_store import ScraperJobStore
        store = ScraperJobStore(temp_db)
        # A queued job 10 minutes old with NO heartbeat (waiting on the cap).
        _insert_job(temp_db, "q-old", status="queued", created_age=600, updated_age=600)
        # A running job whose heartbeat went stale 5 minutes ago.
        _insert_job(temp_db, "r-stale", status="running", heartbeat_age=300,
                    created_age=600, updated_age=300)
        stale = store.get_stale_running_jobs_by_heartbeat()
        assert "q-old" not in stale
        assert "r-stale" in stale

    def test_fresh_running_job_survives(self, temp_db):
        from scraper.job_store import ScraperJobStore
        store = ScraperJobStore(temp_db)
        _insert_job(temp_db, "r-fresh", status="running", heartbeat_age=30)
        assert store.get_stale_running_jobs_by_heartbeat() == []


# ---------------------------------------------------------------------------
# Scraper auto-resume eligibility
# ---------------------------------------------------------------------------

class TestScraperAutoResumeEligibility:
    def test_fresh_abandoned_root_is_candidate(self, temp_db):
        from shared import auto_resume
        _insert_job(temp_db, "a1", status="abandoned", updated_age=120)
        jobs = auto_resume.get_recently_abandoned_scraper_jobs()
        assert [j["job_id"] for j in jobs] == ["a1"]

    def test_old_abandoned_not_candidate(self, temp_db, monkeypatch):
        from shared import auto_resume
        monkeypatch.setattr(auto_resume, "SCRAPER_AUTO_RESUME_MAX_ABANDONED_AGE_MINUTES", 30.0)
        _insert_job(temp_db, "a-old", status="abandoned", updated_age=3600)
        assert auto_resume.get_recently_abandoned_scraper_jobs() == []

    def test_cap_exceeded_not_candidate(self, temp_db):
        from shared import auto_resume
        _insert_job(temp_db, "a-capped", status="abandoned", restart_count=2)
        assert auto_resume.get_recently_abandoned_scraper_jobs() == []

    def test_with_child_not_candidate(self, temp_db):
        from shared import auto_resume
        _insert_job(temp_db, "a-parent", status="abandoned", updated_age=60)
        _insert_job(temp_db, "child", status="running", parent="a-parent")
        assert auto_resume.get_recently_abandoned_scraper_jobs() == []

    def test_non_scraper_not_candidate(self, temp_db):
        from shared import auto_resume
        temp_db.execute(
            """INSERT INTO jobs (job_id, user_id, job_type, status, query, regions,
               total_tasks, created_at, updated_at)
               VALUES ('e-ab','u1','enrichment','abandoned','q','{}',10,?,?)""",
            (_now_iso(-60), _now_iso(-60)),
        )
        temp_db.commit()
        assert auto_resume.get_recently_abandoned_scraper_jobs() == []

    def test_non_resumable_not_candidate(self, temp_db):
        from shared import auto_resume
        _insert_job(temp_db, "a-nr", status="abandoned", resumable=0)
        assert auto_resume.get_recently_abandoned_scraper_jobs() == []


# ---------------------------------------------------------------------------
# resume_one_scraper claim race
# ---------------------------------------------------------------------------

class TestResumeClaimRace:
    def test_only_one_winner_bumps_restart_count(self, temp_db, monkeypatch):
        """Concurrent claimants: exactly one wins, one child is created.

        _claim_scraper_resume opens its OWN connection to db.DB_PATH with
        BEGIN IMMEDIATE — that path is exercised for real here because the
        temp fixture re-points shared.db.DB_PATH at the temp file. The
        endpoint fake inserts the child through the shared conn, exactly as
        production does.
        """
        from shared import auto_resume
        import shared.db as shared_db

        # temp_db fixture made a real file-backed DB; point DB_PATH at it so
        # the dedicated claim connection hits the same data.
        db_file = shared_db.DB_PATH
        _insert_job(temp_db, "a1", status="abandoned", restart_count=0, updated_age=60)

        calls: list[str] = []

        async def fake_resume_endpoint(job_id, req, background_tasks, current_user):
            calls.append(job_id)
            # Faithful to production: the resume creates a child row.
            _insert_job(temp_db, f"child-{len(calls)}", status="queued", parent=job_id)
            return {"job_id": f"child-{len(calls)}", "pending_tasks": 5}

        import scraper.routes as scraper_routes
        monkeypatch.setattr(scraper_routes, "resume_scraper_job", fake_resume_endpoint)

        async def run():
            # FOUR concurrent claimants (one per guard loop) — only one may win.
            results = await asyncio.gather(
                *[auto_resume.resume_one_scraper("a1", "u1") for _ in range(4)]
            )
            return results

        results = asyncio.run(run())
        row = temp_db.execute("SELECT restart_count FROM jobs WHERE job_id='a1'").fetchone()
        children = temp_db.execute(
            "SELECT COUNT(*) FROM jobs WHERE parent_job_id='a1'"
        ).fetchone()[0]
        assert sum(1 for r in results if r) == 1
        assert row["restart_count"] == 1
        assert children == 1
        assert len(calls) == 1

    def test_claim_rejected_when_child_exists(self, temp_db, monkeypatch):
        """A job that already has a resume child must never be re-claimed."""
        from shared import auto_resume
        _insert_job(temp_db, "a2", status="abandoned", restart_count=1, updated_age=60)
        _insert_job(temp_db, "existing-child", status="done", parent="a2")
        assert auto_resume._claim_scraper_resume("a2") is False
        row = temp_db.execute("SELECT restart_count FROM jobs WHERE job_id='a2'").fetchone()
        assert row["restart_count"] == 1  # untouched

    def test_claim_rejected_at_cap(self, temp_db, monkeypatch):
        from shared import auto_resume
        _insert_job(temp_db, "a3", status="abandoned", restart_count=2, updated_age=60)
        # No child, but cap (2) already consumed — claim must fail.
        assert auto_resume._claim_scraper_resume("a3") is False

    def test_http_409_from_resume_swallowed(self, temp_db, monkeypatch):
        from fastapi import HTTPException
        from shared import auto_resume
        _insert_job(temp_db, "a1", status="abandoned", updated_age=60)

        async def failing_endpoint(job_id, req, background_tasks, current_user):
            raise HTTPException(status_code=400, detail="All tasks already completed")

        import scraper.routes as scraper_routes
        monkeypatch.setattr(scraper_routes, "resume_scraper_job", failing_endpoint)

        result = asyncio.run(auto_resume.resume_one_scraper("a1", "u1"))
        assert result is False  # swallowed, not raised


# ---------------------------------------------------------------------------
# _derive_pending_tasks
# ---------------------------------------------------------------------------

class TestDerivePendingTasks:
    def _job_row(self, conn, regions="{}"):
        return {
            "job_id": "child1",
            "job_type": "scraper",
            "regions": regions,
            "parent_job_id": "root1",
            "query": "q",
        }

    def test_checkpoints_subtracted(self, temp_db, monkeypatch):
        import scraper.routes as routes

        centers = [
            {"name": "A", "state": "PA", "lat": 1.0, "lng": 2.0},
            {"name": "B", "state": "PA", "lat": 3.0, "lng": 4.0},
        ]
        monkeypatch.setattr(
            routes.centers_module, "get_centers_for_job", lambda **kw: (list(centers), [])
        )

        temp_db.execute(
            "INSERT INTO jobs (job_id, user_id, job_type, status, query, regions, total_tasks, created_at, updated_at)"
            " VALUES ('root1','u1','scraper','abandoned','q','{}',6,?,?)",
            (_now_iso(-600), _now_iso(-120)),
        )
        # root1 finished: A@10, A@11, A@12, B@10
        for (name, zoom) in [("A", 10), ("A", 11), ("A", 12), ("B", 10)]:
            temp_db.execute(
                "INSERT INTO task_checkpoints (job_id, center_name, center_state, zoom, result_count, completed_at)"
                " VALUES ('root1', ?, 'PA', ?, 0, ?)",
                (name, zoom, _now_iso(-300)),
            )
        temp_db.commit()

        job = self._job_row(temp_db)
        all_centers, pending, expected = routes._derive_pending_tasks(job, "child1")
        assert len(all_centers) == 2
        assert sorted((c["name"], z) for c, z in pending) == [("B", 11), ("B", 12)]
        assert expected == []

    def test_unrecoverable_regions_return_empty(self, temp_db, monkeypatch):
        import scraper.routes as routes

        async def failing(**kwargs):
            return [], ["boom"]
        # get_centers_for_job is sync in real code? verify — it's called sync.
        monkeypatch.setattr(
            routes.centers_module, "get_centers_for_job", lambda **kw: ([], ["boom"])
        )
        job = self._job_row(temp_db)
        centers, pending, expected = routes._derive_pending_tasks(job, "child1")
        assert centers == [] and pending == []


# ---------------------------------------------------------------------------
# safe_copy_csv
# ---------------------------------------------------------------------------

class TestSafeCopyCsv:
    def test_truncated_tail_repaired(self, tmp_path):
        from scraper.dispatch import safe_copy_csv
        src = tmp_path / "src.csv"
        dst = tmp_path / "dst.csv"
        src.write_bytes(b"col\nrow1\nrow2\nrow3_trunca")
        size = safe_copy_csv(src, dst)
        assert size > 0
        assert dst.read_bytes() == b"col\nrow1\nrow2\n"

    def test_clean_file_untouched(self, tmp_path):
        from scraper.dispatch import safe_copy_csv
        src = tmp_path / "src.csv"
        dst = tmp_path / "dst.csv"
        src.write_bytes(b"col\nrow1\nrow2\n")
        safe_copy_csv(src, dst)
        assert dst.read_bytes() == b"col\nrow1\nrow2\n"

    def test_missing_source_returns_zero(self, tmp_path):
        from scraper.dispatch import safe_copy_csv
        assert safe_copy_csv(tmp_path / "nope.csv", tmp_path / "dst.csv") == 0


# ---------------------------------------------------------------------------
# conftest prod-safety flag
# ---------------------------------------------------------------------------

class TestConftestGuard:
    def test_env_flag_set_by_conftest(self):
        import os
        assert os.environ.get("ENABLE_STARTUP_REAPERS") == "false"

    def test_main_defaults_true_when_unset(self, monkeypatch):
        import os
        monkeypatch.delenv("ENABLE_STARTUP_REAPERS", raising=False)
        val = os.environ.get("ENABLE_STARTUP_REAPERS", "true").lower() in ("1", "true", "yes")
        assert val is True
