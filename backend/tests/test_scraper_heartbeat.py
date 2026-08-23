"""
Tests that the scraper job runners write heartbeats and that ``cleanup_stale_jobs``
uses the heartbeat-aware reaper.

Background: gunicorn workers recycle frequently (``--max-requests 1500``) and any
worker boot runs ``cleanup_stale_jobs()``. The OLD scraper cleanup used the
unconditional reaper, so a sibling-worker boot abandoned a live scraper job even
though it was running fine in another worker. The fix: the runners now write
``last_heartbeat`` every 30s (plus an initial beat) and the cleanup uses
``get_stale_running_jobs_by_heartbeat()``, which spares a job whose heartbeat is
fresh. Mirrors enrichment/routes.py and phone_enrichment/routes.py.
"""
from __future__ import annotations

import asyncio
import sqlite3
import tempfile
from pathlib import Path
from unittest import mock

import pytest

from scraper import routes
from scraper.job_store import ScraperJobStore


@pytest.fixture
def _mocked_scraper_runner(tmp_path):
    """Mock the external deps of ``_run_job``; yield (store, tmp_path)."""
    store = mock.MagicMock()
    for m in ("set_running", "heartbeat", "set_done", "set_failed", "append_event",
              "write_task_checkpoint", "update_result_count", "is_job_cancelled"):
        setattr(store, m, mock.MagicMock())
    # The cache-write block tolerates a missing job row (it is wrapped in
    # try/except). Returning None keeps that block quiet.
    store.get_job.return_value = None

    async def fake_crawl(*args, **kwargs):
        # Yield a few times so the heartbeat background task can fire.
        for _ in range(3):
            await asyncio.sleep(0)
        return 7

    # A fast stand-in for asyncio.sleep that still yields control (via the real
    # sleep(0)). An AsyncMock would NOT yield, so the heartbeat loop would never
    # get scheduled and the recurring beats would never fire.
    _orig_sleep = asyncio.sleep

    async def _fast_sleep(_seconds):
        await _orig_sleep(0)

    patches = [
        mock.patch.object(routes.job_store, "get_store", return_value=store),
        mock.patch.object(routes.crawler_module, "run_crawl", fake_crawl),
        # Make the 30s heartbeat interval instant so the loop fires in-test.
        mock.patch.object(asyncio, "sleep", new=_fast_sleep),
    ]
    for p in patches:
        p.start()
    yield store, tmp_path
    for p in patches:
        p.stop()


def _run_once(store, tmp_path, job_id):
    """Drive ``_run_job`` with mocked deps and a throwaway output path."""
    routes._job_signals[job_id] = asyncio.Event()
    routes._active_jobs.add(job_id)
    try:
        asyncio.run(routes._run_job(
            job_id=job_id,
            user_id="u1",
            is_admin=True,  # skip daily-quota accounting
            query="test query",
            filtered_centers=[],
            api_key="key",
            output_path=tmp_path / "out.csv",
        ))
    finally:
        routes._active_jobs.discard(job_id)
        routes._job_signals.pop(job_id, None)


def test_scraper_run_job_writes_heartbeat(_mocked_scraper_runner):
    """``_run_job`` must write an initial heartbeat + recurring beats, and leave
    the job lifecycle (set_running -> set_done) intact."""
    store, tmp_path = _mocked_scraper_runner
    _run_once(store, tmp_path, "scraper-heartbeat-1")

    # Initial heartbeat + at least one recurring beat must fire (the core fix).
    assert store.heartbeat.call_count >= 2, (
        f"heartbeat only fired {store.heartbeat.call_count} time(s); "
        "expected initial + recurring loop"
    )
    # Lifecycle intact; a failed job would be a regression.
    assert store.set_running.called
    assert store.set_done.called
    assert not store.set_failed.called


def test_scraper_heartbeat_set_before_long_work(_mocked_scraper_runner):
    """The initial heartbeat must precede any crawl work so the job is never
    eligible for reaping during its first minutes."""
    store, tmp_path = _mocked_scraper_runner
    call_order: list[str] = []
    store.set_running.side_effect = lambda *a, **k: call_order.append("set_running")
    store.heartbeat.side_effect = lambda *a, **k: call_order.append("heartbeat")

    _run_once(store, tmp_path, "scraper-heartbeat-2")

    # set_running first, then the initial heartbeat, before run_crawl runs.
    assert call_order[:2] == ["set_running", "heartbeat"]


@pytest.fixture
def _reaper_db():
    """Real temp DB with a jobs table; insert jobs in varying staleness states."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE jobs (
            job_id TEXT PRIMARY KEY,
            job_type TEXT DEFAULT 'scraper',
            status TEXT,
            created_at TEXT,
            updated_at TEXT,
            error TEXT,
            last_heartbeat TEXT
        );
        INSERT INTO jobs (job_id, job_type, status, created_at, last_heartbeat) VALUES
            ('stale-job',  'scraper', 'running', datetime('now','-1 hour'),  datetime('now','-30 minutes')),
            ('fresh-job',  'scraper', 'running', datetime('now','-1 hour'),  datetime('now')),
            ('young-job',  'scraper', 'running', datetime('now'),            NULL),
            ('other-type', 'enrichment', 'running', datetime('now','-1 hour'), datetime('now','-30 minutes')),
            ('done-job',   'scraper', 'done',    datetime('now','-1 hour'),  NULL);
        """
    )
    conn.commit()
    store = ScraperJobStore(conn)
    try:
        yield store, conn
    finally:
        conn.close()
        Path(db_path).unlink(missing_ok=True)


def test_cleanup_stale_jobs_uses_heartbeat(_reaper_db):
    """Only jobs with a stale (>2 min) heartbeat AND older than 3 min are
    abandoned. A live, fresh-heartbeat job is spared — the core regression
    guard. If cleanup is reverted to the unconditional reaper, fresh-job would
    be abandoned and this assertion fails."""
    store, conn = _reaper_db
    with mock.patch.object(routes.job_store, "get_store", return_value=store):
        routes.cleanup_stale_jobs()

    statuses = {r["job_id"]: r["status"] for r in
                conn.execute("SELECT job_id, status FROM jobs").fetchall()}

    assert statuses["stale-job"] == "abandoned", "stale-heartbeat running job must be reaped"
    assert statuses["fresh-job"] == "running", "fresh-heartbeat job must be spared"
    assert statuses["young-job"] == "running", "job younger than 3 min must be spared"
    assert statuses["other-type"] == "running", "scraper reaper must not touch enrichment jobs"
    assert statuses["done-job"] == "done", "non-running job must be untouched"
