"""
Tests that ``_run_linkedin_v2_job`` writes heartbeats so the LinkedIn Enrich
flow is no longer reaped as "stale/abandoned" after ~3 minutes.

Background: the stale-job reaper abandons running jobs whose ``last_heartbeat``
is NULL or older than 2 minutes. The Domain path writes a heartbeat every 30s;
the LinkedIn path historically did not, so healthy LinkedIn jobs were abandoned.
These tests lock in the fix (initial heartbeat + recurring loop) and confirm the
job lifecycle (set_running → set_done) stays intact.
"""
from __future__ import annotations

import asyncio
from unittest import mock

import pytest

from enrichment import routes


@pytest.fixture
def _mocked_linkedin_runner(tmp_path):
    """Mock external deps of ``_run_linkedin_v2_job``; yield the store mock."""
    store = mock.MagicMock()
    # Sync DB methods used by the runner
    for m in ("set_running", "heartbeat", "set_done", "set_failed",
              "append_event", "write_checkpoint", "update_used_providers"):
        setattr(store, m, mock.MagicMock())
    # The runner's post-call cancel check calls is_job_cancelled_or_abandoned.
    # An unconfigured MagicMock returns a truthy value which would falsely trip
    # the partial/cancel path — so explicitly model a normal, non-cancelled job.
    store.is_job_cancelled_or_abandoned.return_value = False

    async def fake_enrich(*args, **kwargs):
        # Yield a few times so the heartbeat background task can fire.
        for _ in range(3):
            await asyncio.sleep(0)
        return []

    async def noop_sync(*args, **kwargs):
        return None

    # A fast stand-in for asyncio.sleep that still yields control (a real
    # checkpoint via the original sleep(0)). An AsyncMock would NOT yield, so
    # the heartbeat task would never get scheduled.
    _orig_sleep = asyncio.sleep

    async def _fast_sleep(_seconds):
        await _orig_sleep(0)

    patches = [
        mock.patch.object(routes.job_store, "get_store", return_value=store),
        mock.patch.object(routes, "RawContactCollector", return_value=mock.MagicMock()),
        mock.patch.object(routes.list_builder, "run_unified_linkedin_enrichment", fake_enrich),
        mock.patch.object(routes, "_run_background_sync", noop_sync),
        mock.patch.object(routes, "OUTPUT_DIR", tmp_path),
        # Make the 30s heartbeat interval instant so the loop fires in-test.
        mock.patch.object(asyncio, "sleep", new=_fast_sleep),
    ]
    for p in patches:
        p.start()
    yield store
    for p in patches:
        p.stop()


def test_linkedin_job_writes_heartbeat(_mocked_linkedin_runner):
    store = _mocked_linkedin_runner
    job_id = "job-heartbeat-1"
    routes._job_signals[job_id] = asyncio.Event()
    routes._active_jobs.add(job_id)
    try:
        asyncio.run(routes._run_linkedin_v2_job(
            job_id=job_id,
            rows=[{"linkedin_url": "https://linkedin.com/in/x"}],
            personal_linkedin_col="linkedin_url",
            company_linkedin_col=None,
            max_dms=5,
            include_company=True,
        ))
    finally:
        routes._active_jobs.discard(job_id)
        routes._job_signals.pop(job_id, None)

    # Initial heartbeat + at least one loop iteration must fire (the core fix).
    assert store.heartbeat.call_count >= 2, (
        f"heartbeat only fired {store.heartbeat.call_count} time(s); "
        "expected initial + recurring loop"
    )
    # Job lifecycle intact.
    assert store.set_running.called
    assert store.set_done.called
    # A failed job would be a regression.
    assert not store.set_failed.called


def test_linkedin_job_heartbeat_set_before_long_work(_mocked_linkedin_runner):
    """The initial heartbeat must precede any enrichment work so the job is
    never eligible for reaping during its first 3 minutes."""
    store = _mocked_linkedin_runner
    call_order: list[str] = []
    store.set_running.side_effect = lambda *a, **k: call_order.append("set_running")
    store.heartbeat.side_effect = lambda *a, **k: call_order.append("heartbeat")

    job_id = "job-heartbeat-2"
    routes._job_signals[job_id] = asyncio.Event()
    routes._active_jobs.add(job_id)
    try:
        asyncio.run(routes._run_linkedin_v2_job(
            job_id=job_id,
            rows=[{"linkedin_url": "https://linkedin.com/in/y"}],
            personal_linkedin_col="linkedin_url",
            company_linkedin_col=None,
            max_dms=5,
            include_company=True,
        ))
    finally:
        routes._active_jobs.discard(job_id)
        routes._job_signals.pop(job_id, None)

    # First set_running, then heartbeat immediately after (before enrichment).
    assert call_order[:2] == ["set_running", "heartbeat"]
