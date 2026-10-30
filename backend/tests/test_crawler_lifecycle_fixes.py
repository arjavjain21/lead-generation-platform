"""Regression tests for the 2026-10-30 crawler lifecycle fixes.

Both scraper jobs stalled 'running' with dead heartbeats at 12:53 UTC
(jobs 35ba6e86 / 5f4c48e0, reaped 15:14). Root cause chain:

1. on_progress (a SQLite append_event write) raised "database is locked"
   under write contention; the call sat OUTSIDE the per-task try/except,
   so the exception escaped process_task.
2. asyncio.gather without return_exceptions propagated that first exception
   immediately, unwinding the httpx-client and CSV context managers while
   sibling tasks were still in flight — they then failed with
   "client has been closed" / "I/O operation on closed file" and the job
   coroutine died, leaving the job 'running' with a dead heartbeat.

Fixes under test:
- on_progress failures are caught + logged (warning), never fatal.
- gather drains every task (return_exceptions=True) before unwinding;
  the first exception is re-raised only after all siblings finished.

Run alone:
    cd backend && source venv/bin/activate
    python -m pytest tests/test_crawler_lifecycle_fixes.py -q
"""
import asyncio
import sqlite3
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).parent.parent))

from scraper import crawler as crawler_module


def _center(name: str = "Testville", state: str = "TS") -> dict:
    return {"name": name, "state": state, "lat": 1.0, "lng": 2.0}


class TestOnProgressFailureIsNotFatal:
    def test_locked_db_during_on_progress_does_not_kill_crawl(self, tmp_path, monkeypatch):
        """A 'database is locked' from on_progress must be swallowed per task."""
        calls = {"progress": 0, "fetch": 0}

        async def fake_fetch_one(client, query, center, zoom, semaphore, cancelled, job_id):
            calls["fetch"] += 1
            return []  # no results — enough to exercise the write path

        async def flaky_on_progress(event):
            calls["progress"] += 1
            raise sqlite3.OperationalError("database is locked")

        monkeypatch.setattr(crawler_module, "_fetch_one", fake_fetch_one)

        total = asyncio.run(crawler_module.run_crawl(
            job_id="job-lock-test",
            query="dentist",
            centers=[_center("A"), _center("B"), _center("C")],
            api_key="key",
            output_path=tmp_path / "out.csv",
            on_progress=flaky_on_progress,
            zooms=[10],
        ))

        # Every task's fetch ran and every progress event was attempted:
        # the crawl ran to completion despite all progress writes failing.
        assert calls["fetch"] == 3
        assert calls["progress"] == 3
        assert total == 0
        assert (tmp_path / "out.csv").exists()

    def test_on_progress_recovers_after_transient_failure(self, tmp_path, monkeypatch):
        """Fail the first progress write only; later ones still fire."""
        seen = []

        async def fake_fetch_one(client, query, center, zoom, semaphore, cancelled, job_id):
            return []

        async def on_progress(event):
            seen.append(event["center_name"])
            if len(seen) == 1:
                raise sqlite3.OperationalError("database is locked")

        monkeypatch.setattr(crawler_module, "_fetch_one", fake_fetch_one)

        asyncio.run(crawler_module.run_crawl(
            job_id="job-recover-test",
            query="dentist",
            centers=[_center("A"), _center("B")],
            api_key="key",
            output_path=tmp_path / "out.csv",
            on_progress=on_progress,
            zooms=[10],
        ))

        assert seen == ["A", "B"]


class TestGatherDrainsSiblingsBeforeUnwinding:
    def test_pre_try_exception_waits_for_inflight_siblings(self, tmp_path, monkeypatch):
        """A task raising before its try-block (the cancel path) must not
        unwind the client/CSV while a sibling fetch is still in flight.

        Setup: tasks are created in order [Slow, Fast1, Fast2]. The cancel
        check returns False on its FIRST call (Slow's, which then parks
        mid-fetch) and True afterwards (Fast1/Fast2 raise the pre-try
        cancellation RuntimeError). Under the old bare gather the body
        unwound as soon as Fast1 raised; with return_exceptions=True the
        gather keeps waiting until the parked Slow fetch is released.
        """
        may_finish = asyncio.Event()
        fetch_outcome: dict = {}
        check_calls = {"n": 0}

        async def fake_fetch_one(client, query, center, zoom, semaphore, cancelled, job_id):
            if center["name"] == "Slow":
                await may_finish.wait()
                fetch_outcome["slow_fetch"] = "completed"
                return []
            return []

        async def check_cancelled(job_id):
            check_calls["n"] += 1
            return check_calls["n"] > 1

        async def on_progress(event):
            return None

        monkeypatch.setattr(crawler_module, "_fetch_one", fake_fetch_one)

        async def scenario() -> None:
            task = asyncio.create_task(crawler_module.run_crawl(
                job_id="job-drain-test",
                query="dentist",
                centers=[_center("Slow"), _center("Fast1"), _center("Fast2")],
                api_key="key",
                output_path=tmp_path / "out.csv",
                on_progress=on_progress,
                zooms=[10],
                check_cancelled=check_cancelled,
            ))
            # Fast1/Fast2 have raised by now (their cancel checks ran). The
            # crawl must NOT be done: gather is still draining Slow, whose
            # fetch is parked. Under the old bare gather, task would be
            # done here with the RuntimeError already propagated.
            for _ in range(50):
                await asyncio.sleep(0.02)
                assert check_calls["n"] >= 3, "cancel checks did not all run"
                assert not task.done(), (
                    "run_crawl finished while a sibling fetch was still in "
                    "flight — gather did not drain before unwinding"
                )
            may_finish.set()
            with __import__("pytest").raises(RuntimeError, match="cancelled"):
                await asyncio.wait_for(task, timeout=5)

        asyncio.run(scenario())
        assert fetch_outcome.get("slow_fetch") == "completed"

    def test_gather_reraises_first_exception_after_drain(self, tmp_path, monkeypatch):
        """The first exception still propagates to the caller (cancel flow
        keeps working) — but only after every task has been drained."""
        async def fake_fetch_one(client, query, center, zoom, semaphore, cancelled, job_id):
            return []

        async def on_progress(event):
            return None

        monkeypatch.setattr(crawler_module, "_fetch_one", fake_fetch_one)

        with __import__("pytest").raises(RuntimeError, match="cancelled"):
            asyncio.run(crawler_module.run_crawl(
                job_id="job-cancel-test",
                query="dentist",
                centers=[_center("A"), _center("B")],
                api_key="key",
                output_path=tmp_path / "out.csv",
                on_progress=on_progress,
                zooms=[10],
                cancelled_jobs={"job-cancel-test"},
            ))


class TestCancelSemanticsUnchanged:
    def test_uncancelled_job_still_completes_normally(self, tmp_path, monkeypatch):
        """Happy path: no regressions when nothing raises."""
        rows = [
            {"place_id": "p1", "name": "Place One", "latitude": 1.001, "longitude": 2.001},
            {"place_id": "p1", "name": "Place One", "latitude": 1.001, "longitude": 2.001},  # dedupe
        ]

        async def fake_fetch_one(client, query, center, zoom, semaphore, cancelled, job_id):
            return rows

        events = []

        async def on_progress(event):
            events.append(event)

        monkeypatch.setattr(crawler_module, "_fetch_one", fake_fetch_one)

        total = asyncio.run(crawler_module.run_crawl(
            job_id="job-happy-test",
            query="dentist",
            centers=[_center("A")],
            api_key="key",
            output_path=tmp_path / "out.csv",
            on_progress=on_progress,
            zooms=[10, 11],
        ))

        assert total == 1  # dedupe_key collapse across zooms
        assert len(events) == 2  # one progress event per completed task
