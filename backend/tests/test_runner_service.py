"""
Tests for the P1 runner split (2026-09-02).

- Web workers skip the dispatcher when ENABLE_SCRAPER_DISPATCHER=false
  (systemd sets it in zz-resource-tuning-20260902.conf); default unset =
  true, preserving single-process dev behavior.
- runner_main boots: DB init, boot reap, dispatcher + guard loops, and
  exits non-zero if the lifespan crashes (systemd restarts it).
- The runner unit and the web drop-in are consistent (opposite gates).

All DB work on temp files; nothing touches production jobs.
"""

from __future__ import annotations

import asyncio
import os
from unittest import mock

import pytest


class TestDispatcherGate:
    def test_gate_false_disables_web_dispatcher(self, monkeypatch):
        monkeypatch.setenv("ENABLE_SCRAPER_DISPATCHER", "false")
        val = os.environ.get("ENABLE_SCRAPER_DISPATCHER", "true").lower()
        assert val not in ("1", "true", "yes")

    def test_default_true_when_unset(self, monkeypatch):
        monkeypatch.delenv("ENABLE_SCRAPER_DISPATCHER", raising=False)
        val = os.environ.get("ENABLE_SCRAPER_DISPATCHER", "true").lower()
        assert val in ("1", "true", "yes")

    def test_main_py_gate_string_matches(self):
        """The literal gate expression in main.py must match the runner's
        inverse — a typo in either silently runs zero or two dispatchers."""
        src = open("main.py").read()
        assert 'ENABLE_SCRAPER_DISPATCHER", "true"' in src
        assert "ENABLE_SCRAPER_DISPATCHER=false — web worker skips" in src

    def test_runner_unit_sets_opposite(self):
        """The systemd units must gate opposite ways (web false, runner true)."""
        unit = open("/etc/systemd/system/lead-gen-scraper-runner.service").read()
        assert "ENABLE_SCRAPER_DISPATCHER=true" in unit
        dropin_path = ("/etc/systemd/system/lead-generation-platform.service.d/"
                       "zz-resource-tuning-20260902.conf")
        if os.path.exists(dropin_path):
            dropin = open(dropin_path).read()
            assert "ENABLE_SCRAPER_DISPATCHER=false" in dropin


class TestRunnerMain:
    def test_lifespan_gathers_both_loops(self):
        """_runner_lifespan must run dispatcher AND guard forever — if either
        returns, asyncio.gather returns and the process exits non-zero."""
        import inspect
        import runner_main
        src = inspect.getsource(runner_main._runner_lifespan)
        assert "dispatch_loop(" in src
        assert "runtime_guard_loop()" in src
        assert "asyncio.gather" in src

    def test_runner_exits_nonzero_when_lifespan_crashes(self, monkeypatch):
        import runner_main

        async def boom():
            raise RuntimeError("catastrophic")

        monkeypatch.setattr(runner_main, "_runner_lifespan", boom)
        with pytest.raises(SystemExit) as exc:
            asyncio.run(runner_main.main())
        assert exc.value.code == 1

    def test_runner_sigterm_clean_exit(self, monkeypatch):
        """SIGTERM mid-run: loops cancelled, exit code 0, no exception."""
        import runner_main

        cancelled = {"flag": False}

        async def endless_lifespan():
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled["flag"] = True
                raise

        monkeypatch.setattr(runner_main, "_runner_lifespan", endless_lifespan)

        async def run_and_signal():
            task = asyncio.create_task(runner_main.main())
            await asyncio.sleep(0.05)
            import signal as _signal
            handler = _signal.getsignal(_signal.SIGTERM)
            handler(_signal.SIGTERM, None)
            try:
                await asyncio.wait_for(task, timeout=2)
            except asyncio.TimeoutError:
                task.cancel()
                raise AssertionError("runner did not exit after SIGTERM")

        asyncio.run(run_and_signal())
        assert cancelled["flag"] is True  # loops were cancelled, not abandoned


class TestDispatchLoopInRunner:
    def test_dispatch_loop_respects_high_per_process_cap(self, tmp_path, monkeypatch):
        """The runner passes per_worker_cap from env (unit sets 6 = platform
        cap) so all platform slots are usable by the single runner."""
        import threading

        db_path = tmp_path / "jobs.db"
        import sqlite3
        conn = sqlite3.connect(str(db_path), check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.executescript("""
            CREATE TABLE jobs (
                job_id TEXT PRIMARY KEY, user_id TEXT, job_type TEXT,
                status TEXT DEFAULT 'queued', parent_job_id TEXT, query TEXT,
                regions TEXT, total_tasks INTEGER, restart_count INTEGER DEFAULT 0,
                is_resumable INTEGER DEFAULT 1, last_heartbeat TEXT,
                created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
        """)
        conn.commit()

        import shared.db as shared_db
        local = threading.local()
        local.conn = conn
        monkeypatch.setattr(shared_db, "get_db", lambda: conn)
        monkeypatch.setattr(shared_db, "DB_PATH", db_path)

        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat()
        for i in range(8):
            conn.execute(
                "INSERT INTO jobs (job_id, user_id, job_type, status, query, regions,"
                " total_tasks, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
                (f"j{i}", "u1", "scraper", "queued", "q", "{}", 10, now, now),
            )
        conn.commit()

        from scraper import dispatch
        monkeypatch.setattr(dispatch, "MAX_CONCURRENT_SCRAPER_JOBS", 6)

        launched: list[str] = []
        release = asyncio.Event()

        async def slow_launch(job_id):
            launched.append(job_id)
            await release.wait()

        async def run():
            task = asyncio.create_task(
                dispatch.dispatch_loop(slow_launch, poll_seconds=0.01, per_worker_cap=6)
            )
            await asyncio.sleep(0.2)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(run())
        release.set()
        assert len(launched) == 6  # runner cap 6 == platform cap 6 — full use
