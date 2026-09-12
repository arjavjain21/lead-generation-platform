"""Tests for the thread-based arbiter notify in LeadGenUvicornWorker.

2026-09-11: loop-based callback_notify starved with the event loop under
multi-job load → 900s of missed notifies → gunicorn murdered healthy
workers. The notify now runs on a daemon thread that only dies with the
process.
"""
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from shared import worker as worker_mod
from shared.worker import LeadGenUvicornWorker, _notify_forever, _notify_interval


class TestNotifyInterval:
    def test_interval_capped_by_timeout(self):
        assert _notify_interval(900) == 30
        assert _notify_interval(10) == 10

    def test_interval_without_timeout(self):
        assert _notify_interval(None) == 30

    def test_interval_env_tunable(self, monkeypatch):
        monkeypatch.setattr(worker_mod, "NOTIFY_INTERVAL_SECONDS", 5.0)
        assert _notify_interval(900) == 5.0


class _FakeWorker:
    timeout = 5

    def __init__(self):
        self.calls = 0

    def notify(self):
        self.calls += 1


class TestNotifyForever:
    def test_notifies_each_cycle(self):
        fake = _FakeWorker()
        sleeps = iter([None, None, StopIteration()])

        def fake_sleep(_seconds):
            step = next(sleeps)
            if isinstance(step, BaseException):
                raise step

        with patch.object(worker_mod.time, "sleep", side_effect=fake_sleep):
            with pytest.raises(StopIteration):
                _notify_forever(fake)

        assert fake.calls == 2

    def test_survives_notify_failure(self):
        """A notify() exception must NOT kill the loop (a dead notifier is a
        murdered worker)."""

        class FlakyWorker(_FakeWorker):
            def notify(self):
                self.calls += 1
                if self.calls == 1:
                    raise ValueError("simulated utime failure")

        fake = FlakyWorker()
        sleeps = iter([None, None, StopIteration()])

        def fake_sleep(_seconds):
            step = next(sleeps)
            if isinstance(step, BaseException):
                raise step

        with patch.object(worker_mod.time, "sleep", side_effect=fake_sleep):
            with pytest.raises(StopIteration):
                _notify_forever(fake)

        assert fake.calls == 2  # second cycle still ran after the failure


class TestRunWiring:
    def test_run_starts_daemon_thread_then_delegates(self, monkeypatch):
        threads = []

        class FakeThread:
            def __init__(self, *, target, args, name, daemon):
                threads.append(
                    {"target": target, "args": args, "name": name, "daemon": daemon}
                )

            def start(self):
                threads[-1]["started"] = True

        order = []
        monkeypatch.setattr(worker_mod.threading, "Thread", FakeThread)
        monkeypatch.setattr(
            worker_mod.UvicornWorker, "run", lambda self: order.append("super_run")
        )

        # Skip the heavy gunicorn __init__ (Config/Logger wiring): run() only
        # touches self via the thread args and super().run().
        worker = object.__new__(LeadGenUvicornWorker)
        LeadGenUvicornWorker.run(worker)

        assert order == ["super_run"]
        assert len(threads) == 1
        assert threads[0]["daemon"] is True
        assert threads[0]["started"] is True
        assert threads[0]["name"] == "leadgen-arbiter-notify"
        # Module-attribute lookup, not an import-time binding: sibling tests
        # (test_auto_resume) importlib.reload(shared.worker), which rebinds
        # _notify_forever to a new object in the same module namespace.
        assert threads[0]["target"] is worker_mod._notify_forever
