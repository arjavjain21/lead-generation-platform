"""
Unit tests for ``shared.rate_limiter`` — the cross-process SQLite token bucket.

Covers:
  1. Grant when tokens are available (wait == 0).
  2. Deny returns the correct wait (~ 1 / refill_per_sec) after draining.
  3. Refill accrues over wall-clock time (monkeypatched ``time.time``).
  4. Concurrent acquires serialize: 10 threads, capacity=1 -> exactly 1
     grant in the first window, 9 waits.
  5. Capacity bounds the burst: capacity=2, 5 rapid acquires -> exactly
     2 grants, 3 waits.
  6. Provider isolation: getleads + smartprospect rows are independent.
  7. Lazy row creation on first acquire.
  8. Fail-open: sqlite3.Error -> returns 0.0 (grant).

Each test points the limiter at its own temp SQLite file via
``configure_db_path()`` so nothing ever touches the live jobs.db.

Async pattern: the project does NOT use pytest-asyncio. Synchronous tests
call ``acquire_token`` directly; the concurrency case uses
``asyncio.run(...)`` + ``asyncio.to_thread`` (matching the production
call pattern in getleads_client._acquire_rate_limit).
"""

from __future__ import annotations

import asyncio
import os
import sqlite3
import sys
import threading
import time
from typing import Any, Callable

import pytest

# Make sure the backend root is on sys.path so `shared` is importable
# regardless of where pytest is invoked from.
_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from shared import rate_limiter  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def temp_db(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> str:
    """Point the limiter at a fresh temp SQLite file; restore afterwards."""
    path = str(tmp_path / "rate_limit_test.db")
    rate_limiter.configure_db_path(path)
    yield path
    rate_limiter.configure_db_path(None)


@pytest.fixture
def frozen_time(monkeypatch: pytest.MonkeyPatch) -> Callable[[float], None]:
    """
    Freeze ``time.time`` as seen by the limiter module.

    Returns an ``advance(seconds)`` callable that moves the frozen clock
    forward. Without this, tests would need real sleeps.
    """
    state = {"now": 1_000_000.0}
    monkeypatch.setattr(
        rate_limiter.time, "time", lambda: state["now"], raising=True
    )

    def advance(seconds: float) -> None:
        state["now"] += seconds

    return advance


def _row(db_path: str, provider: str) -> sqlite3.Row | None:
    """Read a provider's bucket row directly from the limiter DB."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        return conn.execute(
            "SELECT * FROM provider_rate_limit WHERE provider = ?", (provider,)
        ).fetchone()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 1 & 2 — grant / deny / wait math
# ---------------------------------------------------------------------------


class TestGrantDeny:
    def test_grant_when_tokens_available(self, temp_db: str):
        """First acquire against a full bucket grants immediately (wait 0)."""
        wait = rate_limiter.acquire_token("getleads", refill_per_sec=2.0)
        assert wait == 0.0

    def test_deny_returns_correct_wait_after_draining(
        self, temp_db: str, frozen_time: Callable[[float], None]
    ):
        """After draining the bucket, wait ~= 1 / refill_per_sec."""
        refill = 2.0  # tokens/sec -> steady 120 RPM equivalent
        # Drain: capacity defaults to refill_per_sec (2.0) -> 2 grants.
        assert rate_limiter.acquire_token("getleads", refill) == 0.0
        assert rate_limiter.acquire_token("getleads", refill) == 0.0
        # Third acquire must wait ~ half a second for one token.
        wait = rate_limiter.acquire_token("getleads", refill)
        assert wait > 0.0
        assert wait == pytest.approx(1.0 / refill, rel=0.05)

    def test_denied_acquire_banks_tokens_and_advances_timestamp(
        self, temp_db: str, frozen_time: Callable[[float], None]
    ):
        """A deny must BANK the accrued tokens and ADVANCE last_refill_ts.

        Keeping ts at its old value while new_tokens already includes the
        accrual makes the NEXT reader add elapsed*rate again over the banked
        amount — inflating the bucket ~N-fold under concurrent denies (the
        2026-08-14 over-admission bug). Banking + advancing keeps the
        balance monotonically correct at the true refill rate.
        """
        refill = 1.0
        assert rate_limiter.acquire_token("getleads", refill, capacity=1.0) == 0.0
        before = _row(temp_db, "getleads")
        assert before is not None
        ts_before = before["last_refill_ts"]
        tokens_before = before["tokens"]

        frozen_time(0.25)  # advance the clock past the deny point (relative)
        wait = rate_limiter.acquire_token("getleads", refill, capacity=1.0)
        assert wait > 0.0

        after = _row(temp_db, "getleads")
        assert after is not None
        # ts advanced to the deny's now
        assert after["last_refill_ts"] == ts_before + 0.25
        # tokens banked exactly the accrual (0.25s * 1.0/s), not re-added later
        assert after["tokens"] == pytest.approx(tokens_before + 0.25, abs=1e-6)

        # The sleep-then-grant guarantee still holds: after sleeping the
        # returned wait, one more acquire grants.
        frozen_time(wait + 1e-6)
        assert rate_limiter.acquire_token("getleads", refill, capacity=1.0) == 0.0


# ---------------------------------------------------------------------------
# 3 — refill accrual
# ---------------------------------------------------------------------------


class TestRefill:
    def test_refill_accrues_over_time(
        self, temp_db: str, frozen_time: Callable[[float], None]
    ):
        """After a deny, advancing the clock re-fills the bucket."""
        refill = 1.0
        assert rate_limiter.acquire_token("getleads", refill, capacity=1.0) == 0.0
        assert rate_limiter.acquire_token("getleads", refill, capacity=1.0) > 0.0

        frozen_time(1.5)  # 1.5 tokens accrued, capped at capacity 1.0
        assert rate_limiter.acquire_token("getleads", refill, capacity=1.0) == 0.0

    def test_partial_refill_shortens_wait(
        self, temp_db: str, frozen_time: Callable[[float], None]
    ):
        """Half a token accrued -> wait is halved."""
        refill = 1.0
        assert rate_limiter.acquire_token("getleads", refill, capacity=1.0) == 0.0

        frozen_time(0.5)
        wait = rate_limiter.acquire_token("getleads", refill, capacity=1.0)
        assert wait == pytest.approx(0.5, rel=0.05)


# ---------------------------------------------------------------------------
# 4 — concurrency
# ---------------------------------------------------------------------------


class TestConcurrency:
    def test_concurrent_acquires_serialize(
        self, temp_db: str, monkeypatch: pytest.MonkeyPatch
    ):
        """
        10 concurrent acquires (asyncio.to_thread, like production) with
        capacity=1: exactly ONE grant in the first window, the other nine
        get wait > 0.
        """
        # Freeze time so the test is deterministic regardless of scheduling.
        state = {"now": 2_000_000.0}
        monkeypatch.setattr(
            rate_limiter.time, "time", lambda: state["now"], raising=True
        )

        def worker() -> float:
            return rate_limiter.acquire_token(
                "getleads", refill_per_sec=1.0, capacity=1.0
            )

        async def go() -> list[float]:
            return list(
                await asyncio.gather(
                    *(asyncio.to_thread(worker) for _ in range(10))
                )
            )

        waits = asyncio.run(go())

        assert len(waits) == 10
        assert sum(1 for w in waits if w == 0.0) == 1
        assert sum(1 for w in waits if w > 0.0) == 9
        # The denied waiters queue up in token order: 1/refill apart.
        assert max(waits) == pytest.approx(9.0 / 10, rel=0.2)

    def test_threads_serialize_on_real_threads(self, temp_db: str):
        """Plain threading (no asyncio) also serializes cleanly."""
        results: list[float] = []
        results_lock = threading.Lock()

        def worker() -> None:
            w = rate_limiter.acquire_token(
                "getleads", refill_per_sec=1.0, capacity=1.0
            )
            with results_lock:
                results.append(w)

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert sum(1 for w in results if w == 0.0) == 1
        assert sum(1 for w in results if w > 0.0) == 4


# ---------------------------------------------------------------------------
# 5 — capacity bounds burst
# ---------------------------------------------------------------------------


class TestCapacity:
    def test_capacity_bounds_burst(
        self, temp_db: str, frozen_time: Callable[[float], None]
    ):
        """capacity=2 -> 5 rapid acquires yield exactly 2 grants, 3 waits."""
        waits = [
            rate_limiter.acquire_token("getleads", refill_per_sec=1.0, capacity=2.0)
            for _ in range(5)
        ]
        assert sum(1 for w in waits if w == 0.0) == 2
        assert sum(1 for w in waits if w > 0.0) == 3

    def test_default_capacity_equals_refill_rate(
        self, temp_db: str, frozen_time: Callable[[float], None]
    ):
        """capacity=None -> capacity == refill_per_sec (~1 call of slack)."""
        # refill_per_sec=3 -> default capacity 3.0 -> 3 grants, then wait.
        for _ in range(3):
            assert rate_limiter.acquire_token("getleads", 3.0) == 0.0
        assert rate_limiter.acquire_token("getleads", 3.0) > 0.0


# ---------------------------------------------------------------------------
# 6 — provider isolation
# ---------------------------------------------------------------------------


class TestProviderIsolation:
    def test_providers_are_independent(
        self, temp_db: str, frozen_time: Callable[[float], None]
    ):
        """Draining getleads does not affect smartprospect's bucket."""
        assert rate_limiter.acquire_token("getleads", 1.0, capacity=1.0) == 0.0
        assert rate_limiter.acquire_token("getleads", 1.0, capacity=1.0) > 0.0

        # smartprospect still grants — independent row, full bucket.
        assert rate_limiter.acquire_token("smartprospect", 1.0, capacity=1.0) == 0.0

        conn = sqlite3.connect(temp_db)
        conn.row_factory = sqlite3.Row
        rows = {
            r["provider"]: dict(r)
            for r in conn.execute(
                "SELECT provider, tokens FROM provider_rate_limit"
            ).fetchall()
        }
        conn.close()

        assert set(rows) == {"getleads", "smartprospect"}
        assert rows["getleads"]["tokens"] == pytest.approx(0.0)
        assert rows["smartprospect"]["tokens"] == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# 7 — lazy row creation
# ---------------------------------------------------------------------------


class TestLazyCreation:
    def test_row_created_on_first_acquire(self, temp_db: str):
        """No row exists until the first acquire; acquire creates it full."""
        conn = sqlite3.connect(temp_db)
        count = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE name = 'provider_rate_limit'"
        ).fetchone()[0]
        conn.close()
        assert count == 0

        assert rate_limiter.acquire_token("getleads", 2.0, capacity=3.0) == 0.0

        row = _row(temp_db, "getleads")
        assert row is not None
        assert row["tokens"] == pytest.approx(2.0)  # 3.0 capacity - 1 token
        assert row["capacity"] == pytest.approx(3.0)
        assert row["refill_per_sec"] == pytest.approx(2.0)


# ---------------------------------------------------------------------------
# 8 — fail-open
# ---------------------------------------------------------------------------


class TestFailOpen:
    def test_sqlite_error_fails_open(
        self, temp_db: str, monkeypatch: pytest.MonkeyPatch
    ):
        """Any sqlite3.Error during the acquire returns 0.0 (grant)."""

        def _broken_conn() -> Any:
            raise sqlite3.OperationalError("database is locked")

        monkeypatch.setattr(rate_limiter, "_get_conn", _broken_conn, raising=True)

        wait = rate_limiter.acquire_token("getleads", 1.0, capacity=1.0)
        assert wait == 0.0

    def test_broken_transaction_fails_open(
        self, temp_db: str, monkeypatch: pytest.MonkeyPatch
    ):
        """A sqlite3.Error raised mid-transaction also fails open."""
        real_conn = rate_limiter._get_conn()

        class _BoomConn:
            def execute(self, *_a: Any, **_k: Any) -> Any:
                raise sqlite3.OperationalError("disk I/O error")

            def commit(self) -> None:
                raise sqlite3.OperationalError("disk I/O error")

            def rollback(self) -> None:
                raise sqlite3.OperationalError("disk I/O error")

        monkeypatch.setattr(
            rate_limiter, "_get_conn", lambda: _BoomConn(), raising=True
        )
        try:
            wait = rate_limiter.acquire_token("getleads", 1.0, capacity=1.0)
            assert wait == 0.0
        finally:
            # Restore the real cached connection for later assertions.
            monkeypatch.setattr(
                rate_limiter, "_get_conn", lambda: real_conn, raising=True
            )

    def test_zero_refill_rate_grants(self, temp_db: str):
        """Degenerate refill_per_sec=0 must not divide by zero — grants."""
        wait = rate_limiter.acquire_token("getleads", refill_per_sec=0.0)
        assert wait == 0.0
