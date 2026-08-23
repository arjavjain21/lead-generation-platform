"""
Cross-process token-bucket rate limiter backed by the shared SQLite DB.

Why this exists: provider account limits (e.g. GetLeads' 100 req/min GLOBAL
cap) apply across ALL gunicorn workers, but the legacy per-module limiters
used in-process globals (``asyncio.Lock`` + ``time.monotonic()``). With 4
workers each running its own limiter, the collective rate was ~4x the
configured cap, causing steady 429 storms. SQLite (WAL + busy_timeout) is
the only cross-process substrate in this deployment, so the bucket lives in
the ``provider_rate_limit`` table inside ``jobs.db``.

Clock choice: ``time.time()`` WALL clock, not ``time.monotonic()`` — a
monotonic clock is per-process and cannot be shared. Consequences of wall
clock drift (safe by design):
    - Forward NTP jump  -> ``elapsed`` spikes -> bucket refills instantly ->
      brief over-throttle for the remainder of the window. Harmless.
    - Backward NTP jump -> ``elapsed`` clamps to 0.0 -> refill pauses until
      the clock catches back up. Over-allowance is bounded by ``capacity``
      (which equals one call's worth of tokens when capacity is unset).

Fail-open: if the limiter DB is unavailable for ANY sqlite3 reason, the
acquire logs a warning and returns 0.0 (grant). Enrichment must never
hard-stop because the limiter itself is down — providers already handle
429s with retry/backoff.

Usage (callers wrap in asyncio.to_thread — this function is SYNC):

    wait = await asyncio.to_thread(
        rate_limiter.acquire_token, "getleads", refill_per_sec, capacity
    )
    if wait > 0:
        await asyncio.sleep(wait)
"""

from __future__ import annotations

import logging
import sqlite3
import threading
import time
from typing import Optional

logger = logging.getLogger(__name__)

# Connection state. ONE lazy connection PER PROCESS, shared across the
# asyncio.to_thread worker pool (check_same_thread=False); all transaction
# work is serialized behind ``_LOCK`` so threads never interleave statements.
_CONN: Optional[sqlite3.Connection] = None
_LOCK = threading.Lock()

# DB file the limiter uses. ``None`` = derive from shared/db.py's DB_PATH
# (jobs.db). Tests point this at a temp file via ``configure_db_path()``.
_DB_PATH: Optional[str] = None

_SCHEMA = """
CREATE TABLE IF NOT EXISTS provider_rate_limit (
    provider       TEXT PRIMARY KEY,
    tokens         REAL NOT NULL,
    last_refill_ts REAL NOT NULL,
    capacity       REAL NOT NULL,
    refill_per_sec REAL NOT NULL
);
"""


def _default_db_path() -> str:
    """Return the jobs.db path, deriving it exactly like shared/db.py does."""
    from pathlib import Path

    data_dir = Path(__file__).parent.parent / "data"
    return str(data_dir / "jobs.db")


def configure_db_path(path: Optional[str]) -> None:
    """
    Point the limiter at a specific SQLite file (tests / tooling).

    Closes any cached connection so the next acquire reconnects against the
    new path. Passing ``None`` restores the default (backend/data/jobs.db).

    NOT for hot-swapping in production — intended for test isolation.
    """
    global _CONN, _DB_PATH
    with _LOCK:
        if _CONN is not None:
            try:
                _CONN.close()
            except sqlite3.Error:
                pass
            _CONN = None
        _DB_PATH = path


def _get_conn() -> sqlite3.Connection:
    """Return the process-wide limiter connection, creating it lazily."""
    global _CONN
    if _CONN is None:
        conn = sqlite3.connect(
            _DB_PATH or _default_db_path(),
            check_same_thread=False,
            timeout=30.0,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA synchronous=NORMAL")
        # Defense in depth: shared/db.py's init_db also creates this table,
        # but the limiter must work even if init_db hasn't run in this
        # process/DB yet.
        conn.executescript(_SCHEMA)
        conn.commit()
        _CONN = conn
    return _CONN


def acquire_token(
    provider: str,
    refill_per_sec: float,
    capacity: Optional[float] = None,
) -> float:
    """
    Atomically take one token from the shared cross-process bucket.

    Args:
        provider: Bucket key (e.g. "getleads"). Each provider is an
            independent row — providers never share tokens.
        refill_per_sec: Steady-state refill rate in tokens/sec
            (e.g. RPM / 60.0).
        capacity: Max burst size in tokens. Defaults to ``refill_per_sec``
            (i.e. ~one call's worth of burst allowance).

    Returns:
        Seconds the caller must wait before proceeding. ``0.0`` = granted
        now. On deny, refill keeps accruing from the ORIGINAL timestamp
        (``last_refill_ts`` is not advanced), so sleeping the returned
        duration guarantees a grant on retry. Fails open (returns 0.0) on
        any sqlite3.Error.
    """
    if capacity is None:
        capacity = refill_per_sec
    if refill_per_sec <= 0:
        # Degenerate config — do not divide by zero; grant.
        return 0.0

    conn = None
    try:
        with _LOCK:
            conn = _get_conn()
            now = time.time()
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """
                SELECT tokens, last_refill_ts, capacity, refill_per_sec
                FROM provider_rate_limit WHERE provider = ?
                """,
                (provider,),
            ).fetchone()

            if row is None:
                # Lazy row creation: a fresh provider starts with a full
                # bucket. INSERT OR IGNORE guards against a concurrent
                # process having created the row between SELECT and INSERT.
                conn.execute(
                    """
                    INSERT OR IGNORE INTO provider_rate_limit
                        (provider, tokens, last_refill_ts, capacity, refill_per_sec)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (provider, capacity, now, capacity, refill_per_sec),
                )
                row = conn.execute(
                    """
                    SELECT tokens, last_refill_ts, capacity, refill_per_sec
                    FROM provider_rate_limit WHERE provider = ?
                    """,
                    (provider,),
                ).fetchone()

            tokens = row["tokens"]
            last_refill_ts = row["last_refill_ts"]

            elapsed = max(0.0, now - last_refill_ts)
            refilled = min(capacity, tokens + elapsed * refill_per_sec)

            if refilled >= 1.0:
                # Grant — consume one token, advance the timestamp.
                new_tokens = refilled - 1.0
                new_ts = now
                wait = 0.0
            else:
                # Deny — BANK the accrued tokens and advance the timestamp.
                # Advancing ts is essential: if ts stayed at its old value
                # while new_tokens already included the accrual, the NEXT
                # reader would add elapsed*rate AGAIN over the banked amount,
                # inflating the bucket ~N-fold under concurrent denies (the
                # 2026-08-14 over-admission bug). Sleeping the returned wait
                # still guarantees a grant on retry: after wait seconds,
                # refilled' = refilled + wait * refill_per_sec == 1.0.
                wait = (1.0 - refilled) / refill_per_sec
                new_tokens = refilled
                new_ts = now

            conn.execute(
                """
                UPDATE provider_rate_limit
                SET tokens = ?, last_refill_ts = ?
                WHERE provider = ?
                """,
                (new_tokens, new_ts, provider),
            )
            conn.commit()
            return wait

    except sqlite3.Error as exc:
        # FAIL-OPEN: enrichment must never hard-stop because the limiter is
        # unavailable. Providers already retry 429s with backoff.
        logger.warning(
            "rate_limiter: DB error acquiring token for %s, failing open: %s",
            provider,
            exc,
        )
        if conn is not None:
            try:
                conn.rollback()
            except sqlite3.Error:
                pass
        return 0.0
