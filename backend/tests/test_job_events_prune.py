"""Direct unit tests for the job_events retention prune + done-cleanup feature.

Covers the two methods added in the WIP that previously had no direct tests:
- ``prune_old_job_events`` (now batched): deletes events for non-running jobs
  older than retention, plus orphaned events; keeps running + recent.
- ``_mark_done_and_cleanup``: marks a job done and removes its row-checkpoints
  (best-effort — a cleanup failure must not block the done).

Timestamps are written in SQLite's ``datetime('now')`` format (space separator,
no tz) so the string comparison in the prune query is reliable.
"""
from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

_BE = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _BE not in sys.path:
    sys.path.insert(0, _BE)

from shared.job_store_base import JobStoreBase  # noqa: E402


def _ts(days_ago: float) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days_ago)).strftime("%Y-%m-%d %H:%M:%S")


def _fresh_db():
    f = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    conn = sqlite3.connect(f.name)
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE jobs (
            job_id TEXT PRIMARY KEY, status TEXT, output_path TEXT,
            updated_at TEXT, created_at TEXT
        );
        CREATE TABLE job_events (job_id TEXT, seq INTEGER, payload TEXT);
        CREATE TABLE job_checkpoints (
            job_id TEXT, row_index INTEGER, processed_at TEXT,
            PRIMARY KEY (job_id, row_index)
        );
        """
    )
    conn.commit()
    return conn, f.name


def _add_job(conn, job_id, status, created):
    conn.execute(
        "INSERT INTO jobs (job_id, status, created_at) VALUES (?, ?, ?)",
        (job_id, status, created),
    )


def _add_events(conn, job_id, n):
    for s in range(n):
        conn.execute(
            "INSERT INTO job_events (job_id, seq, payload) VALUES (?, ?, ?)",
            (job_id, s, "x"),
        )


def test_prune_deletes_old_and_orphaned_keeps_running_and_recent():
    conn, path = _fresh_db()
    try:
        _add_job(conn, "old-done", "done", _ts(30)); _add_events(conn, "old-done", 3)
        _add_job(conn, "old-running", "running", _ts(30)); _add_events(conn, "old-running", 1)
        _add_job(conn, "recent-done", "done", _ts(1)); _add_events(conn, "recent-done", 1)
        _add_events(conn, "orphan", 1)  # no job row
        conn.commit()

        removed = JobStoreBase(conn).prune_old_job_events(7)

        assert removed == 4  # 3 old-done + 1 orphan
        remaining = {r[0] for r in conn.execute("SELECT DISTINCT job_id FROM job_events").fetchall()}
        assert remaining == {"old-running", "recent-done"}
    finally:
        conn.close(); Path(path).unlink(missing_ok=True)


def test_prune_batched_deletes_all_matching():
    """With a small batch size, the loop must still delete every matching row."""
    conn, path = _fresh_db()
    try:
        _add_job(conn, "old", "done", _ts(30))
        _add_events(conn, "old", 7)  # > batch_size of 2
        _add_events(conn, "orphan", 1)
        conn.commit()

        removed = JobStoreBase(conn).prune_old_job_events(7, batch_size=2)

        assert removed == 8
        assert conn.execute("SELECT COUNT(*) AS n FROM job_events").fetchone()["n"] == 0
    finally:
        conn.close(); Path(path).unlink(missing_ok=True)


def test_mark_done_and_cleanup_deletes_checkpoints():
    conn, path = _fresh_db()
    try:
        conn.execute("INSERT INTO jobs (job_id, status, created_at) VALUES (?, 'running', ?)", ("j1", _ts(1)))
        conn.executemany(
            "INSERT INTO job_checkpoints (job_id, row_index, processed_at) VALUES (?, ?, ?)",
            [("j1", 0, "x"), ("j1", 1, "x")],
        )
        conn.commit()

        JobStoreBase(conn)._mark_done_and_cleanup("j1", "/tmp/out.csv")

        assert conn.execute("SELECT status FROM jobs WHERE job_id='j1'").fetchone()["status"] == "done"
        assert conn.execute("SELECT COUNT(*) AS n FROM job_checkpoints WHERE job_id='j1'").fetchone()["n"] == 0
    finally:
        conn.close(); Path(path).unlink(missing_ok=True)


def test_mark_done_and_cleanup_is_best_effort():
    """If checkpoint cleanup fails, the job must still be marked done."""
    conn, path = _fresh_db()
    try:
        conn.execute("INSERT INTO jobs (job_id, status, created_at) VALUES (?, 'running', ?)", ("j2", _ts(1)))
        conn.commit()
        conn.execute("DROP TABLE job_checkpoints")  # force cleanup_checkpoints to raise
        conn.commit()

        JobStoreBase(conn)._mark_done_and_cleanup("j2", "/tmp/out.csv")  # must not raise

        assert conn.execute("SELECT status FROM jobs WHERE job_id='j2'").fetchone()["status"] == "done"
    finally:
        conn.close(); Path(path).unlink(missing_ok=True)
