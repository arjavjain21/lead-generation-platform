"""DB connection recycling on SQLITE_BUSY — the runner-wedge fix.

A connection that hits 'database is locked' mid-commit / mid-auto-checkpoint
can keep holding WAL shm locks (CHECKPOINT byte 124 / RECOVER byte 125) for
the life of the process. After that, every later write on that thread fails
with 'database is locked' — the runner wedged for hours, the WAL bloated to
GBs, and only a service restart recovered it (incidents 2026-10-24, 10-30,
10-31 and 11-04, the last one 9h overnight with 10 errors/10m).

recycle_db() closes the thread's connection (fd close releases all locks at
once); the next get_db() reconnects fresh. See shared/db.py.
"""

import sqlite3

from shared.db import get_db, is_busy_error, recycle_db


def test_is_busy_error_matches_lock_messages():
    assert is_busy_error(sqlite3.OperationalError("database is locked"))
    assert is_busy_error(sqlite3.OperationalError("database table is locked"))
    assert is_busy_error(RuntimeError("wrapped: database is locked"))
    assert not is_busy_error(sqlite3.OperationalError("no such table: jobs"))
    assert not is_busy_error(ValueError("unrelated"))


def test_recycle_db_closes_conn_and_next_get_is_fresh():
    c1 = get_db()
    assert get_db() is c1  # thread-local caching intact
    recycle_db("test: simulated busy wedge")
    c2 = get_db()
    assert c2 is not c1
    # the fresh connection works
    assert c2.execute("SELECT 1").fetchone()[0] == 1
    # the old one is really closed
    try:
        c1.execute("SELECT 1")
        raise AssertionError("old connection should be closed")
    except sqlite3.ProgrammingError:
        pass


def test_recycle_db_safe_with_no_conn_or_double_call():
    recycle_db("test: no conn yet")   # must not raise when nothing to close
    recycle_db("test: double recycle")


def test_recycle_db_recovers_writable_state():
    """End-to-end shape of the incident: conn goes bad, recycle, writes work."""
    c1 = get_db()
    recycle_db("test: wedge simulation")
    c2 = get_db()
    c2.execute("CREATE TABLE IF NOT EXISTS _recycle_smoke (k TEXT)")
    c2.execute("DELETE FROM _recycle_smoke")
    c2.execute("INSERT INTO _recycle_smoke VALUES ('ok')")
    c2.commit()
    assert c2.execute("SELECT k FROM _recycle_smoke").fetchone()[0] == "ok"
    c2.execute("DROP TABLE _recycle_smoke")
    c2.commit()
