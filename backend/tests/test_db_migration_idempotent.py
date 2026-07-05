"""Tests for the DB migration that adds the 4 pre-processing columns.

Verifies the migration against the real production DB:
  * The 4 new columns exist.
  * The defaults are correct.
  * Existing rows have the new columns populated with the schema defaults.
  * Re-running init_db() does not double-add columns.
"""
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))


DB_PATH = Path(__file__).parent.parent / "data" / "jobs.db"


@pytest.fixture
def db_conn():
    """Open a connection to the real production DB."""
    assert DB_PATH.exists(), f"DB not found at {DB_PATH}"
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()


def _get_columns(conn):
    rows = conn.execute("PRAGMA table_info(jobs)").fetchall()
    return {row[1]: row for row in rows}


class TestMigrationApplied:
    def test_new_columns_exist(self, db_conn):
        cols = _get_columns(db_conn)
        assert "normalize_domains" in cols
        assert "dedupe_by_domain" in cols
        assert "deduped_rows" in cols
        assert "dedupe_skipped_domains" in cols

    def test_column_defaults(self, db_conn):
        cols = _get_columns(db_conn)
        # INTEGER columns: default = 1 or 0 (PRAGMA returns dflt_value as string)
        assert cols["normalize_domains"][4] == "1"  # dflt_value
        assert cols["dedupe_by_domain"][4] == "1"
        assert cols["deduped_rows"][4] == "0"
        # TEXT column: default = "''" (SQL string literal)
        assert cols["dedupe_skipped_domains"][4] == "''"

    def test_idempotent_re_run(self, db_conn):
        """Calling init_db() again should not raise and should not
        change the column set."""
        from shared import db as shared_db
        cols_before = set(_get_columns(db_conn).keys())
        # Reset the connection state so init_db re-runs.
        shared_db._local.conn = None
        try:
            shared_db.init_db()
        finally:
            shared_db._local.conn = None
        cols_after = set(_get_columns(db_conn).keys())
        assert cols_before == cols_after

    def test_existing_rows_have_defaults(self, db_conn):
        """Pre-migration rows should have 1/1/0/'' for the new columns."""
        row = db_conn.execute(
            "SELECT normalize_domains, dedupe_by_domain, deduped_rows, dedupe_skipped_domains "
            "FROM jobs ORDER BY created_at LIMIT 1"
        ).fetchone()
        if row is None:
            pytest.skip("No rows in DB to test defaults")
        assert row[0] == 1
        assert row[1] == 1
        assert row[2] == 0
        assert row[3] == ""
