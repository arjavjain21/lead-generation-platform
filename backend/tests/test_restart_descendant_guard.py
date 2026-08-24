"""Tests for the restart descendant guard (2026-08-24 parallel-branch fix).

The Aug 24 hyperke-saas chain: a user clicked Restart on job P while P's
direct child C was abandoned (not "active") but C's own child G was running.
The old guard (direct children with active status only) passed, so a second
branch was born and ran thousands of overlapping domains in parallel with G.

_find_active_descendant must find G through the abandoned intermediate.
"""
import sqlite3
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from enrichment.routes import _find_active_descendant  # noqa: E402


SCHEMA = """
CREATE TABLE jobs (
    job_id TEXT PRIMARY KEY,
    status TEXT,
    parent_job_id TEXT
);
"""


@pytest.fixture
def temp_conn():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    conn.commit()
    yield conn, db_path
    conn.close()
    Path(db_path).unlink(missing_ok=True)


def _insert(conn, job_id, status, parent=None):
    conn.execute(
        "INSERT INTO jobs (job_id, status, parent_job_id) VALUES (?, ?, ?)",
        (job_id, status, parent),
    )
    conn.commit()


class TestFindActiveDescendant:
    def _find(self, conn, job_id):
        """_find_active_descendant reads via shared.db.get_db() (thread-local);
        patch it to the fixture connection."""
        with patch("enrichment.routes.db.get_db", return_value=conn):
            return _find_active_descendant(job_id)

    def test_direct_running_child_found(self, temp_conn):
        conn, _ = temp_conn
        _insert(conn, "P", "abandoned")
        _insert(conn, "C", "running", parent="P")
        found = self._find(conn, "P")
        assert found is not None and found["job_id"] == "C"

    def test_running_grandchild_through_abandoned_child(self, temp_conn):
        """The exact Aug 24 scenario: C abandoned, G (C's child) running.
        Restarting P must be blocked."""
        conn, _ = temp_conn
        _insert(conn, "P", "abandoned")
        _insert(conn, "C", "abandoned", parent="P")
        _insert(conn, "G", "running", parent="C")
        found = self._find(conn, "P")
        assert found is not None and found["job_id"] == "G"

    def test_deep_chain_running_descendant_found(self, temp_conn):
        conn, _ = temp_conn
        _insert(conn, "P", "abandoned")
        _insert(conn, "C1", "partial", parent="P")
        _insert(conn, "C2", "abandoned", parent="C1")
        _insert(conn, "C3", "cancelled", parent="C2")
        _insert(conn, "C4", "queued", parent="C3")
        found = self._find(conn, "P")
        assert found is not None and found["job_id"] == "C4"

    def test_all_terminal_descendants_clear(self, temp_conn):
        conn, _ = temp_conn
        _insert(conn, "P", "abandoned")
        _insert(conn, "C1", "done", parent="P")
        _insert(conn, "C2", "cancelled", parent="P")
        _insert(conn, "C3", "partial", parent="C1")
        assert self._find(conn, "P") is None

    def test_leaf_job_no_children(self, temp_conn):
        conn, _ = temp_conn
        _insert(conn, "P", "abandoned")
        assert self._find(conn, "P") is None

    def test_cycle_does_not_loop_forever(self, temp_conn):
        """Defensive: corrupt data with a parent cycle must terminate."""
        conn, _ = temp_conn
        _insert(conn, "P", "abandoned")
        _insert(conn, "C", "abandoned", parent="P")
        conn.execute("UPDATE jobs SET parent_job_id='C' WHERE job_id='P'")
        conn.commit()
        assert self._find(conn, "P") is None
