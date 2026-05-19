"""Tests for job_store_base checkpoint functionality."""
import pytest
import tempfile
import sqlite3
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from shared.job_store_base import JobStoreBase


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = f.name

    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE jobs (
            job_id TEXT PRIMARY KEY,
            user_id TEXT,
            job_type TEXT,
            status TEXT,
            restart_count INTEGER DEFAULT 0,
            created_at TEXT,
            updated_at TEXT
        );
        CREATE TABLE job_checkpoints (
            job_id TEXT NOT NULL,
            row_index INTEGER NOT NULL,
            processed_at TEXT NOT NULL,
            PRIMARY KEY (job_id, row_index)
        );
    """)
    conn.commit()

    yield db_path

    Path(db_path).unlink()


def test_write_checkpoint(temp_db):
    """Test writing a checkpoint."""
    store = JobStoreBase(sqlite3.connect(temp_db))

    job_id = "test-job-123"
    row_index = 50

    # Write checkpoint
    store.write_checkpoint(job_id, row_index)

    # Verify it was written
    cursor = store.conn.execute(
        "SELECT row_index FROM job_checkpoints WHERE job_id=?", (job_id,)
    )
    rows = cursor.fetchall()
    assert len(rows) == 1
    assert rows[0][0] == 50


def test_write_multiple_checkpoints(temp_db):
    """Test writing multiple checkpoints for same job."""
    store = JobStoreBase(sqlite3.connect(temp_db))

    job_id = "test-job-456"
    indices = [10, 20, 30, 40, 50]

    for idx in indices:
        store.write_checkpoint(job_id, idx)

    # Verify all were written
    processed = store.get_processed_indices(job_id)
    assert set(processed) == set(indices)


def test_get_processed_indices_empty(temp_db):
    """Test getting processed indices for job with no checkpoints."""
    store = JobStoreBase(sqlite3.connect(temp_db))

    processed = store.get_processed_indices("nonexistent-job")
    assert processed == set()


def test_get_processed_indices(temp_db):
    """Test getting processed indices."""
    store = JobStoreBase(sqlite3.connect(temp_db))

    job_id = "test-job-789"
    indices = [5, 15, 25, 35]

    for idx in indices:
        store.write_checkpoint(job_id, idx)

    processed = store.get_processed_indices(job_id)
    assert processed == {5, 15, 25, 35}


def test_get_unprocessed_indices(temp_db):
    """Test getting unprocessed indices."""
    store = JobStoreBase(sqlite3.connect(temp_db))

    job_id = "test-job-unprocessed"
    # Process rows 0-4
    for idx in [0, 1, 2, 3, 4]:
        store.write_checkpoint(job_id, idx)

    total = 10
    unprocessed = store.get_unprocessed_indices(total, job_id)
    assert unprocessed == [5, 6, 7, 8, 9]


def test_increment_restart_count(temp_db):
    """Test incrementing restart count on a job."""
    store = JobStoreBase(sqlite3.connect(temp_db))

    job_id = "test-job-restart"

    # Create a job first
    store.conn.execute("""
        INSERT INTO jobs (job_id, user_id, job_type, status, restart_count, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (job_id, "user1", "enrichment", "abandoned", 0, "2026-05-19", "2026-05-19"))
    store.conn.commit()

    # Increment restart count
    new_count = store.increment_restart_count(job_id)

    # Verify
    assert new_count == 1

    cursor = store.conn.execute("SELECT restart_count FROM jobs WHERE job_id=?", (job_id,))
    row = cursor.fetchone()
    assert row[0] == 1


def test_cleanup_checkpoints(temp_db):
    """Test cleaning up checkpoints."""
    store = JobStoreBase(sqlite3.connect(temp_db))

    job_id = "test-job-cleanup"
    indices = [1, 2, 3, 4, 5]

    for idx in indices:
        store.write_checkpoint(job_id, idx)

    # Verify checkpoints exist
    processed = store.get_processed_indices(job_id)
    assert len(processed) == 5

    # Cleanup
    deleted = store.cleanup_checkpoints(job_id)

    # Verify
    assert deleted == 5
    processed = store.get_processed_indices(job_id)
    assert processed == set()