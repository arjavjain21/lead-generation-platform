# Incremental Resume for Abandoned Jobs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement true incremental resume for abandoned enrichment jobs, allowing restart from the last checkpoint without re-processing already-completed rows.

**Architecture:** Add SQLite checkpoint table to track processed row indices. On restart, filter unprocessed rows and resume. Preserve partial output as `_partial.csv`.

**Tech Stack:** Python, SQLite, FastAPI, existing job_store infrastructure

---

## File Structure

| File | Responsibility |
|------|----------------|
| `backend/migrations/add_checkpoint_support.py` | New migration for job_checkpoints table + restart_count column |
| `backend/shared/job_store_base.py` | Add checkpoint write/read methods |
| `backend/enrichment/routes.py` | Update restart endpoint to handle abandoned jobs + filter unprocessed rows + write checkpoints |
| `backend/enrichment/tests/test_job_store.py` | Unit tests for checkpoint methods |

---

## Task 1: Create Migration for Checkpoint Tables

**Files:**
- Create: `backend/migrations/add_checkpoint_support.py`
- Test: Verify schema changes via sqlite3

- [ ] **Step 1: Write the migration script**

```python
#!/usr/bin/env python3
"""
Migration script to add checkpoint support for incremental resume.

Adds:
- job_checkpoints table: tracks processed row indices per job
- restart_count column on jobs table: tracks number of restarts

Run: python backend/migrations/add_checkpoint_support.py
"""

import sqlite3
from pathlib import Path


def get_db_path():
    """Get the database path."""
    return Path(__file__).parent.parent / "data" / "jobs.db"


def get_db():
    """Get database connection."""
    db_path = get_db_path()
    return sqlite3.connect(db_path)


def migrate():
    """Add checkpoint support to database."""
    conn = get_db()
    cursor = conn.cursor()

    print("Checking current schema...")

    # Check existing tables and columns
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    existing_tables = {row[0] for row in cursor.fetchall()}

    cursor.execute("PRAGMA table_info(jobs)")
    existing_columns = {row[1] for row in cursor.fetchall()}

    changes_made = []

    # 1. Add job_checkpoints table if it doesn't exist
    if "job_checkpoints" not in existing_tables:
        print("Creating job_checkpoints table...")
        cursor.execute("""
            CREATE TABLE job_checkpoints (
                job_id TEXT NOT NULL,
                row_index INTEGER NOT NULL,
                processed_at TEXT NOT NULL,
                PRIMARY KEY (job_id, row_index)
            )
        """)
        cursor.execute("""
            CREATE INDEX idx_checkpoints_job ON job_checkpoints(job_id)
        """)
        changes_made.append("job_checkpoints table")
        print("  ✓ Created job_checkpoints table with index")
    else:
        print("✓ job_checkpoints table already exists")

    # 2. Add restart_count column to jobs table if it doesn't exist
    if "restart_count" not in existing_columns:
        print("Adding restart_count column to jobs table...")
        cursor.execute("""
            ALTER TABLE jobs ADD COLUMN restart_count INTEGER DEFAULT 0
        """)
        changes_made.append("restart_count column")
        print("  ✓ Added restart_count column")
    else:
        print("✓ restart_count column already exists")

    conn.commit()

    if changes_made:
        print(f"\n✓ Migration completed! Added: {', '.join(changes_made)}")
    else:
        print("\n✓ No changes needed - checkpoint support already exists")


if __name__ == "__main__":
    migrate()
```

- [ ] **Step 2: Run the migration**

Run: `cd /var/www/lead-generation-platform/backend && python migrations/add_checkpoint_support.py`
Expected: Output showing table/column creation or "already exists"

- [ ] **Step 3: Verify schema changes**

Run: `sqlite3 data/jobs.db ".schema job_checkpoints"`
Expected: Table definition with job_id, row_index, processed_at

Run: `sqlite3 data/jobs.db "PRAGMA table_info(jobs)" | grep restart`
Expected: restart_count column definition

- [ ] **Step 4: Commit**

```bash
git add backend/migrations/add_checkpoint_support.py
git commit -m "feat: add checkpoint tables for incremental resume"
```

---

## Task 2: Add Checkpoint Methods to JobStoreBase

**Files:**
- Modify: `backend/shared/job_store_base.py:300-350` (add new methods at end)
- Test: `backend/tests/test_job_store.py` (create if not exists)

- [ ] **Step 1: Write the test for checkpoint methods**

Create: `backend/tests/test_job_store.py`

```python
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
```

- [ ] **Step 2: Run test to verify it fails (method not defined)**

Run: `cd /var/www/lead-generation-platform/backend && python -m pytest tests/test_job_store.py -v`
Expected: FAIL with "AttributeError: 'JobStoreBase' object has no attribute 'write_checkpoint'"

- [ ] **Step 3: Add checkpoint methods to job_store_base.py**

Add at end of `backend/shared/job_store_base.py`:

```python
def write_checkpoint(self, job_id: str, row_index: int) -> None:
    """
    Write a checkpoint for a processed row.

    Args:
        job_id: The job identifier
        row_index: The index of the processed row (0-based)
    """
    now = _now()
    self.conn.execute(
        "INSERT OR REPLACE INTO job_checkpoints (job_id, row_index, processed_at) VALUES (?, ?, ?)",
        (job_id, row_index, now),
    )
    self.conn.commit()

def get_processed_indices(self, job_id: str) -> set[int]:
    """
    Get all processed row indices for a job.

    Args:
        job_id: The job identifier

    Returns:
        Set of processed row indices
    """
    rows = self.conn.execute(
        "SELECT row_index FROM job_checkpoints WHERE job_id=? ORDER BY row_index",
        (job_id,),
    ).fetchall()
    return {row[0] for row in rows}

def get_unprocessed_indices(self, total_rows: int, job_id: str) -> list[int]:
    """
    Get list of unprocessed row indices.

    Args:
        total_rows: Total number of rows in the input
        job_id: The job identifier

    Returns:
        List of unprocessed indices in ascending order
    """
    processed = self.get_processed_indices(job_id)
    return [i for i in range(total_rows) if i not in processed]

def increment_restart_count(self, job_id: str) -> int:
    """
    Increment the restart count for a job.

    Args:
        job_id: The job identifier

    Returns:
        The new restart count
    """
    self.conn.execute(
        "UPDATE jobs SET restart_count = restart_count + 1 WHERE job_id=?",
        (job_id,),
    )
    self.conn.commit()

    cursor = self.conn.execute(
        "SELECT restart_count FROM jobs WHERE job_id=?",
        (job_id,),
    )
    row = cursor.fetchone()
    return row[0] if row else 0

def cleanup_checkpoints(self, job_id: str) -> int:
    """
    Remove all checkpoints for a job (used after job completion).

    Args:
        job_id: The job identifier

    Returns:
        Number of checkpoints deleted
    """
    cursor = self.conn.execute(
        "DELETE FROM job_checkpoints WHERE job_id=?",
        (job_id,),
    )
    self.conn.commit()
    return cursor.rowcount
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /var/www/lead-generation-platform/backend && python -m pytest tests/test_job_store.py -v`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add backend/shared/job_store_base.py backend/tests/test_job_store.py
git commit -m "feat: add checkpoint methods to JobStoreBase"
```

---

## Task 3: Update Restart Endpoint to Handle Abandoned Jobs

**Files:**
- Modify: `backend/enrichment/routes.py:2531-2652` (restart endpoint)
- Test: Verify behavior via curl

- [ ] **Step 1: Read current restart endpoint implementation**

Look at `backend/enrichment/routes.py:2531-2700` to understand current flow.

- [ ] **Step 2: Modify restart endpoint to accept abandoned status and filter unprocessed rows**

Find this code block in `restart_enrichment_job()`:
```python
if original_job["status"] != "failed":
    raise HTTPException(status_code=400, detail="Only failed jobs can be restarted")
```

Replace with:
```python
if original_job["status"] not in ("failed", "abandoned"):
    raise HTTPException(status_code=400,
        detail="Only failed or abandoned jobs can be restarted")
```

Then after reading the CSV and before creating the new job, add:

```python
# Handle output file from previous run (rename to partial)
output_path = OUTPUT_DIR / f"{original_job['job_id']}.csv"
if output_path.exists():
    partial_path = OUTPUT_DIR / f"{original_job['job_id']}_partial.csv"
    output_path.rename(partial_path)
    logger.info("Renamed previous output to %s", partial_path)

# Get unprocessed row indices if restarting from abandoned job
unprocessed_indices = None
if original_job["status"] == "abandoned":
    store = job_store.get_store()
    total_rows = len(rows)
    unprocessed_indices = store.get_unprocessed_indices(total_rows, original_job['job_id'])
    logger.info("Job %s abandoned at row %d/%d, resuming with %d unprocessed rows",
                job_id, total_rows - len(unprocessed_indices), total_rows, len(unprocessed_indices))

    if unprocessed_indices:
        # Filter rows to only unprocessed
        rows = [rows[i] for i in unprocessed_indices]
    else:
        # No checkpoints means full re-process
        logger.info("No checkpoints found for job %s, full re-process", job_id)

# Update restart count
store = job_store.get_store()
new_restart_count = store.increment_restart_count(job_id)
```

Then after creating the new job, add:
```python
# If we have unprocessed indices, write checkpoints for them so new job can track progress
if unprocessed_indices:
    for idx in unprocessed_indices:
        store.write_checkpoint(new_job_id, idx)
```

- [ ] **Step 3: Test the restart endpoint**

Start server and test with an abandoned job (or create a test job and manually set status to 'abandoned').

- [ ] **Step 4: Commit**

```bash
git add backend/enrichment/routes.py
git commit -m "feat: extend restart endpoint to handle abandoned jobs with incremental resume"
```

---

## Task 4: Integrate Checkpoint Writing into on_progress Callback

**Files:**
- Modify: `backend/enrichment/routes.py` (on_progress callbacks in _run_job, _run_linkedin_job, etc.)
- Test: Verify checkpoints are written during normal processing

- [ ] **Step 1: Read current on_progress implementation**

Look at `backend/enrichment/routes.py:2336-2345` for `_run_job`'s on_progress callback. This is where checkpoints should be added.

- [ ] **Step 2: Add checkpoint writing to _run_job's on_progress**

Find the `on_progress` function in `_run_job()` (around line 2336) and modify it to write checkpoints:

```python
async def on_progress(e: dict[str, Any]):
    # Get FRESH store instance for this thread
    # This fixes the progress counter bug where background tasks couldn't commit
    progress_store = job_store.get_store()
    progress_store.append_event(job_id, seq[0], e)
    seq[0] += 1

    # Write checkpoint every 100 rows for incremental resume
    row_index = e.get("index", 0)
    if row_index % 100 == 0:
        progress_store.write_checkpoint(job_id, row_index)

    sig = _job_signals.get(job_id)
    if sig:
        sig.set()
        sig.clear()
```

- [ ] **Step 3: Add checkpoint writing to other on_progress callbacks**

Find and update the following other `on_progress` callbacks:
- `_run_linkedin_job` (around line 3271)
- `_run_linkedin_v2_job` (around line 3425)

```python
async def on_progress(e: dict[str, Any]):
    progress_store = job_store.get_store()
    progress_store.append_event(job_id, seq[0], e)
    seq[0] += 1

    # Write checkpoint every 100 rows for incremental resume
    row_index = e.get("index", 0)
    if row_index % 100 == 0:
        progress_store.write_checkpoint(job_id, row_index)

    sig = _job_signals.get(job_id)
    if sig:
        sig.set()
        sig.clear()
```

- [ ] **Step 4: Test checkpoint creation**

Run a small enrichment job and verify checkpoints are written:
```bash
sqlite3 backend/data/jobs.db "SELECT * FROM job_checkpoints LIMIT 10;"
```

- [ ] **Step 5: Commit**

```bash
git add backend/enrichment/routes.py
git commit -m "feat: integrate checkpoint writing into on_progress callbacks"
```

---

## Task 5: End-to-End Testing

**Files:**
- Test: Manual testing with simulated crash

- [ ] **Step 1: Test normal completion (baseline)**

Run a small enrichment job (100 rows) to completion and verify:
1. Output CSV is correct
2. Checkpoints are created

- [ ] **Step 2: Test crash simulation**

1. Start a medium-sized enrichment job (1000+ rows)
2. Wait for some rows to process (check via SSE or database)
3. Kill the process: `pkill -f "uvicorn main:app"`
4. Verify job status is set to 'abandoned' in database
5. Check partial output exists

- [ ] **Step 3: Test restart with checkpoints**

1. Trigger restart on abandoned job via API or frontend
2. Verify:
   - New output file created (not appended)
   - Only unprocessed rows are re-enriched (check via logs or API calls)
   - Partial output preserved as `_partial.csv`
   - `restart_count` incremented

- [ ] **Step 4: Verify no duplicate data**

Check that results don't contain duplicates between partial and new output.

---

## Summary

| Task | Description | Files |
|------|-------------|-------|
| 1 | Database migration for checkpoint tables | `backend/migrations/add_checkpoint_support.py` |
| 2 | Checkpoint read/write methods | `backend/shared/job_store_base.py` |
| 3 | Update restart endpoint | `backend/enrichment/routes.py` |
| 4 | Integrate checkpoints into pipeline | `backend/enrichment/pipeline.py` |
| 5 | End-to-end testing | Manual verification |

---

## Success Criteria

- [ ] Migration creates job_checkpoints table and restart_count column
- [ ] Checkpoints are written during row processing (every 100 rows)
- [ ] Restart endpoint accepts 'abandoned' status
- [ ] Restart filters to only unprocessed rows
- [ ] Partial output preserved as `_partial.csv`
- [ ] `restart_count` increments on each restart
- [ ] No duplicate processing on restart
- [ ] All existing tests still pass

---

**Plan saved to:** `docs/superpowers/plans/2026-05-19-incremental-resume-implementation.md`