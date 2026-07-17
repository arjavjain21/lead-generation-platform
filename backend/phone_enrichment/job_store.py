"""Job store for phone enrichment jobs."""

import json
import logging
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from shared.db import get_db

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent.parent / "data"
OUTPUT_DIR = DATA_DIR / "outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def _ensure_phone_schema() -> None:
    """Idempotent schema migration for phone-enrichment job lifecycle.

    Mirrors the ALTER-TABLE-if-missing pattern in ``shared/db.py::init_db``.
    Phone jobs live in the shared ``jobs`` table (``job_type='phone_enrichment'``)
    and rely on two columns for robust lifecycle management:

      * ``last_heartbeat``  - bumped every 30s by the background runner so the
        startup reaper (``cleanup_stale_phone_jobs``) can detect crashed/recycled
        jobs instead of leaving them 'running' forever.
      * ``cancelled_at``    - records when a user cancelled, mirroring the
        enrichment store's ``set_cancelled``.

    On a fully-migrated database these statements are no-ops (the columns already
    exist). They exist only to be defensive on fresh/minimal installs that import
    this module. Safe to run repeatedly. If the ``jobs`` table has not been
    created yet (init_db pending), the function exits without doing anything.
    """
    db = get_db()
    needed = {
        "last_heartbeat": "TEXT",
        "cancelled_at": "TEXT",
    }
    try:
        existing = {
            row["name"]
            for row in db.execute("PRAGMA table_info(jobs)").fetchall()
        }
    except sqlite3.OperationalError:
        # jobs table not created yet — init_db() owns creation; nothing to migrate
        return
    for col, coltype in needed.items():
        if col not in existing:
            db.execute(f"ALTER TABLE jobs ADD COLUMN {col} {coltype}")
            db.commit()


# Run at import so cleanup_stale_phone_jobs() can rely on the columns at startup
_ensure_phone_schema()


def create_phone_enrichment_job(
    user_id: str,
    filename: str,
    original_filename: str,
    linkedin_col: str,
    total: int,
) -> str:
    """
    Create a new phone enrichment job.

    Args:
        user_id: The user's ID
        filename: Path to the uploaded CSV file
        original_filename: Original filename for display
        linkedin_col: Column name containing LinkedIn URLs
        total: Total number of rows to process

    Returns:
        job_id: The new job's ID
    """
    job_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()

    db = get_db()
    db.execute(
        """
        INSERT INTO jobs (
            job_id, user_id, job_type, status,
            filename, original_filename, linkedin_col,
            total, processed, phones_found,
            created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            job_id,
            user_id,
            "phone_enrichment",
            "queued",
            filename,
            original_filename,
            linkedin_col,
            total,
            0,
            0,
            now,
            now,
        ),
    )
    db.commit()
    logger.info(f"Created phone enrichment job {job_id} for user {user_id}")
    return job_id


def get_phone_job(job_id: str) -> Optional[dict[str, Any]]:
    """Get phone enrichment job by ID."""
    db = get_db()
    row = db.execute(
        """
        SELECT * FROM jobs WHERE job_id = ? AND job_type = 'phone_enrichment'
        """,
        (job_id,),
    ).fetchone()
    return dict(row) if row else None


def get_user_phone_jobs(user_id: str, limit: int = 50) -> list[dict[str, Any]]:
    """Get all phone enrichment jobs for a user."""
    db = get_db()
    rows = db.execute(
        """
        SELECT * FROM jobs
        WHERE user_id = ? AND job_type = 'phone_enrichment'
        ORDER BY created_at DESC
        LIMIT ?
        """,
        (user_id, limit),
    ).fetchall()
    return [dict(row) for row in rows]


def update_job_status(job_id: str, status: str) -> None:
    """Update job status."""
    db = get_db()
    now = datetime.now(timezone.utc).isoformat()
    db.execute(
        "UPDATE jobs SET status = ?, updated_at = ? WHERE job_id = ?",
        (status, now, job_id),
    )
    db.commit()


def update_job_progress(job_id: str, processed: int, phones_found: int) -> None:
    """Update job progress."""
    db = get_db()
    now = datetime.now(timezone.utc).isoformat()
    db.execute(
        """
        UPDATE jobs SET processed = ?, phones_found = ?, updated_at = ?
        WHERE job_id = ?
        """,
        (processed, phones_found, now, job_id),
    )
    db.commit()


def set_job_output(job_id: str, output_path: str) -> None:
    """Set the output path for a completed job."""
    db = get_db()
    now = datetime.now(timezone.utc).isoformat()
    db.execute(
        "UPDATE jobs SET output_path = ?, updated_at = ? WHERE job_id = ?",
        (output_path, now, job_id),
    )
    db.commit()


def set_job_error(job_id: str, error: str) -> None:
    """Set an error message for a failed job."""
    db = get_db()
    now = datetime.now(timezone.utc).isoformat()
    db.execute(
        "UPDATE jobs SET error = ?, status = 'failed', updated_at = ? WHERE job_id = ?",
        (error, now, job_id),
    )
    db.commit()


# -----------------------------------------------------------------------------
# Lifecycle: heartbeat, stale detection, abandonment, cancellation
# (mirrors shared/job_store_base.JobStoreBase — phone jobs reuse the ``jobs``
#  table with job_type='phone_enrichment', so these operate on that table.)
# -----------------------------------------------------------------------------

def heartbeat(job_id: str) -> None:
    """Update last_heartbeat for a running phone job.

    Called every 30s by the background runner's heartbeat task so the startup
    reaper can distinguish a live job from a crashed/recycled one. Mirrors
    ``JobStoreBase.heartbeat``.
    """
    db = get_db()
    now = datetime.now(timezone.utc).isoformat()
    db.execute(
        "UPDATE jobs SET last_heartbeat = ? WHERE job_id = ?",
        (now, job_id),
    )
    db.commit()


def get_stale_running_phone_jobs() -> list[str]:
    """Return phone job_ids whose heartbeat is older than 2 minutes.

    Mirrors ``JobStoreBase.get_stale_running_jobs_by_heartbeat``, scoped to
    ``job_type='phone_enrichment'``. A job is only considered stale if it has
    been alive for more than 3 minutes (so newly-started jobs whose heartbeat
    isn't yet established are not falsely reaped). Used by
    ``cleanup_stale_phone_jobs`` at startup.
    """
    db = get_db()
    rows = db.execute(
        """
        SELECT job_id FROM jobs
        WHERE job_type = 'phone_enrichment'
          AND status IN ('running', 'queued')
          AND (datetime(last_heartbeat) IS NULL
               OR datetime(last_heartbeat) < datetime('now', '-2 minutes'))
          AND datetime(created_at) < datetime('now', '-3 minutes')
        """
    ).fetchall()
    return [row["job_id"] for row in rows]


def set_abandoned(job_id: str, error: str) -> None:
    """Mark a phone job abandoned (server crashed/restarted while processing).

    Mirrors ``JobStoreBase.set_abandoned``.
    """
    db = get_db()
    now = datetime.now(timezone.utc).isoformat()
    db.execute(
        "UPDATE jobs SET status = 'abandoned', error = ?, updated_at = ? WHERE job_id = ?",
        (error, now, job_id),
    )
    db.commit()


def set_cancelled(job_id: str) -> None:
    """Mark a phone job cancelled by the user.

    Mirrors ``JobStoreBase.set_cancelled``. Requires the ``cancelled_at`` column
    (added idempotently by ``_ensure_phone_schema``).
    """
    db = get_db()
    now = datetime.now(timezone.utc).isoformat()
    db.execute(
        "UPDATE jobs SET status = 'cancelled', cancelled_at = ?, updated_at = ? WHERE job_id = ?",
        (now, now, job_id),
    )
    db.commit()


def is_job_cancelled(job_id: str) -> bool:
    """Check if a phone job has been cancelled (DB check, cross-worker safe).

    Mirrors ``JobStoreBase.is_job_cancelled``.
    """
    db = get_db()
    row = db.execute(
        "SELECT status FROM jobs WHERE job_id = ?",
        (job_id,),
    ).fetchone()
    if row:
        return row["status"] == "cancelled"
    return False


# -----------------------------------------------------------------------------
# Job Events (for SSE streaming)
# -----------------------------------------------------------------------------

def append_event(job_id: str, event: dict[str, Any]) -> None:
    """Append an event to the job's event stream."""
    db = get_db()
    # Get current max seq
    row = db.execute(
        "SELECT MAX(seq) as max_seq FROM job_events WHERE job_id = ?",
        (job_id,),
    ).fetchone()
    max_seq = row["max_seq"] if row and row["max_seq"] is not None else -1

    db.execute(
        "INSERT INTO job_events (job_id, seq, payload) VALUES (?, ?, ?)",
        (job_id, max_seq + 1, json.dumps(event)),
    )
    db.commit()


def get_events_since(job_id: str, since_seq: int = -1) -> list[dict[str, Any]]:
    """Get all events for a job since the given sequence number."""
    db = get_db()
    rows = db.execute(
        "SELECT seq, payload FROM job_events WHERE job_id = ? AND seq > ? ORDER BY seq",
        (job_id, since_seq),
    ).fetchall()
    return [
        {"seq": row["seq"], **json.loads(row["payload"])}
        for row in rows
    ]


# -----------------------------------------------------------------------------
# Phone Enrichment Cache
# -----------------------------------------------------------------------------

def get_cached_phone(linkedin_url: str) -> Optional[dict[str, Any]]:
    """
    Get cached phone enrichment result.

    Args:
        linkedin_url: The LinkedIn URL to look up

    Returns:
        dict with phone_number and phone_found, or None if not cached
    """
    db = get_db()
    row = db.execute(
        "SELECT phone_number, phone_found FROM phone_enrichments WHERE linkedin_url = ?",
        (linkedin_url,),
    ).fetchone()
    if row:
        return {
            "phone_number": row["phone_number"],
            "phone_found": bool(row["phone_found"]),
        }
    return None


def cache_phone_enrichment(linkedin_url: str, phone_number: Optional[str], phone_found: bool) -> None:
    """
    Cache a phone enrichment result.

    Args:
        linkedin_url: The LinkedIn URL
        phone_number: The phone number (or None if not found)
        phone_found: Whether a phone was found
    """
    db = get_db()
    now = datetime.now(timezone.utc).isoformat()
    db.execute(
        """
        INSERT OR REPLACE INTO phone_enrichments (linkedin_url, phone_number, phone_found, enriched_at)
        VALUES (?, ?, ?, ?)
        """,
        (linkedin_url, phone_number, 1 if phone_found else 0, now),
    )
    db.commit()
