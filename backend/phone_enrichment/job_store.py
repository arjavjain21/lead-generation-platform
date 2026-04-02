"""Job store for phone enrichment jobs."""

import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from shared.db import get_db

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent.parent / "data"
OUTPUT_DIR = DATA_DIR / "outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


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
