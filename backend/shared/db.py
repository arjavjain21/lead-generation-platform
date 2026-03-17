"""
Shared database connection and initialization for the unified jobs database.

This module provides:
- Thread-local database connections
- Unified schema with job_type discriminator
- Parent-child job relationships for chaining
- Daily API request tracking for non-admin users
"""

from __future__ import annotations

import logging
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)
DB_PATH = DATA_DIR / "jobs.db"

_local = threading.local()

# Daily API request limit for non-admin users
NON_ADMIN_DAILY_REQUEST_LIMIT = 50000


def get_db() -> sqlite3.Connection:
    """Return a per-thread SQLite connection with row factory enabled."""
    if not hasattr(_local, "conn") or _local.conn is None:
        conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        _local.conn = conn
    return _local.conn


def init_db() -> None:
    """
    Initialize the unified database schema.

    Creates tables for both scraper and enrichment jobs with a job_type discriminator.
    Also creates the shared job_events table and indexes.
    """
    c = get_db()
    c.executescript(
        """
        CREATE TABLE IF NOT EXISTS jobs (
            job_id        TEXT PRIMARY KEY,
            user_id       TEXT,
            job_type      TEXT NOT NULL,  -- 'scraper' | 'enrichment'
            status        TEXT NOT NULL DEFAULT 'queued',  -- queued | running | done | failed
            parent_job_id TEXT,          -- FK to jobs.job_id (for chaining)

            -- Scraper-specific fields (NULL for enrichment jobs)
            query         TEXT,
            regions       TEXT,          -- JSON
            total_tasks   INTEGER,
            done_tasks    INTEGER,
            result_count  INTEGER,

            -- Enrichment-specific fields (NULL for scraper jobs)
            total         INTEGER,       -- Total rows to enrich
            processed     INTEGER,       -- Rows processed
            emails_found  INTEGER,
            filename      TEXT,
            domain_col    TEXT,
            original_filename TEXT,     -- Original uploaded filename (for display)

            -- Common fields
            error         TEXT,
            output_path   TEXT,
            created_at    TEXT NOT NULL,
            updated_at    TEXT NOT NULL,

            FOREIGN KEY (user_id) REFERENCES users(user_id),
            FOREIGN KEY (parent_job_id) REFERENCES jobs(job_id)
        );

        CREATE TABLE IF NOT EXISTS job_events (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id  TEXT NOT NULL,
            seq     INTEGER NOT NULL,
            payload TEXT NOT NULL,  -- JSON event
            FOREIGN KEY (job_id) REFERENCES jobs(job_id)
        );

        CREATE TABLE IF NOT EXISTS daily_api_requests (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id     TEXT NOT NULL,
            date        TEXT NOT NULL,  -- UTC date in YYYY-MM-DD format
            request_count INTEGER NOT NULL DEFAULT 0,
            UNIQUE(user_id, date),
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        );

        CREATE INDEX IF NOT EXISTS idx_job_events_job_seq
            ON job_events (job_id, seq);

        CREATE INDEX IF NOT EXISTS idx_jobs_user_type
            ON jobs (user_id, job_type, created_at);

        CREATE INDEX IF NOT EXISTS idx_jobs_parent
            ON jobs (parent_job_id);

        CREATE INDEX IF NOT EXISTS idx_daily_api_requests_user_date
            ON daily_api_requests (user_id, date);
        """
    )
    c.commit()


def _today_date() -> str:
    """Return today's UTC date in YYYY-MM-DD format."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def get_daily_request_count(user_id: str) -> int:
    """Get the API request count for a user today."""
    c = get_db()
    row = c.execute(
        "SELECT request_count FROM daily_api_requests WHERE user_id = ? AND date = ?",
        (user_id, _today_date())
    ).fetchone()
    return row["request_count"] if row else 0


def check_daily_request_limit(user_id: str, is_admin: bool, estimated_requests: int = 0) -> tuple[bool, str]:
    """
    Check if a user can make more API requests today.

    Args:
        user_id: The user's ID
        is_admin: Whether the user is an admin (admins are exempt from limits)
        estimated_requests: Estimated number of requests for the pending operation

    Returns:
        (allowed: bool, message: str)
    """
    if is_admin:
        return True, ""

    current_count = get_daily_request_count(user_id)
    projected_count = current_count + estimated_requests

    if projected_count > NON_ADMIN_DAILY_REQUEST_LIMIT:
        remaining = NON_ADMIN_DAILY_REQUEST_LIMIT - current_count
        return False, (
            f"Daily API request limit exceeded. You have used {current_count:,} of "
            f"{NON_ADMIN_DAILY_REQUEST_LIMIT:,} daily requests. "
            f"Remaining: {remaining:,}. Please try again tomorrow."
        )

    return True, ""


def record_api_requests(user_id: str, count: int) -> None:
    """
    Record API requests for a user today.

    Args:
        user_id: The user's ID
        count: Number of requests to record (can be 0, will upsert)
    """
    c = get_db()
    today = _today_date()

    # Upsert: insert or update the request count
    c.execute(
        """
        INSERT INTO daily_api_requests (user_id, date, request_count)
        VALUES (?, ?, ?)
        ON CONFLICT(user_id, date) DO UPDATE SET
            request_count = request_count + ?
        """,
        (user_id, today, count, count)
    )
    c.commit()


def record_api_request(user_id: str) -> None:
    """Record a single API request for a user today."""
    record_api_requests(user_id, 1)


def get_api_quota_status(user_id: str, is_admin: bool) -> dict[str, any]:
    """
    Get the current API quota status for a user.

    Returns a dict with:
        - limit: The daily limit (None for admins)
        - used: Number of requests used today
        - remaining: Number of requests remaining (None for admins)
        - resets_at: When the quota resets (midnight UTC)
    """
    if is_admin:
        return {
            "limit": None,
            "used": 0,
            "remaining": None,
            "resets_at": None,
            "is_admin": True
        }

    used = get_daily_request_count(user_id)
    remaining = max(0, NON_ADMIN_DAILY_REQUEST_LIMIT - used)

    # Calculate reset time (midnight UTC tomorrow)
    tomorrow = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    from datetime import timedelta
    tomorrow += timedelta(days=1)

    return {
        "limit": NON_ADMIN_DAILY_REQUEST_LIMIT,
        "used": used,
        "remaining": remaining,
        "resets_at": tomorrow.isoformat(),
        "is_admin": False
    }


def close_db() -> None:
    """Close the current thread's database connection."""
    if hasattr(_local, "conn") and _local.conn is not None:
        _local.conn.close()
        _local.conn = None
