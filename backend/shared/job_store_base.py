"""
Base job store class with common operations for both scraper and enrichment jobs.

This module provides:
- Base class with shared job operations
- Common read/write helpers
- Event streaming support
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional

import sqlite3

from . import db

logger = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class JobStoreBase:
    """Base class for job operations shared across scraper and enrichment."""

    def __init__(self, conn: Optional[sqlite3.Connection] = None):
        self.conn = conn or db.get_db()

    def create_job(
        self,
        job_id: str,
        user_id: str,
        job_type: str,  # 'scraper' or 'enrichment'
        **kwargs: Any,
    ) -> None:
        """
        Create a new job.

        Common kwargs: parent_job_id
        Scraper kwargs: query, regions, total_tasks
        Enrichment kwargs: total, filename, domain_col
        """
        now = _now()
        columns = ["job_id", "user_id", "job_type", "status", "created_at", "updated_at"]
        values = [job_id, user_id, job_type, "queued", now, now]

        # Handle optional parent_job_id for chaining
        if parent_job_id := kwargs.get("parent_job_id"):
            columns.append("parent_job_id")
            values.append(parent_job_id)

        # Scraper-specific fields
        if job_type == "scraper":
            columns.extend(["query", "regions", "total_tasks", "done_tasks", "result_count"])
            values.extend([
                kwargs.get("query", ""),
                json.dumps(kwargs.get("regions", {})),
                kwargs.get("total_tasks", 0),
                0,
                0,
            ])

        # Enrichment-specific fields
        elif job_type == "enrichment":
            columns.extend(["total", "processed", "emails_found", "filename", "domain_col", "original_filename",
                           "name_col", "first_name_col", "last_name_col", "cascade_config", "max_results"])
            values.extend([
                kwargs.get("total", 0),
                0,
                0,
                kwargs.get("filename", ""),
                kwargs.get("domain_col", ""),
                kwargs.get("original_filename", ""),
                kwargs.get("name_col", ""),
                kwargs.get("first_name_col", ""),
                kwargs.get("last_name_col", ""),
                kwargs.get("cascade_config", ""),
                kwargs.get("max_results", 5),
            ])

        placeholders = ",".join(["?" for _ in columns])
        self.conn.execute(
            f"INSERT INTO jobs ({','.join(columns)}) VALUES ({placeholders})",
            values,
        )
        self.conn.commit()

    def set_running(self, job_id: str) -> None:
        self.conn.execute(
            "UPDATE jobs SET status='running', updated_at=? WHERE job_id=?",
            (_now(), job_id),
        )
        self.conn.commit()

    def set_done(self, job_id: str, output_path: str) -> None:
        self.conn.execute(
            "UPDATE jobs SET status='done', output_path=?, updated_at=? WHERE job_id=?",
            (output_path, _now(), job_id),
        )
        self.conn.commit()

    def set_failed(self, job_id: str, error: str) -> None:
        self.conn.execute(
            "UPDATE jobs SET status='failed', error=?, updated_at=? WHERE job_id=?",
            (error, _now(), job_id),
        )
        self.conn.commit()

    def append_event(self, job_id: str, seq: int, event: dict[str, Any]) -> None:
        """Append a progress event and update job counters."""
        payload = json.dumps(event)
        c = self.conn
        c.execute(
            "INSERT INTO job_events (job_id, seq, payload) VALUES (?, ?, ?)",
            (job_id, seq, payload),
        )

        # Update job-specific counters
        job = self.get_job(job_id)
        if not job:
            logger.warning(f"Job {job_id} not found when appending event")
            return

        job_type = job.get("job_type")
        updates = ["updated_at = ?"]
        values = [_now(), job_id]

        if job_type == "scraper":
            done_delta = 1 if event.get("task_done") else 0
            result_delta = event.get("new_results", 0)
            updates.extend(["done_tasks = done_tasks + ?", "result_count = result_count + ?"])
            values = [_now(), done_delta, result_delta, job_id]
        elif job_type == "enrichment":
            emails_delta = event.get("emails_found", 0)
            updates.extend(["processed = processed + 1", "emails_found = emails_found + ?"])
            values = [_now(), emails_delta, job_id]

        c.execute(
            f"UPDATE jobs SET {','.join(updates)} WHERE job_id = ?",
            values,
        )
        c.commit()

    def get_job(self, job_id: str) -> Optional[dict[str, Any]]:
        row = self.conn.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
        return dict(row) if row else None

    def delete_job(self, job_id: str) -> bool:
        """Delete a job by ID. Returns True if deleted, False if not found."""
        cursor = self.conn.execute("DELETE FROM jobs WHERE job_id=?", (job_id,))
        self.conn.commit()
        return cursor.rowcount > 0

    def list_jobs(
        self,
        user_id: Optional[str] = None,
        job_type: Optional[str] = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """List jobs with optional filtering by user and job type."""
        conditions = []
        params = []

        if user_id is not None:
            conditions.append("user_id = ?")
            params.append(user_id)

        if job_type is not None:
            conditions.append("job_type = ?")
            params.append(job_type)

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        params.append(limit)

        rows = self.conn.execute(
            f"SELECT * FROM jobs {where_clause} ORDER BY created_at DESC LIMIT ?",
            params,
        ).fetchall()
        return [dict(r) for r in rows]

    def list_all_jobs_with_user(
        self, limit: int = 200, offset: int = 0
    ) -> list[dict[str, Any]]:
        """Admin-only: list all jobs joined with user email."""
        rows = self.conn.execute(
            """
            SELECT j.*, u.email AS user_email
            FROM jobs j
            LEFT JOIN users u ON j.user_id = u.user_id
            ORDER BY j.created_at DESC
            LIMIT ? OFFSET ?
            """,
            (limit, offset),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_events_from(self, job_id: str, from_seq: int) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT payload FROM job_events WHERE job_id=? AND seq>=? ORDER BY seq",
            (job_id, from_seq),
        ).fetchall()
        return [json.loads(r["payload"]) for r in rows]

    def count_events(self, job_id: str) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) as n FROM job_events WHERE job_id=?", (job_id,)
        ).fetchone()
        return row["n"] if row else 0

    def get_stale_running_jobs(self) -> list[str]:
        """Jobs that were 'running' when the server last died — mark them failed on restart."""
        rows = self.conn.execute(
            "SELECT job_id FROM jobs WHERE status IN ('running', 'queued')"
        ).fetchall()
        return [r["job_id"] for r in rows]

    def get_child_jobs(self, parent_job_id: str) -> list[dict[str, Any]]:
        """Get all enrichment jobs that were chained from this scraper job."""
        rows = self.conn.execute(
            "SELECT * FROM jobs WHERE parent_job_id = ? ORDER BY created_at DESC",
            (parent_job_id,),
        ).fetchall()
        return [dict(r) for r in rows]
