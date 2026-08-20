"""
Scraper-specific job store extending the base class.

Adds scraper-specific operations and ensures proper job_type handling.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional

import sqlite3

from shared import db
from shared.job_store_base import JobStoreBase

logger = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class ScraperJobStore(JobStoreBase):
    """Job store for Google Maps scraper jobs."""

    def __init__(self, conn: Optional[sqlite3.Connection] = None):
        super().__init__(conn)

    def create_scraper_job(
        self,
        job_id: str,
        user_id: str,
        query: str,
        regions: dict[str, Any],
        total_tasks: int,
        parent_job_id: Optional[str] = None,
        display_name: Optional[str] = None,
    ) -> None:
        """Create a new scraper job."""
        self.create_job(
            job_id=job_id,
            user_id=user_id,
            job_type="scraper",
            query=query,
            regions=regions,
            total_tasks=total_tasks,
            parent_job_id=parent_job_id,
            display_name=display_name,
        )

    def list_scraper_jobs(
        self, user_id: Optional[str] = None, limit: int = 100
    ) -> list[dict[str, Any]]:
        """List only scraper jobs."""
        return self.list_jobs(user_id=user_id, job_type="scraper", limit=limit)

    def get_scraper_job(self, job_id: str) -> Optional[dict[str, Any]]:
        """Get a scraper job by ID."""
        job = self.get_job(job_id)
        if job and job.get("job_type") == "scraper":
            return job
        return None

    def get_stale_running_jobs_by_heartbeat(self) -> list[str]:
        """Scraper-only stale-job detection.

        Overrides the base query, which is NOT job_type-scoped: without this,
        the scraper reaper would also abandon stale enrichment/phone jobs at
        startup (it runs first in main.py lifespan). Each job type reaps only
        its own.
        """
        rows = self.conn.execute(
            """SELECT job_id FROM jobs
               WHERE job_type='scraper'
               AND status IN ('running', 'queued')
               AND (datetime(last_heartbeat) IS NULL OR datetime(last_heartbeat) < datetime('now', '-2 minutes'))
               AND datetime(created_at) < datetime('now', '-3 minutes')"""
        ).fetchall()
        return [r["job_id"] for r in rows]


def get_store() -> ScraperJobStore:
    """
    Return a new scraper job store instance with a fresh database connection.

    This ensures each thread gets its own database connection, fixing SQLite
    threading issues where connections can't be shared across threads.
    """
    return ScraperJobStore(db.get_db())
