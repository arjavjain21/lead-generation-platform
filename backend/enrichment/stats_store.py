"""
Enrichment source tracking store.

Aggregates email/contact counts by source provider for reporting.
Maps raw source values (e.g., "blitz_email", "contacts_db_linkedin") to provider groups
(contacts_db, blitz, better_enrich, prospeo).

Usage:
    from enrichment.stats_store import EnrichmentStatsStore

    # Aggregate raw sources into provider counts
    source_counts = EnrichmentStatsStore.aggregate_by_provider(["blitz_email", "contacts_db_email"])

    # Record stats for a job
    EnrichmentStatsStore.record_stats(
        job_id="job_123",
        user_id="user_abc",
        source_counts={"blitz": 5, "contacts_db": 10},
        contacts_count=15,
    )

    # Get stats for a specific job
    stats = EnrichmentStatsStore.get_job_stats("job_123")

    # Get aggregated totals
    totals = EnrichmentStatsStore.get_total_stats(user_id="user_abc")
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from shared import db

logger = logging.getLogger(__name__)


# Map raw source values to provider groups
# These source values are set in pipeline.py and list_builder.py when enriching contacts
SOURCE_GROUPS: dict[str, str] = {
    # Contacts DB sources
    "contacts_db_email": "contacts_db",
    "contacts_db_linkedin": "contacts_db",
    "contacts_db_name": "contacts_db",
    "contacts_db_contacts": "contacts_db",
    "contacts_db_domain": "contacts_db",
    # Blitz sources
    "blitz_email": "blitz",
    "blitz_linkedin": "blitz",
    "blitz_contacts": "blitz",
    # WizLeads sources — both `wizleads` and the legacy `wizleads_email`
    # label map to the canonical `wizleads` provider group so historical
    # rows aggregate correctly.
    "wizleads": "wizleads",
    "wizleads_email": "wizleads",
    # SmartProspect sources — both `smartprospect` (unified /enrich path) and
    # `smartprospect_email` (List Building flows path) map to the canonical
    # `smartprospect` provider group so reporting aggregates to one number.
    "smartprospect": "smartprospect",
    "smartprospect_email": "smartprospect",
    # BetterEnrich sources
    "better_enrich_company": "better_enrich",
    "better_enrich_person": "better_enrich",
    "better_enrich_facebook_email": "better_enrich",
    "better_enrich_company_email": "better_enrich",
    # Prospeo sources
    "prospeo": "prospeo",
    "prospeo_person": "prospeo",
}


def normalize_source(source: str) -> str:
    """Normalize a raw source to provider group.

    Args:
        source: Raw source string (e.g., "blitz_email", "contacts_db_contacts")

    Returns:
        Provider group (e.g., "blitz", "contacts_db") or original source if unknown
    """
    return SOURCE_GROUPS.get(source, source)


class EnrichmentStatsStore:
    """Store for enrichment source statistics.

    Tracks email/contact counts by source provider for reporting and analytics.
    """

    @staticmethod
    def _get_connection():
        """Return a database connection."""
        return db.get_db()

    @staticmethod
    def aggregate_by_provider(raw_sources: list[str]) -> dict[str, int]:
        """Aggregate list of raw sources into provider counts.

        Args:
            raw_sources: List of raw source strings (e.g., ["blitz_email", "contacts_db_email"])

        Returns:
            Dict mapping provider group to count (e.g., {"blitz": 1, "contacts_db": 1})

        Example:
            >>> aggregate_by_provider(["blitz_email", "blitz_email", "contacts_db_email"])
            {"blitz": 2, "contacts_db": 1}
        """
        counts: dict[str, int] = {}
        for source in raw_sources:
            provider = normalize_source(source)
            counts[provider] = counts.get(provider, 0) + 1
        return counts

    @classmethod
    def record_stats(
        cls,
        job_id: str,
        user_id: Optional[str],
        source_counts: dict[str, int],
        contacts_count: int = 0,
    ) -> None:
        """Record or update source statistics for a job.

        Uses UPSERT pattern - if stats for (job_id, source) already exist,
        the counts are added to the existing values.

        Args:
            job_id: Job identifier (used to track which job contributed which stats)
            user_id: User who initiated the job (can be None for API-only calls)
            source_counts: Dict of provider -> email count (e.g., {"contacts_db": 5, "blitz": 2})
            contacts_count: Total number of contacts found (added to each source's contacts_count)
        """
        conn = cls._get_connection()
        created_at = datetime.now(timezone.utc).isoformat()

        for source, count in source_counts.items():
            if count > 0:
                conn.execute(
                    """
                    INSERT INTO enrichment_stats (job_id, user_id, source, emails_count, contacts_count, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(job_id, source) DO UPDATE SET
                        emails_count = emails_count + excluded.emails_count,
                        contacts_count = contacts_count + excluded.contacts_count
                    """,
                    (job_id, user_id, source, count, contacts_count, created_at),
                )
        conn.commit()

    @classmethod
    def get_job_stats(cls, job_id: str) -> dict[str, dict[str, int]]:
        """Get source stats for a specific job.

        Args:
            job_id: The job identifier

        Returns:
            Dict mapping source to {"emails": int, "contacts": int}
            Returns empty dict if no stats found for job.

        Example:
            >>> get_job_stats("job_123")
            {"contacts_db": {"emails": 10, "contacts": 15}, "blitz": {"emails": 3, "contacts": 5}}
        """
        conn = cls._get_connection()
        rows = conn.execute(
            "SELECT source, emails_count, contacts_count FROM enrichment_stats WHERE job_id = ?",
            (job_id,),
        ).fetchall()
        return {
            r["source"]: {"emails": r["emails_count"], "contacts": r["contacts_count"]}
            for r in rows
        }

    @classmethod
    def get_total_stats(
        cls,
        user_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> dict[str, int]:
        """Get aggregated stats across all jobs.

        Args:
            user_id: Filter by user (optional). If None, returns all users' stats.
            start_date: Filter by created_at >= this date (ISO format, optional)
            end_date: Filter by created_at <= this date (ISO format, optional)

        Returns:
            Dict mapping source to total email count.
            `wizleads` and `wizleads_email` are aggregated into the
            canonical `wizleads` key so callers see one number.

        Example:
            >>> get_total_stats(user_id="user_abc", start_date="2026-04-01")
            {"contacts_db": 150, "blitz": 45, "better_enrich": 12, "wizleads": 8}
        """
        conn = cls._get_connection()
        conditions = []
        params = []

        if user_id:
            conditions.append("user_id = ?")
            params.append(user_id)
        if start_date:
            conditions.append("created_at >= ?")
            params.append(start_date)
        if end_date:
            conditions.append("created_at <= ?")
            params.append(end_date)

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        rows = conn.execute(
            f"SELECT source, SUM(emails_count) as total_emails FROM enrichment_stats {where} GROUP BY source",
            params,
        ).fetchall()
        # Aggregate wizleads + wizleads_email into canonical wizleads.
        result: dict[str, int] = {}
        for r in rows:
            provider = normalize_source(r["source"])
            result[provider] = result.get(provider, 0) + r["total_emails"]
        return result


def init_table() -> None:
    """Initialize the enrichment_stats table if it doesn't exist.

    This is called automatically by the app on startup via main.py.
    """
    conn = db.get_db()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS enrichment_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT NOT NULL,
            user_id TEXT,
            source TEXT NOT NULL,          -- 'contacts_db', 'blitz', 'better_enrich', 'prospeo'
            emails_count INTEGER DEFAULT 0,
            contacts_count INTEGER DEFAULT 0,
            created_at TEXT NOT NULL,
            UNIQUE(job_id, source)
        )
    """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stats_job ON enrichment_stats (job_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stats_user_date ON enrichment_stats (user_id, created_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stats_source ON enrichment_stats (source)")
    conn.commit()