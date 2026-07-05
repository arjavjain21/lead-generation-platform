"""
Enrichment-specific job store extending the base class.

Adds enrichment-specific operations and ensures proper job_type handling.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Optional

import sqlite3

from shared import db
from shared.job_store_base import JobStoreBase

logger = logging.getLogger(__name__)


class EnrichmentJobStore(JobStoreBase):
    """Job store for domain enrichment jobs."""

    def __init__(self, conn: Optional[sqlite3.Connection] = None):
        super().__init__(conn)

    def create_enrichment_job(
        self,
        job_id: str,
        user_id: str,
        total: int,
        filename: str = "",
        domain_col: str = "",
        original_filename: str = "",
        parent_job_id: Optional[str] = None,
        name_col: Optional[str] = None,
        first_name_col: Optional[str] = None,
        last_name_col: Optional[str] = None,
        cascade_config: Optional[str] = None,
        max_results: Optional[int] = None,
        selected_providers: Optional[list[str]] = None,
        linkedin_url_col: Optional[str] = None,
        phone_col: Optional[str] = None,
        company_name_col: Optional[str] = None,
        existing_email_col: Optional[str] = None,
        normalize_domains: bool = True,
        dedupe_by_domain: bool = True,
        deduped_rows: int = 0,
        dedupe_skipped_domains: str = "",
    ) -> None:
        """Create a new enrichment job.

        The ``normalize_domains`` and ``dedupe_by_domain`` flags control
        pre-processing that runs before the provider cascade. ``deduped_rows``
        is the count of input rows that were collapsed by dedupe (for
        transparency). ``dedupe_skipped_domains`` is a JSON-encoded list of
        the raw domain values that were dropped, for auditability.
        Defaults preserve prior behavior.
        """
        # Convert list to JSON string for storage
        providers_json = json.dumps(selected_providers) if selected_providers else ""
        # Initialize with contacts_db since internal DB is always queried first
        used_providers_json = json.dumps(["contacts_db"])
        self.create_job(
            job_id=job_id,
            user_id=user_id,
            job_type="enrichment",
            total=total,
            filename=filename,
            domain_col=domain_col,
            original_filename=original_filename,
            parent_job_id=parent_job_id,
            name_col=name_col,
            first_name_col=first_name_col,
            last_name_col=last_name_col,
            cascade_config=cascade_config,
            max_results=max_results,
            selected_providers=providers_json,
            used_providers=used_providers_json,
            linkedin_url_col=linkedin_url_col or "",
            phone_col=phone_col or "",
            company_name_col=company_name_col or "",
            existing_email_col=existing_email_col or "",
            normalize_domains=normalize_domains,
            dedupe_by_domain=dedupe_by_domain,
            deduped_rows=deduped_rows,
            dedupe_skipped_domains=dedupe_skipped_domains,
        )

    def list_enrichment_jobs(
        self, user_id: Optional[str] = None, limit: int = 100
    ) -> list[dict[str, Any]]:
        """List only enrichment jobs."""
        return self.list_jobs(user_id=user_id, job_type="enrichment", limit=limit)

    def get_enrichment_job(self, job_id: str) -> Optional[dict[str, Any]]:
        """Get an enrichment job by ID."""
        job = self.get_job(job_id)
        if job and job.get("job_type") == "enrichment":
            return job
        return None


# Singleton instance for convenience
_default_store: Optional[EnrichmentJobStore] = None


def get_store() -> EnrichmentJobStore:
    """
    Return a new EnrichmentJobStore instance with fresh database connection.

    This ensures each call gets its own database connection for the current thread,
    fixing the threading issue where background tasks couldn't commit progress updates.
    """
    # Remove singleton pattern - create fresh instance each time
    # This ensures each thread gets its own database connection
    return EnrichmentJobStore(db.get_db())
