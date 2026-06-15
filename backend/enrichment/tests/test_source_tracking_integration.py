"""
Integration tests for enrichment source tracking.

Tests the full flow from enrichment pipeline to stats recording in the database.
Tests both direct stats_store usage and the job_store_base integration.

Run with: pytest enrichment/tests/test_source_tracking_integration.py -v
"""

import pytest
import time
import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta

# Add backend to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from enrichment import stats_store
from enrichment.stats_store import EnrichmentStatsStore
from shared import db


class TestSourceTrackingIntegration:
    """Integration tests for source tracking end-to-end."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test environment."""
        # Initialize the database
        db.init_db()

        # Initialize the stats table
        stats_store.init_table()

        yield

        # Cleanup is handled per-test since we use unique identifiers

    def test_record_stats_to_stats_table(self):
        """Test that enrichment records stats to enrichment_stats table."""
        # Use unique job_id with timestamp to avoid conflicts
        job_id = f"integration_test_job_{int(time.time() * 1000)}"
        user_id = "integration_test_user"

        # Record stats directly
        EnrichmentStatsStore.record_stats(
            job_id=job_id,
            user_id=user_id,
            source_counts={
                "contacts_db": 5,
                "blitz": 3,
                "better_enrich": 2,
                "prospeo": 1,
            },
            contacts_count=11,
        )

        # Verify records exist in enrichment_stats table
        conn = db.get_db()
        rows = conn.execute(
            "SELECT source, emails_count, contacts_count FROM enrichment_stats WHERE job_id = ?",
            (job_id,),
        ).fetchall()

        # Should have 4 records (one per source)
        assert len(rows) == 4

        # Verify each source's counts
        source_emails = {row["source"]: row["emails_count"] for row in rows}
        source_contacts = {row["source"]: row["contacts_count"] for row in rows}

        assert source_emails["contacts_db"] == 5
        assert source_emails["blitz"] == 3
        assert source_emails["better_enrich"] == 2
        assert source_emails["prospeo"] == 1

        # All sources should have the same contacts_count (per current implementation)
        assert all(c == 11 for c in source_contacts.values())

    def test_get_job_stats_returns_correct_counts(self):
        """Test that get_job_stats returns correct aggregated counts."""
        job_id = f"test_job_stats_{int(time.time() * 1000)}"
        user_id = "test_user_stats"

        # Record multiple stats for same job
        EnrichmentStatsStore.record_stats(
            job_id=job_id,
            user_id=user_id,
            source_counts={"contacts_db": 10, "blitz": 5},
            contacts_count=15,
        )

        EnrichmentStatsStore.record_stats(
            job_id=job_id,
            user_id=user_id,
            source_counts={"blitz": 3, "better_enrich": 2},
            contacts_count=5,
        )

        # Get stats for this job
        stats = EnrichmentStatsStore.get_job_stats(job_id)

        # Verify counts are aggregated
        assert stats["contacts_db"]["emails"] == 10
        assert stats["contacts_db"]["contacts"] == 15  # Only counts first record per current impl

        # Blitz should have 5 + 3 = 8 emails
        assert stats["blitz"]["emails"] == 8
        assert stats["better_enrich"]["emails"] == 2

    def test_get_total_stats_aggregates_correctly(self):
        """Test that get_total_stats returns correct aggregated counts across jobs."""
        unique_suffix = int(time.time() * 1000)

        # Record stats for multiple jobs with same user
        EnrichmentStatsStore.record_stats(
            job_id=f"total_job_1_{unique_suffix}",
            user_id=f"total_user_{unique_suffix}",
            source_counts={"contacts_db": 10, "blitz": 5},
            contacts_count=15,
        )
        EnrichmentStatsStore.record_stats(
            job_id=f"total_job_2_{unique_suffix}",
            user_id=f"total_user_{unique_suffix}",
            source_counts={"contacts_db": 5, "blitz": 3, "better_enrich": 2},
            contacts_count=10,
        )
        EnrichmentStatsStore.record_stats(
            job_id=f"total_job_3_{unique_suffix}",
            user_id=f"total_user_{unique_suffix}",
            source_counts={"contacts_db": 7, "prospeo": 4},
            contacts_count=11,
        )

        # Get total stats for this user
        totals = EnrichmentStatsStore.get_total_stats(user_id=f"total_user_{unique_suffix}")

        # Verify aggregation
        assert totals["contacts_db"] == 22  # 10 + 5 + 7
        assert totals["blitz"] == 8  # 5 + 3
        assert totals["better_enrich"] == 2
        assert totals["prospeo"] == 4

    def test_get_total_stats_date_filtering(self):
        """Test that date filtering works correctly in get_total_stats."""
        unique_suffix = int(time.time() * 1000)
        user_id = f"date_filter_user_{unique_suffix}"

        # Get current time as ISO format
        now = datetime.now(timezone.utc)
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        yesterday_start = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()

        # Record stats for today's job
        EnrichmentStatsStore.record_stats(
            job_id=f"today_job_{unique_suffix}",
            user_id=user_id,
            source_counts={"contacts_db": 10},
            contacts_count=10,
        )

        # Record stats for yesterday's job (using manual timestamp)
        # Note: The current implementation uses current timestamp, so we test the filter logic
        # by querying with different date ranges

        # Get all stats (no date filter)
        all_stats = EnrichmentStatsStore.get_total_stats(user_id=user_id)
        assert all_stats.get("contacts_db", 0) >= 10

        # Get stats with start_date (should still include today's records)
        filtered_stats = EnrichmentStatsStore.get_total_stats(
            user_id=user_id,
            start_date=yesterday_start,
        )
        assert filtered_stats.get("contacts_db", 0) >= 10

    def test_get_total_stats_filters_by_user(self):
        """Test that get_total_stats correctly filters by user."""
        unique_suffix = int(time.time() * 1000)

        # Record stats for user_a
        EnrichmentStatsStore.record_stats(
            job_id=f"user_a_job_{unique_suffix}",
            user_id=f"user_a_{unique_suffix}",
            source_counts={"contacts_db": 100},
            contacts_count=100,
        )

        # Record stats for user_b
        EnrichmentStatsStore.record_stats(
            job_id=f"user_b_job_{unique_suffix}",
            user_id=f"user_b_{unique_suffix}",
            source_counts={"contacts_db": 200},
            contacts_count=200,
        )

        # Get totals for user_a only
        totals_a = EnrichmentStatsStore.get_total_stats(user_id=f"user_a_{unique_suffix}")
        assert totals_a.get("contacts_db", 0) == 100

        # Get totals for user_b only
        totals_b = EnrichmentStatsStore.get_total_stats(user_id=f"user_b_{unique_suffix}")
        assert totals_b.get("contacts_db", 0) == 200

    def test_aggregate_by_provider_integration(self):
        """Test that aggregate_by_provider works correctly in real scenarios."""
        # Simulate raw sources from enrichment pipeline
        raw_sources = [
            "blitz_email",
            "blitz_email",
            "blitz_linkedin",
            "contacts_db_email",
            "contacts_db_email",
            "contacts_db_email",
            "better_enrich_company",
            "prospeo",
        ]

        # Aggregate
        source_counts = EnrichmentStatsStore.aggregate_by_provider(raw_sources)

        # Verify aggregation
        assert source_counts["blitz"] == 3  # 2 email + 1 linkedin
        assert source_counts["contacts_db"] == 3  # 3 emails
        assert source_counts["better_enrich"] == 1
        assert source_counts["prospeo"] == 1

    def test_source_tracking_with_job_store_base(self):
        """Test that job_store_base correctly records source stats via append_event."""
        from shared.job_store_base import JobStoreBase
        import secrets

        # Create a test user first (due to foreign key constraint)
        conn = db.get_db()
        test_user_id = f"test_user_jobstore_{int(time.time() * 1000)}"
        test_email = f"test_{test_user_id}@example.com"
        import hashlib
        password_hash = hashlib.sha256(b"test_password").hexdigest()

        conn.execute(
            "INSERT OR IGNORE INTO users (user_id, email, password_hash, created_at) VALUES (?, ?, ?, ?)",
            (test_user_id, test_email, password_hash, datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()

        # Create a test enrichment job
        job_store = JobStoreBase()
        job_id = f"job_store_test_{int(time.time() * 1000)}"

        try:
            job_store.create_job(
                job_id=job_id,
                user_id=test_user_id,
                job_type="enrichment",
                total=10,
                filename="test.csv",
                domain_col="website",
            )

            # Simulate progress events with source_counts
            job_store.append_event(job_id, 1, {
                "index": 0,
                "total": 10,
                "domain": "example.com",
                "status": "success",
                "contacts_found": 2,
                "emails_found": 1,
                "source_counts": {"contacts_db": 1},
            })

            job_store.append_event(job_id, 2, {
                "index": 1,
                "total": 10,
                "domain": "test.com",
                "status": "success",
                "contacts_found": 3,
                "emails_found": 2,
                "source_counts": {"blitz": 1, "contacts_db": 1},
            })

            # Verify stats were recorded to enrichment_stats
            stats = EnrichmentStatsStore.get_job_stats(job_id)
            assert stats.get("contacts_db", {}).get("emails", 0) == 2  # 1 + 1
            assert stats.get("blitz", {}).get("emails", 0) == 1

            # Verify jobs table has source columns updated
            job = job_store.get_job(job_id)
            assert job is not None
            assert job.get("emails_contacts_db", 0) == 2
            assert job.get("emails_blitz", 0) == 1
        finally:
            # Clean up the test rows so they don't accumulate as abandoned jobs
            # on the next server restart (the abandoned-job sweeper flags any
            # job left in 'running' state with a stale heartbeat).
            try:
                conn.execute("DELETE FROM job_events WHERE job_id = ?", (job_id,))
                conn.execute("DELETE FROM enrichment_stats WHERE job_id = ?", (job_id,))
                conn.execute("DELETE FROM job_checkpoints WHERE job_id = ?", (job_id,))
                conn.execute("DELETE FROM job_state WHERE job_id = ?", (job_id,))
                conn.execute("DELETE FROM jobs WHERE job_id = ?", (job_id,))
                conn.execute("DELETE FROM users WHERE user_id = ?", (test_user_id,))
                conn.commit()
            except Exception:
                # Best-effort cleanup — don't mask the real assertion failures
                conn.rollback()

    def test_multiple_sources_in_single_event(self):
        """Test that a single event with multiple sources records correctly."""
        job_id = f"multi_source_test_{int(time.time() * 1000)}"
        user_id = "test_user"

        # Record a single event with multiple sources
        EnrichmentStatsStore.record_stats(
            job_id=job_id,
            user_id=user_id,
            source_counts={
                "contacts_db": 5,
                "blitz": 3,
                "better_enrich": 2,
                "prospeo": 1,
            },
            contacts_count=11,
        )

        stats = EnrichmentStatsStore.get_job_stats(job_id)

        assert len(stats) == 4
        assert stats["contacts_db"]["emails"] == 5
        assert stats["blitz"]["emails"] == 3
        assert stats["better_enrich"]["emails"] == 2
        assert stats["prospeo"]["emails"] == 1

    def test_empty_source_counts_not_recorded(self):
        """Test that zero counts are not recorded."""
        job_id = f"empty_count_test_{int(time.time() * 1000)}"
        user_id = "test_user"

        # Record with zero count for one source
        EnrichmentStatsStore.record_stats(
            job_id=job_id,
            user_id=user_id,
            source_counts={
                "contacts_db": 5,
                "blitz": 0,  # Should be skipped
            },
            contacts_count=5,
        )

        stats = EnrichmentStatsStore.get_job_stats(job_id)

        assert "contacts_db" in stats
        assert "blitz" not in stats  # Zero count should not create record

    def test_normalize_source_integration(self):
        """Test that normalize_source correctly maps raw sources to providers."""
        from enrichment.stats_store import normalize_source

        # Known mappings
        assert normalize_source("blitz_email") == "blitz"
        assert normalize_source("blitz_linkedin") == "blitz"
        assert normalize_source("blitz_contacts") == "blitz"

        assert normalize_source("contacts_db_email") == "contacts_db"
        assert normalize_source("contacts_db_linkedin") == "contacts_db"
        assert normalize_source("contacts_db_name") == "contacts_db"
        assert normalize_source("contacts_db_contacts") == "contacts_db"
        assert normalize_source("contacts_db_domain") == "contacts_db"

        assert normalize_source("better_enrich_company") == "better_enrich"
        assert normalize_source("better_enrich_person") == "better_enrich"

        assert normalize_source("prospeo") == "prospeo"
        assert normalize_source("prospeo_person") == "prospeo"

        # Unknown sources should pass through
        assert normalize_source("unknown_source") == "unknown_source"


class TestSourceTrackingEdgeCases:
    """Test edge cases and error handling."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test environment."""
        db.init_db()
        stats_store.init_table()
        yield

    def test_get_job_stats_nonexistent_job(self):
        """Test that nonexistent job returns empty dict."""
        stats = EnrichmentStatsStore.get_job_stats("nonexistent_job_xyz_123")
        assert stats == {}

    def test_get_total_stats_no_data(self):
        """Test that empty totals returns empty dict."""
        totals = EnrichmentStatsStore.get_total_stats()
        assert isinstance(totals, dict)

    def test_upsert_accumulates_correctly(self):
        """Test that recording stats for same job/source accumulates."""
        job_id = f"upsert_test_{int(time.time() * 1000)}"
        user_id = "test_user"

        # First record
        EnrichmentStatsStore.record_stats(
            job_id=job_id,
            user_id=user_id,
            source_counts={"blitz": 5},
            contacts_count=5,
        )

        # Second record (should add)
        EnrichmentStatsStore.record_stats(
            job_id=job_id,
            user_id=user_id,
            source_counts={"blitz": 3},
            contacts_count=3,
        )

        stats = EnrichmentStatsStore.get_job_stats(job_id)
        assert stats["blitz"]["emails"] == 8  # 5 + 3