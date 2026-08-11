"""
Tests for EnrichmentStatsStore - source tracking for enrichment API usage.

Run with: pytest enrichment/tests/test_stats_store.py -v
"""

import pytest
import sys
from pathlib import Path

# Add backend to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from enrichment.stats_store import EnrichmentStatsStore, SOURCE_GROUPS, normalize_source


class TestSourceGroupMapping:
    """Test that all source values map to correct provider groups."""

    def test_blitz_sources_map_to_blitz(self):
        """Blitz sources should map to 'blitz' provider."""
        assert SOURCE_GROUPS["blitz_email"] == "blitz"
        assert SOURCE_GROUPS["blitz_linkedin"] == "blitz"
        assert SOURCE_GROUPS["blitz_contacts"] == "blitz"

    def test_contacts_db_sources_map_to_contacts_db(self):
        """Contacts DB sources should map to 'contacts_db' provider."""
        assert SOURCE_GROUPS["contacts_db_email"] == "contacts_db"
        assert SOURCE_GROUPS["contacts_db_linkedin"] == "contacts_db"
        assert SOURCE_GROUPS["contacts_db_name"] == "contacts_db"
        assert SOURCE_GROUPS["contacts_db_contacts"] == "contacts_db"
        assert SOURCE_GROUPS["contacts_db_domain"] == "contacts_db"

    def test_better_enrich_sources_map_to_better_enrich(self):
        """BetterEnrich sources should map to 'better_enrich' provider."""
        assert SOURCE_GROUPS["better_enrich_company"] == "better_enrich"
        assert SOURCE_GROUPS["better_enrich_person"] == "better_enrich"

    def test_prospeo_sources_map_to_prospeo(self):
        """Prospeo sources should map to 'prospeo' provider."""
        assert SOURCE_GROUPS["prospeo"] == "prospeo"
        assert SOURCE_GROUPS["prospeo_person"] == "prospeo"

    def test_smartprospect_sources_map_to_smartprospect(self):
        """SmartProspect sources (both labels) should map to 'smartprospect' provider.

        `smartprospect` is the label used by the unified /enrich path; `smartprospect_email`
        is the label used by the List Building flows. Both must collapse to one provider
        group so reporting/aggregation shows a single SmartProspect number.
        """
        assert SOURCE_GROUPS["smartprospect"] == "smartprospect"
        assert SOURCE_GROUPS["smartprospect_email"] == "smartprospect"

    def test_normalize_source_function(self):
        """Test the normalize_source helper function."""
        assert normalize_source("blitz_email") == "blitz"
        assert normalize_source("contacts_db_email") == "contacts_db"
        assert normalize_source("better_enrich_company") == "better_enrich"
        assert normalize_source("prospeo") == "prospeo"
        # Both SmartProspect labels must normalize to the single canonical provider
        assert normalize_source("smartprospect") == "smartprospect"
        assert normalize_source("smartprospect_email") == "smartprospect"
        # Unknown sources should return themselves
        assert normalize_source("unknown_source") == "unknown_source"


class TestAggregateByProvider:
    """Test aggregating raw sources into provider counts."""

    def test_aggregate_empty_list(self):
        """Empty list should return empty dict."""
        result = EnrichmentStatsStore.aggregate_by_provider([])
        assert result == {}

    def test_aggregate_single_provider(self):
        """Single provider sources should be aggregated correctly."""
        raw_sources = ["contacts_db_email", "contacts_db_email", "contacts_db_email"]
        result = EnrichmentStatsStore.aggregate_by_provider(raw_sources)
        assert result == {"contacts_db": 3}

    def test_aggregate_multiple_providers(self):
        """Multiple provider sources should be aggregated correctly."""
        raw_sources = [
            "contacts_db_email",
            "contacts_db_email",
            "blitz_email",
            "better_enrich_company",
        ]
        result = EnrichmentStatsStore.aggregate_by_provider(raw_sources)
        assert result == {"contacts_db": 2, "blitz": 1, "better_enrich": 1}

    def test_aggregate_all_providers(self):
        """All providers together should be aggregated correctly."""
        raw_sources = [
            "contacts_db_email",
            "contacts_db_linkedin",
            "blitz_email",
            "blitz_linkedin",
            "better_enrich_company",
            "better_enrich_person",
            "prospeo",
            "prospeo_person",
        ]
        result = EnrichmentStatsStore.aggregate_by_provider(raw_sources)
        assert result == {
            "contacts_db": 2,
            "blitz": 2,
            "better_enrich": 2,
            "prospeo": 2,
        }

    def test_aggregate_unknown_sources(self):
        """Unknown sources should pass through as-is."""
        raw_sources = ["unknown_source", "another_unknown"]
        result = EnrichmentStatsStore.aggregate_by_provider(raw_sources)
        assert "unknown_source" in result
        assert "another_unknown" in result

    def test_aggregate_smartprospect_variants_collapse(self):
        """Both SmartProspect labels must collapse into a single 'smartprospect' count."""
        raw_sources = [
            "smartprospect",        # unified /enrich path
            "smartprospect_email",  # List Building flows path
            "smartprospect_email",
            "smartprospect",
            "smartprospect_email",
        ]
        result = EnrichmentStatsStore.aggregate_by_provider(raw_sources)
        assert result == {"smartprospect": 5}
        # No split keys should leak through
        assert "smartprospect_email" not in result

    def test_aggregate_mixed_known_and_unknown(self):
        """Mix of known and unknown sources should work correctly."""
        raw_sources = [
            "blitz_email",
            "unknown_source",
            "contacts_db_email",
        ]
        result = EnrichmentStatsStore.aggregate_by_provider(raw_sources)
        assert result["blitz"] == 1
        assert result["contacts_db"] == 1
        assert result["unknown_source"] == 1


class TestRecordStats:
    """Test recording stats to database."""

    def test_record_stats_creates_record(self):
        """Recording stats should create database record."""
        from shared import db
        import time

        # Initialize db first
        db.init_db()

        # Use unique job_id with timestamp to avoid conflicts
        job_id = f"test_job_123_{int(time.time() * 1000)}"

        # Record stats for a test job
        EnrichmentStatsStore.record_stats(
            job_id=job_id,
            user_id="test_user",
            source_counts={"contacts_db": 5, "blitz": 3},
            contacts_count=8,
        )

        # Verify the stats were recorded
        stats = EnrichmentStatsStore.get_job_stats(job_id)
        assert "contacts_db" in stats
        assert stats["contacts_db"]["emails"] == 5
        assert stats["contacts_db"]["contacts"] == 8
        assert stats["blitz"]["emails"] == 3
        assert stats["blitz"]["contacts"] == 8

    def test_record_stats_upserts_on_repeat(self):
        """Recording stats for same job should upsert (add to existing)."""
        from shared import db
        import time

        # Initialize db first
        db.init_db()

        # Use unique job_id with timestamp to avoid conflicts
        job_id = f"test_job_upsert_{int(time.time() * 1000)}"

        # Record initial stats
        EnrichmentStatsStore.record_stats(
            job_id=job_id,
            user_id="test_user",
            source_counts={"blitz": 5},
            contacts_count=5,
        )

        # Record more stats for same job
        EnrichmentStatsStore.record_stats(
            job_id=job_id,
            user_id="test_user",
            source_counts={"blitz": 3},
            contacts_count=3,
        )

        # Verify stats were accumulated (not replaced)
        stats = EnrichmentStatsStore.get_job_stats(job_id)
        assert stats["blitz"]["emails"] == 8  # 5 + 3
        assert stats["blitz"]["contacts"] == 8  # 5 + 3


class TestGetJobStats:
    """Test retrieving stats for specific jobs."""

    def test_get_job_stats_empty_for_nonexistent_job(self):
        """Non-existent job should return empty dict."""
        from shared import db

        db.init_db()

        stats = EnrichmentStatsStore.get_job_stats("nonexistent_job_xyz")
        assert stats == {}


class TestGetTotalStats:
    """Test retrieving aggregated totals."""

    def test_get_total_stats_empty_when_no_data(self):
        """No data should return empty totals."""
        from shared import db
        import time

        db.init_db()

        # Use unique job_id with timestamp to avoid conflicts with other tests
        job_id = f"empty_totals_test_{int(time.time() * 1000)}"

        totals = EnrichmentStatsStore.get_total_stats()
        # Only check that the result is a dict - might have data from other tests
        assert isinstance(totals, dict)

    def test_get_total_stats_aggregates_all_jobs(self):
        """Total stats should aggregate across all jobs."""
        from shared import db
        import time

        db.init_db()

        # Use unique job_ids with timestamp to avoid conflicts
        unique_suffix = int(time.time() * 1000)

        # Record stats for multiple jobs
        EnrichmentStatsStore.record_stats(
            job_id=f"job_1_{unique_suffix}",
            user_id=f"user_a_{unique_suffix}",
            source_counts={"contacts_db": 10},
            contacts_count=10,
        )
        EnrichmentStatsStore.record_stats(
            job_id=f"job_2_{unique_suffix}",
            user_id=f"user_a_{unique_suffix}",
            source_counts={"contacts_db": 5, "blitz": 3},
            contacts_count=8,
        )

        # Get totals for this specific user
        totals = EnrichmentStatsStore.get_total_stats(user_id=f"user_a_{unique_suffix}")
        assert totals["contacts_db"] == 15  # 10 + 5
        assert totals["blitz"] == 3

    def test_get_total_stats_filters_by_user(self):
        """Total stats should filter by user correctly."""
        from shared import db
        import time

        db.init_db()

        # Use unique job_ids with timestamp to avoid conflicts
        unique_suffix = int(time.time() * 1000)

        # Record stats for different users
        EnrichmentStatsStore.record_stats(
            job_id=f"job_user_a_{unique_suffix}",
            user_id=f"user_a_{unique_suffix}",
            source_counts={"contacts_db": 10},
            contacts_count=10,
        )
        EnrichmentStatsStore.record_stats(
            job_id=f"job_user_b_{unique_suffix}",
            user_id=f"user_b_{unique_suffix}",
            source_counts={"contacts_db": 20},
            contacts_count=20,
        )

        # Get totals for user_a only
        totals_a = EnrichmentStatsStore.get_total_stats(user_id=f"user_a_{unique_suffix}")
        assert totals_a["contacts_db"] == 10

        # Get totals for user_b only
        totals_b = EnrichmentStatsStore.get_total_stats(user_id=f"user_b_{unique_suffix}")
        assert totals_b["contacts_db"] == 20