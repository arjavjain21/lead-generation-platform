"""Tests for the lifetime provider-usage rollup (2026-10-16).

provider_call_log_daily keeps one row per (day, provider, endpoint, status)
FOREVER, so per-provider lifetime usage stays queryable after the raw
provider_call_log rows roll off the RETENTION_DAYS window. purge_old must
never touch either rollup table (provider_call_log_daily or
provider_email_quality_daily).
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from enrichment import call_tracker


@pytest.fixture
def temp_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point call_tracker at a temporary SQLite file and init the schema."""
    db_path = tmp_path / "test_jobs.db"
    monkeypatch.setattr(call_tracker, "DB_PATH", db_path)
    call_tracker.init_schema()
    return db_path


def _fetch_all(db_path: Path, sql: str, params: tuple = ()) -> list[tuple]:
    with sqlite3.connect(db_path) as conn:
        return list(conn.execute(sql, params))


class TestLiveRollupMaintenance:
    def test_each_call_increments_daily_counter(self, temp_db):
        for _ in range(3):
            call_tracker._insert_call_log("blitz", "/v2/enrichment/email", "GET", 200)
        call_tracker._insert_call_log("blitz", "/v2/enrichment/email", "GET", None)

        rows = _fetch_all(
            temp_db,
            "SELECT provider, status, calls FROM provider_call_log_daily",
        )
        by_status = {status: calls for _p, status, calls in rows}
        assert by_status[200] == 3
        assert by_status[0] == 1  # NULL status is stored as 0 in the rollup

    def test_raw_row_still_written(self, temp_db):
        call_tracker._insert_call_log("wizleads", "/email/find-email", "GET", 429)
        raw = _fetch_all(temp_db, "SELECT provider, status FROM provider_call_log")
        assert raw == [("wizleads", 429)]


class TestPurgeSparesRollups:
    def test_purge_deletes_raw_but_keeps_both_rollups(self, temp_db):
        now = call_tracker._now_iso()
        with sqlite3.connect(temp_db) as conn:
            # Raw rows old enough to purge + their rollup counterparts.
            conn.execute(
                "INSERT INTO provider_call_log (ts, provider, endpoint, method, status) "
                "VALUES ('2026-01-01T00:00:00', 'blitz', '/e', 'GET', 200)"
            )
            conn.execute(
                "INSERT INTO provider_call_log_daily "
                "(day, provider, endpoint, status, calls, updated_at) "
                "VALUES ('2026-01-01', 'blitz', '/e', 200, 500, ?)",
                (now,),
            )
            conn.execute(
                "INSERT INTO provider_email_quality_daily "
                "(day, provider, endpoint, email_status, responses, emails, updated_at) "
                "VALUES ('2026-01-01', 'blitz', '/e', 'VALID', 10, 8, ?)",
                (now,),
            )

        deleted = call_tracker.purge_old(days=30)

        assert deleted["call_log"] == 1, "old raw row must be purged"
        assert _fetch_all(temp_db, "SELECT calls FROM provider_call_log_daily") == [(500,)]
        assert _fetch_all(
            temp_db, "SELECT emails FROM provider_email_quality_daily"
        ) == [(8,)]

    def test_purge_keeps_recent_raw_rows(self, temp_db):
        call_tracker._insert_call_log("blitz", "/e", "GET", 200)
        deleted = call_tracker.purge_old(days=30)
        assert deleted["call_log"] == 0
        assert _fetch_all(temp_db, "SELECT COUNT(*) FROM provider_call_log") == [(1,)]


class TestBackfill:
    def test_backfill_matches_raw_counts_and_is_idempotent(self, temp_db):
        with sqlite3.connect(temp_db) as conn:
            conn.executemany(
                "INSERT INTO provider_call_log (ts, provider, endpoint, method, status) "
                "VALUES (?, 'getleads', '/batch', 'POST', ?)",
                [
                    ("2026-10-01T05:00:00", 200),
                    ("2026-10-01T06:00:00", 200),
                    ("2026-10-01T07:00:00", 500),
                    ("2026-10-02T05:00:00", 200),
                ],
            )

        first = call_tracker.backfill_call_log_daily()
        assert first["days"] == 2

        rows = _fetch_all(
            temp_db,
            "SELECT day, status, calls FROM provider_call_log_daily ORDER BY day, status",
        )
        assert rows == [
            ("2026-10-01", 200, 2),
            ("2026-10-01", 500, 1),
            ("2026-10-02", 200, 1),
        ]

        # Running again must not double-count (recount overwrites, not adds).
        call_tracker.backfill_call_log_daily()
        rows_again = _fetch_all(
            temp_db,
            "SELECT day, status, calls FROM provider_call_log_daily ORDER BY day, status",
        )
        assert rows_again == rows

    def test_lifetime_usage_query_pattern(self, temp_db):
        """The query the team needs: lifetime calls per provider, any horizon."""
        with sqlite3.connect(temp_db) as conn:
            conn.executemany(
                "INSERT INTO provider_call_log (ts, provider, endpoint, method, status) "
                "VALUES (?, ?, '/e', 'GET', 200)",
                [
                    ("2026-09-20T05:00:00", "blitz"),
                    ("2026-10-01T05:00:00", "blitz"),
                    ("2026-10-02T05:00:00", "wizleads"),
                ],
            )
        call_tracker.backfill_call_log_daily()

        lifetime = _fetch_all(
            temp_db,
            "SELECT provider, SUM(calls) FROM provider_call_log_daily "
            "GROUP BY provider ORDER BY provider",
        )
        assert lifetime == [("blitz", 2), ("wizleads", 1)]
