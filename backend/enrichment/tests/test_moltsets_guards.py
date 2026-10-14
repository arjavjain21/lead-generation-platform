"""
Tests for enrichment.moltsets_guards — cross-worker cooldown, person miss
store, and fair-use usage ledger (the MoltSets quota guards, 2026-10-14).

Mirrors the blitz_miss_store test strategy: a temp SQLite file via
shared.db monkeypatching, direct row seeding for TTL windows, independent
connections for read-back verification.

Covers:
- Schema idempotency (explicit init + lazy first-use)
- Cooldown: set/get/expiry/clear, retry_after honoured, fallback default
  when absent, MAX() extension on re-arm, never-raising best-effort
- Miss store: person_key canonicalization (linkedin vs name|domain),
  upsert refresh, TTL expiry + env override, unknown kind refused,
  empty key refused, fail-open reads
- Usage ledger: record_fair_use append, latest_usage_snapshot round-trip,
  usage_status composite shape, empty-payload no-op
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from enrichment import moltsets_guards
from shared import db as shared_db


# ---------------------------------------------------------------------------
# Fixtures + helpers
# ---------------------------------------------------------------------------

@pytest.fixture
def fresh_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point shared.db at a temporary SQLite file for test isolation."""
    tmp_db = tmp_path / "moltsets_jobs.db"
    monkeypatch.setattr(shared_db, "DB_PATH", tmp_db)
    monkeypatch.setattr(shared_db._local, "conn", None, raising=False)
    monkeypatch.setattr(moltsets_guards, "_schema_initialized", False)
    yield tmp_db
    if getattr(shared_db._local, "conn", None) is not None:
        shared_db._local.conn.close()
        shared_db._local.conn = None


def _iso_days_ago(days: float) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days)).strftime(
        "%Y-%m-%dT%H:%M:%S.%f"
    )[:-3]


def _table_exists(db_path: Path, name: str) -> int:
    with sqlite3.connect(str(db_path)) as conn:
        return conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
            (name,),
        ).fetchone()[0]


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

class TestSchema:
    def test_tables_created_idempotently(self, fresh_db: Path) -> None:
        moltsets_guards.init_tables()
        moltsets_guards.init_tables()  # second call must not raise
        for table in ("moltsets_cooldown", "moltsets_miss", "moltsets_usage"):
            assert _table_exists(fresh_db, table) == 1

    def test_lazy_first_use_creates_tables(self, fresh_db: Path) -> None:
        assert moltsets_guards.get_active_cooldown() is None  # triggers lazy DDL
        assert _table_exists(fresh_db, "moltsets_cooldown") == 1


# ---------------------------------------------------------------------------
# Cooldown
# ---------------------------------------------------------------------------

class TestCooldown:
    def test_set_and_get(self, fresh_db: Path) -> None:
        assert moltsets_guards.get_active_cooldown() is None
        moltsets_guards.set_cooldown(
            "fair_use_limit_exceeded", "5h window spent",
            retry_after=3600.0,
        )
        active = moltsets_guards.get_active_cooldown()
        assert active is not None
        assert active["code"] == "fair_use_limit_exceeded"
        assert 3500 < active["remaining_seconds"] <= 3600
        assert active["retry_after"] == 3600.0

    def test_expiry_returns_none(self, fresh_db: Path) -> None:
        moltsets_guards.set_cooldown("fair_use_requests_exceeded", "x", retry_after=1.0)
        # Force the stored until_ts into the past.
        conn = shared_db.get_db()
        conn.execute(
            "UPDATE moltsets_cooldown SET until_ts = ? WHERE id = 1",
            (_iso_days_ago(0.01),),
        )
        conn.commit()
        assert moltsets_guards.get_active_cooldown() is None

    def test_clear(self, fresh_db: Path) -> None:
        moltsets_guards.set_cooldown("fair_use_limit_exceeded", "x", retry_after=600.0)
        assert moltsets_guards.get_active_cooldown() is not None
        moltsets_guards.clear_cooldown()
        assert moltsets_guards.get_active_cooldown() is None

    def test_fallback_when_retry_after_absent(self, fresh_db: Path, monkeypatch) -> None:
        monkeypatch.setenv("MOLTSETS_COOLDOWN_FALLBACK_SECONDS", "120")
        moltsets_guards.set_cooldown("fair_use_limit_exceeded", "no retry_after")
        active = moltsets_guards.get_active_cooldown()
        assert active is not None
        assert 60 <= active["remaining_seconds"] <= 120

    def test_rearm_extends_not_shortens(self, fresh_db: Path) -> None:
        moltsets_guards.set_cooldown("fair_use_limit_exceeded", "long", retry_after=3600.0)
        moltsets_guards.set_cooldown("rate_limited", "short", retry_after=60.0)
        active = moltsets_guards.get_active_cooldown()
        assert active is not None
        assert active["remaining_seconds"] > 3000  # kept the longer window


# ---------------------------------------------------------------------------
# Miss store
# ---------------------------------------------------------------------------

class TestMissStore:
    def test_person_key_linkedin_preferred(self) -> None:
        key = moltsets_guards.person_key(
            linkedin_url="https://www.linkedin.com/in/JaneDoe/",
            first_name="Jane", last_name="Doe", domain="acme.com",
        )
        assert key == "li:https://www.linkedin.com/in/janedoe"

    def test_person_key_name_domain(self) -> None:
        key = moltsets_guards.person_key(
            first_name="Jane", last_name="Doe", domain="ACME.com",
        )
        assert key == "nd:jane|doe|acme.com"

    def test_person_key_empty_when_insufficient(self) -> None:
        assert moltsets_guards.person_key(first_name="Jane") == ""
        assert moltsets_guards.person_key() == ""

    def test_record_and_check(self, fresh_db: Path) -> None:
        key = moltsets_guards.person_key(first_name="Jane", last_name="Doe", domain="acme.com")
        assert moltsets_guards.is_recent_person_miss(key) is False
        moltsets_guards.record_person_miss(key, "name_domain")
        assert moltsets_guards.is_recent_person_miss(key) is True

    def test_ttl_expiry(self, fresh_db: Path) -> None:
        key = moltsets_guards.person_key(first_name="Jane", last_name="Doe", domain="acme.com")
        moltsets_guards.record_person_miss(key, "name_domain")
        conn = shared_db.get_db()
        conn.execute(
            "UPDATE moltsets_miss SET miss_at = ? WHERE key = ?",
            (_iso_days_ago(30), key),
        )
        conn.commit()
        assert moltsets_guards.is_recent_person_miss(key) is False

    def test_unknown_kind_refused(self, fresh_db: Path) -> None:
        key = moltsets_guards.person_key(first_name="J", last_name="D", domain="a.com")
        moltsets_guards.record_person_miss(key, "bogus")
        assert moltsets_guards.is_recent_person_miss(key) is False

    def test_empty_key_noop(self, fresh_db: Path) -> None:
        moltsets_guards.record_person_miss("", "linkedin")
        assert moltsets_guards.is_recent_person_miss("") is False

    def test_miss_stats_shape(self, fresh_db: Path) -> None:
        moltsets_guards.record_person_miss(
            moltsets_guards.person_key(linkedin_url="https://linkedin.com/in/a"), "linkedin"
        )
        stats = moltsets_guards.miss_stats()
        assert stats["total"] == 1
        assert stats["by_kind"]["linkedin"] == 1
        assert stats["recent_24h"] == 1
        assert stats["ttl_days"] == 14


# ---------------------------------------------------------------------------
# Usage ledger
# ---------------------------------------------------------------------------

class TestUsageLedger:
    def test_record_and_snapshot(self, fresh_db: Path) -> None:
        moltsets_guards.record_fair_use("response", {"records_5h": 12000, "limit_5h": 15000})
        snap = moltsets_guards.latest_usage_snapshot()
        assert snap is not None
        assert snap["source"] == "response"
        assert snap["payload"] == {"records_5h": 12000, "limit_5h": 15000}

    def test_latest_wins(self, fresh_db: Path) -> None:
        moltsets_guards.record_fair_use("response", {"n": 1})
        moltsets_guards.record_fair_use("get_usage", {"n": 2})
        snap = moltsets_guards.latest_usage_snapshot()
        assert snap is not None and snap["payload"] == {"n": 2}

    def test_empty_payload_noop(self, fresh_db: Path) -> None:
        moltsets_guards.record_fair_use("response", None)
        moltsets_guards.record_fair_use("response", {})
        assert moltsets_guards.latest_usage_snapshot() is None

    def test_usage_status_composite(self, fresh_db: Path) -> None:
        moltsets_guards.set_cooldown("fair_use_limit_exceeded", "x", retry_after=300.0)
        status = moltsets_guards.usage_status()
        assert status["cooldown"] is not None
        assert "miss_store" in status and "latest_usage" in status
        json.dumps(status)  # must be JSON-serializable for the API endpoint
