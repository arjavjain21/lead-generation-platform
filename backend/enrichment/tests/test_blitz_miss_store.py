"""
Tests for enrichment.blitz_miss_store — persistent Blitz miss markers.

Covers the store contract in isolation (the cascade wiring lands in a later
wave and has its own tests):
- Table creation is idempotent (explicit init + lazy first-use)
- Upsert refreshes miss_at, kind AND company_url — never duplicates a domain
- TTL semantics: unexpired marker returns its info dict, expired returns
  None, env override BLITZ_MISS_TTL_DAYS honours, invalid env falls back
  to 30
- company_url caching: recorded alongside 'contacts' misses, returned by
  is_recent_miss, cleared by a URL-less re-record; truthiness of the dict
  return stays hit/miss-compatible with the legacy Optional[str] shape
  (the blitz_batch prepass callback contract)
- Domain normalization (lowercase/strip variants collapse to one row)
- Guards: empty/junk domains and unknown kinds are refused
- Best-effort: every entry point swallows sqlite errors (reads fail open)
- miss_stats shape (stable keys, by_kind pre-seeded, recent_24h window)
- Schema backfill: a pre-company_url table is upgraded via ALTER TABLE
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from enrichment import blitz_miss_store
from shared import db as shared_db


# ---------------------------------------------------------------------------
# Fixtures + helpers
# ---------------------------------------------------------------------------

@pytest.fixture
def fresh_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point shared.db at a temporary SQLite file for test isolation.

    Resets the thread-local cached connection (so get_db() opens the temp
    file) and the store's process-local _schema_initialized flag (so each
    test exercises the CREATE path against ITS temp file).
    """
    tmp_db = tmp_path / "miss_jobs.db"
    monkeypatch.setattr(shared_db, "DB_PATH", tmp_db)
    monkeypatch.setattr(shared_db._local, "conn", None, raising=False)
    monkeypatch.setattr(blitz_miss_store, "_schema_initialized", False)
    yield tmp_db
    # Close the thread-local connection — leaving it cached-closed poisons
    # later tests that reuse shared_db.get_db() on this thread.
    if getattr(shared_db._local, "conn", None) is not None:
        shared_db._local.conn.close()
        shared_db._local.conn = None


def _iso_days_ago(days: float) -> str:
    """Timestamp `days` ago in the store's exact format (_now_iso style)."""
    return (datetime.now(timezone.utc) - timedelta(days=days)).strftime(
        "%Y-%m-%dT%H:%M:%S.%f"
    )[:-3]


def _seed_row(domain: str, kind: str, miss_at: str, company_url: str = "") -> None:
    """Insert a marker row directly, bypassing the public API (TTL tests)."""
    blitz_miss_store.init_table()
    conn = shared_db.get_db()
    conn.execute(
        "INSERT OR REPLACE INTO blitz_domain_miss (domain, kind, miss_at, company_url) "
        "VALUES (?, ?, ?, ?)",
        (domain, kind, miss_at, company_url or None),
    )
    conn.commit()


def _fetch_all_rows(db_path: Path) -> list[tuple]:
    """Read rows via an independent connection (not the thread-local one)."""
    with sqlite3.connect(str(db_path)) as conn:
        return conn.execute(
            "SELECT domain, kind, miss_at, company_url FROM blitz_domain_miss "
            "ORDER BY domain"
        ).fetchall()


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
    def test_table_creation_idempotent(self, fresh_db: Path) -> None:
        blitz_miss_store.init_table()
        blitz_miss_store.init_table()  # second call must not raise
        assert _table_exists(fresh_db, "blitz_domain_miss") == 1
        # Exactly one marker table — no accidental duplicates.
        with sqlite3.connect(str(fresh_db)) as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE name='blitz_domain_miss'"
            ).fetchone()[0]
        assert count == 1

    def test_lazy_first_use_creates_table(self, fresh_db: Path) -> None:
        # No init_table() call — record_miss must create the table itself.
        blitz_miss_store.record_miss("acme.com", "company")
        assert _table_exists(fresh_db, "blitz_domain_miss") == 1
        rows = _fetch_all_rows(fresh_db)
        assert len(rows) == 1
        domain, kind, miss_at, company_url = rows[0]
        assert domain == "acme.com"
        assert kind == "company"
        assert miss_at > _iso_days_ago(days=1 / 24)
        assert company_url is None  # no URL passed -> NULL, not ""

    def test_read_lazy_first_use_creates_table(self, fresh_db: Path) -> None:
        # Reads must also self-create (fail-open must not crash on a fresh DB).
        assert blitz_miss_store.is_recent_miss("acme.com") is None
        assert _table_exists(fresh_db, "blitz_domain_miss") == 1


# ---------------------------------------------------------------------------
# record_miss
# ---------------------------------------------------------------------------

class TestRecordMiss:
    def test_creates_row(self, fresh_db: Path) -> None:
        blitz_miss_store.record_miss("acme.com", "company")
        rows = _fetch_all_rows(fresh_db)
        assert len(rows) == 1
        domain, kind, miss_at, _url = rows[0]
        assert domain == "acme.com"
        assert kind == "company"
        # Timestamp is fresh (within the last minute) and ISO-formatted.
        assert miss_at > _iso_days_ago(days=1 / 24)

    def test_upsert_refreshes_miss_at_and_kind(self, fresh_db: Path) -> None:
        _seed_row("acme.com", "company", _iso_days_ago(days=40))
        blitz_miss_store.record_miss("acme.com", "contacts")
        rows = _fetch_all_rows(fresh_db)
        assert len(rows) == 1  # upsert, never a duplicate
        _, kind, miss_at, _url = rows[0]
        assert kind == "contacts"
        assert miss_at > _iso_days_ago(days=1)  # clock restarted

    def test_upsert_same_kind_restarts_ttl(self, fresh_db: Path) -> None:
        _seed_row("acme.com", "company", _iso_days_ago(days=40))
        blitz_miss_store.record_miss("acme.com", "company")
        # Was expired before the re-record; fresh miss_at makes it live again.
        assert blitz_miss_store.is_recent_miss("acme.com") == {
            "kind": "company", "company_url": ""
        }

    def test_invalid_kind_refused(self, fresh_db: Path) -> None:
        blitz_miss_store.init_table()  # guards return before lazy table creation
        blitz_miss_store.record_miss("acme.com", "bogus")
        assert _fetch_all_rows(fresh_db) == []

    def test_empty_or_junk_domain_refused(self, fresh_db: Path) -> None:
        blitz_miss_store.init_table()  # guards return before lazy table creation
        for bad in ("", "   ", None, "not a domain", "user@example.com"):
            blitz_miss_store.record_miss(bad, "company")  # type: ignore[arg-type]
        assert _fetch_all_rows(fresh_db) == []


# ---------------------------------------------------------------------------
# is_recent_miss
# ---------------------------------------------------------------------------

class TestIsRecentMiss:
    def test_returns_kind_within_ttl(self, fresh_db: Path) -> None:
        blitz_miss_store.record_miss("acme.com", "contacts")
        assert blitz_miss_store.is_recent_miss("acme.com") == {
            "kind": "contacts", "company_url": ""
        }

    def test_unknown_domain_returns_none(self, fresh_db: Path) -> None:
        blitz_miss_store.record_miss("acme.com", "company")
        assert blitz_miss_store.is_recent_miss("nope.com") is None

    def test_empty_domain_returns_none(self, fresh_db: Path) -> None:
        assert blitz_miss_store.is_recent_miss("") is None
        assert blitz_miss_store.is_recent_miss(None) is None  # type: ignore[arg-type]

    def test_ttl_expiry_returns_none(self, fresh_db: Path) -> None:
        # Default TTL is 30 days — a 40-day-old marker is expired.
        _seed_row("acme.com", "company", _iso_days_ago(days=40))
        assert blitz_miss_store.is_recent_miss("acme.com") is None

    def test_just_inside_ttl_returns_kind(self, fresh_db: Path) -> None:
        # 29 days old vs the 30-day default — still a recent miss.
        _seed_row("acme.com", "company", _iso_days_ago(days=29))
        assert blitz_miss_store.is_recent_miss("acme.com")["kind"] == "company"

    def test_ttl_env_override(self, fresh_db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("BLITZ_MISS_TTL_DAYS", "1")
        _seed_row("old.com", "company", _iso_days_ago(days=2))
        _seed_row("new.com", "company", _iso_days_ago(days=0.5))
        assert blitz_miss_store.is_recent_miss("old.com") is None
        assert blitz_miss_store.is_recent_miss("new.com")["kind"] == "company"

    def test_ttl_env_invalid_falls_back_to_default(
        self, fresh_db: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("BLITZ_MISS_TTL_DAYS", "banana")
        _seed_row("old.com", "company", _iso_days_ago(days=31))
        _seed_row("new.com", "company", _iso_days_ago(days=29))
        assert blitz_miss_store.is_recent_miss("old.com") is None
        assert blitz_miss_store.is_recent_miss("new.com")["kind"] == "company"

    def test_ttl_env_below_one_falls_back_to_default(
        self, fresh_db: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("BLITZ_MISS_TTL_DAYS", "0")
        _seed_row("new.com", "company", _iso_days_ago(days=29))
        # TTL 0 is invalid -> default 30 applies instead of expiring instantly.
        assert blitz_miss_store.is_recent_miss("new.com")["kind"] == "company"


# ---------------------------------------------------------------------------
# company_url caching (2026-09-16)
# ---------------------------------------------------------------------------


class TestCompanyUrl:
    def test_recorded_alongside_contacts_miss(self, fresh_db: Path) -> None:
        blitz_miss_store.record_miss(
            "acme.com", "contacts",
            company_url="https://www.linkedin.com/company/acme",
        )
        assert blitz_miss_store.is_recent_miss("acme.com") == {
            "kind": "contacts",
            "company_url": "https://www.linkedin.com/company/acme",
        }

    def test_stripped_before_store(self, fresh_db: Path) -> None:
        blitz_miss_store.record_miss(
            "acme.com", "contacts", company_url="  https://x.co/acme  \t",
        )
        rows = _fetch_all_rows(fresh_db)
        assert rows[0][3] == "https://x.co/acme"

    def test_rerecord_without_url_clears_it(self, fresh_db: Path) -> None:
        """The latest definitive answer wins — a URL-less re-record must not
        leave a stale URL behind (it would misattribute a company page to a
        later 'company' miss)."""
        blitz_miss_store.record_miss(
            "acme.com", "contacts", company_url="https://x.co/acme",
        )
        blitz_miss_store.record_miss("acme.com", "company")
        rows = _fetch_all_rows(fresh_db)
        assert len(rows) == 1
        assert rows[0][1] == "company"
        assert rows[0][3] is None
        assert blitz_miss_store.is_recent_miss("acme.com") == {
            "kind": "company", "company_url": "",
        }

    def test_rerecord_with_url_refreshes_it(self, fresh_db: Path) -> None:
        blitz_miss_store.record_miss("acme.com", "contacts")
        blitz_miss_store.record_miss(
            "acme.com", "contacts", company_url="https://x.co/acme",
        )
        assert blitz_miss_store.is_recent_miss("acme.com") == {
            "kind": "contacts", "company_url": "https://x.co/acme",
        }

    def test_dict_return_truthiness_compatible(self, fresh_db: Path) -> None:
        """The blitz_batch prepass callback contract is ``if is_recent_miss(d):``
        — a hit must stay truthy (non-empty dict) and a miss falsy (None),
        exactly like the legacy Optional[str] shape."""
        blitz_miss_store.record_miss("acme.com", "contacts")
        assert blitz_miss_store.is_recent_miss("acme.com")
        assert not blitz_miss_store.is_recent_miss("nope.com")

    def test_seed_marker_without_url_reads_empty_string(
        self, fresh_db: Path
    ) -> None:
        """Markers written before the column existed (or through the 2-arg
        callback seam) read company_url='' — never crash, never a URL."""
        _seed_row("legacy.com", "contacts", _iso_days_ago(days=1))
        info = blitz_miss_store.is_recent_miss("legacy.com")
        assert info == {"kind": "contacts", "company_url": ""}


# ---------------------------------------------------------------------------
# Schema backfill for pre-company_url tables
# ---------------------------------------------------------------------------


class TestSchemaBackfill:
    def test_legacy_three_column_table_upgraded(self, fresh_db: Path) -> None:
        """A table created by the previous (3-column) build is upgraded via
        ALTER TABLE on first use — record_miss with a URL must not fail."""
        with sqlite3.connect(str(fresh_db)) as conn:
            conn.execute(
                "CREATE TABLE blitz_domain_miss ("
                "domain TEXT PRIMARY KEY, kind TEXT NOT NULL, miss_at TEXT NOT NULL)"
            )
        blitz_miss_store.record_miss(
            "acme.com", "contacts", company_url="https://x.co/acme",
        )
        rows = _fetch_all_rows(fresh_db)
        assert len(rows) == 1
        assert rows[0][3] == "https://x.co/acme"
        assert blitz_miss_store.is_recent_miss("acme.com") == {
            "kind": "contacts", "company_url": "https://x.co/acme",
        }

    def test_legacy_table_read_also_upgrades(self, fresh_db: Path) -> None:
        """A read on a legacy table must self-heal too (fail-open contract
        must not crash on the missing column)."""
        with sqlite3.connect(str(fresh_db)) as conn:
            conn.execute(
                "CREATE TABLE blitz_domain_miss ("
                "domain TEXT PRIMARY KEY, kind TEXT NOT NULL, miss_at TEXT NOT NULL)"
            )
        # Fresh marker via the (upgraded) public API, then a read that must
        # SELECT the backfilled company_url column without crashing.
        blitz_miss_store.record_miss("legacy.com", "company")
        assert blitz_miss_store.is_recent_miss("legacy.com") == {
            "kind": "company", "company_url": "",
        }


# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------

class TestNormalization:
    def test_stored_domain_is_normalized(self, fresh_db: Path) -> None:
        blitz_miss_store.record_miss("  https://WWW.Example.COM/path  ", "company")
        rows = _fetch_all_rows(fresh_db)
        assert [r[0] for r in rows] == ["example.com"]

    def test_lookup_variants_hit_same_row(self, fresh_db: Path) -> None:
        blitz_miss_store.record_miss("Example.COM", "company")
        for variant in ("example.com", "  example.com  ", "EXAMPLE.com"):
            assert blitz_miss_store.is_recent_miss(variant)["kind"] == "company"

    def test_record_variants_collapse_to_one_row(self, fresh_db: Path) -> None:
        blitz_miss_store.record_miss("Example.COM", "company")
        blitz_miss_store.record_miss("  example.com  ", "contacts")
        rows = _fetch_all_rows(fresh_db)
        assert len(rows) == 1
        assert rows[0][1] == "contacts"  # last definitive answer wins


# ---------------------------------------------------------------------------
# Error swallowing (best-effort contract)
# ---------------------------------------------------------------------------

class TestErrorSwallowing:
    def _break_db(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def boom(*args, **kwargs):
            raise sqlite3.OperationalError("database is locked")

        monkeypatch.setattr(shared_db, "get_db", boom)

    def test_record_miss_never_raises(self, fresh_db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        self._break_db(monkeypatch)
        blitz_miss_store.record_miss("acme.com", "company")  # must not raise

    def test_is_recent_miss_fails_open(self, fresh_db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        self._break_db(monkeypatch)
        assert blitz_miss_store.is_recent_miss("acme.com") is None

    def test_miss_stats_returns_zeroed_shape(
        self, fresh_db: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._break_db(monkeypatch)
        assert blitz_miss_store.miss_stats() == {
            "total": 0,
            "by_kind": {"company": 0, "contacts": 0},
            "recent_24h": 0,
        }

    def test_init_table_never_raises(self, fresh_db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        self._break_db(monkeypatch)
        blitz_miss_store.init_table()  # must not raise


# ---------------------------------------------------------------------------
# miss_stats
# ---------------------------------------------------------------------------

class TestMissStats:
    def test_empty_db_shape(self, fresh_db: Path) -> None:
        blitz_miss_store.init_table()
        assert blitz_miss_store.miss_stats() == {
            "total": 0,
            "by_kind": {"company": 0, "contacts": 0},
            "recent_24h": 0,
        }

    def test_stats_counts_and_windows(self, fresh_db: Path) -> None:
        blitz_miss_store.record_miss("fresh1.com", "company")
        blitz_miss_store.record_miss("fresh2.com", "company")
        blitz_miss_store.record_miss("fresh3.com", "contacts")
        # Expired marker: still counted in total/by_kind, NOT in recent_24h.
        _seed_row("stale.com", "company", _iso_days_ago(days=40))
        stats = blitz_miss_store.miss_stats()
        assert stats["total"] == 4
        assert stats["by_kind"] == {"company": 3, "contacts": 1}
        assert stats["recent_24h"] == 3
