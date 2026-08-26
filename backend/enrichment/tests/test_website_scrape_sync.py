"""Tests for enrichment.website_scrape_sync — the nightly curate-and-push sync
(docs/WEBSITE_SCRAPE_INTEGRATION_PLAN.md).

Pins:
1. Watermark — persisted in jobs.db table website_scrape_sync_state, monotonic
   (never regresses), advanced only after a fully-pushed batch.
2. SSH transport safety — remote psql runs as the locked-down leadgen_sync
   role via TCP+scram (.pgpass on the remote), SQL rides stdin (never -c, never
   argv), never sudo. Keyset pagination via (completed_at, id).
3. Dry-run — reads real rows, curates them, pushes NOTHING, writes no
   watermark. The core safety property of the pre-backfill validation gate.
4. Kill-switch — WEBSITE_SCRAPE_SYNC_ENABLED unset/false (default) makes
   run_sync refuse to run.
5. Batch loop — batches, watermark checkpoint per batch, throttle sleep from
   batch size + RPS, LoudFailure aborts without watermark advance.
6. Push isolation — pushes ONLY via contacts_writer.write_enrichment_result_batch;
   zero direct SQL to the contacts DB.
"""

from __future__ import annotations

import asyncio
import sqlite3
import sys
from pathlib import Path

_BACKEND = Path(__file__).resolve().parents[2]
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

import pytest  # noqa: E402

from enrichment import website_scrape_sync as wss  # noqa: E402


# ---------------------------------------------------------------------------
# Watermark / state store
# ---------------------------------------------------------------------------


class TestSyncStateStore:
    def test_initial_watermark_is_none(self, tmp_path):
        store = _state_store(tmp_path)
        assert store.get_watermark() is None

    def test_watermark_persisted(self, tmp_path):
        store = _state_store(tmp_path)
        store.set_watermark("2026-08-26T10:00:00+00:00", rows_pulled=10, rows_pushed=8)
        assert store.get_watermark() == "2026-08-26T10:00:00+00:00"

    def test_watermark_never_regresses(self, tmp_path):
        store = _state_store(tmp_path)
        store.set_watermark("2026-08-26T11:00:00+00:00", rows_pulled=5, rows_pushed=5)
        store.set_watermark("2026-08-26T10:00:00+00:00", rows_pulled=99, rows_pushed=99)
        assert store.get_watermark() == "2026-08-26T11:00:00+00:00"

    def test_record_run_outcome(self, tmp_path):
        store = _state_store(tmp_path)
        store.record_run(status="success", rows_pulled=100, rows_pushed=90, skipped_junk=10, errors=0)
        state = store.get_state()
        assert state["last_run_status"] == "success"
        assert state["rows_pulled"] == 100
        assert state["rows_pushed"] == 90

    def test_store_works_across_reconnect(self, tmp_path):
        db_path = tmp_path / "jobs.db"
        conn = sqlite3.connect(db_path)
        wss.init_state_table(conn)
        conn.close()
        conn2 = sqlite3.connect(db_path)
        store2 = wss.SyncStateStore(conn2)
        assert store2.get_watermark() is None


# ---------------------------------------------------------------------------
# SSH transport safety
# ---------------------------------------------------------------------------


class TestSshTransportSafety:
    def test_uses_leadgen_sync_role(self):
        joined = " ".join(wss.build_remote_psql_command("SELECT 1"))
        assert "leadgen_sync" in joined
        assert "sudo" not in joined

    def test_sql_via_stdin_not_argv(self):
        cmd = wss.build_remote_psql_command("SELECT secret FROM t")
        assert not any(a == "-c" for a in cmd), "SQL must go via stdin, not -c"
        joined = " ".join(cmd)
        assert "SELECT secret FROM t" not in joined

    def test_no_db_password_in_argv(self):
        cmd = wss.build_remote_psql_command("SELECT 1")
        for arg in cmd:
            assert "PGPASSWORD" not in arg

    def test_host_and_db_pinned(self):
        joined = " ".join(wss.build_remote_psql_command("SELECT 1"))
        assert "email_enrichment" in joined
        assert "127.0.0.1" in joined

    def test_keyset_query_initial_backfill(self):
        sql = wss.build_pull_query(watermark=None, limit=10)
        assert "status = 'completed'" in sql
        assert "ORDER BY completed_at, id" in sql
        assert "LIMIT 10" in sql
        assert "gmaps_places" not in sql  # join removed — two-phase pull

    def test_keyset_query_with_watermark(self):
        wm = "2026-08-26T10:00:00+00:00"
        sql = wss.build_pull_query(watermark=wm, limit=10)
        assert wm in sql
        assert "(completed_at, id) >" in sql


# ---------------------------------------------------------------------------
# Dry-run
# ---------------------------------------------------------------------------


class TestDryRun:
    def test_dry_run_pushes_nothing_and_stamps_no_watermark(self, tmp_path):
        store = _state_store(tmp_path)
        pushed: list[list[dict]] = []

        async def fake_push(payloads, job_id=None):
            pushed.append(payloads)
            return _fake_write_result(len(payloads))

        rows = [_remote_row(i) for i in range(5)]
        result = asyncio.run(
            wss.run_sync(enabled=True, dry_run=True, store=store, _pull=make_fake_pull(rows), _push=fake_push)
        )
        assert result["dry_run"] is True
        assert result["rows_pushed"] == 0
        assert pushed == []
        assert store.get_watermark() is None

    def test_dry_run_still_curates_and_counts(self, tmp_path):
        store = _state_store(tmp_path)
        rows = [_remote_row(i) for i in range(5)]
        rows += [_remote_row(100 + i, email_class="freemail") for i in range(3)]
        result = asyncio.run(
            wss.run_sync(enabled=True, dry_run=True, store=store, _pull=make_fake_pull(rows), _push=_fail_push)
        )
        assert result["rows_pulled"] == 8
        assert result["curated"] == 5
        assert result["skipped_junk"] == 3


# ---------------------------------------------------------------------------
# Kill-switch
# ---------------------------------------------------------------------------


class TestKillSwitch:
    def test_disabled_when_flag_unset(self, tmp_path, monkeypatch):
        monkeypatch.delenv("WEBSITE_SCRAPE_SYNC_ENABLED", raising=False)
        store = _state_store(tmp_path)
        result = asyncio.run(
            wss.run_sync(store=store, _pull=make_fake_pull([_remote_row(1)]), _push=_fail_push)
        )
        assert result["status"] == "disabled"

    def test_disabled_when_flag_false(self, tmp_path, monkeypatch):
        monkeypatch.setenv("WEBSITE_SCRAPE_SYNC_ENABLED", "false")
        store = _state_store(tmp_path)
        result = asyncio.run(
            wss.run_sync(store=store, _pull=make_fake_pull([_remote_row(1)]), _push=_fail_push)
        )
        assert result["status"] == "disabled"

    def test_explicit_enabled_kwarg_runs(self, tmp_path):
        store = _state_store(tmp_path)

        async def fake_push(payloads, job_id=None):
            return _fake_write_result(len(payloads))

        result = asyncio.run(
            wss.run_sync(enabled=True, store=store, _pull=make_fake_pull([_remote_row(1)]), _push=fake_push)
        )
        assert result["status"] == "success"
        assert result["rows_pushed"] >= 1


# ---------------------------------------------------------------------------
# Batch loop
# ---------------------------------------------------------------------------


class TestBatchLoop:
    def test_batches_pushed_and_watermark_advanced(self, tmp_path):
        store = _state_store(tmp_path)
        rows = [_remote_row(i) for i in range(23)]
        pushed_sizes: list[int] = []

        async def fake_push(payloads, job_id=None):
            pushed_sizes.append(len(payloads))
            return _fake_write_result(len(payloads))

        result = asyncio.run(
            wss.run_sync(
                enabled=True, store=store, _pull=make_fake_pull(rows), _push=fake_push, batch_size=10, throttle_rps=1000
            )
        )
        assert result["rows_pushed"] == 23
        assert pushed_sizes[0] == 10  # per-batch payloads (company + named contacts)
        assert result["batches"] == 3
        assert store.get_watermark() == rows[-1]["completed_at"]

    def test_loud_failure_aborts_without_watermark_advance(self, tmp_path):
        store = _state_store(tmp_path)
        store.set_watermark("2026-08-25T00:00:00+00:00", rows_pulled=0, rows_pushed=0)
        rows = [_remote_row(i) for i in range(5)]

        async def failing_push(payloads, job_id=None):
            raise wss.LoudFailure("outbox insert failed")

        with pytest.raises(wss.LoudFailure):
            asyncio.run(
                wss.run_sync(
                    enabled=True,
                    store=store,
                    _pull=make_fake_pull(rows),
                    _push=failing_push,
                    batch_size=10,
                    throttle_rps=1000,
                )
            )
        assert store.get_watermark() == "2026-08-25T00:00:00+00:00"

    def test_failed_rows_counted_not_raised(self, tmp_path):
        store = _state_store(tmp_path)
        rows = [_remote_row(i) for i in range(4)]

        async def partial_push(payloads, job_id=None):
            return _fake_write_result(0, failed=len(payloads))

        result = asyncio.run(
            wss.run_sync(enabled=True, store=store, _pull=make_fake_pull(rows), _push=partial_push, batch_size=10, throttle_rps=1000)
        )
        assert result["status"] == "partial"
        assert result["rows_pushed"] == 0
        assert result["rows_failed"] == 4

    def test_throttle_sleep_computed(self):
        s40 = wss.compute_throttle_sleep(10, 40)
        s75 = wss.compute_throttle_sleep(10, 75)
        assert 0 < s40 <= 1.0
        assert s75 < s40


# ---------------------------------------------------------------------------
# Push isolation
# ---------------------------------------------------------------------------


class TestPushIsolation:
    def test_module_has_no_direct_contacts_sql(self):
        """The sync module must never talk to the contacts DB except via
        contacts_writer. Guard: no psycopg/sqlite connections to contacts,
        no raw upsert SQL strings in module source."""
        import inspect

        src = inspect.getsource(wss)
        assert "contacts_writer" in src
        assert "psycopg" not in src
        assert "INSERT INTO core" not in src
        assert "5432" not in src or "contacts" not in src.split("5432")[1][:200]


class TestGmapsParsing:
    def test_parse_gmaps_row(self):
        fields = ["acme.com", "Austin", "TX", "4.8", "210", '["plumber"]', "https://maps.google.com/?cid=1", "12 Main St", "78701", "US"]
        website, payload = wss.parse_gmaps_row(fields)
        assert website == "acme.com"
        assert payload["city"] == "Austin"
        assert payload["rating"] == 4.8
        assert payload["gmaps_types"] == ["plumber"]

    def test_gmaps_query_in_list_chunks(self):
        sql = wss.build_gmaps_query(["acme.com", "zeta.org"])
        assert "lower(website) IN" in sql
        assert "'acme.com'" in sql and "'zeta.org'" in sql

    def test_gmaps_query_escapes_quotes(self):
        sql = wss.build_gmaps_query(["o'brien.com"])
        assert "''" in sql  # escaped, no injection

    def test_strip_www(self):
        assert wss._strip_www("www.acme.com") == "acme.com"
        assert wss._strip_www("www.www.x.com") == "x.com"
        assert wss._strip_www("acme.com") == "acme.com"


class TestTimeoutSafety:
    def test_remote_psql_timeout_kills_proc(self):
        import asyncio

        async def fake_communicate_raise(*a, **k):
            raise asyncio.TimeoutError()

        # _run_remote_psql kills the process on timeout — verified by contract:
        # a RuntimeError with 'timed out' propagates (not a hang).
        class FakeProc:
            returncode = None

            async def communicate(self, input=None):
                raise asyncio.TimeoutError()

            def kill(self):
                self.killed = True

            async def wait(self):
                return 0

        fake = FakeProc()

        async def run():
            import unittest.mock as mock

            with mock.patch.object(wss.asyncio, "create_subprocess_exec", return_value=fake):
                with mock.patch.object(wss.asyncio, "wait_for", side_effect=asyncio.TimeoutError):
                    with pytest.raises(RuntimeError, match="timed out"):
                        await wss._run_remote_psql("SELECT 1", 1)

        asyncio.run(run())
        assert getattr(fake, "killed", False) is True


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _state_store(tmp_path):
    db = tmp_path / "jobs.db"
    conn = sqlite3.connect(db)
    conn.execute("PRAGMA journal_mode=WAL")
    wss.init_state_table(conn)
    return wss.SyncStateStore(conn)


def _remote_row(i, **over):
    row = {
        "id": i + 1,
        "domain": f"site{i}.com",
        "email": f"owner@site{i}.com",
        "email_class": "own_domain",
        "email_type": "domain_named",
        "email_confidence": 0.9,
        "email_shared_nd": 1,
        "status": "completed",
        "business_name": f"Site {i}",
        "page_title": f"Site {i} — Home",
        "industry": "plumber",
        "completed_at": f"2026-08-26T10:00:{i:02d}+00:00",
        "metadata": {},
        "gmaps": None,
    }
    row.update(over)
    return row


def make_fake_pull(rows):
    """Fake pull that keyset-paginates like the real one: rows are ordered by
    completed_at; each call returns the next `limit` rows after the watermark."""

    async def _pull(watermark, limit):
        start = 0
        if watermark:
            start = next(
                (i for i, r in enumerate(rows) if r["completed_at"] > watermark),
                len(rows),
            )
        return rows[start : start + limit]

    return _pull


async def _fail_push(payloads, job_id=None):
    raise AssertionError("push must not be called in this test")


def _fake_write_result(inserted=0, failed=0):
    from enrichment.contacts_writer import WriteResult

    return WriteResult(inserted=inserted, updated=0, skipped=0, failed=failed, queued=0, no_data=0)
