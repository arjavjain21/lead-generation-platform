"""Tests for enrichment.website_scrape_sync — the nightly curate-and-push sync
(docs/WEBSITE_SCRAPE_INTEGRATION_PLAN.md).

Pins (updated after the 2026-08-26 adversarial review):
1. COMPOSITE watermark (completed_at, id) — a ts-only anchor re-pulls the
   boundary tie-group forever when one timestamp has ≥ batch_size rows (bulk
   completions). The pair must round-trip the store and the SQL predicate.
2. Watermark shape validation before SQL interpolation (injection + mixed-
   format monotonicity guard). Postgres emits 'YYYY-MM-DD HH:MM:SS[.f]+TZ'.
3. CSV parsing — psql --csv output with tabs/newlines INSIDE fields parses
   correctly (the old -A -F'\\t' tab-split silently corrupted rows).
4. SSH transport — leadgen_sync role, SQL via stdin (never -c/argv), no sudo,
   no DB password in argv, --csv flag present.
5. Dry-run — curates and counts, pushes NOTHING, writes no watermark.
6. Kill-switch — WEBSITE_SCRAPE_SYNC_ENABLED unset/false ⇒ refuse to run.
7. Batch loop — composite watermark advanced per batch, elapsed-aware
   throttle, LoudFailure aborts without watermark advance, queued rows make
   the run 'partial', NULL completed_at raises (never poisons the watermark).
8. Push isolation — pushes ONLY via contacts_writer; no direct contacts SQL.
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

_WM = "2026-08-26 10:00:00+00"


class TestSyncStateStore:
    def test_initial_watermark_is_none(self, tmp_path):
        store = _state_store(tmp_path)
        assert store.get_watermark() is None

    def test_composite_watermark_round_trips(self, tmp_path):
        store = _state_store(tmp_path)
        store.set_watermark(_WM, 4242, rows_pulled=10, rows_pushed=8)
        assert store.get_watermark() == (_WM, 4242)

    def test_composite_watermark_never_regresses(self, tmp_path):
        store = _state_store(tmp_path)
        store.set_watermark(_WM, 100, rows_pulled=5, rows_pushed=5)
        # same ts, smaller id → no regress
        store.set_watermark(_WM, 50, rows_pulled=99, rows_pushed=99)
        assert store.get_watermark() == (_WM, 100)
        # smaller ts → no regress
        store.set_watermark("2026-08-25 09:00:00+00", 999, rows_pulled=1, rows_pushed=1)
        assert store.get_watermark() == (_WM, 100)
        # same ts, bigger id → advances
        store.set_watermark(_WM, 200, rows_pulled=1, rows_pushed=1)
        assert store.get_watermark() == (_WM, 200)

    def test_tie_group_watermark_advances(self, tmp_path):
        """The infinite-loop fix: identical completed_at across MANY rows must
        still advance the watermark (via the id component)."""
        store = _state_store(tmp_path)
        store.set_watermark(_WM, 1, rows_pulled=1, rows_pushed=1)
        store.set_watermark(_WM, 10_000, rows_pulled=1, rows_pushed=1)
        assert store.get_watermark() == (_WM, 10_000)

    def test_migration_from_single_column_watermark(self, tmp_path):
        """An install created before watermark_id existed migrates in place."""
        db = tmp_path / "jobs.db"
        conn = sqlite3.connect(db)
        conn.execute(
            """CREATE TABLE website_scrape_sync_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                watermark TEXT, last_run_at TEXT, last_run_status TEXT,
                rows_pulled INTEGER DEFAULT 0, rows_pushed INTEGER DEFAULT 0,
                skipped_junk INTEGER DEFAULT 0, errors INTEGER DEFAULT 0)"""
        )
        conn.execute("INSERT INTO website_scrape_sync_state (id, watermark) VALUES (1, ?)", (_WM,))
        conn.commit()
        wss.init_state_table(conn)  # must ALTER + not crash
        store = wss.SyncStateStore(conn)
        assert store.get_watermark() == (_WM, 0)  # legacy id defaults to 0

    def test_record_run_outcome(self, tmp_path):
        store = _state_store(tmp_path)
        store.record_run(status="success", rows_pulled=100, rows_pushed=90, skipped_junk=10, errors=0)
        state = store.get_state()
        assert state["last_run_status"] == "success"
        assert state["rows_pulled"] == 100
        assert state["rows_pushed"] == 90


# ---------------------------------------------------------------------------
# Watermark validation + SQL construction
# ---------------------------------------------------------------------------


class TestWatermarkValidation:
    def test_valid_postgres_timestamp_shape(self):
        for wm in (
            "2026-08-26 10:00:00+00",
            "2026-08-06 23:22:17.962021+00",
            "2026-08-26 10:00:00.5+05:30",
            "2026-08-26 10:00:00-08",
        ):
            assert wss.build_pull_query((wm, 5), 10)

    def test_injection_shaped_watermark_rejected(self):
        for bad in ("'; DROP TABLE x; --", "2026-08-26' OR '1'='1", "not a ts", "2026-08-26T10:00:00Z"):
            with pytest.raises(ValueError):
                wss.build_pull_query((bad, 5), 10)

    def test_predicate_carries_composite_pair(self):
        sql = wss.build_pull_query((_WM, 4242), 10)
        assert f"('{_WM}', 4242)" in sql

    def test_null_completed_at_excluded(self):
        sql = wss.build_pull_query(None, 10)
        assert "completed_at IS NOT NULL" in sql


class TestSshTransportSafety:
    def test_uses_leadgen_sync_role_and_no_sudo(self):
        joined = " ".join(wss.build_remote_psql_command("SELECT 1"))
        assert "leadgen_sync" in joined
        assert "sudo" not in joined

    def test_sql_via_stdin_not_argv(self):
        cmd = wss.build_remote_psql_command("SELECT secret FROM t")
        assert not any(a == "-c" for a in cmd)
        assert "SELECT secret FROM t" not in " ".join(cmd)

    def test_no_db_password_in_argv(self):
        for arg in wss.build_remote_psql_command("SELECT 1"):
            assert "PGPASSWORD" not in arg

    def test_csv_output_mode(self):
        joined = " ".join(wss.build_remote_psql_command("SELECT 1"))
        assert "--csv" in joined
        assert "$'" not in joined  # no remote bash-ism

    def test_host_and_db_pinned(self):
        joined = " ".join(wss.build_remote_psql_command("SELECT 1"))
        assert "email_enrichment" in joined
        assert "127.0.0.1" in joined

    def test_keyset_query_initial_backfill(self):
        sql = wss.build_pull_query(None, 10)
        assert "status = 'completed'" in sql
        assert "ORDER BY completed_at, id" in sql
        assert "LIMIT 10" in sql
        assert "gmaps_places" not in sql  # two-phase pull — no remote join

    def test_keyset_query_with_watermark(self):
        sql = wss.build_pull_query((_WM, 7), 10)
        assert "(completed_at, id) >" in sql
        assert ", 7)" in sql


# ---------------------------------------------------------------------------
# CSV parsing
# ---------------------------------------------------------------------------


class TestCsvParsing:
    def test_plain_row(self):
        out = '8,castelloofniantic.com,x@y.com,own_domain,domain_named,0.9,1,completed,Name,Title,ind,"2026-08-06 23:22:17.962021+00",{}\n'
        rows = wss.parse_csv_output(out)
        assert len(rows) == 1 and rows[0][0] == "8"

    def test_tab_inside_quoted_field(self):
        out = '8,dom,a@b.com,own_domain,domain_named,0.9,1,completed,"Na\tme with tab",Title,ind,ts,{}\n'
        rows = wss.parse_csv_output(out)
        assert rows[0][8] == "Na\tme with tab"

    def test_newline_inside_quoted_field(self):
        out = '8,dom,a@b.com,own_domain,domain_named,0.9,1,completed,"Line1\nLine2",Title,ind,ts,{}\n'
        rows = wss.parse_csv_output(out)
        assert len(rows) == 1
        assert rows[0][8] == "Line1\nLine2"

    def test_multiple_rows(self):
        out = "1,d1,a@b.com,own_domain,x,1,1,completed,n,t,i,ts,{}\n2,d2,c@d.com,own_domain,x,1,1,completed,n,t,i,ts,{}\n"
        assert len(wss.parse_csv_output(out)) == 2

    def test_parse_pull_row_full(self):
        fields = ["8", "dom", "a@b.com", "own_domain", "domain_named", "0.9", "1", "completed", "Biz", "Title", "ind", "2026-08-06 23:22:17.962021+00", '{"phone": "+1555"}']
        row = wss.parse_pull_row(fields)
        assert row["id"] == 8
        assert row["email_confidence"] == 0.9
        assert row["metadata"] == {"phone": "+1555"}
        assert row["gmaps"] is None

    def test_parse_pull_row_short_row_safe(self):
        # Column-count mismatch (shouldn't happen with CSV, but never explode).
        row = wss.parse_pull_row(["8", "dom"])
        assert row["id"] == 8
        assert row["metadata"] == {}


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
        assert "''" in sql

    def test_strip_www(self):
        assert wss._strip_www("www.acme.com") == "acme.com"
        assert wss._strip_www("www.www.x.com") == "x.com"
        assert wss._strip_www("acme.com") == "acme.com"


class TestTimeoutSafety:
    def test_remote_psql_timeout_kills_proc(self):
        class FakeProc:
            returncode = None
            killed = False

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
        assert fake.killed is True


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
    def test_batches_pushed_and_composite_watermark_advanced(self, tmp_path):
        store = _state_store(tmp_path)
        rows = [_remote_row(i) for i in range(23)]
        pushed_sizes: list[int] = []

        async def fake_push(payloads, job_id=None):
            pushed_sizes.append(len(payloads))
            return _fake_write_result(len(payloads))

        result = asyncio.run(
            wss.run_sync(
                enabled=True, store=store, _pull=make_fake_pull(rows), _push=fake_push, batch_size=10, throttle_rps=10_000
            )
        )
        assert result["rows_pushed"] == 23
        assert pushed_sizes[0] == 10
        assert result["batches"] == 3
        assert store.get_watermark() == (rows[-1]["completed_at"], rows[-1]["id"])

    def test_tie_group_terminates_and_advances(self, tmp_path):
        """The review's CRITICAL scenario: ≥ batch_size rows sharing ONE
        completed_at. The composite keyset must walk through them by id."""
        store = _state_store(tmp_path)
        rows = [_remote_row(i, completed_at="2026-08-26 10:00:00+00") for i in range(25)]

        async def fake_push(payloads, job_id=None):
            return _fake_write_result(len(payloads))

        result = asyncio.run(
            wss.run_sync(
                enabled=True, store=store, _pull=make_fake_pull(rows), _push=fake_push,
                batch_size=10, throttle_rps=10_000,
            )
        )
        assert result["rows_pulled"] == 25
        assert result["batches"] == 3
        assert store.get_watermark() == ("2026-08-26 10:00:00+00", rows[-1]["id"])

    def test_loud_failure_aborts_without_watermark_advance(self, tmp_path):
        store = _state_store(tmp_path)
        store.set_watermark("2026-08-25 00:00:00+00", 1, rows_pulled=0, rows_pushed=0)
        rows = [_remote_row(i) for i in range(5)]

        async def failing_push(payloads, job_id=None):
            raise wss.LoudFailure("outbox insert failed")

        with pytest.raises(wss.LoudFailure):
            asyncio.run(
                wss.run_sync(
                    enabled=True, store=store, _pull=make_fake_pull(rows), _push=failing_push,
                    batch_size=10, throttle_rps=10_000,
                )
            )
        assert store.get_watermark() == ("2026-08-25 00:00:00+00", 1)

    def test_failed_rows_make_run_partial(self, tmp_path):
        store = _state_store(tmp_path)
        rows = [_remote_row(i) for i in range(4)]

        async def partial_push(payloads, job_id=None):
            return _fake_write_result(0, failed=len(payloads))

        result = asyncio.run(
            wss.run_sync(enabled=True, store=store, _pull=make_fake_pull(rows), _push=partial_push, batch_size=10, throttle_rps=10_000)
        )
        assert result["status"] == "partial"
        assert result["rows_pushed"] == 0
        assert result["rows_failed"] == 4

    def test_queued_rows_make_run_partial(self, tmp_path):
        store = _state_store(tmp_path)
        rows = [_remote_row(i) for i in range(4)]

        async def queued_push(payloads, job_id=None):
            return _fake_write_result(0, queued=len(payloads))

        result = asyncio.run(
            wss.run_sync(enabled=True, store=store, _pull=make_fake_pull(rows), _push=queued_push, batch_size=10, throttle_rps=10_000)
        )
        assert result["status"] == "partial"
        assert result["rows_queued"] == 4

    def test_null_completed_at_raises_not_poisons(self, tmp_path):
        store = _state_store(tmp_path)
        rows = [_remote_row(i) for i in range(3)]
        rows.append(_remote_row(99, completed_at=None))

        async def fake_push(payloads, job_id=None):
            return _fake_write_result(len(payloads))

        with pytest.raises(RuntimeError, match="NULL completed_at"):
            asyncio.run(
                wss.run_sync(enabled=True, store=store, _pull=make_fake_pull(rows), _push=fake_push, batch_size=10, throttle_rps=10_000)
            )

    def test_throttle_sleep_uncapped(self):
        # 500 payloads at 40 rps must want ~12.5s — NOT capped at 1s.
        assert wss.compute_throttle_sleep(500, 40) == pytest.approx(12.5)
        assert wss.compute_throttle_sleep(10, 40) == pytest.approx(0.25)
        assert wss.compute_throttle_sleep(10, 0) == 0.0


# ---------------------------------------------------------------------------
# Push isolation
# ---------------------------------------------------------------------------


class TestPushIsolation:
    def test_module_has_no_direct_contacts_sql(self):
        import inspect

        src = inspect.getsource(wss)
        assert "contacts_writer" in src
        assert "psycopg" not in src
        assert "INSERT INTO core" not in src


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
        "completed_at": f"2026-08-26 10:00:{i:02d}+00",
        "metadata": {},
        "gmaps": None,
    }
    row.update(over)
    return row


def make_fake_pull(rows):
    """Fake pull that keyset-paginates like the real one on (completed_at, id)."""

    async def _pull(watermark, limit):
        start = 0
        if watermark:
            wm_ts, wm_id = watermark
            start = next(
                (
                    i
                    for i, r in enumerate(rows)
                    if (r["completed_at"], r["id"]) > (wm_ts, wm_id)
                ),
                len(rows),
            )
        return rows[start : start + limit]

    return _pull


async def _fail_push(payloads, job_id=None):
    raise AssertionError("push must not be called in this test")


def _fake_write_result(inserted=0, failed=0, queued=0):
    from enrichment.contacts_writer import WriteResult

    return WriteResult(inserted=inserted, updated=0, skipped=0, failed=failed, queued=queued, no_data=0)
