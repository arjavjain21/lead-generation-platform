"""Tests for enrichment/dm_email_backfill.py (idle-time DM email backfill).

Covers the design pins from the module docstring:
* watermark advances only after a fully-processed chunk (pause/error leave it)
* 402 fair-use pause -> EXIT_PAUSED without consuming the chunk
* dry-run writes nothing and stamps no watermark
* payload construction: hit fields win, row fields fall back, domain chain
* DM mirror update rides a CSV + temp table, never interpolated values
"""

from __future__ import annotations

import asyncio
import os
from unittest import mock

import pytest

from enrichment import dm_email_backfill as dmb


def _row(dm_id: str, url: str | None = None) -> dict[str, str]:
    return {
        "dm_id": dm_id,
        "linkedin_url": url or f"https://www.linkedin.com/in/u{dm_id}",
        "first_name": "Row",
        "last_name": "Person",
        "full_name": "Row Person",
        "job_title": "Row Title",
        "headline": "Row Headline",
        "company_website": "https://www.rowcompany.com",
    }


def _hit(email: str = "ann@bcorp.com") -> dict[str, object]:
    return {
        "email": email,
        "first_name": "Ann",
        "last_name": "Bee",
        "person_full_name": "Ann Bee",
        "job_title": "Founder",
        "linkedin_headline": "Founder at BCorp",
        "phone": "+15550001111",
        "verification_status": "Valid",
        "domain": "bcorp.com",
        "company_name": "BCorp",
    }


def _miss() -> dict[str, object]:
    return {"email": None, "first_name": "", "last_name": "", "domain": ""}


class TestPullBatch:
    def test_parses_wellformed_tsv(self):
        lines = "\n".join(
            [
                "11111111-1111-1111-1111-111111111111\thttps://www.linkedin.com/in/a\tA\tB\tA B\tCEO\th\tweb.com",
                "22222222-2222-2222-2222-222222222222\thttps://www.linkedin.com/in/b\tC\tD\tC D\tCTO\th2\t",
            ]
        )
        with mock.patch.object(dmb, "_psql", return_value=lines + "\n"):
            rows = dmb._pull_batch(dmb._ZERO_UUID, 100)
        assert len(rows) == 2
        assert rows[0]["dm_id"] == "11111111-1111-1111-1111-111111111111"
        assert rows[0]["linkedin_url"] == "https://www.linkedin.com/in/a"
        assert rows[1]["company_website"] == ""

    def test_skips_malformed_lines(self):
        lines = "aaa\tbbb\n11111111-1111-1111-1111-111111111111\thttps://x/in/a\tA\tB\tA B\tT\tH\tc.com\n"
        with mock.patch.object(dmb, "_psql", return_value=lines):
            rows = dmb._pull_batch(dmb._ZERO_UUID, 100)
        assert len(rows) == 1

    def test_rejects_non_uuid_watermark(self):
        with pytest.raises(ValueError):
            dmb._pull_batch("'; DROP TABLE core.decision_makers; --", 100)


class TestBuildPayload:
    def test_hit_fields_win_with_row_fallbacks(self):
        row = _row("d1")
        hit = _hit()
        payload = dmb._build_payload(row, hit)
        assert payload["dm_email"] == "ann@bcorp.com"
        assert payload["dm_first_name"] == "Ann"
        assert payload["dm_full_name"] == "Ann Bee"
        assert payload["dm_title"] == "Founder"
        assert payload["dm_linkedin_url"] == row["linkedin_url"]
        assert payload["source_name"] == "getleads_dm_backfill"
        assert payload["dm_email_verified"] == "yes"

    def test_row_fallbacks_used_when_hit_sparse(self):
        row = _row("d2")
        hit = {"email": "x@y.com"}
        payload = dmb._build_payload(row, hit)
        assert payload["dm_first_name"] == "Row"
        assert payload["dm_title"] == "Row Title"
        assert payload["dm_email_verified"] == ""

    def test_domain_preference_chain(self):
        row = _row("d3")
        # hit domain wins
        assert dmb._build_payload(row, _hit())["domain"] == "bcorp.com"
        # company website next
        sparse = {"email": "x@rowcompany.com"}
        assert dmb._build_payload(row, sparse)["domain"] == "rowcompany.com"
        # email domain last, www-stripped
        bare = dict(row, company_website="")
        assert dmb._build_payload(bare, sparse)["domain"] == "rowcompany.com"
        foreign = dict(row, company_website="")
        assert dmb._build_payload(foreign, {"email": "z@zdomain.io"})["domain"] == "zdomain.io"

    def test_does_not_mutate_inputs(self):
        row = _row("d4")
        hit = _hit()
        row_copy, hit_copy = dict(row), dict(hit)
        dmb._build_payload(row, hit)
        assert row == row_copy
        assert hit == hit_copy


class TestApplyDmUpdates:
    def test_noop_on_empty(self):
        with mock.patch.object(dmb, "_psql") as psql:
            assert dmb._apply_dm_updates([]) == 0
        psql.assert_not_called()

    def test_builds_temp_table_update_and_parses_count(self):
        captured: dict[str, object] = {}

        def fake_psql(script: str) -> str:
            captured["script"] = script
            path = script.split("\\copy _dm_email_upd FROM '")[1].split("'")[0]
            with open(path) as fh:
                captured["csv_lines"] = fh.read().splitlines()
            return "UPDATE 2\n"

        with mock.patch.object(dmb, "_psql", side_effect=fake_psql):
            applied = dmb._apply_dm_updates(
                [
                    {"dm_id": "d1", "email": "a@b.com", "phone": "+1"},
                    {"dm_id": "d2", "email": "c@d.com", "phone": ""},
                ]
            )
        assert applied == 2
        script = captured["script"]
        assert "CREATE TEMP TABLE _dm_email_upd" in script
        assert "WITH (FORMAT csv)" in script
        assert "work_email = u.email" in script
        assert "dm.dm_id::text = u.dm_id" in script
        assert captured["csv_lines"] == ["d1,a@b.com,+1", "d2,c@d.com,"]

    def test_falls_back_to_len_when_no_update_tag(self):
        with mock.patch.object(dmb, "_psql", return_value=""):
            assert dmb._apply_dm_updates([{"dm_id": "d", "email": "a@b.c"}]) == 1


class _StateStub:
    """In-memory replacement for the jobs.db state row."""

    def __init__(self, state: dict[str, object]) -> None:
        self.state = dict(state)

    def load(self) -> dict[str, object]:
        return dict(self.state)

    def update(self, sets=None, increments=None) -> None:
        merged = dict(self.state)
        merged.update(sets or {})
        for col, inc in (increments or {}).items():
            merged[col] = merged.get(col, 0) + inc
        self.state = merged


def _install_state(monkeypatch, state: dict[str, object]) -> _StateStub:
    stub = _StateStub(state)
    monkeypatch.setattr(dmb, "_load_state", stub.load)
    monkeypatch.setattr(dmb, "_update_state", stub.update)
    return stub


class TestRunBackfill:
    def _rows(self, count: int, start: int = 0) -> list[dict[str, str]]:
        return [
            _row(f"0000000{start + i:02d}-0000-0000-0000-000000000000")
            for i in range(count)
        ]

    def test_sweep_done_short_circuits(self, monkeypatch):
        _install_state(monkeypatch, {"sweep_status": "done", "last_dm_id": dmb._ZERO_UUID})
        with mock.patch.object(dmb, "_pull_batch") as pull:
            assert asyncio.run(dmb.run_backfill(limit=10, chunk_size=100, dry_run=False)) == dmb.EXIT_OK
        pull.assert_not_called()

    def test_empty_pull_marks_sweep_done(self, monkeypatch):
        stub = _install_state(monkeypatch, {"sweep_status": "running", "last_dm_id": dmb._ZERO_UUID})
        monkeypatch.setattr(dmb, "_pull_batch", lambda watermark, limit: [])
        assert asyncio.run(dmb.run_backfill(limit=10, chunk_size=100, dry_run=False)) == dmb.EXIT_OK
        assert stub.state["sweep_status"] == "done"

    def test_happy_path_writes_and_advances_watermark(self, monkeypatch):
        stub = _install_state(monkeypatch, {"sweep_status": "running", "last_dm_id": dmb._ZERO_UUID})
        rows = self._rows(3)
        monkeypatch.setattr(dmb, "_pull_batch", lambda watermark, limit: list(rows))

        async def lookup(client, urls):
            all_hits = {rows[0]["linkedin_url"]: _hit(), rows[2]["linkedin_url"]: _hit("z@z.io")}
            all_hits[rows[1]["linkedin_url"]] = _miss()
            return "ok", {u: all_hits[u] for u in urls}

        monkeypatch.setattr(dmb, "_lookup_chunk", lookup)
        monkeypatch.setattr(dmb, "_apply_dm_updates", lambda updates: len(updates))
        with mock.patch.object(dmb.contacts_writer, "write_enrichment_result_batch") as write:
            write.return_value.total = 2
            code = asyncio.run(dmb.run_backfill(limit=10, chunk_size=2, dry_run=False))

        assert code == dmb.EXIT_OK
        assert stub.state["last_dm_id"] == rows[-1]["dm_id"]
        assert stub.state["rows_processed"] == 3
        assert stub.state["emails_found"] == 2
        assert stub.state["dm_rows_updated"] == 2
        payloads = [p for c in write.call_args_list for p in c[0][0]]
        assert len(payloads) == 2
        assert payloads[0]["source_name"] == "getleads_dm_backfill"

    def test_duplicate_urls_across_dm_rows_fan_out(self, monkeypatch):
        """Two DM rows sharing one LinkedIn URL both get the email."""
        stub = _install_state(monkeypatch, {"sweep_status": "running", "last_dm_id": dmb._ZERO_UUID})
        shared = "https://www.linkedin.com/in/shared-slug"
        rows = [_row("00000001-0000-0000-0000-000000000000", shared),
                _row("00000002-0000-0000-0000-000000000000", shared)]
        monkeypatch.setattr(dmb, "_pull_batch", lambda watermark, limit: list(rows))

        async def lookup(client, urls):
            return "ok", {shared: _hit()}

        monkeypatch.setattr(dmb, "_lookup_chunk", lookup)
        updates_seen: list[list[dict[str, str]]] = []
        monkeypatch.setattr(dmb, "_apply_dm_updates",
                            lambda updates: (updates_seen.append(list(updates)), len(updates))[1])
        with mock.patch.object(dmb.contacts_writer, "write_enrichment_result_batch") as write:
            write.return_value.total = 2
            asyncio.run(dmb.run_backfill(limit=10, chunk_size=100, dry_run=False))
        assert len(updates_seen[0]) == 2
        assert {u["dm_id"] for u in updates_seen[0]} == {rows[0]["dm_id"], rows[1]["dm_id"]}

    def test_fair_use_pause_keeps_watermark(self, monkeypatch):
        stub = _install_state(monkeypatch, {"sweep_status": "running", "last_dm_id": dmb._ZERO_UUID})
        rows = self._rows(4)
        monkeypatch.setattr(dmb, "_pull_batch", lambda watermark, limit: list(rows))

        async def lookup(client, urls):
            return "paused", {}

        monkeypatch.setattr(dmb, "_lookup_chunk", lookup)
        with mock.patch.object(dmb.contacts_writer, "write_enrichment_result_batch") as write:
            code = asyncio.run(dmb.run_backfill(limit=10, chunk_size=2, dry_run=False))

        assert code == dmb.EXIT_PAUSED
        assert stub.state["last_dm_id"] == dmb._ZERO_UUID
        assert stub.state["last_run_status"] == "paused"
        write.assert_not_called()

    def test_chunk_error_keeps_watermark(self, monkeypatch):
        stub = _install_state(monkeypatch, {"sweep_status": "running", "last_dm_id": dmb._ZERO_UUID})
        monkeypatch.setattr(dmb, "_pull_batch", lambda watermark, limit: self._rows(2))

        async def lookup(client, urls):
            return "error", {}

        monkeypatch.setattr(dmb, "_lookup_chunk", lookup)
        code = asyncio.run(dmb.run_backfill(limit=10, chunk_size=2, dry_run=False))
        assert code == dmb.EXIT_ERROR
        assert stub.state["last_dm_id"] == dmb._ZERO_UUID

    def test_dry_run_writes_nothing_and_keeps_watermark(self, monkeypatch):
        stub = _install_state(monkeypatch, {"sweep_status": "running", "last_dm_id": dmb._ZERO_UUID})
        rows = self._rows(2)
        monkeypatch.setattr(dmb, "_pull_batch", lambda watermark, limit: list(rows))

        async def lookup(client, urls):
            return "ok", {rows[0]["linkedin_url"]: _hit(), rows[1]["linkedin_url"]: _miss()}

        monkeypatch.setattr(dmb, "_lookup_chunk", lookup)
        with mock.patch.object(dmb.contacts_writer, "write_enrichment_result_batch") as write:
            with mock.patch.object(dmb, "_apply_dm_updates") as apply_dm:
                code = asyncio.run(dmb.run_backfill(limit=10, chunk_size=100, dry_run=True))

        assert code == dmb.EXIT_OK
        write.assert_not_called()
        apply_dm.assert_not_called()
        assert stub.state["last_dm_id"] == dmb._ZERO_UUID


class TestMain:
    def test_kill_switch_blocks_run(self, monkeypatch):
        monkeypatch.delenv("DM_EMAIL_BACKFILL_ENABLED", raising=False)
        with mock.patch.object(dmb, "run_backfill") as run:
            assert dmb.main(["--limit", "5"]) == dmb.EXIT_ERROR
        run.assert_not_called()

    def test_missing_api_key_blocks_run(self, monkeypatch):
        monkeypatch.setenv("DM_EMAIL_BACKFILL_ENABLED", "true")
        with mock.patch.object(dmb.getleads_client, "API_KEY", ""):
            with mock.patch.object(dmb, "run_backfill") as run:
                assert dmb.main(["--limit", "5"]) == dmb.EXIT_ERROR
        run.assert_not_called()

    def test_held_lock_blocks_run(self, monkeypatch):
        monkeypatch.setenv("DM_EMAIL_BACKFILL_ENABLED", "true")
        monkeypatch.setattr(dmb.getleads_client, "API_KEY", "k")
        monkeypatch.setattr(dmb, "_acquire_lock", lambda: None)
        with mock.patch.object(dmb, "run_backfill") as run:
            assert dmb.main(["--limit", "5"]) == dmb.EXIT_ERROR
        run.assert_not_called()

    def test_reset_runs_a_fresh_sweep(self, monkeypatch):
        monkeypatch.setenv("DM_EMAIL_BACKFILL_ENABLED", "true")
        monkeypatch.setattr(dmb.getleads_client, "API_KEY", "k")
        monkeypatch.setattr(dmb, "_acquire_lock", lambda: mock.MagicMock())
        with mock.patch.object(dmb, "_reset_sweep") as reset:
            with mock.patch.object(dmb, "run_backfill", return_value=dmb.EXIT_OK):
                assert dmb.main(["--reset"]) == dmb.EXIT_OK
        reset.assert_called_once()

    def test_armed_run_invokes_backfill(self, monkeypatch):
        monkeypatch.setenv("DM_EMAIL_BACKFILL_ENABLED", "true")
        monkeypatch.setattr(dmb.getleads_client, "API_KEY", "k")
        monkeypatch.setattr(dmb, "_acquire_lock", lambda: mock.MagicMock())
        with mock.patch.object(dmb, "run_backfill", return_value=dmb.EXIT_OK) as run:
            assert dmb.main(["--limit", "7", "--chunk", "50"]) == dmb.EXIT_OK
        assert run.call_args.kwargs["limit"] == 7
        assert run.call_args.kwargs["chunk_size"] == 50
