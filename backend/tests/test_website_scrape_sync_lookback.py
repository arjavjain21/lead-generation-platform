"""Regression tests for the website-scrape sync lookback re-scan loop.

2026-10-31 incident: the nightly sync ran 18h38m because the lookback loop
pulled a FIXED trailing window with no pagination — whenever the window held
>= batch_size rows (e.g. a bulk-completed tie-group larger than the batch
size), every iteration re-pulled and re-pushed the SAME first page forever,
wedging the jobs.db write lock (WAL → 4.4GB, scraper jobs frozen).

Fix under test:
- build_lookback_query takes a composite (completed_at, id) keyset cursor
- the re-scan loop paginates with that cursor, breaks on a non-advancing
  cursor, and is bounded by a page cap

Run alone:
    cd backend && source venv/bin/activate
    python -m pytest tests/test_website_scrape_sync_lookback.py -q
"""
import asyncio
import logging
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from enrichment import website_scrape_sync as wss


TS = "2026-10-30 12:00:00"


class FakeStore:
    """Duck-typed SyncStateStore — no DB touched."""

    def __init__(self) -> None:
        self.watermark = None
        self.runs: list[dict] = []

    def get_watermark(self):
        return self.watermark

    def set_watermark(self, watermark, watermark_id, rows_pulled, rows_pushed):
        self.watermark = (watermark, watermark_id)

    def record_run(self, **kwargs):
        self.runs.append(kwargs)


class KeysetLookbackPull:
    """Fake remote lookback that honors (completed_at, id) keyset pagination
    exactly like the real ORDER BY completed_at, id LIMIT n query."""

    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows
        self.calls: list[tuple[int, int, object]] = []

    async def __call__(self, hours: int, limit: int, after=None):
        self.calls.append((hours, limit, after))
        filtered = self.rows
        if after is not None:
            ts, row_id = after
            filtered = [r for r in self.rows if (r["completed_at"], r["id"]) > (ts, row_id)]
        return filtered[:limit]


def _row(i: int, completed_at: str = TS) -> dict:
    return {"id": i, "completed_at": completed_at, "domain": f"d{i}.com", "email": f"x@d{i}.com"}


@pytest.fixture(autouse=True)
def _stub_curation(monkeypatch):
    """run_sync imports curation helpers at call time from the module —
    patch them there so no real curation logic runs."""
    from enrichment import website_scrape_curation as curation

    class _Policy:
        def __init__(self, **kwargs):
            pass

    monkeypatch.setattr(curation, "CurationPolicy", _Policy)
    monkeypatch.setattr(curation, "curate_row", lambda row, policy: row)
    monkeypatch.setattr(curation, "build_company_payload", lambda curated, job_id=None: {"id": curated["id"]})
    monkeypatch.setattr(curation, "build_named_contact_payloads", lambda curated, job_id=None: [])


class PushRecorder:
    def __init__(self) -> None:
        self.batches: list[list[dict]] = []

    async def __call__(self, payloads, job_id=None):
        self.batches.append(list(payloads))
        return {"inserted": len(payloads)}


async def _empty_pull(watermark, limit):
    return []


def _run(store, lookback_pull, push):
    return asyncio.run(wss.run_sync(
        enabled=True,
        dry_run=False,
        batch_size=500,
        throttle_rps=1e9,  # skip throttle sleeps
        lookback_hours=48,
        store=store,
        _pull=_empty_pull,
        _push=push,
        _pull_lookback=lookback_pull,
    ))


class TestTieGroupLargerThanBatch:
    def test_terminates_and_pushes_each_row_exactly_once(self):
        """The exact 2026-10-31 wedge: a tie-group (all rows share one
        completed_at) larger than batch_size. 1200 rows / batch 500."""
        rows = [_row(i) for i in range(1, 1201)]  # ids 1..1200, same TS
        pull = KeysetLookbackPull(rows)
        push = PushRecorder()
        store = FakeStore()

        result = _run(store, pull, push)

        # Three pages: 500 + 500 + 200 (short page ends the re-scan)
        assert len(pull.calls) == 3
        assert [c[2] for c in pull.calls] == [None, (TS, 500), (TS, 1000)]
        assert result["lookback_pulled"] == 1200
        # Every row pushed exactly once — no re-push of earlier pages
        pushed_ids = [p["id"] for batch in push.batches for p in batch]
        assert sorted(pushed_ids) == list(range(1, 1201))
        assert result["status"] == "success"

    def test_window_smaller_than_batch_single_pass(self):
        """Short window (< batch_size) still does one pull + one push."""
        rows = [_row(i) for i in range(1, 301)]
        pull = KeysetLookbackPull(rows)
        push = PushRecorder()

        result = _run(FakeStore(), pull, push)

        assert len(pull.calls) == 1
        assert pull.calls[0][2] is None
        assert result["lookback_pulled"] == 300
        assert len(push.batches) == 1


class TestNoProgressGuard:
    def test_non_advancing_cursor_breaks_instead_of_looping(self, caplog):
        """A pull that ignores the cursor (the old behavior) must be caught
        by the no-progress guard on the second iteration."""
        rows = [_row(i) for i in range(1, 501)]

        class StuckPull:
            def __init__(self) -> None:
                self.calls = 0

            async def __call__(self, hours, limit, after=None):
                self.calls += 1
                return rows[:limit]  # always the SAME first page

        pull = StuckPull()
        push = PushRecorder()

        with caplog.at_level(logging.ERROR, logger="enrichment.website_scrape_sync"):
            result = _run(FakeStore(), pull, push)

        assert pull.calls == 2  # second call proves cursor didn't advance
        assert any("did not advance" in r.message for r in caplog.records)
        # The duplicate page was caught BEFORE pushing — only the first
        # page ever got pushed.
        pushed = [p["id"] for batch in push.batches for p in batch]
        assert pushed == list(range(1, 501))
        assert result["lookback_pages"] == 1


class TestPageCap:
    def test_runaway_window_stops_at_cap(self, monkeypatch, caplog):
        """An endless stream of advancing rows (window grows faster than we
        page) is bounded by WEBSITE_SCRAPE_LOOKBACK_MAX_PAGES."""
        monkeypatch.setenv("WEBSITE_SCRAPE_LOOKBACK_MAX_PAGES", "3")

        class EndlessPull:
            def __init__(self) -> None:
                self.next_id = 1

            async def __call__(self, hours, limit, after=None):
                rows = [_row(self.next_id + i) for i in range(limit)]
                self.next_id += limit
                return rows

        pull = EndlessPull()
        push = PushRecorder()

        with caplog.at_level(logging.WARNING, logger="enrichment.website_scrape_sync"):
            result = _run(FakeStore(), pull, push)

        assert result["lookback_pages"] == 3
        assert result["lookback_pulled"] == 1500  # 3 × 500
        assert any("page cap" in r.message for r in caplog.records)


class TestLookbackQuerySQL:
    def test_without_cursor_matches_old_shape(self):
        sql = wss.build_lookback_query(48, 500)
        assert "(completed_at, id) >" not in sql
        assert "interval '48 hours'" in sql
        assert "LIMIT 500" in sql

    def test_with_cursor_adds_keyset_predicate(self):
        sql = wss.build_lookback_query(48, 500, after=(TS, 900))
        assert "(completed_at, id) > ('2026-10-30 12:00:00', 900)" in sql

    def test_injection_attempt_is_rejected_by_validator(self):
        """Malformed/injected cursor timestamps never reach the SQL — the
        watermark validator raises before interpolation (the doubling in
        the f-string is unreachable defense-in-depth)."""
        with pytest.raises(ValueError):
            wss.build_lookback_query(48, 500, after=("2026-01-01 00:00:00' --", 1))

    def test_invalid_cursor_shape_raises(self):
        with pytest.raises(ValueError):
            wss.build_lookback_query(48, 500, after=("not a timestamp", 1))

    def test_garbage_hours_falls_back_to_default(self):
        sql = wss.build_lookback_query("garbage", 10)  # type: ignore[arg-type]
        assert f"interval '{wss.DEFAULT_LOOKBACK_HOURS} hours'" in sql
