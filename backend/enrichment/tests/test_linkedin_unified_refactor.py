"""
Tests for the run_unified_linkedin_enrichment refactor (2026-07-15):

- Progress events use the ``emails_found`` count key (what append_event reads
  to bump jobs.emails_found). The old code used ``email_found`` (bool), so the
  counter never moved and the UI always showed "0 emails".
- Rows with a valid URL where enrichment finds nothing are labeled ``not_found``
  (not ``skipped``). ``skipped`` is reserved for rows with no usable URL.
- The batched-gather refactor processes every row (no early stop) — replacing
  the old sequential for…await loop (effective concurrency 1).
"""
from __future__ import annotations

import asyncio
from unittest import mock

import httpx
import pytest

from enrichment import list_builder


@pytest.fixture
def fake_http_clients() -> tuple[httpx.AsyncClient, httpx.AsyncClient]:
    return httpx.AsyncClient(), httpx.AsyncClient()


def _capture_progress() -> tuple[list[dict], mock.MagicMock]:
    events: list[dict] = []

    def cb(e: dict) -> None:
        events.append(e)

    return events, cb


def _enriched_row(email: str = "", status: str = list_builder.STATUS_ENRICHED) -> dict:
    r = {**list_builder._empty_enriched()}
    r["dm_email"] = email
    r["dm_email_source"] = list_builder.SOURCE_BLITZ_LINKEDIN if email else ""
    r["row_status"] = status
    return r


def test_progress_uses_emails_found_count_key(fake_http_clients) -> None:
    _blitz, _contacts = fake_http_clients
    events, cb = _capture_progress()

    async def fake_single(*a, **kw):
        return _enriched_row("joe@x.com")

    rows = [{"linkedin_url": "https://linkedin.com/in/joe"}]
    with mock.patch.object(list_builder, "_enrich_single_linkedin", fake_single):
        out = asyncio.run(list_builder.run_unified_linkedin_enrichment(
            rows=rows, personal_col="linkedin_url", on_progress=cb,
        ))

    assert out and out[0]["dm_email"] == "joe@x.com"
    assert any(e.get("emails_found") == 1 for e in events), events
    # the old wrong key must be gone
    assert all("email_found" not in e for e in events)


def test_no_result_labeled_not_found_not_skipped(fake_http_clients) -> None:
    _blitz, _contacts = fake_http_clients
    events, cb = _capture_progress()

    async def fake_single(*a, **kw):
        return _enriched_row("", list_builder.STATUS_NOT_FOUND)

    rows = [{"linkedin_url": "https://linkedin.com/in/nobody"}]
    with mock.patch.object(list_builder, "_enrich_single_linkedin", fake_single):
        out = asyncio.run(list_builder.run_unified_linkedin_enrichment(
            rows=rows, personal_col="linkedin_url", on_progress=cb,
        ))

    assert len(out) == 1
    assert out[0]["row_status"] == list_builder.STATUS_NOT_FOUND
    assert any(e.get("status") == list_builder.STATUS_NOT_FOUND for e in events)


def test_skipped_only_when_no_url(fake_http_clients) -> None:
    _blitz, _contacts = fake_http_clients
    events, cb = _capture_progress()

    rows = [{"linkedin_url": ""}]
    out = asyncio.run(list_builder.run_unified_linkedin_enrichment(
        rows=rows, personal_col="linkedin_url", on_progress=cb,
    ))

    assert out[0]["row_status"] == list_builder.STATUS_SKIPPED
    assert any(e.get("status") == list_builder.STATUS_SKIPPED for e in events)


def test_processes_every_row(fake_http_clients) -> None:
    """Batched-gather refactor must process all rows (regression guard for the
    old sequential loop / early-stop partial-output pattern)."""
    _blitz, _contacts = fake_http_clients
    seen: list[str] = []

    async def fake_single(_blitz_http, _contacts_http, _row, url, **kw):
        seen.append(url)
        return _enriched_row(f"user{url[-1]}@x.com")

    rows = [{"linkedin_url": f"https://linkedin.com/in/u{i}"} for i in range(12)]
    with mock.patch.object(list_builder, "_enrich_single_linkedin", fake_single):
        out = asyncio.run(list_builder.run_unified_linkedin_enrichment(
            rows=rows, personal_col="linkedin_url",
        ))

    assert len(out) == 12
    assert len(seen) == 12
    assert all(o["row_status"] == list_builder.STATUS_ENRICHED for o in out)


def test_emails_found_counter_aggregates_across_rows(fake_http_clients) -> None:
    """Sum of 'emails_found' across events equals the number of emails found —
    i.e. the jobs.emails_found counter will be correct."""
    _blitz, _contacts = fake_http_clients
    events, cb = _capture_progress()

    async def fake_single(_b, _c, _row, url, **kw):
        # rows u0,u1,u2 get an email; u3 does not
        email = f"{url[-1]}@x.com" if url[-1] in "012" else ""
        return _enriched_row(email, list_builder.STATUS_ENRICHED if email else list_builder.STATUS_NOT_FOUND)

    rows = [{"linkedin_url": f"https://linkedin.com/in/u{i}"} for i in range(4)]
    with mock.patch.object(list_builder, "_enrich_single_linkedin", fake_single):
        asyncio.run(list_builder.run_unified_linkedin_enrichment(
            rows=rows, personal_col="linkedin_url", on_progress=cb,
        ))

    total_emails = sum(e.get("emails_found", 0) for e in events)
    assert total_emails == 3
