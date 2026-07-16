"""
Tests for accurate ``used_providers`` tracking in the LinkedIn Enrich path.

The LinkedIn (by-linkedin-v2) flow invokes the ``record_provider_use``
callback for every provider it actually queries so the job's
``used_providers`` tally is accurate. Previously the LinkedIn path never
wired this callback, so it always reported only the default ``["contacts_db"]``
even though Blitz was also being called.
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


def _recorder() -> tuple[list[str], mock.MagicMock]:
    """Return (seen_list, callback) capturing every provider recorded."""
    seen: list[str] = []

    def _cb(provider: str) -> None:
        seen.append(provider)

    return seen, _cb


def test_single_linkedin_records_contacts_db_on_hit(fake_http_clients) -> None:
    """Contacts DB hit short-circuits Blitz — only contacts_db recorded."""
    blitz_http, contacts_http = fake_http_clients
    seen, cb = _recorder()

    with mock.patch.object(
        list_builder.contacts_client, "person_by_linkedin",
        mock.AsyncMock(return_value={"full_name": "Jane Doe", "email": "jane@x.com"}),
    ), mock.patch.object(
        list_builder.contacts_client, "extract_email_from_contacts_response",
        return_value="jane@x.com",
    ):
        row = asyncio.run(list_builder._enrich_single_linkedin(
            blitz_http, contacts_http,
            {"linkedin_url": "https://linkedin.com/in/jane"},
            "https://linkedin.com/in/jane",
            record_provider_use=cb,
        ))

    assert row["row_status"] == list_builder.STATUS_ENRICHED
    assert "contacts_db" in seen
    assert "blitz" not in seen


def test_single_linkedin_records_blitz_on_fallback(fake_http_clients) -> None:
    """Contacts DB miss + Blitz hit → both providers recorded.

    Also verifies the flat-response parsing fix: /v2/enrichment/email returns
    {found, email, all_emails} (no "person" key), so the email must be read
    from result["email"].
    """
    blitz_http, contacts_http = fake_http_clients
    seen, cb = _recorder()

    with mock.patch.object(
        list_builder.contacts_client, "person_by_linkedin",
        mock.AsyncMock(return_value=None),
    ), mock.patch.object(
        list_builder.blitz_client, "person_enrich_by_linkedin",
        mock.AsyncMock(return_value={
            "found": True,
            "email": "joe@x.com",
            "all_emails": [{"email": "joe@x.com", "verified": True}],
        }),
    ):
        row = asyncio.run(list_builder._enrich_single_linkedin(
            blitz_http, contacts_http,
            {"linkedin_url": "https://linkedin.com/in/joe"},
            "https://linkedin.com/in/joe",
            record_provider_use=cb,
        ))

    assert row["row_status"] == list_builder.STATUS_ENRICHED
    assert row["dm_email"] == "joe@x.com"  # flat-response parsing fix
    assert row["dm_email_source"] == list_builder.SOURCE_BLITZ_LINKEDIN
    assert seen.count("contacts_db") == 1
    assert seen.count("blitz") == 1


def test_single_linkedin_blitz_person_key_is_ignored(fake_http_clients) -> None:
    """Regression guard: a stray "person" key must NOT be read. Only the flat
    {found, email} shape is honored (the old bug read result["person"])."""
    blitz_http, contacts_http = fake_http_clients

    with mock.patch.object(
        list_builder.contacts_client, "person_by_linkedin",
        mock.AsyncMock(return_value=None),
    ), mock.patch.object(
        list_builder.blitz_client, "person_enrich_by_linkedin",
        mock.AsyncMock(return_value={
            "found": True,
            # Old code expected this; correct code must ignore it:
            "person": {"verified_email": "should-not-be-used@x.com"},
            # Correct source of the email:
            "email": "real@x.com",
            "all_emails": [],
        }),
    ):
        row = asyncio.run(list_builder._enrich_single_linkedin(
            blitz_http, contacts_http,
            {"linkedin_url": "https://linkedin.com/in/joe"},
            "https://linkedin.com/in/joe",
        ))

    assert row["dm_email"] == "real@x.com"
    assert "should-not-be-used" not in row["dm_email"]


def test_single_linkedin_records_both_when_neither_hits(fake_http_clients) -> None:
    """Both providers attempted, nothing found → both still recorded."""
    blitz_http, contacts_http = fake_http_clients
    seen, cb = _recorder()

    with mock.patch.object(
        list_builder.contacts_client, "person_by_linkedin",
        mock.AsyncMock(return_value=None),
    ), mock.patch.object(
        list_builder.blitz_client, "person_enrich_by_linkedin",
        mock.AsyncMock(return_value={"found": False, "person": None}),
    ):
        row = asyncio.run(list_builder._enrich_single_linkedin(
            blitz_http, contacts_http,
            {"linkedin_url": "https://linkedin.com/in/nobody"},
            "https://linkedin.com/in/nobody",
            record_provider_use=cb,
        ))

    assert row["row_status"] == list_builder.STATUS_NOT_FOUND
    assert "contacts_db" in seen
    assert "blitz" in seen


def test_unified_records_blitz_for_company_url(fake_http_clients) -> None:
    """Company-URL branch uses the Blitz title-waterfall → blitz recorded."""
    _blitz_http, _contacts_http = fake_http_clients
    seen, cb = _recorder()

    rows = [{"company": "https://linkedin.com/company/acme"}]

    with mock.patch.object(
        list_builder, "_enrich_by_company_waterfall",
        mock.AsyncMock(return_value=[{
            "first_name": "A", "last_name": "B", "full_name": "A B",
            "title": "CEO", "email": "a@acme.com", "verified_email": "a@acme.com",
        }]),
    ):
        out = asyncio.run(list_builder.run_unified_linkedin_enrichment(
            rows=rows,
            company_col="company",
            record_provider_use=cb,
        ))

    assert out
    assert "blitz" in seen


def test_no_callback_does_not_crash(fake_http_clients) -> None:
    """Omitting record_provider_use (default None) must not raise."""
    blitz_http, contacts_http = fake_http_clients

    with mock.patch.object(
        list_builder.contacts_client, "person_by_linkedin",
        mock.AsyncMock(return_value=None),
    ), mock.patch.object(
        list_builder.blitz_client, "person_enrich_by_linkedin",
        mock.AsyncMock(return_value={"found": False, "person": None}),
    ):
        row = asyncio.run(list_builder._enrich_single_linkedin(
            blitz_http, contacts_http,
            {"linkedin_url": "https://linkedin.com/in/x"},
            "https://linkedin.com/in/x",
        ))

    assert row["row_status"] == list_builder.STATUS_NOT_FOUND
