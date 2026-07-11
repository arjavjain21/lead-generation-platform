"""
Tests for enrichment.call_tracker — provider HTTP call observability.

Covers:
- Schema creation is idempotent
- Response hook fires and records the row
- Host filtering (untracked hosts are ignored)
- Defensive behaviour: DB errors / exceptions in the hook never propagate
- Monkeypatch installs exactly once and is idempotent
- purge_old deletes only old rows
- Counts_since returns expected aggregations
"""

from __future__ import annotations

import asyncio
import sqlite3
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import httpx
import pytest

from enrichment import call_tracker


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def temp_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point call_tracker at a temporary SQLite file for test isolation."""
    db_path = tmp_path / "test_jobs.db"
    monkeypatch.setattr(call_tracker, "DB_PATH", db_path)
    return db_path


@pytest.fixture
def installed(temp_db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Init schema + reset install state + install globally.

    Resets the module-level _installed flag so each test gets a clean install.
    Relies on call_tracker._ORIGINAL_ASYNC_INIT (captured at module import)
    being the true original AsyncClient.__init__.
    """
    # Force fresh install for the test, then restore afterwards.
    monkeypatch.setattr(call_tracker, "_installed", False)
    call_tracker.init()
    yield
    # Restore the original __init__ so we don't leak the patch into other tests.
    httpx.AsyncClient.__init__ = call_tracker._ORIGINAL_ASYNC_INIT  # type: ignore[method-assign]
    monkeypatch.setattr(call_tracker, "_installed", False)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

def test_schema_creates_table(temp_db: Path) -> None:
    call_tracker.init_schema()
    with sqlite3.connect(temp_db) as conn:
        tables = {
            r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
    assert "provider_call_log" in tables


def test_schema_is_idempotent(temp_db: Path) -> None:
    call_tracker.init_schema()
    call_tracker.init_schema()  # second call must not raise
    with sqlite3.connect(temp_db) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE name='provider_call_log'"
        ).fetchone()[0]
    assert count == 1


def test_schema_creates_indexes(temp_db: Path) -> None:
    call_tracker.init_schema()
    with sqlite3.connect(temp_db) as conn:
        indexes = {
            r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='provider_call_log'"
            )
        }
    assert "idx_pcl_ts" in indexes
    assert "idx_pcl_provider_endpoint_ts" in indexes


# ---------------------------------------------------------------------------
# Hook behaviour
# ---------------------------------------------------------------------------

def _make_response(host: str, path: str, method: str = "POST", status: int = 200) -> httpx.Response:
    """Build a minimal httpx.Response for testing without real HTTP."""
    request = httpx.Request(method, f"https://{host}{path}")
    return httpx.Response(status_code=status, request=request)


def test_hook_records_tracked_host(installed: None) -> None:
    asyncio.run(call_tracker._on_response(_make_response("api.blitz-api.ai", "/v2/enrichment/person")))
    with sqlite3.connect(call_tracker.DB_PATH) as conn:
        rows = conn.execute(
            "SELECT provider, endpoint, method, status FROM provider_call_log"
        ).fetchall()
    assert rows == [("blitz", "/v2/enrichment/person", "POST", 200)]


def test_hook_records_better_enrich(installed: None) -> None:
    asyncio.run(call_tracker._on_response(_make_response("app.betterenrich.com", "/api/v1/find-company-email", "POST", 403)))
    with sqlite3.connect(call_tracker.DB_PATH) as conn:
        rows = conn.execute(
            "SELECT provider, endpoint, status FROM provider_call_log"
        ).fetchall()
    assert rows == [("better_enrich", "/api/v1/find-company-email", 403)]


def test_hook_ignores_untracked_host(installed: None) -> None:
    asyncio.run(call_tracker._on_response(_make_response("api.unknown.com", "/foo")))
    with sqlite3.connect(call_tracker.DB_PATH) as conn:
        count = conn.execute("SELECT COUNT(*) FROM provider_call_log").fetchone()[0]
    assert count == 0


def test_hook_case_insensitive_host(installed: None) -> None:
    asyncio.run(call_tracker._on_response(_make_response("API.Blitz-API.ai", "/v2/foo")))
    with sqlite3.connect(call_tracker.DB_PATH) as conn:
        count = conn.execute("SELECT COUNT(*) FROM provider_call_log").fetchone()[0]
    assert count == 1


def test_hook_is_defensive_when_db_unavailable(temp_db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """If the DB connect raises, the hook must still return None (not raise)."""
    monkeypatch.setattr(call_tracker, "_installed", False)
    call_tracker.init()

    def _boom(*_a, **_k):
        raise sqlite3.OperationalError("simulated lock")

    monkeypatch.setattr(call_tracker, "_connect", _boom)
    # Must not raise:
    asyncio.run(call_tracker._on_response(_make_response("api.blitz-api.ai", "/x")))


def test_hook_is_defensive_on_malformed_request(installed: None) -> None:
    """If response.request.url.host returns None (e.g. relative URL), skip silently."""
    request = httpx.Request("GET", "https://api.blitz-api.ai/x")
    response = httpx.Response(200, request=request)
    # Force host to None to exercise the early-return path
    with mock.patch.object(
        type(response.request.url), "host", new_callable=mock.PropertyMock, return_value=None
    ):
        asyncio.run(call_tracker._on_response(response))


# ---------------------------------------------------------------------------
# Monkeypatch behaviour
# ---------------------------------------------------------------------------

def test_install_globally_adds_hook_to_new_clients(installed: None) -> None:
    client = httpx.AsyncClient()
    try:
        assert call_tracker._on_response in client.event_hooks["response"]
    finally:
        asyncio.run(client.aclose())


def test_install_globally_does_not_clobber_existing_hooks(temp_db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """If caller supplies their own response hooks, ours is appended, not replaced."""
    monkeypatch.setattr(call_tracker, "_installed", False)
    call_tracker.install_globally()

    sentinel = mock.AsyncMock()
    client = httpx.AsyncClient(event_hooks={"response": [sentinel]})
    try:
        assert sentinel in client.event_hooks["response"]
        assert call_tracker._on_response in client.event_hooks["response"]
        # Order: caller's hook first, ours appended.
        assert client.event_hooks["response"].index(sentinel) < client.event_hooks["response"].index(
            call_tracker._on_response
        )
    finally:
        asyncio.run(client.aclose())


def test_install_globally_is_idempotent(monkeypatch: pytest.MonkeyPatch, temp_db: Path) -> None:
    monkeypatch.setattr(call_tracker, "_installed", False)
    call_tracker.install_globally()
    first_init = call_tracker._ORIGINAL_ASYNC_INIT

    call_tracker.install_globally()  # second call must be no-op
    second_init = call_tracker._ORIGINAL_ASYNC_INIT
    assert first_init is second_init
    assert call_tracker._installed is True


def test_install_globally_does_not_double_add_hook(installed: None) -> None:
    """Repeated AsyncClient constructions all get exactly one tracker hook."""
    c1 = httpx.AsyncClient()
    c2 = httpx.AsyncClient()
    try:
        assert c1.event_hooks["response"].count(call_tracker._on_response) == 1
        assert c2.event_hooks["response"].count(call_tracker._on_response) == 1
    finally:
        asyncio.run(c1.aclose())
        asyncio.run(c2.aclose())


# ---------------------------------------------------------------------------
# Retention
# ---------------------------------------------------------------------------

def test_purge_old_deletes_only_old_rows(installed: None) -> None:
    # Insert an old row (40 days ago) and a fresh row (now).
    with sqlite3.connect(call_tracker.DB_PATH) as conn:
        old_ts = (datetime.now(timezone.utc) - timedelta(days=40)).strftime(
            "%Y-%m-%dT%H:%M:%S.%f"
        )
        fresh_ts = call_tracker._now_iso()
        conn.execute(
            "INSERT INTO provider_call_log (ts, provider, endpoint, method, status) "
            "VALUES (?, 'blitz', '/old', 'POST', 200)", (old_ts,)
        )
        conn.execute(
            "INSERT INTO provider_call_log (ts, provider, endpoint, method, status) "
            "VALUES (?, 'blitz', '/fresh', 'POST', 200)", (fresh_ts,)
        )
    deleted = call_tracker.purge_old(days=30)
    assert deleted == {"call_log": 1, "email_ledger": 0}
    with sqlite3.connect(call_tracker.DB_PATH) as conn:
        remaining = conn.execute("SELECT endpoint FROM provider_call_log").fetchall()
    assert remaining == [("/fresh",)]


def test_purge_old_returns_zero_when_nothing_to_delete(installed: None) -> None:
    assert call_tracker.purge_old(days=30) == {"call_log": 0, "email_ledger": 0}


# ---------------------------------------------------------------------------
# Counts helper
# ---------------------------------------------------------------------------

def test_counts_since_returns_expected_aggregation(installed: None) -> None:
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=1)).strftime(
        "%Y-%m-%dT%H:%M:%S.%f"
    )
    asyncio.run(call_tracker._on_response(_make_response("api.blitz-api.ai", "/v2/a", "POST", 200)))
    asyncio.run(call_tracker._on_response(_make_response("api.blitz-api.ai", "/v2/a", "POST", 200)))
    asyncio.run(call_tracker._on_response(_make_response("api.blitz-api.ai", "/v2/a", "POST", 429)))
    asyncio.run(call_tracker._on_response(_make_response("app.betterenrich.com", "/x", "POST", 403)))

    counts = call_tracker.counts_since(cutoff)
    assert counts[("blitz", "/v2/a", 200)] == 2
    assert counts[("blitz", "/v2/a", 429)] == 1
    assert counts[("better_enrich", "/x", 403)] == 1


# ---------------------------------------------------------------------------
# End-to-end through a real httpx.AsyncClient transport (no network)
# ---------------------------------------------------------------------------

def test_end_to_end_with_mock_transport(installed: None) -> None:
    """A real httpx.AsyncClient with MockTransport still triggers the hook."""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, request=request, json={"ok": True})

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    try:
        asyncio.run(client.get("https://api.blitz-api.ai/v2/enrichment/person"))
    finally:
        asyncio.run(client.aclose())

    with sqlite3.connect(call_tracker.DB_PATH) as conn:
        rows = conn.execute(
            "SELECT provider, endpoint, method, status FROM provider_call_log"
        ).fetchall()
    assert rows == [("blitz", "/v2/enrichment/person", "GET", 200)]


# ---------------------------------------------------------------------------
# health_check — self-monitoring used by the in-app health loop
# ---------------------------------------------------------------------------

def test_health_check_returns_expected_keys_on_empty_table(installed: None) -> None:
    h = call_tracker.health_check()
    assert set(h.keys()) == {
        "installed",
        "call_log_table_exists", "email_ledger_table_exists",
        "call_log_total", "call_log_last_hour", "call_log_last_day",
        "email_ledger_total", "email_ledger_last_hour", "email_ledger_last_day",
        "call_log_newest", "email_ledger_newest",
        "error",
    }
    assert h["installed"] is True
    assert h["call_log_table_exists"] is True
    assert h["email_ledger_table_exists"] is True
    assert h["call_log_total"] == 0
    assert h["email_ledger_total"] == 0
    assert h["error"] is None


def test_health_check_counts_after_inserts(installed: None) -> None:
    asyncio.run(call_tracker._on_response(_make_response("api.blitz-api.ai", "/v2/a", "POST", 200)))
    asyncio.run(call_tracker._on_response(_make_response("app.betterenrich.com", "/x", "POST", 403)))
    h = call_tracker.health_check()
    assert h["call_log_total"] == 2
    assert h["call_log_last_hour"] == 2
    assert h["call_log_last_day"] == 2
    assert h["call_log_newest"] is not None
    assert h["error"] is None


def test_health_check_detects_missing_table(temp_db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """If a table was somehow dropped, health_check must report table_exists=False."""
    monkeypatch.setattr(call_tracker, "DB_PATH", temp_db)
    call_tracker.init_schema()
    with sqlite3.connect(temp_db) as conn:
        conn.execute("DROP TABLE provider_call_log")
    h = call_tracker.health_check()
    assert h["call_log_table_exists"] is False
    assert h["call_log_total"] == 0
    assert h["error"] is None


def test_health_check_is_defensive_on_db_error(temp_db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """If the DB connection raises, health_check captures the error and never raises."""
    monkeypatch.setattr(call_tracker, "DB_PATH", temp_db)
    call_tracker.init_schema()

    def _boom(*_a, **_k):
        raise sqlite3.OperationalError("simulated disk failure")

    monkeypatch.setattr(call_tracker, "_connect", _boom)
    h = call_tracker.health_check()
    assert h["error"] is not None
    assert "simulated disk failure" in h["error"]


# ---------------------------------------------------------------------------
# Email extraction — provider response body parsing
# ---------------------------------------------------------------------------

def test_extract_emails_finds_simple_email() -> None:
    body = {"email": "alice@acme.com", "status": "found"}
    emails = call_tracker._extract_emails("blitz", body)
    assert emails == ["alice@acme.com"]


def test_extract_emails_finds_multiple_emails() -> None:
    body = {"emails": ["alice@acme.com", "bob@acme.com"], "primary": "alice@acme.com"}
    emails = call_tracker._extract_emails("blitz", body)
    assert set(emails) == {"alice@acme.com", "bob@acme.com"}


def test_extract_emails_finds_nested_email() -> None:
    body = {"result": {"person": {"contact": "deeply@nested.com"}}, "ok": True}
    emails = call_tracker._extract_emails("better_enrich", body)
    assert "deeply@nested.com" in emails


def test_extract_emails_dedupes() -> None:
    body = {"email": "dup@example.com", "also": "dup@example.com", "list": ["dup@example.com"]}
    emails = call_tracker._extract_emails("blitz", body)
    assert emails == ["dup@example.com"]


def test_extract_emails_lowercases() -> None:
    body = {"email": "Alice@ACME.com"}
    emails = call_tracker._extract_emails("blitz", body)
    assert emails == ["alice@acme.com"]


def test_extract_emails_filters_provider_own_domain() -> None:
    """Emails from the provider's own domain (in metadata/support) are false positives."""
    body = {"email": "real@customer.com", "support": "help@betterenrich.com"}
    emails = call_tracker._extract_emails("better_enrich", body)
    assert emails == ["real@customer.com"]


def test_extract_emails_filters_provider_subdomain() -> None:
    body = {"email": "real@customer.com", "meta": "noreply@api.betterenrich.com"}
    emails = call_tracker._extract_emails("better_enrich", body)
    assert emails == ["real@customer.com"]


def test_extract_emails_handles_none_body() -> None:
    assert call_tracker._extract_emails("blitz", None) == []


def test_extract_emails_handles_non_json_string() -> None:
    assert call_tracker._extract_emails("blitz", "no emails here") == []
    assert call_tracker._extract_emails("blitz", "contact me at foo@bar.com") == ["foo@bar.com"]


def test_extract_emails_skips_oversized_body() -> None:
    """Body over the cap is skipped to prevent memory pressure."""
    big = {"junk": "x" * (call_tracker._MAX_BODY_BYTES_FOR_EXTRACTION * 3)}
    assert call_tracker._extract_emails("blitz", big) == []


# ---------------------------------------------------------------------------
# Email ledger writes — end-to-end through the response hook
# ---------------------------------------------------------------------------

def _make_response_with_body(
    host: str, path: str, body: dict, method: str = "POST", status: int = 200
) -> httpx.Response:
    """Build a Response with a JSON body, simulating a real provider reply."""
    request = httpx.Request(method, f"https://{host}{path}")
    return httpx.Response(
        status_code=status, request=request, json=body,
    )


def test_hook_extracts_email_from_2xx_response(installed: None) -> None:
    body = {"email": "found@example.com", "status": "ok"}
    asyncio.run(call_tracker._on_response(
        _make_response_with_body("api.blitz-api.ai", "/v2/enrichment/email", body)
    ))
    with sqlite3.connect(call_tracker.DB_PATH) as conn:
        emails = conn.execute(
            "SELECT email FROM provider_email_ledger"
        ).fetchall()
    assert emails == [("found@example.com",)]


def test_hook_extracts_from_better_enrich_company_email(installed: None) -> None:
    body = {"email": "hello@acme-corp.com", "type": "generic"}
    asyncio.run(call_tracker._on_response(
        _make_response_with_body(
            "app.betterenrich.com", "/api/v1/find-company-email", body
        )
    ))
    with sqlite3.connect(call_tracker.DB_PATH) as conn:
        rows = conn.execute(
            "SELECT provider, endpoint, email, status_code FROM provider_email_ledger"
        ).fetchall()
    assert rows == [("better_enrich", "/api/v1/find-company-email", "hello@acme-corp.com", 200)]


def test_hook_does_not_extract_from_error_responses(installed: None) -> None:
    """4xx/5xx responses should not have their bodies scraped for emails."""
    body = {"error": "invalid_request", "contact": "support@betterenrich.com"}
    asyncio.run(call_tracker._on_response(
        _make_response_with_body(
            "app.betterenrich.com", "/api/v1/find-company-email", body, status=403
        )
    ))
    with sqlite3.connect(call_tracker.DB_PATH) as conn:
        ledger_count = conn.execute(
            "SELECT COUNT(*) FROM provider_email_ledger"
        ).fetchone()[0]
        calllog_count = conn.execute(
            "SELECT COUNT(*) FROM provider_call_log"
        ).fetchone()[0]
    assert ledger_count == 0  # no emails extracted from error
    assert calllog_count == 1  # but the call itself was still logged


def test_hook_skips_scraper_tech_email_extraction(installed: None) -> None:
    """Scraper returns business listings, not emails — extraction would be wasteful false positives."""
    body = {"results": [{"name": "Acme", "site": "acme.com"}]}
    asyncio.run(call_tracker._on_response(
        _make_response_with_body("api.scraper.tech", "/searchmaps.php", body)
    ))
    with sqlite3.connect(call_tracker.DB_PATH) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM provider_email_ledger"
        ).fetchone()[0]
    assert count == 0


def test_hook_handles_non_json_body_gracefully(installed: None) -> None:
    """If response body isn't JSON (HTML error page, empty), hook doesn't crash."""
    request = httpx.Request("POST", "https://api.blitz-api.ai/v2/x")
    response = httpx.Response(200, request=request, content=b"<html>not json</html>")
    asyncio.run(call_tracker._on_response(response))  # must not raise
    with sqlite3.connect(call_tracker.DB_PATH) as conn:
        ledger = conn.execute("SELECT COUNT(*) FROM provider_email_ledger").fetchone()[0]
        calllog = conn.execute("SELECT COUNT(*) FROM provider_call_log").fetchone()[0]
    assert ledger == 0
    assert calllog == 1  # metadata still logged


def test_hook_records_multiple_emails_from_one_response(installed: None) -> None:
    body = {"emails": ["alice@x.com", "bob@x.com", "carol@x.com"]}
    asyncio.run(call_tracker._on_response(
        _make_response_with_body("api.blitz-api.ai", "/v2/x", body)
    ))
    with sqlite3.connect(call_tracker.DB_PATH) as conn:
        emails = sorted(r[0] for r in conn.execute("SELECT email FROM provider_email_ledger"))
    assert emails == ["alice@x.com", "bob@x.com", "carol@x.com"]


def test_purge_old_clears_both_tables(installed: None) -> None:
    """purge_old should delete from both provider_call_log and provider_email_ledger."""
    # Insert an old row in each table
    old_ts = (datetime.now(timezone.utc) - timedelta(days=40)).strftime("%Y-%m-%dT%H:%M:%S.%f")
    with sqlite3.connect(call_tracker.DB_PATH) as conn:
        conn.execute(
            "INSERT INTO provider_call_log (ts, provider, endpoint, method, status) "
            "VALUES (?, 'blitz', '/old', 'POST', 200)", (old_ts,)
        )
        conn.execute(
            "INSERT INTO provider_email_ledger (ts, provider, endpoint, email, status_code, metadata) "
            "VALUES (?, 'blitz', '/old', 'old@x.com', 200, '{}')", (old_ts,)
        )
    result = call_tracker.purge_old(days=30)
    assert result["call_log"] == 1
    assert result["email_ledger"] == 1
    with sqlite3.connect(call_tracker.DB_PATH) as conn:
        cl = conn.execute("SELECT COUNT(*) FROM provider_call_log").fetchone()[0]
        el = conn.execute("SELECT COUNT(*) FROM provider_email_ledger").fetchone()[0]
    assert cl == 0
    assert el == 0
