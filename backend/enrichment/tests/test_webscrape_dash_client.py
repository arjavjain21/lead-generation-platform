"""Tests for enrichment/webscrape_dash_client.py — the thin scrape-emails API
client backing the website-email followup flow.

Pins:
* Request shapes: paths, bearer auth header, Idempotency-Key on submit,
  callback_url only when provided, batch-size cap enforced client-side.
* Backpressure: 429/503 retried honouring Retry-After, exhaustion raises
  WebscrapeDashBusy carrying the hint; 401/403 raise WebscrapeDashAuthError.
* Result paging: cursor (last-domain) loop terminates on null next_cursor.

NOTE: pytest-asyncio is NOT installed — async work goes through asyncio.run(),
the established convention in enrichment/tests/.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Any, Optional
from unittest import mock

_BACKEND = Path(__file__).resolve().parents[2]
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

import httpx  # noqa: E402
import pytest  # noqa: E402

from enrichment import webscrape_dash_client as wsc  # noqa: E402


def run(coro):
    return asyncio.run(coro)


class FakeResponse:
    def __init__(self, status_code: int, json_body: Any = None, text: str = "", headers: Optional[dict] = None):
        self.status_code = status_code
        self._json = json_body
        self.text = text or (str(json_body) if json_body is not None else "")
        self.headers = headers or {}

    def json(self):
        if self._json is None:
            raise ValueError("no json")
        return self._json


class RequestLog:
    def __init__(self):
        self.calls: list[dict[str, Any]] = []

    def record(self, method, url, **kwargs):
        self.calls.append({"method": method, "url": url, **kwargs})


def install_client(monkeypatch, responses: list[FakeResponse], log: RequestLog) -> None:
    """Patch httpx.AsyncClient so each recorded call pops the next FakeResponse."""

    class FakeAsyncClient:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def request(self, method, url, json=None, params=None, headers=None):
            log.record(method, url, json=json, params=params, headers=headers)
            if not responses:
                raise AssertionError("unexpected extra HTTP call")
            return responses.pop(0)

    monkeypatch.setattr(wsc.httpx, "AsyncClient", FakeAsyncClient)


@pytest.fixture
def client() -> wsc.WebscrapeDashClient:
    return wsc.WebscrapeDashClient(base_url="https://dash.test", api_key="sk_live_test")


# ---------------------------------------------------------------------------
# Request shapes
# ---------------------------------------------------------------------------


def test_estimate_happy_path(monkeypatch, client):
    log = RequestLog()
    install_client(
        monkeypatch,
        [FakeResponse(200, {"total": 3, "new": 1, "already_done": 2})],
        log,
    )
    out = run(client.estimate(["a.com", "b.com", "c.com"], client_tag="lgp-estimate"))
    assert out["total"] == 3
    call = log.calls[0]
    assert call["method"] == "POST"
    assert call["url"] == "https://dash.test/api/scrape-emails/estimate"
    assert call["json"] == {"websites": ["a.com", "b.com", "c.com"], "client": "lgp-estimate"}
    assert call["headers"]["Authorization"] == "Bearer sk_live_test"


def test_submit_shape_and_idempotency_header(monkeypatch, client):
    log = RequestLog()
    install_client(monkeypatch, [FakeResponse(202, {"job_id": "j1", "total": 2})], log)
    out = run(
        client.submit(
            ["a.com", "b.com"],
            client_tag="lgp:abc",
            idempotency_key="lgp-followup-job-0",
            callback_url="https://cb.test/hook",
        )
    )
    assert out["job_id"] == "j1"
    call = log.calls[0]
    assert call["url"] == "https://dash.test/api/scrape-emails"
    assert call["headers"]["Idempotency-Key"] == "lgp-followup-job-0"
    assert call["json"]["callback_url"] == "https://cb.test/hook"
    assert call["json"]["client"] == "lgp:abc"


def test_submit_omits_callback_url_when_none(monkeypatch, client):
    log = RequestLog()
    install_client(monkeypatch, [FakeResponse(202, {"job_id": "j1"})], log)
    run(client.submit(["a.com"], client_tag="t", idempotency_key="k"))
    assert "callback_url" not in log.calls[0]["json"]


def test_submit_rejects_oversized_batch(client):
    with pytest.raises(wsc.WebscrapeDashError, match="chunk"):
        run(client.submit(["a.com"] * (wsc.MAX_WEBSITES_PER_BATCH + 1), client_tag="t", idempotency_key="k"))


def test_get_job_params_and_cancel_path(monkeypatch, client):
    log = RequestLog()
    install_client(
        monkeypatch,
        [FakeResponse(200, {"job_id": "j1", "done": False}), FakeResponse(200, {"cancelled": 3})],
        log,
    )
    run(client.get_job("j1", cursor="last.domain", limit=500))
    run(client.cancel("j1"))
    assert log.calls[0]["url"] == "https://dash.test/api/scrape-emails/j1"
    assert log.calls[0]["params"] == {"limit": 500, "cursor": "last.domain"}
    assert log.calls[1]["method"] == "POST"
    assert log.calls[1]["url"] == "https://dash.test/api/scrape-emails/j1/cancel"


# ---------------------------------------------------------------------------
# Backpressure / errors
# ---------------------------------------------------------------------------


def test_429_retried_honouring_retry_after(monkeypatch, client):
    log = RequestLog()
    install_client(
        monkeypatch,
        [
            FakeResponse(429, {"detail": "slow"}, headers={"Retry-After": "7"}),
            FakeResponse(202, {"job_id": "j1"}),
        ],
        log,
    )
    sleeps: list[float] = []

    async def fake_sleep(s):
        sleeps.append(s)

    monkeypatch.setattr(wsc.asyncio, "sleep", fake_sleep)
    out = run(client.submit(["a.com"], client_tag="t", idempotency_key="k"))
    assert out["job_id"] == "j1"
    assert sleeps == [7.0]
    assert len(log.calls) == 2


def test_429_exhaustion_raises_busy_with_hint(monkeypatch, client):
    busy = FakeResponse(503, {"detail": "backpressure"}, headers={"Retry-After": "3"})
    install_client(monkeypatch, [busy] * (wsc.MAX_AUTO_RETRIES + 1), RequestLog())
    monkeypatch.setattr(wsc.asyncio, "sleep", mock.AsyncMock(return_value=None))
    with pytest.raises(wsc.WebscrapeDashBusy) as excinfo:
        run(client.submit(["a.com"], client_tag="t", idempotency_key="k"))
    assert excinfo.value.retry_after_s == 3.0


def test_auth_error_not_retried(monkeypatch, client):
    install_client(monkeypatch, [FakeResponse(401, {"detail": "not authenticated"})], RequestLog())
    with pytest.raises(wsc.WebscrapeDashAuthError):
        run(client.estimate(["a.com"]))


def test_http_status_error_raises_mapped(monkeypatch, client):
    install_client(monkeypatch, [FakeResponse(404, {"detail": "no such job"})], RequestLog())
    with pytest.raises(wsc.WebscrapeDashError, match="404"):
        run(client.get_job("missing"))


def test_client_requires_key(monkeypatch):
    monkeypatch.delenv("WEBSCRAPER_API_KEY", raising=False)
    with pytest.raises(wsc.WebscrapeDashAuthError):
        wsc.WebscrapeDashClient(api_key="")


# ---------------------------------------------------------------------------
# Result paging
# ---------------------------------------------------------------------------


def test_collect_results_pages_until_null_cursor(monkeypatch, client):
    log = RequestLog()
    install_client(
        monkeypatch,
        [
            FakeResponse(200, {"results": [{"website": "a.com"}], "next_cursor": "a.com"}),
            FakeResponse(200, {"results": [{"website": "b.com"}], "next_cursor": None}),
        ],
        log,
    )
    rows = run(client.collect_results("j1"))
    assert [r["website"] for r in rows] == ["a.com", "b.com"]
    assert log.calls[1]["params"]["cursor"] == "a.com"


def test_parse_retry_after_variants():
    assert wsc._parse_retry_after("5") == 5.0
    assert wsc._parse_retry_after(" 2.5 ") == 2.5
    assert wsc._parse_retry_after(None) is None
    assert wsc._parse_retry_after("Wed, 21 Oct 2015") is None  # HTTP-date tolerated


def test_is_configured_env_gate(monkeypatch):
    monkeypatch.setenv("WEBSCRAPER_API_KEY", "sk_live_x")
    assert wsc.is_configured() is True
    monkeypatch.setenv("WEBSCRAPER_API_KEY", "  ")
    assert wsc.is_configured() is False
