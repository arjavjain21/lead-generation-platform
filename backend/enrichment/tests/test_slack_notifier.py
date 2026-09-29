"""Tests for enrichment/slack_notifier.py — direct Slack notifications
(2026-09-29, SMTP-credentials-out period). Pins: config gating (token +
channel + kill-switch), chat.postMessage shape, never-raises semantics, and
the compact job message format used by send_job_notification.

NOTE: pytest-asyncio is NOT installed — async work goes through asyncio.run().
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

import pytest  # noqa: E402

from enrichment import slack_notifier as sn  # noqa: E402


def run(coro):
    return asyncio.run(coro)


class _Resp:
    def __init__(self, payload: dict[str, Any]):
        self._payload = payload

    def json(self):
        return self._payload


def _install(monkeypatch, response: Optional[_Resp], raises: Optional[Exception] = None):
    calls: list[dict[str, Any]] = []

    class FakeClient:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def post(self, url, headers=None, json=None):
            calls.append({"url": url, "headers": headers, "json": json})
            if raises:
                raise raises
            return response

    monkeypatch.setattr(sn.httpx, "AsyncClient", FakeClient)
    return calls


@pytest.fixture
def configured(monkeypatch):
    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-test")
    monkeypatch.setenv("SLACK_CHANNEL", "C123")
    monkeypatch.delenv("SLACK_NOTIFY_ENABLED", raising=False)


def test_config_gating(monkeypatch, configured):
    assert sn.is_slack_configured() is True
    monkeypatch.setenv("SLACK_NOTIFY_ENABLED", "false")
    assert sn.is_slack_configured() is False
    monkeypatch.setenv("SLACK_NOTIFY_ENABLED", "true")
    monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)
    assert sn.is_slack_configured() is False


def test_send_posts_to_chat_postmessage(monkeypatch, configured):
    calls = _install(monkeypatch, _Resp({"ok": True}))
    assert run(sn.send_slack_message("hello")) is True
    assert calls[0]["url"] == "https://slack.com/api/chat.postMessage"
    assert calls[0]["headers"]["Authorization"] == "Bearer xoxb-test"
    assert calls[0]["json"] == {"channel": "C123", "text": "hello"}


def test_send_slack_error_returns_false(monkeypatch, configured):
    _install(monkeypatch, _Resp({"ok": False, "error": "channel_not_found"}))
    assert run(sn.send_slack_message("x")) is False


def test_send_never_raises_on_transport_error(monkeypatch, configured):
    _install(monkeypatch, None, raises=RuntimeError("boom"))
    assert run(sn.send_slack_message("x")) is False


def test_send_skipped_when_unconfigured(monkeypatch):
    monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)
    monkeypatch.delenv("SLACK_CHANNEL", raising=False)
    calls = _install(monkeypatch, _Resp({"ok": True}))
    assert run(sn.send_slack_message("x")) is False
    assert calls == []


def test_job_message_contains_key_fields():
    msg = sn.job_message(
        job_type="website_email_scrape", status="done",
        filename="website_emails_ab12.csv", total=9, processed=9, emails_found=3,
    )
    assert "DONE" in msg and "9/9" in msg and "3" in msg
    assert "website_emails_ab12.csv" in msg
