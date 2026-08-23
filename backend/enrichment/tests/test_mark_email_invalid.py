"""Tests for contacts_client.mark_email_invalid — the upsert-400 leak fix.

Background (2026-08-23 RCA): mark_email_invalid sent ``{"email": ""}`` to
/v1/persons/upsert. The server's PersonUpsertRequest validator rejects an
empty email with HTTP 400 *always* (proven live: pydantic "email is
required"). The client then treated the raise_for_status() exception like a
transient error and retried 4 more times — a permanently-invalid request,
5 calls, 0 effect. ~1,500 such events/day during active enrichment.

The correct mechanism (verified against the live server): a NORMAL upsert
with the real email + ``verification_status`` that maps to falsy
(e.g. "invalid") sets ``core.email.is_verified = false`` — that IS the
invalid marking.

Run alone:
    cd backend && source venv/bin/activate
    python -m pytest enrichment/tests/test_mark_email_invalid.py -q
"""
import asyncio
import os
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# _headers() raises without a token; tests exercise HTTP semantics only.
os.environ.setdefault("CONTACTS_API_TOKEN", "test-token-for-marks")


def _fake_response(status_code: int, text: str = "") -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.text = text
    resp.json = MagicMock(return_value={})
    resp.raise_for_status = MagicMock(
        side_effect=Exception(f"HTTP {status_code}") if status_code >= 400 else None
    )
    return resp


class TestMarkEmailInvalid:
    def test_exists_and_importable(self):
        from enrichment import contacts_client

        assert hasattr(contacts_client, "mark_email_invalid")

    def test_empty_email_short_circuits_no_calls(self):
        """Empty email must not hit the network at all."""
        from enrichment import contacts_client as cc

        client = MagicMock()
        client.post = AsyncMock()

        result = asyncio.run(cc.mark_email_invalid(client, email=""))

        assert result is None
        client.post.assert_not_awaited()

    def test_sends_real_email_with_invalid_verification_status(self):
        """Valid email must upsert itself with a falsy verification_status."""
        from enrichment import contacts_client as cc

        client = MagicMock()
        client.post = AsyncMock(return_value=_fake_response(200))

        asyncio.run(cc.mark_email_invalid(client, email="a@b.com", domain="b.com"))

        assert client.post.await_count == 1
        _, kwargs = client.post.await_args
        body = kwargs.get("json") or {}
        assert body.get("email") == "a@b.com"
        # The falsy mapping is what flips core.email.is_verified to false
        assert body.get("verification_status") == "invalid"
        assert body.get("domain") == "b.com"

    def test_no_retry_on_400(self):
        """A 400 is permanent (validation) — must NOT be retried."""
        from enrichment import contacts_client as cc

        client = MagicMock()
        client.post = AsyncMock(return_value=_fake_response(400, '{"detail": "email is required"}'))

        result = asyncio.run(cc.mark_email_invalid(client, email="a@b.com"))

        assert client.post.await_count == 1
        assert result is None

    def test_no_retry_on_404(self):
        """Person not found — permanent, one call only."""
        from enrichment import contacts_client as cc

        client = MagicMock()
        client.post = AsyncMock(return_value=_fake_response(404))

        asyncio.run(cc.mark_email_invalid(client, email="a@b.com"))

        assert client.post.await_count == 1

    def test_retries_5xx_up_to_max(self):
        """Transient 503 keeps the retry semantics (with backoff)."""
        from enrichment import contacts_client as cc

        client = MagicMock()
        client.post = AsyncMock(return_value=_fake_response(503))

        asyncio.run(cc.mark_email_invalid(client, email="a@b.com"))

        # 1 initial + _MAX_RETRIES retries
        assert client.post.await_count == cc._MAX_RETRIES + 1

    def test_success_returns_json(self):
        from enrichment import contacts_client as cc

        ok = _fake_response(200)
        ok.json.return_value = {"success": True}
        client = MagicMock()
        client.post = AsyncMock(return_value=ok)

        result = asyncio.run(cc.mark_email_invalid(client, email="a@b.com"))

        assert result == {"success": True}
