"""Direct Slack notifications via the workspace bot (chat.postMessage).

Why this exists (2026-09-29): SMTP credentials died platform-wide (Gmail 535 —
account password rotated after a suspected compromise; owner will restore
creds later). Until then — and as a durable second channel after — job and
website-email-followup notifications also post to Slack directly. This reuses
the workspace bot the contacts-api DM-stats alert already uses (same team,
same workspace), stored here as env vars — never in code.

Design pins:
* NEVER raises: every entry point swallows+logs — a Slack outage must not
  fail a job completion or a followup finalize.
* Off unless configured: no SLACK_BOT_TOKEN ⇒ is_slack_configured() False ⇒
  callers skip silently. SLACK_NOTIFY_ENABLED=false is a runtime kill-switch.
* One lightweight httpx POST per notification; no retries (Slack's API is
  reliable enough for notifications; the email channel remains the record).

Env vars:
* SLACK_BOT_TOKEN   — xoxb-… bot token (workspace bot)
* SLACK_CHANNEL     — default channel id (e.g. C0AHJCP4V99)
* SLACK_NOTIFY_ENABLED — default true when a token is present
"""

from __future__ import annotations

import logging
import os
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

_SLACK_API = "https://slack.com/api/chat.postMessage"
_TIMEOUT_S = 10.0


def is_slack_configured() -> bool:
    if os.getenv("SLACK_NOTIFY_ENABLED", "true").strip().lower() in ("0", "false", "no"):
        return False
    return bool(os.getenv("SLACK_BOT_TOKEN", "").strip() and os.getenv("SLACK_CHANNEL", "").strip())


async def send_slack_message(text: str, *, channel: Optional[str] = None) -> bool:
    """Post one message. Returns True on Slack 'ok'; never raises."""
    if not is_slack_configured():
        return False
    token = os.getenv("SLACK_BOT_TOKEN", "").strip()
    target = (channel or os.getenv("SLACK_CHANNEL", "")).strip()
    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT_S) as client:
            response = await client.post(
                _SLACK_API,
                headers={"Authorization": f"Bearer {token}"},
                json={"channel": target, "text": text},
            )
        body = response.json()
        if body.get("ok"):
            return True
        logger.warning("Slack post rejected: %s", body.get("error"))
        return False
    except Exception as exc:  # noqa: BLE001 — notifications must never raise
        logger.warning("Slack post failed: %s", exc)
        return False


def job_message(
    *, job_type: str, status: str, filename: str, total: int, processed: int, emails_found: int
) -> str:
    """Compact one-line job notification (mirrors the email's key fields)."""
    icon = {"done": "✅", "failed": "❌", "partial": "⚠️"}.get(status.lower(), "📋")
    return (
        f"{icon} ListBuilding *{status.upper()}* — {job_type}\n"
        f"• File: `{filename}`\n"
        f"• Processed: {processed}/{total}\n"
        f"• Emails found: *{emails_found}*"
    )
