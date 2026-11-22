"""
Email Finder service client — terminal person-level email fallback.

Calls the internal Email Finder service (systemd `email-finder.service`,
127.0.0.1:8910) which brute-forces pattern permutations from
first+last+domain and validates each candidate through the shared MailTester
proxy (validation.hyperke.org). Only vendor-verified (`code == "ok"`)
addresses are returned as emails — catch-all guesses are never returned
unless a pattern hint explicitly asks for them, and even then they carry
hint_status="unverifiable_catch_all" + reduced confidence in the payload.

Rate limiting: NONE here by design. The finder service enforces its own
token bucket AND sits behind the shared proxy's centralized limiter, so
every platform worker funneling through one service process cannot exceed
the vendor window. (Same rationale as contacts_db being unthrottled
client-side.)

Cache reuse across jobs: the finder service persists all verdicts, domain
metadata and learned patterns in its own SQLite cache, so repeat persons
and second employees at a known domain resolve with zero vendor calls.

Kill switch: ENABLE_EMAIL_FINDER (default false) — see enrichment/providers.py.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Optional

import httpx

logger = logging.getLogger(__name__)

BASE_URL = os.getenv("EMAIL_FINDER_BASE_URL", "http://127.0.0.1:8910")
# A full primary-tier sweep is ~16 vendor calls; at the service's rate limit
# plus ~2.5 s/call SMTP-probing latency that is ≤ ~45 s. Leave headroom.
_REQUEST_TIMEOUT = 90.0


def verify_existing_enabled() -> bool:
    """ENABLE_EMAIL_FINDER_VERIFY_EXISTING (default false): send the
    person's existing Contacts-DB email candidates to the finder so they
    are validated BEFORE pattern guessing (~1 call vs a full sweep)."""
    return os.environ.get("ENABLE_EMAIL_FINDER_VERIFY_EXISTING", "false").lower() == "true"


async def find_email(
    client: httpx.AsyncClient,
    first_name: str,
    last_name: str,
    domain: str,
    pattern_hint: Optional[str] = None,
    hint_first_name: Optional[str] = None,
    hint_last_name: Optional[str] = None,
    existing_emails: Optional[list[str]] = None,
) -> dict[str, Any]:
    """Find a verified person email via the Email Finder service.

    Returns a dict shaped like every other provider result:
      {"email": <verified email or "">, "source": "email_finder_email",
       "verification": {...}, "mailtester_calls": N, "skipped_reason": ...}
    Raises httpx errors upward (the pipeline classifies them like any
    provider failure).
    """
    payload: dict[str, Any] = {
        "first_name": first_name,
        "last_name": last_name,
        "domain": domain,
    }
    if pattern_hint:
        payload["pattern_hint"] = pattern_hint
        if hint_first_name:
            payload["hint_first_name"] = hint_first_name
        if hint_last_name:
            payload["hint_last_name"] = hint_last_name
    if existing_emails and verify_existing_enabled():
        # Candidates only ship when the verify-existing flag is armed;
        # the finder validates them first and marks a hit as
        # found_source="existing_email" (still vendor-verified only).
        payload["existing_emails"] = existing_emails[:10]

    resp = await client.post(
        f"{BASE_URL}/api/v1/find",
        json=payload,
        timeout=_REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    data = resp.json()

    stats = data.get("statistics") or {}
    valid = data.get("valid_emails") or []
    skipped_reason = stats.get("skipped_reason")

    if valid:
        match = valid[0]
        return {
            "email": match.get("email", ""),
            "source": "email_finder_email",
            "pattern": match.get("pattern"),
            "confidence": match.get("confidence"),
            "hint_status": stats.get("hint_status"),
            # Only vendor-verified results reach valid_emails without a
            # hint; catch-all hint guesses are flagged as unverified.
            "verified": stats.get("hint_status") != "unverifiable_catch_all",
            "found_source": stats.get("found_source") or "",
            "mailtester_calls": stats.get("mailtester_calls", 0),
            "millionverifier_calls": stats.get("millionverifier_calls", 0),
            "skipped_reason": skipped_reason,
        }

    return {
        "email": "",
        "source": "not_found",
        "mailtester_calls": stats.get("mailtester_calls", 0),
        "millionverifier_calls": stats.get("millionverifier_calls", 0),
        "skipped_reason": skipped_reason or "not_found",
    }
