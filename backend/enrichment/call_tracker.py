"""
Provider HTTP call tracker + email ledger — observability for outbound enrichment API calls.

Two complementary tables, both written from the same httpx response hook:

1. provider_call_log — metadata for EVERY HTTP call (ts, provider, endpoint,
   method, status). Tells you "a call happened". Useful for rate/cost analysis.

2. provider_email_ledger — every email extracted from EVERY provider response
   body (ts, provider, endpoint, email, status_code, metadata). Append-only
   source of truth for "what did providers actually return to us". Survives
   every downstream pipeline loss point (silent rejection, outbox failure,
   stats write lock, normalizer field drop, worker crash).

Design constraints honoured:
- Pure SQLite (no PostgreSQL load).
- Best-effort: every hook is wrapped in try/except so a tracker failure never
  breaks the underlying HTTP call.
- Per-call connection: no in-memory queue, no thread state, no RAM accumulation.
- Global install via a single monkeypatch on httpx.AsyncClient.__init__: zero
  edits to existing provider clients, automatically covers every AsyncClient
  constructed anywhere in the codebase.
- Idempotent schema and hook installation (safe across worker restarts).
- 30-day rolling retention on both tables (bounded disk usage).
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import httpx

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DB_PATH: Path = Path(__file__).resolve().parent.parent / "data" / "jobs.db"
RETENTION_DAYS: int = 30

# Cap response body size for email extraction. Bodies larger than this are
# skipped (the provider's primary payload should be small; huge bodies are
# usually paginated lists we don't want to scan).
_MAX_BODY_BYTES_FOR_EXTRACTION: int = 200_000  # 200 KB

# Hostname → provider name. Single source of truth for "which provider".
# Hostnames are matched case-insensitively against response.request.url.host.
_HOSTS: dict[str, str] = {
    "api.blitz-api.ai": "blitz",
    "app.betterenrich.com": "better_enrich",
    "prospect-api.smartlead.ai": "smartprospect",
    "leadsdatabase.cc": "contacts_db",
    "api.wizleads.io": "wizleads",
    "api.prospeo.io": "prospeo",
    "app.getleads.io": "getleads",
    "validation.hyperke.org": "mailtester",
    "api.scraper.tech": "scraper_tech",
}

# Provider domains to filter OUT of email extraction — these appear in response
# metadata (support emails, headers, error contact info) and would create false
# positives if captured. Domains here block exact match AND any subdomain.
_PROVIDER_OWN_DOMAINS: dict[str, set[str]] = {
    "blitz": {"blitz-api.ai", "blitz.com"},
    "better_enrich": {"betterenrich.com", "betterenrich.ai"},
    "smartprospect": {"smartlead.in", "smartlead.ai"},
    "contacts_db": {"leadsdatabase.cc", "leadsdatabase.com"},
    "wizleads": {"wizleads.io", "wizleads.com"},
    "prospeo": {"prospeo.io"},
    "getleads": {"getleads.io"},
    "mailtester": {"hyperke.org", "validation.hyperke.org"},
    "scraper_tech": {"scraper.tech"},
}

# Email regex — simple and permissive. Captures standard emails.
# We accept the small risk of false positives (e.g. version strings like
# "1.2.3@something") in exchange for catching every real email.
_EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS provider_call_log (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    ts       TEXT    NOT NULL,
    provider TEXT    NOT NULL,
    endpoint TEXT    NOT NULL,
    method   TEXT    NOT NULL,
    status   INTEGER
);
CREATE INDEX IF NOT EXISTS idx_pcl_ts
    ON provider_call_log(ts);
CREATE INDEX IF NOT EXISTS idx_pcl_provider_endpoint_ts
    ON provider_call_log(provider, endpoint, ts);

CREATE TABLE IF NOT EXISTS provider_email_ledger (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT    NOT NULL,
    provider    TEXT    NOT NULL,
    endpoint    TEXT    NOT NULL,
    email       TEXT    NOT NULL,
    status_code INTEGER,
    metadata    TEXT
);
CREATE INDEX IF NOT EXISTS idx_pel_ts
    ON provider_email_ledger(ts);
CREATE INDEX IF NOT EXISTS idx_pel_provider_email
    ON provider_email_ledger(provider, email);
CREATE INDEX IF NOT EXISTS idx_pel_provider_endpoint_ts
    ON provider_email_ledger(provider, endpoint, ts);
"""

# ---------------------------------------------------------------------------
# Internal state — guarded by _installed flag for idempotent install.
# ---------------------------------------------------------------------------

# Capture the true original AsyncClient.__init__ ONCE at module import, before
# any monkeypatch has been applied. Referencing this from inside _patched_async_init
# avoids the recursion trap of reading httpx.AsyncClient.__init__ at call time.
_ORIGINAL_ASYNC_INIT: Any = httpx.AsyncClient.__init__

_installed: bool = False


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def _connect() -> sqlite3.Connection:
    """Open a short-lived connection with WAL + busy timeout. Caller closes it."""
    conn = sqlite3.connect(str(DB_PATH), timeout=5.0, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]


def _insert_call_log(
    provider: str, endpoint: str, method: str, status: Optional[int]
) -> None:
    """Best-effort INSERT into provider_call_log. Never raises into the caller."""
    try:
        with _connect() as conn:
            conn.execute(
                "INSERT INTO provider_call_log (ts, provider, endpoint, method, status) "
                "VALUES (?, ?, ?, ?, ?)",
                (_now_iso(), provider, endpoint, method, status),
            )
    except Exception:
        logger.warning("call_tracker insert failed", exc_info=True)


def _insert_email_ledger_row(
    provider: str,
    endpoint: str,
    email: str,
    status_code: int,
    metadata: str,
) -> None:
    """Best-effort INSERT of a single email into provider_email_ledger."""
    try:
        with _connect() as conn:
            conn.execute(
                "INSERT INTO provider_email_ledger "
                "(ts, provider, endpoint, email, status_code, metadata) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (_now_iso(), provider, endpoint, email, status_code, metadata),
            )
    except Exception:
        logger.warning("call_tracker email ledger insert failed", exc_info=True)


def _insert_emails_batch(
    provider: str,
    endpoint: str,
    status_code: int,
    emails: list[str],
    metadata: str,
) -> None:
    """Best-effort batch INSERT of multiple emails from one response.

    Uses a single transaction so we don't pay connection overhead per email.
    """
    if not emails:
        return
    ts = _now_iso()
    rows = [(ts, provider, endpoint, e, status_code, metadata) for e in emails]
    try:
        with _connect() as conn:
            conn.executemany(
                "INSERT INTO provider_email_ledger "
                "(ts, provider, endpoint, email, status_code, metadata) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                rows,
            )
    except Exception:
        logger.warning(
            "call_tracker email ledger batch insert failed (%d rows)", len(rows),
            exc_info=True,
        )


# ---------------------------------------------------------------------------
# Email extraction from provider response bodies
# ---------------------------------------------------------------------------

def _extract_emails(provider: str, body: Any) -> list[str]:
    """Extract unique, lowercased emails from a provider response body.

    Uses generic regex extraction over the JSON-serialized body. Filters out
    emails from the provider's own domain (false positives from metadata).
    Returns deduplicated list.
    """
    if body is None:
        return []
    # Serialize body to a string. Handles dict, list, primitives, already-str.
    try:
        if isinstance(body, (dict, list)):
            body_str = json.dumps(body, default=str)
        else:
            body_str = str(body)
    except Exception:
        return []

    # Skip if body is suspiciously large (already capped upstream, defense in depth)
    if len(body_str) > _MAX_BODY_BYTES_FOR_EXTRACTION * 2:
        return []

    raw_emails = _EMAIL_RE.findall(body_str)
    blocked = _PROVIDER_OWN_DOMAINS.get(provider, set())

    unique: set[str] = set()
    for email in raw_emails:
        email_lower = email.lower().strip()
        if not email_lower or "@" not in email_lower:
            continue
        domain = email_lower.rsplit("@", 1)[-1] if "@" in email_lower else ""
        # Filter provider's own domain (and any subdomain of it)
        if any(domain == bd or domain.endswith("." + bd) for bd in blocked):
            continue
        unique.add(email_lower)
    return list(unique)


# ---------------------------------------------------------------------------
# httpx event hook
# ---------------------------------------------------------------------------

async def _on_response(response: httpx.Response) -> None:
    """httpx response hook. Fires on every AsyncClient response.

    Records metadata to provider_call_log. On 2xx responses to known enrichment
    providers, also extracts emails from the response body into
    provider_email_ledger. Defensive: any failure is swallowed so the underlying
    call is unaffected.
    """
    try:
        host = response.request.url.host
        if not host:
            return
        provider = _HOSTS.get(host.lower())
        if not provider:
            return  # not a tracked provider (e.g. external CDNs, AWS endpoints)

        endpoint = response.request.url.path
        status = response.status_code

        # Always record call metadata
        _insert_call_log(
            provider=provider, endpoint=endpoint,
            method=response.request.method, status=status,
        )

        # On 2xx, attempt email extraction from body
        # (mailtester/scraper_tech return non-email payloads; skip cleanly)
        if not (200 <= status < 300):
            return
        if provider in ("scraper_tech",):
            return  # no emails expected

        try:
            content_length = int(response.headers.get("content-length", "0") or "0")
        except ValueError:
            content_length = 0
        # If header reported size > cap, skip. If unknown (0), still try but
        # _extract_emails has its own cap.
        if content_length and content_length > _MAX_BODY_BYTES_FOR_EXTRACTION:
            return

        try:
            body = response.json()
        except Exception:
            # Body isn't JSON — nothing to extract
            return

        emails = _extract_emails(provider, body)
        if emails:
            # Compact metadata: top-level keys only, capped length
            try:
                meta_str = json.dumps({
                    k: (str(v)[:80] if not isinstance(v, (dict, list)) else f"<{type(v).__name__}>")
                    for k, v in (body.items() if isinstance(body, dict) else {"_": body}).items()
                    if k.lower() not in ("email", "emails")  # already captured
                })[:500]
            except Exception:
                meta_str = "{}"
            _insert_emails_batch(provider, endpoint, status, emails, meta_str)
    except Exception:
        # Tracker must never break the underlying HTTP call.
        logger.debug("call_tracker hook swallowed error", exc_info=True)


# ---------------------------------------------------------------------------
# Global installation via httpx.AsyncClient.__init__ monkeypatch
# ---------------------------------------------------------------------------

def _patched_async_init(self, *args: Any, **kwargs: Any) -> None:
    """Wrap httpx.AsyncClient.__init__ to auto-install our response hook.

    Appends _on_response to event_hooks['response'] without disturbing any
    existing hooks the caller supplied. Idempotent per instance.
    """
    event_hooks = kwargs.get("event_hooks")
    if event_hooks is None:
        event_hooks = {}
        kwargs["event_hooks"] = event_hooks
    response_hooks = event_hooks.get("response")
    if response_hooks is None:
        response_hooks = []
        event_hooks["response"] = response_hooks
    if _on_response not in response_hooks:
        response_hooks.append(_on_response)
    return _ORIGINAL_ASYNC_INIT(self, *args, **kwargs)


def install_globally() -> None:
    """Monkeypatch httpx.AsyncClient so every instance gets the tracker hook.

    Idempotent: safe to call multiple times across worker restarts. Does nothing
    if already installed.
    """
    global _installed
    if _installed:
        return
    httpx.AsyncClient.__init__ = _patched_async_init  # type: ignore[method-assign]
    _installed = True
    logger.info("call_tracker installed globally on httpx.AsyncClient")


# ---------------------------------------------------------------------------
# Schema + retention
# ---------------------------------------------------------------------------

def init_schema() -> None:
    """Create tracker tables if they don't exist. Idempotent."""
    try:
        with _connect() as conn:
            conn.executescript(_SCHEMA)
    except Exception:
        logger.error("call_tracker schema init failed", exc_info=True)


def purge_old(days: int = RETENTION_DAYS) -> dict[str, int]:
    """Delete rows older than `days` from BOTH tables. Returns counts. Best-effort."""
    out: dict[str, int] = {"call_log": 0, "email_ledger": 0}
    try:
        with _connect() as conn:
            cur = conn.execute(
                "DELETE FROM provider_call_log WHERE ts < datetime('now', ?)",
                (f"-{days} days",),
            )
            out["call_log"] = cur.rowcount or 0
            cur = conn.execute(
                "DELETE FROM provider_email_ledger WHERE ts < datetime('now', ?)",
                (f"-{days} days",),
            )
            out["email_ledger"] = cur.rowcount or 0
        if any(out.values()):
            logger.info(
                "call_tracker purged %d call_log + %d email_ledger rows older than %d days",
                out["call_log"], out["email_ledger"], days,
            )
    except Exception:
        logger.warning("call_tracker purge failed", exc_info=True)
    return out


def init() -> None:
    """One-shot bootstrap: schema + global install. Call once at app startup."""
    init_schema()
    install_globally()


# ---------------------------------------------------------------------------
# Read helpers (for verification + future dashboards)
# ---------------------------------------------------------------------------

def counts_since(since_iso_ts: str) -> dict[tuple[str, str, Optional[int]], int]:
    """Return {(provider, endpoint, status): count} for call_log rows with ts >= since."""
    out: dict[tuple[str, str, Optional[int]], int] = {}
    try:
        with _connect() as conn:
            for provider, endpoint, _method, status, cnt in conn.execute(
                "SELECT provider, endpoint, method, status, COUNT(*) "
                "FROM provider_call_log WHERE ts >= ? "
                "GROUP BY provider, endpoint, method, status",
                (since_iso_ts,),
            ):
                out[(provider, endpoint, status)] = out.get(
                    (provider, endpoint, status), 0
                ) + cnt
    except Exception:
        logger.warning("call_tracker counts_since failed", exc_info=True)
    return out


# ---------------------------------------------------------------------------
# Self-monitoring — called by the in-app health loop in main.py
# ---------------------------------------------------------------------------

def health_check() -> dict:
    """Return a tracker health snapshot. Best-effort, never raises.

    Reports stats for both provider_call_log and provider_email_ledger so the
    health loop can confirm both layers are receiving data.
    """
    result: dict = {
        "installed": _installed,
        "call_log_table_exists": False,
        "email_ledger_table_exists": False,
        "call_log_total": 0,
        "call_log_last_hour": 0,
        "call_log_last_day": 0,
        "email_ledger_total": 0,
        "email_ledger_last_hour": 0,
        "email_ledger_last_day": 0,
        "call_log_newest": None,
        "email_ledger_newest": None,
        "error": None,
    }
    try:
        with _connect() as conn:
            for table, exists_key in [
                ("provider_call_log", "call_log_table_exists"),
                ("provider_email_ledger", "email_ledger_table_exists"),
            ]:
                exists = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                    (table,),
                ).fetchone()
                result[exists_key] = bool(exists)

            if result["call_log_table_exists"]:
                result["call_log_total"] = conn.execute(
                    "SELECT COUNT(*) FROM provider_call_log"
                ).fetchone()[0]
                result["call_log_last_hour"] = conn.execute(
                    "SELECT COUNT(*) FROM provider_call_log "
                    "WHERE ts >= datetime('now', '-1 hour')"
                ).fetchone()[0]
                result["call_log_last_day"] = conn.execute(
                    "SELECT COUNT(*) FROM provider_call_log "
                    "WHERE ts >= datetime('now', '-1 day')"
                ).fetchone()[0]
                result["call_log_newest"] = conn.execute(
                    "SELECT MAX(ts) FROM provider_call_log"
                ).fetchone()[0]

            if result["email_ledger_table_exists"]:
                result["email_ledger_total"] = conn.execute(
                    "SELECT COUNT(*) FROM provider_email_ledger"
                ).fetchone()[0]
                result["email_ledger_last_hour"] = conn.execute(
                    "SELECT COUNT(*) FROM provider_email_ledger "
                    "WHERE ts >= datetime('now', '-1 hour')"
                ).fetchone()[0]
                result["email_ledger_last_day"] = conn.execute(
                    "SELECT COUNT(*) FROM provider_email_ledger "
                    "WHERE ts >= datetime('now', '-1 day')"
                ).fetchone()[0]
                result["email_ledger_newest"] = conn.execute(
                    "SELECT MAX(ts) FROM provider_email_ledger"
                ).fetchone()[0]
    except Exception as e:
        result["error"] = str(e)
        logger.warning("call_tracker health_check error", exc_info=True)
    return result
