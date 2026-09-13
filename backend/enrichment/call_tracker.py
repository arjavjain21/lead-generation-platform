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
    # SEG MX classification (enrichment/seg.py): Cloudflare DoH. dns.google
    # is NOT mapped — this module only calls cloudflare-dns.com, and mapping
    # an unused host would be dead config.
    "cloudflare-dns.com": "seg",
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
    # SEG DoH responses are DNS answers, never provider emails. cloudflare.com
    # deliberately NOT added: no other provider claims it, but the DoH body
    # contains no emails anyway and a stray "hostmaster@cloudflare.com" SOA
    # contact would be DNS metadata, not a real lead. Kept minimal.
    "seg": {"cloudflare-dns.com"},
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

-- Blitz fair-usage meter: every 2xx Blitz response carries a top-level
-- `fair_usage` object (records_used per response, records_remaining against
-- the 15M records/month plan cap, next_reset_at). Single-row snapshot of the
-- latest gauge + one rollup row per UTC day for burn-rate trending.
CREATE TABLE IF NOT EXISTS blitz_fair_use (
    id                INTEGER PRIMARY KEY CHECK (id = 1),
    endpoint          TEXT    NOT NULL,
    records_used      REAL,
    records_remaining REAL,
    next_reset_at     TEXT,
    rate_limit        TEXT,
    request_id        TEXT,
    updated_at        TEXT    NOT NULL
);
CREATE TABLE IF NOT EXISTS blitz_fair_use_daily (
    day               TEXT PRIMARY KEY,
    records_remaining REAL,
    next_reset_at     TEXT,
    updated_at        TEXT NOT NULL
);

-- Email-quality scoreboard (2026-09-13): per-(day, provider, endpoint,
-- email_status) counters. VALID/CATCH_ALL/INVALID come from providers that
-- report per-email status (GetLeads email_status, SmartProspect
-- verification_status); everything else is UNKNOWN pending external
-- validation. Populated from the same response hook as the ledger.
CREATE TABLE IF NOT EXISTS provider_email_quality_daily (
    day          TEXT    NOT NULL,
    provider     TEXT    NOT NULL,
    endpoint     TEXT    NOT NULL,
    email_status TEXT    NOT NULL,
    responses    INTEGER NOT NULL DEFAULT 0,
    emails       INTEGER NOT NULL DEFAULT 0,
    updated_at   TEXT    NOT NULL,
    PRIMARY KEY (day, provider, endpoint, email_status)
);
"""

# ---------------------------------------------------------------------------
# Internal state — guarded by _installed flag for idempotent install.
# ---------------------------------------------------------------------------

# Capture the true original AsyncClient.__init__ ONCE at module import, before
# any monkeypatch has been applied. Referencing this from inside _patched_async_init
# avoids the recursion trap of reading httpx.AsyncClient.__init__ at call time.
_ORIGINAL_ASYNC_INIT: Any = httpx.AsyncClient.__init__

_installed: bool = False

# UTC day (YYYY-MM-DD) of the last blitz_fair_use_daily rollup write — gates
# the daily row to one write per day instead of one per response.
_last_fair_use_daily_day: str = ""


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


def _upsert_blitz_fair_usage(endpoint: str, fair_usage: dict) -> None:
    """Best-effort snapshot of Blitz's fair_usage FUP gauge. Never raises."""
    global _last_fair_use_daily_day
    try:
        now = _now_iso()
        rate_limit = fair_usage.get("rate_limit")
        with _connect() as conn:
            conn.execute(
                """
                INSERT INTO blitz_fair_use
                    (id, endpoint, records_used, records_remaining, next_reset_at,
                     rate_limit, request_id, updated_at)
                VALUES (1, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    endpoint = excluded.endpoint,
                    records_used = excluded.records_used,
                    records_remaining = excluded.records_remaining,
                    next_reset_at = excluded.next_reset_at,
                    rate_limit = excluded.rate_limit,
                    request_id = excluded.request_id,
                    updated_at = excluded.updated_at
                """,
                (
                    endpoint,
                    fair_usage.get("records_used"),
                    fair_usage.get("records_remaining"),
                    fair_usage.get("next_reset_at"),
                    json.dumps(rate_limit) if rate_limit is not None else None,
                    fair_usage.get("request_id"),
                    now,
                ),
            )
            today = now[:10]
            if _last_fair_use_daily_day != today:
                conn.execute(
                    """
                    INSERT INTO blitz_fair_use_daily (day, records_remaining, next_reset_at, updated_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(day) DO UPDATE SET
                        records_remaining = excluded.records_remaining,
                        next_reset_at = excluded.next_reset_at,
                        updated_at = excluded.updated_at
                    """,
                    (today, fair_usage.get("records_remaining"), fair_usage.get("next_reset_at"), now),
                )
                _last_fair_use_daily_day = today
    except sqlite3.Error:
        logger.debug("blitz fair_usage upsert failed", exc_info=True)


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


# Cap on ledger rows recorded per response — huge payloads (batch endpoints)
# must not flood the ledger; the daily counters stay exact regardless.
_MAX_LEDGER_EMAILS_PER_RESPONSE: int = 25

# Recognized quality buckets. Providers that report per-email status map into
# these; everything else lands in UNKNOWN.
_EMAIL_STATUSES = ("VALID", "CATCH_ALL", "INVALID", "UNKNOWN")


def _norm_email_status(raw: Any) -> str:
    s = str(raw or "").strip().upper()
    return s if s in _EMAIL_STATUSES[:3] else "UNKNOWN"


def _structured_email_status(provider: str, body: Any) -> dict[str, str]:
    """Extract {email: STATUS} from provider-native response shapes.

    GetLeads: enrich endpoints report ``results[].data.email_status``
    (VALID/CATCH_ALL/INVALID); decision-makers reports
    ``contacts[].Email Verification Status``.
    SmartProspect: ``data[].email_id`` + ``verification_status`` ("Valid"
    is the only trusted value — everything else is UNKNOWN).
    """
    out: dict[str, str] = {}
    if not isinstance(body, dict):
        return out

    if provider == "getleads":
        results = body.get("results")
        if isinstance(results, list):
            for item in results:
                if not isinstance(item, dict):
                    continue
                data = item.get("data")
                if isinstance(data, dict) and data.get("email_address"):
                    out[str(data["email_address"]).strip().lower()] = _norm_email_status(
                        data.get("email_status")
                    )
        contacts = body.get("contacts")
        if isinstance(contacts, list):
            for contact in contacts:
                if isinstance(contact, dict) and contact.get("Email"):
                    out[str(contact["Email"]).strip().lower()] = _norm_email_status(
                        contact.get("Email Verification Status")
                    )
    elif provider == "smartprospect":
        data = body.get("data")
        if isinstance(data, list):
            for item in data:
                if not isinstance(item, dict):
                    continue
                email = item.get("email_id") or item.get("email")
                if not email:
                    continue
                verified = str(item.get("verification_status", "")).strip().lower() == "valid"
                out[str(email).strip().lower()] = "VALID" if verified else "UNKNOWN"
    return out


def _extract_email_records(provider: str, body: Any) -> dict[str, str]:
    """Return {email: STATUS} for one response: structured first, regex
    fallback for the rest, own-domain noise filtered for both."""
    records = {
        email: st
        for email, st in _structured_email_status(provider, body).items()
        if not _is_own_domain(provider, email)
    }
    for email in _extract_emails(provider, body):
        records.setdefault(email, "UNKNOWN")
    return records


def _insert_email_records(
    provider: str,
    endpoint: str,
    status_code: int,
    records: dict[str, str],
) -> None:
    """Ledger insert carrying the per-email quality status in metadata."""
    if not records:
        return
    ts = _now_iso()
    items = list(records.items())[:_MAX_LEDGER_EMAILS_PER_RESPONSE]
    rows = [
        (ts, provider, endpoint, email, status_code, json.dumps({"email_status": st}))
        for email, st in items
    ]
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
            "call_tracker email ledger insert failed (%d rows)", len(rows),
            exc_info=True,
        )


def _bump_quality_counters(provider: str, endpoint: str, records: dict[str, str]) -> None:
    """Upsert the per-(day, provider, endpoint, status) scoreboard counters.

    One row per distinct status per response — far lighter than the raw
    ledger and exact (no sampling).
    """
    if not records:
        return
    day = _now_iso()[:10]
    status_counts: dict[str, int] = {}
    for st in records.values():
        status_counts[st] = status_counts.get(st, 0) + 1
    try:
        with _connect() as conn:
            for st, n in status_counts.items():
                conn.execute(
                    """
                    INSERT INTO provider_email_quality_daily
                        (day, provider, endpoint, email_status, responses, emails, updated_at)
                    VALUES (?, ?, ?, ?, 1, ?, ?)
                    ON CONFLICT(day, provider, endpoint, email_status) DO UPDATE SET
                        responses = responses + 1,
                        emails = emails + excluded.emails,
                        updated_at = excluded.updated_at
                    """,
                    (day, provider, endpoint, st, n, _now_iso()),
                )
    except Exception:
        logger.debug("quality counter upsert failed", exc_info=True)


# ---------------------------------------------------------------------------
# Email extraction from provider response bodies
# ---------------------------------------------------------------------------

def _is_own_domain(provider: str, email_lower: str) -> bool:
    """True when the email sits on the provider's own domain (or a subdomain
    of it) — metadata noise, never a real lead."""
    blocked = _PROVIDER_OWN_DOMAINS.get(provider, set())
    domain = email_lower.rsplit("@", 1)[-1] if "@" in email_lower else ""
    return any(domain == bd or domain.endswith("." + bd) for bd in blocked)


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

    unique: set[str] = set()
    for email in raw_emails:
        email_lower = email.lower().strip()
        if not email_lower or "@" not in email_lower:
            continue
        if _is_own_domain(provider, email_lower):
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

        # ROOT-CAUSE FIX (2026-09-13): on live network responses httpx fires
        # async response hooks BEFORE the body stream is read, so
        # response.json() raised httpx.ResponseNotRead and the swallowing
        # except below silently dropped EVERY real response — the email
        # ledger (and the fair_usage meter) recorded nothing but MockTransport
        # test artifacts since 2026-07-11. Reading the stream here is
        # idempotent (no-op when the caller already read it).
        if not response.is_stream_consumed:
            await response.aread()

        try:
            body = response.json()
        except Exception:
            # Body isn't JSON — nothing to extract
            return

        # Blitz FUP meter: capture the plan's fair_usage gauge (single-row
        # snapshot + daily rollup). Best-effort, never breaks the call.
        if provider == "blitz" and isinstance(body, dict):
            fair_usage = body.get("fair_usage")
            if isinstance(fair_usage, dict):
                _upsert_blitz_fair_usage(endpoint, fair_usage)

        records = _extract_email_records(provider, body)
        if records:
            _bump_quality_counters(provider, endpoint, records)
            _insert_email_records(provider, endpoint, status, records)
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
    """Delete rows older than `days` from ALL tracker tables. Returns counts. Best-effort."""
    out: dict[str, int] = {"call_log": 0, "email_ledger": 0, "quality_daily": 0}
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
            cur = conn.execute(
                "DELETE FROM provider_email_quality_daily WHERE day < date('now', ?)",
                (f"-{days} days",),
            )
            out["quality_daily"] = cur.rowcount or 0
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
