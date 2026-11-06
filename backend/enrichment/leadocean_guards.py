"""
LeadOcean quota guards — DB-backed cross-worker cooldown, person miss store,
and usage ledger (mirrors moltsets_guards; see that module for the rationale).

LeadOcean plan (Agency · UGC, 2026-11-16): $247/mo, 1M enrichments/day,
25M search records/month, 200 RPS account-wide. The daily enrichment wall
does NOT error — the API throttles to ~1/60th of a percent of normal speed
until midnight UTC (LEADOCEAN_PROVIDER_TEST_REPORT.md §7.7). Past the wall
every request still answers 200, so the ONLY sane behavior is to detect
exhaustion up front (free /v1/usage + /v1/account pace data + throttle
notices) and skip the provider instantly until reset.

Enrichment lookups bill 1 record whether they find anything or not (unlike
QuickEnrich's credit-on-match), so the miss store (7d TTL default) is what
keeps repeat jobs from re-paying the same misses out of the daily window.

Stores (jobs.db — the only cross-worker source of truth, 4 gunicorn workers):
1. ``leadocean_cooldown`` — singleton; armed on daily-window exhaustion
   (until 00:00 UTC / resetsAt), monthly search exhaustion (phase 2), or
   sustained 429s. Every worker checks before calling.
2. ``leadocean_miss`` — person-keyed negative cache for DEFINITIVE misses:
   HTTP 404 (no profile), or 200 with no work AND no personal email.
3. ``leadocean_usage`` — append-only ledger of /v1/usage snapshots (the
   vendor's own dashboard under-counts — report §7.8 — so ours is the truth;
   read via GET /api/enrichment/leadocean/usage).

Best-effort everywhere: a guard outage must never break enrichment.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from shared import db
from shared.env_utils import env_float

logger = logging.getLogger(__name__)

VALID_MISS_KINDS: tuple[str, ...] = ("email",)

DEFAULT_MISS_TTL_DAYS: float = 7
DEFAULT_COOLDOWN_SECONDS: float = 900.0

_SCHEMA = """
CREATE TABLE IF NOT EXISTS leadocean_cooldown (
    id          INTEGER PRIMARY KEY CHECK (id = 1),
    until_ts    TEXT NOT NULL,
    code        TEXT NOT NULL,
    reason      TEXT NOT NULL,
    retry_after REAL,
    set_at      TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS leadocean_miss (
    key     TEXT PRIMARY KEY,
    kind    TEXT NOT NULL,
    miss_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_leadocean_miss_miss_at
    ON leadocean_miss(miss_at);
CREATE TABLE IF NOT EXISTS leadocean_usage (
    ts      TEXT NOT NULL,
    source  TEXT NOT NULL,
    payload TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_leadocean_usage_ts
    ON leadocean_usage(ts);
"""

_schema_initialized: bool = False


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    return _now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]


def _iso_in(seconds: float) -> str:
    return (_now() + timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]


def _iso_cutoff(days: float) -> str:
    return (_now() - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]


def _miss_ttl_days() -> float:
    raw = os.getenv("LEADOCEAN_MISS_TTL_DAYS", "")
    if not raw:
        return DEFAULT_MISS_TTL_DAYS
    try:
        value = float(raw.strip())
    except ValueError:
        return DEFAULT_MISS_TTL_DAYS
    return value if value >= 1 else DEFAULT_MISS_TTL_DAYS


def _cooldown_fallback_seconds() -> float:
    return max(60.0, env_float("LEADOCEAN_COOLDOWN_FALLBACK_SECONDS", str(DEFAULT_COOLDOWN_SECONDS)))


def _writes_armed() -> bool:
    """pytest sets LEADOCEAN_GUARDS_ARMED=false (same pattern as
    ENABLE_BLITZ_MISS_SKIP) so tests driving runners with stubbed providers
    never create tables / write markers into the PRODUCTION jobs.db — the
    DDL+writes contend for the write lock with the live service (30s
    busy_timeout each), which hung whole test files."""
    return os.environ.get("LEADOCEAN_GUARDS_ARMED", "true").lower() == "true"


def _ensure_table(conn: sqlite3.Connection) -> None:
    global _schema_initialized
    if _schema_initialized:
        return
    conn.executescript(_SCHEMA)
    conn.commit()
    _schema_initialized = True


def init_tables() -> None:
    try:
        _ensure_table(db.get_db())
    except Exception:
        logger.debug("leadocean_guards: init_tables failed", exc_info=True)


def _seconds_until_utc_midnight() -> float:
    """Seconds from now to the next 00:00 UTC (the daily enrich window reset)."""
    now = _now()
    tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return max(60.0, (tomorrow - now).total_seconds())


# ---------------------------------------------------------------------------
# Cooldown
# ---------------------------------------------------------------------------


def get_active_cooldown() -> Optional[dict[str, Any]]:
    try:
        conn = db.get_db()
        row = conn.execute(
            "SELECT until_ts, code, reason, retry_after, set_at "
            "FROM leadocean_cooldown WHERE id = 1"
        ).fetchone()
        if row is None:
            return None
        until = datetime.fromisoformat(row["until_ts"]).replace(tzinfo=timezone.utc)
        remaining = (until - _now()).total_seconds()
        if remaining <= 0:
            return None
        return {
            "until_ts": row["until_ts"],
            "code": row["code"],
            "reason": row["reason"],
            "retry_after": row["retry_after"],
            "set_at": row["set_at"],
            "remaining_seconds": int(remaining),
        }
    except Exception:
        logger.debug("leadocean_guards: get_active_cooldown failed", exc_info=True)
        return None


def set_cooldown(
    code: str,
    reason: str,
    retry_after: Optional[float] = None,
    until_ts: Optional[str] = None,
) -> None:
    """Arm (or extend) the cross-worker cooldown. Never raises.

    ``until_ts`` (ISO-8601) wins when supplied — used for the daily-window
    reset (resetsAt from /v1/usage). Otherwise seconds = retry_after or the
    env fallback.
    """
    if not _writes_armed():
        return
    if until_ts:
        try:
            target = datetime.fromisoformat(until_ts.replace("Z", "+00:00"))
            if target.tzinfo is None:
                target = target.replace(tzinfo=timezone.utc)
            seconds = (target - _now()).total_seconds()
        except (ValueError, TypeError):
            seconds = 0.0
        seconds = max(seconds, 60.0)
        final_until = until_ts
    else:
        seconds = max(
            float(retry_after) if retry_after is not None else 0.0,
            _cooldown_fallback_seconds() if retry_after is None else 0.0,
        )
        final_until = _iso_in(seconds)
    try:
        conn = db.get_db()
        _ensure_table(conn)
        conn.execute(
            """
            INSERT INTO leadocean_cooldown (id, until_ts, code, reason, retry_after, set_at)
            VALUES (1, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                until_ts = MAX(until_ts, excluded.until_ts),
                code = excluded.code,
                reason = excluded.reason,
                retry_after = excluded.retry_after,
                set_at = excluded.set_at
            """,
            (final_until, code, reason, retry_after, _now_iso()),
        )
        conn.commit()
        logger.warning(
            "leadocean_guards: cooldown armed until %s (code=%s)", final_until, code
        )
    except Exception:
        logger.debug("leadocean_guards: set_cooldown failed", exc_info=True)


def set_daily_exhausted_cooldown(resets_at: Optional[str] = None) -> None:
    """Arm the cooldown for the daily 1M-enrichment window being spent."""
    if resets_at:
        set_cooldown(
            "daily_enrich_exhausted",
            "1M/day enrichment window spent — throttled until reset",
            until_ts=resets_at,
        )
    else:
        set_cooldown(
            "daily_enrich_exhausted",
            "1M/day enrichment window spent — throttled until 00:00 UTC",
            retry_after=_seconds_until_utc_midnight(),
        )


def clear_cooldown() -> None:
    try:
        conn = db.get_db()
        _ensure_table(conn)
        conn.execute("DELETE FROM leadocean_cooldown WHERE id = 1")
        conn.commit()
    except Exception:
        logger.debug("leadocean_guards: clear_cooldown failed", exc_info=True)


# ---------------------------------------------------------------------------
# Person miss store
# ---------------------------------------------------------------------------


def person_key(linkedin_url: str) -> str:
    li = (linkedin_url or "").strip().lower().rstrip("/")
    return f"li:{li}" if li else ""


def record_person_miss(key: str, kind: str = "email") -> None:
    if kind not in VALID_MISS_KINDS or not key:
        return
    if not _writes_armed():
        return
    try:
        conn = db.get_db()
        _ensure_table(conn)
        conn.execute(
            """
            INSERT INTO leadocean_miss (key, kind, miss_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET miss_at = excluded.miss_at
            """,
            (key, kind, _now_iso()),
        )
        conn.commit()
    except Exception:
        logger.debug("leadocean_guards: record_person_miss failed", exc_info=True)


def is_recent_person_miss(key: str) -> bool:
    if not key:
        return False
    try:
        conn = db.get_db()
        row = conn.execute(
            "SELECT 1 FROM leadocean_miss WHERE key = ? AND miss_at > ?",
            (key, _iso_cutoff(days=_miss_ttl_days())),
        ).fetchone()
        return row is not None
    except Exception:
        logger.debug("leadocean_guards: is_recent_person_miss failed", exc_info=True)
        return False


# ---------------------------------------------------------------------------
# Usage ledger
# ---------------------------------------------------------------------------


def record_usage(source: str, payload: Any) -> None:
    if not payload:
        return
    if not _writes_armed():
        return
    try:
        conn = db.get_db()
        _ensure_table(conn)
        conn.execute(
            "INSERT INTO leadocean_usage (ts, source, payload) VALUES (?, ?, ?)",
            (_now_iso(), source, json.dumps(payload, default=str)),
        )
        conn.commit()
    except Exception:
        logger.debug("leadocean_guards: record_usage failed", exc_info=True)


def latest_usage_snapshot() -> Optional[dict[str, Any]]:
    try:
        conn = db.get_db()
        _ensure_table(conn)
        row = conn.execute(
            "SELECT ts, source, payload FROM leadocean_usage "
            "ORDER BY ts DESC LIMIT 1"
        ).fetchone()
        if row is None:
            return None
        try:
            payload = json.loads(row["payload"])
        except ValueError:
            payload = {"raw": row["payload"]}
        return {"ts": row["ts"], "source": row["source"], "payload": payload}
    except Exception:
        logger.debug("leadocean_guards: latest_usage_snapshot failed", exc_info=True)
        return None


def miss_stats() -> dict[str, Any]:
    out: dict[str, Any] = {"total": 0, "recent_24h": 0, "ttl_days": _miss_ttl_days()}
    try:
        conn = db.get_db()
        _ensure_table(conn)
        out["total"] = conn.execute("SELECT COUNT(*) FROM leadocean_miss").fetchone()[0]
        out["recent_24h"] = conn.execute(
            "SELECT COUNT(*) FROM leadocean_miss WHERE miss_at >= ?",
            (_iso_cutoff(days=1),),
        ).fetchone()[0]
    except Exception:
        logger.debug("leadocean_guards: miss_stats failed", exc_info=True)
    return out


def usage_status() -> dict[str, Any]:
    return {
        "cooldown": get_active_cooldown(),
        "miss_store": miss_stats(),
        "latest_usage": latest_usage_snapshot(),
    }
