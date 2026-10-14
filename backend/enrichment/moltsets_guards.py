"""
MoltSets quota guards — DB-backed cross-worker cooldown, person miss store,
and fair-use usage ledger.

MoltSets (api.moltsets.com) runs on tight fair-use windows ($97/mo plan:
15k records / 5h, 75k records / week, 7.5k searches / 5h). Four gunicorn
workers each hold their own in-memory state, so per-process breakers would
let every worker independently rediscover an exhausted account — jobs.db is
the only cross-worker source of truth, exactly like job state.

Three stores live here:

1. ``moltsets_cooldown`` (single row) — armed when ANY worker receives a
   ``fair_use_limit_exceeded`` / ``fair_use_requests_exceeded`` error. Every
   worker checks it BEFORE calling MoltSets, so once a 5h/weekly window is
   spent the provider is skipped instantly (zero added latency) and the
   cascade falls through to better_enrich / wizleads. Owner requirement
   2026-10-14: "if we have hit the 5h limit we just move on."

2. ``moltsets_miss`` — person-keyed negative cache for DEFINITIVE
   ``status="not_found"`` answers (kind ``linkedin`` or ``name_domain``).
   Without it every job re-burns the weekly record budget on the same
   misses — the exact structural hole that gave BetterEnrich a 47%
   repeat-billing rate (2026-09-29 post-mortem). TTL
   ``MOLTSETS_MISS_TTL_DAYS`` (default 14).

3. ``moltsets_usage`` — append-only ledger of the ``metadata.fair_use``
   block MoltSets attaches to 2xx responses, plus account ``get_usage``
   snapshots. Read via ``usage_status()`` for the observability endpoint
   (GET /api/enrichment/moltsets/usage).

CRITICAL SEMANTICS (mirrors blitz_miss_store):
- Miss markers are written ONLY on definitive not-found responses — never
  on exceptions, timeouts, 429/5xx, fair-use errors, cooldown skips, or
  risk-policy rejections (a rejected grade is not proof the person has no
  good email at another provider... and MoltSets may return a different
  best email later).
- The cooldown is written ONLY on fair_use error codes (and cleared on
  expiry). A short 429 is handled by the client's retry loop, not here.
- Every entry point is best-effort: sqlite failures are swallowed with a
  debug log — a guard outage must never break enrichment itself.
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

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

VALID_MISS_KINDS: tuple[str, ...] = ("linkedin", "name_domain")

# Default negative-cache TTL in days. Weekly record cap (75k) makes repeat
# misses extra costly; 14d balances budget protection against coverage
# refresh. Env: MOLTSETS_MISS_TTL_DAYS.
DEFAULT_MISS_TTL_DAYS: float = 14

# Fallback cooldown seconds when a fair_use error carries no retry_after.
# Conservative — short enough not to strand quota if the error was the 5h
# search cap while records remained, long enough that a sustained exhausted
# account is not re-probed every request. Each subsequent fair_use error
# re-arms the cooldown, so a truly exhausted window stays skipped.
DEFAULT_COOLDOWN_SECONDS: float = 900.0  # 15 minutes

_SCHEMA = """
CREATE TABLE IF NOT EXISTS moltsets_cooldown (
    id          INTEGER PRIMARY KEY CHECK (id = 1),  -- singleton row
    until_ts    TEXT NOT NULL,     -- ISO-8601 UTC, ms precision
    code        TEXT NOT NULL,     -- fair_use_limit_exceeded | ...
    reason      TEXT NOT NULL,     -- human-readable message
    retry_after REAL,              -- seconds the API asked us to wait
    set_at      TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS moltsets_miss (
    key     TEXT PRIMARY KEY,      -- linkedin_url | first|last|domain
    kind    TEXT NOT NULL,         -- 'linkedin' | 'name_domain'
    miss_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_moltsets_miss_miss_at
    ON moltsets_miss(miss_at);
CREATE TABLE IF NOT EXISTS moltsets_usage (
    ts      TEXT NOT NULL,         -- ISO-8601 UTC, ms precision
    source  TEXT NOT NULL,         -- 'response' (metadata.fair_use) | 'get_usage'
    payload TEXT NOT NULL          -- raw JSON block, shape per MoltSets docs
);
CREATE INDEX IF NOT EXISTS idx_moltsets_usage_ts
    ON moltsets_usage(ts);
"""

_schema_initialized: bool = False


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    """UTC now as ISO-8601 with millisecond precision (call_tracker format)."""
    return _now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]


def _iso_in(seconds: float) -> str:
    return (_now() + timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]


def _iso_cutoff(days: float) -> str:
    return (_now() - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]


def _miss_ttl_days() -> float:
    """Resolve MOLTSETS_MISS_TTL_DAYS. Missing/unparseable/<1 -> default 14."""
    raw = os.getenv("MOLTSETS_MISS_TTL_DAYS", "")
    if not raw:
        return DEFAULT_MISS_TTL_DAYS
    try:
        value = float(raw.strip())
    except ValueError:
        logger.warning(
            "moltsets_guards: invalid MOLTSETS_MISS_TTL_DAYS=%r, using default %s",
            raw, DEFAULT_MISS_TTL_DAYS,
        )
        return DEFAULT_MISS_TTL_DAYS
    if value < 1:
        logger.warning(
            "moltsets_guards: MOLTSETS_MISS_TTL_DAYS=%s below 1, using default %s",
            raw, DEFAULT_MISS_TTL_DAYS,
        )
        return DEFAULT_MISS_TTL_DAYS
    return value


def _cooldown_fallback_seconds() -> float:
    """Resolve MOLTSETS_COOLDOWN_FALLBACK_SECONDS (min 60)."""
    value = env_float("MOLTSETS_COOLDOWN_FALLBACK_SECONDS", str(DEFAULT_COOLDOWN_SECONDS))
    return max(60.0, value)


def _ensure_table(conn: sqlite3.Connection) -> None:
    global _schema_initialized
    if _schema_initialized:
        return
    conn.executescript(_SCHEMA)
    conn.commit()
    _schema_initialized = True


def init_tables() -> None:
    """Public idempotent bootstrap for app-startup wiring. Best-effort."""
    try:
        _ensure_table(db.get_db())
    except Exception:
        logger.debug("moltsets_guards: init_tables failed", exc_info=True)


# ---------------------------------------------------------------------------
# Cooldown (fair-use window exhaustion) — cross-worker skip
# ---------------------------------------------------------------------------


def get_active_cooldown() -> Optional[dict[str, Any]]:
    """Return the active cooldown, or None if none is armed / it expired.

    Shape::
        {"until_ts": str, "code": str, "reason": str,
         "retry_after": float | None, "set_at": str,
         "remaining_seconds": int}

    Best-effort: on any sqlite failure returns None (fail-open — calls
    proceed, we merely lose the skip).
    """
    try:
        conn = db.get_db()
        _ensure_table(conn)
        row = conn.execute(
            "SELECT until_ts, code, reason, retry_after, set_at "
            "FROM moltsets_cooldown WHERE id = 1"
        ).fetchone()
        if row is None:
            return None
        # _iso_in() writes naive UTC strings — re-tag as UTC so the
        # subtraction against the aware _now() below is valid.
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
        logger.debug("moltsets_guards: get_active_cooldown failed", exc_info=True)
        return None


def set_cooldown(
    code: str,
    reason: str,
    retry_after: Optional[float] = None,
    min_seconds: float = 0.0,
) -> None:
    """Arm (or extend) the cross-worker cooldown. Never raises.

    Args:
        code: the MoltSets ``error.code`` (e.g. ``fair_use_limit_exceeded``).
        reason: the API's human-readable message.
        retry_after: seconds the API asked to wait; when None/absent the
            MOLTSETS_COOLDOWN_FALLBACK_SECONDS default is used.
        min_seconds: floor for the armed window — lets a caller widen the
            cooldown beyond the API's ask (e.g. cap the probe rate for an
            account repeatedly bouncing off the window).
    """
    seconds = max(
        float(retry_after) if retry_after is not None else 0.0,
        _cooldown_fallback_seconds() if retry_after is None else 0.0,
        min_seconds,
    )
    if seconds <= 0:
        seconds = _cooldown_fallback_seconds()
    try:
        conn = db.get_db()
        _ensure_table(conn)
        conn.execute(
            """
            INSERT INTO moltsets_cooldown (id, until_ts, code, reason, retry_after, set_at)
            VALUES (1, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                until_ts = MAX(until_ts, excluded.until_ts),
                code = excluded.code,
                reason = excluded.reason,
                retry_after = excluded.retry_after,
                set_at = excluded.set_at
            """,
            (_iso_in(seconds), code, reason, retry_after, _now_iso()),
        )
        conn.commit()
        logger.warning(
            "moltsets_guards: cooldown armed for %.0fs (code=%s)", seconds, code
        )
    except Exception:
        logger.debug("moltsets_guards: set_cooldown failed", exc_info=True)


def clear_cooldown() -> None:
    """Manually clear the cooldown (operator escape hatch). Never raises."""
    try:
        conn = db.get_db()
        _ensure_table(conn)
        conn.execute("DELETE FROM moltsets_cooldown WHERE id = 1")
        conn.commit()
    except Exception:
        logger.debug("moltsets_guards: clear_cooldown failed", exc_info=True)


# ---------------------------------------------------------------------------
# Person miss store — definitive not-found negative cache
# ---------------------------------------------------------------------------


def person_key(
    linkedin_url: str = "",
    first_name: str = "",
    last_name: str = "",
    domain: str = "",
) -> str:
    """Build the canonical miss-store key for a person lookup.

    LinkedIn-keyed when a URL exists (strongest identity), else
    ``first|last|domain`` lowercased/stripped. Empty string means "no key"
    (caller must not record/check a miss).
    """
    li = (linkedin_url or "").strip().lower().rstrip("/")
    if li:
        return f"li:{li}"
    first = (first_name or "").strip().lower()
    last = (last_name or "").strip().lower()
    dom = (domain or "").strip().lower()
    if first and last and dom:
        return f"nd:{first}|{last}|{dom}"
    return ""


def record_person_miss(key: str, kind: str) -> None:
    """Persist a definitive MoltSets not-found for a person. Never raises.

    THE CALLER must only invoke this on clean ``status="not_found"``
    responses — see module docstring CRITICAL SEMANTICS.
    """
    if kind not in VALID_MISS_KINDS:
        logger.warning(
            "moltsets_guards: refusing to record unknown kind=%r", kind
        )
        return
    if not key:
        return
    try:
        conn = db.get_db()
        _ensure_table(conn)
        conn.execute(
            """
            INSERT INTO moltsets_miss (key, kind, miss_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                kind = excluded.kind,
                miss_at = excluded.miss_at
            """,
            (key, kind, _now_iso()),
        )
        conn.commit()
    except Exception:
        logger.debug("moltsets_guards: record_person_miss failed", exc_info=True)


def is_recent_person_miss(key: str) -> bool:
    """True if `key` has an unexpired miss marker (safe to skip MoltSets).

    Best-effort: sqlite failure -> False (fail-open, call proceeds).
    """
    if not key:
        return False
    try:
        conn = db.get_db()
        _ensure_table(conn)
        row = conn.execute(
            "SELECT 1 FROM moltsets_miss WHERE key = ? AND miss_at > ?",
            (key, _iso_cutoff(days=_miss_ttl_days())),
        ).fetchone()
        return row is not None
    except Exception:
        logger.debug("moltsets_guards: is_recent_person_miss failed", exc_info=True)
        return False


# ---------------------------------------------------------------------------
# Usage ledger — metadata.fair_use + get_usage snapshots
# ---------------------------------------------------------------------------


def record_fair_use(source: str, payload: Any) -> None:
    """Append a fair-use/usage snapshot row. Best-effort, never raises.

    Args:
        source: ``response`` (parsed from a 2xx ``metadata.fair_use`` block)
            or ``get_usage`` (account endpoint snapshot).
        payload: the raw block, stored as JSON.
    """
    if not payload:
        return
    try:
        conn = db.get_db()
        _ensure_table(conn)
        conn.execute(
            "INSERT INTO moltsets_usage (ts, source, payload) VALUES (?, ?, ?)",
            (_now_iso(), source, json.dumps(payload, default=str)),
        )
        conn.commit()
    except Exception:
        logger.debug("moltsets_guards: record_fair_use failed", exc_info=True)


def miss_stats() -> dict[str, Any]:
    """Marker counts for the observability endpoint. Best-effort."""
    out: dict[str, Any] = {
        "total": 0,
        "by_kind": {k: 0 for k in VALID_MISS_KINDS},
        "recent_24h": 0,
        "ttl_days": _miss_ttl_days(),
    }
    try:
        conn = db.get_db()
        _ensure_table(conn)
        out["total"] = conn.execute(
            "SELECT COUNT(*) FROM moltsets_miss"
        ).fetchone()[0]
        for kind, count in conn.execute(
            "SELECT kind, COUNT(*) FROM moltsets_miss GROUP BY kind"
        ):
            out["by_kind"][kind] = count
        out["recent_24h"] = conn.execute(
            "SELECT COUNT(*) FROM moltsets_miss WHERE miss_at >= ?",
            (_iso_cutoff(days=1),),
        ).fetchone()[0]
    except Exception:
        logger.debug("moltsets_guards: miss_stats failed", exc_info=True)
    return out


def latest_usage_snapshot() -> Optional[dict[str, Any]]:
    """Most recent usage row (any source) with its parsed payload."""
    try:
        conn = db.get_db()
        _ensure_table(conn)
        row = conn.execute(
            "SELECT ts, source, payload FROM moltsets_usage "
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
        logger.debug("moltsets_guards: latest_usage_snapshot failed", exc_info=True)
        return None


def usage_status() -> dict[str, Any]:
    """Composite status for GET /api/enrichment/moltsets/usage."""
    return {
        "cooldown": get_active_cooldown(),
        "miss_store": miss_stats(),
        "latest_usage": latest_usage_snapshot(),
    }
