"""
Blitz domain miss-marker store — persistent negative cache for definitive
Blitz "not found" outcomes.

Blitz is the 2nd cascade provider and the first paid one (15M records/month
fair-use cap, metered in blitz_fair_use). For domains where Blitz has
DEFINITIVELY answered "not found", the cascade re-asks on every job — burning
FUP records for a guaranteed zero. This store is a domain-keyed negative
cache so the wiring layer (pipeline / cascade call sites, added in a later
wave) can skip known-miss domains until the TTL expires.

Cost note: one indexed SQLite upsert/select in jobs.db per call — zero
provider cost, zero network. The value is measured in skipped Blitz calls.

CRITICAL SEMANTICS — definitive misses ONLY:
- kind='company'   : the domain -> LinkedIn company lookup returned found=false.
- kind='contacts'  : the company resolved, but zero decision-makers were found.
- Markers MUST NEVER be written on exceptions, HTTP 429/5xx/402, timeouts,
  circuit-breaker trips, or cancelled calls. A transient failure is NOT a
  miss — recording one would poison the domain for TTL days and silently
  suppress a provider that may well succeed on retry.
- The wiring layer is responsible for invoking record_miss() ONLY on
  definitive not-found responses. This store is deliberately dumb: it never
  inspects HTTP responses or status codes and trusts its caller.

TTL: BLITZ_MISS_TTL_DAYS (env), default 30. Blitz claims +40% coverage this
summer, so older misses must be retried — an expired marker reads as "not a
recent miss" (is_recent_miss -> None) even though the row is retained for
stats.

Schema (idempotent CREATE TABLE IF NOT EXISTS at first use, mirroring the
call_tracker / stats_store pattern; thread-local connections via
shared.db.get_db):

    blitz_domain_miss(domain TEXT PRIMARY KEY, kind TEXT NOT NULL,
                      miss_at TEXT NOT NULL, company_url TEXT)

``company_url`` (2026-09-16) caches the resolved company LinkedIn URL
alongside a 'contacts' miss. A later job that hits that marker re-injects
the URL into the flow (skipping BOTH the blitz d2l call AND the waterfall)
so execution continues down the waterfall-empty path — Contacts DB DMs, the
GetLeads decision-makers fallback, the BetterEnrich generic path — instead
of dying on an early no-LinkedIn return that fires BEFORE the GetLeads
fallback. NULL/empty for 'company' misses (no page exists to store).

Every entry point is best-effort: any sqlite failure is swallowed with a
debug log — a marker-store outage must never break enrichment itself.
"""

from __future__ import annotations

import logging
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Optional

from enrichment.identifier_utils import normalize_domain
from shared import db

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# The only legal `kind` values. Guarded at write time — see record_miss().
VALID_KINDS: tuple[str, ...] = ("company", "contacts")

# Default marker TTL in days. Overridable via BLITZ_MISS_TTL_DAYS; see
# _ttl_days() for the invalid-value policy.
DEFAULT_TTL_DAYS: int = 30

# Process-local "table already created" flag. The CREATE IF NOT EXISTS is
# safe to re-run (and is re-run whenever a fresh process starts); the flag
# just avoids paying DDL on every call within one worker process.
_schema_initialized: bool = False

_SCHEMA = """
CREATE TABLE IF NOT EXISTS blitz_domain_miss (
    domain       TEXT PRIMARY KEY,
    kind         TEXT NOT NULL,        -- 'company' | 'contacts'
    miss_at      TEXT NOT NULL,        -- ISO-8601 UTC, ms precision
    company_url  TEXT                  -- resolved company LinkedIn URL
                                       -- ('contacts' misses; NULL for 'company')
);
CREATE INDEX IF NOT EXISTS idx_blitz_domain_miss_miss_at
    ON blitz_domain_miss(miss_at);
"""


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    """UTC now as ISO-8601 with millisecond precision (call_tracker format)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]


def _iso_cutoff(days: float) -> str:
    """UTC now minus `days`, in the same format _now_iso() emits.

    Identical formatting on both sides keeps the SQL string comparison
    (`miss_at > ?`) chronologically correct.
    """
    return (datetime.now(timezone.utc) - timedelta(days=days)).strftime(
        "%Y-%m-%dT%H:%M:%S.%f"
    )[:-3]


def _ttl_days() -> int:
    """Resolve BLITZ_MISS_TTL_DAYS. Missing/unparseable/<1 -> default 30.

    A TTL below 1 day would make every marker instantly stale (markers
    silently stop working); that is never what an operator wants, so it is
    treated as invalid rather than honoured.
    """
    raw = os.getenv("BLITZ_MISS_TTL_DAYS", "")
    if not raw:
        return DEFAULT_TTL_DAYS
    try:
        value = int(raw.strip())
    except ValueError:
        logger.warning(
            "blitz_miss_store: invalid BLITZ_MISS_TTL_DAYS=%r, using default %d",
            raw, DEFAULT_TTL_DAYS,
        )
        return DEFAULT_TTL_DAYS
    if value < 1:
        logger.warning(
            "blitz_miss_store: BLITZ_MISS_TTL_DAYS=%d below 1, using default %d",
            value, DEFAULT_TTL_DAYS,
        )
        return DEFAULT_TTL_DAYS
    return value


def _ensure_table(conn: sqlite3.Connection) -> None:
    """Create blitz_domain_miss (+ index) if missing. Idempotent, in-DB safe.

    Also upgrades a pre-company_url table (created by an earlier build of
    this undeployed module) with an ALTER TABLE ADD COLUMN so INSERTs that
    carry a company_url never fail. The duplicate-column error on fresh
    schemas is expected and swallowed; any other failure propagates to the
    caller's best-effort try/except (fail-open reads, skipped writes).
    """
    global _schema_initialized
    if _schema_initialized:
        return
    conn.executescript(_SCHEMA)
    try:
        conn.execute("ALTER TABLE blitz_domain_miss ADD COLUMN company_url TEXT")
    except sqlite3.OperationalError as exc:
        if "duplicate column" not in str(exc).lower():
            raise
    conn.commit()
    _schema_initialized = True


def init_table() -> None:
    """Public idempotent bootstrap for the app-startup wiring (later wave).

    Best-effort: a schema failure is swallowed with a debug log, matching
    call_tracker.init_schema(). Normal operation does not depend on this
    being called — every entry point lazily creates the table too.
    """
    try:
        _ensure_table(db.get_db())
    except Exception:
        logger.debug("blitz_miss_store: init_table failed", exc_info=True)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def record_miss(domain: str, kind: str, company_url: Optional[str] = None) -> None:
    """Persist a definitive Blitz miss for a domain (upsert; best-effort).

    Re-recording an existing domain refreshes miss_at, kind AND company_url —
    the latest definitive answer wins and the TTL clock restarts (a
    re-record without ``company_url`` therefore CLEARS a previously stored
    URL; callers that have one must always pass it).

    Args:
        domain: domain string; normalized (lowercase/strip via the shared
            identifier_utils normalizer) before store, so lookups with any
            casing/whitespace variant hit the same row.
        kind: 'company' (domain->LinkedIn lookup returned found=false) or
            'contacts' (company resolved, zero decision-makers). Any other
            value is refused with a warning — the kind vocabulary is part
            of the store's contract with the observability endpoint.
        company_url: the resolved company LinkedIn URL to cache alongside a
            'contacts' miss, so later jobs can re-enter the waterfall-empty
            flow (GetLeads decision-makers fallback) without re-billing the
            blitz domain-to-linkedin call. Optional/None for 'company'
            misses (no page exists) and for 'contacts' misses recorded
            through a callback seam that has no URL in hand (the wiring
            layer backfills it when the flow re-confirms the miss).

    Never raises. THE CALLER must only invoke this on definitive not-found
    responses — see the module docstring's CRITICAL SEMANTICS.
    """
    if kind not in VALID_KINDS:
        logger.warning(
            "blitz_miss_store: refusing to record unknown kind=%r for domain=%r",
            kind, domain,
        )
        return
    normalized = normalize_domain(domain)
    if not normalized:
        logger.debug(
            "blitz_miss_store: domain %r empty after normalization, kind=%r skipped",
            domain, kind,
        )
        return
    normalized_url = str(company_url).strip() if company_url else ""
    try:
        conn = db.get_db()
        _ensure_table(conn)
        conn.execute(
            """
            INSERT INTO blitz_domain_miss (domain, kind, miss_at, company_url)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(domain) DO UPDATE SET
                kind = excluded.kind,
                miss_at = excluded.miss_at,
                company_url = excluded.company_url
            """,
            (normalized, kind, _now_iso(), normalized_url or None),
        )
        conn.commit()
    except Exception:
        logger.debug(
            "blitz_miss_store: record_miss failed for %s (kind=%s)",
            normalized, kind, exc_info=True,
        )


def is_recent_miss(domain: str) -> Optional[dict[str, str]]:
    """Return miss info if `domain` has an unexpired marker, else None.

    Shape (stable keys):
        {"kind": "company" | "contacts", "company_url": "<url or empty>"}

    A None return means "safe to ask Blitz" — either the domain was never
    marked, or its marker aged past BLITZ_MISS_TTL_DAYS (default 30) and the
    domain must be retried. ``company_url`` is "" for 'company' misses and
    for markers recorded before the column existed / without a URL.

    Truthiness-compatible with the earlier ``Optional[str]`` return: hit ->
    non-empty dict (truthy), no hit -> None, so ``if is_recent_miss(d):``
    call sites (e.g. the blitz_batch prepass callback contract) are
    unaffected.

    Best-effort: on any sqlite failure the answer is None (fail-open —
    enrichment proceeds, we merely lose the skip).

    Args:
        domain: normalized exactly like record_miss (lowercase/strip), so
            any casing/whitespace variant of a recorded domain matches.
    """
    normalized = normalize_domain(domain)
    if not normalized:
        return None
    try:
        conn = db.get_db()
        _ensure_table(conn)
        row = conn.execute(
            "SELECT kind, company_url FROM blitz_domain_miss "
            "WHERE domain = ? AND miss_at > ?",
            (normalized, _iso_cutoff(days=_ttl_days())),
        ).fetchone()
        if row is None:
            return None
        return {
            "kind": row["kind"],
            "company_url": str(row["company_url"] or ""),
        }
    except Exception:
        logger.debug(
            "blitz_miss_store: is_recent_miss failed for %s", normalized,
            exc_info=True,
        )
        return None


def miss_stats() -> dict:
    """Return marker counts for the observability endpoint (later wave).

    Shape (stable — by_kind is pre-seeded with both kinds so consumers never
    see a missing key):
        {
            'total': int,          # all rows, including expired markers
            'by_kind': {'company': int, 'contacts': int},
            'recent_24h': int,     # markers recorded in the last 24 hours
        }

    Best-effort: on any sqlite failure returns the zeroed shape.
    """
    out: dict = {
        "total": 0,
        "by_kind": {"company": 0, "contacts": 0},
        "recent_24h": 0,
    }
    try:
        conn = db.get_db()
        _ensure_table(conn)
        out["total"] = conn.execute(
            "SELECT COUNT(*) FROM blitz_domain_miss"
        ).fetchone()[0]
        for kind, count in conn.execute(
            "SELECT kind, COUNT(*) FROM blitz_domain_miss GROUP BY kind"
        ):
            out["by_kind"][kind] = count
        out["recent_24h"] = conn.execute(
            "SELECT COUNT(*) FROM blitz_domain_miss WHERE miss_at >= ?",
            (_iso_cutoff(days=1),),
        ).fetchone()[0]
    except Exception:
        logger.debug("blitz_miss_store: miss_stats failed", exc_info=True)
    return out
