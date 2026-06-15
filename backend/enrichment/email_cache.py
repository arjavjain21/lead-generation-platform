"""
Persistent email cache on /mnt/disk/enrichment-email-cache/.

Each cache entry is keyed by (normalized_domain, normalized_full_name) and
stores the full enriched row payload.  The cache is:

  * Written incrementally — every successful row append is flushed to disk.
  * Read at the start of each pipeline run — if a (domain, name) pair
    already has a cached result, the row is skipped entirely (no API calls).
  * Survives DB wipes and process restarts — it lives outside the DB.

Design choices:
  - One SQLite DB per cache (named by run) with a single table; compact,
    indexable, and atomic.
  - Domain-only fallback keyed by domain alone (no name) so that company-
    email lookups also benefit.
  - No TTL — the data is correct indefinitely; cleaning is handled by the
    separate 30-day output cleanup.
"""
from __future__ import annotations

import os
import sqlite3
import time
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Location
# ---------------------------------------------------------------------------
_CACHE_ROOT = Path("/mnt/disk/enrichment-email-cache")
_CACHE_ROOT.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
_CREATE_SQL = """\
CREATE TABLE IF NOT EXISTS cache (
    domain             TEXT    NOT NULL,
    full_name          TEXT,
    email              TEXT    NOT NULL,
    source_path        TEXT,
    source_provider    TEXT,
    source_method      TEXT,
    no_email_reason    TEXT,
    final_email_status TEXT,
    mailtester_code    TEXT,
    mailtester_message TEXT,
    provider_attempts  TEXT,
    enriched_row_json  TEXT,
    created_at         REAL    NOT NULL,
    PRIMARY KEY (domain, full_name)
);
CREATE INDEX IF NOT EXISTS idx_cache_domain ON cache(domain);
"""


# ---------------------------------------------------------------------------
# Per-run connection (one per process)
# ---------------------------------------------------------------------------
_conn: Optional[sqlite3.Connection] = None
_cache_db_path: Optional[str] = None


def get_cache_path(job_id: str) -> str:
    return str(_CACHE_ROOT / f"{job_id}.db")


def _get_conn(cache_path: str) -> sqlite3.Connection:
    global _conn, _cache_db_path
    if _conn is None or _cache_db_path != cache_path:
        if _conn is not None:
            try:
                _conn.close()
            except Exception:
                pass
        _conn = sqlite3.connect(cache_path, timeout=30, check_same_thread=False)
        _conn.execute("PRAGMA journal_mode=WAL")
        _conn.execute("PRAGMA synchronous=NORMAL")
        _conn.executescript(_CREATE_SQL)
        _cache_db_path = cache_path
    return _conn


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def open_cache(job_id: str) -> sqlite3.Connection:
    """Open (or create) the email cache for a job. Returns the connection."""
    path = get_cache_path(job_id)
    return _get_conn(path)


def _cols() -> str:
    return ("domain, full_name, email, source_path, source_provider, "
            "source_method, no_email_reason, final_email_status, "
            "mailtester_code, mailtester_message, provider_attempts, "
            "enriched_row_json, created_at")


def _row_to_dict(conn: sqlite3.Connection, row: tuple) -> dict:
    import json
    cols = [d.strip() for d in _cols().split(",")]
    d = dict(zip(cols, row))
    d["enriched_row"] = json.loads(d.pop("enriched_row_json") or "{}")
    return d


def lookup(
    conn: sqlite3.Connection,
    domain: str,
    full_name: str = "",
) -> Optional[dict]:
    """Return cached enrichment result for (domain, full_name), or None."""
    row = conn.execute(
        f"SELECT {_cols()} FROM cache WHERE domain=? AND full_name=?",
        (domain.strip().lower(), full_name.strip().lower()),
    ).fetchone()
    if row is None:
        return None
    return _row_to_dict(conn, row)


def lookup_domain_only(
    conn: sqlite3.Connection,
    domain: str,
) -> Optional[dict]:
    """Return cached company-email result for domain (full_name is NULL/empty)."""
    row = conn.execute(
        f"SELECT {_cols()} FROM cache WHERE domain=? AND (full_name IS NULL OR full_name='')",
        (domain.strip().lower(),),
    ).fetchone()
    if row is None:
        return None
    return _row_to_dict(conn, row)


def store(
    conn: sqlite3.Connection,
    domain: str,
    enriched_row: dict,
    *,
    full_name: str = "",
) -> None:
    """Upsert one enrichment result into the cache."""
    import json
    domain_key = domain.strip().lower()
    name_key = full_name.strip().lower()
    conn.execute(
        """INSERT OR REPLACE INTO cache
           (domain, full_name, email, source_path, source_provider,
            source_method, no_email_reason, final_email_status,
            mailtester_code, mailtester_message, provider_attempts,
            enriched_row_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            domain_key,
            name_key or None,
            enriched_row.get("email", ""),
            enriched_row.get("source_path", ""),
            enriched_row.get("source_provider", ""),
            enriched_row.get("source_method", ""),
            enriched_row.get("no_email_reason", ""),
            enriched_row.get("final_email_status", ""),
            enriched_row.get("mailtester_code", ""),
            enriched_row.get("mailtester_message", ""),
            enriched_row.get("provider_attempts_json", ""),
            json.dumps(enriched_row, ensure_ascii=False),
            time.time(),
        ),
    )
    conn.commit()


def close_cache() -> None:
    global _conn, _cache_db_path
    if _conn is not None:
        try:
            _conn.commit()
            _conn.close()
        except Exception:
            pass
        _conn = None
        _cache_db_path = None
