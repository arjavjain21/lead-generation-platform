"""
Shared database connection and initialization for the unified jobs database.

This module provides:
- Thread-local database connections
- Unified schema with job_type discriminator
- Parent-child job relationships for chaining
- Daily API request tracking for non-admin users
"""

from __future__ import annotations

import logging
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)
DB_PATH = DATA_DIR / "jobs.db"

# Cache configuration
CACHE_DIR = Path("/mnt/disk/lead-generation-platform/cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_EXPIRY_DAYS = 90  # 90-day cache freshness as requested

_local = threading.local()

# Daily API request limit for non-admin users
NON_ADMIN_DAILY_REQUEST_LIMIT = 50000


def get_db() -> sqlite3.Connection:
    """Return a per-thread SQLite connection with row factory enabled."""
    if not hasattr(_local, "conn") or _local.conn is None:
        conn = sqlite3.connect(str(DB_PATH), check_same_thread=False, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA journal_size_limit=200000000")
        conn.execute("PRAGMA wal_autocheckpoint=400")
        conn.execute("PRAGMA synchronous=NORMAL")
        _local.conn = conn
    return _local.conn


def init_db() -> None:
    """
    Initialize the unified database schema.

    Creates tables for both scraper and enrichment jobs with a job_type discriminator.
    Also creates the shared job_events table and indexes.
    """
    c = get_db()

    c.executescript(
        """
        CREATE TABLE IF NOT EXISTS jobs (
            job_id        TEXT PRIMARY KEY,
            user_id       TEXT,
            job_type      TEXT NOT NULL,  -- 'scraper' | 'enrichment' | 'phone_enrichment'
            status        TEXT NOT NULL DEFAULT 'queued',  -- queued | running | done | failed
            parent_job_id TEXT,          -- FK to jobs.job_id (for chaining)

            -- Scraper-specific fields (NULL for enrichment/phone_enrichment jobs)
            query         TEXT,
            regions       TEXT,          -- JSON
            total_tasks   INTEGER,
            done_tasks    INTEGER,
            result_count  INTEGER,

            -- Enrichment-specific fields (NULL for scraper/phone_enrichment jobs)
            total         INTEGER,       -- Total rows to enrich
            processed     INTEGER,       -- Rows processed
            emails_found  INTEGER,
            filename      TEXT,
            domain_col    TEXT,
            original_filename TEXT,     -- Original uploaded filename (for display)
            selected_providers TEXT,   -- JSON array of providers user selected (e.g., ["contacts_db","blitz","wizleads","better_enrich"])
            used_providers TEXT,       -- JSON array of providers that actually executed (e.g., ["contacts_db","blitz"])

            -- Phone Enrichment-specific fields (NULL for scraper/enrichment jobs)
            linkedin_col  TEXT,         -- Column name containing LinkedIn URLs
            phones_found  INTEGER,     -- Number of phones found

            -- Common fields
            error         TEXT,
            output_path   TEXT,
            created_at    TEXT NOT NULL,
            updated_at    TEXT NOT NULL,
            last_heartbeat TEXT,        -- Set by background heartbeat task every 30s; used for stale detection

            FOREIGN KEY (user_id) REFERENCES users(user_id),
            FOREIGN KEY (parent_job_id) REFERENCES jobs(job_id)
        );

        CREATE TABLE IF NOT EXISTS job_events (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id  TEXT NOT NULL,
            seq     INTEGER NOT NULL,
            payload TEXT NOT NULL,  -- JSON event
            FOREIGN KEY (job_id) REFERENCES jobs(job_id)
        );

        CREATE TABLE IF NOT EXISTS daily_api_requests (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id     TEXT NOT NULL,
            date        TEXT NOT NULL,  -- UTC date in YYYY-MM-DD format
            request_count INTEGER NOT NULL DEFAULT 0,
            UNIQUE(user_id, date),
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        );

        CREATE INDEX IF NOT EXISTS idx_job_events_job_seq
            ON job_events (job_id, seq);

        CREATE INDEX IF NOT EXISTS idx_jobs_user_type
            ON jobs (user_id, job_type, created_at);

        CREATE INDEX IF NOT EXISTS idx_jobs_parent
            ON jobs (parent_job_id);

        CREATE INDEX IF NOT EXISTS idx_daily_api_requests_user_date
            ON daily_api_requests (user_id, date);

        -- API Keys table for programmatic access
        CREATE TABLE IF NOT EXISTS api_keys (
            key_id        TEXT PRIMARY KEY,
            user_id       TEXT NOT NULL,
            key_hash      TEXT NOT NULL,
            key_plain     TEXT NOT NULL,  -- Plain text key for viewing (in production, encrypt this!)
            name          TEXT NOT NULL,
            created_at    TEXT NOT NULL,
            last_used_at  TEXT,
            is_active     INTEGER NOT NULL DEFAULT 1,
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        );

        CREATE INDEX IF NOT EXISTS idx_api_keys_user
            ON api_keys (user_id);

        CREATE INDEX IF NOT EXISTS idx_api_keys_hash
            ON api_keys (key_hash);

        -- Phone Enrichments cache table (stores phone lookup results for caching)
        CREATE TABLE IF NOT EXISTS phone_enrichments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            linkedin_url TEXT UNIQUE NOT NULL,
            phone_number TEXT,
            phone_found INTEGER NOT NULL,  -- 1 = true, 0 = false
            enriched_at TEXT NOT NULL
        );

        -- Persistent job state (survives worker restarts)
        -- Stores _cancelled_jobs and _active_jobs sets so worker recycling doesn't lose them
        CREATE TABLE IF NOT EXISTS job_state (
            job_id    TEXT PRIMARY KEY,
            state     TEXT NOT NULL,  -- 'cancelled' | 'active'
            set_at    TEXT NOT NULL,
            FOREIGN KEY (job_id) REFERENCES jobs(job_id)
        );

        CREATE INDEX IF NOT EXISTS idx_job_state_type
            ON job_state (state);

        -- Per-row enrichment checkpoints (incremental resume support)
        -- Tracks processed row indices per enrichment job so a resumed run
        -- can skip already-completed rows. Idempotent — the live DB already
        -- has this table (created by migrations/add_checkpoint_support.py),
        -- so this is a no-op there; fresh DBs need it.
        CREATE TABLE IF NOT EXISTS job_checkpoints (
            job_id      TEXT NOT NULL,
            row_index   INTEGER NOT NULL,
            processed_at TEXT NOT NULL,
            PRIMARY KEY (job_id, row_index)
        );

        CREATE INDEX IF NOT EXISTS idx_checkpoints_job
            ON job_checkpoints (job_id);

        -- Domain-keyed enrichment checkpoints (2026-08-24). Positional
        -- job_checkpoints are written in the PARENT job's deduped-subset index
        -- space, but /restart filters against the FULL re-deduped file — an
        -- index-space mismatch that re-enriched already-done domains (the
        -- hyperke-saas chain). Resume now also unions the DONE DOMAINS across
        -- the whole restart chain. Idempotent CREATE IF NOT EXISTS: existing
        -- installs get the table on next boot; done jobs' rows are removed by
        -- cleanup_checkpoints alongside the index rows.
        CREATE TABLE IF NOT EXISTS job_checkpoints_domains (
            job_id      TEXT NOT NULL,
            domain      TEXT NOT NULL,
            processed_at TEXT NOT NULL,
            PRIMARY KEY (job_id, domain)
        );

        CREATE INDEX IF NOT EXISTS idx_checkpoints_domain_job
            ON job_checkpoints_domains (job_id);

        CREATE INDEX IF NOT EXISTS idx_phone_enrichments_url
            ON phone_enrichments (linkedin_url);

        -- Contacts DB write-back outbox (2026-06-14)
        -- Durable retry queue for write_enrichment_result() calls that fail
        -- with transient errors. Idempotent — the underlying upsert is keyed
        -- by email, so re-sending a queued payload is safe.
        CREATE TABLE IF NOT EXISTS contacts_write_outbox (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id          TEXT,
            row_index       INTEGER,
            payload_json    TEXT NOT NULL,
            status          TEXT NOT NULL DEFAULT 'pending',  -- pending|done|failed
            attempt_count   INTEGER NOT NULL DEFAULT 0,
            last_error      TEXT,
            next_retry_at   INTEGER NOT NULL DEFAULT 0,
            created_at      TEXT NOT NULL,
            updated_at      TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_outbox_status
            ON contacts_write_outbox (status, next_retry_at);

        CREATE INDEX IF NOT EXISTS idx_outbox_job
            ON contacts_write_outbox (job_id, row_index);

        -- Cross-process token-bucket rate limiter state (2026-08-14)
        -- Shared across gunicorn workers via SQLite (WAL). One row per
        -- provider; shared/rate_limiter.py performs atomic refills under
        -- BEGIN IMMEDIATE. Idempotent — the limiter module also creates
        -- this lazily as defense in depth.
        CREATE TABLE IF NOT EXISTS provider_rate_limit (
            provider       TEXT PRIMARY KEY,
            tokens         REAL NOT NULL,
            last_refill_ts REAL NOT NULL,
            capacity       REAL NOT NULL,
            refill_per_sec REAL NOT NULL
        );

        -- SEG (Secure Email Gateway) MX classification cache (2026-08-25).
        -- Written by enrichment/seg.py: one row per normalized domain, both
        -- contacts-DB hits (source='contacts_db') and DoH scan results
        -- (source='doh'). Domains no layer could classify are negative-cached
        -- with seg_classification='' (empty-string sentinel, column is NOT
        -- NULL but '' is legal) so repeated misses don't re-hit the API/DNS;
        -- '' rows are re-checked after a 7-day TTL via fetched_at. Idempotent
        -- CREATE IF NOT EXISTS — no migration of existing tables.
        CREATE TABLE IF NOT EXISTS domain_seg_cache (
            domain            TEXT PRIMARY KEY,
            seg_classification TEXT,
            seg_provider       TEXT,
            source             TEXT NOT NULL DEFAULT '',
            mx_hosts           TEXT,                      -- JSON array text
            fetched_at         TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_domain_seg_cache_fetched
            ON domain_seg_cache(fetched_at);

        -- Website-scrape nightly sync state (2026-08-26). Single row (id=1)
        -- holding the incremental watermark (remote terminal completed_at of
        -- the last fully-pushed batch) plus last-run telemetry. Written ONLY
        -- by the standalone sync process (enrichment/website_scrape_sync.py);
        -- the app creates the table and reads it for the UI "data as of"
        -- display. Idempotent CREATE IF NOT EXISTS — no migration.
        CREATE TABLE IF NOT EXISTS website_scrape_sync_state (
            id               INTEGER PRIMARY KEY CHECK (id = 1),
            watermark        TEXT,
            watermark_id     INTEGER,
            last_run_at      TEXT,
            last_run_status  TEXT,
            rows_pulled      INTEGER NOT NULL DEFAULT 0,
            rows_pushed      INTEGER NOT NULL DEFAULT 0,
            skipped_junk     INTEGER NOT NULL DEFAULT 0,
            errors           INTEGER NOT NULL DEFAULT 0
        );
        """
    )

    # Safe migrations: add columns to existing jobs table (idempotent).
    # (job_events/users are created above by the script, or by auth.init_auth_db
    # / history on an existing install — the script's CREATE IF NOT EXISTS no-ops.)
    # last_heartbeat must migrate AFTER the CREATE so a FRESH database — where
    # jobs did not exist before this call — does not crash on the ALTER.
    try:
        c.execute("SELECT last_heartbeat FROM jobs LIMIT 1")
    except sqlite3.OperationalError:
        c.execute("ALTER TABLE jobs ADD COLUMN last_heartbeat TEXT")
    existing_columns = {row[1] for row in c.execute("PRAGMA table_info(jobs)").fetchall()}
    if "used_providers" not in existing_columns:
        c.execute("ALTER TABLE jobs ADD COLUMN used_providers TEXT DEFAULT ''")
    if "selected_providers" not in existing_columns:
        c.execute("ALTER TABLE jobs ADD COLUMN selected_providers TEXT DEFAULT ''")
    # Per-provider email counters (2026-06-12) — surface in frontend job
    # stats. Frontend line ~2633 in frontend/index.html reads
    # job.emails_wizleads; without this column the value is undefined.
    for col in (
        "emails_contacts_db",
        "emails_blitz",
        "emails_smartprospect",
        "emails_better_enrich",
        "emails_wizleads",
        "emails_prospeo",
    ):
        if col not in existing_columns:
            c.execute(f"ALTER TABLE jobs ADD COLUMN {col} INTEGER DEFAULT 0")
    # Identifier column mappings (2026-06-12) — survive restarts and resume.
    for col in (
        "linkedin_url_col",
        "phone_col",
        "company_name_col",
        "existing_email_col",
    ):
        if col not in existing_columns:
            c.execute(f"ALTER TABLE jobs ADD COLUMN {col} TEXT DEFAULT ''")
    # Partial output path (2026-06-14) — tracks where the in-progress CSV
    # was renamed to when a job was abandoned. Used by the resume flow to
    # surface a "Download Partial" button and to merge into the restarted
    # job's output. Without this column the user had no way to recover any
    # rows that were completed before a server crash.
    if "partial_output_path" not in existing_columns:
        c.execute("ALTER TABLE jobs ADD COLUMN partial_output_path TEXT DEFAULT ''")
    # Pre-processing flags (2026-06-16) — two user-toggleable knobs that
    # run before the provider cascade: normalize_domains (whether
    # identifier_utils.normalize_domain() is applied per row) and
    # dedupe_by_domain (whether to collapse input rows by the domain
    # column before enrichment). deduped_rows counts the collapsed rows;
    # dedupe_skipped_domains stores the raw values of the dropped rows
    # for auditability. Defaults preserve prior behavior.
    for col, default in (
        ("normalize_domains", "1"),
        ("dedupe_by_domain", "1"),
        ("deduped_rows", "0"),
    ):
        if col not in existing_columns:
            c.execute(
                f"ALTER TABLE jobs ADD COLUMN {col} INTEGER DEFAULT {default}"
            )
    if "dedupe_skipped_domains" not in existing_columns:
        c.execute("ALTER TABLE jobs ADD COLUMN dedupe_skipped_domains TEXT DEFAULT ''")
    # Source provenance (2026-07-30) — distinguishes how an enrichment job
    # originated so the UI can label/filter jobs by origin in one click,
    # disambiguating the overloaded parent_job_id (scraper-chain vs restart):
    #   'google_maps_chain' — chained from a Google Maps scraper job
    #   'csv_upload'        — started from a manually-uploaded domains CSV
    #   'restart'           — resumed/restarted from a prior enrichment job
    if "source_type" not in existing_columns:
        c.execute("ALTER TABLE jobs ADD COLUMN source_type TEXT DEFAULT ''")
    # Website-only mode flag (2026-08-27) — Flow 1 jobs run with
    # website_only=True read exclusively from the website_scrape cohort in the
    # Contacts DB (zero paid providers). Persisted so restarts/resumes keep
    # the mode; '0'/NULL = normal cascade (today's behavior).
    if "website_only" not in existing_columns:
        c.execute("ALTER TABLE jobs ADD COLUMN website_only INTEGER DEFAULT 0")
    # Resume claim marker (2026-08-24) — cross-process mutex for enrichment
    # auto-resume. The 4-worker fan-out bug (hyperke-saas chain, 4 children in
    # 5ms) happened because the "already has a child?" check and the child
    # creation were not atomic. The claim transaction sets this timestamp on
    # the abandoned parent inside BEGIN IMMEDIATE; only the winner proceeds to
    # build the resume child. NULL = never claimed.
    if "resume_claimed_at" not in existing_columns:
        c.execute("ALTER TABLE jobs ADD COLUMN resume_claimed_at TEXT")

    c.commit()


def _today_date() -> str:
    """Return today's UTC date in YYYY-MM-DD format."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def get_daily_request_count(user_id: str) -> int:
    """Get the API request count for a user today."""
    c = get_db()
    row = c.execute(
        "SELECT request_count FROM daily_api_requests WHERE user_id = ? AND date = ?",
        (user_id, _today_date())
    ).fetchone()
    return row["request_count"] if row else 0


def check_daily_request_limit(user_id: str, is_admin: bool, estimated_requests: int = 0) -> tuple[bool, str]:
    """
    Check if a user can make more API requests today.

    Args:
        user_id: The user's ID
        is_admin: Whether the user is an admin (admins are exempt from limits)
        estimated_requests: Estimated number of requests for the pending operation

    Returns:
        (allowed: bool, message: str)
    """
    if is_admin:
        return True, ""

    current_count = get_daily_request_count(user_id)
    projected_count = current_count + estimated_requests

    if projected_count > NON_ADMIN_DAILY_REQUEST_LIMIT:
        remaining = NON_ADMIN_DAILY_REQUEST_LIMIT - current_count
        return False, (
            f"Daily API request limit exceeded. You have used {current_count:,} of "
            f"{NON_ADMIN_DAILY_REQUEST_LIMIT:,} daily requests. "
            f"Remaining: {remaining:,}. Please try again tomorrow."
        )

    return True, ""


def record_api_requests(user_id: str, count: int) -> None:
    """
    Record API requests for a user today.

    Args:
        user_id: The user's ID
        count: Number of requests to record (can be 0, will upsert)
    """
    c = get_db()
    today = _today_date()

    # Upsert: insert or update the request count
    c.execute(
        """
        INSERT INTO daily_api_requests (user_id, date, request_count)
        VALUES (?, ?, ?)
        ON CONFLICT(user_id, date) DO UPDATE SET
            request_count = request_count + ?
        """,
        (user_id, today, count, count)
    )
    c.commit()


def record_api_request(user_id: str) -> None:
    """Record a single API request for a user today."""
    record_api_requests(user_id, 1)


def get_api_quota_status(user_id: str, is_admin: bool) -> dict[str, any]:
    """
    Get the current API quota status for a user.

    Returns a dict with:
        - limit: The daily limit (None for admins)
        - used: Number of requests used today
        - remaining: Number of requests remaining (None for admins)
        - resets_at: When the quota resets (midnight UTC)
    """
    if is_admin:
        return {
            "limit": None,
            "used": 0,
            "remaining": None,
            "resets_at": None,
            "is_admin": True
        }

    used = get_daily_request_count(user_id)
    remaining = max(0, NON_ADMIN_DAILY_REQUEST_LIMIT - used)

    # Calculate reset time (midnight UTC tomorrow)
    tomorrow = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    from datetime import timedelta
    tomorrow += timedelta(days=1)

    return {
        "limit": NON_ADMIN_DAILY_REQUEST_LIMIT,
        "used": used,
        "remaining": remaining,
        "resets_at": tomorrow.isoformat(),
        "is_admin": False
    }


def close_db() -> None:
    """Close the current thread's database connection."""
    if hasattr(_local, "conn") and _local.conn is not None:
        _local.conn.close()
        _local.conn = None
