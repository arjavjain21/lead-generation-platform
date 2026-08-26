"""Nightly curate-and-push sync: webscraper.eagleinfoservice.com → Contacts DB.

Docs: docs/WEBSITE_SCRAPE_INTEGRATION_PLAN.md. Runs OUTSIDE the web workers as
a standalone systemd service (+timer), invoked as::

    python -m enrichment.website_scrape_sync [--dry-run] [--limit N]

Design pins (enrichment/tests/test_website_scrape_sync.py):
* Kill-switch: WEBSITE_SCRAPE_SYNC_ENABLED unset/false ⇒ refuse to run.
* Watermark: single row in jobs.db ``website_scrape_sync_state``; keyset
  pagination on remote (completed_at, id); monotonic; advanced only after a
  fully-processed batch so a mid-run crash loses nothing (re-pull is harmless
  — pushes are idempotent upserts keyed on email).
* Transport: SSH to the scraping VPS (key auth only), remote psql as the
  locked-down ``leadgen_sync`` SELECT-only role via TCP scram + remote
  ~/.pgpass. SQL rides stdin — never -c, never argv, never sudo. No DB
  credentials exist on this VPS; the SSH key is the only secret.
* Push: ONLY via ``contacts_writer.write_enrichment_result_batch`` (outbox,
  retry, person/company split, idempotency). Zero direct SQL to the contacts DB.
* Throttle: push-side rate limited (default 40 rows/s — contacts cluster
  saturation incident 2026-08-06); pull-side batches are small.
* Dry-run: curates and counts, pushes nothing, stamps no watermark.

Env vars (all optional; sane defaults):
* WEBSITE_SCRAPE_SYNC_ENABLED  — 'true' to arm (default false)
* WEBSITE_SCRAPE_BATCH_SIZE    — rows per batch (default 500)
* WEBSITE_SCRAPE_SYNC_RPS      — push throttle rows/sec (default 40)
* WEBSITE_SCRAPE_SHARED_ND_CAP — curation cap (default 20)
* WEBSITE_SCRAPE_TIMEOUT_S     — per-pull SSH timeout seconds (default 300)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable, Optional

if __name__ == "__main__":  # pragma: no cover - standalone bootstrap
    _BACKEND = Path(__file__).resolve().parents[1]
    if str(_BACKEND) not in sys.path:
        sys.path.insert(0, str(_BACKEND))
    from dotenv import load_dotenv

    load_dotenv(_BACKEND / ".env")

logger = logging.getLogger("website_scrape_sync")

DEFAULT_HOST_ALIAS = "webscraper-vps"
DEFAULT_BATCH_SIZE = 500
DEFAULT_RPS = 40.0
DEFAULT_SHARED_ND_CAP = 20
DEFAULT_TIMEOUT_S = 300
STATE_TABLE = "website_scrape_sync_state"

_REMOTE_DB = "email_enrichment"
_REMOTE_USER = "leadgen_sync"
_REMOTE_HOST_DB = "127.0.0.1"


class LoudFailure(Exception):
    """Mirrors contacts_writer.LoudFailure abort semantics (outbox down)."""


# ---------------------------------------------------------------------------
# State store (jobs.db — single-row table, written only by the sync process)
# ---------------------------------------------------------------------------


def init_state_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {STATE_TABLE} (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            watermark TEXT,
            last_run_at TEXT,
            last_run_status TEXT,
            rows_pulled INTEGER NOT NULL DEFAULT 0,
            rows_pushed INTEGER NOT NULL DEFAULT 0,
            skipped_junk INTEGER NOT NULL DEFAULT 0,
            errors INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    conn.execute(f"INSERT OR IGNORE INTO {STATE_TABLE} (id) VALUES (1)")
    conn.commit()


class SyncStateStore:
    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def get_watermark(self) -> Optional[str]:
        row = self._conn.execute(
            f"SELECT watermark FROM {STATE_TABLE} WHERE id=1"
        ).fetchone()
        return row[0] if row and row[0] else None

    def get_state(self) -> dict[str, Any]:
        cursor = self._conn.execute(f"SELECT * FROM {STATE_TABLE} WHERE id=1")
        row = cursor.fetchone()
        cols = [d[0] for d in cursor.description]
        return dict(zip(cols, row))

    def set_watermark(self, watermark: str, rows_pulled: int, rows_pushed: int) -> None:
        """Monotonic: never regresses. Lexicographic compare is valid for the
        single canonical ISO-8601 format remote completed_at emits."""
        current = self.get_watermark()
        if current is not None and watermark <= current:
            return
        self._conn.execute(
            f"UPDATE {STATE_TABLE} SET watermark=?, rows_pulled=?, rows_pushed=? WHERE id=1",
            (watermark, rows_pulled, rows_pushed),
        )
        self._conn.commit()

    def record_run(self, status: str, rows_pulled: int, rows_pushed: int, skipped_junk: int, errors: int) -> None:
        self._conn.execute(
            f"""
            UPDATE {STATE_TABLE}
            SET last_run_at=datetime('now'), last_run_status=?, rows_pulled=?, rows_pushed=?,
                skipped_junk=?, errors=?
            WHERE id=1
            """,
            (status, rows_pulled, rows_pushed, skipped_junk, errors),
        )
        self._conn.commit()


# ---------------------------------------------------------------------------
# Remote pull (SSH + psql over stdin)
# ---------------------------------------------------------------------------


def build_remote_psql_command(sql: str) -> list[str]:
    """Local argv that runs psql ON the scraping VPS as leadgen_sync.

    The SQL itself travels via stdin (subprocess input=), never argv. Remote
    psql authenticates via TCP scram with ~/.pgpass ON THE REMOTE host — no DB
    password ever exists locally, and sudo is never used.
    """
    remote = (
        f"psql -h {_REMOTE_HOST_DB} -U {_REMOTE_USER} -d {_REMOTE_DB} "
        "--no-psqlrc --quiet -A -F $'\\t' -t -v ON_ERROR_STOP=1"
    )
    return ["ssh", DEFAULT_HOST_ALIAS, "--", remote]


def build_pull_query(watermark: Optional[str], limit: int) -> str:
    """Keyset-paginated pull of terminal rows, oldest first so the watermark
    advances monotonically. Reads ONLY email_enrichment — the gmaps_places
    enrichment happens in a second, PK-keyed query (build_gmaps_query); joining
    them remotely forces a double seq-scan over 2.4M+2.4M rows (measured:
    times out)."""
    predicate = ""
    if watermark:
        predicate = f"AND (completed_at, id) > ('{watermark}', 0)"
    return f"""
SELECT id, domain, email, email_class, email_type, email_confidence,
       email_shared_nd, status, business_name, page_title, industry,
       completed_at, metadata::text
FROM email_enrichment
WHERE status = 'completed'
  AND email IS NOT NULL
  {predicate}
ORDER BY completed_at, id
LIMIT {int(limit)}
""".strip()


def build_gmaps_query(domains: list[str]) -> str:
    """Fetch gmaps_places rows for an already-pulled batch, keyed by website.
    Deduplicates per domain locally (one place per domain — franchise branches
    share sites; first match wins). domains are normalized bare domains."""
    rendered = ",".join("'" + d.replace("'", "''") + "'" for d in sorted(set(domains)))
    return f"""
SELECT website, city, state, rating, reviews_count, gmaps_types::text,
       google_maps_url, address, postal_code, country
FROM gmaps_places
WHERE website IS NOT NULL
  AND lower(website) IN ({rendered})
""".strip()


def _remote_psql_prefix() -> str:
    return (
        f"psql -h {_REMOTE_HOST_DB} -U {_REMOTE_USER} -d {_REMOTE_DB} "
        "--no-psqlrc --quiet -A -F $'\\t' -t -v ON_ERROR_STOP=1"
    )


_PULL_COLUMNS = (
    "id", "domain", "email", "email_class", "email_type", "email_confidence",
    "email_shared_nd", "status", "business_name", "page_title", "industry",
    "completed_at", "metadata",
)

_GMAPS_COLUMNS = (
    "website", "city", "state", "rating", "reviews_count", "gmaps_types",
    "google_maps_url", "address", "postal_code", "country",
)


def _maybe_float(value: Optional[str]) -> Optional[float]:
    try:
        return float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _maybe_int(value: Optional[str]) -> Optional[int]:
    try:
        return int(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _maybe_json(value: Optional[str]) -> Any:
    try:
        return json.loads(value) if value else None
    except (ValueError, TypeError):
        return None


def parse_pull_row(fields: list[str]) -> dict[str, Any]:
    """Map one tab-split psql row (main query) to the curation dict shape."""
    record = dict(zip(_PULL_COLUMNS, fields))
    return {
        "id": _maybe_int(record.get("id")),
        "domain": record.get("domain"),
        "email": record.get("email"),
        "email_class": record.get("email_class"),
        "email_type": record.get("email_type"),
        "email_confidence": _maybe_float(record.get("email_confidence")),
        "email_shared_nd": _maybe_int(record.get("email_shared_nd")),
        "status": record.get("status"),
        "business_name": record.get("business_name"),
        "page_title": record.get("page_title"),
        "industry": record.get("industry"),
        "completed_at": record.get("completed_at"),
        "metadata": _maybe_json(record.get("metadata")) or {},
        "gmaps": None,
    }


def parse_gmaps_row(fields: list[str]) -> tuple[str, dict[str, Any]]:
    """Map one gmaps row to (website_key, payload)."""
    record = dict(zip(_GMAPS_COLUMNS, fields))
    website = (record.get("website") or "").strip().lower()
    payload = {
        "city": record.get("city") or None,
        "state": record.get("state") or None,
        "rating": _maybe_float(record.get("rating")),
        "reviews_count": _maybe_int(record.get("reviews_count")),
        "gmaps_types": _maybe_json(record.get("gmaps_types")),
        "google_maps_url": record.get("google_maps_url") or None,
        "address": record.get("address") or None,
        "postal_code": record.get("postal_code") or None,
        "country": record.get("country") or None,
    }
    return website, payload


def _strip_www(website: str) -> str:
    cleaned = website
    while cleaned.startswith("www."):
        cleaned = cleaned[4:]
    return cleaned


async def _run_remote_psql(sql: str, timeout_s: int) -> str:
    """Run one psql command on the scraping VPS; SQL via stdin. Raises on
    non-zero exit. Timeout kills the local ssh, and ssh's remote-side channel
    close terminates the remote psql (no orphan sessions)."""
    cmd = build_remote_psql_command(sql)
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(
            proc.communicate(input=sql.encode()), timeout=timeout_s
        )
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        raise RuntimeError(f"remote psql timed out after {timeout_s}s") from None
    if proc.returncode != 0:
        raise RuntimeError(
            f"remote psql failed rc={proc.returncode}: {stderr.decode()[:500]}"
        )
    return stdout.decode()


def make_ssh_pull(
    host_alias: str = DEFAULT_HOST_ALIAS, timeout_s: int = DEFAULT_TIMEOUT_S
) -> Callable[[Optional[str], int], Any]:
    """Build the real pull callable: phase 1 pulls the email_enrichment batch;
    phase 2 enriches with gmaps data via a website-keyed IN-list query. The old
    single-query LEFT JOIN forced a double seq-scan (measured: >5 min for a
    1K batch); two indexed queries return in ~2s."""

    async def _pull(watermark: Optional[str], limit: int) -> list[dict[str, Any]]:
        main_sql = build_pull_query(watermark, limit)
        stdout = await _run_remote_psql(main_sql, timeout_s)
        rows = [
            parse_pull_row(line.split("\t"))
            for line in stdout.splitlines()
            if line.strip()
        ]

        domains = sorted({row["domain"] for row in rows if row.get("domain")})
        if not domains:
            return rows

        # Chunk to keep the IN-list sane for large batches.
        gmaps_by_domain: dict[str, dict[str, Any]] = {}
        chunk_size = 500
        for start in range(0, len(domains), chunk_size):
            chunk = domains[start : start + chunk_size]
            gmaps_sql = build_gmaps_query(chunk)
            gmaps_out = await _run_remote_psql(gmaps_sql, timeout_s)
            for line in gmaps_out.splitlines():
                if not line.strip():
                    continue
                website, payload = parse_gmaps_row(line.split("\t"))
                key = _strip_www(website)
                # First match wins (franchise branches share websites).
                if key and key not in gmaps_by_domain:
                    gmaps_by_domain[key] = payload

        for row in rows:
            key = _strip_www((row.get("domain") or "").lower())
            if key in gmaps_by_domain:
                row["gmaps"] = gmaps_by_domain[key]
        return rows

    return _pull


# ---------------------------------------------------------------------------
# Push (contacts_writer only)
# ---------------------------------------------------------------------------


def make_writer_push() -> Callable[[list[dict[str, Any]], Optional[str]], Any]:
    """Build the real push callable (async) delegating to contacts_writer."""
    from . import contacts_writer

    async def _push(payloads: list[dict[str, Any]], job_id: Optional[str] = None) -> Any:
        return await contacts_writer.write_enrichment_result_batch(payloads, job_id=job_id)

    return _push


def compute_throttle_sleep(batch_size: int, rps: float) -> float:
    """Seconds to sleep after pushing a batch to hold ~rps overall."""
    if rps <= 0:
        return 0.0
    return max(0.0, min(1.0, batch_size / rps))


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def _is_enabled_env() -> bool:
    return os.environ.get("WEBSITE_SCRAPE_SYNC_ENABLED", "false").lower() == "true"


async def run_sync(
    *,
    enabled: Optional[bool] = None,
    dry_run: bool = False,
    limit: Optional[int] = None,
    store: SyncStateStore,
    _pull: Optional[Callable] = None,
    _push: Optional[Callable] = None,
    batch_size: Optional[int] = None,
    throttle_rps: Optional[float] = None,
    shared_nd_cap: Optional[int] = None,
) -> dict[str, Any]:
    """One sync run. Returns a summary dict; never raises except LoudFailure."""
    from .website_scrape_curation import (
        CurationPolicy,
        build_company_payload,
        build_named_contact_payloads,
        curate_row,
    )

    if enabled is None:
        enabled = _is_enabled_env()
    if not enabled:
        logger.info("website-scrape sync disabled (WEBSITE_SCRAPE_SYNC_ENABLED != true)")
        return {"status": "disabled"}

    batch_size = batch_size or int(os.environ.get("WEBSITE_SCRAPE_BATCH_SIZE", DEFAULT_BATCH_SIZE))
    throttle_rps = throttle_rps or float(os.environ.get("WEBSITE_SCRAPE_SYNC_RPS", DEFAULT_RPS))
    cap = shared_nd_cap if shared_nd_cap is not None else int(
        os.environ.get("WEBSITE_SCRAPE_SHARED_ND_CAP", DEFAULT_SHARED_ND_CAP)
    )
    policy = CurationPolicy(shared_nd_cap=cap)

    pull = _pull or make_ssh_pull()
    push = _push or make_writer_push()

    watermark = store.get_watermark()
    job_id = f"website_scrape_sync_{int(time.time())}"

    rows_pulled = 0
    rows_pushed = 0
    rows_failed = 0
    skipped_junk = 0
    batches = 0

    while True:
        fetch = batch_size if limit is None else min(batch_size, limit - rows_pulled)
        if fetch <= 0:
            break
        rows = await pull(watermark, fetch)
        if not rows:
            break

        payloads: list[dict[str, Any]] = []
        for row in rows:
            curated = curate_row(row, policy)
            if curated is None:
                skipped_junk += 1
                continue
            payloads.append(build_company_payload(curated, job_id=job_id))
            payloads.extend(build_named_contact_payloads(curated, job_id=job_id))

        rows_pulled += len(rows)
        if payloads and not dry_run:
            result = await push(payloads, job_id=job_id)
            summary = result.to_dict() if hasattr(result, "to_dict") else {}
            rows_pushed += summary.get("inserted", 0) + summary.get("updated", 0) + summary.get("skipped", 0)
            rows_failed += summary.get("failed", 0)
            batches += 1
            await asyncio.sleep(compute_throttle_sleep(len(payloads), throttle_rps))
        elif dry_run:
            batches += 1

        # Advance watermark to this batch's last row so the next pull keysets
        # past it. On dry-run we deliberately do NOT persist.
        watermark = rows[-1]["completed_at"]
        if not dry_run:
            store.set_watermark(watermark, rows_pulled=rows_pulled, rows_pushed=rows_pushed)

        if limit is not None and rows_pulled >= limit:
            break
        if len(rows) < fetch:
            break

    status = "success" if rows_failed == 0 else "partial"
    if not dry_run:
        store.record_run(
            status=status,
            rows_pulled=rows_pulled,
            rows_pushed=rows_pushed,
            skipped_junk=skipped_junk,
            errors=rows_failed,
        )
    return {
        "status": status,
        "dry_run": dry_run,
        "rows_pulled": rows_pulled,
        "curated": rows_pulled - skipped_junk,
        "rows_pushed": rows_pushed,
        "rows_failed": rows_failed,
        "skipped_junk": skipped_junk,
        "batches": batches,
        "watermark": watermark if dry_run else store.get_watermark(),
    }


def open_state_store(db_path: Optional[Path] = None) -> SyncStateStore:
    """Open jobs.db state table with WAL + busy_timeout (call_tracker pattern —
    safe to run alongside the live app)."""
    if db_path is None:
        db_path = Path(__file__).resolve().parents[1] / "data" / "jobs.db"
    conn = sqlite3.connect(str(db_path), timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    init_state_table(conn)
    return SyncStateStore(conn)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Website-scrape → Contacts DB nightly sync")
    parser.add_argument("--dry-run", action="store_true", help="curate + count only; push nothing")
    parser.add_argument("--limit", type=int, default=None, help="max rows to pull (testing)")
    parser.add_argument("--batch-size", type=int, default=None)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    if not _is_enabled_env() and not args.dry_run:
        print(json.dumps({"status": "disabled"}))
        return 0

    store = open_state_store()
    result = asyncio.run(
        run_sync(
            enabled=True,  # CLI already gated above
            dry_run=args.dry_run,
            limit=args.limit,
            batch_size=args.batch_size,
            store=store,
        )
    )
    print(json.dumps(result, indent=2))
    return 0 if result.get("status") in ("success", "partial") else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
