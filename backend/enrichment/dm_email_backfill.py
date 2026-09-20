"""Idle-time backfill: work_email for decision-makers with LinkedIn but no email.

Standalone maintenance job (website_scrape_sync pattern) — runs OUTSIDE the
web workers as a systemd oneshot + timer::

    DM_EMAIL_BACKFILL_ENABLED=true python enrichment/dm_email_backfill.py \
        [--limit N] [--chunk N] [--dry-run] [--reset]

Run via the FILE PATH, not ``python -m``: ``-m`` executes the package
``__init__`` (which imports pipeline → getleads_client) BEFORE this module's
dotenv bootstrap, so GETLEADS_API_KEY would be captured empty at import time.

What it does
------------
1. Pulls a keyset-paginated batch of ``core.decision_makers`` rows that have
   a ``linkedin_url`` but no ``work_email`` (LEFT JOIN company website), via
   local peer-auth psql (``sudo -n -u postgres`` — verified passwordless).
2. Looks up verified emails via GetLeads ``/api/v1/enrich/from-linkedin``
   (chunks of 100 through ``getleads_client._post_enrich`` — the SHARED
   cross-process token bucket, so this job can never overspend RPS against
   the web workers; a 402 fair-use response pauses the run).
3. Writes hits through ``contacts_writer.write_enrichment_result_batch`` —
   the sanctioned path (outbox, person/company split, industry
   classification, ``source_name="getleads_dm_backfill"``). The contacts-api
   upsert lands on the existing linkedin_norm owner (56c1c1f fix), so the
   email attaches to the person instead of forking a duplicate.
4. Mirrors ``work_email`` (plus a bonus phone when the DM has none) onto
   ``core.decision_makers`` — the DM search surface filters on that
   denormalized column, so it must be set for users to see the email.

Design pins (enrichment/tests/test_dm_email_backfill.py):
* Kill-switch: ``DM_EMAIL_BACKFILL_ENABLED`` unset/false => refuse to run.
* Cost: GetLeads unlimited-plan fair use ONLY — zero Blitz FUP records.
  A 402 PAUSES the run WITHOUT advancing the watermark past the affected
  chunk (exit code 75); the next timer tick resumes the same rows.
* Watermark: single row in jobs.db ``dm_email_backfill_state``; keyset
  pagination on ``dm_id``; advanced only after a chunk is fully processed,
  so a mid-run crash re-pulls at most one chunk (writes are idempotent
  email-keyed upserts; the pull only returns rows still missing email).
* One sweep = one pass over dm_id space. When the pass exhausts, the state
  flips to 'done' and the job goes idle; ``--reset`` starts a fresh pass
  (previous misses get one re-try per pass — GetLeads coverage changes).
* Dry-run: pulls + lookups + reports, writes nothing, stamps no watermark.
  Lookups still spend fair use — keep ``--limit`` small.

Env vars (all optional; sane defaults):
* ``DM_EMAIL_BACKFILL_ENABLED`` — 'true' to arm (default false)
* ``DM_BACKFILL_RUN_ROWS``      — rows per run (default 6000; hourly timer
                                  ~= 144k/day, well under the 500k fair-use
                                  daily cap shared with interactive work)
* ``DM_BACKFILL_CHUNK``         — URLs per GetLeads call (default 100 = API max)
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import fcntl
import logging
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Optional

if __name__ == "__main__":  # pragma: no cover - standalone bootstrap
    _BACKEND = Path(__file__).resolve().parents[1]
    if str(_BACKEND) not in sys.path:
        sys.path.insert(0, str(_BACKEND))
    from dotenv import load_dotenv

    load_dotenv(_BACKEND / ".env")

import httpx

from enrichment import contacts_writer, getleads_client, pipeline
from shared import db

logger = logging.getLogger("dm_email_backfill")

_ZERO_UUID = "00000000-0000-0000-0000-000000000000"
_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)
_SOURCE_TAG = "getleads_dm_backfill"
_LOCK_PATH = "/tmp/dm_email_backfill.lock"
_PSQL_BASE = ["sudo", "-n", "-u", "postgres", "psql", "-p", "5432", "contacts"]
_PSQL_TIMEOUT_S = 180

EXIT_OK = 0
EXIT_PAUSED = 75  # GetLeads fair-use/credits 402 — retry next timer tick
EXIT_ERROR = 1

DEFAULT_RUN_ROWS = 6000
DEFAULT_CHUNK = 100  # GetLeads server hard cap per request


# ---------------------------------------------------------------------------
# State (single watermark row in jobs.db, website_scrape_sync_state pattern)
# ---------------------------------------------------------------------------


def _init_state_table() -> None:
    conn = db.get_db()
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS dm_email_backfill_state (
            id               INTEGER PRIMARY KEY CHECK (id = 1),
            last_dm_id       TEXT    NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000',
            sweep_status     TEXT    NOT NULL DEFAULT 'idle',  -- idle|running|done
            sweep_started_at TEXT,
            last_run_at      TEXT,
            last_run_status  TEXT,
            rows_processed   INTEGER NOT NULL DEFAULT 0,
            emails_found     INTEGER NOT NULL DEFAULT 0,
            dm_rows_updated  INTEGER NOT NULL DEFAULT 0
        );
        INSERT OR IGNORE INTO dm_email_backfill_state (id) VALUES (1);
        """
    )
    conn.commit()


def _load_state() -> dict[str, Any]:
    _init_state_table()
    conn = db.get_db()
    row = conn.execute(
        "SELECT * FROM dm_email_backfill_state WHERE id = 1"
    ).fetchone()
    return dict(row) if row else {}


def _update_state(
    sets: Optional[dict[str, Any]] = None,
    increments: Optional[dict[str, int]] = None,
) -> None:
    """Update the watermark row. Column names come from code only."""
    assignments: list[str] = []
    values: list[Any] = []
    for col, val in (sets or {}).items():
        assignments.append(f"{col} = ?")
        values.append(val)
    for col, inc in (increments or {}).items():
        assignments.append(f"{col} = {col} + ?")
        values.append(inc)
    if not assignments:
        return
    conn = db.get_db()
    conn.execute(
        f"UPDATE dm_email_backfill_state SET {', '.join(assignments)} WHERE id = 1",
        tuple(values),
    )
    conn.commit()


def _now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()


def _reset_sweep() -> None:
    _update_state(
        sets={
            "last_dm_id": _ZERO_UUID,
            "sweep_status": "running",
            "sweep_started_at": _now_iso(),
            "rows_processed": 0,
            "emails_found": 0,
            "dm_rows_updated": 0,
        }
    )
    logger.info("sweep reset — starting a fresh pass from %s", _ZERO_UUID)


# ---------------------------------------------------------------------------
# Contacts DB access (local peer-auth psql via sudo; SELECT for pulls,
# one temp-table UPDATE for the DM column mirror)
# ---------------------------------------------------------------------------


def _psql(script: str) -> str:
    """Run one psql script (rides stdin, never argv) as the postgres peer user."""
    proc = subprocess.run(
        _PSQL_BASE + ["-A", "-t", "-F", "\t", "-v", "ON_ERROR_STOP=1"],
        input=script,
        capture_output=True,
        text=True,
        timeout=_PSQL_TIMEOUT_S,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"psql failed (rc={proc.returncode}): {proc.stderr.strip()[:300]}"
        )
    return proc.stdout


_PULL_SQL = """
SELECT dm.dm_id::text,
       dm.linkedin_url,
       COALESCE(dm.first_name, ''),
       COALESCE(dm.last_name, ''),
       COALESCE(dm.full_name, ''),
       COALESCE(dm.job_title, ''),
       COALESCE(dm.headline, ''),
       COALESCE(c.website, '')
FROM core.decision_makers dm
LEFT JOIN core.company c ON c.company_id = dm.company_id
WHERE dm.dm_id > '{watermark}'::uuid
  AND (dm.work_email IS NULL OR dm.work_email = '')
  AND dm.linkedin_url IS NOT NULL AND dm.linkedin_url <> ''
ORDER BY dm.dm_id
LIMIT {limit};
"""


def _pull_batch(watermark: str, limit: int) -> list[dict[str, str]]:
    """Keyset-paginated pull of no-email DM rows above the watermark."""
    if not _UUID_RE.match(watermark or ""):
        raise ValueError(f"invalid watermark (expected uuid): {watermark!r}")
    out = _psql(_PULL_SQL.format(watermark=watermark, limit=int(limit)))
    rows: list[dict[str, str]] = []
    for line in out.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) != 8:
            logger.warning("skipping malformed pull row (%d fields)", len(parts))
            continue
        rows.append(
            {
                "dm_id": parts[0],
                "linkedin_url": parts[1],
                "first_name": parts[2],
                "last_name": parts[3],
                "full_name": parts[4],
                "job_title": parts[5],
                "headline": parts[6],
                "company_website": parts[7],
            }
        )
    return rows


def _apply_dm_updates(updates: list[dict[str, str]]) -> int:
    """Mirror emails onto core.decision_makers (work_email + bonus phone).

    The DM search endpoint filters on the denormalized ``dm.work_email``
    column, so the API-side person/email upsert alone is not user-visible.
    Values ride a CSV + temp table (never string-interpolated SQL).
    """
    if not updates:
        return 0
    with tempfile.NamedTemporaryFile(
        "w", suffix=".dm_backfill.csv", delete=False, newline=""
    ) as fh:
        writer = csv.writer(fh)
        for u in updates:
            writer.writerow([u["dm_id"], u["email"], u.get("phone") or ""])
        csv_path = fh.name
    os.chmod(csv_path, 0o644)  # psql (\copy) runs as the postgres user
    script = (
        "CREATE TEMP TABLE _dm_email_upd(dm_id text, email text, phone text);\n"
        f"\\copy _dm_email_upd FROM '{csv_path}' WITH (FORMAT csv)\n"
        "UPDATE core.decision_makers dm SET "
        "work_email = u.email, "
        "phone = COALESCE(NULLIF(dm.phone, ''), NULLIF(u.phone, '')), "
        "updated_at = now() "
        "FROM _dm_email_upd u WHERE dm.dm_id::text = u.dm_id;\n"
    )
    try:
        out = _psql(script)
    finally:
        os.unlink(csv_path)
    match = re.search(r"UPDATE (\d+)\s*$", out.strip())
    return int(match.group(1)) if match else len(updates)


# ---------------------------------------------------------------------------
# GetLeads lookup + payload construction
# ---------------------------------------------------------------------------


def _domain_of(email: str) -> str:
    return email.split("@", 1)[1].strip().lower() if "@" in email else ""


def _build_payload(row: dict[str, str], hit: dict[str, Any]) -> dict[str, Any]:
    """contacts_writer payload for one GetLeads hit (never mutates inputs)."""
    email = str(hit.get("email") or "").strip()
    domain = contacts_writer._normalize_domain(
        hit.get("domain") or row.get("company_website") or _domain_of(email)
    )
    return {
        "dm_email": email,
        "dm_linkedin_url": row["linkedin_url"],
        "dm_first_name": hit.get("first_name") or row.get("first_name") or "",
        "dm_last_name": hit.get("last_name") or row.get("last_name") or "",
        "dm_full_name": hit.get("person_full_name") or row.get("full_name") or "",
        "dm_title": hit.get("job_title") or row.get("job_title") or "",
        "dm_headline": hit.get("linkedin_headline") or row.get("headline") or "",
        "dm_phone": hit.get("phone") or "",
        "dm_email_verified": "yes"
        if hit.get("verification_status") == "Valid"
        else "",
        "domain": domain,
        "company_name": hit.get("company_name") or "",
        "source_name": _SOURCE_TAG,
    }


async def _lookup_chunk(
    client: httpx.AsyncClient, urls: list[str]
) -> tuple[str, dict[str, dict[str, Any]]]:
    """One GetLeads from-linkedin call (<=100 URLs).

    Returns (status, normalized-by-url) where status is 'ok' | 'paused' |
    'error'. 'paused' = 402 fair-use/credits — the caller must stop WITHOUT
    advancing the watermark; 'error' = network/breaker chunk failure —
    retried on the next run from the same watermark.
    """
    result = await getleads_client._post_enrich(
        client,
        [{"linkedin_url": u} for u in urls],
        path=getleads_client._FROM_LINKEDIN_PATH,
        method_name="dm_email_backfill",
        raw=True,
        extra_body={"limit_per_item": 1},
    )
    if pipeline._is_provider_error(result):
        return "paused", {}
    if result is None:
        return "error", {}
    normalized: dict[str, dict[str, Any]] = {}
    for i, url in enumerate(urls):
        item = result[i] if i < len(result) else {}
        normalized[url] = getleads_client._normalize_linkedin_result_item(item, url)
    return "ok", normalized


# ---------------------------------------------------------------------------
# Run loop
# ---------------------------------------------------------------------------


async def run_backfill(*, limit: int, chunk_size: int, dry_run: bool) -> int:
    """Process up to ``limit`` no-email DM rows. Returns an EXIT_* code."""
    state = _load_state()
    if state.get("sweep_status") == "done":
        logger.info("sweep already complete — use --reset for a fresh pass")
        return EXIT_OK

    watermark = state.get("last_dm_id") or _ZERO_UUID
    rows = _pull_batch(watermark, limit)
    if not rows:
        _update_state(sets={"sweep_status": "done", "last_run_at": _now_iso()})
        logger.info("pass complete — no more no-email DM rows above watermark")
        return EXIT_OK

    if not dry_run:
        _update_state(sets={"sweep_status": "running"})

    stats = {"rows": 0, "hits": 0, "written": 0, "dm_updated": 0}
    total_chunks = (len(rows) + chunk_size - 1) // chunk_size
    async with httpx.AsyncClient(timeout=60.0) as client:
        for chunk_index in range(total_chunks):
            chunk_rows = rows[chunk_index * chunk_size : (chunk_index + 1) * chunk_size]
            url_map: dict[str, list[dict[str, str]]] = {}
            for r in chunk_rows:
                url_map.setdefault(r["linkedin_url"], []).append(r)

            status, normalized = await _lookup_chunk(client, list(url_map.keys()))
            if status == "paused":
                logger.warning(
                    "GetLeads fair-use/credits pause — stopping without "
                    "advancing past chunk %d/%d",
                    chunk_index + 1,
                    total_chunks,
                )
                _update_state(
                    sets={"last_run_at": _now_iso(), "last_run_status": "paused"},
                    increments={
                        "rows_processed": stats["rows"],
                        "emails_found": stats["hits"],
                        "dm_rows_updated": stats["dm_updated"],
                    },
                )
                return EXIT_PAUSED
            if status == "error":
                logger.error(
                    "GetLeads chunk %d/%d failed — next run retries this chunk",
                    chunk_index + 1,
                    total_chunks,
                )
                _update_state(
                    sets={"last_run_at": _now_iso(), "last_run_status": "error"},
                    increments={
                        "rows_processed": stats["rows"],
                        "emails_found": stats["hits"],
                        "dm_rows_updated": stats["dm_updated"],
                    },
                )
                return EXIT_ERROR

            payloads: list[dict[str, Any]] = []
            dm_updates: list[dict[str, str]] = []
            for url, norm in normalized.items():
                if not norm.get("email"):
                    continue
                for r in url_map.get(url, []):
                    payloads.append(_build_payload(r, norm))
                    dm_updates.append(
                        {
                            "dm_id": r["dm_id"],
                            "email": str(norm["email"]),
                            "phone": str(norm.get("phone") or ""),
                        }
                    )

            if payloads:
                if dry_run:
                    for sample in payloads[:3]:
                        logger.info(
                            "[dry-run] would write %s -> %s",
                            sample["dm_linkedin_url"],
                            sample["dm_email"],
                        )
                else:
                    write = await contacts_writer.write_enrichment_result_batch(
                        payloads, job_id=None
                    )
                    stats["written"] += write.total
                    logger.info(
                        "contacts_writer: inserted=%d updated=%d skipped=%d "
                        "queued=%d failed=%d",
                        write.inserted,
                        write.updated,
                        write.skipped,
                        write.queued,
                        write.failed,
                    )

            applied = _apply_dm_updates(dm_updates) if dm_updates and not dry_run else 0
            stats["dm_updated"] += applied

            stats["rows"] += len(chunk_rows)
            stats["hits"] += len(payloads)
            if not dry_run:
                _update_state(
                    sets={"last_dm_id": chunk_rows[-1]["dm_id"]},
                    increments={
                        "rows_processed": len(chunk_rows),
                        "emails_found": len(payloads),
                        "dm_rows_updated": applied,
                    },
                )
            logger.info(
                "chunk %d/%d: rows=%d hits=%d (cum rows=%d hits=%d)",
                chunk_index + 1,
                total_chunks,
                len(chunk_rows),
                len(payloads),
                stats["rows"],
                stats["hits"],
            )

    _update_state(
        sets={"last_run_at": _now_iso(), "last_run_status": "ok" if not dry_run else "dry_run"},
        increments={} if dry_run else None,
    )
    logger.info("run complete: %s", stats)
    return EXIT_OK


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def _acquire_lock() -> Optional[Any]:
    """Non-blocking flock so a timer tick never overlaps a live run."""
    fh = open(_LOCK_PATH, "w")
    try:
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        fh.close()
        return None
    return fh


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning("%s=%r is not an int — using default %d", name, raw, default)
        return default


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--limit", type=int, default=_env_int("DM_BACKFILL_RUN_ROWS", DEFAULT_RUN_ROWS),
                        help="max DM rows to process this run")
    parser.add_argument("--chunk", type=int, default=min(_env_int("DM_BACKFILL_CHUNK", DEFAULT_CHUNK), 100),
                        help="URLs per GetLeads call (max 100)")
    parser.add_argument("--dry-run", action="store_true",
                        help="lookups only — no writes, no watermark advance")
    parser.add_argument("--reset", action="store_true",
                        help="start a fresh sweep pass from the beginning")
    args = parser.parse_args(argv)

    if os.getenv("DM_EMAIL_BACKFILL_ENABLED", "").strip().lower() != "true":
        logger.error("DM_EMAIL_BACKFILL_ENABLED is not 'true' — refusing to run")
        return EXIT_ERROR
    if not getleads_client.API_KEY:
        logger.error("GETLEADS_API_KEY not configured — refusing to run")
        return EXIT_ERROR

    lock = _acquire_lock()
    if lock is None:
        logger.warning("another dm_email_backfill instance holds the lock — exiting")
        return EXIT_ERROR

    try:
        if args.reset:
            _reset_sweep()
        return asyncio.run(
            run_backfill(limit=max(args.limit, 1), chunk_size=max(args.chunk, 1), dry_run=args.dry_run)
        )
    finally:
        lock.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    sys.exit(main())
