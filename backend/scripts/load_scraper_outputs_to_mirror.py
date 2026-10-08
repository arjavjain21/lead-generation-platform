#!/usr/bin/env python3
"""
Scrape-mirror loader (Phase 2, 2026-10-07) — keeps `scraped_places` (PG 5433
lead_gen) evergreen by ingesting scraper job-output CSVs from the outputs dir.

Designed to run nightly from scrape-mirror-loader.timer (systemd) as User=postgres
(peer-auth Unix socket, NO password anywhere), and manually for backfills:
    sudo -u postgres /var/www/lead-generation-platform/backend/venv/bin/python \
        load_scraper_outputs_to_mirror.py --since 2026-08-24

Rules of engagement (deliberate):
  - Only files whose header is SCRAPER-shaped (has place_id + name + full_address
    and no dm_email column) are touched. The outputs dir is shared with
    enrichment-job CSVs; those are never ingested.
  - Idempotent per FILE: mirror_ingest_log tracks (path, mtime, size); files
    already logged with identical mtime+size are skipped. Changed file = re-ingest.
  - Rows go in under the 'out:' dedupe-key namespace ('out:<place_id>', or
    'out:h:<md5(name|address|website)>' when place_id is empty). Existing rows in
    OTHER namespaces (platform_scrape / gmaps_import_20260824) are never modified.
  - Within the namespace an upsert refreshes base fields (a re-scrape of the same
    place updates reviews/rating/website), data_source stays 'scraper_output'.
  - Exit code is always 0 (a loader must not fail its unit); errors are logged
    per-file into mirror_ingest_log.status and to stderr.

Tables touched (all additive): scraped_places (base columns only),
mirror_ingest_log (new). DB: lead_gen on port 5433 — the isolated mirror cluster.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Iterator, Optional

import psycopg2
from psycopg2.extras import execute_values

# --- wiring -----------------------------------------------------------------

BACKEND_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT_DEFAULT = "/mnt/disk/outputs"
PG_DSN_KWARGS = {
    "dbname": "lead_gen",
    "host": "/var/run/postgresql",  # Unix socket, peer auth (run as postgres)
    "port": 5433,
    "connect_timeout": 15,
}
BATCH_ROWS = 2000
# Base identity columns shared with scraper output CSVs (same lineage).
MIRROR_COLUMNS = [
    "dedupe_key", "place_id", "name", "category_name", "full_address", "city",
    "city_state", "latitude", "longitude", "rating", "review_count", "website",
    "phone", "types", "place_link", "query", "data_source",
]
# A file is scraper-shaped when the place-column core is present. Pure
# enrichment outputs (domain-keyed, company_name/dm_*) lack these, so the
# required set alone discriminates — chain-enriched place files (place core +
# appended dm_* columns) ARE wanted and their extra columns are simply ignored.
SCRAPER_REQUIRED = {"place_id", "name", "full_address"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("mirror_loader")


# --- classification ----------------------------------------------------------

def is_scraper_output(path: str) -> bool:
    """Header sniff: scraper CSVs carry place columns and never dm_* columns."""
    try:
        with open(path, "r", encoding="utf-8", errors="replace", newline="") as fh:
            header = next(csv.reader(fh), [])
    except OSError:
        return False
    cols = {h.strip() for h in header}
    return SCRAPER_REQUIRED.issubset(cols)


def candidate_files(outputs_dir: str, since: Optional[str]) -> list[str]:
    out: list[str] = []
    since_ts = (
        datetime.strptime(since, "%Y-%m-%d")
        .replace(tzinfo=timezone.utc)
        .timestamp()
        if since
        else None
    )
    for entry in sorted(os.listdir(outputs_dir)):
        path = os.path.join(outputs_dir, entry)
        if not path.endswith(".csv") or not os.path.isfile(path):
            continue
        mtime = os.path.getmtime(path)
        if since_ts and mtime < since_ts:
            continue
        out.append((mtime, path))
    return [p for _, p in sorted(out)]


# --- ingest ------------------------------------------------------------------

def row_dedupe_key(row: dict) -> str:
    place_id = (row.get("place_id") or "").strip()
    if place_id:
        return f"out:{place_id}"
    basis = "|".join(
        (row.get(k) or "").strip().lower()
        for k in ("name", "full_address", "website")
    )
    return "out:h:" + hashlib.md5(basis.encode("utf-8")).hexdigest()


def iter_mirror_rows(path: str) -> Iterator[tuple]:
    """Yield MIRROR_COLUMNS tuples for well-formed rows; skip blanks."""
    with open(path, "r", encoding="utf-8", errors="replace", newline="") as fh:
        reader = csv.DictReader(fh)
        cols = {h.strip() for h in (reader.fieldnames or [])}
        if not SCRAPER_REQUIRED.issubset(cols):
            return
        for raw in reader:
            if not (raw.get("name") or "").strip():
                continue
            row = {k.strip(): v for k, v in raw.items() if k is not None}
            record = []
            for col in MIRROR_COLUMNS:
                if col == "dedupe_key":
                    record.append(row_dedupe_key(row))
                elif col == "data_source":
                    record.append("scraper_output")
                else:
                    val = (row.get(col) or "").strip()
                    record.append(val if val else None)
            yield tuple(record)


def ingest_file(conn, path: str) -> tuple[int, int]:
    """Insert/update one file's rows. Returns (rows_read, rows_written).

    A place can appear multiple times in one CSV (multi-zoom jobs return the
    same place at several zooms). ON CONFLICT cannot affect the same row twice
    in one statement, so repeated dedupe_keys within a file are collapsed to
    their FIRST occurrence via `seen`.
    """
    rows_read = rows_written = 0
    buffer: list[tuple] = []
    seen: set[str] = set()

    def flush() -> None:
        nonlocal rows_written
        if not buffer:
            return
        execute_values(
            conn.cursor(),
            """
            INSERT INTO scraped_places (dedupe_key, place_id, name, category_name,
              full_address, city, city_state, latitude, longitude, rating,
              review_count, website, phone, types, place_link, query, data_source)
            VALUES %s
            ON CONFLICT (dedupe_key) DO UPDATE SET
              name           = COALESCE(NULLIF(EXCLUDED.name, ''), scraped_places.name),
              category_name  = COALESCE(NULLIF(EXCLUDED.category_name, ''), scraped_places.category_name),
              full_address   = COALESCE(NULLIF(EXCLUDED.full_address, ''), scraped_places.full_address),
              city           = COALESCE(NULLIF(EXCLUDED.city, ''), scraped_places.city),
              city_state     = COALESCE(NULLIF(EXCLUDED.city_state, ''), scraped_places.city_state),
              latitude       = COALESCE(EXCLUDED.latitude, scraped_places.latitude),
              longitude      = COALESCE(EXCLUDED.longitude, scraped_places.longitude),
              rating         = COALESCE(EXCLUDED.rating, scraped_places.rating),
              review_count   = COALESCE(EXCLUDED.review_count, scraped_places.review_count),
              website        = COALESCE(NULLIF(EXCLUDED.website, ''), scraped_places.website),
              phone          = COALESCE(NULLIF(EXCLUDED.phone, ''), scraped_places.phone),
              types          = COALESCE(NULLIF(EXCLUDED.types, ''), scraped_places.types),
              place_link     = COALESCE(NULLIF(EXCLUDED.place_link, ''), scraped_places.place_link)
            """,
            buffer,
            page_size=BATCH_ROWS,
        )
        rows_written += len(buffer)
        buffer.clear()
        # Commit per batch (not per file): a 300MB nationwide output can take
        # an hour+ to stream; per-batch commits keep transactions short and a
        # crash mid-file costs at most one 2K-row batch (re-ingest is idempotent).
        conn.commit()

    numeric = {"latitude", "longitude", "rating"}
    int_cols = {"review_count"}
    for record in iter_mirror_rows(path):
        key = record[0]  # dedupe_key is MIRROR_COLUMNS[0]
        if key in seen:
            continue  # same place earlier in this file — already queued
        seen.add(key)
        rows_read += 1
        typed = list(record)
        for i, col in enumerate(MIRROR_COLUMNS):
            if col in numeric and typed[i] is not None:
                try:
                    typed[i] = float(typed[i])
                except ValueError:
                    typed[i] = None
            elif col in int_cols and typed[i] is not None:
                digits = str(typed[i]).split(".")[0]
                typed[i] = int(digits) if digits.lstrip("-").isdigit() else None
        buffer.append(tuple(typed))
        if len(buffer) >= BATCH_ROWS:
            flush()
    flush()
    conn.commit()
    return rows_read, rows_written


# --- bookkeeping --------------------------------------------------------------

def ensure_tables(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS mirror_ingest_log (
              file_path   TEXT PRIMARY KEY,
              mtime       DOUBLE PRECISION NOT NULL,
              size_bytes  BIGINT NOT NULL,
              rows_read   INTEGER,
              rows_written INTEGER,
              status      TEXT NOT NULL DEFAULT 'ok',
              note        TEXT,
              ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
    conn.commit()


def logged_fingerprint(conn, path: str) -> Optional[tuple]:
    """Fingerprint of a successfully-ingested file — error files return None so
    they are retried on the next run instead of being skipped forever."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT mtime, size_bytes FROM mirror_ingest_log "
            "WHERE file_path = %s AND status = 'ok'",
            (path,),
        )
        return cur.fetchone()


def mark_logged(conn, path: str, mtime: float, size: int,
                rows_read: int, rows_written: int,
                status: str = "ok", note: Optional[str] = None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO mirror_ingest_log (file_path, mtime, size_bytes, rows_read,
                                           rows_written, status, note, ingested_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (file_path) DO UPDATE SET
              mtime = EXCLUDED.mtime, size_bytes = EXCLUDED.size_bytes,
              rows_read = EXCLUDED.rows_read, rows_written = EXCLUDED.rows_written,
              status = EXCLUDED.status, note = EXCLUDED.note,
              ingested_at = now()
            """,
            (path, mtime, size, rows_read, rows_written, status, note),
        )
    conn.commit()


# --- main ---------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--outputs-dir", default=OUT_DEFAULT)
    ap.add_argument("--since", help="only files with mtime on/after YYYY-MM-DD")
    ap.add_argument("--limit", type=int, default=0, help="max files this run (0=all)")
    ap.add_argument("--dry-run", action="store_true",
                    help="classify + report only, no writes")
    args = ap.parse_args()

    if args.dry_run:
        scraper = skipped = 0
        for path in candidate_files(args.outputs_dir, args.since):
            if is_scraper_output(path):
                scraper += 1
            else:
                skipped += 1
        print(f"dry-run: scraper-shaped={scraper} non-scraper={skipped} "
              f"(dir={args.outputs_dir}, since={args.since})")
        return 0

    conn = psycopg2.connect(**PG_DSN_KWARGS)
    try:
        ensure_tables(conn)
        processed = ingested = skipped = errors = 0
        total_rows = total_written = 0
        for path in candidate_files(args.outputs_dir, args.since):
            if args.limit and processed >= args.limit:
                break
            processed += 1
            if not is_scraper_output(path):
                skipped += 1
                continue
            mtime = os.path.getmtime(path)
            size = os.path.getsize(path)
            fp = logged_fingerprint(conn, path)
            if fp and abs(fp[0] - mtime) < 1 and fp[1] == size:
                skipped += 1
                continue
            try:
                rows_read, rows_written = ingest_file(conn, path)
                mark_logged(conn, path, mtime, size, rows_read, rows_written)
                ingested += 1
                total_rows += rows_read
                total_written += rows_written
                log.info("%s: %d rows read, %d written",
                         os.path.basename(path), rows_read, rows_written)
            except Exception as exc:  # per-file isolation
                conn.rollback()
                errors += 1
                mark_logged(conn, path, mtime, size, None, None,
                            status="error", note=str(exc)[:500])
                log.error("%s: %s", os.path.basename(path), exc)
        log.info(
            "run done: files=%d ingested=%d skipped=%d errors=%d "
            "rows_read=%d rows_written=%d",
            processed, ingested, skipped, errors, total_rows, total_written,
        )
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
