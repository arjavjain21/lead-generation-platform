#!/usr/bin/env python3
"""Standalone phone-backfill driver (runs in the backend venv, own process).

Reads the tier-ordered queue at /mnt/disk/phone_backfill/queue/urls.tsv
(lines: "<tier>\t<url>"), calls Blitz Direct Phone at BLITZ_RPS_PHONE (25),
stores found numbers into core.phone (source-tagged), and is fully resumable
(processed slugs in state/processed.log). Per-batch guards:

  * fair-use pool floor: stops if records_remaining < POOL_FLOOR (probe uses
    a known-miss URL — misses are free, live-verified 2026-10-10)
  * consecutive-error circuit: stops after ERROR_STOP consecutive call
    failures (auth death / sustained 5xx)

Billing truths (live-verified 2026-10-10): phone MISS = 0 FUP records,
phone HIT = 1 record from the shared 15M pool. Cost of this backfill ≈
number of phones found.

Usage:
    cd backend && source venv/bin/activate
    BLITZ_RPS_PHONE=25 nohup python scripts/phone_backfill_driver.py &
"""

from __future__ import annotations

import asyncio
import csv
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import httpx  # noqa: E402

# The phone client reads BLITZ_API_KEY from os.environ at import time; this
# standalone process never runs main.py's load_dotenv — inject from .env first.
_ENV = (Path(__file__).resolve().parent.parent / ".env").read_text()
for _line in _ENV.splitlines():
    if "=" in _line and not _line.startswith("#"):
        _k, _, _v = _line.partition("=")
        os.environ.setdefault(_k.strip(), _v.strip())

from phone_enrichment import client as phone_client  # noqa: E402

BASE = Path("/mnt/disk/phone_backfill")
QUEUE = Path(sys.argv[1]) if len(sys.argv) > 1 else BASE / "queue" / "urls.tsv"
STATE = BASE / "state"
RESULTS = BASE / "results"
PROCESSED = STATE / "processed.log"
STATUS = STATE / "status.json"
DRIVER_LOG = STATE / "driver.log"

BATCH = 2000
CONCURRENCY = 40
# STORE_MODE=off: write result CSVs but skip core.phone upserts (used when a
# recovery/storage transaction holds the table — CSVs are recovered later by
# the same staging upsert).
STORE_MODE = os.getenv("PHONE_BACKFILL_STORE", "on").lower() != "off"
POOL_FLOOR = 1_000_000
ERROR_STOP = 60
SOURCE_NAME = os.getenv("PHONE_BACKFILL_SOURCE", "blitz_direct_phone_backfill_20261010")
PROBE_URL = "https://www.linkedin.com/in/zzz-nonexistent-pool-probe"  # known miss -> free
BLITZ_KEY = re.search(r"^BLITZ_API_KEY=(.+)$", (Path(__file__).resolve().parent.parent / ".env").read_text(), re.M).group(1).strip()


def log(msg: str) -> None:
    line = f"{time.strftime('%H:%M:%S')} {msg}"
    print(line, flush=True)
    with open(DRIVER_LOG, "a") as f:
        f.write(line + "\n")


def write_status(**kw) -> None:
    data = json.loads(STATUS.read_text()) if STATUS.exists() else {}
    data.update(kw, updated_at=time.strftime("%Y-%m-%dT%H:%M:%S"))
    STATUS.write_text(json.dumps(data, indent=1))


async def pool_remaining() -> int | None:
    """Free fair-use pool sample via a known-miss probe (misses cost 0)."""
    try:
        async with httpx.AsyncClient() as c:
            r = await c.post(
                "https://api.blitz-api.ai/v2/enrichment/phone",
                headers={"x-api-key": BLITZ_KEY, "Content-Type": "application/json"},
                json={"person_linkedin_url": PROBE_URL},
                timeout=20.0,
            )
            fu = (r.json() or {}).get("fair_usage") or {}
            return int(fu.get("records_remaining") or 0) or None
    except Exception as e:
        log(f"pool probe failed: {e}")
        return None


def _db_stored_count() -> int:
    r = subprocess.run(
        ["sudo", "-u", "postgres", "psql", "-p", "5432", "contacts", "-qAt",
         "-c", f"SELECT COUNT(*) FROM core.phone WHERE source_name='{SOURCE_NAME}';"],
        capture_output=True, text=True, timeout=120,
    )
    try:
        return int(r.stdout.strip() or 0)
    except ValueError:
        return 0


def store_batch(rows: list[tuple[str, str]]) -> int:
    """Upsert found (slug, phone) rows into core.phone. Returns NEW rows stored
    (computed as a DB count delta — the reliable way; INSERT tags and RETURNING
    variants both proved fragile through psql stdin scripting)."""
    if not rows:
        return 0
    before = _db_stored_count()
    stage = RESULTS / "stage.tsv"
    with open(stage, "w") as f:
        for slug, phone in rows:
            f.write(f"{slug}\t{phone}\n")
    script = f"""
BEGIN;
CREATE TEMP TABLE _stage (linkedin_norm text, phone_e164 text);
\\copy _stage FROM '{stage}' WITH (FORMAT csv, DELIMITER E'\\t')
INSERT INTO core.phone (person_id, phone_e164, type, source_name)
SELECT p.person_id, s.phone_e164, 'mobile', '{SOURCE_NAME}'
FROM _stage s JOIN core.person p ON p.linkedin_norm = s.linkedin_norm
ON CONFLICT (phone_e164) DO NOTHING;
COMMIT;
"""
    proc = subprocess.run(
        ["sudo", "-u", "postgres", "psql", "-p", "5432", "contacts", "-qAt",
         "-v", "ON_ERROR_STOP=1"],
        input=script, capture_output=True, text=True, timeout=300,
    )
    if proc.returncode != 0:
        log(f"STORE ERROR: {proc.stderr[:300]}")
        return 0
    return max(0, _db_stored_count() - before)


async def main() -> None:
    if not QUEUE.exists():
        log("queue empty — nothing to do")
        return
    processed: set[str] = set()
    if PROCESSED.exists():
        processed = {ln.strip() for ln in PROCESSED.read_text().splitlines() if ln.strip()}

    pending: list[tuple[str, str]] = []
    seen: set[str] = set()
    with open(QUEUE) as f:
        for line in f:
            # Tolerate BOTH a real tab and a literal "\t" two-char sequence
            # (a psql -c escaping quirk observed in extractor output).
            parts = line.rstrip("\n").replace("\\t", "\t").split("\t")
            if len(parts) != 2:
                continue
            slug = parts[1].rstrip("/").split("/in/")[-1]
            if slug in processed or slug in seen:
                continue
            seen.add(slug)
            pending.append((parts[0], parts[1]))
    log(f"queue={len(pending)} processed={len(processed)} rps={phone_client._RATE_LIMIT_RPS}")

    done = found_total = stored_total = errors = 0
    write_status(started=True, queue=len(pending))

    http = httpx.AsyncClient()
    sem = asyncio.Semaphore(CONCURRENCY)
    outf = open(PROCESSED, "a")

    async def call_one(url: str) -> tuple[str, str | None]:
        nonlocal errors
        async with sem:
            for attempt in range(3):
                try:
                    r = await phone_client.find_phone(http, url)
                    errors = 0
                    return url, (r.get("phone") if r.get("found") else None)
                except Exception as e:
                    await asyncio.sleep(2 ** attempt)
            errors += 1
            return url, None

    batch_no = 0
    try:
        for i in range(0, len(pending), BATCH):
            batch = pending[i : i + BATCH]
            rows = await asyncio.gather(*(call_one(u) for _, u in batch))

            hits = [(u.rstrip("/").split("/in/")[-1], p) for u, p in rows if p]
            found_total += len(hits)
            done += len(batch)
            batch_no += 1

            _stamp = time.strftime("%H%M%S")
            with open(RESULTS / f"batch_{_stamp}_{batch_no:05d}.csv", "w", newline="") as f:
                w = csv.writer(f)
                w.writerow(["linkedin_url", "phone_number", "phone_found"])
                for u, p in rows:
                    w.writerow([u, p or "", bool(p)])

            stored = store_batch(hits) if STORE_MODE else 0
            stored_total += stored
            for _, u in batch:
                outf.write(u.rstrip("/").split("/in/")[-1] + "\n")
            outf.flush()

            pool = await pool_remaining()
            write_status(
                batch=batch_no, done=done, queue=len(pending),
                found_total=found_total, stored_total=stored_total,
                pool_remaining=pool,
            )
            log(f"batch {batch_no}: done={done}/{len(pending)} found={found_total} stored={stored_total} pool={pool}")

            if errors >= ERROR_STOP:
                write_status(stopped_reason=f"consecutive_errors={errors}")
                log(f"STOP: {errors} consecutive errors")
                break
            if pool is not None and pool < POOL_FLOOR:
                write_status(stopped_reason=f"pool_floor {pool} < {POOL_FLOOR}")
                log(f"STOP: pool {pool} below floor {POOL_FLOOR}")
                break
        else:
            write_status(stopped_reason=None, finished=True)
            log("QUEUE COMPLETE")
    finally:
        await http.aclose()
        outf.close()


if __name__ == "__main__":
    (STATE / "driver.pid").write_text(str(os.getpid()))
    asyncio.run(main())
