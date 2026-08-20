#!/usr/bin/env python3
"""Backfill company emails from completed-job CSVs into leadsdatabase.cc.

Context: the contacts-api `ON CONFLICT (website_norm)` bug rejected every
company-shaped upsert, so company/generic emails (info@, contact@, hello@, ...)
from jobs that ran during the bug window exist only in the output CSVs — not in
leadsdatabase.cc, hence not retrievable by API. This script re-pushes them now
that the bug is fixed.

- Pushes ONLY rows with a non-empty `company_email` (company path). Person
  (`dm_email`) emails are deliberately NOT re-pushed (they already succeeded);
  payloads here carry no `dm_email`, so the writer's person path is a no-op.
- Idempotent: email-keyed upsert; re-running is safe (already-stored rows SKIP).
- CONCURRENT push (bounded by a semaphore) → saturates the contacts-api 75 RPS
  rate limiter (~20x faster than sequential). The limiter itself caps us at 75/s,
  which is exactly what live jobs already use.

Usage:
  python scripts/backfill_company_emails.py --csv <name> --limit 20 --print-sample   # dry-run
  python scripts/backfill_company_emails.py                                            # full run, all CSVs
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import sys
from collections import Counter
from pathlib import Path

import httpx

BACKEND = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BACKEND))

from dotenv import load_dotenv  # noqa: E402
load_dotenv(BACKEND / ".env")

from enrichment import contacts_writer  # noqa: E402

OUTPUT_DIR = BACKEND / "data" / "outputs"
_NOT_EMAILS = {"no_email", "n/a", "none", "null", ""}


def _meaningful(s: str) -> bool:
    s = (s or "").strip().lower()
    return bool(s) and "@" in s and s not in _NOT_EMAILS


def build_company_payload(row: dict, idx: int) -> dict | None:
    """Build a company-only payload from a CSV row (no dm_email)."""
    ce = (row.get("company_email") or "").strip()
    if not _meaningful(ce):
        return None
    domain = (
        row.get("input_domain")
        or row.get("domain")
        or row.get("website")
        or row.get("normalized_domain")
        or ""
    ).strip()
    if not domain and "@" in ce:
        domain = ce.split("@", 1)[-1]
    return {
        "company_email": ce,
        "domain": domain,
        "company_name": (row.get("company_name") or "").strip(),
        "company_linkedin_url": (row.get("company_linkedin_url") or "").strip(),
        "company_email_source": (row.get("company_email_source") or "").strip(),
        "company_email_verified": (row.get("company_email_verified") or "").strip(),
        "company_email_type": (row.get("company_email_type") or "").strip(),
        "source_path": (
            row.get("company_email_source_path") or row.get("source_path") or ""
        ).strip(),
        "row_index": idx,
        # NOTE: no dm_email -> person upsert is a no-op; only the company writes.
    }


def iter_company_payloads(csv_path: Path, limit: int | None = None) -> list[dict]:
    payloads: list[dict] = []
    with open(csv_path, newline="", encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        if "company_email" not in (reader.fieldnames or []):
            return payloads
        for i, row in enumerate(reader):
            p = build_company_payload(row, i)
            if p:
                payloads.append(p)
                if limit and len(payloads) >= limit:
                    break
    return payloads


async def push_concurrent(payloads: list[dict], job_id: str, concurrency: int = 20, chunk: int = 2000) -> dict:
    """Push payloads concurrently through contacts_writer.

    A semaphore bounds in-flight calls; the contacts-api rate limiter
    (_acquire_upsert_rate_limit, asyncio.Lock + interval) paces the actual
    upserts to 75/s regardless of concurrency. A shared httpx client is reused.
    """
    if not payloads:
        return {}
    sem = asyncio.Semaphore(concurrency)
    client = httpx.AsyncClient(timeout=30.0)
    counts: Counter = Counter()
    total = len(payloads)
    done = 0

    async def one(payload: dict):
        async with sem:
            return await contacts_writer.write_enrichment_result(payload, client=client, job_id=job_id)

    try:
        for i in range(0, total, chunk):
            batch = payloads[i:i + chunk]
            results = await asyncio.gather(*(one(p) for p in batch), return_exceptions=True)
            for r in results:
                if isinstance(r, Exception):
                    counts["ERROR"] += 1
                else:
                    # WriteStatus enum -> its name (INSERTED/UPDATED/SKIPPED/QUEUED/FAILED/NO_DATA)
                    counts[getattr(r, "name", str(r))] += 1
            done += len(batch)
            ins = counts.get("INSERTED", 0)
            print(f"   ... {done}/{total} done | inserted={ins} skipped={counts.get('SKIPPED',0)} updated={counts.get('UPDATED',0)} queued={counts.get('QUEUED',0)} failed={counts.get('FAILED',0)} errors={counts.get('ERROR',0)}", flush=True)
    finally:
        await client.aclose()
    return dict(counts)


async def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--csv", help="single CSV filename in data/outputs/ (default: all)")
    ap.add_argument("--limit", type=int, help="max company rows to push (per CSV)")
    ap.add_argument("--job-id", default="backfill-company-emails")
    ap.add_argument("--concurrency", type=int, default=20, help="in-flight upserts (rate capped at 75/s anyway)")
    ap.add_argument("--print-sample", action="store_true", help="print pushed payloads for read-back")
    args = ap.parse_args()

    if not contacts_writer.is_v2_enabled():
        print("ERROR: USE_CONTACTS_WRITER_V2 is off — enable it to use the outbox path.", file=sys.stderr)
        sys.exit(2)

    if args.csv:
        csvs = [OUTPUT_DIR / args.csv]
    else:
        csvs = sorted(OUTPUT_DIR.glob("*.csv"))

    grand: Counter = Counter()
    files_done = 0
    for csv_path in csvs:
        if not csv_path.exists():
            print(f"skip (missing): {csv_path.name}")
            continue
        payloads = iter_company_payloads(csv_path, limit=args.limit)
        if not payloads:
            continue
        print(f"\n=== {csv_path.name}: {len(payloads)} company emails ===", flush=True)
        counts = await push_concurrent(payloads, args.job_id, args.concurrency)
        grand.update(counts)
        files_done += 1
        print(f"{csv_path.name}: {counts}", flush=True)
        if args.print_sample:
            for p in payloads[: (args.limit or 5)]:
                print(f"   sample: {p.get('company_email')}  domain={p.get('domain')}")
    print(f"\nFILES DONE: {files_done}   AGGREGATE: {dict(grand)}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
