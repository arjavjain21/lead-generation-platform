"""
Backfill script: route historical enrichment CSVs through contacts_writer.

Re-routes rows from past enrichment jobs that never went through the
centralized writer (e.g. completed before the writer existed, or ran on
a path that bypassed it).  Idempotent — the writer's upsert is keyed by
email, so re-sending rows is safe.

Usage:
    # Dry run: count rows, report schema
    python -m enrichment.scripts.backfill_contacts --dry-run \
        --jobs chiropractors_d60ccf6b,cosmetic_derm_cb484575

    # Live: actually call contacts_writer against the Contacts DB
    python -m enrichment.scripts.backfill_contacts --live \
        --jobs chiropractors_d60ccf6b,cosmetic_derm_cb484575

    # Use a literal job_id instead of an alias
    python -m enrichment.scripts.backfill_contacts --live \
        --job-ids 1782ac2c-65ed-49fe-a82c-c408dd4ba9aa

    # Custom aliases (e.g. for ad-hoc backfills)
    python -m enrichment.scripts.backfill_contacts --live \
        --alias chiropractors=1782ac2c-65ed-49fe-a82c-c408dd4ba9aa \
        --alias cosmetic_derm=1e44cc87-f83e-4384-b73d-471b02ce58ca
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Optional

# Make this script runnable both as `python -m enrichment.scripts.backfill_contacts`
# and as `python backend/enrichment/scripts/backfill_contacts.py`.
_HERE = Path(__file__).resolve()
_BACKEND = _HERE.parents[2]
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

# Load .env so environment variables (CONTACTS_API_TOKEN, etc.) are available
# when this script is invoked outside the systemd service environment.
from dotenv import load_dotenv  # noqa: E402

load_dotenv(_BACKEND / ".env")

from shared.db import get_db  # noqa: E402

logger = logging.getLogger("backfill_contacts")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

# Known aliases → job_id mapping (extensible via --alias flag).
_DEFAULT_ALIASES: dict[str, str] = {
    "chiropractors": "1782ac2c-65ed-49fe-a82c-c408dd4ba9aa",
    "chiropractors_d60ccf6b": "1782ac2c-65ed-49fe-a82c-c408dd4ba9aa",
    "cosmetic_derm": "1e44cc87-f83e-4384-b73d-471b02ce58ca",
    "cosmetic_dermatology": "1e44cc87-f83e-4384-b73d-471b02ce58ca",
    "cosmetic_derm_cb484575": "1e44cc87-f83e-4384-b73d-471b02ce58ca",
    "chiropractors_failed": "c2f64b78-bd9b-41ef-98fd-6cb04a3e56d3",
}


def _resolve_job_ids(
    aliases: list[str],
    job_ids: list[str],
    extra_aliases: list[str],
) -> list[str]:
    """Resolve --jobs aliases and --job-ids into concrete job_ids."""
    table = dict(_DEFAULT_ALIASES)
    for pair in extra_aliases:
        if "=" not in pair:
            raise SystemExit(f"--alias must be in name=job_id form, got: {pair!r}")
        name, jid = pair.split("=", 1)
        table[name.strip()] = jid.strip()
    resolved: list[str] = []
    for alias in aliases:
        if alias not in table:
            raise SystemExit(
                f"Unknown alias: {alias!r}. "
                f"Use --alias name=job_id to register it, or --job-ids to specify directly."
            )
        resolved.append(table[alias])
    resolved.extend(job_ids)
    if not resolved:
        raise SystemExit("No jobs specified. Use --jobs or --job-ids.")
    return resolved


def _load_job(job_id: str) -> Optional[dict[str, Any]]:
    """Read job metadata from jobs.db."""
    c = get_db()
    row = c.execute(
        "SELECT job_id, job_type, original_filename, filename, status, "
        "total, processed, emails_found, output_path "
        "FROM jobs WHERE job_id = ?",
        (job_id,),
    ).fetchone()
    if not row:
        return None
    return dict(row)


def _csv_to_payloads(output_path: Path) -> list[dict[str, Any]]:
    """Read an enrichment CSV and convert each row to a writer payload."""
    if not output_path.exists():
        raise FileNotFoundError(f"Output CSV missing: {output_path}")
    payloads: list[dict[str, Any]] = []
    with open(output_path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for idx, row in enumerate(reader):
            dm_email = (row.get("dm_email") or "").strip()
            company_email = (row.get("company_email") or "").strip()
            if not dm_email and not company_email:
                continue
            # Domain: try input_domain first, then derive from email
            domain = (row.get("input_domain") or "").strip()
            if not domain and dm_email and "@" in dm_email:
                domain = dm_email.split("@", 1)[1]
            payloads.append({
                "row_index": idx,
                "domain": domain,
                "normalized_domain": domain,
                "dm_email": dm_email,
                "dm_full_name": (row.get("dm_full_name") or "").strip(),
                "dm_first_name": (row.get("dm_first_name") or "").strip(),
                "dm_last_name": (row.get("dm_last_name") or "").strip(),
                "dm_title": (row.get("dm_title") or "").strip(),
                "dm_linkedin_url": (row.get("dm_linkedin_url") or "").strip(),
                "dm_email_source": (row.get("dm_email_source") or "").strip(),
                "dm_email_verified": (row.get("dm_email_verified") or "unknown").strip(),
                "dm_headline": (row.get("dm_headline") or "").strip(),
                "dm_location_city": (row.get("dm_location_city") or "").strip(),
                "dm_location_country": (row.get("dm_location_country") or "").strip(),
                "dm_icp_tier": row.get("dm_icp_tier") or "",
                "mailtester_code": (row.get("mailtester_code") or "").strip(),
                "mailtester_message": (row.get("mailtester_message") or "").strip(),
                "company_email": company_email,
                "company_email_source": (row.get("company_email_source") or "").strip(),
                "company_email_verified": (row.get("company_email_verified") or "").strip(),
                "company_linkedin_url": (row.get("company_linkedin_url") or "").strip(),
                "company_name": (row.get("company_name") or "").strip(),
            })
    return payloads


def _report_dry(job_id: str, job: dict[str, Any], payloads: list[dict[str, Any]]) -> dict[str, Any]:
    """Build a dry-run report for one job."""
    dm_count = sum(1 for p in payloads if p.get("dm_email"))
    company_count = sum(1 for p in payloads if p.get("company_email") and not p.get("dm_email"))
    sources: dict[str, int] = {}
    for p in payloads:
        src = p.get("dm_email_source") or "(none)"
        sources[src] = sources.get(src, 0) + 1
    return {
        "job_id": job_id,
        "original_filename": job.get("original_filename"),
        "job_status": job.get("status"),
        "total": job.get("total"),
        "processed": job.get("processed"),
        "emails_found": job.get("emails_found"),
        "output_csv": job.get("output_path"),
        "payloads_to_write": len(payloads),
        "with_dm_email": dm_count,
        "with_company_email_only": company_count,
        "email_sources": sources,
    }


async def _run_live(
    job_id: str,
    job: dict[str, Any],
    payloads: list[dict[str, Any]],
) -> dict[str, Any]:
    """Run live write-back for one job, return aggregate result."""
    from enrichment import contacts_writer  # local import: heavy + async
    if not payloads:
        return {"job_id": job_id, "skipped": True, "reason": "no payloads"}
    logger.info("Backfilling job %s (%s): %d rows", job_id, job.get("original_filename"), len(payloads))
    result = await contacts_writer.write_enrichment_result_batch(payloads, job_id=job_id)
    out = result.to_dict()
    out["job_id"] = job_id
    out["original_filename"] = job.get("original_filename")
    return out


async def amain(args: argparse.Namespace) -> int:
    job_ids = _resolve_job_ids(args.jobs, args.job_ids, args.alias or [])
    reports: list[dict[str, Any]] = []
    for jid in job_ids:
        job = _load_job(jid)
        if not job:
            logger.error("Job not found in jobs.db: %s", jid)
            reports.append({"job_id": jid, "error": "not_found"})
            continue
        output_path = Path(job["output_path"]) if job.get("output_path") else None
        if not output_path or not output_path.exists():
            logger.error("Job %s has no output CSV (path=%s)", jid, output_path)
            reports.append({"job_id": jid, "error": "no_output_csv"})
            continue
        try:
            payloads = _csv_to_payloads(output_path)
        except Exception as e:
            logger.exception("Failed to read %s: %s", output_path, e)
            reports.append({"job_id": jid, "error": str(e)})
            continue
        if args.dry_run:
            reports.append(_report_dry(jid, job, payloads))
        else:
            reports.append(await _run_live(jid, job, payloads))
    print(json.dumps(reports, indent=2, default=str))
    # Exit code: 0 on dry-run, 0 if no failures on live, 1 if any errors.
    if not args.dry_run:
        for r in reports:
            if r.get("error") or r.get("failed", 0) > 0:
                return 1
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dry-run", action="store_true", help="Report what would be written without calling the API.")
    p.add_argument("--live", action="store_true", help="Actually call contacts_writer (overrides --dry-run).")
    p.add_argument("--jobs", default="", help="Comma-separated aliases (e.g. chiropractors,cosmetic_derm).")
    p.add_argument("--job-ids", default="", help="Comma-separated job_ids (UUIDs) to backfill.")
    p.add_argument("--alias", action="append", default=[], help="Register alias as name=job_id (repeatable).")
    args = p.parse_args()
    if args.dry_run and args.live:
        p.error("Specify only one of --dry-run or --live.")
    if not args.dry_run and not args.live:
        args.dry_run = True  # default to dry-run for safety
    args.jobs = [s.strip() for s in args.jobs.split(",") if s.strip()]
    args.job_ids = [s.strip() for s in args.job_ids.split(",") if s.strip()]
    return asyncio.run(amain(args))


if __name__ == "__main__":
    raise SystemExit(main())
