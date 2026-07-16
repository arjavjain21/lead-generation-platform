"""Recovery script: re-run a failed enrichment job in-place.

Resets the job's state and re-executes the original cascade. Output
overwrites the 0-byte file so the user's existing download link in the
UI works without any frontend changes.

Used to recover jobs affected by the 2026-07-11..13 silent-failure bug
(csv_jobs_silent_failure_2026-07-13.md).

Usage:
    cd backend && source venv/bin/activate
    python recover_failed_job.py <job_id>

Calls ``run_domain_enrichment`` directly (bypassing ``_run_domain_enrich_job``)
with ``check_cancelled=lambda: False`` — recovery jobs should not be
aborted by the abandonment check, which can fire spuriously when this
script runs outside the service's job-tracking infrastructure.
"""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from dotenv import load_dotenv
load_dotenv()

import pandas as pd
from enrichment import list_builder, job_store, identifier_utils, providers as _providers
from enrichment.raw_contact_collector import RawContactCollector
from enrichment.routes import (
    _cancelled_jobs,
    OUTPUT_DIR,
    _job_signals,
    _run_background_sync,
)
import logging
logger = logging.getLogger("recovery")


def load_job_record(job_id: str) -> dict:
    store = job_store.get_store()
    job = store.get_enrichment_job(job_id)
    if not job:
        raise SystemExit(f"Job {job_id} not found")
    return job


def reset_job_for_rerun(job_id: str) -> None:
    """Clear status/counters/error so the job looks fresh. Output_path is
    preserved so the same file gets overwritten."""
    store = job_store.get_store()
    conn = store.conn
    conn.execute(
        """UPDATE jobs SET
             status='running',
             processed=0,
             emails_found=0,
             emails_contacts_db=0,
             emails_blitz=0,
             emails_better_enrich=0,
             emails_prospeo=0,
             emails_wizleads=0,
             error='',
             used_providers='[]',
             last_heartbeat=datetime('now'),
             updated_at=datetime('now')
           WHERE job_id=?""",
        (job_id,),
    )
    conn.commit()
    print(f"  Reset job {job_id[:8]} → status=running, counters zeroed")


async def rerun(job_id: str) -> None:
    job = load_job_record(job_id)
    print(f"Re-running job {job_id[:8]}: {job.get('original_filename') or job.get('filename')}")

    upload_id = job["filename"]
    csv_path = Path(f"data/uploads/{upload_id}.csv")
    if not csv_path.exists():
        raise SystemExit(f"  CSV missing: {csv_path} — cannot recover")

    # Load CSV (same as flows endpoint)
    df = pd.read_csv(csv_path, low_memory=False).fillna("").astype(str)
    rows = df.to_dict(orient="records")
    print(f"  Loaded {len(rows)} rows from {csv_path.name}")

    # Dedupe (matches the flows endpoint logic)
    domain_col = job["domain_col"]
    normalize_domains = bool(job.get("normalize_domains", 1))
    dedupe_by_domain = bool(job.get("dedupe_by_domain", 1))
    if dedupe_by_domain:
        pre_dedupe_count = len(rows)
        rows, deduped_count, skipped = identifier_utils.dedupe_rows_by_domain(
            rows, domain_col, normalize_domains
        )
        print(f"  Deduped: {len(rows)} unique rows (dropped {deduped_count} of {pre_dedupe_count})")

    # Parse selected_providers
    sp = job.get("selected_providers") or "[]"
    try:
        selected_providers = json.loads(sp) if sp else None
    except json.JSONDecodeError:
        selected_providers = None

    reset_job_for_rerun(job_id)

    output_path = OUTPUT_DIR / f"{job_id}.csv"
    collector = RawContactCollector(job_id=job_id)
    seq = [0]
    progress_store = job_store.get_store()

    async def on_progress(e):
        try:
            ps = job_store.get_store()
            ps.append_event(job_id, seq[0], e)
            ps.conn.commit()
            seq[0] += 1
            sig = _job_signals.get(job_id)
            if sig:
                sig.set()
                sig.clear()
        except Exception as ex:
            logger.warning("progress callback failed: %s", ex)

    def record_provider_use(provider: str) -> None:
        try:
            cur = progress_store.conn.execute(
                "SELECT used_providers FROM jobs WHERE job_id=?", (job_id,)
            ).fetchone()
            existing = cur["used_providers"] if cur else "[]"
            try:
                lst = json.loads(existing or "[]")
            except Exception:
                lst = []
            if provider not in lst:
                lst.append(provider)
                progress_store.conn.execute(
                    "UPDATE jobs SET used_providers=?, updated_at=datetime('now') WHERE job_id=?",
                    (json.dumps(lst), job_id),
                )
                progress_store.conn.commit()
        except Exception as ex:
            logger.warning("record_provider_use failed: %s", ex)

    # Aggressive heartbeat: every 10s (faster than service's 30s default)
    # so cleanup_stale_jobs (2-min threshold) never fires.
    async def heartbeat_loop():
        try:
            while True:
                await asyncio.sleep(10)
                try:
                    job_store.get_store().heartbeat(job_id)
                except Exception as ex:
                    logger.warning("heartbeat failed: %s", ex)
        except asyncio.CancelledError:
            pass

    heartbeat_task = asyncio.create_task(heartbeat_loop())

    # RECOVERY-MODE: check_cancelled always returns False. We never want
    # a recovery to be marked abandoned mid-flight. Cancellation would lose
    # all progress and re-running would have to start over.
    def no_cancel(_jid):
        return False

    try:
        print(f"  Calling list_builder.run_domain_enrichment "
              f"with selected_providers={selected_providers}")
        output_rows = await list_builder.run_domain_enrichment(
            rows=rows,
            domain_col=domain_col,
            name_col=job.get("name_col") or None,
            first_name_col=job.get("first_name_col") or None,
            last_name_col=job.get("last_name_col") or None,
            max_decision_makers=job.get("max_results") or 5,
            include_generic_emails=True,
            on_progress=on_progress,
            selected_providers=selected_providers,
            cancelled_jobs=set(),  # empty set — never matches
            check_cancelled=no_cancel,  # always returns False
            job_id=job_id,
            record_provider_use=record_provider_use,
            normalize_domains=normalize_domains,
            collector=collector,
            company_linkedin_col=None,
            linkedin_url_col=job.get("linkedin_url_col") or None,
        )
    finally:
        heartbeat_task.cancel()

    # Write output (mirrors _run_domain_enrich_job)
    if output_rows:
        out_df = pd.DataFrame(output_rows)
        input_cols = [c for c in out_df.columns if c not in list_builder.ENRICHED_COLUMNS]
        ordered = input_cols + [c for c in list_builder.ENRICHED_COLUMNS if c in out_df.columns]
        out_df[ordered].to_csv(str(output_path), index=False)
    else:
        output_path.write_text("")

    # Mark done + trigger background sync
    progress_store.set_done(job_id, str(output_path))
    print(f"  ✓ completed: {len(output_rows)} output rows written to {output_path.name}")

    # Drain collector to Contacts DB (best-effort, async)
    try:
        asyncio.create_task(_run_background_sync(job_id, output_path, collector=collector))
    except Exception:
        pass

    # Final state
    final = load_job_record(job_id)
    output_size = output_path.stat().st_size if output_path.exists() else 0
    print()
    print(f"  ✓ status:         {final['status']}")
    print(f"  ✓ processed:      {final.get('processed', 0)}/{final.get('total', 0)}")
    print(f"  ✓ emails_found:   {final.get('emails_found', 0)}")
    print(f"  ✓ used_providers: {final.get('used_providers', '[]')}")
    print(f"  ✓ output_size:    {output_size} bytes")
    if final.get("error"):
        print(f"  ✗ error:          {final['error']}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    asyncio.run(rerun(sys.argv[1]))
