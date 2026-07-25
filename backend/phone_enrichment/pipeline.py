"""Phone enrichment pipeline - process LinkedIn URLs to get phone numbers."""

import asyncio
import csv
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import httpx

from .client import find_phone
from . import job_store

logger = logging.getLogger(__name__)

# Concurrency: 5 RPS max due to Blitz API limit
CONCURRENCY = 5

# Batch size for processing (smaller batches for better progress tracking)
BATCH_SIZE = 50

# Output columns to add
PHONE_OUTPUT_COLS = ["phone_number", "phone_found"]


async def run_phone_enrichment(
    job_id: str,
    input_path: Path,
    linkedin_col: str,
    on_progress: Optional[Any] = None,
    cancelled_jobs: Optional[set] = None,
) -> dict[str, Any]:
    """
    Run phone enrichment on a CSV file.

    Args:
        job_id: The job ID
        input_path: Path to the input CSV file
        linkedin_col: Column name containing LinkedIn URLs
        on_progress: Optional callback for progress updates
        cancelled_jobs: Optional shared set of cancelled job_ids. When the
            caller (cancel endpoint) adds this job_id, the loop stops promptly
            between rows and marks the job 'cancelled'.

    Returns:
        dict with total, processed, phones_found, output_path, status

    Behaviour notes:
      * The output CSV is written INCREMENTALLY — header at start, each completed
        row appended + flushed in order — so a crash/OOM/worker-recycle retains
        all rows processed so far (Fix 5).
      * A 100% provider failure (every API call errored, 0 phones found) is
        surfaced as FAILED instead of a false 'done' (Fix 3).
      * The cancel flag is checked before each row's API call and between batches
        so cancellation stops promptly (Fix 4).
    """
    # Read input CSV
    with open(input_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        original_columns = reader.fieldnames or []
        rows = list(reader)

    total = len(rows)
    processed = 0
    phones_found = 0
    failure_count = 0  # rows where the provider (Blitz) call raised an exception
    cancelled_flag = False

    # Create output file path
    output_path = job_store.OUTPUT_DIR / f"{job_id}.csv"
    all_columns = (original_columns or []) + PHONE_OUTPUT_COLS

    # Create HTTP client
    client = httpx.AsyncClient(timeout=30.0)

    # Semaphore for concurrency control
    semaphore = asyncio.Semaphore(CONCURRENCY)

    # Cancellation check (in-memory set for fast path + DB for cross-worker safety)
    def is_cancelled() -> bool:
        if cancelled_jobs is not None and job_id in cancelled_jobs:
            return True
        try:
            return job_store.is_job_cancelled(job_id)
        except Exception:
            return False

    # Progress callback
    async def update_progress(current_processed: int, current_phones_found: int):
        nonlocal processed, phones_found
        # Update DB with current progress
        job_store.update_job_progress(job_id, current_processed, current_phones_found)
        # Send SSE event
        if on_progress:
            await on_progress({
                "processed": current_processed,
                "total": total,
                "phones_found": current_phones_found,
            })

    # Process a single row
    async def process_row(idx: int, row: dict[str, Any]) -> dict[str, Any]:
        nonlocal processed, phones_found, failure_count

        # Stop promptly on cancel: skip the API call entirely for this row
        if is_cancelled():
            return {**row, "phone_number": "", "phone_found": "false", "_skipped": True}

        linkedin_url = str(row.get(linkedin_col, "")).strip()

        # Default values
        phone_number = ""
        phone_found = False

        if not linkedin_url or "linkedin.com" not in linkedin_url.lower():
            # Invalid or missing LinkedIn URL — not a provider failure
            phone_found = False
        else:
            # Check cache first
            cached = job_store.get_cached_phone(linkedin_url)
            if cached:
                phone_number = cached.get("phone_number") or ""
                phone_found = cached.get("phone_found", False)
                logger.debug(f"Cache hit for {linkedin_url}")
            else:
                # Call Blitz API
                try:
                    async with semaphore:
                        result = await find_phone(client, linkedin_url)

                    phone_found = result.get("found", False)
                    phone_number = result.get("phone") or ""

                    # Cache the result
                    job_store.cache_phone_enrichment(linkedin_url, phone_number, phone_found)
                    logger.debug(f"Found phone for {linkedin_url}: {phone_number if phone_found else 'not found'}")

                except Exception as e:
                    # Per-row provider failure — track so a 100% failure isn't
                    # reported as a green 'done' with 0 phones (Fix 3).
                    logger.warning(f"Error enriching {linkedin_url}: {e}")
                    failure_count += 1
                    phone_found = False
                    phone_number = ""

        if phone_found:
            phones_found += 1

        processed += 1

        # Update row with phone data
        return {**row, "phone_number": phone_number, "phone_found": str(phone_found).lower()}

    # Incremental output: write header now, append each completed row + flush as
    # it finishes (in batch order) so a crash retains all rows processed so far.
    out_file = open(output_path, "w", newline="", encoding="utf-8")
    writer = csv.DictWriter(out_file, fieldnames=all_columns, extrasaction="ignore")
    writer.writeheader()
    out_file.flush()

    # Process rows in batches
    try:
        for batch_start in range(0, total, BATCH_SIZE):
            # Check cancel between batches
            if is_cancelled():
                cancelled_flag = True
                break

            batch_end = min(batch_start + BATCH_SIZE, total)
            batch_rows = rows[batch_start:batch_end]
            batch_tasks = [process_row(batch_start + i, row) for i, row in enumerate(batch_rows)]

            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)

            # Write results in order, incrementally + flush (Fix 5)
            for i, result in enumerate(batch_results):
                base_row = rows[batch_start + i]
                if isinstance(result, Exception):
                    logger.error(f"Row {batch_start + i} failed: {result}")
                    failure_count += 1
                    writer.writerow({**base_row, "phone_number": "", "phone_found": "false"})
                elif isinstance(result, dict) and result.get("_skipped"):
                    # Row skipped due to cancellation — don't write/count it
                    cancelled_flag = True
                    continue
                else:
                    write_row = {
                        k: v for k, v in result.items() if not k.startswith("_")
                    } if isinstance(result, dict) else {**base_row, "phone_number": "", "phone_found": "false"}
                    writer.writerow(write_row)
                out_file.flush()

            # Update progress after each batch
            await update_progress(processed, phones_found)
            logger.info(f"Phone enrichment progress: {processed}/{total} rows, {phones_found} phones found")

            # Check cancel after batch
            if is_cancelled():
                cancelled_flag = True
                break

        # Determine final status
        if cancelled_flag:
            status = "cancelled"
            job_store.update_job_status(job_id, "cancelled")
            # Persist output path so the partial file is locatable on disk
            job_store.set_job_output(job_id, str(output_path))
            logger.info(f"Phone enrichment cancelled at {processed}/{total} rows")
        elif total > 0 and failure_count > 0 and phones_found == 0:
            # 100% provider failure masquerading as success — surface as FAILED
            # (Fix 3). The output file still contains all processed rows.
            status = "failed"
            err = (
                f"All phone lookups failed: 0 phones found across {total} rows "
                f"({failure_count} provider error(s)). This usually indicates a "
                f"provider outage or authentication issue — please retry."
            )
            logger.error(f"Phone enrichment job {job_id}: {err}")
            job_store.set_job_error(job_id, err)
            job_store.set_job_output(job_id, str(output_path))
        else:
            status = "done"
            # Update job in database
            job_store.update_job_progress(job_id, total, phones_found)
            job_store.set_job_output(job_id, str(output_path))
            job_store.update_job_status(job_id, "done")
            logger.info(f"Phone enrichment complete: {total} rows, {phones_found} phones found")

        # Send final progress
        if on_progress:
            await on_progress({
                "processed": processed,
                "total": total,
                "phones_found": phones_found,
                "status": status,
            })

        return {
            "total": total,
            "processed": processed,
            "phones_found": phones_found,
            "output_path": str(output_path),
            "status": status,
        }

    except Exception as e:
        logger.error(f"Phone enrichment job {job_id} failed: {e}")
        job_store.set_job_error(job_id, str(e))
        raise

    finally:
        out_file.close()
        await client.aclose()
