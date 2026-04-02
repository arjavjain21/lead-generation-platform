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
) -> dict[str, Any]:
    """
    Run phone enrichment on a CSV file.

    Args:
        job_id: The job ID
        input_path: Path to the input CSV file
        linkedin_col: Column name containing LinkedIn URLs
        on_progress: Optional callback for progress updates

    Returns:
        dict with total, processed, phones_found, output_path
    """
    # Read input CSV
    with open(input_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        original_columns = reader.fieldnames or []
        rows = list(reader)

    total = len(rows)
    processed = 0
    phones_found = 0

    # Create output file path
    output_path = job_store.OUTPUT_DIR / f"{job_id}.csv"

    # Create HTTP client
    client = httpx.AsyncClient(timeout=30.0)

    # Semaphore for concurrency control
    semaphore = asyncio.Semaphore(CONCURRENCY)

    # Process results collector
    enriched_rows = []

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
        nonlocal processed, phones_found

        linkedin_url = str(row.get(linkedin_col, "")).strip()

        # Default values
        phone_number = ""
        phone_found = False

        if not linkedin_url or "linkedin.com" not in linkedin_url.lower():
            # Invalid or missing LinkedIn URL
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
                    logger.warning(f"Error enriching {linkedin_url}: {e}")
                    phone_found = False
                    phone_number = ""

        if phone_found:
            phones_found += 1

        processed += 1

        # Update row with phone data
        return {**row, "phone_number": phone_number, "phone_found": str(phone_found).lower()}

    # Process rows in batches
    try:
        for batch_start in range(0, total, BATCH_SIZE):
            batch_end = min(batch_start + BATCH_SIZE, total)
            batch_rows = rows[batch_start:batch_end]
            batch_tasks = [process_row(batch_start + i, row) for i, row in enumerate(batch_rows)]

            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)

            # Handle exceptions and collect results
            for i, result in enumerate(batch_results):
                if isinstance(result, Exception):
                    logger.error(f"Row {batch_start + i} failed: {result}")
                    # Keep original row with empty phone data
                    enriched_rows.append({**rows[batch_start + i], "phone_number": "", "phone_found": "false"})
                else:
                    enriched_rows.append(result)

            # Update progress after each batch
            await update_progress(processed, phones_found)
            logger.info(f"Phone enrichment progress: {processed}/{total} rows, {phones_found} phones found")

        # Write output CSV
        all_columns = (original_columns or []) + PHONE_OUTPUT_COLS
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=all_columns, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(enriched_rows)

        # Update job in database
        job_store.update_job_progress(job_id, total, phones_found)
        job_store.set_job_output(job_id, str(output_path))
        job_store.update_job_status(job_id, "done")

        # Send final progress
        if on_progress:
            await on_progress({
                "processed": total,
                "total": total,
                "phones_found": phones_found,
                "status": "done",
            })

        logger.info(f"Phone enrichment complete: {total} rows, {phones_found} phones found")

        return {
            "total": total,
            "processed": total,
            "phones_found": phones_found,
            "output_path": str(output_path),
        }

    except Exception as e:
        logger.error(f"Phone enrichment job {job_id} failed: {e}")
        job_store.set_job_error(job_id, str(e))
        raise

    finally:
        await client.aclose()
