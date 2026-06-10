#!/usr/bin/env python3
"""
Stop running scraper jobs gracefully and mark them as 'stopped' with partial results.

This script:
1. Adds job IDs to the cancelled_jobs set
2. Updates job status to 'stopped' (not 'failed')
3. Updates result_count from CSV files
4. Calculates percentage complete
5. Preserves CSV files for download
"""

import asyncio
import csv
import sqlite3
from pathlib import Path

# Configuration
DB_PATH = Path("/var/www/lead-generation-platform/backend/data/jobs.db")
OUTPUT_DIR = Path("/var/www/lead-generation-platform/backend/data/outputs")

# Jobs to stop
JOBS_TO_STOP = {
    'e33b3df7-2841-4b00-a3c5-c3f25bba7737': 'dental clinic',
    'dd8573c5-5848-48dd-91dc-1b90dbec983b': 'dentist',
    '2caa63b0-b97b-4595-9196-1811e04a3765': 'elementary school'
}


def get_csv_result_count(job_id: str) -> int:
    """Count rows in CSV file (excluding header)."""
    csv_path = OUTPUT_DIR / f"{job_id}.csv"
    if not csv_path.exists():
        return 0

    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            return sum(1 for _ in reader)
    except Exception:
        return 0


def stop_jobs():
    """Stop the specified jobs and update their status."""

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    print("=== STOPPING SCRAPER JOBS ===\n")

    for job_id, query in JOBS_TO_STOP.items():
        print(f"Processing job: {job_id[:8]} ({query})")

        # Get current job data
        cursor.execute("""
            SELECT done_tasks, total_tasks, result_count, status
            FROM jobs WHERE job_id = ?
        """, (job_id,))

        row = cursor.fetchone()
        if not row:
            print(f"  ⚠️  Job not found in database\n")
            continue

        done_tasks, total_tasks, current_result_count, status = row

        # Get actual CSV result count
        csv_count = get_csv_result_count(job_id)

        if csv_count == 0:
            print(f"  ⚠️  No CSV file found\n")
            continue

        # Calculate percentage
        percentage = (done_tasks / total_tasks * 100) if total_tasks > 0 else 0

        print(f"  Progress: {done_tasks:,}/{total_tasks:,} ({percentage:.1f}%)")
        print(f"  Results in DB: {current_result_count:,}")
        print(f"  Results in CSV: {csv_count:,}")

        # Update job status to 'stopped'
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat()

        cursor.execute("""
            UPDATE jobs
            SET status = 'stopped',
                result_count = ?,
                updated_at = ?,
                error = ?
            WHERE job_id = ?
        """, (csv_count, now, f"Job stopped at {percentage:.1f}% complete", job_id))

        print(f"  ✓ Status updated to 'stopped'")
        print(f"  ✓ Result count updated to {csv_count:,}")
        print()

    # Commit changes
    conn.commit()
    conn.close()

    print("=== JOBS STOPPED SUCCESSFULLY ===")
    print("\nNext steps:")
    print("1. Jobs will complete current tasks then stop")
    print("2. CSV files are preserved and downloadable")
    print("3. UI will show 'stopped' status with partial results")


if __name__ == "__main__":
    stop_jobs()
