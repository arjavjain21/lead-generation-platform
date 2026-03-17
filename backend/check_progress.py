#!/usr/bin/env python3
"""
Progress monitoring script for enrichment jobs.
Shows actual progress by counting rows in output file (more accurate than database).
"""

import sqlite3
import sys
from pathlib import Path

# Configuration
JOB_ID = "d15c3247-a754-495e-8e02-1b6f6a7bd374"
DB_PATH = Path(__file__).parent / "data" / "jobs.db"
OUTPUT_FILE = Path(__file__).parent / "data" / "outputs" / f"{JOB_ID}.csv"


def get_job_from_db():
    """Get job info from database."""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute(
        "SELECT status, total, processed, emails_found, created_at, updated_at FROM jobs WHERE job_id=?",
        (JOB_ID,)
    )
    job = cursor.fetchone()
    conn.close()
    return dict(job) if job else None


def count_output_rows():
    """Count rows in output CSV file (excluding header)."""
    if not OUTPUT_FILE.exists():
        return 0

    try:
        with open(OUTPUT_FILE, 'r') as f:
            # Subtract 1 for header row
            return sum(1 for _ in f) - 1
    except Exception as e:
        print(f"Error reading output file: {e}")
        return 0


def format_size(bytes_size):
    """Format bytes to human readable size."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.1f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.1f} TB"


def main():
    print("=" * 70)
    print(f"ENRICHMENT JOB PROGRESS MONITOR")
    print(f"Job ID: {JOB_ID}")
    print("=" * 70)
    print()

    # Get database info
    job = get_job_from_db()
    if not job:
        print("❌ Job not found in database")
        sys.exit(1)

    total_rows = job['total'] or 0

    # Count actual progress from output file
    actual_processed = count_output_rows()
    file_size = OUTPUT_FILE.stat().st_size if OUTPUT_FILE.exists() else 0

    # Display progress
    print(f"Database Status:      {job['status']}")
    print(f"Database Shows:       {job['processed']} / {total_rows} rows")
    print(f"Actual Progress:      {actual_processed} / {total_rows} rows")
    print()

    if actual_processed > 0:
        percentage = (actual_processed / total_rows) * 100
        print(f"Completion:           {percentage:.1f}%")
        print(f"Output File Size:     {format_size(file_size)}")

        # Estimate time remaining
        if percentage > 5:  # Only estimate if we have meaningful progress
            from datetime import datetime
            created = datetime.fromisoformat(job['created_at'].replace('Z', '+00:00'))
            elapsed = (datetime.now(created.tzinfo) - created).total_seconds() / 60  # minutes

            if elapsed > 0:
                rate = actual_processed / elapsed  # rows per minute
                remaining_rows = total_rows - actual_processed
                eta_minutes = remaining_rows / rate if rate > 0 else 0

                print(f"Processing Rate:      {rate:.1f} rows/minute")
                print(f"Estimated Remaining:  {eta_minutes:.0f} minutes ({eta_minutes/60:.1f} hours)")

    print()
    print("=" * 70)
    print("NOTE: Actual progress is counted from output file.")
    print("      Database progress may show 0 due to known bug.")
    print("=" * 70)


if __name__ == "__main__":
    main()
