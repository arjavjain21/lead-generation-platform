#!/usr/bin/env python3
"""
Signal running scraper jobs to stop by adding them to the cancelled_jobs set.

This script imports the scraper routes module and adds job IDs to the
_cancelled_jobs set, which is checked by the running jobs.
"""

import sys
sys.path.insert(0, '/var/www/lead-generation-platform/backend')

# Import after adding to path
from scraper import routes

# Jobs to signal to stop
JOBS_TO_STOP = [
    'e33b3df7-2841-4b00-a3c5-c3f25bba7737',
    'dd8573c5-5848-48dd-91dc-1b90dbec983b',
    '2caa63b0-b97b-4595-9196-1811e04a3765'
]

def signal_jobs_to_stop():
    """Add job IDs to the cancelled_jobs set."""
    print("=== SIGNALING JOBS TO STOP ===\n")

    for job_id in JOBS_TO_STOP:
        print(f"Adding {job_id[:8]} to cancelled_jobs set...")
        routes._cancelled_jobs.add(job_id)
        routes._active_jobs.discard(job_id)
        print(f"  ✓ Job {job_id[:8]} signalled to stop")

    print("\n=== JOBS SIGNALED ===")
    print("\nJobs will complete their current task and then stop.")
    print("CSV files are preserved with partial results.")


if __name__ == "__main__":
    signal_jobs_to_stop()
