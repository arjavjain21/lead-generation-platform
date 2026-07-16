"""Queue runner: re-run multiple failed jobs sequentially.

Used to recover jobs affected by the 2026-07-11..13 silent-failure bug.
Runs jobs one at a time to avoid hammering provider rate-limits.

Usage:
    cd backend && source venv/bin/activate
    python recover_queue.py <job_id> [<job_id> ...]

Each job runs to completion (or failure) before the next starts. Progress
is visible in the UI (status flips to running, processed/emails_found update
live, output_path gets overwritten with real data).
"""
from __future__ import annotations

import asyncio
import subprocess
import sys

JOBS = sys.argv[1:]
if not JOBS:
    print(__doc__)
    sys.exit(1)

for job_id in JOBS:
    print(f"\n{'='*70}\n→ Recovering {job_id}\n{'='*70}")
    result = subprocess.run(
        ["python", "recover_failed_job.py", job_id],
        capture_output=False,
    )
    if result.returncode != 0:
        print(f"!! Job {job_id} failed; continuing to next")
    else:
        print(f"✓ Job {job_id} recovered")

print(f"\n{'='*70}\nQueue complete. {len(JOBS)} job(s) processed.\n{'='*70}")
