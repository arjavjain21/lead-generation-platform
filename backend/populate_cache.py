#!/usr/bin/env python3
"""
Populate the cache with partial results from stopped jobs.

This script:
1. Loads stopped jobs from the database
2. Generates cache IDs from query + regions
3. Populates the scraped_cache table
4. Calculates checksums for result files
"""

import hashlib
import json
import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Configuration
DB_PATH = Path("/var/www/lead-generation-platform/backend/data/jobs.db")
OUTPUT_DIR = Path("/var/www/lead-generation-platform/backend/data/outputs")

# Jobs to cache
JOBS_TO_CACHE = {
    'e33b3df7-2841-4b00-a3c5-c3f25bba7737': 'dental clinic',
    'dd8573c5-5848-48dd-91dc-1b90dbec983b': 'dentist',
    '2caa63b0-b97b-4595-9196-1811e04a3765': 'elementary school'
}


def generate_region_signature(regions_json: str) -> str:
    """Generate deterministic hash for region configuration."""
    regions = json.loads(regions_json)
    normalized = json.dumps(regions, sort_keys=True)
    return hashlib.sha256(normalized.encode()).hexdigest()[:16]


def generate_cache_id(query: str, region_signature: str) -> str:
    """Generate unique cache ID."""
    combined = f"{query.lower().strip()}:{region_signature}"
    return hashlib.sha256(combined.encode()).hexdigest()[:16]


def calculate_file_checksum(file_path: Path) -> str:
    """Calculate SHA256 checksum of a file."""
    sha256 = hashlib.sha256()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            sha256.update(chunk)
    return sha256.hexdigest()


def count_csv_results(file_path: Path) -> int:
    """Count rows in CSV file (excluding header)."""
    count = 0
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for _ in reader:
            count += 1
    return count


import csv  # Import csv module


def populate_cache():
    """Populate the cache with stopped job results."""

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    print("=== POPULATING CACHE WITH STOPPED JOBS ===\n")

    for job_id, expected_query in JOBS_TO_CACHE.items():
        print(f"Processing job: {job_id[:8]}")

        # Get job data from database
        cursor.execute("""
            SELECT job_id, user_id, query, regions, total_tasks, done_tasks,
                   result_count, status, created_at
            FROM jobs WHERE job_id = ?
        """, (job_id,))

        row = cursor.fetchone()
        if not row:
            print(f"  ⚠️  Job not found\n")
            continue

        (job_id, user_id, query, regions_json, total_tasks, done_tasks,
         result_count, status, created_at) = row

        # Verify query matches expected
        if query != expected_query:
            print(f"  ⚠️  Query mismatch: expected '{expected_query}', got '{query}'")
            continue

        # Generate cache keys
        region_sig = generate_region_signature(regions_json)
        cache_id = generate_cache_id(query, region_sig)

        # Verify CSV file exists
        csv_path = OUTPUT_DIR / f"{job_id}.csv"
        if not csv_path.exists():
            print(f"  ⚠️  CSV file not found: {csv_path}")
            continue

        # Count actual results in CSV
        csv_count = count_csv_results(csv_path)

        # Calculate checksum
        checksum = calculate_file_checksum(csv_path)

        # Calculate expiry (60 days from creation)
        created_dt = datetime.fromisoformat(created_at.replace('+00:00', ''))
        expires_at = (created_dt + timedelta(days=60)).isoformat()

        # Calculate percentage complete
        percentage = (done_tasks / total_tasks * 100) if total_tasks > 0 else 0

        # Insert into cache
        now = datetime.now(timezone.utc).isoformat()

        try:
            cursor.execute("""
                INSERT INTO scraped_cache (
                    cache_id, query, region_signature, regions,
                    total_results, result_file_path,
                    created_at, updated_at, expires_at, last_accessed_at,
                    is_partial, percentage_complete, checksum, status,
                    job_id, user_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                cache_id, query, region_sig, regions_json,
                csv_count, str(csv_path),
                created_at, now, expires_at, now,
                1, percentage, checksum, 'active',
                job_id, user_id
            ))

            print(f"  ✓ Cache entry created: {cache_id}")
            print(f"    Query: {query}")
            print(f"    Results: {csv_count:,}")
            print(f"    Partial: {percentage:.1f}% complete")
            print(f"    Expires: {expires_at[:10]}")
            print()

        except sqlite3.IntegrityError as e:
            print(f"  ⚠️  Cache entry already exists: {cache_id}")
            print(f"    Error: {e}\n")

    # Commit changes
    conn.commit()
    conn.close()

    print("=== CACHE POPULATION COMPLETE ===")
    print(f"\nCache entries created: {len(JOBS_TO_CACHE)}")
    print("Partial results are now available for instant retrieval.")


if __name__ == "__main__":
    populate_cache()
