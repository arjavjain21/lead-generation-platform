#!/usr/bin/env python3
"""
validate_migration.py

Run after migrate_scraped_places_to_pg.py to verify:
1. Row count matches SQLite
2. Sample rows exist
3. Indexes are present
4. Key columns have data

Run: python scripts/validate_migration.py
"""

import sqlite3
import psycopg2

SQLITE_DB = "/var/www/lead-generation-platform/backend/data/jobs.db"
PG_HOST = "127.0.0.1"  # TCP/IP for scram-sha-256 password auth
PG_PORT = 5433
PG_DB = "lead_gen"
PG_USER = "postgres"
PG_PASSWORD = "leadgen_migrate_2024"


def validate():
    print("=== Migration Validation ===\n")

    errors = []

    # 1. Row counts
    sqlite_conn = sqlite3.connect(SQLITE_DB)
    sqlite_count = sqlite_conn.execute("SELECT COUNT(*) FROM scraped_places").fetchone()[0]
    sqlite_conn.close()

    pg_conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD)
    cur = pg_conn.cursor()
    cur.execute("SELECT COUNT(*) FROM scraped_places")
    pg_count = cur.fetchone()[0]
    cur.close()
    pg_conn.close()

    if pg_count == sqlite_count:
        print(f"[PASS] Row count: PG={pg_count:,} == SQLite={sqlite_count:,}")
    else:
        print(f"[FAIL] Row count: PG={pg_count:,} != SQLite={sqlite_count:,}")
        errors.append("Row count mismatch")

    # 2. Sample row
    pg_conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD)
    cur = pg_conn.cursor()
    cur.execute("SELECT name, city, website, dedupe_key FROM scraped_places LIMIT 1")
    row = cur.fetchone()
    cur.close()
    pg_conn.close()
    if row:
        print(f"[PASS] Sample row: name={row[0]!r}, city={row[1]!r}, website={row[2]!r}")
    else:
        print("[FAIL] No sample rows found")
        errors.append("No sample rows")

    # 3. Indexes
    pg_conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD)
    cur = pg_conn.cursor()
    cur.execute("""
        SELECT indexname FROM pg_indexes
        WHERE tablename = 'scraped_places'
        ORDER BY indexname
    """)
    indexes = cur.fetchall()
    cur.close()
    pg_conn.close()

    expected_indexes = [
        "idx_sp_city", "idx_sp_dedupe", "idx_sp_inserted",
        "idx_sp_job_id", "idx_sp_website"
    ]
    found = {idx[0] for idx in indexes}

    for idx in expected_indexes:
        if idx in found:
            print(f"[PASS] Index exists: {idx}")
        else:
            print(f"[FAIL] Missing index: {idx}")
            errors.append(f"Missing index: {idx}")

    # 4. Key columns have data
    pg_conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD)
    cur = pg_conn.cursor()
    cur.execute(
        "SELECT COUNT(website) FROM scraped_places WHERE website IS NOT NULL AND website != ''"
    )
    website_count = cur.fetchone()[0]
    cur.close()
    pg_conn.close()

    print(f"[INFO] Rows with website: {website_count:,}")

    if errors:
        print(f"\n=== VALIDATION FAILED: {len(errors)} errors ===")
        for e in errors:
            print(f"  - {e}")
        return False
    else:
        print(f"\n=== ALL CHECKS PASSED ===")
        return True


if __name__ == "__main__":
    import sys
    success = validate()
    sys.exit(0 if success else 1)