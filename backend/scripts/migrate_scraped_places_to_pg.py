#!/usr/bin/env python3
"""
migrate_scraped_places_to_pg.py

One-time migration: copies scraped_places from SQLite jobs.db
into PostgreSQL lead_gen database on port 5433.

Run: python scripts/migrate_scraped_places_to_pg.py

Safety:
- Exports full snapshot before touching PG
- Transaction-wrapped TRUNCATE
- Validates row count before declaring success
- Cleans up temp CSV on success
"""

import os
import sys
import csv
import subprocess
import tempfile
import psycopg2
from psycopg2 import sql

# Config
SQLITE_DB = "/var/www/lead-generation-platform/backend/data/jobs.db"
PG_HOST = "localhost"
PG_PORT = 5433
PG_DB = "lead_gen"
PG_USER = "postgres"
CSV_PATH = "/tmp/scraped_places_migrate.csv"

# Schema — exact mirror of SQLite scraped_places
CREATE_TABLE_SQL = """
DROP TABLE IF EXISTS scraped_places CASCADE;

CREATE TABLE scraped_places (
    id                        SERIAL PRIMARY KEY,
    dedupe_key                VARCHAR(512) NOT NULL,
    job_id                    VARCHAR(64) NOT NULL,
    job_created_at            TIMESTAMPTZ,
    query                     TEXT,
    center_name               TEXT,
    center_state              TEXT,
    center_lat                DOUBLE PRECISION,
    center_lng                DOUBLE PRECISION,
    zoom                      INTEGER,
    place_id                  TEXT,
    business_id                TEXT,
    name                      TEXT,
    category_name             TEXT,
    full_address              TEXT,
    city                      TEXT,
    city_state                TEXT,
    latitude                  DOUBLE PRECISION,
    longitude                 DOUBLE PRECISION,
    distance_km               DOUBLE PRECISION,
    rating                    DOUBLE PRECISION,
    review_count              INTEGER,
    website                   TEXT,
    phone                     TEXT,
    types                     TEXT,
    price_level               INTEGER,
    timezone                  TEXT,
    working_hours             TEXT,
    is_claimed                SMALLINT,
    verified                  SMALLINT,
    is_permanently_closed     SMALLINT,
    is_temporarily_closed     SMALLINT,
    place_link                TEXT,
    photo_count               INTEGER,
    first_photo_url           TEXT,
    inserted_at               TIMESTAMPTZ,
    company_linkedin_url      TEXT,
    dm_first_name             TEXT,
    dm_last_name              TEXT,
    dm_full_name              TEXT,
    dm_title                  TEXT,
    dm_linkedin_url           TEXT,
    dm_email                  TEXT,
    dm_email_source           TEXT,
    dm_headline               TEXT,
    dm_location_city          TEXT,
    dm_location_country       TEXT,
    dm_icp_tier               INTEGER,
    row_status                TEXT,
    enriched_at               TIMESTAMPTZ
);
"""

CREATE_INDEXES_SQL = [
    "CREATE UNIQUE INDEX idx_sp_dedupe ON scraped_places(dedupe_key);",
    "CREATE INDEX idx_sp_job_id ON scraped_places(job_id);",
    "CREATE INDEX idx_sp_website ON scraped_places(website);",
    "CREATE INDEX idx_sp_city ON scraped_places(city);",
    "CREATE INDEX idx_sp_inserted ON scraped_places(inserted_at);",
]


def get_sqlite_count():
    """Get row count from SQLite."""
    import sqlite3
    conn = sqlite3.connect(SQLITE_DB)
    count = conn.execute("SELECT COUNT(*) FROM scraped_places").fetchone()[0]
    conn.close()
    return count


def export_sqlite_to_csv(csv_path):
    """Export scraped_places from SQLite to CSV file."""
    import sqlite3

    # Get column names
    conn = sqlite3.connect(SQLITE_DB)
    conn.execute("PRAGMA journal_mode=OFF")  # Faster export
    cursor = conn.execute("PRAGMA table_info(scraped_places)")
    cols = [row[1] for row in cursor]
    conn.close()

    # Export
    conn = sqlite3.connect(SQLITE_DB)
    cursor = conn.execute(f"SELECT {','.join(cols)} FROM scraped_places")
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        writer.writerows(cursor)
    conn.close()

    row_count = sum(1 for _ in open(csv_path)) - 1  # subtract header
    return row_count, cols


def migrate():
    print("=== PostgreSQL Scraper Data Migration ===")
    print(f"SQLite DB: {SQLITE_DB}")
    print(f"PostgreSQL: {PG_HOST}:{PG_PORT}/{PG_DB}")
    print()

    # Step 1: Get expected row count from SQLite
    sqlite_count = get_sqlite_count()
    print(f"[1/6] SQLite row count: {sqlite_count:,}")

    # Step 2: Export SQLite to CSV
    print("[2/6] Exporting SQLite to CSV...")
    if os.path.exists(CSV_PATH):
        os.remove(CSV_PATH)
    exported_count, cols = export_sqlite_to_csv(CSV_PATH)
    csv_size_mb = os.path.getsize(CSV_PATH) / (1024 * 1024)
    print(f"      Exported {exported_count:,} rows ({csv_size_mb:.1f} MB CSV)")

    if exported_count != sqlite_count:
        print(f"ERROR: Export count {exported_count} != SQLite count {sqlite_count}")
        sys.exit(1)

    # Step 3: Connect to PG and create schema
    print("[3/6] Creating schema in PostgreSQL...")
    pg_conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER)
    pg_conn.autocommit = True
    cursor = pg_conn.cursor()

    cursor.execute(CREATE_TABLE_SQL)
    print("      Table created.")

    for idx_sql in CREATE_INDEXES_SQL:
        cursor.execute(idx_sql)
    print("      Indexes created.")

    # Step 4: Truncate and COPY
    print("[4/6] Loading data into PostgreSQL via \\COPY...")
    cursor.execute("TRUNCATE scraped_places;")

    with open(CSV_PATH, "r") as f:
        cursor.copy_expert(
            sql.SQL("COPY scraped_places ({}) FROM STDIN WITH (FORMAT csv, HEADER true)").format(
                sql.SQL(", ").join(sql.Identifier(c) for c in cols)
            ),
            f
        )
    pg_conn.commit()

    # Step 5: Validate
    print("[5/6] Validating row count...")
    cursor.execute("SELECT COUNT(*) FROM scraped_places")
    pg_count = cursor.fetchone()[0]

    if pg_count != sqlite_count:
        print(f"ERROR: PG count {pg_count:,} != SQLite count {sqlite_count:,}")
        pg_conn.close()
        sys.exit(1)

    print(f"      Validation passed: {pg_count:,} rows in PG == {sqlite_count:,} rows in SQLite")

    # Step 6: Cleanup
    print("[6/6] Cleaning up temp CSV...")
    os.remove(CSV_PATH)
    pg_conn.close()

    print()
    print(f"=== Migration complete: {pg_count:,} rows migrated successfully ===")


if __name__ == "__main__":
    migrate()
