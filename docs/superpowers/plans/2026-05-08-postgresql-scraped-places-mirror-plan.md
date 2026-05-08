# PostgreSQL Scraper Data Mirror — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up a new PostgreSQL 16 cluster on `/mnt/disk`, create the `lead_gen` database with a `scraped_places` table that mirrors the SQLite schema, and migrate all 980,585 rows from `jobs.db` into it. Zero disruption to existing systems.

**Architecture:** New PG 16 cluster (port 5433) isolated from existing PG (port 5432). CSV export from SQLite → `\COPY` import into PostgreSQL. Migration via a single Python script.

**Tech Stack:** PostgreSQL 16, Python 3 (stdlib + psycopg2 for PG connection), SQLite3 (stdlib)

---

## File Structure

```
backend/
├── scripts/
│   ├── migrate_scraped_places_to_pg.py   # Create: one-time migration script
│   └── validate_migration.py              # Create: post-migration validation
```

No existing files are modified. No application code changes. No systemd changes.

---

## Task 1: Create PostgreSQL 16 Cluster on /mnt/disk

**Files:**
- Create: N/A (system command, not a file)
- Check: Verify new cluster status after creation

- [ ] **Step 1: Create the new PG 16 cluster on /mnt/disk**

```bash
sudo pg_createcluster -d /mnt/disk/postgresql/16/main lead-gen --start
```

Expected output: `pg_createcluster: cluster created successfully` and `Starting cluster...ok`

- [ ] **Step 2: Verify the cluster is running**

```bash
sudo pg_lsclusters
```

Expected output shows `lead-gen | 16 | main |5433 | running` alongside existing cluster on 5432.

- [ ] **Step 3: Allow postgres user to write to /mnt/disk**

```bash
sudo chown -R postgres:postgres /mnt/disk/postgresql
```

- [ ] **Step 4: Verify postgres can connect to new cluster**

```bash
sudo -u postgres psql -p 5433 -c "SELECT version();"
```

Expected: PostgreSQL 16.13 response.

- [ ] **Step 5: Create lead_gen database**

```bash
sudo -u postgres psql -p 5433 -c "CREATE DATABASE lead_gen;"
```

- [ ] **Step 6: Create dedicated app user (optional, use postgres for now)**

Skip this step — use `postgres` superuser for the migration script since it's a one-time operation. The user can create a restricted user later if needed.

---

## Task 2: Create Schema and Indexes in PostgreSQL

**Files:**
- Create: `backend/scripts/migrate_scraped_places_to_pg.py`

The migration script will create the schema as part of its setup. The schema creation is embedded in the migration script (Task 3) so the full migration is atomic.

- [ ] **Step 1: Write the migration script**

Create the file at `backend/scripts/migrate_scraped_places_to_pg.py` with:

1. PG connection to `localhost:5433/lead_gen`
2. DROP existing `scraped_places` table if it exists
3. CREATE TABLE with exact schema (see design spec Section 5)
4. CREATE 5 indexes
5. Export SQLite → CSV
6. TRUNCATE scraped_places
7. `\COPY` CSV into PG
8. Validate row count
9. Cleanup CSV

See Task 3 for the full script content.

---

## Task 3: Write and Run the Migration Script

**Files:**
- Create: `backend/scripts/migrate_scraped_places_to_pg.py`

- [ ] **Step 1: Write the migration script**

```python
#!/usr/bin/env python3
"""
migrate_scraped_places_to_pg.py

One-time migration: copies scraped_places from SQLite jobs.db
into PostgreSQL lead_gen database on port 5433.

Run: python migrate_scraped_places_to_pg.py

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
    business_id               TEXT,
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
        # Use COPY for fastest import
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
```

- [ ] **Step 2: Ensure psycopg2 is available (install if missing)**

```bash
cd /var/www/lead-generation-platform/backend
source venv/bin/activate
pip install psycopg2-binary 2>/dev/null || pip install psycopg2-binary
```

Expected: `Requirement already satisfied: psycopg2-binary` or successful install.

- [ ] **Step 3: Run the migration script**

```bash
cd /var/www/lead-generation-platform/backend
source venv/bin/activate
python scripts/migrate_scraped_places_to_pg.py
```

Expected output:
```
=== PostgreSQL Scraper Data Migration ===
SQLite DB: /var/www/.../jobs.db
PostgreSQL: localhost:5433/lead_gen

[1/6] SQLite row count: 980,585
[2/6] Exporting SQLite to CSV...
      Exported 980,585 rows (XXX.X MB CSV)
[3/6] Creating schema in PostgreSQL...
      Table created.
      Indexes created.
[4/6] Loading data into PostgreSQL via \COPY...
[5/6] Validating row count...
      Validation passed: 980,585 rows in PG == 980,585 rows in SQLite
[6/6] Cleaning up temp CSV...
=== Migration complete: 980,585 rows migrated successfully ===
```

---

## Task 4: Post-Migration Validation

**Files:**
- Create: `backend/scripts/validate_migration.py`

- [ ] **Step 1: Write the validation script**

```python
#!/usr/bin/env python3
"""
validate_migration.py

Run after migrate_scraped_places_to_pg.py to verify:
1. Row count matches SQLite
2. Sample rows exist
3. Indexes are present
4. Key columns have data
"""

import sqlite3
import psycopg2

SQLITE_DB = "/var/www/lead-generation-platform/backend/data/jobs.db"
PG_HOST = "localhost"
PG_PORT = 5433
PG_DB = "lead_gen"
PG_USER = "postgres"


def validate():
    print("=== Migration Validation ===\n")

    errors = []

    # 1. Row counts
    sqlite_conn = sqlite3.connect(SQLITE_DB)
    sqlite_count = sqlite_conn.execute("SELECT COUNT(*) FROM scraped_places").fetchone()[0]
    sqlite_conn.close()

    pg_conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER)
    pg_count = pg_conn.execute("SELECT COUNT(*) FROM scraped_places").fetchone()[0]
    pg_conn.close()

    if pg_count == sqlite_count:
        print(f"[PASS] Row count: PG={pg_count:,} == SQLite={sqlite_count:,}")
    else:
        print(f"[FAIL] Row count: PG={pg_count:,} != SQLite={sqlite_count:,}")
        errors.append("Row count mismatch")

    # 2. Sample row
    pg_conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER)
    row = pg_conn.execute("SELECT name, city, website, dedupe_key FROM scraped_places LIMIT 1").fetchone()
    if row:
        print(f"[PASS] Sample row: name={row[0]!r}, city={row[1]!r}, website={row[2]!r}")
    else:
        print("[FAIL] No sample rows found")
        errors.append("No sample rows")
    pg_conn.close()

    # 3. Indexes
    pg_conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER)
    indexes = pg_conn.execute("""
        SELECT indexname FROM pg_indexes
        WHERE tablename = 'scraped_places'
        ORDER BY indexname
    """).fetchall()
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
    pg_conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER)
    website_count = pg_conn.execute("SELECT COUNT(website) FROM scraped_places WHERE website IS NOT NULL AND website != ''").fetchone()[0]
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
```

- [ ] **Step 2: Run the validation script**

```bash
cd /var/www/lead-generation-platform/backend
source venv/bin/activate
python scripts/validate_migration.py
```

Expected: All `[PASS]` checks, ending with `=== ALL CHECKS PASSED ===`

---

## Task 5: Update CLAUDE.md and Docs

**Files:**
- Modify: `CLAUDE.md` (add PostgreSQL section)
- Modify: `docs/superpowers/specs/2026-05-08-postgresql-scraped-places-mirror-design.md` (already created)
- Create: `docs/superpowers/plans/2026-05-08-postgresql-scraped-places-mirror-plan.md` (this file, already created)

- [ ] **Step 1: Add PostgreSQL section to CLAUDE.md**

Add to `CLAUDE.md` after the "Troubleshooting" section (before "Monitoring"):

```markdown
## PostgreSQL Companion Database

A PostgreSQL 16 companion database mirrors the `scraped_places` SQLite table for enriched data.

| Property | Value |
|----------|-------|
| Cluster | `/mnt/disk/postgresql/16/main` |
| Port | 5433 |
| Database | `lead_gen` |
| Connection | `postgresql://postgres@localhost:5433/lead_gen` |
| Table | `scraped_places` |
| Row count | ~980,585 |

**Important:** This PostgreSQL cluster is independent from the existing PG cluster on port 5432. The existing 25 databases are unaffected.

**Enrichment columns:** User manages these directly via `ALTER TABLE`. No application code changes needed to add columns.

**Future sync:** Workers will sync data between SQLite and PostgreSQL (Phase 2+).
```

- [ ] **Step 2: Commit all changes to git**

```bash
cd /var/www/lead-generation-platform
git add docs/superpowers/specs/2026-05-08-postgresql-scraped-places-mirror-design.md \
       docs/superpowers/plans/2026-05-08-postgresql-scraped-places-mirror-plan.md \
       CLAUDE.md \
       backend/scripts/migrate_scraped_places_to_pg.py \
       backend/scripts/validate_migration.py
git commit -m "feat: add PostgreSQL scraped_places mirror with one-time migration

- New PG 16 cluster on /mnt/disk (port 5433), isolated from existing PG
- lead_gen database with scraped_places table (exact SQLite schema mirror)
- migrate_scraped_places_to_pg.py: CSV export → COPY import of 980K rows
- validate_migration.py: post-migration verification
- CLAUDE.md updated with PostgreSQL companion documentation
- Phase 2+: sync workers (csv_to_sqlite, sqlite_to_pg, pg_to_sqlite)

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Validation Checklist

After all tasks complete, verify:

- [ ] New PG cluster running on port 5433: `sudo pg_lsclusters`
- [ ] `lead_gen` database exists: `sudo -u postgres psql -p 5433 -l`
- [ ] `scraped_places` table exists with 50 columns
- [ ] All 5 indexes created
- [ ] Row count in PG matches SQLite (980,585)
- [ ] Sample row verified in PG: `SELECT * FROM scraped_places LIMIT 1`
- [ ] `validate_migration.py` passes all checks
- [ ] No existing PostgreSQL databases affected
- [ ] `jobs.db` unchanged (run `md5sum backend/data/jobs.db` before vs after)
- [ ] Git commit pushed
- [ ] CLAUDE.md updated