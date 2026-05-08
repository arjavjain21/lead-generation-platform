# PostgreSQL Scraper Data Mirror — Design

> **Status:** Approved
> **Date:** 2026-05-08
> **Author:** Claude (brainstorming session with Arjav)

---

## 1. Goal

Create a PostgreSQL companion database that mirrors the `scraped_places` SQLite table, hosted on `/mnt/disk`. The PostgreSQL instance runs as a **new, independent cluster** (port 5433) so the existing 25 databases on port 5432 are completely unaffected. No modifications to existing SQLite schema, application code, or service configuration.

---

## 2. Non-Goals (Out of Scope for Phase 1)

- No changes to the FastAPI application code
- No changes to the scraper workflow (CSV output unchanged)
- No changes to existing SQLite enrichment (Blitz, Contacts DB writes to SQLite `dm_*` columns as before)
- No background sync workers yet — those are Phase 2
- No modifications to `jobs.db` schema

---

## 3. Architecture

```
Existing PostgreSQL cluster (port 5432, 25 databases)
  └── Unchanged — no impact

New PostgreSQL cluster (port 5433) on /mnt/disk/postgresql/16/main
  └── Database: lead_gen
       └── Table: scraped_places (exact mirror of SQLite schema)

Phase 1 deliverables:
  ├── New PG 16 cluster on /mnt/disk (port 5433, isolated)
  ├── Database: lead_gen
  ├── Table: scraped_places (mirrors SQLite schema)
  ├── Indexes: dedupe_key UNIQUE, job_id, website, city, inserted_at
  ├── Migration script: migrate_scraped_places_to_pg.py (one-time)
  └── Validation script: validate_migration.py
```

---

## 4. PostgreSQL Cluster Setup

### 4.1 Cluster Creation

```bash
# Create new PG 16 cluster on /mnt/disk, on port 5433 (avoids conflict with existing 5432)
sudo pg_createcluster -d /mnt/disk/postgresql/16/main lead-gen --start

# Verify it's running
sudo pg_lsclusters
```

### 4.2 Connection String

```
postgresql://postgres@localhost:5433/lead_gen
```

### 4.3 Configuration

PostgreSQL 16 default configuration is sufficient for this workload. No tuning required at Phase 1 scale (~1M rows, ~2 GB data).

---

## 5. Schema

Exact mirror of SQLite `scraped_places`, translated to PostgreSQL native types.

```sql
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

-- Indexes matching SQLite originals
CREATE UNIQUE INDEX idx_sp_dedupe ON scraped_places(dedupe_key);
CREATE INDEX idx_sp_job_id ON scraped_places(job_id);
CREATE INDEX idx_sp_website ON scraped_places(website);
CREATE INDEX idx_sp_city ON scraped_places(city);
CREATE INDEX idx_sp_inserted ON scraped_places(inserted_at);
```

### Type Translation Notes (SQLite → PostgreSQL)

| SQLite Type | PostgreSQL Type | Reason |
|-------------|----------------|--------|
| `INTEGER PRIMARY KEY AUTOINCREMENT` | `SERIAL PRIMARY KEY` | PostgreSQL identity column |
| `TEXT` | `TEXT` | No size limit, same as SQLite |
| `REAL` | `DOUBLE PRECISION` | Higher precision, PostgreSQL default for floats |
| `INTEGER` (0/1 booleans) | `SMALLINT` | Keep as integer — existing data uses 0/1 |

---

## 6. New Enrichment Columns (Future — User Adds Directly)

User will add enrichment columns directly via `ALTER TABLE` in PostgreSQL as needed. No application code changes required to add columns.

Example (future):
```sql
ALTER TABLE scraped_places ADD COLUMN website_title TEXT;
ALTER TABLE scraped_places ADD COLUMN website_meta_description TEXT;
ALTER TABLE scraped_places ADD COLUMN website_technologies JSONB;
```

---

## 7. Migration Strategy

### Approach: CSV Export → COPY Import

**Why this approach:** Direct SQLite → PostgreSQL via Python drivers has type compatibility issues (TEXT vs BYTEA, NULL handling). Exporting to CSV then using PostgreSQL's `\COPY` via `psql` is the fastest, most reliable method for ~1M rows.

### Migration Script: `migrate_scraped_places_to_pg.py`

**Location:** `backend/scripts/migrate_scraped_places_to_pg.py`

**Steps:**
1. Export `scraped_places` from SQLite as CSV
2. Create schema + indexes in PostgreSQL
3. `TRUNCATE scraped_places` (in transaction, safe)
4. `\COPY` CSV into PostgreSQL
5. Validate row count matches SQLite
6. Print summary

**Safety:**
- Full table snapshot exported before TRUNCATE
- If count mismatches, raise error and exit (no silent data loss)
- Transaction-wrapped TRUNCATE

**Rollback:**
- If migration fails, `scraped_places` in PG is empty — re-run the script
- SQLite is untouched throughout

---

## 8. File Structure

```
backend/
├── scripts/
│   ├── migrate_scraped_places_to_pg.py   # One-time migration script
│   └── validate_migration.py              # Post-migration validation
```

---

## 9. Safety & Risk Mitigations

| Risk | Mitigation |
|------|-----------|
| Data loss | Full `jobs.db` backup before migration via existing `backup.sh` |
| Duplicate rows | `dedupe_key` UNIQUE index prevents duplicates in PG |
| Migration failure | SQLite untouched — re-run migration script |
| PG cluster affects existing PG | New cluster on port 5433 — completely isolated |
| Disk space exhaustion | `/mnt/disk` has 307 GB free — 60x more than needed |
| Wrong row count | Validation step fails script if counts don't match |

---

## 10. What This Does NOT Change

- Existing SQLite `jobs.db` — untouched
- FastAPI app — no code changes
- Scraper workflow — CSV output unchanged
- Existing PostgreSQL (port 5432) — completely untouched
- `sync_contacts.py` — unchanged
- Systemd services — unchanged

---

## 11. Future Phases (Out of Scope for Phase 1)

- **Phase 2:** `csv_to_sqlite_loader.py` — async worker to load scraper CSVs into SQLite `scraped_places`
- **Phase 3:** `sqlite_to_pg_sync.py` — continuous sync from SQLite to PostgreSQL
- **Phase 4:** `pg_to_sqlite_sync.py` — enrichment write-back from PostgreSQL to SQLite
- **Phase 5:** API endpoints to read enrichment from PostgreSQL

---

## 12. Validation Checklist

After Phase 1 is complete, verify:

- [ ] New PG cluster is running on port 5433
- [ ] `lead_gen` database exists
- [ ] `scraped_places` table exists with correct schema
- [ ] All 5 indexes created
- [ ] Row count in PG matches SQLite (980,585)
- [ ] Sample row verified in PG (`SELECT * FROM scraped_places LIMIT 1`)
- [ ] No existing PostgreSQL databases affected
- [ ] `jobs.db` unchanged (compare file hash before/after)
