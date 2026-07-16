# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## IMPORTANT: Safety & Workflow Guidelines

**BEFORE ANY WORK ON THIS PROJECT, ALWAYS:**

1. Read `.claude/skills/lead-generation-platform-workflow.md` for complete workflow guidelines
2. Check `Skill` tool for available skills and activate relevant ones:
   - Bug fixes: `superpowers:systematic-debugging`
   - New features: `superpowers:brainstorming`
   - Code changes: `superpowers:requesting-code-review`
   - Database changes: `everything-claude-code:database-reviewer`
   - Security-sensitive: `everything-claude-code:security-reviewer`

**ABSOLUTE PROHIBITIONS:**
- NEVER delete or truncate `jobs.db` database
- NEVER modify `.env` or commit secrets
- NEVER kill running background jobs without cause
- NEVER modify systemd service files without backup

**Quick Safety Reference:** See `.claude/LEAD_GENERATION_SAFETY.md`

## Project Overview

**Unified Lead Generation Platform** combining:
1. **Google Maps Scraper** - Scrapes business listings via scraper.tech API
2. **Domain Enrichment** - Enriches domains with decision-maker contacts via cascading API calls
3. **Phone Enrichment** - Enriches LinkedIn profiles with phone numbers via Blitz Direct Phone API

**Architecture:** FastAPI backend (Python) + React frontend (static build via Nginx reverse proxy)
**URL:** https://listbuilding.eagleinfoservice.com/
**Backend Port:** 8765 (managed by systemd: `lead-generation-platform.service`)

## Directory Structure

```
/var/www/lead-generation-platform/
├── backend/
│   ├── main.py                  # FastAPI app, unified routes, job chaining
│   ├── routes.py                # Scraper API routes
│   ├── data/                    # SQLite DB (jobs.db), uploads/, outputs/
│   ├── enrichment/              # Domain enrichment module
│   │   ├── routes.py            # All enrichment endpoints (~3K lines)
│   │   ├── pipeline.py          # Workflow orchestrator
│   │   ├── list_builder.py      # List Building Tool (Flows 1, 2, 3)
│   │   ├── blitz_client.py      # Blitz API wrapper (25 RPS, retry logic)
│   │   ├── smartprospect_client.py  # SmartLead Find Emails wrapper (30 RPS, batch ≤10)
│   │   ├── contacts_client.py   # Contacts DB wrapper (75 RPS)
│   │   ├── better_enrich_client.py
│   │   └── prospeo_client.py    # Disabled fallback (set ENABLE_PROSPEO=true + flip providers.py to re-enable)
│   ├── phone_enrichment/        # Phone enrichment module (Blitz Direct Phone)
│   │   ├── routes.py            # Phone enrichment API endpoints
│   │   ├── pipeline.py          # Phone enrichment workflow
│   │   └── job_store.py         # Phone enrichment job storage
│   ├── scraper/                 # Google Maps scraper
│   │   ├── crawler.py           # Async scraper (8 workers)
│   │   ├── centers.py           # Region/city center data loader
│   │   └── data/                # Country CSV files
│   └── shared/
│       ├── auth.py              # JWT auth, API keys, user management
│       ├── db.py                # Thread-local SQLite with WAL mode
│       ├── job_store_base.py    # Base class for job stores
│       └── circuit_breaker.py   # Circuit breaker for API resilience
├── scripts/
│   ├── migrate_scraped_places_to_pg.py  # One-time SQLite → PG migration (Phase 1)
│   └── validate_migration.py             # Post-migration validation
├── frontend/                    # Pre-built static files (React)
├── *.sh                         # backup.sh, restore.sh, monitor.sh
└── *.service, *.timer          # Systemd service/timer files
```

## Core Architecture

### Unified Job System
Single SQLite database (`jobs.db`) with `job_type` discriminator ('scraper' | 'enrichment' | 'phone_enrichment'):

| Table | Purpose |
|-------|---------|
| `jobs` | Job metadata, status, counters |
| `job_events` | SSE progress events (`seq`, `payload` JSON) |
| `users` | Email/password (bcrypt) |
| `api_keys` | API key authentication |
| `daily_api_requests` | 50K/day quota tracking (non-admin) |

**Phone enrichment jobs** use `job_type='phone_enrichment'` with additional columns: `linkedin_col`, `phones_found`.

**Thread-local connections:** `db.get_db()` returns per-thread SQLite with WAL mode.

### Job Chaining
Scraper → Enrichment via `POST /api/jobs/{scraper_job_id}/chain`:
1. Reads scraper output CSV (requires `website` column)
2. Filters valid domains
3. Creates enrichment job with `parent_job_id`

### SSE Streaming
Per-job `asyncio.Event` signals in `enrichment_routes._job_signals[job_id]`:
- Background jobs call `append_event()` → triggers wake
- Frontend: `GET /api/{scraper|enrichment}/stream/{job_id}`

### Authentication
- **User creation:** CLI only (`backend/create_user.py`)
- **Login:** `POST /api/auth/login` → JWT (HS256, 7-day expiry)
- **API Keys:** `POST /api/api-keys` → Header `X-API-Key: <key>` or `Authorization: Bearer <key>`
- **Dependencies:** `Depends(auth.get_current_user)`, `Depends(auth.get_current_user_with_api_key)`

## Common Commands

```bash
cd /var/www/lead-generation-platform/backend
source venv/bin/activate

# Development
uvicorn main:app --reload --host 0.0.0.0 --port 8765
python create_user.py

# Service management
sudo systemctl restart lead-generation-platform.service
journalctl -u lead-generation-platform.service -f
curl http://localhost:8765/api/health

# Database
sqlite3 data/jobs.db "PRAGMA wal_checkpoint(TRUNCATE);"  # Fix locks
```

## API Integrations (Enrichment Cascade)

Each enrichment source has different cost/quality tradeoffs:

| API | Rate Limit | Priority | Purpose |
|-----|------------|----------|---------|
| **Contacts DB** | 75 RPS | 1st (free) | Domain → company → contacts with emails |
| **Blitz** | 25 RPS | 2nd | LinkedIn-based enrichment with title cascade |
| **smartprospect** | 30 RPS | 3rd | Person-email finder, batch up to 10, self-verifying |
| **WizLeads** | 10 RPS | 4th | Catch-all verified email enrichment |
| **BetterEnrich** | 10 RPS | 5th | Person email, company email |

> **Prospeo** is implemented in `backend/enrichment/prospeo_client.py` but currently **disabled** end-to-end. The frontend no longer exposes it as a selectable provider, and the backend cascade will skip it via both `ENABLED_PROVIDERS["prospeo"]=False` (in `backend/enrichment/providers.py`) and the `ENABLE_PROSPEO=false` env kill-switch in `backend/.env`. To re-enable: set `ENABLE_PROSPEO=true` in `.env` AND flip `prospeo` to `True` in `providers.py`.

### Blitz Cascade (title tiers)
```
Tier 1: Owner, CEO, Founder, Co-Founder, President
Tier 2: C-level (CMO, CTO, COO, VP...)
Tier 3: Director-level (Director of Marketing, etc.)
```

## Enrichment Endpoints

### Unified Enrichment
**POST** `/api/enrichment/enrich` - Returns contacts AND syncs to database
**GET** `/api/enrichment/enrich` - Quick lookup without sync

| Input | Mode | Flow |
|-------|------|------|
| `domain` only | `domain_only` | All decision makers (cascade) |
| `linkedin_url` only | `linkedin_only` | Specific person via cascade |
| `domain` + `full_name` or `linkedin_url` | `enhanced` | Specific person only → NOT FOUND if not found |

**Enhanced mode:** Looks for SPECIFIC person only. Returns 0 contacts if not found (no fallback to domain cascade).

### List Building Tool (`enrichment/list_builder.py`)

**Flow 1:** `POST /api/enrichment/flows/domain-enrich` - Domain CSV → decision makers
**Flow 2:** `POST /api/enrichment/flows/search` - Company search by criteria
**Flow 3:** `POST /api/enrichment/flows/linkedin-enrich` - Bulk LinkedIn enrichment

**Concurrency:** 25 domains, 15 LinkedIn URLs, 5 searches in parallel

### Job Cancellation
`POST /api/enrichment/jobs/{job_id}/cancel` sets `_cancelled_jobs[job_id]`, checked by pipelines periodically.

### Phone Enrichment (`/api/phone-enrichment`)

Enriches LinkedIn profiles with phone numbers using Blitz Direct Phone API:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/phone-enrichment/jobs` | GET | List phone enrichment jobs |
| `/api/phone-enrichment/jobs` | POST | Create new job (upload CSV) |
| `/api/phone-enrichment/jobs/{job_id}` | GET | Get job status |
| `/api/phone-enrichment/jobs/{job_id}/stream` | GET | SSE progress stream |
| `/api/phone-enrichment/jobs/{job_id}/download` | GET | Download enriched CSV |

**Create job:** Upload CSV with LinkedIn URLs, auto-detects the URL column or specify via `linkedin_col` query param.

## Environment Variables

```bash
# Required
JWT_SECRET=<secret>
SCRAPER_TECH_KEY=<scraper-tech-key>
BLITZ_API_KEY=<blitz-key>
SMARTPROSPECT_API_KEY=<smartprospect-key>
CONTACTS_API_TOKEN=<contacts-token>
BETTER_ENRICH_API_KEY=<betterenrich-key>
PROSPEO_API_KEY=<prospeo-key>
ALLOWED_ORIGINS=http://localhost:5173,http://localhost:3000

# Optional (email notifications)
SMTP_SERVER, SMTP_PORT, SMTP_USER, SMTP_PASSWORD, SENDER_EMAIL, DEFAULT_RECIPIENT
```

## PostgreSQL Companion Database

A PostgreSQL 16 cluster on `/mnt/disk` hosts the `lead_gen` database — a companion to SQLite for enriched data and future website enrichment.

| Property | Value |
|----------|-------|
| Cluster | `/mnt/disk/postgresql/16/main` |
| Port | **5433** (independent from existing PG on 5432) |
| Database | `lead_gen` |
| Connection | `postgresql://postgres@localhost:5433/lead_gen` |
| Table | `scraped_places` |
| Row count | ~980,585 |

**Important:** This cluster is isolated from the existing 25 PostgreSQL databases on port 5432. They are completely unaffected.

**Managing columns:** New enrichment columns are added directly via `ALTER TABLE` in PostgreSQL — no application code changes needed.

**Future sync (Phase 2+):**
- `backend/scripts/csv_to_sqlite_loader.py` — loads scraper CSV output into SQLite `scraped_places`
- `backend/scripts/sqlite_to_pg_sync.py` — continuous sync from SQLite to PostgreSQL
- `backend/scripts/pg_to_sqlite_sync.py` — enrichment write-back from PostgreSQL to SQLite

## Troubleshooting

```bash
# Service won't start
journalctl -u lead-generation-platform.service -n 50
ss -tlnp | grep 8765

# Database locked
cd backend && sqlite3 data/jobs.db "PRAGMA wal_checkpoint(TRUNCATE);"

# PostgreSQL (port 5433) — verify cluster is up
sudo pg_lsclusters

# PostgreSQL — connect to lead_gen database
sudo -u postgres psql -p 5433 lead_gen

# PostgreSQL — check row counts
sudo -u postgres psql -p 5433 lead_gen -c "SELECT COUNT(*) FROM scraped_places;"

# Stale running jobs (auto-cleaned on restart)
sqlite3 data/jobs.db "SELECT job_id, status FROM jobs WHERE status='running';"

# Monitoring scripts
./monitor.sh                    # Check system health, running jobs, recent errors
./backup.sh                     # Backup database and uploads
./restore.sh <backup_file>      # Restore from backup
```

## Monitoring

The platform includes systemd-based monitoring:
- `lead-generation-platform-monitor.service` - Runs health checks periodically
- `lead-generation-platform-monitor.timer` - Triggers the monitor service on schedule

The monitor script checks: backend process, Nginx, disk space, stale jobs, recent errors.

## Scraper Countries

The Google Maps scraper supports multiple countries with geographic center data:

| Code | Country | Cities | Notes |
|------|---------|--------|-------|
| us | United States | 842 | Full coverage with offset rings |
| gb | United Kingdom | 50+ | With offset rings |
| ie | Ireland | 15 | With offset rings |
| au | Australia | 31 | With offset rings |
| ca | Canada | ~30 | With offset rings |
| de | Germany | 20 | Major cities only |
| fr | France | 15 | Major cities only |
| it | Italy | 15 | Major cities only |
| es | Spain | 12 | Major cities only |
| nl | Netherlands | 8 | Major cities only |
| be | Belgium | 5 | Major cities only |
| pl | Poland | 10 | Major cities only |
| se | Sweden | 8 | Major cities only |
| dk | Denmark | 4 | Major cities only |
| at | Austria | 7 | Major cities only |
| ch | Switzerland | 7 | Major cities only |
| pt | Portugal | 5 | Major cities only |

### Adding New Countries

To add a new country to the scraper:

1. Create a CSV file in `backend/scraper/data/` with the schema:
   ```csv
   name,state,lat,lng,tier,rank,population_basis,center_type,anchor_city,country
   Berlin,Berlin,52.5200,13.4050,metro,1,major_cities,anchor_city,Berlin,de
   ```

2. Update `backend/scraper/centers.py`:
   - Add to `COUNTRY_FILES`: `(DATA_DIR / "xx_centers.csv", "xx")`
   - Add to `COUNTRY_NAMES`: `"xx": "Country Name"`
   - Update the order list in `get_countries()`

3. Update `frontend/index.html` country dropdown

### Simplified Approach (European Countries)

European countries use a simplified approach:
- Major cities only (no offset rings like US/UK)
- Each city = 1 center, 3 zoom levels (10, 11, 12)
- Good B2B coverage since most businesses are in major cities
- Faster to implement and maintain

---

## Caching System (Implemented 2026-06-07)

### Overview
A comprehensive caching system prevents re-scraping identical queries, saving API costs and time.

### Cache Storage
- **Location:** `/mnt/disk/lead-generation-platform/cache/`
- **Expiry:** 90 days
- **Available Space:** 86GB

### Cache Tables
- `scraped_cache` - Main cache with metadata, checksums, expiry
- `task_checkpoints` - Resume capability (per-task progress)
- `cache_center_counts` - Geographic subset filtering
- `cache_stats` - Hit/miss tracking

### Cache Endpoints
- `POST /api/scraper/cache/check` - Check for cached results
- `GET /api/scraper/cache/download/{cache_id}` - Download cached files
- `POST /api/scraper/cache/subset-count` - Get subset counts
- `GET /api/scraper/jobs/{id}/resume-info` - Resume eligibility
- `POST /api/scraper/jobs/{id}/resume` - Resume from checkpoints

### Cached Data (as of 2026-06-07)
- dental clinic: 93,127 results
- dentist: 127,012 results
- elementary school: 50,867 results

### Resume Capability
Jobs can be resumed from last checkpoint:
- Tracks completed (center, zoom) combinations
- Skips already-processed tasks on resume
- Requires new jobs to create checkpoints

### Download Filename Format
```
{query}_{centers}_centers_{results}_results_{status}.csv
```
Example: `dental_clinic_2526_centers_46556_results_done.csv`

### Cache Key Components
- Query (normalized)
- Region signature (hash of geographic config)
- Zoom signature (hash of [10,11,12])
- Expected types signature (hash of filters)

---

## Recent Updates (2026-06-07)

### Authentication
- `/api/auth/me` now accepts both JWT tokens AND API keys
- Fixed frontend authentication race conditions
- Added timeout protection (10s) to auth requests

### UI Improvements
- Filter tabs now functional (All, Done, Running, Failed, etc.)
- Stopped jobs show download buttons
- Added cache modal for reusing cached results
- Added resume modal for stopped jobs

### API Key Authentication
Updated to support API keys on user-facing endpoints:
- `/api/auth/me`
- `/api/scraper/cache/*`
- `/api/scraper/jobs/{id}/download`
- `/api/scraper/jobs/{id}/resume-info`

### Bug Fixes
- Fixed JavaScript syntax error (line 1912)
- Fixed DOM loading race condition
- Fixed filter tabs not working
- Fixed missing download buttons for stopped jobs

---

## Current Status (2026-06-07)

### Production Status
- ✅ Backend: Healthy and running on port 8765
- ✅ Frontend: Loading correctly at listbuilding.eagleinfoservice.com
- ✅ Caching: Operational with 3 cached entries
- ✅ Authentication: Working with JWT and API keys
- ✅ Downloads: Enhanced format with center counts

### Known Limitations
- Resume requires new jobs to build checkpoints
- Cached downloads don't show exact center count
- Subset queries pending full implementation

### Performance
- Cache lookup: < 50ms
- API calls saved per cached query: ~88,620
- Expected cache hit rate: 30-40% after 60 days
