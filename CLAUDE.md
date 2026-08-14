# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## IMPORTANT: Safety & Workflow Guidelines

**BEFORE ANY WORK ON THIS PROJECT, load the `lead-generation-platform-workflow` skill**
(user-level: `~/.claude/skills/lead-generation-platform-workflow/`). It is the operating manual +
safety guardrail set, and holds per-action runbooks in its `references/` (provider lifecycle,
contacts write-back, external/MCP integration, testing & deploy, safety invariants, troubleshooting).
> The legacy paths `.claude/skills/lead-generation-platform-workflow.md` and `.claude/LEAD_GENERATION_SAFETY.md`
> referenced here in the past **did not exist** — the skill replaces them.

Other relevant skills: bug fixes (`superpowers:systematic-debugging`), new features
(`superpowers:brainstorming`), code review (`superpowers:requesting-code-review`),
DB changes (`everything-claude-code:database-reviewer`), security (`everything-claude-code:security-reviewer`).

**ABSOLUTE PROHIBITIONS** (full list in the skill's `references/safety-and-invariants.md`):
- NEVER delete or truncate `jobs.db` (~4.4 GB live data; the 0-byte top-level `backend/jobs.db` is dead)
- NEVER modify `.env` or commit secrets
- NEVER kill running background jobs without cause — cancel via the endpoint and wait for
  `status='cancelled'` before any `systemctl restart` (4 gunicorn workers; the DB is the only
  cross-worker source of truth for job state)
- NEVER modify systemd service files without a backup
- NEVER add a contacts-returning endpoint without routing it through `contacts_writer`

**Conventions:** conventional commits `type(scope): desc`; **attribution is DISABLED** — do NOT add a `Co-Authored-By` footer.

## Project Overview

**Unified Lead Generation Platform** combining:
1. **Google Maps Scraper** - Scrapes business listings via scraper.tech API
2. **Domain Enrichment** - Enriches domains with decision-maker contacts via cascading API calls
3. **Phone Enrichment** - Enriches LinkedIn profiles with phone numbers via Blitz Direct Phone API

**Architecture:** FastAPI backend (Python) + React frontend (static build via Nginx reverse proxy)
**URL:** https://listbuilding.eagleinfoservice.com/
**Backend Port:** 8765 (managed by systemd: `lead-generation-platform.service`)

**Lead Universe classification:** every lead in the contacts DB carries `core.person.lead_universe` — one of `local_business` / `b2b_agency` / `saas` / `ecom` (NULL = unclassified, ~38%). Filter by it on the **Find People** UI page, via `POST /api/enrichment/search/employees` (`universe` field), or directly on the Contacts DB API `GET https://leadsdatabase.cc/v1/people/search?universe=`. New leads are auto-tagged on write-back (`classify_industry` in `enrichment/contacts_writer.py`); DB classifiers: `core.fn_classify_industry`, `core.fn_classify_industry_category`. Full rules: `docs/LEAD_UNIVERSE_CLASSIFICATION.md`.

## Directory Structure

```
/var/www/lead-generation-platform/
├── backend/
│   ├── main.py                  # FastAPI app, router mounts, job chaining, MCP mount, lifespan
│   ├── routes.py                # DEAD duplicate — NOT imported. Live scraper routes: scraper/routes.py
│   ├── mcp_oracle/              # ListBuilding MCP server (read-only docs oracle, mounted at /mcp)
│   ├── data/                    # SQLite DB (jobs.db ~4.4 GB), uploads/, outputs/
│   ├── enrichment/              # Domain enrichment module
│   │   ├── routes.py            # All enrichment endpoints (~3K lines)
│   │   ├── pipeline.py          # Workflow orchestrator / cascade
│   │   ├── list_builder.py      # List Building Tool (Flows 1, 2, 3) + unified /enrich
│   │   ├── providers.py         # ENABLED_PROVIDERS — single source of truth for provider on/off
│   │   ├── contacts_writer.py   # Contacts DB write-back (single entry point; USE_CONTACTS_WRITER_V2)
│   │   ├── response_normalizer.py / raw_contact_collector.py  # provider→canonical contact + collector
│   │   ├── call_tracker.py      # provider_call_log + provider_email_ledger observability
│   │   ├── blitz_client.py      # Blitz API wrapper (25 RPS)
│   │   ├── smartprospect_client.py  # SmartLead Find Emails (30 RPS, batch ≤10)
│   │   ├── contacts_client.py   # Contacts DB wrapper (75 RPS) + business upsert
│   │   ├── wizleads_client.py   # WizLeads (10 RPS)
│   │   ├── better_enrich_client.py  # BetterEnrich (10/5 RPS)
│   │   └── prospeo_client.py    # Disabled end-to-end (imported, never called) — see API Integrations
│   ├── phone_enrichment/        # Phone enrichment module (Blitz Direct Phone)
│   │   ├── routes.py            # Phone enrichment API endpoints
│   │   ├── pipeline.py          # Phone enrichment workflow
│   │   └── job_store.py         # Phone enrichment job storage
│   ├── scraper/                 # Google Maps scraper
│   │   ├── crawler.py           # Async scraper (8 workers)
│   │   ├── centers.py           # Region/city center data loader
│   │   └── data/                # Country CSV files
│   └── shared/
│       ├── auth.py              # JWT auth (7-day), API keys (lgp_), user mgmt (CLI-only creation)
│       ├── db.py                # Thread-local SQLite, WAL (30s busy_timeout, 200MB cap), 50K quota fns
│       ├── job_store_base.py    # Base class for job stores (append_event, heartbeat, checkpoints)
│       ├── mcp_auth.py          # MCPAuthMiddleware (self-scopes to /mcp*, X-API-Key or Bearer)
│       └── circuit_breaker.py   # Circuit breaker (Blitz / Contacts DB / SmartProspect only)
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
| **GetLeads** | batch 100 (~10k/min) | 3rd | Verified DM emails + bonus phones (batch of 100, unlimited plan) |
| **smartprospect** | 30 RPS | 4th | Person-email finder, batch up to 10, self-verifying |
| **WizLeads** | 10 RPS | 5th | Catch-all verified email enrichment |
| **BetterEnrich** | 10 RPS | 6th | Person email, company email |

> **Prospeo** is implemented in `backend/enrichment/prospeo_client.py` but **disabled end-to-end via a 4-layer belt-and-suspenders**: (1) `ENABLED_PROVIDERS["prospeo"]=False` in `providers.py`; (2) `ENABLE_PROSPEO=false` in `backend/.env`; (3) a hard guard at `pipeline.py` (~L294) that reads `ENABLE_PROSPEO` and skips *before* the global check; (4) `prospeo_client` is imported but **never called** anywhere in the cascade. **To re-enable requires all of:** `ENABLE_PROSPEO=true` + flip the dict to `True` + wire an actual cascade step (currently absent). Full provider lifecycle in the `lead-generation-platform-workflow` skill → `references/providers.md`.

### Blitz Cascade (title tiers)
```
Tier 1: Owner, CEO, Founder, Co-Founder, President
Tier 2: C-level (CMO, CTO, COO, VP...)
Tier 3: Director-level (Director of Marketing, etc.)
```

## Enrichment Endpoints

### Unified Enrichment
**POST** `/api/enrichment/enrich` - Returns contacts AND syncs to Contacts DB
**GET** `/api/enrichment/enrich` - **Also returns contacts AND syncs** (NOT a pure lookup — the "no sync" note in older docs is stale; both paths persist to the external Contacts DB at `leadsdatabase.cc`)

Both accept request-time cascade restrictors (mutually exclusive): `force_provider` (single provider) and `selected_providers` (allowlist; `contacts_db` is always allowed even if omitted).

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
GETLEADS_API_KEY=<getleads-key>
ENABLE_GETLEADS=true
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

## Caching System

### Overview
A comprehensive caching system prevents re-scraping identical queries, saving API costs and time.

### Cache Storage
- **Location:** cache rows live in the `scraped_cache` table inside `jobs.db` (the `/mnt/disk/lead-generation-platform/cache/` dir is created but NOT used for storage)
- **Expiry:** 90 days (`CACHE_EXPIRY_DAYS`, `shared/db.py`)
- **Available Space:** ~257 GB free on `/mnt/disk`

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

### Cached Data
Check live counts: `sqlite3 backend/data/jobs.db "SELECT query, result_count FROM scraped_cache ORDER BY created_at DESC LIMIT 10;"`

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

## Recent Changes & Status

This project changes often; do not rely on a frozen snapshot here. For current state:
- **Operating rules, safety guardrails, runbooks:** load the `lead-generation-platform-workflow` skill.
- **Recent code history:** `git log --oneline -20` and the per-session memory notes in
  `~/.claude/projects/-var-www-lead-generation-platform/memory/`.
- **Live health:** `curl -s http://localhost:8765/api/health` and `./monitor.sh`.
- **Canonical API contract:** `docs/ListBuilding_Platform_Full_API_Reference_2026-07-16.md`.
