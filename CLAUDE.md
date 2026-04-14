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
│   │   ├── contacts_client.py   # Contacts DB wrapper (75 RPS)
│   │   ├── better_enrich_client.py
│   │   └── prospeo_client.py    # Final fallback enrichment
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
| **BetterEnrich** | 10 RPS | 3rd | Person email, company email |
| **Prospeo** | 30 RPS | 4th (paid) | Final fallback - person/company enrichment |

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
CONTACTS_API_TOKEN=<contacts-token>
BETTER_ENRICH_API_KEY=<betterenrich-key>
PROSPEO_API_KEY=<prospeo-key>
ALLOWED_ORIGINS=http://localhost:5173,http://localhost:3000

# Optional (email notifications)
SMTP_SERVER, SMTP_PORT, SMTP_USER, SMTP_PASSWORD, SENDER_EMAIL, DEFAULT_RECIPIENT
```

## Troubleshooting

```bash
# Service won't start
journalctl -u lead-generation-platform.service -n 50
ss -tlnp | grep 8765

# Database locked
cd backend && sqlite3 data/jobs.db "PRAGMA wal_checkpoint(TRUNCATE);"

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
