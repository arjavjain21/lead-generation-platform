# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **unified lead generation platform** combining two core tools:
1. **Google Maps Scraper** - Scrapes business listings from Google Maps via scraper.tech API
2. **Domain Enrichment** - Enriches domains with decision-maker contacts via Blitz API and Contacts DB fallback

**Architecture:** FastAPI backend (Python) + React frontend (static build via Nginx reverse proxy)
**URL:** https://listbuilding.eagleinfoservice.com/
**Backend Port:** 8765 (managed by systemd: `lead-generation-platform.service`)

## Directory Structure

```
/var/www/lead-generation-platform/
├── backend/                    # FastAPI application
│   ├── main.py                 # Main FastAPI app, unified routes
│   ├── data/                   # SQLite DB (jobs.db), uploads/, outputs/
│   ├── enrichment/             # Domain enrichment module
│   │   ├── routes.py           # Enrichment API endpoints
│   │   ├── pipeline.py         # Enrichment workflow orchestrator
│   │   ├── blitz_client.py     # Blitz API wrapper (rate-limited, retry logic)
│   │   └── contacts_client.py  # Contacts DB fallback client
│   ├── scraper/                # Google Maps scraper module
│   │   ├── routes.py           # Scraper API endpoints
│   │   ├── crawler.py          # Async scraper with concurrency control
│   │   ├── centers.py          # Region/city center data loader
│   │   └── data/               # Country CSV files (us_centers_842_high_value.csv, etc.)
│   ├── shared/                 # Shared utilities
│   │   ├── auth.py             # JWT auth, user management (bcrypt)
│   │   ├── db.py               # Unified SQLite connection, schema init
│   │   └── job_store_base.py   # Base job store for both scraper & enrichment
│   ├── requirements.txt
│   └── .env                    # API keys (BLITZ_API_KEY, SCRAPER_TECH_KEY, etc.)
├── frontend/                   # React build output (static files)
│   ├── index.html
│   ├── assets/                 # JS/CSS bundles
│   └── categories.json         # 4,275+ business categories for autocomplete
└── health-check.sh             # Systemd health check with auto-restart
```

## Core Architecture

### Unified Job System
Both scraper and enrichment jobs share a unified SQLite database schema with `job_type` discriminator:

**Database Tables:**
- `jobs` - Unified job table with `job_type` ('scraper' | 'enrichment')
  - Scraper fields: `query`, `regions`, `total_tasks`, `done_tasks`, `result_count`
  - Enrichment fields: `total`, `processed`, `emails_found`, `filename`, `domain_col`
  - Common: `status` (queued|running|done|failed), `parent_job_id` (for chaining)
- `job_events` - Progress events for SSE streaming (`seq`, `payload` JSON)
- `users` - Auth users (email, password_hash bcrypt, is_admin)
- `daily_api_requests` - API quota tracking for non-admin users (50K/day limit)

**Thread-local connections:** `db.get_db()` returns per-thread SQLite connections with WAL mode.

### Job Chaining
Scraper jobs can chain directly into enrichment jobs via `POST /api/jobs/{scraper_job_id}/chain`:
1. Reads scraper output CSV (must have `website` column)
2. Filters rows with valid domains
3. Creates enrichment job with `parent_job_id` reference
4. Runs enrichment pipeline in background

### Server-Sent Events (SSE)
Real-time progress streaming to frontend:
- Per-job `asyncio.Event` signals: `enrichment_routes._job_signals[job_id]`
- Background jobs call `append_event()` → triggers SSE wake
- Frontend consumes `GET /api/{scraper|enrichment}/stream/{job_id}`

### Authentication & Authorization
- **User creation:** CLI only via `backend/create_user.py` (no public registration)
- **Login:** `POST /api/auth/login` → JWT (HS256, 7-day expiry)
- **Dependencies:** `Depends(auth.get_current_user)` or `Depends(auth.require_admin)`
- **Admin privileges:** Unlimited API quota, view all jobs across users

## Common Development Commands

### Backend Development
```bash
cd /var/www/lead-generation-platform/backend

# Install dependencies
source venv/bin/activate
pip install -r requirements.txt

# Run development server (auto-reload)
uvicorn main:app --reload --host 0.0.0.0 --port 8765

# Create new user
python create_user.py

# Check database
sqlite3 data/jobs.db "SELECT * FROM jobs ORDER BY created_at DESC LIMIT 5;"
```

### Service Management
```bash
# Check service status
systemctl status lead-generation-platform.service

# Restart service
sudo systemctl restart lead-generation-platform.service

# View logs
journalctl -u lead-generation-platform.service -f

# Health check
curl http://localhost:8765/api/health
```

### Frontend
Frontend is pre-built static files. To rebuild frontend (requires separate frontend repo):
```bash
# Frontend build output goes to: /var/www/lead-generation-platform/frontend/
# Then reload nginx: sudo systemctl reload nginx
```

## Key Integrations

### Blitz API (api.blitz-api.ai)
**Rate limit:** 4 RPS (conservative, API limit is 5 RPS)
**Endpoints:**
- `POST /v2/enrichment/domain-to-linkedin` - Domain → company LinkedIn URL
- `POST /v2/search/waterfall-icp-keyword` - Company LinkedIn → decision makers
- `POST /v2/enrichment/email` - Person LinkedIn → work email

**Retry logic:** Up to 3 retries with exponential backoff (base 2s, cap 60s), respects `Retry-After` header.

**Default cascade:** 3 tiers (Owner/CEO/F → C-level/VP → Director/Head) defined in `blitz_client.DEFAULT_CASCADE`.

### Scraper.tech API
**Endpoint:** `GET https://api.scraper.tech/searchmaps.php`
**Auth header:** `Scraper-key: <SCRAPER_TECH_KEY>`
**Concurrency:** 8 async workers per job
**Radius filtering:** Haversine distance with query-based heuristics (city-heavy: 100km, rural: 200km)

### Contacts DB (leadsdatabase.cc)
**Fallback endpoints:**
- `GET /v1/person/by-linkedin?linkedin_url=...` - Person lookup by LinkedIn
- `GET /v1/person/by-name?name=...&domain=...` - Person lookup by name + domain

## Critical Implementation Details

### Job Store Pattern
Both `scraper/job_store.py` and `enrichment/job_store.py` are thin wrappers around `shared.job_store_base.JobStoreBase`. The base class handles:
- Unified CRUD operations for both job types
- Event append with automatic counter updates
- Parent-child relationship queries
- Stale job cleanup on server restart

### Enrichment Pipeline Flow
For each domain row (`enrichment/pipeline.py`):
1. **Blitz:** domain → company LinkedIn URL
2. **Blitz:** Waterfall ICP search → up to 5 decision makers (titles cascade)
3. **For each person:**
   - Blitz: person LinkedIn → work email
   - Fallback: Contacts DB by LinkedIn URL
   - Fallback: Contacts DB by name + domain
4. **If no company LinkedIn found AND has name columns:** Direct Contacts DB lookup by name + domain

**Concurrency:** 5 domains concurrently, 10 email lookups concurrently (`DOMAIN_CONCURRENCY`, `EMAIL_CONCURRENCY`).

### Scraper Region System
**USA:** State-based (all 50 states + territories) or city-based (fuzzy search)
**Non-US:** Center-based selection (lat/lng centers from CSV files)
**Zoom levels:** [10, 11, 12] for each center/region (progressive radius)
**Deduplication:** In-memory by `place_id`, fallback to hash(name+website+address)

### Environment Variables
Required in `backend/.env`:
```
JWT_SECRET=<random-secret-key>
SCRAPER_TECH_KEY=<scraper-tech-api-key>
BLITZ_API_KEY=<blitz-api-key>
CONTACTS_API_BASE_URL=https://leadsdatabase.cc
CONTACTS_API_TOKEN=<contacts-api-token>
ALLOWED_ORIGINS=http://localhost:5173,http://localhost:3000
```

## Troubleshooting

### Service Won't Start
```bash
# Check logs
journalctl -u lead-generation-platform.service -n 50

# Verify port availability
ss -tlnp | grep 8765

# Check .env file exists
ls -la /var/www/lead-generation-platform/backend/.env
```

### Database Locked
SQLite uses WAL mode. If locks persist:
```bash
cd /var/www/lead-generation-platform/backend
sqlite3 data/jobs.db "PRAGMA wal_checkpoint(TRUNCATE);"
```

### High Memory Usage
Check for runaway background jobs:
```bash
sqlite3 data/jobs.db "SELECT job_id, status, created_at FROM jobs WHERE status='running';"
```

Stale jobs auto-cleanup on server restart via `@app.on_event("startup")` → `cleanup_stale_jobs()`.
