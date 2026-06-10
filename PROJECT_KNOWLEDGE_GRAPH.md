# Project Knowledge Graph - Lead Generation Platform

**Last Updated:** 2026-06-09
**Status:** Comprehensive analysis complete. Ready for next task assignment.

---

## 1. Project Identity

| Property | Value |
|----------|-------|
| **Name** | Unified Lead Generation Platform |
| **URL** | https://listbuilding.eagleinfoservice.com/ |
| **Backend** | FastAPI (Python) on port 8765, managed by `lead-generation-platform.service` |
| **Frontend** | Static React build served via Nginx |
| **Database** | SQLite (`backend/data/jobs.db`, WAL mode) |
| **Cache Storage** | `/mnt/disk/lead-generation-platform/cache/` |
| **Git** | branch `master`, user `arjavjain21` |

---

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│  Browser (https://listbuilding.eagleinfoservice.com/)           │
│  Static React frontend (frontend/index.html, 3053 lines)        │
└─────────────────────┬───────────────────────────────────────────┘
                      │ HTTPS via Nginx reverse proxy
┌─────────────────────▼───────────────────────────────────────────┐
│  FastAPI Backend (port 8765)                                    │
│  ┌──────────────┬──────────────┬────────────────────────────┐  │
│  │  /scraper    │ /enrichment  │ /phone-enrichment           │  │
│  │  routes.py   │ routes.py    │ routes.py                   │  │
│  │  1557 lines  │              │                             │  │
│  └──────┬───────┴──────┬───────┴──────────┬──────────────────┘  │
│         │              │                  │                      │
│  ┌──────▼──────────────▼──────────────────▼──────────────────┐  │
│  │  JobStoreBase (shared/job_store_base.py, 471 lines)       │  │
│  │  - checkpoint writes, resume logic                        │  │
│  └────────────────────────┬──────────────────────────────────┘  │
│         ┌─────────────────┼─────────────────┐                    │
│  ┌──────▼──────┐  ┌────────▼────────┐ ┌─────▼─────┐             │
│  │  crawler.py │  │ enrichment/     │ │ phone/    │             │
│  │  463 lines  │  │ pipeline.py     │ │ pipeline  │             │
│  │  Async 8+  │  │ 25 domains RPS  │ │ .py       │             │
│  │  workers    │  │                 │ │           │             │
│  └──────┬──────┘  └─────────────────┘ └───────────┘             │
│         │                                                          │
│  ┌──────▼──────────────────────────────────────────────────────┐ │
│  │  External APIs                                              │ │
│  │  • scraper.tech (Google Maps)                               │ │
│  │  • Blitz (LinkedIn)         • BetterEnrich (V3, LinkedIn)   │ │
│  │  • Contacts DB (75 RPS)     • Prospeo (paid, 30 RPS)        │ │
│  │  • Wizleads (newly integrated)                              │ │
│  └─────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────┘
```

---

## 3. Core Components (Code Map)

### 3.1 Backend Core

| File | Lines | Purpose |
|------|-------|---------|
| `backend/main.py` | 443 | FastAPI app, unified routes, job chaining, auth wiring |
| `backend/shared/db.py` | 271 | Thread-local SQLite, schema init, daily quota, **CACHE_DIR + CACHE_EXPIRY_DAYS=90** |
| `backend/shared/job_store_base.py` | 471 | Job CRUD, cancellation, **task checkpoint methods** |
| `backend/shared/auth.py` | - | JWT + API key auth (`get_current_user`, `get_current_user_with_api_key`) |
| `backend/shared/circuit_breaker.py` | - | API resilience |

### 3.2 Scraper Module (the biggest concern area)

| File | Lines | Purpose |
|------|-------|---------|
| `backend/scraper/routes.py` | **1557** | All scraper API endpoints (cache, resume, download) |
| `backend/scraper/crawler.py` | 463 | Async scraper (writes `center_id` to CSV) |
| `backend/scraper/centers.py` | 700+ | Country CSV loader, state aliases, tier system |
| `backend/scraper/cache.py` | 265 | Cache lookup/store, signatures, checksums |
| `backend/scraper/job_store.py` | - | Scraper-specific job ops |

### 3.3 Cache & Resume Implementation

| File | Purpose |
|------|---------|
| `backend/cache_schema.sql` | scraped_cache + cache_stats + cache_center_counts |
| `backend/scraper/cache.py` | `check_cache()`, `store_cache()`, signature generation |
| `backend/shared/job_store_base.py` | `write_task_checkpoint()`, `get_task_checkpoints()`, `can_resume_job()` |
| `backend/populate_cache.py` | Populate cache from 3 historical stopped jobs |
| `backend/shared/cache_utils.py` | (per docs - not in current file listing) |

### 3.4 Enrichment Cascade

| Provider | Rate | Priority | Source File |
|----------|------|----------|-------------|
| Contacts DB | 75 RPS | 1st (free) | `enrichment/contacts_client.py` |
| Blitz | 25 RPS | 2nd | `enrichment/blitz_client.py` |
| BetterEnrich V3 | 10 RPS | 3rd | `enrichment/better_enrich_client.py` |
| Prospeo | 30 RPS | 4th (paid) | `enrichment/prospeo_client.py` |
| **Wizleads** | - | NEW (just integrated) | `enrichment/wizleads_client.py` |

---

## 4. Database Schema

### 4.1 Existing Tables
- `jobs` (unified, `job_type` discriminator: scraper | enrichment | phone_enrichment)
- `job_events` (SSE progress)
- `users`, `api_keys`, `daily_api_requests`
- `scraped_places` (987K rows in PG companion at port 5433)
- `phone_enrichments` (LinkedIn → phone cache)

### 4.2 NEW Cache Tables (added 2026-06-07)
```sql
scraped_cache              -- 12 entries, 90-day expiry, partial results supported
task_checkpoints           -- 116,287 rows! resume tracking
cache_center_counts        -- per-center result counts for subset queries
cache_stats                -- hit/miss tracking
job_checkpoints            -- older row-index-based checkpoints for enrichment
```

### 4.3 NEW Columns on `jobs`
- `is_resumable INTEGER DEFAULT 1`
- `checkpoint_count INTEGER DEFAULT 0`
- `restart_count` (used in restart logic)

---

## 5. Geographic Coverage Expansion

### 5.1 US (842 centers)
- File: `us_centers_842_high_value.csv`
- 3 tiers via "All US" mode:
  - **Tier 1** (Quick): 43 major cities, ~129 API calls
  - **Tier 2** (Standard - default): 241 cities, ~723 API calls
  - **Tier 3** (Comprehensive): 29,540 cities, **88,638 API calls** (3 zooms × cities)

### 5.2 Other Countries (per `COUNTRY_FILES` in `centers.py`)
| Code | File | Cities | Notes |
|------|------|--------|-------|
| gb | uk_centers.csv | 50+ | **Recently expanded** via `generate_uk_centers.py` |
| ie | ie_centers.csv | 15 | |
| au | au_centers.csv | 31+ | **Recently expanded** via `generate_au_centers.py` |
| ca | ca_centers.csv | 30+ | Supports postal codes |
| de/fr/es/it/nl/be/pl/se/dk/at/ch/pt | various | 5-20 | Major cities only |
| no | no_centers.csv | - | New |
| nz | nz_centers.csv | - | New |

### 5.3 UK Postcode + AU Postcode Support
- `process_au_postcodes.py` - processes au_postcodes.csv
- `generate_uk_centers.py` - generates UK centers
- File `uk_centers.csv` is in `data/` (created 2026-06-07)
- `parse_uk_postcode_csv` and `parse_ca_postal_csv` endpoints exist

---

## 6. Caching System (90-day expiry)

### 6.1 Cache Key Generation (in `cache.py`)
```python
region_sig = SHA256(sorted regions JSON)[:16]      # geographic config
zoom_sig   = SHA256(sorted zooms JSON)[:8]         # [10,11,12] typically
types_sig  = SHA256(sorted expected_types)[:8]     # "none" if empty
cache_id   = SHA256(query + region_sig + zoom_sig + types_sig)[:16]
```

### 6.2 Endpoints
- `POST /api/scraper/cache/check` - check before job creation
- `GET /api/scraper/cache/download/{cache_id}` - download cached CSV
- `POST /api/scraper/cache/subset-count` - count results in subset
- `GET /api/scraper/jobs/{id}/resume-info` - resume eligibility
- `POST /api/scraper/jobs/{id}/resume` - resume job

### 6.3 Current Cached Data (12 entries)
- 3 manually populated via `populate_cache.py` (dental clinic, dentist, elementary school)
- 9 from completed jobs with the new system
- All are partial results (since stopped jobs don't fully complete)

### 6.4 Cache Trigger Point
**BUG (from CRITICAL_ISSUES_ANALYSIS.md):** Cache check is NOT integrated into `/api/scraper/jobs` POST endpoint. The endpoint exists but frontend does NOT call it before creating a new job. **Implementation incomplete.**

---

## 7. Resume Capability

### 7.1 How It Works
1. During crawl, on each `task_done` event, `write_task_checkpoint()` records:
   - `(job_id, center_name, center_state, zoom, completed_at, result_count)`
2. On resume, `get_resume_job()` builds `all_tasks = centers × zooms` and filters out completed ones
3. New job created with `parent_job_id` linking back to original
4. Previous CSV is copied to new job's output path
5. `_run_job_with_tasks` only processes pending tasks

### 7.2 Limitation (per docs)
> "Resume Capability: Requires new jobs to create checkpoints (existing jobs have 0 checkpoints)"

The 3 historical stopped jobs (`e33b3df7`, `dd8573c5`, `2caa63b0`) have **0 checkpoints** because they were stopped before this feature was added. They are cacheable but not resumable. Other stopped jobs (e.g., `a3ba2136`, `6aeb6a4c`) similarly have 0 checkpoints.

### 7.3 Resume Endpoint Allows Statuses
`cancelled`, `abandoned`, `failed` (note: NOT `stopped` in the validation)

---

## 8. Frontend (index.html, 3053 lines)

### 8.1 Key Functions
- `checkAuth()` - 10s timeout (fixed)
- `loadSearchOptions()` - dropdowns
- `checkScraperCache(payload)` - calls /api/scraper/cache/check
- `showCacheModal(data)` - cache hit UI
- `startScraperJobInternal(payload)` - creates job
- `startScraperProgressStream(jobId)` - SSE consumer
- `loadScraperJobs()` / `filterScraperJobs()` - jobs list

### 8.2 Job Card Statuses (rendered)
- `done` - download button
- `failed` - download button (or error)
- `abandoned` - download button
- `cancelled` - download button
- `stopped` - download button (FIXED in last session)
- `running` - progress bar
- `queued` - waiting

### 8.3 Frontend Download Filename Format
`{query}_{centers}_centers_{results}_results_{status}.csv`
Example: `dental_clinic_2526_centers_46556_results_done.csv`

---

## 9. State of Jobs Database (2026-06-09)

| Status | Count |
|--------|-------|
| done | 299 |
| failed | 21 |
| abandoned | 25 |
| cancelled | 9 |
| stopped | 4 |
| running | 1 (ee8d3965, plumber, 22/88638 = 0.02%) |
| **Total** | **359** |

### 9.1 Job Observations
- Many "abandoned" jobs have `output_path` = NULL (no CSV)
- 3 "stopped" jobs DO have output_path set
- 1 currently running job has done_tasks=22, result_count=26 (very low)

### 9.2 116,287 Checkpoint Rows
These are from previous successful jobs. Shows the checkpoint system was working for at least some jobs.

---

## 10. Known Issues & Bug Catalog

### 10.1 RECENTLY FIXED BUGS (2026-06-09)

#### Bug 1: Stop Button Not Working - ✅ FIXED
**Original Problem:** Clicking "Stop" button didn't halt running jobs immediately.
**Root Cause:** Cancel endpoint called `store.set_failed()` but crawler checks for status=='cancelled'
**Fix Applied:** Changed `store.set_failed(job_id)` to `store.set_cancelled(job_id)` in `routes.py:936`
**Files Modified:** `backend/scraper/routes.py`
**Status:** ✅ VERIFIED - stop button now properly cancels jobs

#### Bug 2: Resume Job Showing Wrong Counts - ✅ FIXED
**Original Problem:** Resume jobs started counting from 0 instead of showing cumulative progress from parent job
**Root Cause:** SQL UPDATE failing due to wrong API (`store.get_db()` instead of `store.conn.execute()`)
**Fix Applied:** Corrected the API call to update initial counters in `routes.py:1132-1146`
**Files Modified:** `backend/scraper/routes.py`
**Status:** ✅ VERIFIED - resume now shows "28564 done, 112992 results" + new progress

#### Bug 3: Resume Jobs Running Extremely Slow - ✅ FIXED
**Original Problem:** Resume jobs were sequential instead of parallel processing
**Root Cause:** Called `run_crawl` once per task instead of once with all centers
**Fix Applied:** Consolidated to single `run_crawl` call with unique_centers list in `routes.py:1523-1568`
**Files Modified:** `backend/scraper/routes.py`
**Status:** ✅ VERIFIED - resume now processes tasks in parallel (8 workers)

#### Bug 4: Resume Endpoint Not Accepting "stopped" Jobs - ✅ FIXED
**Original Problem:** Resume endpoint allowed `cancelled | abandoned | failed` but NOT `stopped`
**Root Cause:** Status validation excluded "stopped" jobs from resuming
**Fix Applied:** Added "stopped" to allowed statuses in `routes.py:1006`
**Files Modified:** `backend/scraper/routes.py`
**Status:** ✅ VERIFIED - stopped jobs can now be resumed

### 10.2 PENDING ISSUES

#### Issue 1: "Refresh Results" Button Confusion
**Problem:** `retryScraperJob()` calls `/jobs/{id}/restart` which starts from scratch. Doesn't use cache.
**Recommendation:** Rename to "Re-scrape (Ignore Cache)" or implement smart refresh.
**Status:** Not yet addressed.

#### Issue 2: Cache Lookup NOT Integrated - ⚠️ INCOMPLETE
**Problem:** `/api/scraper/cache/check` endpoint exists, but `/api/scraper/jobs` POST does NOT call it.
**Code location:** `routes.py:649-735` (start_job) does not check cache.
**Status:** Endpoint exists, frontend has `checkScraperCache()` function, but NOT wired into job creation flow.

#### Issue 3: Disk Space Critical - 🚨 ACTIVE
- `/var/www/lead-generation-platform` is at **97%** (5.9GB free)
- `/mnt/disk` is at 31% (323GB free) ← cache is stored here
- Repeated alerts every 15 minutes in `alerts.log`
- Cleanup commands not being run

#### Issue 4: 8 Abandoned Jobs with NULL output_path
- `b86a5995`, `ff6d7ff8`, `a0b0103c`, `af257469`, `c6b4311b` (partial), `75e8ba41`, `ded8a72b`, others
- These can't be downloaded because no CSV was created

#### Issue 5: Many "abandoned" jobs from same query ("plumber", "local SEO agency", etc.)
- Suggests users repeatedly running same searches
- The cache system would help with this — but cache lookup isn't integrated (Issue 2)

#### Issue 6: Running Job is Stalled
- `ee8d3965` is "running" but has only 22/88638 tasks (0.02%) and 26 results
- May be hung or extremely slow

### 10.3 Architectural / Code Smell Issues

#### Issue 7: Frontend "Refresh Results" Workflow
The button labeled "Refresh Results" is actually a full re-scrape. The semantics are wrong.

#### Issue 8: Cache Lookup Logic in Frontend
`checkScraperCache()` exists (line 1809) but is it actually called in `startScraperJob` flow? Need to verify.

---

## 11. Database Storage Health

| Item | Size |
|------|------|
| `backend/data/jobs.db` | (WAL 109MB - large) |
| `backend/data/outputs/` | 3.3GB (cached CSVs) |
| `/mnt/disk/lead-generation-platform` | 8KB (cache dir not populated properly) |

**Discrepancy:** The cache directory is supposed to be at `/mnt/disk/lead-generation-platform/cache/` but is nearly empty (8KB). The 3.3GB of outputs are in the main data directory, not in the cache directory. **Cache may not be using the dedicated partition.**

---

## 12. Environment & Configuration

| Variable | Purpose |
|----------|---------|
| `JWT_SECRET` | Auth signing |
| `SCRAPER_TECH_KEY` | Google Maps scraper |
| `BLITZ_API_KEY` | LinkedIn enrichment |
| `CONTACTS_API_TOKEN` | Contacts API |
| `BETTER_ENRICH_API_KEY` | BetterEnrich V3 |
| `PROSPEO_API_KEY` | Prospeo enrichment |
| `WIZLEADS_API_KEY` | Wizleads (new) |
| `ALLOWED_ORIGINS` | CORS |
| `SMTP_*` | Email notifications |
| `SCRAPER_CONCURRENCY` | (Proposed in OPTIMIZATION_AND_CACHING_PLAN.md) |

---

## 13. Recent Changes Timeline (2026-06-04 to 2026-06-09)

| Date | Commit | Change |
|------|--------|--------|
| 2026-06-09 | - | ✅ STOP BUTTON BUG FIX - Changed set_failed to set_cancelled |
| 2026-06-09 | - | ✅ RESUME COUNTS BUG FIX - Fixed SQL UPDATE to show cumulative counts |
| 2026-06-09 | - | ✅ PARALLEL RESUME BUG FIX - Consolidated run_crawl to single call |
| 2026-06-09 | - | ✅ STOPPED STATUS FIX - Added "stopped" to resume allowed statuses |
| 2026-06-09 | - | Sync Status Failed (smartlead-daily-inboxes - separate project?) |
| 2026-06-08 | - | Disk space critical alerts |
| 2026-06-07 | `f62a52c` | ✅ Comprehensive caching system + auth fixes (f62a52c) |
| 2026-06-07 | `d3b9558` | GitHub commit summary for caching |
| 2026-06-07 | `b925c42` | Better Enrich V3 + LinkedIn URL support |
| 2026-06-07 | `07b527c` | Better Enrich V3 upgrade |
| 2026-06-07 | `bcf9b46` | Postcode scraping expansion + city search |
| 2026-06-06 | - | Frontend tier selector for US coverage |
| 2026-06-06 | - | Caching strategy plan created |
| 2026-06-06 | - | Critical issues analysis |
| 2026-06-06 | - | Pause/resume brainstorming |

---

## 14. Top 3 Bugs to Fix Immediately

Based on analysis:

### Priority 1: Wire Cache Check into Job Creation (Issue 2 in Pending)
- Cache endpoint exists, frontend has the function, but the flow doesn't connect
- Would dramatically reduce duplicate API calls
- **Effort:** ~30 minutes
- **File to modify:** `backend/scraper/routes.py:649-735` (start_job)

### Priority 2: Investigate Running Job Stall (Issue 6 in Pending)
- `ee8d3965` running for unknown time with 22/88638 done
- **Effort:** 10 minutes
- **Action:** Check service logs, see if it's actually progressing

### Priority 3: Disk Space Cleanup (Issue 3 in Pending)
- 97% full on root partition
- Old uploaded CSVs and outputs can be cleaned
- **Effort:** 5 minutes
- **Action:** Run cleanup commands

---

## 15. File Quick-Reference for Common Tasks

| Want to... | Look in |
|------------|---------|
| Add a new country | `backend/scraper/centers.py` (COUNTRY_FILES, COUNTRY_NAMES) + create CSV in `data/` |
| Change cache expiry | `backend/shared/db.py:28` (CACHE_EXPIRY_DAYS = 90) |
| Modify tier definitions | `backend/scraper/centers.py` - look for `get_all_mode_tiers()` |
| Add new API integration | Create client in `backend/enrichment/`, add to `providers.py` |
| Debug cache issues | `backend/scraper/cache.py` + `backend/scraper/routes.py:474-630` |
| Debug resume issues | `backend/scraper/routes.py:953-1150` + `backend/shared/job_store_base.py:426-475` |
| Update frontend | `frontend/index.html` (single file, 3053 lines) |

---

## 16. Open Questions / Needs Verification

1. **Cache directory:** Why is `/mnt/disk/lead-generation-platform/cache/` only 8KB when the system has 3.3GB of cached results in `data/outputs/`? Are cached results being stored in the wrong place?
2. **Checkpoint writes:** The `crawler.py` was supposed to emit `task_done` events with `center_name`, `center_state`, `zoom`. Are these events being emitted? Need to verify.
3. **Cache flow:** Is `checkScraperCache()` actually called from frontend's `startScraperJob()` function? Need to trace.
4. **Resume for stopped jobs:** Why was `stopped` excluded from the resumable statuses? `cancelled` and `abandoned` are included.

---

## 17. Next Steps

Ready for user's next task. The above knowledge graph is comprehensive and includes:
- ✅ Architecture (code map, components)
- ✅ Geographic expansion (all 18+ countries)
- ✅ Caching system (90-day, all endpoints, signature generation)
- ✅ Resume capability (checkpoint table, flow)
- ✅ All 10 known issues identified
- ✅ File quick-reference
- ✅ Prioritized bug list

**Awaiting next task instruction.**
