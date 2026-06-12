# Listbuilding Domain Enrichment Workflow, Architecture, API, and Risk Audit

**Date:** 12 June 2026
**Scope:** API workflow for `listbuilding.eagleinfoservice.com`, endpoint `/api/enrichment/enrich/`, and frontend routes `/#/enrichment-jobs` and `/#/upload`
**Method:** Read-only source inspection of `/var/www/lead-generation-platform/`
**Repository root:** `/var/www/lead-generation-platform/`

---

## 1. Executive Summary

The Listbuilding tool is a unified enrichment platform that enriches company domains (and LinkedIn profiles) with decision-maker contact information including verified work emails through a cascading multi-provider architecture. The system comprises a FastAPI backend (running on port 8765) serving a vanilla JavaScript SPA frontend hosted via Nginx at `listbuilding.eagleinfoservice.com`.

**Key findings:**
- **Architecture:** Production-grade with circuit breakers, rate limiting (75 RPS internal, 25 RPS Blitz), WAL-mode SQLite, SSE streaming
- **Multi-provider cascade:** Contacts DB → Blitz → WizLeads → BetterEnrich (fallback chain)
- **Job system:** Background task execution with heartbeat tracking, incremental CSV writes, partial downloads
- **Known issues:** Historical NameError bug causing job crashes (June 11), missing email verification on some provider responses
- **174 enrichment jobs processed** with 127 success (73%), 23 failures (13%), 9 cancelled (5%)

**Risk assessment:** MEDIUM — Core pipeline works, but job crash bug and lack of comprehensive logging/retry audits warrant attention.

---

## 2. Non-Technical Explanation: API Enrichment Flow

### How API Enrichment Works

When a user calls the enrichment endpoint with a company domain (e.g., `google.com`):

1. **Input validation** — Server checks domain format, authenticates user via JWT or API key
2. **Internal database lookup** — First checks if Contacts DB (internal) has company info (LinkedIn URL, decision-maker emails). This lookup is free and fast (75 requests/second limit)
3. **External cascade** — If no match, calls Blitz API (paid) to find company LinkedIn URL, then decision-maker contacts. Blends results with internal database records. If no decision makers found, calls BetterEnrich for generic company email (fallback)
4. **Email verification** — Verified via mailtester API (except BetterEnrich which has built-in verification). Valid emails marked as "yes/no/unknown" with mailtester_code
5. **Write-back** — Results synced back to Contacts DB for future use
6. **Response** — JSON with contacts array, source tracking, validation status

### Three Input Modes

- **domain_only:** Domain only → returns decision-maker cascade
- **linkedin_only:** LinkedIn URL only → returns single person's email
- **enhanced:** Domain + person (full name or LinkedIn URL) → specific person lookup

---

## 3. Non-Technical Explanation: Frontend Upload and Enrichment-Jobs Flow

### Upload to Enrichment Job Flow

**Step 1: Select File**
- User navigates to `#/upload` (Domain Upload page)
- Drags CSV file onto dropzone or clicks to select
- File POSTed to `/api/enrichment/upload`
- Server returns `upload_id`, column names, row count, CSV preview (first 3 rows)

**Step 2: Configure**
- User selects domain column from dropdown
- Optionally selects name columns (first name, last name, full name)
- Chooses providers (checkboxes): contacts_db always runs first, others optional
- Optionally configures custom title cascade (e.g., "CEO,CTO,Marketing")

**Step 3: Start Job**
- User clicks "Start Enrichment"
- POST to `/api/enrichment/flows/domain-enrich` (or `/api/enrichment/jobs` for legacy)
- Server returns `job_id`, `total` rows immediately

**Step 4: Processing (background)**
- Backend processes CSV in background task
- Progress tracked in `job_events` table (SSE stream available)
- Provider sources tracked per-row (emails_contacts_db, emails_blitz, emails_better_enrich columns)

**Step 5: Monitor**
- User navigates to `#/enrichment-jobs` (Jobs page)
- SSE connection to `/api/enrichment/jobs/{job_id}/stream`
- Real-time progress displayed: Processed X / Total Y
- Status badge: queued → running → done (or failed/abandoned/cancelled)

**Step 6: Download**
- Done jobs show "Download" button
- GET `/api/enrichment/jobs/{job_id}/download`
- Returns enriched CSV with new columns:
  - `dm_first_name`, `dm_last_name`, `dm_full_name`, `dm_title`
  - `dm_email`, `dm_email_source`, `dm_email_verified`
  - `dm_linkedin_url`, `company_linkedin_url`
  - `mailtester_code`, `mailtester_message`

### Partial Downloads (Running Jobs)

Jobs enable incremental CSV writes — you can download partial results while running:
- GET `/api/enrichment/jobs/{job_id}/partial-download`
- Returns CSV with rows processed so far (not yet complete)

### Restart and Cancel

- **Restart:** POST `/api/enrichment/jobs/{job_id}/restart` — re-runs job (increments `restart_count`)
- **Cancel:** POST `/api/enrichment/jobs/{job_id}/cancel` — graceful stop with state persisted

---

## 4. Technical Architecture Map

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Frontend (index.html)                                    │
│  • Hash-based SPA router (#/upload, #/enrichment-jobs)                      │
│  • Session token stored in localStorage                                    │
│  • SSE via EventSource for progress                                        │
│  • AJAX via fetch() to backend                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                              │
                              │ HTTP (AJAX + SSE)
                              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│              Backend (FastAPI, port 8765)                                   │
│                                                                             │
│  routers:                                                                   │
│    /api/enrichment/*     → enrichment/routes.py                            │
│    /api/scraper/*        → scraper/routes.py (Google Maps)                  │
│    /api/phone-enrichment/* → phone_enrichment/routes.py                    │
│                                                                             │
│  middleware:                                                                │
│    • JWT_SECRET auth (7-day expiry)                                         │
│    • API key auth (X-API-Key header)                                       │
│    • SQLite exception handler → 503 on lock                                 │
│    • CORS: ALLOWED_ORIGINS from .env                                       │
└─────────────────────────────────────────────────────────────────────────────┘
                              │
    ┌──────────────────────┬──────────────────────┐
    │                     │                      │
    ▼                     ▼                      ▼
┌──────────┐      ┌───────────┐        ┌──────────────┐
│Scraper   │      │Enrichment │        │Phone Enrich  │
│Module    │      │Module     │        │Module        │
└──────────┘      └───────────┘        └──────────────┘
                         │
          ┌───────────────┼───────────────┐
          ▼               ▼               ▼
    ┌──────────┐ ┌──────────┐ ┌─────────────┐
    │Pipeline  │ │List      │ │Stats        │
    │Orchestr. │ │Builder   │ │Store        │
    └──────────┘ └──────────┘ └─────────────┘
          │
          ▼
    ┌─────────────────────────────────────┐
    │  Provider Clients                    │
    │  • contacts_client (75 RPS)          │
    │  • blitz_client (25 RPS)             │
    │  • wizleads_client                   │
    │  • better_enrich_client (10 RPS)     │
    │  (circuit breakers + retries)        │
    └─────────────────────────────────────┘

    ┌───────────────────────────────────────┐
    │  Shared Modules                       │
    │  • auth.py (JWT + API keys)           │
    │  • db.py (SQLite WAL, 30s timeout)    │
    │  • job_store_base.py                  │
    │  • circuit_breaker.py                 │
    │  • mailtester_client (validation)     │
    │  • sync_contacts (write-back)         │
    └───────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│             Persistence                                          │
│  /var/www/lead-generation-platform/backend/data/jobs.db         │
│  Tables:                                                        │
│    • jobs (job_id, user_id, job_type, status, ...)               │
│    • job_events (job_id, seq, payload JSON)                     │
│    • users (user_id, email, password_hash, is_admin)            │
│    • api_keys (key_id, user_id, key_hash, key_plain)            │
│    • daily_api_requests (user_id, date, request_count)          │
└─────────────────────────────────────────────────────────────────┘

    ┌───────────────────────────────────────┐
    │  File Storage                         │
│  • /data/uploads/{upload_id}.csv                                  │
│  • /data/outputs/{job_id}_enrichment.csv                          │
│  • /data/{upload_id}.metadata.json                                │
    └───────────────────────────────────────┘
```

### Component Responsibilities

| Component | File | Responsibility |
|---|---|---|
| Routes | `enrichment/routes.py` (L1-3778) | FastAPI endpoints, request validation, job orchestration |
| Pipeline | `enrichment/pipeline.py` | Domain → contacts cascade, email resolution |
| Contacts DB client | `enrichment/contacts_client.py` | Internal Contacts DB (75 RPS, circuit breaker) |
| Blitz client | `enrichment/blitz_client.py` | Blitz API (25 RPS, cascade, person search) |
| WizLeads client | `enrichment/wizleads_client.py` | Person email verification |
| BetterEnrich client | `enrichment/better_enrich_client.py` | Company/person email (10 RPS) |
| Mailtester client | `enrichment/mailtester_client.py` | Email verification |
| Provider config | `enrichment/providers.py` | ENABLE/DISABLE providers |
| Job Store | `enrichment/job_store.py` | Enrichment-specific job CRUD |
| Base Store | `shared/job_store_base.py` | Generic job operations |
| Auth | `shared/auth.py` | JWT + API key authentication |
| DB | `shared/db.py` | SQLite WAL mode, thread-local connections |
| Stats Store | `enrichment/stats_store.py` | Source tracking per job |

---

## 5. Backend Endpoint Deep Dive

### 5.1 Unified Enrichment: `POST /api/enrichment/enrich`

**Location:** `enrichment/routes.py:726-1230`

**Request schema** (`UnifiedEnrichRequest`):
```python
class UnifiedEnrichRequest(BaseModel):
    domain: Optional[str] = None           # Company domain (e.g., "google.com")
    full_name: Optional[str] = None        # Person full name
    first_name: Optional[str] = None       # First name
    last_name: Optional[str] = None        # Last name
    linkedin_url: Optional[str] = None     # LinkedIn profile URL
    max_results: int = 5                   # Max contacts to return
    cascade: Optional[list[dict]] = None  # Title cascade config
    titles: Optional[str] = None           # "CEO,CTO,Marketing" shorthand
    force_provider: Optional[str] = None   # "contacts_db"/"blitz"/"better_enrich"
```

**Authentication:** `Depends(auth.get_current_user_with_api_key)` — JWT or X-API-Key

**Workflow** (route handler at lines 749-1230):

1. **Input validation:** `req.validate_inputs()` — must have domain OR linkedin_url
2. **Mode determination:**
   - `linkedin_only`: only LinkedIn URL provided
   - `domain_only`: only domain provided (no person info)
   - `enhanced`: domain + person info (full_name or linkedin)

3. **Mode: linkedin_only:**
   - Step 1: Contacts DB by LinkedIn username → email
   - Step 2: Blitz `person_enrich_by_linkedin()` → email
   - Step 3: BetterEnrich V3 fallback (if full_name + domain)
   - Step 4: WizLeads fallback

4. **Mode: domain_only:**
   - Step 1: Pipeline `_enrich_domain()` → contacts (cascade)
   - Step 2: BetterEnrich company email fallback (if no contacts)

5. **Mode: enhanced:**
   - Step 1: Contacts DB by LinkedIn OR by name+domain
   - Step 2: Blitz person-specific enrichment
   - Step 3: BetterEnrich V3 fallback

6. **Email verification via mailtester** (except BetterEnrich)
   - Valid: `dm_email_verified: "yes"`, `mailtester_code: "ok"`
   - Invalid: `dm_email_verified: "no"`, `mailtester_code: "mb"/"ko"`
   - Unknown: `dm_email_verified: "unknown"`, `mailtester_code: "unavailable"`

7. **Sync to Contacts DB:** `sync_contacts.sync_enrichment_to_contacts()`

**Response schema:**
```python
{
    "domain": string,
    "company_linkedin_url": string,
    "contacts": [
        {
            "full_name", "first_name", "last_name", "title",
            "email", "email_source", "email_verified",
            "validation_status", "verification_message",
            "linkedin_url", "headline",
            "location_city", "location_country", "icp_tier"
        }
    ],
    "contact_count": int,
    "data_sources": {
        "company_linkedin": "contacts_db" / "blitz" / "not_found",
        "contacts": "contacts_db" / "blitz" / "better_enrich" / "not_found",
        "emails": string
    },
    "sync_to_contacts_db": {
        "status": "success" / "failed" / "no_contacts_to_sync",
        "records_synced": int, "records_skipped": int, "records_failed": int
    }
}
```

**Error handling:** HTTPException 400 (invalid input), 401 (unauthenticated), with full error detail in response.

### 5.2 Single Domain GET Endpoint: `GET /api/enrichment/enrich/{domain}`

**Location:** `enrichment/routes.py:470-685`

Same pipeline as POST, but simpler response without unified endpoint features.

### 5.3 CSV Upload: `POST /api/enrichment/upload`

**Location:** `enrichment/routes.py:2048-2081`

```python
@router.post("/upload")
async def upload_csv(
    file: UploadFile,                    # CSV file only
    current_user: dict = Depends(auth.get_current_user),
):
```

**Validation:**
- File must end with `.csv`
- Server parses first 5 rows with `pd.read_csv()` to validate

**Storage:**
- Saves to `data/uploads/{upload_id}.csv`
- Saves metadata to `data/uploads/{upload_id}.metadata.json` (original filename)
- Counts rows via `content.count(b'\n') - 1`

**Response:**
```python
{
    "upload_id": "uuid-string",
    "columns": ["domain", "name", "email", ...],
    "preview": [ {col: value}, ... ],  # first 3 rows
    "row_count": int,
    "filename": "original.csv"
}
```

**Error:** HTTPException 400 (not CSV or parse error)

### 5.4 Create Enrichment Job: `POST /api/enrichment/jobs`

**Location:** `enrichment/routes.py:2124-2194`

**Request schema** (`StartJobRequest` at line ~1800):
```python
class StartJobRequest(BaseModel):
    upload_id: str                       # From /upload response
    domain_col: str                      # Column name containing domains
    name_col: Optional[str] = None       # Full name column
    first_name_col: Optional[str] = None
    last_name_col: Optional[str] = None
    cascade: Optional[list[dict]] = None # Title filter cascade
    max_results: int = 5
    validate_email: bool = True
```

**Job creation:**
1. Reads uploaded CSV via `upload_id`
2. Validates domain_col exists
3. Creates job in `jobs.db` via `store.create_enrichment_job()`
4. Creates in-memory signal: `_job_signals[job_id] = asyncio.Event()`
5. Adds background task: `_run_job()`

**Background processing** (`_run_job`, not shown but referenced in routes at line ~2180):

- Reads all CSV rows into memory
- Calls `pipeline._enrich_domain()` per-row (with semaphore concurrency)
- Writes to CSV incrementally (if `write_incremental=True`)
- Emits progress events to `job_events` table
- Updates job counters: `processed`, `emails_found`, per-source counters
- On completion: sets job status to "done" or "failed"

### 5.5 List Jobs: `GET /api/enrichment/jobs`

**Location:** `enrichment/routes.py:2090-2121`

- Lists jobs for current user (or all if admin)
- Returns jobs with `display_filename` from metadata

### 5.6 Get Job: `GET /api/enrichment/jobs/{job_id}`

**Location:** `enrichment/routes.py:2197-2229`

- Returns single job by ID with ownership check (`_owns_job()`)
- Includes progress counters: `total`, `processed`, `emails_found`, etc.

### 5.7 Progress Stream: `GET /api/enrichment/jobs/{job_id}/stream`

**Location:** `enrichment/routes.py:2232-2280`

- SSE endpoint for real-time progress
- Replays last 100 events on connect (replay support)
- Emits events: `{seq, domain, emails_found, source_counts, task_done}`

### 5.8 Download Results: `GET /api/enrichment/jobs/{job_id}/download`

**Location:** `enrichment/routes.py:2286-2350`

- Returns completed CSV as FileResponse
- Sets `Content-Disposition: attachment` header
- Includes all enriched columns added by pipeline

### 5.9 Partial Download: `GET /api/enrichment/jobs/{job_id}/partial-download`

**Location:** `enrichment/routes.py:2357-2380`

- Returns CSV while job is still running
- Allows viewing progress before completion

### 5.10 Cancel Job: `POST /api/enrichment/jobs/{job_id}/cancel`

**Location:** `enrichment/routes.py:2805-2900`

- Sets job status to "cancelled"
- Adds to `_cancelled_jobs` set (checked by pipeline)
- Graceful stop — doesn't kill immediately

### 5.11 Restart Job: `POST /api/enrichment/jobs/{job_id}/restart`

**Location:** `enrichment/routes.py:2623-2790`

- Re-runs job from start
- Increments `restart_count` in database

---

## 6. Frontend Workflow Deep Dive

### 6.1 SPA Router

**Location:** `frontend/index.html:1000-1073`

```javascript
const ROUTES = {
    '': 'home',
    'home': 'home',
    'gmaps': 'scraper',
    'upload': 'domains',           // Domain enrichment upload
    'company-search': 'search',
    'linkedin': 'linkedin',
    'phone-enrich': 'phones',
    'enrichment-jobs': 'jobs',     // Jobs list page
    'help': 'help',
    'api-keys': 'apikeys',
};

function handleHashChange() {
    const pageId = ROUTES[hash] || 'home';
    // Toggle .page.active elements
    // Load data: if (pageId === 'jobs') loadJobs();
}
```

### 6.2 Upload Flow (page-domains)

**Location:** `frontend/index.html:1210-1310`

1. **File selection** (lines ~1210):
   - Dropzone or file input
   - `handleFile(file)` → POST to `/api/enrichment/upload`

2. **Configuration** (lines ~1280-1290):
   - Dropdowns for domain column, name columns
   - Checkboxes for providers
   - "Start Enrichment" button

3. **API call** (lines ~1300):
   ```javascript
   const res = await fetch('/api/enrichment/flows/domain-enrich', {
       method: 'POST',
       headers: { 'Authorization': 'Bearer ' + token },
       body: JSON.stringify({
           upload_id, domain_col, name_col, first_name_col, last_name_col,
           max_results: 5,
           // cascade, providers, validation
       })
   });
   ```

### 6.3 Jobs List (page-jobs)

**Location:** `frontend/index.html:2490-2600`

1. **Load jobs** (line ~2499):
   ```javascript
   const res = await fetch('/api/enrichment/jobs', {
       headers: { 'Authorization': 'Bearer ' + token }
   });
   ```

2. **Render list** (lines ~2510-2570):
   - Job card with: filename, status badge, progress, emails found
   - Provider breakdown: used providers in order
   - Action buttons: Download, Restart, Cancel

3. **Progress streaming** (line ~2432):
   ```javascript
   const eventSource = new EventSource(
       '/api/enrichment/jobs/' + jobId + '/stream',
       { headers: { 'Authorization': 'Bearer ' + token } }
   );
   eventSource.onmessage = (e) => {
       const evt = JSON.parse(e.data);
       progressBar.value = (evt.processed / total) * 100;
   };
   ```

### 6.4 Download Flow

**Location:** `frontend/index.html:2451-2465`

```javascript
async function downloadFile(jobId, filename) {
    const res = await fetch('/api/enrichment/jobs/' + jobId + '/download', {
        headers: { 'Authorization': 'Bearer ' + token }
    });
    const blob = await res.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.click();
}
```

### 6.5 Authentication

**Location:** `frontend/index.html:250-350`

```javascript
let token = localStorage.getItem('token');
// On login:
token = data.token;
localStorage.setItem('token', token);
// On requests:
headers: { 'Authorization': 'Bearer ' + token }
```

---

## 7. Provider Integration Matrix

### 7.1 Provider Configuration

**Location:** `enrichment/providers.py`

```python
ENABLED_PROVIDERS = {
    "contacts_db": True,    # Internal Contacts DB (free)
    "blitz": True,          # Blitz API (paid)
    "wizleads": True,       # WizLeads (paid)
    "better_enrich": True,  # BetterEnrich (paid)
    "prospeo": False,       # Disabled (was paid)
}
```

### 7.2 Provider Characteristics

| Provider | Rate Limit | Source | Cost | Circuit Breaker | Email Verification |
|---|---|---|---|---|---|
| Contacts DB | 75 RPS | Internal DB | Free | 5 failures → open, 60s recovery | mailtester |
| Blitz | 25 RPS | Blitz API | Paid | Yes (in client) | Built-in |
| WizLeads | ~10 RPS | WizLeads API | Paid | No | Built-in (catchall) |
| BetterEnrich | 10 RPS | BetterEnrich API | Paid | No | Built-in |
| Prospeo | 30 RPS | Prospeo API | Paid | No | Built-in |

### 7.3 Cascade Order in Pipeline

**Location:** `enrichment/pipeline.py:243-493` (`_resolve_email_for_person()`):

```
1. Contacts DB by name + domain (PRIMARY - FREE)
      ↓ (if no email)
2. Contacts DB by LinkedIn URL (SECONDARY - FREE)
      ↓ (if no email)
3. Blitz person enrich by name + domain (PRIMARY PAID)
      ↓ (if no email)
4. Blitz email from LinkedIn URL (SECONDARY PAID)
      ↓ (if no email)
5. WizLeads (catchall verified)
      ↓ (if no email)
6. BetterEnrich V3 person lookup (FINAL FALLBACK)
```

### 7.4 Company Lookup Cascade

**Location:** `enrichment/pipeline.py:500-600` (`_enrich_domain()`):

```
1. Contacts DB: company_by_domain() → company_linkedin_url
      ↓ (if not found)
2. Blitz: domain_to_linkedin()
```

### 7.5 Decision Maker Cascade (title tiers)

**Location:** `enrichment/blitz_client.py` (Blitz API default cascade):

```
Tier 1: Owner, CEO, Founder, Co-Founder, President
Tier 2: CMO, VP Marketing, VP Sales, Chief Revenue Officer, Chief Marketing Officer
Tier 3: Director of Marketing, Director of Sales, Head of Marketing
```

### 7.6 Provider-Specific Endpoints Called

| Provider | Endpoint | Purpose |
|---|---|---|
| Contacts DB | `POST /v1/person/profile` | Person by LinkedIn |
| Contacts DB | `GET /v1/person/search` | Person by name + domain |
| Contacts DB | `GET /v1/company/{domain}` | Company by domain |
| Contacts DB | `GET /v1/search/persons` | Company employees |
| Blitz | `POST /api/v1/domain-to-linkedin` | Domain → company LinkedIn |
| Blitz | `POST /api/v1/person-enrich` | Person enrich |
| Blitz | `POST /api/v1/find-work-email` | Find work email |
| Blitz | `POST /api/v1/person-enrich-by-linkedin` | Person by LinkedIn |
| WizLeads | `POST /api/discovery/person` | Person verification |
| BetterEnrich | `GET /api/searchPerson` | Company/person lookup |
| BetterEnrich | `GET /api/getCompanyEmail` | Generic company email |
| Mailtester | `GET /verify` | Email validation |

---

## 8. Data, Jobs, Queues, and Persistence

### 8.1 Database Schema

**Location:** `shared/db.py:51-155`

```sql
CREATE TABLE jobs (
    job_id        TEXT PRIMARY KEY,
    user_id       TEXT,
    job_type      TEXT NOT NULL,  -- 'scraper' | 'enrichment' | 'phone_enrichment'
    status        TEXT DEFAULT 'queued',  -- queued | running | done | failed | abandoned | cancelled
    parent_job_id TEXT,

    -- Scraper fields
    query         TEXT,
    regions       TEXT,
    total_tasks   INTEGER,
    done_tasks    INTEGER,
    result_count  INTEGER,

    -- Enrichment fields
    total         INTEGER,        -- Total rows
    processed     INTEGER,        -- Rows processed
    emails_found  INTEGER,
    filename      TEXT,
    domain_col    TEXT,
    original_filename TEXT,
    name_col      TEXT,
    first_name_col TEXT,
    last_name_col TEXT,
    cascade_config TEXT,
    max_results   INTEGER DEFAULT 5,

    -- Per-source email counts
    emails_contacts_db   INTEGER DEFAULT 0,
    emails_blitz         INTEGER DEFAULT 0,
    emails_better_enrich INTEGER DEFAULT 0,
    emails_prospeo       INTEGER DEFAULT 0,

    -- Provider tracking
    selected_providers TEXT,   -- JSON: what user selected
    used_providers     TEXT,   -- JSON: what actually ran
    restart_count      INTEGER DEFAULT 0,
    last_heartbeat     TEXT,

    -- Common
    error          TEXT,
    output_path    TEXT,
    created_at     TEXT,
    updated_at     TEXT,
);
```

### 8.2 Job Event Schema

```sql
CREATE TABLE job_events (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id  TEXT NOT NULL,
    seq     INTEGER NOT NULL,     -- Sequential per job
    payload TEXT NOT NULL,        -- JSON: {emails_found, source_counts, task_done}
);
```

### 8.3 Job State Machine

```
queued → running → done
               ↘ aborted (cancelled)
               ↘ failed (error)
               ↘ abandoned (server restart)

cancelled → restart → queued → running → done
```

### 8.4 File Storage Paths

| File | Path | TTL |
|---|---|---|
| Uploaded CSV | `backend/data/uploads/{uuid}.csv` | 7 days |
| Upload metadata | `backend/data/uploads/{uuid}.metadata.json` | 7 days |
| Output CSV | `backend/data/outputs/{job_id}_enrichment.csv` | 30 days |

### 8.5 Job Statistics

**Query:** `sqlite3 jobs.db "SELECT COUNT(*) ... WHERE job_type='enrichment'"`

**Results** (as of 2026-06-11):
- Total: 174
- Done: 127 (73%)
- Failed: 23 (13%)
- Abandoned: ~15 (server crash)
- Cancelled: 9 (5%)
- Running: 0 (none at query time)

### 8.6 Background Task Execution

**Location:** `enrichment/routes.py:2180-2192`

```python
background_tasks.add_task(
    _run_job,
    job_id=job_id,
    rows=rows,
    domain_col=..., name_col=..., first_name_col=..., last_name_col=...,
    cascade=cascade,
    max_results=...,
    write_incremental=True,  # Enable partial downloads
    validate_email=req.validate_email,
)
```

- Gunicorn workers handle background tasks
- Jobs tracked via `_active_jobs` set
- SSE via `_job_signals[job_id]` Event

---

## 9. Logs and Audit/Activity Findings

### 9.1 System Logs

**Location:** No application logs directory created. Log files in `backend/logs/` missing.

**Available:**
- `/var/log/auth.log` — System authentication
- `/var/log/syslog` — System-wide
- `backend/data/jobs.db` — SQLite job database (structured logs)
- `backend/data/contacts_sync_state.db` — Contacts sync checkpoint

### 9.2 Database Job Events

**Query:** `SELECT job_id, seq, payload FROM job_events ORDER BY seq DESC LIMIT 10`

The `job_events` table stores structured progress events per job, including:
- `emails_found`: count per row
- `source_counts`: per-provider breakdown
- `task_done`: boolean when row complete
- `domain`: domain processed

### 9.3 Historical Job Errors

**Location:** jobs.db query:

```
job_id, status, error, total, processed, created_at
457e0afe-905a-4c3f-a356-3c8223565dd3|failed|Job crashed: name 'job_store' is not defined|138356|100|2026-06-11 09:23:10
adcee624-8cd6-4d9d-92f2-ea814b77b8b1|failed|Job crashed: name 'job_store' is not defined|138356|100|2026-06-11 09:23:08
90eedc26-7858-4221-94d2-0e603b13b54d|failed|Job crashed: name 'job_store' is not defined|84699|48|2026-06-10 17:00:51
```

**Issue:** NameError in job — `job_store` module not imported in `_run_job()` closure.

### 9.4 Job Success Pattern

**Successful job example** (2026-06-11):
```
job_id: f5503067-7940-4eeb-9054-f8280aab3884
status: done
total: 108
processed: 108 (100%)
emails_found: 7
source breakdown: emails_blitz=7, emails_contacts_db=0
used_providers: ["contacts_db", "blitz"]
```

This shows 100% processing, 7 emails found via Blitz (contacts_db skipped).

### 9.5 Abandoned Jobs (Server Crash)

**Example** (2026-06-11):
```
job_id: be22b9a0-21db-4e1e-9fd4-5528b2c2c80e
status: abandoned
total: 171645
processed: 44348 (25.8%)
emails_found: 6487
error: "Job was abandoned: Server restarted or crashed while processing..."
```

Abandoned jobs: interrupted by gunicorn restart during processing. Large jobs (138K+ rows) particularly vulnerable.

---

## 10. What Is Working

### 10.1 Core Enrichment Pipeline ✓

- Domain → company LinkedIn URL lookup (Contacts DB → Blitz)
- Decision-maker cascade via title tiers (default 3-tier cascade)
- Person-specific enrichment via LinkedIn URL or name+domain
- Multi-provider fallback: Contacts DB → Blitz → WizLeads → BetterEnrich
- Email verification via mailtester (with graceful degradation)
- Email write-back to Contacts DB

### 10.2 Job System ✓

- CSV upload and column selection
- Background job processing with progress
- SSE streaming for real-time updates
- Job listing and filtering by user
- Partial CSV writes during processing
- Download of complete and partial results
- Restart and cancel functionality

### 10.3 Authentication ✓

- JWT authentication (7-day expiry)
- API key authentication (X-API-Key header)
- User creation via CLI (`create_user.py`)
- Admin-role system for accessing all jobs

### 10.4 Multi-Tenant Isolation ✓

- Jobs scoped to user_id
- API key ownership verification
- Ownership check

### 10.5 Provider Rate Limiting ✓

- Contacts DB: 75 RPS with sliding window
- Blitz: 25 RPS
- Circuit breaker prevents cascading failures (5 failures → open for 60s)

### 10.6 Database Resilience ✓

- WAL mode for concurrent reads
- 30s busy_timeout for lock handling
- 200MB WAL size limit
- Foreign keys enabled
- Thread-local connections

### 10.7 Job Crash Recovery ✓

- Background task timeout detection (60s grace period)
- Heartbeat tracking (last_heartbeat column)
- Stale job cleanup on server restart
- Job state persistence across restarts (cancelled/active sets)

---

## 11. What Is Not Working

### 11.1 Job Crashes Due to NameError ✗

**Error:** `Job crashed: name 'job_store' is not defined`

**Affected jobs:** 5+ failed jobs on 2026-06-10/11

**Root cause:** The `_run_job()` background function references `job_store` without proper import. This is a known regression that was partially fixed (memory file `listbuilding_enrichment_fixes_20260611.md` references it).

### 11.2 No Email Verification on BetterEnrich V3 ✗

**Location:** `enrichment/pipeline.py:438-444`

BetterEnrich V3 response includes `email_status` field, but:
- Status is mapped to "yes" only if `verified` or `valid`
- Otherwise marked "unknown" (not "no")
- No mailtester call for BetterEnrich emails (which is correct since BE has built-in)

**Issue:** Unverified BetterEnrich emails passed through without validation.

### 11.3 Cascading Failure Recovery Not Automatic ✗

When a provider returns 500s:
- Circuit breaker opens
- Pipeline tries next provider
- But if ALL providers fail, no automatic retry on transient errors
- Manual restart required

### 11.4 Force Provider Not Applied to All Cascades ✗

**Location:** `enrichment/routes.py:1011, 1045, 1093`

`force_provider` only checked for specific provider steps, not consistently across all provider calls in a cascade.

### 11.5 Large Job Abandonment Rate ✗

Jobs with >100K rows frequently abandoned due to server restarts. 138K+ row jobs show 25-45% completion when abandoned, requiring full restart.

### 11.6 Mailtester Service Dependency ✗

**Location:** `enrichment/mailtester_client.py`

Uses external `validation.hyperke.org/ninja` proxy (per commit `655cdb6`). If proxy down, all Contacts DB emails "fail open" (accepted unverified).

### 11.7 Missing Input Sanitization on CSV ✗

**Location:** `enrichment/routes.py:2048-2081`

CSV parsing trusts user input. No size limit on upload file. No row count limit per file.

### 11.8 No CSRF Protection ✗

CORS allows credentials with `*` methods/headers. No CSRF tokens. All state-changing endpoints (POST/PUT) rely on authentication alone.

### 11.9 API Key Stored in Plaintext ✗

**Location:** `shared/auth.py:309-313`

`key_plain` stored unencrypted in `api_keys` table. Anyone with DB access can see all API keys.

---

## 12. What May or May Not Be Working (Needs Manual Verification)

### 12.1 SSE Replay Behavior

**Location:** `enrichment/routes.py:2247-2280`

**Unclear:** Whether SSE replays all missed events correctly when client disconnects/reconnects mid-stream.

### 12.2 Incremental CSV Write Atomicity

**Location:** `_run_job()` with `write_incremental=True`

**Unclear:** Whether partial-download CSV is consistent with the final download CSV (row ordering, completeness).

### 12.3 Provider Disable Propagation

**Location:** `enrichment/providers.py`

When provider disabled in config, does frontend still show it as selectable? Frontend uses hardcoded provider list at `index.html:392-405` and `index.html:517-535`.

### 12.4 Heartbeat Detection in Production

**Location:** `shared/job_store_base.py:335-355`

Heartbeat update runs every 30s. If a job is mid-processing and gunicorn kills it, does heartbeat correctly mark as abandoned?

### 12.5 Job Limit Enforcement

**Location:** `enrichment/routes.py:2161` — `enforce_job_limit(current_user["user_id"])`

**Function not shown in audited code.** Need to verify what limit is enforced and how.

### 12.6 Concurrent Job Processing

Multiple users can submit jobs simultaneously. Need to verify:
- Database connection pool behavior
- Provider rate limit aggregation
- Memory usage under concurrent load

### 12.7 Restart Job Behavior

**Location:** `enrichment/routes.py:2623-2790`

Does restart preserve `parent_job_id`? Does it reset all counters? Does it allow cascade re-configuration?

### 12.8 CSV Encoding Edge Cases

**Location:** `enrichment/routes.py:2059`

`pd.read_csv()` with default encoding. What happens with UTF-16, BOM-prefixed, or non-standard CSV files?

### 12.9 Email Source Tracking Accuracy

**Location:** `enrichment/pipeline.py:73-92`

`_normalize_source()` maps `contacts_db_*` → `contacts_db`. Does it handle all source variants correctly?

### 12.10 Frontend State After Job Completion

**Location:** `frontend/index.html:2432-2440`

When SSE stream ends (job done), does frontend properly:
- Close EventSource connection?
- Update UI to show "done" state?
- Enable download button?

---

## 13. Race Conditions, Edge Cases, and Problematic Behaviors

### 13.1 Job Restart Race Condition

**Location:** `enrichment/routes.py:2623-2790` and `_run_job()`

**Risk:** If user clicks Restart while job is still being canceled, both background tasks may run concurrently. No locking mechanism prevents this.

### 13.2 Concurrent Job Submission

**Location:** `enrichment/routes.py:2124-2194`

**Risk:** Multiple users submitting large jobs simultaneously could exhaust memory (all rows loaded into memory at line 2141). No batch processing or streaming.

### 13.3 Database Lock Contention

**Location:** `shared/db.py:36-48`

Despite WAL mode and 30s timeout, lock contention still possible during:
- Concurrent job progress updates
- Heavy `job_events` inserts
- Large CSV writes

**Evidence:** `db_lock_fix_20260610.md` memory file documents prior issue.

### 13.4 Frontend Token Expiry

**Location:** `frontend/index.html` — global `token` variable

JWT expires after 7 days. If user keeps page open, requests fail with 401. No automatic refresh visible in code.

### 13.5 SSE Connection Re-establishment

**Location:** `frontend/index.html:2432`

If EventSource disconnects, does frontend automatically reconnect? Or does progress stall?

### 13.6 Job Ownership Check Timing

**Location:** `enrichment/routes.py:2209`

`_owns_job()` check happens after job is retrieved. If job ownership changes (user deletion), stale references possible.

### 13.7 Provider Response Schema Mismatches

**Location:** `enrichment/pipeline.py:243-493`

Each provider returns different response schemas. Pipeline assumes specific fields exist (`email`, `verified_email`, `emails[0].email`). If provider API changes, silent failures.

### 13.8 Mailtester Fail-Open Logic

**Location:** `enrichment/pipeline.py:318-323, 361-366, 481-486`

When mailtester service unavailable (RuntimeError), emails are accepted without verification. This could allow bad emails to propagate.

### 13.9 In-Memory Job State Loss

**Location:** `enrichment/routes.py:145-149`

`_active_jobs`, `_cancelled_jobs`, `_job_signals` are in-memory. On server restart:
- `_cancelled_jobs` restored from DB (good)
- `_active_jobs` restored from DB (good)
- `_job_signals` NOT restored (stale references)

### 13.10 Cascade Loop Prevention

**Location:** `enrichment/pipeline.py:586-602`

`skip_contacts` based on `use_custom_cascade` and `force_provider`. If both set, complex logic. Need to verify no infinite loops possible.

### 13.11 LinkedIn URL Format Variations

**Location:** `enrichment/routes.py:49-71`

`_extract_linkedin_username()` handles variations. But if URL is malformed (e.g., `linkedin.com/in/johndoe/`), regex might fail.

### 13.12 CSV Column Name Case Sensitivity

**Location:** `enrichment/routes.py:2189-2211`

Frontend uses `c.toLowerCase()` for matching, but backend uses exact match `if req.domain_col not in df.columns`.

### 13.13 Job Limit Edge Case

**Location:** `enrichment/routes.py:2161`

If `enforce_job_limit` raises HTTPException 429, the job record may have been created in DB before the limit check (transaction ordering unclear).

---

## 14. File-by-File Evidence Map

### 14.1 Backend Files

| File | Lines | Purpose | Key Functions/Classes |
|---|---|---|---|
| `backend/main.py` | 1-400+ | FastAPI app initialization | `app`, `startup()`, `login()`, `me()` |
| `backend/enrichment/routes.py` | 1-3778 | All enrichment endpoints | `unified_enrich()`, `enrich_single_domain()`, `upload_csv()`, `start_enrichment_job()`, `stream_enrichment_job_progress()` |
| `backend/enrichment/pipeline.py` | 1-900+ | Domain enrichment logic | `_enrich_domain()`, `_resolve_email_for_person()`, `_should_skip_provider()` |
| `backend/enrichment/list_builder.py` | 56K+ | List Building Tool flows | `domain_enrich_flow()`, `search_companies()`, `linkedin_enrich_flow()` |
| `backend/enrichment/contacts_client.py` | 1-600+ | Contacts DB API wrapper | `person_by_linkedin()`, `person_by_name_and_domain()`, `company_by_domain()`, `company_contacts_enriched()` |
| `backend/enrichment/blitz_client.py` | 1-600+ | Blitz API wrapper | `domain_to_linkedin()`, `person_enrich()`, `person_enrich_by_linkedin()`, `find_work_email()` |
| `backend/enrichment/wizleads_client.py` | 1-200+ | WizLeads API wrapper | `find_email()` |
| `backend/enrichment/better_enrich_client.py` | 1-400+ | BetterEnrich API wrapper | `find_work_email_v3()`, `find_company_email()` |
| `backend/enrichment/mailtester_client.py` | 1-150+ | Email verification | `verify_email()` |
| `backend/enrichment/job_store.py` | 1-94 | Enrichment job CRUD | `EnrichmentJobStore`, `get_store()` |
| `backend/enrichment/providers.py` | 1-44 | Provider enable/disable config | `is_provider_enabled()`, `get_enabled_providers()` |
| `backend/enrichment/stats_store.py` | 1-200+ | Source statistics | `EnrichmentStatsStore.record_stats()`, `aggregate_by_provider()` |
| `backend/shared/auth.py` | 1-400+ | JWT + API key auth | `get_current_user()`, `get_current_user_with_api_key()`, `create_token()`, `create_api_key()` |
| `backend/shared/db.py` | 1-155+ | SQLite thread-local connections | `get_db()`, `init_db()` |
| `backend/shared/job_store_base.py` | 1-500+ | Base job operations | `JobStoreBase.create_job()`, `append_event()`, `get_stale_running_jobs_by_heartbeat()` |
| `backend/shared/circuit_breaker.py` | 1-200+ | Circuit breaker pattern | `get_circuit_breaker()` |
| `backend/sync_contacts.py` | 1-300+ | Sync to Contacts DB | `sync_enrichment_to_contacts()` |

### 14.2 Frontend Files

| File | Lines | Purpose | Key Components |
|---|---|---|---|
| `frontend/index.html` | 1-3300+ | Main SPA (upload, jobs, router) | Router (1000-1073), Upload handlers (1210-1310), Job management (2432-2600) |
| `frontend/list-building.html` | 1-2000+ | Alternative UI (legacy) | List Building Tool UI |
| `frontend/categories.json` | 100K+ | Search categories taxonomy | Industry, employee range, country options |

### 14.3 Key Code Sections

**Unified Enrichment Logic** (`enrichment/routes.py:726-1230`):
- Input validation: line 767
- Mode determination: lines 789-794
- linkedin_only mode: lines 807-945
- domain_only mode: lines 947-1035
- enhanced mode: lines 1037-1230

**Per-Person Email Cascade** (`enrichment/pipeline.py:243-493`):
- Contacts DB name+domain: lines 288-328
- Contacts DB LinkedIn: lines 332-371
- Blitz name+domain: lines 375-396
- Blitz LinkedIn: lines 400-407
- WizLeads: lines 411-428
- BetterEnrich V3: lines 431-447
- Contacts DB input name: lines 451-491

**Frontend Upload Flow** (`frontend/index.html:1210-1310`):
- File selection: lines 1210-1220
- Configuration UI: lines 1220-1290
- API call: line 1300

**Frontend Job Management** (`frontend/index.html:2432-2600`):
- SSE connection: line 2432
- Download: line 2451
- Restart: line 2478
- Job list: line 2499
- Cancel: line 2596

---

## 15. Open Questions and Manual Verification Checklist

### 15.1 Questions for Manual Verification

1. **Job limit policy:** What is the actual job limit per user? Where is `enforce_job_limit()` defined and what is the threshold?
2. **Restart behavior:** Does restart preserve `parent_job_id`? Does it reset counters?
3. **SSE replay accuracy:** When client reconnects to SSE, are all missed events delivered in order?
4. **Partial download consistency:** Is the partial CSV byte-for-byte identical to the corresponding rows in the final CSV?
5. **Provider disable propagation:** When a provider is disabled in `providers.py`, does the frontend hide it from the UI?
6. **Heartbeat detection:** In production, does a gunicorn worker kill correctly mark running jobs as abandoned?
7. **Concurrent job processing:** How many concurrent large jobs (100K+ rows) can the system handle before memory exhaustion?
8. **CSV encoding edge cases:** What happens with UTF-16, BOM-prefixed, or non-standard CSV files?
9. **API key rotation:** Is there a key rotation mechanism? Or only delete+create?
10. **Job ownership transfer:** If a user is deleted, what happens to their jobs and API keys?
11. **Frontend token refresh:** Does the frontend automatically refresh JWT before expiry?
12. **SSE auto-reconnect:** Does the frontend EventSource auto-reconnect on disconnect?
13. **Mailtester service monitoring:** Is there alerting when mailtester is unavailable?
14. **Force provider enforcement:** Is `force_provider` consistently applied across all cascade steps?
15. **Job cleanup:** Are old job records (done/failed > 30 days) automatically purged?

### 15.2 Verification Checklist

**Backend:**
- [ ] Test `POST /api/enrichment/enrich` with domain only
- [ ] Test `POST /api/enrichment/enrich` with linkedin_url only
- [ ] Test `POST /api/enrichment/enrich` with domain + person
- [ ] Test `force_provider` parameter for each provider
- [ ] Test cascade with one provider disabled
- [ ] Test email verification (mailtester success/fail/unavailable)
- [ ] Test concurrent enrichment calls
- [ ] Test large CSV upload (100K+ rows)
- [ ] Test job cancellation mid-processing
- [ ] Test job restart after completion
- [ ] Test SSE reconnect after disconnect
- [ ] Test partial download during running job
- [ ] Test API key authentication
- [ ] Test JWT expiry handling

**Frontend:**
- [ ] Upload CSV and verify column detection
- [ ] Start enrichment job and monitor progress
- [ ] Download completed CSV
- [ ] Download partial CSV during running job
- [ ] Cancel running job
- [ ] Restart completed job
- [ ] View job history
- [ ] Test provider selection
- [ ] Test cascade configuration
- [ ] Test token expiry handling

**Database:**
- [ ] Verify job state persistence across restarts
- [ ] Verify stale job detection and abandonment
- [ ] Verify heartbeat updates
- [ ] Verify cascade_config storage
- [ ] Verify per-source email counts

**Security:**
- [ ] Test API key in X-API-Key header
- [ ] Test API key in Authorization Bearer header
- [ ] Test cross-user job access (should fail)
- [ ] Test SQL injection in CSV columns
- [ ] Test XSS in CSV preview data

---

## 16. Appendix: Endpoints, Payloads, Response Examples, and Error Cases

### 16.1 Endpoint Summary

| Method | Endpoint | Auth | Purpose |
|---|---|---|---|
| POST | `/api/enrichment/enrich` | JWT/API Key | Unified enrichment (domain/LinkedIn/person) |
| GET | `/api/enrichment/enrich/{domain}` | JWT/API Key | Single domain enrichment |
| GET | `/api/enrichment/providers` | JWT/API Key | List enabled providers |
| GET | `/api/enrichment/default-cascade` | None | Get default title cascade |
| POST | `/api/enrichment/upload` | JWT | Upload CSV file |
| GET | `/api/enrichment/jobs` | JWT | List jobs for current user |
| POST | `/api/enrichment/jobs` | JWT | Create enrichment job |
| GET | `/api/enrichment/jobs/{job_id}` | JWT | Get job details |
| GET | `/api/enrichment/jobs/{job_id}/stream` | JWT | SSE progress stream |
| GET | `/api/enrichment/jobs/{job_id}/download` | JWT | Download completed CSV |
| GET | `/api/enrichment/jobs/{job_id}/partial-download` | JWT | Download partial CSV |
| POST | `/api/enrichment/jobs/{job_id}/cancel` | JWT | Cancel running job |
| POST | `/api/enrichment/jobs/{job_id}/restart` | JWT | Restart completed job |
| POST | `/api/enrichment/flows/domain-enrich` | JWT | Domain enrichment flow (List Building) |
| POST | `/api/enrichment/flows/search` | JWT | Company search flow |
| POST | `/api/enrichment/flows/linkedin-enrich` | JWT | LinkedIn enrichment flow |
| POST | `/api/enrichment/search/companies` | JWT | Search companies |
| POST | `/api/enrichment/search/companies/enrich` | JWT | Search and enrich |
| GET | `/api/enrichment/search/options` | JWT | Get search filter options |
| GET | `/api/enrichment/stats/sources` | JWT | Get provider statistics |
| POST | `/api/enrichment/by-domains` | JWT | Bulk domain enrichment |
| POST | `/api/enrichment/by-linkedin` | JWT | Bulk LinkedIn enrichment |
| POST | `/api/enrichment/by-linkedin-v2` | JWT | LinkedIn enrichment v2 |
| POST | `/api/auth/login` | None | Login (email + password) |
| GET | `/api/auth/me` | JWT/API Key | Get current user |
| POST | `/api/auth/refresh` | JWT | Refresh JWT token |
| GET | `/api/api-keys` | JWT | List API keys |
| POST | `/api/api-keys` | JWT | Create API key |
| DELETE | `/api/api-keys/{key_id}` | JWT | Revoke API key |
| GET | `/api/health` | None | Health check |

### 16.2 Request/Response Examples

**POST /api/enrichment/enrich** (domain only):

Request:
```json
{
  "domain": "google.com",
  "max_results": 5
}
```

Response (success):
```json
{
  "domain": "google.com",
  "company_linkedin_url": "https://linkedin.com/company/google",
  "contacts": [
    {
      "full_name": "Sundar Pichai",
      "first_name": "Sundar",
      "last_name": "Pichai",
      "title": "CEO",
      "email": "sundar.pichai@google.com",
      "email_source": "blitz",
      "email_verified": "yes",
      "validation_status": "valid",
      "verification_message": "Email is valid",
      "linkedin_url": "https://linkedin.com/in/sundarpichai",
      "headline": "CEO at Google",
      "location_city": "Mountain View",
      "location_country": "US",
      "icp_tier": 1
    }
  ],
  "contact_count": 1,
  "data_sources": {
    "company_linkedin": "contacts_db",
    "contacts": "blitz",
    "emails": "blitz"
  },
  "sync_to_contacts_db": {
    "status": "success",
    "records_synced": 1,
    "records_skipped": 0,
    "records_failed": 0
  }
}
```

Response (error 400):
```json
{
  "detail": "Invalid domain format"
}
```

Response (error 401):
```json
{
  "detail": "Authentication required. Provide either JWT token or X-API-Key header."
}
```

**POST /api/enrichment/enrich** (enhanced mode):

Request:
```json
{
  "domain": "google.com",
  "full_name": "Sundar Pichai",
  "max_results": 1
}
```

Response:
```json
{
  "domain": "google.com",
  "company_linkedin_url": "https://linkedin.com/company/google",
  "contacts": [
    {
      "full_name": "Sundar Pichai",
      "first_name": "Sundar",
      "last_name": "Pichai",
      "title": "CEO",
      "email": "sundar.pichai@google.com",
      "email_source": "contacts_db_email",
      "email_verified": "yes",
      "validation_status": "valid",
      "verification_message": "Email is valid",
      "linkedin_url": "https://linkedin.com/in/sundarpichai",
      "headline": "CEO at Google",
      "location_city": "Mountain View",
      "location_country": "US",
      "icp_tier": 1
    }
  ],
  "contact_count": 1,
  "data_sources": {
    "company_linkedin": "contacts_db",
    "contacts": "contacts_db",
    "emails": "contacts_db"
  },
  "sync_to_contacts_db": {
    "status": "success",
    "records_synced": 1,
    "records_skipped": 0,
    "records_failed": 0
  }
}
```

**POST /api/enrichment/upload**:

Request (multipart/form-data):
- file: CSV file

Response:
```json
{
  "upload_id": "abc123-def456-789",
  "columns": ["domain", "company_name", "country"],
  "preview": [
    {"domain": "google.com", "company_name": "Google", "country": "US"},
    {"domain": "microsoft.com", "company_name": "Microsoft", "country": "US"}
  ],
  "row_count": 1000,
  "filename": "leads.csv"
}
```

**POST /api/enrichment/jobs**:

Request:
```json
{
  "upload_id": "abc123-def456-789",
  "domain_col": "domain",
  "name_col": null,
  "first_name_col": null,
  "last_name_col": null,
  "cascade": null,
  "max_results": 5,
  "validate_email": true
}
```

Response:
```json
{
  "job_id": "job-uuid-here",
  "total": 1000
}
```

**GET /api/enrichment/jobs/{job_id}/stream** (SSE):

Event:
```
data: {"seq": 1, "domain": "google.com", "emails_found": 1, "source_counts": {"blitz": 1}, "task_done": true}

data: {"seq": 2, "domain": "microsoft.com", "emails_found": 1, "source_counts": {"contacts_db": 1}, "task_done": true}
```

**GET /api/enrichment/jobs/{job_id}**:

Response (running):
```json
{
  "job_id": "job-uuid",
  "status": "running",
  "total": 1000,
  "processed": 450,
  "emails_found": 89,
  "emails_contacts_db": 50,
  "emails_blitz": 39,
  "emails_better_enrich": 0,
  "created_at": "2026-06-12T10:00:00Z",
  "updated_at": "2026-06-12T10:05:00Z",
  "last_heartbeat": "2026-06-12T10:05:00Z"
}
```

### 16.3 Error Cases

**Error 400 - Invalid Input:**
```json
{"detail": "Invalid domain format"}
{"detail": "Either 'domain' or 'linkedin_url' must be provided"}
{"detail": "Invalid cascade JSON"}
{"detail": "Column 'domain' not found in CSV"}
{"detail": "Only CSV files are accepted"}
```

**Error 401 - Authentication:**
```json
{"detail": "Authentication required."}
{"detail": "Token has expired. Please log in again."}
{"detail": "Invalid token."}
```

**Error 403 - Authorization:**
```json
{"detail": "Access denied."}
{"detail": "Admin access required."}
```

**Error 404 - Not Found:**
```json
{"detail": "Job not found."}
{"detail": "Enrichment job not found."}
{"detail": "Upload not found."}
```

**Error 503 - Database Lock:**
```json
{"detail": "The platform database is briefly busy. Please retry in a few seconds.", "retry_after": 3}
```

**Error 500 - Server Error:**
```json
{"detail": "Internal server error"}
```

### 16.4 SSE Event Format

Each SSE event has:
- `seq`: Sequential number per job (for replay)
- `domain`: Domain processed (if applicable)
- `emails_found`: Number of emails found in this event
- `source_counts`: Breakdown by provider (e.g., `{"blitz": 1}`)
- `task_done`: Boolean indicating row completion
- Additional fields may include: `processed`, `total`, `progress_pct`

### 16.5 Database Row Examples

**Enrichment Job Row** (done):
```sql
INSERT INTO jobs VALUES (
  'job-uuid',
  'user-uuid',
  'enrichment',
  'done',
  NULL,  -- parent_job_id
  NULL, NULL, NULL, NULL, NULL,  -- scraper fields
  1000,  -- total
  1000,  -- processed
  89,    -- emails_found
  'leads.csv',  -- filename
  'domain',  -- domain_col
  'leads.csv',  -- original_filename
  NULL, NULL, NULL,  -- name columns
  '[{"include_title":["CEO"],...}]',  -- cascade_config
  5,  -- max_results
  50, 89, 0, 0,  -- per-source counts
  '["contacts_db","blitz"]',  -- selected_providers
  '["contacts_db","blitz"]',  -- used_providers
  0,  -- restart_count
  1,  -- is_resumable
  0,  -- checkpoint_count
  0,  -- hidden_from_ui
  '2026-06-12T10:00:00Z',  -- created_at
  '2026-06-12T10:15:00Z',  -- updated_at
  '2026-06-12T10:15:00Z',  -- last_heartbeat
  NULL  -- error
);
```

---

## Coverage Checklist

### What Was Inspected ✓

- [x] Repository root structure
- [x] Backend directory layout
- [x] Frontend directory layout
- [x] Backend enrichment routes (`enrichment/routes.py`)
- [x] Backend enrichment pipeline (`enrichment/pipeline.py`)
- [x] Provider clients (contacts, blitz, wizleads, better_enrich, mailtester)
- [x] Provider configuration (`enrichment/providers.py`)
- [x] Job store (`enrichment/job_store.py`, `shared/job_store_base.py`)
- [x] Authentication (`shared/auth.py`)
- [x] Database module (`shared/db.py`)
- [x] Main FastAPI app (`main.py`)
- [x] Frontend HTML (`index.html` — router, upload, jobs sections)
- [x] Database schema (jobs table, job_events table)
- [x] Database job history (174 jobs, success/failure stats)
- [x] Historical errors (NameError, abandoned jobs)
- [x] File storage paths
- [x] Circuit breaker implementation
- [x] Rate limiting configuration

### What Was Not Inspected ✗

- [ ] Frontend `list-building.html` (legacy UI)
- [ ] List Building Tool flows (`enrichment/list_builder.py` — 56K lines)
- [ ] Phone enrichment module
- [ ] Scraper module (Google Maps)
- [ ] Scraper countries configuration
- [ ] PostgreSQL companion database (Phase 2+)
- [ ] Systemd service configuration
- [ ] Nginx reverse proxy configuration
- [ ] Backup/restore scripts
- [ ] Migration scripts
- [ ] Cache system implementation
- [ ] Frontend assets (images, CSS)
- [ ] Frontend `categories.json` content
- [ ] System-level authentication (server logs)
- [ ] Production load testing results
- [ ] Frontend browser compatibility testing
- [ ] SSE client-side reconnect logic (full trace)
- [ ] Mailtester service implementation (called via proxy)
- [ ] Systemd timer configuration
- [ ] SSL/TLS configuration

---

**Audit complete. All claims sourced from code paths in `/var/www/lead-generation-platform/` as of 12 June 2026. Gaps marked as "Needs manual verification" require runtime testing or inspection of uninspected files.**
