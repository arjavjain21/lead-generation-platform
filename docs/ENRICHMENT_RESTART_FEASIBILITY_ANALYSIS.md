# Enrichment Job Restart Feature - Feasibility Analysis

## Executive Summary

**Question:** "Is it possible/feasible to have a restart failed job option? Are we keeping the CSV file temporarily for that?"

**Answer:** **YES, it's feasible**, and we're **already keeping the CSV files persistently**! However, we need to add a few database columns to support full restart functionality.

---

## Current State Analysis

### What We Already Have ✅

**1. CSV Files Are Kept Persistently**

**Location:** `/var/www/lead-generation-platform/backend/data/uploads/`

**Evidence:**
```bash
ls -lh backend/data/uploads/
# total 296M
# -rw-r--r-- 1 ubuntu ubuntu  12M Mar  9 12:27 01fe8be4-f696-4b95-b69d-5c0ea6c71d8d.csv
# -rw-r--r-- 1 ubuntu ubuntu  54M Mar  9 09:22 078e3029-95b7-4497-9c80-319d4a090932.csv
# -rw-r--r-- 1 ubuntu ubuntu 6.9M Mar 10 11:15 1c9cf1d9-cdcf-4cdb-b0fa-202680376227.csv
# ...
```

**Count:** 29 CSV files currently stored (total 296 MB)

**Retention:** **PERMANENT** - No cleanup mechanism exists

**File Naming:** `{upload_id}.csv` where upload_id is a UUID

**Metadata Stored Alongside:**
```json
{
  "original_filename": "uk-marketing-agency_bb9d4786.csv"
}
```

**2. Database Already Stores Key Configuration**

**Current Schema (`jobs` table):**
```sql
CREATE TABLE jobs (
    job_id          TEXT PRIMARY KEY,
    user_id         TEXT,
    job_type        TEXT,              -- 'scraper' | 'enrichment'
    status          TEXT,              -- queued | running | done | failed

    -- Enrichment-specific fields
    total           INTEGER,           -- Total rows to enrich
    processed       INTEGER,           -- Rows processed so far
    emails_found    INTEGER,           -- Total emails found
    filename        TEXT,              -- upload_id (points to CSV file)
    domain_col      TEXT,              -- Which column has the domain
    original_filename TEXT,            -- User-facing filename

    -- Common fields
    error           TEXT,
    output_path     TEXT,
    created_at      TEXT,
    updated_at      TEXT
);
```

**Evidence from Database:**
```
job_id: 6e8f3856-2856-4ff0-932e-6dbacd219764
filename: 1c9cf1d9-cdcf-4cdb-b0fa-202680376227  ← This is the upload_id!
original_filename: uk-marketing-agency_bb9d4786.csv
domain_col: website
status: failed
```

**What This Means:**
- ✅ We can find the CSV file using the `filename` column
- ✅ We know which column contains domains
- ✅ We know the original filename for display
- ✅ We have all the user's original data

---

## What's Missing for Full Restart ⚠️

### Missing Database Columns

**When Creating a Job, These Parameters Are Used:**

From `enrichment/routes.py` lines 152-208:

```python
class StartJobRequest(BaseModel):
    upload_id: str          # ✅ Stored in `filename` column
    domain_col: str         # ✅ Stored in `domain_col` column
    name_col: Optional[str]             # ❌ NOT stored
    first_name_col: Optional[str]       # ❌ NOT stored
    last_name_col: Optional[str]        # ❌ NOT stored
    cascade: Optional[list[dict]]       # ❌ NOT stored (ICP tier configuration)
    max_results: int = 5                # ❌ NOT stored (default: 5 decision makers per domain)
```

**Impact:**
- Can restart with domain enrichment only
- **Cannot** restart with name-based fallback (if original job used it)
- **Cannot** use the same ICP cascade configuration
- **Cannot** use the same `max_results` setting

**Example Scenario:**
```
Original Job:
- domain_col: "website"
- name_col: "company_name"
- cascade: [tier1: Owner/CEO, tier2: VP, tier3: Director]
- max_results: 10

Restarted Job (with current schema):
- domain_col: "website" ✅
- name_col: NULL ❌ (will skip name-based fallback)
- cascade: DEFAULT ❌ (will use default 3-tier cascade)
- max_results: 5 ❌ (will use default 5 instead of 10)
```

---

## Proposed Solution

### Option 1: Full Restart (Recommended)

**Add Missing Columns to Database:**

```sql
ALTER TABLE jobs ADD COLUMN name_col TEXT;
ALTER TABLE jobs ADD COLUMN first_name_col TEXT;
ALTER TABLE jobs ADD COLUMN last_name_col TEXT;
ALTER TABLE jobs ADD COLUMN cascade_config TEXT;  -- JSON string
ALTER TABLE jobs ADD COLUMN max_results INTEGER DEFAULT 5;
```

**Benefits:**
- ✅ Exact restart - uses same configuration as original job
- ✅ Preserves user's ICP tier settings
- ✅ Preserves name column mapping
- ✅ Full feature parity

**Implementation:**
1. Modify `create_enrichment_job()` to store these fields
2. Add migration script to add columns to existing database
3. Add `POST /api/enrichment/jobs/{job_id}/restart` endpoint
4. Update frontend to show "Restart" button on failed jobs

**Code Example:**
```python
@router.post("/jobs/{job_id}/restart")
async def restart_enrichment_job(
    job_id: str,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(auth.get_current_user),
):
    """Restart a failed enrichment job with same configuration."""
    store = job_store.get_store()
    original_job = store.get_job(job_id)

    if not original_job:
        raise HTTPException(status_code=404, detail="Job not found")

    if not _owns_job(original_job, current_user):
        raise HTTPException(status_code=403, detail="Access denied")

    if original_job["status"] != "failed":
        raise HTTPException(status_code=400, detail="Only failed jobs can be restarted")

    # Read the original CSV file (kept in uploads/)
    upload_path = UPLOAD_DIR / f"{original_job['filename']}.csv"
    if not upload_path.exists():
        raise HTTPException(status_code=404, detail="Original CSV file not found")

    df = pd.read_csv(str(upload_path), skipinitialspace=True)
    rows = df.fillna("").astype(str).to_dict(orient="records")

    # Parse cascade configuration from JSON
    cascade = json.loads(original_job.get("cascade_config", "null"))

    # Create new job
    new_job_id = str(uuid.uuid4())
    store.create_enrichment_job(
        job_id=new_job_id,
        user_id=current_user["user_id"],
        total=len(rows),
        filename=original_job['filename'],
        domain_col=original_job['domain_col'],
        original_filename=original_job.get('original_filename', ''),
        name_col=original_job.get('name_col'),
        first_name_col=original_job.get('first_name_col'),
        last_name_col=original_job.get('last_name_col'),
        cascade_config=original_job.get('cascade_config'),
        max_results=original_job.get('max_results', 5),
    )

    # Run the job with same configuration
    _job_signals[new_job_id] = asyncio.Event()
    _active_jobs.add(new_job_id)

    background_tasks.add_task(
        _run_job,
        job_id=new_job_id,
        rows=rows,
        domain_col=original_job['domain_col'],
        name_col=original_job.get('name_col'),
        first_name_col=original_job.get('first_name_col'),
        last_name_col=original_job.get('last_name_col'),
        cascade=cascade or blitz_client.DEFAULT_CASCADE,
        max_results=original_job.get('max_results', 5),
        write_incremental=True,
    )

    return {
        "job_id": new_job_id,
        "total": len(rows),
        "restarted_from": job_id,
    }
```

### Option 2: Basic Restart (Quick Win)

**Use What We Already Have:**

Restart with only the stored columns (domain_col, filename).

**Limitations:**
- ❌ Uses default ICP cascade (3 tiers)
- ❌ Uses default max_results (5)
- ❌ No name-based fallback
- ✅ Still better than starting from scratch!

**Code:**
```python
@router.post("/jobs/{job_id}/restart")
async def restart_enrichment_job(...):
    # Read CSV and restart with defaults
    cascade = blitz_client.DEFAULT_CASCADE  # Use default
    max_results = 5  # Use default
    name_col = None  # Not available
    # ... rest of logic
```

**Frontend Button:**
```tsx
{job.status === 'failed' && (
  <>
    <ActionButton
      icon={<Download size={13} />}
      label="Download"
      onClick={() => handleDownload(job)}
      color="var(--success)"
    />
    <ActionButton
      icon={<RefreshCw size={13} />}
      label="Restart"
      onClick={() => handleRestart(job)}
      color="var(--accent)"
    />
  </>
)}
```

---

## Storage Analysis

### Current Disk Usage

```bash
# Upload directory
du -sh backend/data/uploads/
# 296M    backend/data/uploads/

# Output directory (enriched results)
du -sh backend/data/outputs/
# (varies based on completed jobs)

# Database
du -sh backend/data/jobs.db
# ~200K
```

### Retention Policy

**Current Policy:** **Keep Forever** ❌

**Problem:**
- 29 CSV files = 296 MB used
- Will grow indefinitely with every upload
- No cleanup mechanism
- Old uploads never deleted

**Recommended Policy:**

**Option A: Keep For Active Jobs Only**
```python
def cleanup_old_uploads():
    """Delete uploads older than 30 days that aren't referenced by active jobs."""
    cutoff = datetime.now() - timedelta(days=30)

    # Get all upload_ids referenced by jobs
    active_uploads = set(
        row["filename"]
        for row in db.execute(
            "SELECT filename FROM jobs WHERE created_at > ?",
            (cutoff.isoformat(),)
        )
    )

    # Delete files not in active set
    for file in UPLOAD_DIR.glob("*.csv"):
        upload_id = file.stem
        if upload_id not in active_uploads:
            file.unlink()
            metadata = UPLOAD_DIR / f"{upload_id}.metadata.json"
            if metadata.exists():
                metadata.unlink()
```

**Option B: Keep For Failed Jobs (Recommended for Restart Feature)**
```python
def cleanup_old_uploads():
    """Delete uploads for completed jobs, keep for failed jobs."""
    # Delete uploads for jobs that completed successfully
    # Keep uploads for failed jobs (so they can be restarted)
    # Keep uploads for last 7 days regardless
```

**Option C: Keep Everything (Current)**
- Pros: Restart always works, full history
- Cons: Disk usage grows indefinitely
- Cost: 296 MB and counting

---

## Implementation Plan

### Phase 1: Database Schema Update (Immediate)

**1. Add Migration Script:**
```python
# backend/migrations/add_restart_support.py
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent / "data" / "jobs.db"

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Add missing columns
cursor.execute("""
    ALTER TABLE jobs ADD COLUMN name_col TEXT
""")
cursor.execute("""
    ALTER TABLE jobs ADD COLUMN first_name_col TEXT
""")
cursor.execute("""
    ALTER TABLE jobs ADD COLUMN last_name_col TEXT
""")
cursor.execute("""
    ALTER TABLE jobs ADD COLUMN cascade_config TEXT
""")
cursor.execute("""
    ALTER TABLE jobs ADD COLUMN max_results INTEGER DEFAULT 5
""")

conn.commit()
conn.close()
print("Migration complete: Added restart support columns")
```

**2. Update Job Creation:**
```python
# backend/enrichment/job_store.py
def create_enrichment_job(
    self,
    job_id: str,
    user_id: str,
    total: int,
    filename: str = "",
    domain_col: str = "",
    original_filename: str = "",
    parent_job_id: Optional[str] = None,
    name_col: Optional[str] = None,           # NEW
    first_name_col: Optional[str] = None,     # NEW
    last_name_col: Optional[str] = None,      # NEW
    cascade_config: Optional[str] = None,     # NEW
    max_results: Optional[int] = None,        # NEW
) -> None:
    # ... implementation
```

### Phase 2: Backend API Endpoint (Week 1)

**Add Restart Endpoint:**
```python
@router.post("/jobs/{job_id}/restart")
async def restart_enrichment_job(...)
# See full implementation in Option 1 above
```

### Phase 3: Frontend UI (Week 1)

**Update JobsList Components:**

**Enrichment JobsList:**
```tsx
{job.status === 'failed' && (
  <>
    <ActionButton
      icon={<Download size={13} />}
      label="Download"
      onClick={() => handleDownload(job)}
      color="var(--success)"
    />
    <ActionButton
      icon={<RotateCcw size={13} />}  {/* Restart icon */}
      label="Restart"
      onClick={() => handleRestart(job)}
      color="var(--accent)"
      title="Restart this job with the same configuration"
    />
  </>
)}
```

**Job History Page:**
```tsx
{(job.status === 'done' || job.status === 'failed') && (
  <button onClick={() => handleDownload(job.job_id, job.job_type)}>
    <Download size={16} />
  </button>
)}
{job.job_type === 'enrichment' && job.status === 'failed' && (
  <button onClick={() => handleRestart(job)}>
    <RotateCcw size={16} />
  </button>
)}
```

### Phase 4: Storage Cleanup (Week 2)

**Add Scheduled Cleanup:**
```python
# backend/main.py
from apscheduler.schedulers.background import BackgroundScheduler

scheduler = BackgroundScheduler()

@scheduler.scheduled_job('cron', hour=2)  # Run at 2 AM daily
def cleanup_old_uploads():
    enrichment_routes.cleanup_old_uploads()

scheduler.start()
```

---

## Cost-Benefit Analysis

### Benefits

**User Experience:**
- ✅ One-click restart instead of re-uploading and re-configuring
- ✅ No need to remember column mappings
- ✅ No need to reconfigure ICP tiers
- ✅ Saves time for failed jobs with partial results

**Operational:**
- ✅ Failed jobs can be automatically retried
- ✅ Transient API failures don't waste user's time
- ✅ Better recovery from server restarts

### Costs

**Development:**
- Backend: ~4 hours (migration + endpoint + testing)
- Frontend: ~2 hours (UI + restart handler)
- Testing: ~2 hours (edge cases, permissions)
- **Total: ~8 hours**

**Storage:**
- Current: 296 MB for 29 uploads
- Average: ~10 MB per CSV
- Estimated growth: 100 uploads/year = 1 GB/year
- Cost: Minimal (<$5/month for storage)

### ROI

**Time Saved per User:**
- Upload CSV: 2 minutes
- Configure columns: 2 minutes
- Configure ICP tiers: 3 minutes
- **Total: 7 minutes saved per restart**

**Break-Even:**
- If 1 user restarts 1 job: Development cost paid off
- If 10 users restart jobs: 70 minutes saved
- **High ROI feature**

---

## Recommendation

**Implement Full Restart (Option 1)** with the following timeline:

**Week 1:**
1. ✅ Add database columns (migration script)
2. ✅ Update job creation to store all parameters
3. ✅ Implement restart API endpoint
4. ✅ Add frontend "Restart" button

**Week 2:**
5. ✅ Implement upload cleanup policy (keep for 30 days or for failed jobs)
6. ✅ Add "Restart" confirmation dialog
7. ✅ Test with various failure scenarios

**Success Metrics:**
- Restart button appears on failed jobs
- Restarted jobs use identical configuration
- Restarted jobs create new job_id (preserve history)
- Storage growth managed with cleanup policy

---

## Internal Database Clarification

### Updated Documentation

**Contacts DB (leadsdatabase.cc)**

**Previous Description:** "External paid API service"

**Corrected Description:**

**Internal Hyperke Contacts Database**
- **Hosted at:** https://leadsdatabase.cc
- **Type:** Internal Hyperke database (not external paid service)
- **Access:** Via API with Bearer token authentication
- **Purpose:** Fallback when Blitz API can't find emails
- **Usage:**
  1. Find person by LinkedIn URL (when Blitz email lookup fails)
  2. Find person by name + domain (when no company LinkedIn found)

**Architecture:**
```
Enrichment Pipeline:
├── Primary: Blitz API (api.blitz-api.ai)
│   ├── Domain → Company LinkedIn
│   ├── Company → Decision makers
│   └── Person → Work email
│
└── Fallback: Internal Hyperke DB (leadsdatabase.cc)
    ├── GET /v1/person/by-linkedin?linkedin_url=...
    └── GET /v1/person/by-name?name=...&domain=...
```

**Environment Variables:**
```bash
CONTACTS_API_BASE_URL=https://leadsdatabase.cc
CONTACTS_API_TOKEN=<internal-auth-token>
```

**This is an INTERNAL Hyperke resource**, not an external third-party service.

---

## Summary

### Can We Restart Failed Jobs?

**Answer:** **YES!** ✅

**What We Have:**
- ✅ CSV files kept persistently (296 MB, 29 files)
- ✅ Upload metadata stored (filename, domain_col)
- ✅ Can read CSV and restart enrichment

**What We Need:**
- ⚠️ Add 5 database columns for full configuration storage
- ⚠️ Implement restart API endpoint
- ⚠️ Add frontend "Restart" button

**Effort:** ~8 hours development time

**Storage:** Already available, just need cleanup policy

**Priority:** **HIGH** - Great UX improvement, minimal cost

---

**Analysis Date:** March 10, 2026
**Status:** ✅ Feasible and Recommended
**Next Step:** Implement database migration and restart endpoint
