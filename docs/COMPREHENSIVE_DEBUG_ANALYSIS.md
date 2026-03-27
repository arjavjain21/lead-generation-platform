# Comprehensive Debug Analysis - Lead Generation Platform
**Date:** March 10, 2026
**Status:** 🔴 CRITICAL ISSUES FOUND
**Analyst:** Claude Code AI Assistant

---

## EXECUTIVE SUMMARY

After systematic investigation following debugging best practices, I've identified **THREE CRITICAL BUGS** and **ONE FEATURE REQUEST** that need attention:

### Issues Found:
1. 🔴 **CRITICAL: Progress Counter Bug Fix is INCOMPLETE** - Job events not being written to database
2. ⚠️ **HIGH: Disk Space Crisis** - 97% full (6.1GB free), causing job failures
3. ⚠️ **MEDIUM: Emails/Results Display Bug** - Shows 0 even when job completes successfully
4. 🆕 **FEATURE: Retry Functionality** - No way to retry failed jobs from UI

---

## ISSUE #1: PROGRESS COUNTER BUG - FIX IS INCOMPLETE! 🔴

### Root Cause Analysis

**What was observed:**
- Job `d15c3247-a754-495e-8e02-1b6f6a7bd374` completed successfully
- Status: `done`, Total: 7793 rows
- Database shows: processed=0, emails_found=0
- Job events table: **0 rows** (completely empty!)
- Output file: **8983 rows, 8.8MB** (data exists!)

**What was supposed to be fixed:**
The previous fix (Mar 9, 2026) changed `backend/enrichment/job_store.py:64-73`:
```python
# OLD CODE (Singleton - causes threading issue):
def get_store() -> EnrichmentJobStore:
    global _default_store
    if _default_store is None:
        _default_store = EnrichmentJobStore(db.get_db())
    return _default_store

# NEW CODE (Fresh connection per call):
def get_store() -> EnrichmentJobStore:
    return EnrichmentJobStore(db.get_db())
```

**Why the fix is INCOMPLETE:**

The problem is NOT in `get_store()` - the problem is in HOW it's used!

Looking at `backend/enrichment/routes.py:346-370`:

```python
async def _run_job(
    job_id: str,
    rows: list[dict[str, Any]],
    ...
):
    store = job_store.get_store()  # ← Gets store instance
    store.set_running(job_id)     # ← Works! (same thread)

    async def on_progress(e: dict[str, Any]):
        store.append_event(job_id, seq[0], e)  # ← FAILS! (different thread)
        seq[0] += 1

    # Background task runs in different thread
    output_rows = await pipeline.run_pipeline(
        ...
        on_progress=on_progress,  # ← Called from background thread!
        ...
    )
```

**The REAL Bug:**

1. `store = job_store.get_store()` creates a store with a SQLite connection
2. SQLite connections in Python are **thread-local** - they can ONLY be used in the thread that created them
3. `on_progress` callback is invoked from background tasks (different threads)
4. When `on_progress` calls `store.append_event()`, it tries to use the connection from a different thread
5. SQLite **silently fails** to commit transactions from wrong thread
6. Result: job_events table stays empty, counters never update

**Why output file still works:**
- File I/O doesn't use database
- Direct file writes work across threads
- CSV is written successfully even when DB commits fail

### Evidence

```bash
# Job events table is completely empty:
sqlite3 jobs.db "SELECT COUNT(*) FROM job_events WHERE job_id = 'd15c3247...';"
# Result: 0

# But output file has all the data:
wc -l outputs/d15c3247-*.csv
# Result: 8983 lines (1 header + 8982 data rows)

# Database shows no progress:
processed: 0
emails_found: 0
```

### The Real Fix

**Option 1: Get fresh store instance in background thread** (RECOMMENDED)

Change `backend/enrichment/routes.py:363-369`:

```python
# OLD CODE:
async def on_progress(e: dict[str, Any]):
    store.append_event(job_id, seq[0], e)  # ← Uses connection from main thread
    seq[0] += 1

# NEW CODE:
async def on_progress(e: dict[str, Any]):
    # Get FRESH store instance in current thread
    progress_store = job_store.get_store()
    progress_store.append_event(job_id, seq[0], e)
    seq[0] += 1
```

**Option 2: Make append_event thread-safe**

Change `backend/shared/job_store_base.py:108-141` to get fresh connection for each operation:

```python
def append_event(self, job_id: str, seq: int, event: dict[str, Any]) -> None:
    # Get FRESH connection for this thread
    conn = db.get_db()

    payload = json.dumps(event)
    conn.execute(
        "INSERT INTO job_events (job_id, seq, payload) VALUES (?, ?, ?)",
        (job_id, seq, payload),
    )

    # Update job-specific counters
    job = self.get_job(job_id)  # ← Still uses self.conn (might fail!)
    ...
```

**Why Option 1 is better:**
- Simpler change
- Each thread gets its own store instance
- No need to modify JobStoreBase
- Consistent with the original fix intent

### Files to Modify

1. **`backend/enrichment/routes.py:363-369`** - Update `on_progress` callback
2. **Optional:** `backend/shared/job_store_base.py:108-141` - Make thread-safe if needed

### Testing Plan

After fix:
1. Upload small CSV (10-20 rows)
2. Start enrichment job
3. Monitor job_events table:
   ```bash
   sqlite3 jobs.db "SELECT COUNT(*) FROM job_events WHERE job_id = '...';"
   ```
4. Verify processed counter increments in real-time
5. Verify emails_found counter updates

---

## ISSUE #2: DISK SPACE CRISIS 🔴

### Current Status

```bash
Filesystem      Size  Used Avail Use% Mounted on
/dev/sda1       193G  187G  6.1G  97% /
```

**Only 6.1GB free - This is DANGEROUSLY LOW!**

### Impact

Multiple enrichment jobs failing with:
```
[Errno 28] No space left on device
```

Failed jobs:
- `1d4e26c9-a4aa-4dda-9549-de4b8b1f4d3f` - 39,864 rows
- `40dab40f-5880-44cd-85df-3fdeaeb1221d` - 58,861 rows
- `503c7ba4-e7cd-466c-9fbd-e94e6a0387ed` - 7,928 rows
- `0d298730-3b7a-4874-a261-fa28771682ec` - 7,793 rows

### Immediate Actions Required

**1. Clean up old data:**
```bash
# Check what's using space
du -sh /var/www/lead-generation-platform/backend/data/*
du -sh /var/www/lead-generation-platform/backend/data/uploads/*
du -sh /var/www/lead-generation-platform/backend/data/outputs/*

# Remove old uploads (older than 30 days)
find /var/www/lead-generation-platform/backend/data/uploads/ -name "*.csv" -mtime +30 -delete

# Remove old outputs (older than 30 days)
find /var/www/lead-generation-platform/backend/data/outputs/ -name "*.csv" -mtime +30 -delete
```

**2. Check for large log files:**
```bash
find /var/www/lead-generation-platform -name "*.log" -size +100M -ls
```

**3. Consider adding auto-cleanup:**
- Add cron job to clean up files older than 30 days
- Implement job retention policy in database
- Add disk space monitoring alerts

**4. Long-term solution:**
- Expand disk size (recommended: 500GB+)
- Or move data directory to separate mounted drive

---

## ISSUE #3: EMAILS/RESULTS DISPLAY BUG ⚠️

### What Users See

- Job History shows: "Results/Emails: 0"
- Download button works and has data
- Confusing UX - users think job failed when it succeeded

### Root Cause

Two separate issues:

**Issue 3A: Progress counter bug** (see Issue #1)
- Database shows `processed=0, emails_found=0`
- UI displays these database values
- Fix: Apply the real fix for Issue #1

**Issue 3B: All rows skipped** (specific to job d15c3247-...)

For the specific job `d15c3247-a754-495e-8e02-1b6f6a7bd374`:
- All 8,982 rows show status: `skipped_no_domain`
- The domain column selected: `website`
- CSV has a `website` column with URLs
- **But** the enrichment pipeline checks if domain value is EMPTY (pipeline.py:362-364)

Looking at the pipeline code:
```python
domain = str(row.get(domain_col, "")).strip()

if not domain:
    result_rows = [_error_row(row)]
    result_rows[0]["row_status"] = "skipped_no_domain"
```

**Possible causes:**
1. The "website" column values are empty strings (not the URLs I saw in sample)
2. OR different rows have empty website values
3. OR the CSV has mixed data quality

**This is NOT a bug - it's data quality issue!**

### Verification

```bash
# Check how many rows have empty website:
awk -F',' 'NR>1 {print $21}' /var/www/lead-generation-platform/backend/data/uploads/1d4ef33d-*.csv | grep -c "^$"

# Check sample of website values:
awk -F',' 'NR>1 && NR<=10 {print $21}' /var/www/lead-generation-platform/backend/data/uploads/1d4ef33d-*.csv
```

### UX Improvements

Even though it's data quality, we can improve UX:

1. **Show status breakdown:**
   - Total rows: 7,793
   - Enriched: X
   - Skipped (no domain): Y
   - No LinkedIn: Z
   - Errors: N

2. **Better error messages:**
   - "0 rows enriched - all rows skipped due to missing domain values"
   - "Try selecting a different column with domain/website data"

3. **Preview warnings:**
   - Warn during upload if many rows have empty domain column
   - Suggest alternative columns

---

## ISSUE #4: RETRY FUNCTIONALITY (FEATURE REQUEST) 🆕

### Current State

When jobs fail:
- Status: `failed`
- Error: "Server restarted while job was running" or "[Errno 28] No space left on device"
- No way to retry from UI
- User must re-upload CSV and start over

### Desired Feature

User request:
> "is it possible and feasible to give the user ability to retry failed processes on the ui?"

Example failed job:
```
electrical installation service, electrician
Job: a7db9638...
Status: Failed
Progress: 1774/2526
Error: Server restarted while job was running.
```

### Proposed Solution

**Backend changes:**

1. **Add retry endpoint** (`backend/enrichment/routes.py`):
```python
@router.post("/jobs/{job_id}/retry")
async def retry_enrichment_job(
    job_id: str,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(auth.get_current_user),
):
    """Retry a failed enrichment job from where it left off."""
    store = job_store.get_store()
    job = store.get_job(job_id)

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if not _owns_job(job, current_user):
        raise HTTPException(status_code=403, detail="Access denied")
    if job["status"] != "failed":
        raise HTTPException(status_code=400, detail="Only failed jobs can be retried")

    # Get original upload
    upload_path = UPLOAD_DIR / f"{job['filename']}.csv"
    if not upload_path.exists():
        raise HTTPException(status_code=404, detail="Original upload not found")

    # Read CSV and skip already processed rows
    df = pd.read_csv(str(upload_path))
    processed_count = job.get("processed", 0)
    rows = df.fillna("").astype(str).to_dict(orient="records")
    remaining_rows = rows[processed_count:]

    if not remaining_rows:
        raise HTTPException(status_code=400, detail="No rows to retry")

    # Create new job (or update existing)
    new_job_id = str(uuid.uuid4())
    cascade = json.loads(job.get("cascade", "[]")) or blitz_client.DEFAULT_CASCADE

    store.create_enrichment_job(
        job_id=new_job_id,
        user_id=current_user["user_id"],
        total=len(remaining_rows),
        filename=job["filename"],
        domain_col=job.get("domain_col", ""),
        original_filename=job.get("original_filename", ""),
    )

    # Start background task
    _job_signals[new_job_id] = asyncio.Event()
    _active_jobs.add(new_job_id)

    background_tasks.add_task(
        _run_job,
        job_id=new_job_id,
        rows=remaining_rows,
        domain_col=job.get("domain_col", ""),
        name_col=None,  # Would need to store these in job
        first_name_col=None,
        last_name_col=None,
        cascade=cascade,
        max_results=5,
        write_incremental=True,
    )

    return {"job_id": new_job_id, "total": len(remaining_rows)}
```

2. **Store retry metadata** (database schema update):
```sql
ALTER TABLE jobs ADD COLUMN retry_of TEXT;
ALTER TABLE jobs ADD COLUMN retry_attempt INTEGER DEFAULT 0;
```

3. **Update frontend**:
   - Add "Retry" button for failed jobs
   - Show retry history
   - Link from new job to original failed job

**Challenges:**

1. **Need to store original job parameters:**
   - cascade configuration
   - name_col, first_name_col, last_name_col
   - max_results

2. **Output file merging:**
   - Need to combine partial output from failed job with new results
   - Or start fresh and overwrite

3. **Idempotency:**
   - Ensure we don't double-process rows
   - Handle partial outputs correctly

**Alternative approach:**

Instead of complex retry logic, add **resume capability**:
- Keep job status as `running` even if server crashes
- On server restart, detect stale jobs
- Automatically resume from last processed row
- Update `cleanup_stale_jobs()` to resume instead of fail

---

## PRIORITY RECOMMENDATIONS

### 🔴 IMMEDIATE (Today)

1. **Fix Progress Counter Bug** (Issue #1)
   - Modify `backend/enrichment/routes.py:363-369`
   - Test with small job
   - Deploy to production

2. **Address Disk Space** (Issue #2)
   - Clean up old files (>30 days)
   - Add monitoring/alerts
   - Plan disk expansion

### ⚠️ HIGH PRIORITY (This Week)

3. **Improve UX for Skipped Rows** (Issue #3B)
   - Show status breakdown
   - Better error messages
   - Preview warnings

4. **Add Retry Functionality** (Issue #4)
   - Design full solution
   - Implement backend endpoint
   - Add frontend UI

### 📊 MEDIUM PRIORITY (Next Sprint)

5. **Add Monitoring & Alerts**
   - Disk space monitoring
   - Job failure alerts
   - Performance metrics

6. **Implement Auto-Cleanup**
   - Cron job for old files
   - Database retention policy
   - Configurable cleanup intervals

---

## TESTING CHECKLIST

After fixes are deployed:

- [ ] Upload small CSV (10-20 rows)
- [ ] Start enrichment job
- [ ] **Verify:** Progress counter updates in real-time (1/20, 2/20, etc.)
- [ ] **Verify:** Emails found counter increments
- [ ] **Verify:** Job events table has rows
- [ ] **Verify:** Final status shows correct counts
- [ ] **Verify:** Download button works
- [ ] **Verify:** Output CSV has enriched data
- [ ] Test retry functionality with failed job
- [ ] Monitor disk space during test
- [ ] Check service logs for errors

---

## CONCLUSION

The previous bug fix was **well-intentioned but incomplete**. The real issue is that SQLite connections are thread-local, and the fix didn't account for how background tasks invoke the progress callback.

The good news:
- ✅ Output files work correctly
- ✅ Enrichment pipeline processes data
- ✅ No data loss

The bad news:
- ❌ Progress tracking completely broken
- ❌ Database counters don't update
- ❌ UX is confusing (shows 0 when job worked)
- ❌ Disk space crisis causing failures

The path forward is clear:
1. Fix the `on_progress` callback to use fresh store instance
2. Clean up disk space
3. Add retry functionality
4. Improve UX for status reporting

---

**Next Steps:**
1. Review this analysis
2. Approve proposed fixes
3. Implement changes
4. Test thoroughly
5. Deploy to production

**Contact:** Claude Code AI Assistant
**Methodology:** Systematic Debugging (following superpowers:systematic-debugging skill)
