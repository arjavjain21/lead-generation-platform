# Incremental Resume for Abandoned Jobs - Design

**Status:** Draft
**Created:** 2026-05-19
**Author:** Claude

---

## Problem Statement

When a server crashes or restarts while an enrichment job is running, the job is marked as `abandoned`. Currently:

1. **Abandoned jobs cannot be retried** — backend rejects anything except `failed` status
2. **All rows are re-processed** on restart — wastes API calls on already-processed domains
3. **Users lose time but not data** — CSV is preserved, but pipeline restarts from row 1

**Goal:** Implement true incremental resume so abandoned jobs can resume from where they left off.

---

## Design Decision Summary

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Checkpoint granularity | Row-level | Most precise, no wasted API calls |
| Checkpoint storage | SQLite table | Space-efficient, ACID compliant, same DB |
| Output handling | New file per restart | Simpler than append+dedup, partial preserved |

---

## Database Schema

### New `job_checkpoints` Table

```sql
CREATE TABLE job_checkpoints (
    job_id TEXT NOT NULL,
    row_index INTEGER NOT NULL,
    processed_at TEXT NOT NULL,  -- ISO timestamp
    PRIMARY KEY (job_id, row_index)
);

CREATE INDEX idx_checkpoints_job ON job_checkpoints(job_id);
```

**Space estimate:**
- 50K rows ≈ 50K entries
- Each entry: ~40 bytes (UUID + int + timestamp)
- Total: ~2 MB per 50K-row job
- Reasonable for SQLite

### New `jobs` Table Column

```sql
ALTER TABLE jobs ADD COLUMN restart_count INTEGER DEFAULT 0;
```

Tracks how many times a job has been restarted (for diagnostics).

---

## Architecture

### Checkpoint Writer

The pipeline will periodically write checkpoints as rows are processed:

```
Process row N
    → API call(s) → write results
    → append_event() called
    → checkpoint written every 100 rows OR job completes
```

**Implementation:** Modify `append_event()` in `job_store_base.py` to also write checkpoint if:
- `row_index % 100 == 0` (batch checkpoint)
- OR job completes (final checkpoint)

### Checkpoint Reader

On job restart, read unprocessed rows:

```python
# Get all processed row indices
processed = get_processed_indices(job_id)

# Filter input to only unprocessed
unprocessed_rows = [row for i, row in enumerate(rows) if i not in processed]
```

### Restart Endpoint Update

1. Accept `abandoned` status (in addition to `failed`)
2. Read checkpoint to find unprocessed rows
3. Create new output file (rename old partial)
4. Set `restart_count = original + 1`
5. Process only unprocessed rows

### Abandoned Detection on Startup

Already implemented in `mark_abandoned_jobs()` — no changes needed.

---

## API Changes

### `POST /api/enrichment/jobs/{job_id}/restart`

**Current behavior:**
```python
if original_job["status"] != "failed":
    raise HTTPException(status_code=400, detail="Only failed jobs can be restarted")
```

**New behavior:**
```python
if original_job["status"] not in ("failed", "abandoned"):
    raise HTTPException(status_code=400,
        detail="Only failed or abandoned jobs can be restarted")
```

**Additional steps on restart:**
1. Read `job_checkpoints` for this job_id
2. Rename existing output to `*_partial.csv` if exists
3. Filter input CSV to skip processed rows
4. Increment `restart_count`
5. Process unprocessed rows to new output

### `GET /api/enrichment/jobs/{job_id}`

Add `processed_indices` field to response so frontend can show progress percentage without recounting.

---

## File Handling

### On Restart

| Before Restart | After Restart |
|---------------|---------------|
| `uploads/{upload_id}.csv` | Same (unchanged) |
| `outputs/{job_id}.csv` | `outputs/{job_id}_partial.csv` |
| (new file created) | `outputs/{job_id}.csv` (fresh) |

### Download Options

Users can download:
1. **Current output** (`{job_id}.csv`) — most recent complete run
2. **Partial output** (`{job_id}_partial.csv`) — data collected before last crash

---

## Scope Boundaries

### In Scope

- Enrichment jobs (domain enrichment)
- Row-level checkpoint tracking
- Restart endpoint update
- Partial output preservation

### Out of Scope (Future)

- Phone enrichment jobs (separate module)
- Scraper jobs (different architecture)
- Real-time progress in SSE (already works)
- Checkpoint cleanup/garbage collection

---

## Implementation Phases

### Phase 1: Database Migration
- Add `job_checkpoints` table
- Add `restart_count` column

### Phase 2: Checkpoint Writer
- Modify `append_event()` to write checkpoints
- Modify pipeline to respect existing checkpoints on start

### Phase 3: Restart Endpoint
- Accept `abandoned` status
- Filter unprocessed rows
- Handle output file renaming

### Phase 4: Testing
- Test normal completion
- Test crash mid-job (SIGKILL simulation)
- Test restart and verify no duplicate processing
- Test multiple restarts

---

## Success Criteria

1. Abandoned jobs show "Retry" button (frontend already supports this)
2. Backend accepts `abandoned` status for restart
3. Restarted jobs only process unprocessed rows
4. Partial output is preserved and downloadable
5. `restart_count` increments correctly
6. Multiple restarts don't cause data loss or duplication

---

## Risk Mitigation

| Risk | Mitigation |
|------|------------|
| Checkpoint write lag loses last N rows | Write checkpoint on every `append_event` + periodic flush |
| Corrupt checkpoint data | Validate on read, fallback to full re-process if invalid |
| Large checkpoint table grows unbounded | Cleanup checkpoints for completed jobs older than 30 days |
| Concurrent restart attempts | Job status changes to `queued` atomically; second request fails |

---

## Rollback Plan

If issues arise:
1. Revert restart endpoint to only accept `failed` status
2. Checkpoints remain in DB (harmless, just unused)
3. No data migration needed — backward compatible

---

## File Locations

| File | Changes |
|------|---------|
| `backend/shared/job_store_base.py` | Add checkpoint write/read methods |
| `backend/enrichment/job_store.py` | Store `restart_count` on create |
| `backend/enrichment/routes.py` | Update restart endpoint |
| `backend/enrichment/pipeline.py` | Check existing checkpoints before processing |
| `backend/migrations/` | New migration script |
| `frontend/` | No changes needed (already shows Retry for abandoned) |