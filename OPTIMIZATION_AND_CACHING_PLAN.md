# Scraping Optimization & Caching Strategy Plan

## Current Performance Baseline

### Processing Speed
- **Current Rate**: ~5,000 tasks/hour (comprehensive jobs)
- **Job Duration**: 88,620 tasks = ~17.7 hours
- **Concurrency**: 8 workers
- **Success Rate**: ~41% completion before user cancellation

### Storage Architecture
- **jobs.db**: Job metadata (query, regions, counts)
- **CSV files**: Full scraped results (72k-100k rows per job)
- **Job events**: SSE progress tracking
- **No caching layer**: Every query scrapes from scratch

---

## PART 1: SPEED OPTIMIZATION

### Immediate Wins (Low Risk, High Impact)

#### 1.1 Increase Concurrency
**Current**: 8 workers
**Proposed**: 12-16 workers (configurable via env)
**Expected Gain**: 50-100% speed improvement
**Risk**: Medium - may hit API rate limits harder
**Implementation**:
```python
# crawler.py
CONCURRENCY = int(os.getenv("SCRAPER_CONCURRENCY", "12"))
```

#### 1.2 Optimize Zoom Strategy for Comprehensive Mode
**Current**: All cities use 3 zoom levels (10, 11, 12)
**Problem**: Zoom levels overlap significantly
**Proposed**: Adaptive zoom based on city size
- Large cities (50+ zips): All 3 zooms
- Medium cities (10-50 zips): Zooms 11, 12
- Small cities (<10 zips): Zoom 12 only
**Expected Gain**: 30-40% reduction in tasks for Tier 3
**Risk**: Low - better targeting, less redundant data

#### 1.3 Batch CSV Writes
**Current**: Flush after every task
**Proposed**: Batch writes every 10 tasks or 5 seconds
**Expected Gain**: 10-15% I/O improvement
**Risk**: Low - still flush frequently enough

### Medium-Term Optimizations

#### 1.4 Parallel Job Processing
**Current**: 3 jobs running sequentially at ~5k tasks/hour each
**Proposed**: Run jobs in true parallel with separate worker pools
**Expected Gain**: 3x throughput for multiple users
**Risk**: Medium - requires job queue architecture

#### 1.5 Result Streaming
**Current**: Wait for job completion to download CSV
**Proposed**: Stream results via SSE as they arrive
**Expected Gain**: Better UX, earlier access to data
**Risk**: Low - already have SSE infrastructure

#### 1.6 Smart Retry with Deadzone Detection
**Current**: Retry failed tasks immediately
**Proposed**: Detect geographic deadzones and skip them
**Expected Gain**: Skip hopeless tasks (0 results)
**Risk**: Low - track city+zoom combos with 0 results

### Long-Term Optimizations

#### 1.7 Distributed Scraping
**Current**: Single server processing
**Proposed**: Multiple workers across containers/servers
**Expected Gain**: Linear scalability
**Risk**: High - requires significant architecture changes

---

## PART 2: 60-DAY CACHING STRATEGY

### Problem Statement
- Users run identical queries repeatedly
- Same "dental clinic" query for "All US" = 88,620 API calls each time
- Wasted API costs and time
- No way to leverage previous work

### Solution Architecture

#### 2.1 Database Schema

**New Table: `scraped_results`**
```sql
CREATE TABLE scraped_results (
    -- Cache identification
    cache_id TEXT PRIMARY KEY,  -- hash of query + region signature
    
    -- Query parameters
    query TEXT NOT NULL,
    region_signature TEXT NOT NULL,  -- hash of regions JSON
    regions TEXT NOT NULL,  -- Full regions JSON for reconstruction
    
    -- Result metadata
    total_results INTEGER NOT NULL,
    result_file_path TEXT NOT NULL,
    
    -- Cache management
    created_at TEXT NOT NULL,
    last_accessed_at TEXT NOT NULL,
    access_count INTEGER DEFAULT 1,
    expires_at TEXT NOT NULL,
    is_fresh BOOLEAN DEFAULT TRUE,  -- True if <60 days old
    
    -- Data integrity
    checksum TEXT,  -- SHA256 of result file
    status TEXT NOT NULL  -- 'pending', 'complete', 'expired'
);

CREATE INDEX idx_cache_lookup ON scraped_results(query, region_signature, expires_at);
CREATE INDEX idx_cache_expiry ON scraped_results(expires_at);
```

**New Table: `cache_stats`**
```sql
CREATE TABLE cache_stats (
    date TEXT PRIMARY KEY,
    cache_hits INTEGER DEFAULT 0,
    cache_misses INTEGER DEFAULT 0,
    api_calls_saved INTEGER DEFAULT 0
);
```

#### 2.2 Cache Key Generation

**Region Signature Hash:**
```python
def generate_region_signature(regions: dict) -> str:
    """
    Generate deterministic hash for region configuration.
    Same query + same regions = same cache key.
    """
    import hashlib
    import json
    
    # Normalize and hash
    normalized = json.dumps(regions, sort_keys=True)
    return hashlib.sha256(normalized.encode()).hexdigest()[:16]

def generate_cache_id(query: str, region_signature: str) -> str:
    """Generate unique cache ID."""
    import hashlib
    combined = f"{query.lower().strip()}:{region_signature}"
    return hashlib.sha256(combined.encode()).hexdigest()[:16]
```

#### 2.3 Cache Lifecycle

**On Job Start (Check Phase):**
```
1. User submits: query="dental clinic", regions={...}
2. Generate cache_id from query + regions
3. Query scraped_results WHERE cache_id=X AND expires_at > NOW()
4. If HIT:
   - Increment access_count, update last_accessed_at
   - If user wants fresh: Start new job, mark old as stale
   - If user accepts cached: Return cached results immediately
5. If MISS:
   - Create new cache entry with status='pending'
   - Proceed to scraping
```

**On Job Completion (Store Phase):**
```
1. Scrape completes, generate CSV
2. Calculate checksum of result file
3. Update cache entry:
   - status='complete'
   - result_file_path=path
   - checksum=sha256
   - total_results=row_count
4. Set expires_at = created_at + 60 days
```

**On Cache Access (Return Phase):**
```
1. User accepts cached results
2. Copy result file to user's output path
3. Create new job record with status='done', source='cached'
4. Update cache_stats
```

#### 2.4 User Interface

**Toggle in Frontend:**
```html
<div class="filter-group">
    <label>
        <input type="checkbox" id="useCache" checked>
        Use cached results if available (last 60 days)
    </label>
    <div id="cacheStatus" style="display:none;">
        <span class="badge cached">✓ Cached results from <span id="cacheDate"></span></span>
        <span class="badge cached-results"><span id="cacheCount"></span> results</span>
    </div>
</div>
```

**API Response (Enhanced):**
```json
{
  "job_id": "...",
  "cache_hit": true,
  "cached_results": {
    "created_at": "2026-05-10T10:00:00Z",
    "total_results": 100000,
    "days_old": 15
  },
  "use_fresh": false
}
```

#### 2.5 Cache Maintenance

**Daily Cleanup Job:**
```python
async def cleanup_expired_cache():
    """Mark cache entries as expired after 60 days."""
    expired = db.execute("""
        UPDATE scraped_results
        SET is_fresh = FALSE, status = 'expired'
        WHERE expires_at < datetime('now')
    """)
    
    # Optionally delete old files
    old_files = db.execute("""
        SELECT result_file_path
        FROM scraped_results
        WHERE status = 'expired'
        AND created_at < datetime('now', '-90 days')
    """)
```

**Cache Statistics:**
```python
async def get_cache_stats():
    """Return cache performance metrics."""
    return {
        "total_entries": count_cached_results(),
        "total_size_bytes": sum_file_sizes(),
        "hit_rate": calculate_hit_rate(),
        "api_calls_saved": calculate_saved_calls(),
        "popular_queries": top_cached_queries(10)
    }
```

---

## PART 3: IMPLEMENTATION ROADMAP

### Phase 1: Speed Optimization (Week 1)
**Priority: HIGH | Risk: LOW**

1. ✅ Test increased concurrency (8 → 12 → 16)
2. ✅ Implement adaptive zoom strategy
3. ✅ Optimize CSV write batching
4. ✅ Add smart retry with deadzone detection

**Deliverables:**
- Configurable concurrency via env var
- 30-50% speed improvement
- A/B test results showing performance gains

### Phase 2: Cache Database (Week 2)
**Priority: HIGH | Risk: LOW**

1. ✅ Create `scraped_results` table
2. ✅ Create `cache_stats` table
3. ✅ Implement cache key generation
4. ✅ Add migration script

**Deliverables:**
- Database schema ready
- Cache key generation tested
- Migration script validated

### Phase 3: Cache Backend (Week 3)
**Priority: HIGH | Risk: MEDIUM**

1. ✅ Implement cache check on job start
2. ✅ Implement cache store on job completion
3. ✅ Implement cache retrieval API
4. ✅ Add cache maintenance cron job

**Deliverables:**
- Cache check working
- Cached results retrievable
- Daily cleanup scheduled

### Phase 4: Frontend Integration (Week 4)
**Priority: MEDIUM | Risk: LOW**

1. ✅ Add cache toggle to UI
2. ✅ Show cache status/hit
3. ✅ Add cache stats dashboard
4. ✅ Update job creation flow

**Deliverables:**
- Users can see cached results option
- Cache status visible in UI
- Stats dashboard functional

### Phase 5: Testing & Validation (Week 5)
**Priority: HIGH | Risk: LOW**

1. ✅ Load test cache with 1000 jobs
2. ✅ Validate cache integrity (checksums)
3. ✅ Test cache expiry workflow
4. ✅ Measure actual API savings

**Deliverables:**
- Performance benchmarks
- Cache hit rate >30% target
- API cost reduction report

---

## PART 4: EXPECTED OUTCOMES

### Speed Improvements
| Metric | Current | Target | Improvement |
|--------|---------|--------|-------------|
| Tasks/hour | 5,000 | 7,500-10,000 | 50-100% |
| Tier 3 duration | 17.7h | 10-12h | 30-45% |
| I/O wait time | ~15% | <5% | 67% reduction |

### Cache Performance (After 60 days of operation)
| Metric | Target | Impact |
|--------|--------|--------|
| Cache hit rate | 30-40% | High reuse queries |
| API calls saved | 40-50% | Major cost savings |
| Result delivery | <5 seconds | Instant vs hours |
| Storage growth | ~50GB/month | Manageable |

---

## PART 5: RISK MITIGATION

### Speed Optimization Risks
| Risk | Mitigation |
|------|------------|
| API rate limit increase | Gradual concurrency ramp-up with monitoring |
| Memory exhaustion | Profile memory, add limits |
| Deadzone misclassification | Track and review skipped areas |

### Cache Risks
| Risk | Mitigation |
|------|------------|
| Stale data delivery | Clear 60-day expiry, user choice |
| Storage overflow | Automatic cleanup, size limits |
| Cache key collisions | Robust hash function, testing |
| Performance degradation | Indexed lookups, async writes |

---

## PART 6: SUCCESS METRICS

### Technical Metrics
- ✅ Average job completion time <12 hours (Tier 3)
- ✅ Cache hit rate >30% after 60 days
- ✅ API calls reduced by >40%
- ✅ System uptime >99.9%

### Business Metrics
- ✅ User satisfaction (instant results)
- ✅ API cost reduction
- ✅ Query volume increase (faster = more queries)

---

## PART 7: CURRENT STATUS CHECKLIST

### What We Have ✅
- [x] Job storage in jobs.db
- [x] CSV output with full results
- [x] Query and region parameters stored
- [x] Created_at timestamps
- [x] Result counts tracked

### What We Need ❌
- [ ] Cache identification (hash keys)
- [ ] Cache result storage layer
- [ ] Cache expiry logic
- [ ] User-facing cache toggle
- [ ] Cache statistics tracking
- [ ] Automatic cleanup mechanism

---

## NEXT STEPS

### Immediate (This Week)
1. ✅ Test concurrency increase (8 → 12)
2. ✅ Design cache database schema
3. ✅ Prototype cache key generation

### Short Term (Next 2 Weeks)
1. ✅ Implement adaptive zoom strategy
2. ✅ Build cache backend
3. ✅ Add cache maintenance jobs

### Medium Term (Next Month)
1. ✅ Frontend cache integration
2. ✅ Performance optimization
3. ✅ Load testing and validation

---

**Status**: Planning Complete
**Ready for Implementation**: Yes
**Risk Level**: Low-Medium
**Expected ROI**: High (40-50% API savings, 50% speed improvement)
