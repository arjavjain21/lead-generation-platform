-- Cache schema for scraped results
-- This table stores query + region combinations with their results for 60-day caching

CREATE TABLE IF NOT EXISTS scraped_cache (
    -- Cache identification
    cache_id TEXT PRIMARY KEY,  -- hash(query + region_signature)

    -- Query parameters
    query TEXT NOT NULL,
    region_signature TEXT NOT NULL,  -- hash of regions JSON
    regions TEXT NOT NULL,  -- Full regions JSON for reconstruction

    -- Result metadata
    total_results INTEGER NOT NULL,
    result_file_path TEXT NOT NULL,

    -- Cache management
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,  -- created_at + 60 days
    last_accessed_at TEXT,
    access_count INTEGER DEFAULT 1,
    is_partial BOOLEAN DEFAULT 0,  -- True if job was stopped mid-way
    percentage_complete REAL DEFAULT 100.0,  -- For partial results

    -- Data integrity
    checksum TEXT,  -- SHA256 of result file
    status TEXT NOT NULL DEFAULT 'active',  -- 'active', 'expired', 'deleted'

    -- Job reference
    job_id TEXT,  -- Original job ID that created this cache entry
    user_id TEXT  -- User who created the cache entry
);

-- Indexes for efficient lookups
CREATE INDEX IF NOT EXISTS idx_cache_lookup ON scraped_cache(query, region_signature, expires_at, status);
CREATE INDEX IF NOT EXISTS idx_cache_expiry ON scraped_cache(expires_at, status);
CREATE INDEX IF NOT EXISTS idx_cache_query ON scraped_cache(query, status);
CREATE INDEX IF NOT EXISTS idx_cache_job ON scraped_cache(job_id);

-- Cache statistics table
CREATE TABLE IF NOT EXISTS cache_stats (
    date TEXT PRIMARY KEY,
    cache_hits INTEGER DEFAULT 0,
    cache_misses INTEGER DEFAULT 0,
    api_calls_saved INTEGER DEFAULT 0,
    results_served INTEGER DEFAULT 0
);