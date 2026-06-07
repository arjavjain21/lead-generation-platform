-- Migration: 001_cache_tables
-- Description: Cache and checkpoint infrastructure
-- Created: 2026-06-07

-- Main cache table with 90-day freshness
CREATE TABLE IF NOT EXISTS scraped_cache (
    cache_id TEXT PRIMARY KEY,
    query TEXT NOT NULL,
    region_signature TEXT NOT NULL,
    regions TEXT NOT NULL,
    zoom_signature TEXT NOT NULL,
    expected_types_signature TEXT NOT NULL,
    total_results INTEGER NOT NULL,
    result_file_path TEXT NOT NULL,

    -- Cache management
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    last_accessed_at TEXT,
    access_count INTEGER DEFAULT 1,
    is_partial BOOLEAN DEFAULT 0,
    percentage_complete REAL DEFAULT 100.0,

    -- Data integrity
    checksum TEXT,
    status TEXT NOT NULL DEFAULT 'active',

    -- Job reference
    job_id TEXT,
    user_id TEXT
);

-- Indexes for efficient lookups
CREATE INDEX IF NOT EXISTS idx_cache_lookup
    ON scraped_cache(query, region_signature, zoom_signature, expected_types_signature, expires_at, status);
CREATE INDEX IF NOT EXISTS idx_cache_expiry
    ON scraped_cache(expires_at, status);

-- Add parent cache support (for subset entries)
-- These columns will be added when subset logic is implemented
-- ALTER TABLE scraped_cache ADD COLUMN parent_cache_id TEXT;
-- ALTER TABLE scraped_cache ADD COLUMN is_subset BOOLEAN DEFAULT 0;
-- CREATE INDEX IF NOT EXISTS idx_cache_parent ON scraped_cache(parent_cache_id);

-- Per-center result counts for subset queries
CREATE TABLE IF NOT EXISTS cache_center_counts (
    cache_id TEXT NOT NULL,
    center_id TEXT NOT NULL,
    center_name TEXT NOT NULL,
    center_state TEXT NOT NULL,
    result_count INTEGER NOT NULL,
    PRIMARY KEY (cache_id, center_id)
);

CREATE INDEX IF NOT EXISTS idx_center_counts_cache
    ON cache_center_counts(cache_id);

-- Task-level checkpoints for resume capability
CREATE TABLE IF NOT EXISTS task_checkpoints (
    job_id TEXT NOT NULL,
    center_name TEXT NOT NULL,
    center_state TEXT NOT NULL,
    zoom INTEGER NOT NULL,
    completed_at TEXT NOT NULL,
    result_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (job_id, center_name, center_state, zoom)
);

CREATE INDEX IF NOT EXISTS idx_task_checkpoints_job
    ON task_checkpoints(job_id);
CREATE INDEX IF NOT EXISTS idx_task_checkpoints_resume
    ON task_checkpoints(job_id, center_name, center_state, zoom);

-- Cache statistics
CREATE TABLE IF NOT EXISTS cache_stats (
    date TEXT PRIMARY KEY,
    cache_hits INTEGER DEFAULT 0,
    cache_misses INTEGER DEFAULT 0,
    api_calls_saved INTEGER DEFAULT 0,
    results_served INTEGER DEFAULT 0
);

-- Add resumable flag to jobs table (skip if exists)
-- These columns may already exist from previous migration attempts
BEGIN;

-- Create columns if they don't exist
-- Note: SQLite doesn't support IF NOT EXISTS for ALTER TABLE ADD COLUMN
-- Check if column exists first
-- SQLite doesn't have a simple way, so we'll use a try-catch approach
-- For now, if these fail, it's because they already exist

-- The following statements may fail if columns already exist - that's okay
-- ALTER TABLE jobs ADD COLUMN is_resumable INTEGER DEFAULT 1;
-- ALTER TABLE jobs ADD COLUMN checkpoint_count INTEGER DEFAULT 0;

COMMIT;
