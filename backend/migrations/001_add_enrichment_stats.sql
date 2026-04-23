-- Migration: 001_add_enrichment_stats
-- Description: Add enrichment_stats table for tracking API usage by source provider
-- and convenience columns on jobs table for email counts by source
-- Created: 2026-04-23

-- Create enrichment_stats table for source tracking
CREATE TABLE IF NOT EXISTS enrichment_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL,
    user_id TEXT,
    source TEXT NOT NULL,          -- 'contacts_db', 'blitz', 'better_enrich', 'prospeo'
    emails_count INTEGER DEFAULT 0,
    contacts_count INTEGER DEFAULT 0,
    created_at TEXT NOT NULL,
    UNIQUE(job_id, source)
);

CREATE INDEX IF NOT EXISTS idx_stats_job ON enrichment_stats (job_id);
CREATE INDEX IF NOT EXISTS idx_stats_user_date ON enrichment_stats (user_id, created_at);
CREATE INDEX IF NOT EXISTS idx_stats_source ON enrichment_stats (source);

-- Add source columns to jobs table for convenience
ALTER TABLE jobs ADD COLUMN emails_contacts_db INTEGER DEFAULT 0;
ALTER TABLE jobs ADD COLUMN emails_blitz INTEGER DEFAULT 0;
ALTER TABLE jobs ADD COLUMN emails_better_enrich INTEGER DEFAULT 0;
ALTER TABLE jobs ADD COLUMN emails_prospeo INTEGER DEFAULT 0;
