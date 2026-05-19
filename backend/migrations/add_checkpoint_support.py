#!/usr/bin/env python3
"""
Migration script to add checkpoint support for incremental resume.

Adds:
- job_checkpoints table: tracks processed row indices per job
- restart_count column on jobs table: tracks number of restarts

Run: python backend/migrations/add_checkpoint_support.py
"""

import sqlite3
from pathlib import Path


def get_db_path():
    """Get the database path."""
    return Path(__file__).parent.parent / "data" / "jobs.db"


def get_db():
    """Get database connection."""
    db_path = get_db_path()
    return sqlite3.connect(db_path)


def migrate():
    """Add checkpoint support to database."""
    conn = get_db()
    cursor = conn.cursor()

    print("Checking current schema...")

    # Check existing tables and columns
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    existing_tables = {row[0] for row in cursor.fetchall()}

    cursor.execute("PRAGMA table_info(jobs)")
    existing_columns = {row[1] for row in cursor.fetchall()}

    changes_made = []

    # 1. Add job_checkpoints table if it doesn't exist
    if "job_checkpoints" not in existing_tables:
        print("Creating job_checkpoints table...")
        cursor.execute("""
            CREATE TABLE job_checkpoints (
                job_id TEXT NOT NULL,
                row_index INTEGER NOT NULL,
                processed_at TEXT NOT NULL,
                PRIMARY KEY (job_id, row_index)
            )
        """)
        cursor.execute("""
            CREATE INDEX idx_checkpoints_job ON job_checkpoints(job_id)
        """)
        changes_made.append("job_checkpoints table")
        print("  ✓ Created job_checkpoints table with index")
    else:
        print("✓ job_checkpoints table already exists")

    # 2. Add restart_count column to jobs table if it doesn't exist
    if "restart_count" not in existing_columns:
        print("Adding restart_count column to jobs table...")
        cursor.execute("""
            ALTER TABLE jobs ADD COLUMN restart_count INTEGER DEFAULT 0
        """)
        changes_made.append("restart_count column")
        print("  ✓ Added restart_count column")
    else:
        print("✓ restart_count column already exists")

    conn.commit()

    if changes_made:
        print(f"\n✓ Migration completed! Added: {', '.join(changes_made)}")
    else:
        print("\n✓ No changes needed - checkpoint support already exists")


if __name__ == "__main__":
    migrate()
