#!/usr/bin/env python3
"""
Migration script to add restart support columns for enrichment jobs.

Adds the following columns to the jobs table:
- name_col: Column name containing person/company name
- first_name_col: Column name containing first name
- last_name_col: Column name containing last name
- cascade_config: ICP cascade configuration (JSON string)
- max_results: Maximum decision makers to find per domain

Run this script to prepare the database for the restart feature.
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
    """Add restart support columns to jobs table."""
    conn = get_db()
    cursor = conn.cursor()

    print("Checking current schema...")

    # Check if columns already exist
    cursor.execute("PRAGMA table_info(jobs)")
    existing_columns = {row[1] for row in cursor.fetchall()}

    columns_to_add = {
        'name_col': 'TEXT',
        'first_name_col': 'TEXT',
        'last_name_col': 'TEXT',
        'cascade_config': 'TEXT',
        'max_results': 'INTEGER DEFAULT 5',
    }

    # Filter out columns that already exist
    new_columns = {k: v for k, v in columns_to_add.items() if k not in existing_columns}

    if not new_columns:
        print("✓ All restart support columns already exist.")
        return

    print(f"Adding {len(new_columns)} new columns to jobs table...")

    # Add each column
    for column_name, column_type in new_columns.items():
        try:
            cursor.execute(f"ALTER TABLE jobs ADD COLUMN {column_name} {column_type}")
            print(f"  ✓ Added column: {column_name} ({column_type})")
        except sqlite3.OperationalError as e:
            print(f"  ✗ Failed to add column {column_name}: {e}")
            raise

    conn.commit()
    print("\n✓ Migration completed successfully!")
    print("\nNew schema:")
    cursor.execute("PRAGMA table_info(jobs)")
    for row in cursor.fetchall():
        if row[1] in columns_to_add:
            print(f"  - {row[1]}: {row[2]}")

    print("\nThe database is now ready for the restart feature.")


def rollback():
    """Rollback the migration (requires recreating table)."""
    print("WARNING: Rolling back this migration requires recreating the jobs table.")
    print("This is a destructive operation that will lose all job data.")
    response = input("Are you sure you want to proceed? (type 'yes' to confirm): ")

    if response.lower() != 'yes':
        print("Rollback cancelled.")
        return

    print("\nRollback not implemented - would need to recreate table.")
    print("Instead, just ignore the new columns if you don't want to use them.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Add restart support to jobs table")
    parser.add_argument("--rollback", action="store_true", help="Rollback migration (not implemented)")
    args = parser.parse_args()

    if args.rollback:
        rollback()
    else:
        migrate()
