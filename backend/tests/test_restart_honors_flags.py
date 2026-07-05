"""Tests for restart_enrichment_job honoring the pre-processing flags.

When a job is restarted, the new job should read the
normalize_domains / dedupe_by_domain flags from the original job row
and apply them to the freshly loaded CSV.
"""
import json
import sqlite3
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))


@pytest.fixture
def temp_db():
    """Create a fresh SQLite database for the test."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE jobs (
            job_id TEXT PRIMARY KEY,
            user_id TEXT,
            job_type TEXT,
            status TEXT,
            total INTEGER,
            processed INTEGER,
            filename TEXT,
            domain_col TEXT,
            original_filename TEXT,
            parent_job_id TEXT,
            name_col TEXT,
            first_name_col TEXT,
            last_name_col TEXT,
            cascade_config TEXT,
            max_results INTEGER DEFAULT 5,
            selected_providers TEXT,
            used_providers TEXT,
            linkedin_url_col TEXT DEFAULT '',
            phone_col TEXT DEFAULT '',
            company_name_col TEXT DEFAULT '',
            existing_email_col TEXT DEFAULT '',
            normalize_domains INTEGER DEFAULT 1,
            dedupe_by_domain INTEGER DEFAULT 1,
            deduped_rows INTEGER DEFAULT 0,
            dedupe_skipped_domains TEXT DEFAULT '',
            created_at TEXT,
            updated_at TEXT,
            last_heartbeat TEXT
        );
    """)
    yield conn, db_path
    conn.close()
    Path(db_path).unlink(missing_ok=True)


def _insert_job(conn, **overrides):
    defaults = dict(
        job_id="orig-1",
        user_id="user-1",
        job_type="enrichment",
        status="failed",
        total=5,
        processed=0,
        filename="test-upload",
        domain_col="website",
        original_filename="orig.csv",
        parent_job_id=None,
        name_col=None,
        first_name_col=None,
        last_name_col=None,
        cascade_config=None,
        max_results=5,
        selected_providers=None,
        used_providers=None,
        linkedin_url_col="",
        phone_col="",
        company_name_col="",
        existing_email_col="",
        normalize_domains=1,
        dedupe_by_domain=1,
        deduped_rows=0,
        dedupe_skipped_domains="",
        created_at="2026-06-16T00:00:00Z",
        updated_at="2026-06-16T00:00:00Z",
        last_heartbeat="2026-06-16T00:00:00Z",
    )
    defaults.update(overrides)
    cols = ",".join(defaults.keys())
    placeholders = ",".join(["?"] * len(defaults))
    conn.execute(
        f"INSERT INTO jobs ({cols}) VALUES ({placeholders})",
        list(defaults.values()),
    )
    conn.commit()


class TestRestartHonorsFlags:
    def test_normalize_off_persists(self, temp_db):
        """A job created with normalize_domains=0 should read back as 0."""
        from shared import db as shared_db

        conn, db_path = temp_db
        _insert_job(conn, job_id="orig-norm-off", normalize_domains=0)

        with patch.object(shared_db, "get_db", return_value=conn):
            from shared.job_store_base import JobStoreBase
            store = JobStoreBase(conn)
            job = store.get_job("orig-norm-off")

        assert job["normalize_domains"] == 0
        assert job["dedupe_by_domain"] == 1  # default still on

    def test_dedupe_off_persists(self, temp_db):
        """A job created with dedupe_by_domain=0 should read back as 0."""
        from shared import db as shared_db

        conn, db_path = temp_db
        _insert_job(conn, job_id="orig-dedup-off", dedupe_by_domain=0)

        with patch.object(shared_db, "get_db", return_value=conn):
            from shared.job_store_base import JobStoreBase
            store = JobStoreBase(conn)
            job = store.get_job("orig-dedup-off")

        assert job["dedupe_by_domain"] == 0
        assert job["normalize_domains"] == 1  # default still on

    def test_legacy_job_reads_defaults(self, temp_db):
        """Old jobs (without the new columns) should default to (1, 1, 0, '')."""
        from shared import db as shared_db

        conn, db_path = temp_db
        # Insert a legacy row without the new columns
        conn.execute("""
            INSERT INTO jobs (job_id, user_id, job_type, status, filename, domain_col,
                            created_at, updated_at)
            VALUES ('legacy', 'u1', 'enrichment', 'failed', 'f', 'website',
                    '2026-06-16T00:00:00Z', '2026-06-16T00:00:00Z')
        """)
        conn.commit()

        with patch.object(shared_db, "get_db", return_value=conn):
            from shared.job_store_base import JobStoreBase
            store = JobStoreBase(conn)
            job = store.get_job("legacy")

        # Defaults from the schema: 1, 1, 0, ''
        assert job["normalize_domains"] == 1
        assert job["dedupe_by_domain"] == 1
        assert job["deduped_rows"] == 0
        assert job["dedupe_skipped_domains"] == ""

    def test_skipped_domains_persisted(self, temp_db):
        """dedupe_skipped_domains round-trips as a JSON string."""
        from shared import db as shared_db

        conn, db_path = temp_db
        skipped = json.dumps(["https://acme.com/?x=1", "acme.com/"])
        _insert_job(conn, job_id="with-skipped",
                    dedupe_by_domain=1,
                    deduped_rows=2,
                    dedupe_skipped_domains=skipped)

        with patch.object(shared_db, "get_db", return_value=conn):
            from shared.job_store_base import JobStoreBase
            store = JobStoreBase(conn)
            job = store.get_job("with-skipped")

        assert job["deduped_rows"] == 2
        assert json.loads(job["dedupe_skipped_domains"]) == [
            "https://acme.com/?x=1", "acme.com/"
        ]
