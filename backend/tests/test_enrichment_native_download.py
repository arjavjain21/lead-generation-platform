"""Native (browser download manager) CSV downloads for enrichment jobs (2026-09-25).

Contract under test:

    GET /api/enrichment/jobs/{id}/download, /recover-partial, /shard/{shard}
    accept X-API-Key, Bearer JWT, OR ?token= JWT. Browser-native downloads
    (anchor click → download manager) cannot send Authorization headers, and
    the old fetch()+blob() path truncated 50-100MB CSVs mid-stream in the
    browser ("Failed to fetch"). Mirrors the scraper fix (c1adcde).

    /recover-partial also answers HEAD (the frontend's partial-download
    button preflights with HEAD so a just-started job gets the friendly
    "No partial file available yet." alert instead of a dead download).

    /shards must list shards covering ALL rows on disk: enrichment `total`
    counts INPUT rows (domains) while the CSV holds OUTPUT contact rows —
    often several per domain — so the basis is max(total, rows_on_disk).

Job-store isolation: patches ``routes.job_store.get_store`` to a store over a
throwaway temp DB (pattern: test_enrichment_api_key_auth) and patches
``routes.OUTPUT_DIR`` to a tmp_path so no live CSVs are touched.
"""
from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from shared import auth as _auth  # noqa: E402

OWNER_UID = "native-dl-owner"

# Minimal jobs schema — the columns the download endpoints read.
SCHEMA = """
CREATE TABLE jobs (
    job_id TEXT PRIMARY KEY,
    user_id TEXT,
    job_type TEXT,
    status TEXT,
    original_filename TEXT DEFAULT '',
    filename TEXT DEFAULT '',
    total INTEGER DEFAULT 0,
    output_path TEXT,
    created_at TEXT,
    updated_at TEXT
);
"""


@pytest.fixture
def temp_db():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    conn.commit()
    yield conn
    conn.close()
    Path(db_path).unlink(missing_ok=True)


@pytest.fixture
def store(temp_db, monkeypatch, tmp_path):
    """Patch routes.job_store.get_store + routes.OUTPUT_DIR to throwaway paths."""
    from enrichment import routes
    from enrichment.job_store import EnrichmentJobStore

    monkeypatch.setattr(routes.job_store, "get_store", lambda: EnrichmentJobStore(temp_db))
    monkeypatch.setattr(routes, "OUTPUT_DIR", tmp_path)
    yield temp_db


def _insert_job(conn, job_id: str, *, status: str = "running", total: int = 0,
                output_path: str | None = None):
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO jobs (job_id, user_id, job_type, status, original_filename,"
        " filename, total, output_path, created_at, updated_at)"
        " VALUES (?,?,?,?,?,?,?,?,?,?)",
        (job_id, OWNER_UID, "enrichment", status, "leads.csv", "leads.csv",
         total, output_path, now, now),
    )
    conn.commit()


def _write_csv(path: Path, data_rows: int) -> None:
    lines = ["Company Domain,Contact Email,Full Name\n"]
    for i in range(data_rows):
        lines.append(f"example{i}.com,dm{i}@example{i}.com,Person {i}\n")
    path.write_text("".join(lines), encoding="utf-8")


@pytest.fixture
def client():
    from fastapi.testclient import TestClient
    from main import app

    app.dependency_overrides.clear()
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


@pytest.fixture
def owner_token():
    # Real signed JWT — exercises decode_token end-to-end, no overrides.
    return _auth.create_token({"user_id": OWNER_UID, "email": "owner@test", "is_admin": 0})


class TestTokenQueryAuth:
    """?token= JWT (no headers) must authenticate the three download endpoints."""

    def _job_with_csv(self, store, tmp_path, *, status="running", via_output_path=False):
        job_id = "native-dl-1"
        csv_path = tmp_path / f"{job_id}.csv"
        _write_csv(csv_path, 20)
        _insert_job(
            store, job_id, status=status, total=100,
            output_path=str(csv_path) if (via_output_path or status == "done") else None,
        )
        return job_id, csv_path

    def test_recover_partial_via_token(self, store, tmp_path, client, owner_token):
        job_id, csv_path = self._job_with_csv(store, tmp_path)
        r = client.get(f"/api/enrichment/jobs/{job_id}/recover-partial?token={owner_token}")
        assert r.status_code == 200, r.text
        assert "partial_leads.csv" in r.headers.get("content-disposition", "")
        assert r.text.count("@example") == 20

    def test_recover_partial_head_preflight(self, store, tmp_path, client, owner_token):
        job_id, _ = self._job_with_csv(store, tmp_path)
        r = client.head(f"/api/enrichment/jobs/{job_id}/recover-partial?token={owner_token}")
        assert r.status_code == 200
        assert r.headers.get("content-type", "").startswith("text/csv")
        # HEAD must not ship the body
        assert r.content == b""

    def test_recover_partial_head_404_when_no_file(self, store, client, owner_token):
        _insert_job(store, "native-dl-empty", status="running", total=100)
        r = client.head(f"/api/enrichment/jobs/native-dl-empty/recover-partial?token={owner_token}")
        assert r.status_code == 404

    def test_shard_download_via_token(self, store, tmp_path, client, owner_token):
        job_id, _ = self._job_with_csv(store, tmp_path)
        r = client.get(f"/api/enrichment/jobs/{job_id}/shard/0?token={owner_token}")
        assert r.status_code == 200, r.text
        assert "shard_0_" in r.headers.get("content-disposition", "")
        # header row + all 20 rows (shard larger than the file)
        assert len(r.text.strip().splitlines()) == 21

    def test_full_download_via_token(self, store, tmp_path, client, owner_token):
        job_id, _ = self._job_with_csv(store, tmp_path, status="done", via_output_path=True)
        r = client.get(f"/api/enrichment/jobs/{job_id}/download?token={owner_token}")
        assert r.status_code == 200, r.text
        assert "enriched_" in r.headers.get("content-disposition", "")

    def test_bad_token_is_401(self, store, client):
        _insert_job(store, "native-dl-bad", status="running", total=100)
        r = client.get("/api/enrichment/jobs/native-dl-bad/recover-partial?token=not.a.jwt")
        assert r.status_code == 401

    def test_other_users_job_is_403(self, store, client):
        _insert_job(store, "native-dl-foreign", status="running", total=100)
        other = _auth.create_token({"user_id": "someone-else", "email": "x@t", "is_admin": 0})
        r = client.get(f"/api/enrichment/jobs/native-dl-foreign/recover-partial?token={other}")
        assert r.status_code == 403


class TestShardsBasisCoversRowsOnDisk:
    """Enrichment CSVs hold contact rows (several per input domain) — the
    shard list must cover rows_on_disk, not just `total` input rows."""

    def test_rows_on_disk_exceeding_total_lists_all_shards(self, store, tmp_path, client, owner_token):
        # 14,200 input domains but 25,000 contact rows on disk (the 2026-09-25
        # bug: only 2 shards were listed, hiding 10K+ rows from the chunk UI).
        job_id = "native-dl-shards"
        _write_csv(tmp_path / f"{job_id}.csv", 25_000)
        _insert_job(store, job_id, status="running", total=14_200)
        # /shards stays a fetch()-with-headers JSON listing (no ?token=)
        r = client.get(
            f"/api/enrichment/jobs/{job_id}/shards",
            headers={"Authorization": f"Bearer {owner_token}"},
        )
        assert r.status_code == 200
        d = r.json()
        assert d["rows_on_disk"] == 25_000
        shards = d["shards"]
        assert len(shards) == 3  # ceil(25000/10000)
        assert shards[0]["end_row"] == 10_000 and shards[0]["complete"]
        assert shards[2]["start_row"] == 20_000 and shards[2]["end_row"] == 25_000
        assert shards[2]["rows_available"] == 5_000 and shards[2]["complete"]

    def test_early_job_keeps_forward_looking_shards(self, store, tmp_path, client, owner_token):
        # 25,000 input rows but only 5,000 written: shard 0 half-ready, shards
        # 1-2 listed-but-disabled (forward visibility while the job runs).
        job_id = "native-dl-early"
        _write_csv(tmp_path / f"{job_id}.csv", 5_000)
        _insert_job(store, job_id, status="running", total=25_000)
        r = client.get(
            f"/api/enrichment/jobs/{job_id}/shards",
            headers={"Authorization": f"Bearer {owner_token}"},
        )
        d = r.json()
        assert len(d["shards"]) == 3
        assert d["shards"][0]["rows_available"] == 5_000 and not d["shards"][0]["complete"]
        assert d["shards"][1]["rows_available"] == 0 and not d["shards"][1]["complete"]
