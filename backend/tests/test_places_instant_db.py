"""Tests for the Instant US Database (GetLeads places) source — phase 1
(2026-11-11): client filter sanitization, row normalization, job creation
(running-at-birth, dispatcher-safe), the run_places_job driver paths, restart
re-arm CAS, and the /api/scraper/places/* route surface.
"""

import asyncio
import csv
import json
import sqlite3
import sys
import tempfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from scraper import places_client, places_flow, places_routes
from scraper.places_client import PlacesError

# ---------------------------------------------------------------------------
# Temp DB fixture (schema = every column the flow + base store touch)
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE jobs (
    job_id TEXT PRIMARY KEY,
    user_id TEXT,
    job_type TEXT,
    status TEXT,
    query TEXT,
    regions TEXT,
    total_tasks INTEGER,
    done_tasks INTEGER DEFAULT 0,
    result_count INTEGER DEFAULT 0,
    source_type TEXT DEFAULT '',
    is_resumable INTEGER DEFAULT 1,
    group_name TEXT,
    display_name TEXT,
    created_at TEXT,
    updated_at TEXT,
    last_heartbeat TEXT,
    error TEXT,
    output_path TEXT,
    restart_count INTEGER DEFAULT 0,
    resume_claimed_at TEXT
);
CREATE TABLE job_events (
    job_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    payload TEXT NOT NULL,
    PRIMARY KEY (job_id, seq)
);
CREATE TABLE job_checkpoints (
    job_id TEXT NOT NULL,
    row_index INTEGER NOT NULL,
    processed_at TEXT NOT NULL,
    PRIMARY KEY (job_id, row_index)
);
"""


@pytest.fixture
def temp_store():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    conn.commit()

    from scraper import job_store as scraper_job_store

    store = scraper_job_store.ScraperJobStore(conn)

    def _get_store():
        return store

    # Route every get_store() the flow makes to the temp store.
    monkeypatch_target = scraper_job_store
    original = monkeypatch_target.get_store
    monkeypatch_target.get_store = _get_store
    try:
        yield store
    finally:
        monkeypatch_target.get_store = original
        conn.close()
        Path(db_path).unlink()


@pytest.fixture
def quota_ok(monkeypatch):
    monkeypatch.setattr(
        places_flow.db, "check_daily_request_limit",
        lambda **kwargs: (True, ""),
    )


FAIR_USE_OK = {"daily": {"cap": 50000, "used": 0, "remaining": 50000, "resets_at": "X"}}
FAIR_USE_SPENT = {"daily": {"cap": 50000, "used": 50000, "remaining": 0, "resets_at": "X"}}

USER = {"user_id": "u1", "is_admin": False}

FILTERS = {"categories": ["Roofing contractor"], "states": ["TX"]}


# ---------------------------------------------------------------------------
# sanitize_filters
# ---------------------------------------------------------------------------


class TestSanitizeFilters:
    def test_filterless_raises(self):
        with pytest.raises(ValueError):
            places_client.sanitize_filters({})

    def test_unknown_keys_dropped(self):
        cleaned = places_client.sanitize_filters({"categories": ["Dentist"], "evil": 1})
        assert "evil" not in cleaned
        assert cleaned == {"categories": ["Dentist"]}

    def test_empty_values_stripped(self):
        cleaned = places_client.sanitize_filters(
            {"categories": ["Dentist", "  "], "business_name": "", "zips": ["78701-1234"]}
        )
        assert cleaned == {"categories": ["Dentist"], "zips": ["78701"]}

    def test_geo_only_is_valid(self):
        assert places_client.sanitize_filters({"states": ["TX"]}) == {"states": ["TX"]}

    def test_zip_plus4_normalized(self):
        cleaned = places_client.sanitize_filters({"business_name": "acme", "zips": ["78701-1234", "10001"]})
        assert cleaned["zips"] == ["78701", "10001"]


# ---------------------------------------------------------------------------
# Row normalization / CSV columns
# ---------------------------------------------------------------------------


class TestNormalizeRow:
    def test_website_column_present(self):
        assert "website" in places_flow.CSV_COLUMNS  # chain endpoint gates on it

    def test_mapping_and_extra_emails(self):
        row = places_flow._normalize_row({
            "business_name": "Acme Roofing", "category": "Roofing contractor",
            "phone": "512-555-0100", "website": "https://acme.com/about",
            "primary_email": "info@acme.com", "email_type": "Generic",
            "email_2": "a@acme.com", "email_3": "", "email_4": "b@acme.com",
            "street_address": "1 Main St", "city": "Austin", "state": "TX", "zip": "78701",
            "rating": "4.8", "review_count": "31", "google_maps_url": "https://maps/x",
        })
        assert row["name"] == "Acme Roofing"
        assert row["website"] == "https://acme.com/about"
        assert row["email"] == "info@acme.com"
        assert row["additional_emails"] == "a@acme.com; b@acme.com"
        assert row["full_address"] == "1 Main St, Austin, TX, 78701"


# ---------------------------------------------------------------------------
# Job creation
# ---------------------------------------------------------------------------


class TestCreateAndLaunch:
    def test_row_born_running_not_queued(self, temp_store, quota_ok, monkeypatch, tmp_path):
        async def _noop(job_id, **kwargs):
            return None

        monkeypatch.setattr(places_flow, "run_places_job", _noop)
        monkeypatch.setattr(places_flow, "OUTPUT_DIR", tmp_path)
        monkeypatch.setattr(places_client, "API_KEY", "test-key")

        async def go():
            result = places_flow.create_and_launch(
                user=USER, filters=dict(FILTERS), group="test-group"
            )
            await asyncio.sleep(0)  # let the spawned no-op driver task run
            return result

        result = asyncio.run(go())
        job = temp_store.get_job(result["job_id"])
        # NEVER queued — a queued row would be claimed by the scraper
        # dispatcher and grid-scraped by scraper.tech.
        assert job["status"] == "running"
        assert job["source_type"] == places_flow.SOURCE_TYPE
        assert job["job_type"] == "scraper"
        assert job["is_resumable"] == 0
        assert job["group_name"] == "test-group"
        assert "Instant DB" in job["display_name"]
        regions = json.loads(job["regions"])
        assert regions["filters"] == FILTERS

    def test_kill_switch_blocks_creation(self, temp_store, quota_ok, monkeypatch):
        monkeypatch.setattr(places_client, "API_KEY", "test-key")
        monkeypatch.setenv("ENABLE_GETLEADS_PLACES", "false")
        try:
            with pytest.raises(PlacesError):
                places_flow.create_and_launch(user=USER, filters=dict(FILTERS))
        finally:
            monkeypatch.delenv("ENABLE_GETLEADS_PLACES")

    def test_quota_denied_raises(self, temp_store, monkeypatch):
        monkeypatch.setattr(places_client, "API_KEY", "test-key")
        monkeypatch.setattr(
            places_flow.db, "check_daily_request_limit",
            lambda **kwargs: (False, "Daily limit reached"),
        )
        with pytest.raises(ValueError):
            places_flow.create_and_launch(user=USER, filters=dict(FILTERS))


def _make_places_job(store, job_id="j1", filters=None, status="running"):
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc).isoformat()
    store.conn.execute(
        """INSERT INTO jobs (job_id, user_id, job_type, status, query, regions,
               total_tasks, source_type, is_resumable, created_at, updated_at)
           VALUES (?, 'u1', 'scraper', ?, 'label', ?, 3, ?, 0, ?, ?)""",
        (job_id, status, json.dumps({"mode": "places", "filters": filters or FILTERS,
                                     "places": {"export_id": None, "expected": None}}),
         places_flow.SOURCE_TYPE, now, now),
    )
    store.conn.commit()


def _getleads_csv_payload():
    header = "business_name,category,phone,website,primary_email,email_type,street_address,city,state,zip,rating,review_count,google_maps_url,email_2"
    row = "Acme,Roofing contractor,512,https://acme.com,info@acme.com,Generic,1 Main,Austin,TX,78701,4.8,31,https://maps/x,a@acme.com"
    return ([h for h in header.split(",")],
            [dict(zip(header.split(","), row.split(",")))])


# ---------------------------------------------------------------------------
# run_places_job driver paths
# ---------------------------------------------------------------------------


class TestRunPlacesJob:
    def _patch_client(self, monkeypatch, *, fair_use=FAIR_USE_OK, count=100,
                      export_rows=None):
        export_rows = export_rows or [dict(
            business_name="Acme", category="Roofing contractor", phone="512",
            website="https://acme.com", primary_email="info@acme.com",
            email_type="Generic", street_address="1 Main", city="Austin",
            state="TX", zip="78701", rating="4.8", review_count="31",
        )]
        async def _fair_use():
            return fair_use

        async def _count(filters):
            return count

        async def _start(filters):
            return "exp-1"

        async def _poll(export_id):
            return {"job_status": "completed", "export_url": "https://s3/x.csv",
                    "rows_exported": len(export_rows)}

        async def _download(url):
            return ["business_name"], export_rows

        monkeypatch.setattr(places_flow.places_client, "fair_use", _fair_use)
        monkeypatch.setattr(places_flow.places_client, "count_places", _count)
        monkeypatch.setattr(places_flow.places_client, "start_export", _start)
        monkeypatch.setattr(places_flow.places_client, "poll_export", _poll)
        monkeypatch.setattr(places_flow.places_client, "download_export_csv", _download)
        monkeypatch.setattr(places_flow.places_client, "EXPORT_POLL_INTERVAL_S", 0.01)

    def test_happy_path_writes_csv_and_marks_done(self, temp_store, monkeypatch, tmp_path):
        self._patch_client(monkeypatch)
        monkeypatch.setattr(places_flow, "OUTPUT_DIR", tmp_path)
        _make_places_job(temp_store, "j1")

        asyncio.run(places_flow.run_places_job("j1"))

        job = temp_store.get_job("j1")
        assert job["status"] == "done"
        assert job["result_count"] == 1
        csv_path = Path(job["output_path"])
        assert csv_path.exists()
        with open(csv_path) as f:
            rows = list(csv.DictReader(f))
        assert rows[0]["website"] == "https://acme.com"  # chain-compat column
        assert rows[0]["email"] == "info@acme.com"

    def test_budget_exhausted_fails_with_reset_hint(self, temp_store, monkeypatch, tmp_path):
        self._patch_client(monkeypatch, fair_use=FAIR_USE_SPENT)
        monkeypatch.setattr(places_flow, "OUTPUT_DIR", tmp_path)
        _make_places_job(temp_store, "j2")

        asyncio.run(places_flow.run_places_job("j2"))

        job = temp_store.get_job("j2")
        assert job["status"] == "failed"
        assert "budget is used up" in job["error"]
        assert "resets" in job["error"]

    def test_oversize_fails_with_narrow_hint(self, temp_store, monkeypatch, tmp_path):
        self._patch_client(monkeypatch, count=30_000)
        monkeypatch.setattr(places_flow, "OUTPUT_DIR", tmp_path)
        _make_places_job(temp_store, "j3")

        asyncio.run(places_flow.run_places_job("j3"))

        job = temp_store.get_job("j3")
        assert job["status"] == "failed"
        assert "25,000" in job["error"]

    def test_zero_matches_writes_empty_csv_done(self, temp_store, monkeypatch, tmp_path):
        self._patch_client(monkeypatch, count=0)
        monkeypatch.setattr(places_flow, "OUTPUT_DIR", tmp_path)
        _make_places_job(temp_store, "j4")

        asyncio.run(places_flow.run_places_job("j4"))

        job = temp_store.get_job("j4")
        assert job["status"] == "done"
        assert job["result_count"] == 0

    def test_cancelled_job_returns_without_api_calls(self, temp_store, monkeypatch, tmp_path):
        called = {"count": False}

        async def _count(filters):
            called["count"] = True
            return 5

        monkeypatch.setattr(places_flow.places_client, "fair_use",
                            lambda: asyncio.sleep(0, FAIR_USE_OK))
        monkeypatch.setattr(places_flow.places_client, "count_places", _count)
        monkeypatch.setattr(places_flow, "OUTPUT_DIR", tmp_path)
        _make_places_job(temp_store, "j5", status="cancelled")

        asyncio.run(places_flow.run_places_job("j5"))

        assert called["count"] is False  # bailed before phase 1 spent anything

    def test_export_failure_marks_failed(self, temp_store, monkeypatch, tmp_path):
        self._patch_client(monkeypatch)

        async def _poll(export_id):
            return {"job_status": "failed"}

        monkeypatch.setattr(places_flow.places_client, "poll_export", _poll)
        monkeypatch.setattr(places_flow, "OUTPUT_DIR", tmp_path)
        _make_places_job(temp_store, "j6")

        asyncio.run(places_flow.run_places_job("j6"))

        job = temp_store.get_job("j6")
        assert job["status"] == "failed"
        assert "GetLeads export failed" in job["error"]


# ---------------------------------------------------------------------------
# Restart re-arm CAS
# ---------------------------------------------------------------------------


class TestRearm:
    def test_exactly_one_claim_wins(self, temp_store, monkeypatch):
        rearm_calls = []

        async def _fake_run(job_id, **kwargs):
            rearm_calls.append(job_id)

        monkeypatch.setattr(places_flow, "run_places_job", _fake_run)
        _make_places_job(temp_store, "r1")

        async def go():
            first = places_flow.rearm_running_places_jobs()
            second = places_flow.rearm_running_places_jobs()  # second worker
            await asyncio.sleep(0)  # let spawned tasks run
            return first, second

        first, second = asyncio.run(go())
        assert first == 1
        assert second == 0  # CAS held — the other workers skip
        assert rearm_calls == ["r1"]


# ---------------------------------------------------------------------------
# Routes (minimal app + auth override)
# ---------------------------------------------------------------------------


class TestPlacesRoutes:
    @pytest.fixture
    def client(self, monkeypatch):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from shared import auth

        monkeypatch.setattr(places_routes.places_client, "API_KEY", "test-key")
        app = FastAPI()
        app.include_router(places_routes.router)
        app.dependency_overrides[auth.get_current_user] = lambda: USER
        return TestClient(app)

    def test_categories_pass_through(self, client, monkeypatch):
        async def _cats(q="", page=1):
            return {"categories": ["Roofing contractor"], "total": 1, "has_more": False}

        monkeypatch.setattr(places_routes.places_client, "search_categories", _cats)
        res = client.get("/api/scraper/places/categories?q=roofing")
        assert res.status_code == 200
        assert res.json()["categories"] == ["Roofing contractor"]

    def test_count_requires_a_filter(self, client):
        res = client.post("/api/scraper/places/count", json={"filters": {}})
        assert res.status_code == 400

    def test_count_returns_budget(self, client, monkeypatch):
        async def _count(filters):
            return 12_677

        async def _fu():
            return FAIR_USE_OK

        monkeypatch.setattr(places_routes.places_client, "count_places", _count)
        monkeypatch.setattr(places_routes.places_client, "fair_use", _fu)
        res = client.post("/api/scraper/places/count",
                          json={"filters": {"categories": ["Roofing contractor"], "states": ["TX"]}})
        assert res.status_code == 200
        body = res.json()
        assert body["total_matching"] == 12_677
        assert body["fair_use"]["daily_remaining"] == 50000

    def test_kill_switch_503s(self, client, monkeypatch):
        monkeypatch.setenv("ENABLE_GETLEADS_PLACES", "false")
        try:
            res = client.get("/api/scraper/places/categories?q=x")
            assert res.status_code == 503
        finally:
            monkeypatch.delenv("ENABLE_GETLEADS_PLACES")
