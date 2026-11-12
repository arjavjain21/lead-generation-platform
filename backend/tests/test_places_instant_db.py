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
def temp_store(monkeypatch, tmp_path):
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

    # Route every get_store() the flow makes to the temp store, and redirect
    # direct db.get_db() calls (cache layer) to the same temp connection —
    # tests must never touch the live jobs.db.
    monkeypatch.setattr(scraper_job_store, "get_store", _get_store)
    monkeypatch.setattr(places_flow.db, "get_db", lambda: conn)
    monkeypatch.setattr(places_flow, "CACHE_DIR", tmp_path / "places_cache")
    yield store
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


# ---------------------------------------------------------------------------
# Phase 2: cities→ZIP resolution, partition planning, result cache
# ---------------------------------------------------------------------------


class TestResolveFilters:
    def test_cities_expanded_to_zips(self):
        resolved, notes = places_flow.resolve_filters(
            {"categories": ["Dentist"], "cities": ["Austin"]}
        )
        assert "cities" not in resolved
        assert resolved["zips"], "expected Austin to resolve to ZIPs"
        assert resolved["categories"] == ["Dentist"]
        assert any("ZIPs" in n for n in notes)

    def test_unknown_city_raises(self):
        with pytest.raises(ValueError):
            places_flow.resolve_filters({"cities": ["Definitely Not A City 9x9z"]})

    def test_no_cities_passthrough(self):
        resolved, notes = places_flow.resolve_filters({"zips": ["78701"]})
        assert resolved == {"zips": ["78701"]}
        assert notes == []


class TestPlanPartitions:
    def test_small_pull_single_partition(self):
        parts = asyncio.run(places_flow.plan_partitions(
            {"categories": ["Dentist"], "zips": ["78701"]}, 100,
            count_fn=lambda f: asyncio.sleep(0, 100),
        ))
        assert len(parts) == 1

    def test_big_pull_bisects_until_fits(self):
        # 8 ZIPs, counts: total 40K; halves 20K+20K → each at target → 2 parts.
        async def fake_count(filters):
            if len(filters.get("zips", [])) == 8:
                return 40_000
            return 10_000 if len(filters.get("zips", [])) == 4 else 20_000

        parts = asyncio.run(places_flow.plan_partitions(
            {"categories": ["Dentist"], "zips": [str(10000 + i) for i in range(8)]},
            40_000, count_fn=fake_count,
        ))
        assert len(parts) == 2
        all_zips = sorted(z for p in parts for z in p["zips"])
        assert all_zips == sorted(str(10000 + i) for i in range(8))  # disjoint cover

    def test_partitions_carry_base_filters(self):
        async def fake_count(filters):
            return 10_000

        parts = asyncio.run(places_flow.plan_partitions(
            {"categories": ["Dentist"], "has_email": True,
             "zips": [str(20000 + i) for i in range(4)]}, 40_000, count_fn=fake_count,
        ))
        assert all(p["categories"] == ["Dentist"] and p["has_email"] is True for p in parts)

    def test_no_geo_axis_expands_states(self):
        # No zips/states: partitioner expands every state's ZIPs via us_zips.
        async def fake_count(filters):
            # Half the country fits → exactly 2 partitions.
            return 10_000 if len(filters.get("zips", [])) < 40000 else 40_000

        parts = asyncio.run(places_flow.plan_partitions(
            {"categories": ["Dentist"]}, 40_000, count_fn=fake_count,
        ))
        assert 1 <= len(parts) <= places_flow.MAX_PARTITIONS
        for p in parts:
            assert p["zips"], "partitions must be ZIP-keyed"


class TestPlacesCache:
    def test_store_then_hit(self, temp_store, tmp_path):
        filters = {"categories": ["Dentist"], "zips": ["78701"]}
        src = tmp_path / "src.csv"
        src.write_text("name,website\nAcme,https://acme.com\n")

        assert places_flow.places_cache_lookup(filters) is None
        key = places_flow.places_cache_store(filters, src, 1)
        hit = places_flow.places_cache_lookup(filters)
        assert hit is not None
        assert hit["row_count"] == 1
        assert Path(hit["csv_path"]).exists()
        assert places_flow.places_cache_key(filters) == key

    def test_key_ignores_dict_ordering(self):
        a = places_flow.places_cache_key({"zips": ["78701"], "categories": ["Dentist"]})
        b = places_flow.places_cache_key({"categories": ["Dentist"], "zips": ["78701"]})
        assert a == b

    def test_cache_hit_job_short_circuits_export(self, temp_store, monkeypatch, tmp_path):
        calls = {"start": 0}

        async def _start(filters):
            calls["start"] += 1
            return "exp-never"

        monkeypatch.setattr(places_flow.places_client, "fair_use",
                            lambda: asyncio.sleep(0, FAIR_USE_OK))
        monkeypatch.setattr(places_flow.places_client, "count_places",
                            lambda f: asyncio.sleep(0, 999))
        monkeypatch.setattr(places_flow.places_client, "start_export", _start)
        monkeypatch.setattr(places_flow, "OUTPUT_DIR", tmp_path)

        cached_src = tmp_path / "cached.csv"
        cached_src.write_text(
            "name,website,email\nAcme,https://acme.com,info@acme.com\n"
        )
        places_flow.places_cache_store(FILTERS, cached_src, 1)

        _make_places_job(temp_store, "c1")
        asyncio.run(places_flow.run_places_job("c1"))

        job = temp_store.get_job("c1")
        assert job["status"] == "done"
        assert job["result_count"] == 1
        assert calls["start"] == 0  # zero budget spent


# ---------------------------------------------------------------------------
# Phase 2: partial delivery when the budget runs out mid-run
# ---------------------------------------------------------------------------


class TestPartialOnBudgetExhaustion:
    def test_midrun_402_finalizes_partial(self, temp_store, monkeypatch, tmp_path):
        # Force 2 partitions by planning manually into the state blob.
        filters = dict(FILTERS)
        parts = [{"categories": filters["categories"], "zips": ["78701"]},
                 {"categories": filters["categories"], "zips": ["78702"]}]
        rows = [dict(business_name="Acme", category="Roofing contractor",
                     website="https://acme.com", primary_email="info@acme.com")]

        async def _fair_use():
            return FAIR_USE_OK

        async def _count(f):
            return 30_000

        exports = {"n": 0}

        async def _start(f):
            exports["n"] += 1
            if exports["n"] == 1:
                return "exp-ok"
            raise PlacesError("start_export", 402, "fair use limit exceeded")

        async def _poll(export_id):
            return {"job_status": "completed", "export_url": "https://s3/x.csv"}

        async def _download(url):
            return ["business_name"], rows

        for name, fn in [("fair_use", _fair_use), ("count_places", _count),
                         ("start_export", _start), ("poll_export", _poll),
                         ("download_export_csv", _download)]:
            monkeypatch.setattr(places_flow.places_client, name, fn)
        monkeypatch.setattr(places_flow.places_client, "EXPORT_POLL_INTERVAL_S", 0.01)
        monkeypatch.setattr(places_flow, "OUTPUT_DIR", tmp_path)

        # Pre-plan so plan_partitions never runs (count=30K would replan).
        _make_places_job(temp_store, "p1")
        store = temp_store
        job = store.get_job("p1")
        regions = json.loads(job["regions"])
        regions["places"] = {"partitions": parts, "done_parts": [],
                             "export_id": None, "expected": 30_000}
        store.conn.execute("UPDATE jobs SET regions=? WHERE job_id='p1'",
                           (json.dumps(regions),))
        store.conn.commit()

        asyncio.run(places_flow.run_places_job("p1", resume=True))

        job = store.get_job("p1")
        assert job["status"] == "done"  # partial delivered, not failed
        assert job["result_count"] == 1
        events = [json.loads(r["payload"]) for r in store.conn.execute(
            "SELECT payload FROM job_events WHERE job_id='p1' ORDER BY seq")]
        final = [e for e in events if e.get("status") == "done"][-1]
        assert "PARTIAL" in final["message"]
        assert "budget ran out" in final["message"]
        # partial results must NOT poison the cache
        assert places_flow.places_cache_lookup(parts[0]) is None


# ---------------------------------------------------------------------------
# Phase 3: external API mirror
# ---------------------------------------------------------------------------


class TestExternalPlacesRoutes:
    @pytest.fixture
    def client(self, monkeypatch):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from scraper import external_routes
        from scraper.external_helpers import ExternalError
        from shared import auth
        import main as main_module

        # The lazy `from . import places_client` inside the impls resolves at
        # call time — patching the module attrs covers every indirection.
        monkeypatch.setattr(places_client, "API_KEY", "test-key")
        app = FastAPI()
        app.include_router(external_routes.router)
        app.add_exception_handler(ExternalError, main_module.external_error_handler)
        app.dependency_overrides[auth.get_current_user_with_api_key] = lambda: USER
        return TestClient(app)

    def test_count_envelope(self, client, monkeypatch):
        async def _count(filters):
            return 1_234

        async def _fu():
            return FAIR_USE_OK

        monkeypatch.setattr(places_client, "count_places", _count)
        monkeypatch.setattr(places_client, "fair_use", _fu)
        res = client.post("/api/external/scraper/places/count",
                          json={"filters": {"categories": ["Dentist"]}})
        assert res.status_code == 200
        body = res.json()
        assert body["success"] is True
        assert body["data"]["total_matching"] == 1_234
        assert body["data"]["estimated_exports"] >= 1

    def test_jobs_dry_run_default_creates_nothing(self, client, monkeypatch):
        async def _count(filters):
            return 42

        async def _fu():
            return FAIR_USE_OK

        monkeypatch.setattr(places_client, "count_places", _count)
        monkeypatch.setattr(places_client, "fair_use", _fu)
        res = client.post("/api/external/scraper/places/jobs",
                          json={"filters": {"categories": ["Dentist"]}})
        assert res.status_code == 200
        data = res.json()["data"]
        assert "total_matching" in data and "job_id" not in data

    def test_kill_switch_external_error(self, client, monkeypatch):
        monkeypatch.setenv("ENABLE_GETLEADS_PLACES", "false")
        try:
            res = client.post("/api/external/scraper/places/count",
                              json={"filters": {"categories": ["Dentist"]}})
            assert res.status_code == 503
            assert res.json()["success"] is False  # envelope, not a bare 500
        finally:
            monkeypatch.delenv("ENABLE_GETLEADS_PLACES")


# ---------------------------------------------------------------------------
# Phase 3: MCP tool registration
# ---------------------------------------------------------------------------


class TestMcpPlacesTools:
    def test_tools_register_on_fresh_instance(self):
        from mcp.server.fastmcp import FastMCP
        from mcp_oracle import scraper_actions

        fresh = FastMCP("test-places")
        scraper_actions.register_tools(fresh)
        tools = asyncio.run(fresh.list_tools())
        names = {t.name for t in tools}
        assert "count_local_businesses_database" in names
        assert "fetch_local_businesses_database" in names
        assert "scrape_local_businesses" in names  # existing 5 unaffected
