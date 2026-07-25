"""Focused tests for the 5 scraper resume/restart bug fixes in scraper/routes.py.

These run in isolation (no FastAPI app, no live DB) by mocking the crawler,
job_store, and cache layer. They validate the *behavioral* guarantees of each
fix rather than HTTP plumbing.

Run alone:
    cd backend && source venv/bin/activate
    python -m pytest tests/test_scraper_resume_restart_fixes.py -q
"""
import asyncio
import sqlite3
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))


# ---------------------------------------------------------------------------
# Fix 4 helper: _regions_for_cache strips expected_types only
# ---------------------------------------------------------------------------

class TestRegionsForCache:
    def test_strips_expected_types_only(self):
        from scraper.routes import _regions_for_cache

        regions = {
            "mode": "zips", "country": "us", "states": [], "cities": [],
            "zips": ["10001"], "center_ids": [], "expected_types": ["dentist"],
        }
        out = _regions_for_cache(regions)

        assert "expected_types" not in out
        # All other keys preserved verbatim
        assert out["zips"] == ["10001"]
        assert out["mode"] == "zips"
        assert len(out) == 6

    def test_idempotent_when_no_expected_types(self):
        from scraper.routes import _regions_for_cache

        regions = {"mode": "all", "country": "us", "zips": []}
        out = _regions_for_cache(regions)
        assert out == regions

    def test_does_not_mutate_input(self):
        from scraper.routes import _regions_for_cache

        regions = {"mode": "all", "expected_types": ["dentist"]}
        _regions_for_cache(regions)
        # Caller's dict still has expected_types (immutability)
        assert "expected_types" in regions


# ---------------------------------------------------------------------------
# Fix 4 + 5: cache identity preserved across fresh/restart/resume
# ---------------------------------------------------------------------------

def _cache_id(query, regions, zooms, expected_types):
    from scraper.cache import (
        generate_cache_id,
        generate_region_signature,
        generate_zoom_signature,
        generate_expected_types_signature,
    )
    return generate_cache_id(
        query,
        generate_region_signature(regions),
        generate_zoom_signature(zooms),
        generate_expected_types_signature(expected_types),
    )


class TestCacheIdentityPreserved:
    def test_fresh_stored_matches_ui_check(self):
        """A fresh job stores expected_types in regions; after stripping, its
        cache_id must equal what the (unchanged) check_cache UI flow computes."""
        from scraper.routes import _regions_for_cache

        query, zooms, et = "dentist", [10, 11, 12], ["dentist"]
        # What start_job now persists in the jobs.regions blob
        stored_regions = {
            "mode": "zips", "country": "us", "states": [], "cities": [],
            "zips": ["10001"], "center_ids": [], "expected_types": et,
        }
        # What _run_job passes to store_cache (stripped)
        stripped = _regions_for_cache(stored_regions)
        # What check_cache / subset-count build (never had expected_types)
        check_regions = {
            "mode": "zips", "country": "us", "states": [], "cities": [],
            "zips": ["10001"], "center_ids": [],
        }

        assert _cache_id(query, stripped, zooms, et) == _cache_id(query, check_regions, zooms, et)

    def test_restart_with_zips_matches_original(self):
        """Fix 5: a restarted zip-mode job must produce the same region
        signature as the original run (zips carried forward)."""
        from scraper.cache import generate_region_signature
        from scraper.routes import _regions_for_cache

        original = {
            "mode": "zips", "country": "us", "states": [], "cities": [],
            "zips": ["10001", "10002"], "center_ids": [], "expected_types": ["dentist"],
        }
        # Restart now rebuilds WITH zips + expected_types
        restart_fixed = {
            "mode": "zips", "country": "us", "states": [], "cities": [],
            "zips": ["10001", "10002"], "center_ids": [], "expected_types": ["dentist"],
        }
        # The OLD buggy restart dropped zips
        restart_buggy = {
            "mode": "zips", "country": "us", "states": [], "cities": [],
            "zips": [], "center_ids": [], "expected_types": ["dentist"],
        }

        sig_orig = generate_region_signature(_regions_for_cache(original))
        assert sig_orig == generate_region_signature(_regions_for_cache(restart_fixed))
        # Demonstrate the bug existed: dropping zips changes the signature
        assert sig_orig != generate_region_signature(_regions_for_cache(restart_buggy))

    def test_expected_types_drop_changes_cache_id(self):
        """Fix 4: dropping expected_types (the old restart/resume bug) must
        produce a DIFFERENT cache_id than honoring it."""
        from scraper.routes import _regions_for_cache

        query, zooms = "dentist", [10, 11, 12]
        regions = _regions_for_cache({
            "mode": "all", "country": "us", "states": [], "cities": [],
            "zips": [], "center_ids": [], "expected_types": ["dentist"],
        })
        honored = _cache_id(query, regions, zooms, ["dentist"])
        dropped = _cache_id(query, regions, zooms, None)  # old buggy behavior
        assert honored != dropped


# ---------------------------------------------------------------------------
# Fix 2: resume must not re-run already-checkpointed (center, zoom) pairs
# ---------------------------------------------------------------------------

class TestResumeZoomBucketing:
    def test_resume_only_runs_pending_zooms(self, monkeypatch, tmp_path):
        """A center with zoom-10 already checkpointed must NOT have zoom-10
        re-executed on resume. Verify via the per-zoom-subset bucketing."""
        from scraper import routes
        from scraper import crawler as crawler_module
        from scraper import cache as cache_module

        center_a = {"name": "Alpha", "state": "ST", "lat": 1.0, "lng": 1.0}
        center_b = {"name": "Beta", "state": "ST", "lat": 2.0, "lng": 2.0}
        center_c = {"name": "Gamma", "state": "ST", "lat": 3.0, "lng": 3.0}

        # pending_tasks already excludes completed (center, zoom) checkpoints:
        #  - Alpha: only zoom 11 pending (10 & 12 were checkpointed done)
        #  - Beta:  all 3 zooms pending (untouched)
        #  - Gamma: only zoom 10 pending
        pending_tasks = [
            (center_a, 11),
            (center_b, 10), (center_b, 11), (center_b, 12),
            (center_c, 10),
        ]

        calls = []

        async def fake_run_crawl(**kwargs):
            calls.append({
                "centers": [c["name"] for c in kwargs["centers"]],
                "zooms": sorted(kwargs["zooms"]),
            })
            return len(kwargs["centers"])

        monkeypatch.setattr(crawler_module, "run_crawl", fake_run_crawl)
        monkeypatch.setattr(cache_module, "store_cache", lambda **kw: "fake_cache_id")

        fake_store = MagicMock()
        fake_store.is_job_cancelled = lambda jid: False
        fake_store.get_job = lambda jid: {
            "regions": {"mode": "all", "country": "us"},
            "result_count": 0,
        }
        monkeypatch.setattr(routes.job_store, "get_store", lambda: fake_store)

        asyncio.run(routes._run_job_with_tasks(
            job_id="job-1",
            user_id="u1",
            is_admin=True,  # skip db.record_api_requests
            query="dentist",
            tasks=pending_tasks,
            api_key="key",
            output_path=tmp_path / "out.csv",
            expected_types=["dentist"],
            cancelled_jobs=set(),
        ))

        # One run_crawl call per distinct pending-zoom set (3 buckets here)
        assert len(calls) == 3
        by_zoom = {tuple(c["zooms"]): c["centers"] for c in calls}
        assert by_zoom[(11,)] == ["Alpha"]
        assert sorted(by_zoom[(10, 11, 12)]) == ["Beta"]
        assert by_zoom[(10,)] == ["Gamma"]

        # CRITICAL invariant: a completed zoom is never re-run for its center
        for c in calls:
            if "Alpha" in c["centers"]:
                assert 10 not in c["zooms"]
                assert 12 not in c["zooms"]
            if "Gamma" in c["centers"]:
                assert c["zooms"] == [10]

    def test_resume_single_bucket_all_zooms(self, monkeypatch, tmp_path):
        """When no zooms are checkpointed (fresh resume), all centers share the
        same pending-zoom set {10,11,12} and run in a single run_crawl call."""
        from scraper import routes
        from scraper import crawler as crawler_module
        from scraper import cache as cache_module

        c1 = {"name": "One", "state": "ST"}
        c2 = {"name": "Two", "state": "ST"}
        pending = [(c1, z) for z in (10, 11, 12)] + [(c2, z) for z in (10, 11, 12)]

        calls = []

        async def fake_run_crawl(**kwargs):
            calls.append({
                "centers": [c["name"] for c in kwargs["centers"]],
                "zooms": sorted(kwargs["zooms"]),
            })
            return 0

        monkeypatch.setattr(crawler_module, "run_crawl", fake_run_crawl)
        monkeypatch.setattr(cache_module, "store_cache", lambda **kw: "fake_cache_id")

        fake_store = MagicMock()
        fake_store.is_job_cancelled = lambda jid: False
        fake_store.get_job = lambda jid: {"regions": {"mode": "all"}, "result_count": 0}
        monkeypatch.setattr(routes.job_store, "get_store", lambda: fake_store)

        asyncio.run(routes._run_job_with_tasks(
            job_id="job-2", user_id="u1", is_admin=True, query="dentist",
            tasks=pending, api_key="k", output_path=tmp_path / "o.csv",
            expected_types=None, cancelled_jobs=set(),
        ))

        assert len(calls) == 1
        assert sorted(calls[0]["zooms"]) == [10, 11, 12]
        assert sorted(calls[0]["centers"]) == ["One", "Two"]


# ---------------------------------------------------------------------------
# Fix 3: cancelled-job partial-cache must not hit UnboundLocalError
# ---------------------------------------------------------------------------

class TestCancelledPartialCache:
    def test_partial_cache_runs_without_unbound_error(self, monkeypatch, tmp_path):
        """When run_crawl raises a cancellation RuntimeError before assigning
        result_count, the partial-cache path must still execute store_cache
        (previously: UnboundLocalError on result_count)."""
        from scraper import routes
        from scraper import crawler as crawler_module
        from scraper import cache as cache_module

        output = tmp_path / "out.csv"
        output.write_text("place_id,name\n1,A\n")  # non-empty partial output

        async def raise_cancelled(**kwargs):
            raise RuntimeError("Job job-3 was cancelled")

        store_cache_calls = []

        def fake_store_cache(**kwargs):
            store_cache_calls.append(kwargs)
            return "fake_cache_id"

        monkeypatch.setattr(crawler_module, "run_crawl", raise_cancelled)
        monkeypatch.setattr(cache_module, "store_cache", fake_store_cache)

        fake_store = MagicMock()
        fake_store.is_job_cancelled = lambda jid: False
        fake_store.get_job = lambda jid: {
            "regions": {"mode": "all", "country": "us", "states": [],
                        "cities": [], "zips": [], "center_ids": []},
            "total_tasks": 10, "done_tasks": 3, "result_count": 0,
        }
        monkeypatch.setattr(routes.job_store, "get_store", lambda: fake_store)

        # Must not raise UnboundLocalError
        asyncio.run(routes._run_job(
            job_id="job-3", user_id="u1", is_admin=True, query="dentist",
            filtered_centers=[{"name": "X", "state": "ST"}],
            api_key="k", output_path=output,
            expected_types=["dentist"], cancelled_jobs=set(),
        ))

        assert len(store_cache_calls) == 1
        assert store_cache_calls[0]["is_partial"] is True
        # result_count initialized to 0 -> total_results 0 (no crash)
        assert store_cache_calls[0]["total_results"] == 0
        # expected_types still forwarded to the partial cache entry
        assert store_cache_calls[0]["expected_types"] == ["dentist"]


# ---------------------------------------------------------------------------
# Fix 1 + Fix 4/5 integration: start_job persists zips + expected_types so
# downstream restart/resume can recover them. Uses a real temp SQLite store.
# ---------------------------------------------------------------------------

@pytest.fixture
def temp_scraper_db():
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
            parent_job_id TEXT,
            query TEXT,
            regions TEXT,
            total_tasks INTEGER,
            done_tasks INTEGER,
            result_count INTEGER,
            created_at TEXT,
            updated_at TEXT,
            is_resumable INTEGER DEFAULT 1
        );
    """)
    yield conn, db_path
    conn.close()
    Path(db_path).unlink(missing_ok=True)


class TestStartJobPersistsConfig:
    def test_regions_blob_carries_zips_and_expected_types(self, temp_scraper_db, monkeypatch):
        """The persisted regions blob (read back by restart/resume) must contain
        both zips (Fix 5 cache identity) and expected_types (Fix 4 filter)."""
        from shared import db as shared_db
        from scraper import routes

        conn, _db_path = temp_scraper_db
        monkeypatch.setattr(shared_db, "get_db", lambda: conn)
        monkeypatch.setattr(shared_db, "check_daily_request_limit",
                            lambda **kw: (True, ""))
        monkeypatch.setattr(shared_db, "get_api_quota_status",
                            lambda *a, **kw: {"used": 0, "limit": 50000})

        # Minimal centers module stub: mode="zips" returns one synthetic center
        fake_center = {"name": "Zip10001", "state": "NY", "lat": 1.0, "lng": 1.0}

        class FakeCenters:
            get_centers_for_job = staticmethod(lambda **kw: ([fake_center], []))
            estimate_task_count = staticmethod(lambda centers: len(centers) * 3)

        monkeypatch.setattr(routes, "centers_module", FakeCenters)

        captured = {}

        def fake_add_task(func, **kwargs):
            captured["regions"] = kwargs.get("filtered_centers")
            # The real background task is not executed; capture the created job instead.

        # Intercept the store to read back the persisted regions blob
        created_regions = {}

        class FakeStore:
            def create_scraper_job(self, **kwargs):
                created_regions.update(kwargs.get("regions", {}))

        monkeypatch.setattr(routes.job_store, "get_store", lambda: FakeStore())

        import pydantic
        req = routes.StartJobRequest(
            query="dentist", mode="zips", country="us",
            zips=["10001"], expected_types=["dentist"],
        )

        # Build a fake background_tasks + admin user
        bg = MagicMock()
        bg.add_task = fake_add_task
        user = {"user_id": "u1", "is_admin": True}
        monkeypatch.setenv("SCRAPER_TECH_KEY", "fake-key")

        result = asyncio.run(routes.start_job(req, bg, user))

        # The persisted regions blob must include BOTH zips and expected_types
        assert created_regions.get("zips") == ["10001"]
        assert created_regions.get("expected_types") == ["dentist"]
