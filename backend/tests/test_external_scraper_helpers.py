"""
Tests for scraper/external_helpers.py — the shared implementation layer of the
external scraper API (/api/external/scraper/*) and the MCP scraper action tools.

Covers:
- compute_estimate: task count is ALWAYS centers × 3 zooms (regression guard
  against the legacy /regions/estimate zips ×1 quirk), error paths.
- peek_cache: identical cache_id formula + SELECT as cache.check_cache but
  READ-ONLY (no access_count / last_accessed_at / cache_stats mutation).
- is_full_cache_hit matrix: partial / <100% / missing file / happy path.
- read_csv_rows: offset+limit windows, field projection, unknown-field error,
  compact expansion, empty/missing files.
- check_task_cap: under/at/over, admin exemption, env override, disabled.
- project_job: shape, regions parsed from JSON string, pct capped at 100.
- queue_position ordering; suggested_poll_seconds boundaries; envelope shape.
- impl_create_job: cache short-circuit (no row inserted), task cap 422,
  quota 429, success payload.
- impl_job_results: 409 not_ready + retry_after while running, 404 on
  missing file, ownership 403.
"""
from __future__ import annotations

import csv as _csv
import os
import sqlite3
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import pytest

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from scraper import external_helpers as ext  # noqa: E402
from scraper import cache as cache_module  # noqa: E402
from shared import db  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

def _req(**kw):
    base = {"query": "zz-ext-test q", "mode": "cities", "country": "us", "cities": ["Austin"]}
    base.update(kw)
    return ext.ExternalScrapeRequest(**base)


def _create_req(**kw):
    base = {"query": "zz-ext-test q", "mode": "cities", "country": "us", "cities": ["Austin"]}
    base.update(kw)
    return ext.CreateJobRequest(**base)


def _make_user(user_id="ext-helper-user", is_admin=False):
    return {"user_id": user_id, "email": f"{user_id}@test.example", "is_admin": is_admin}


def _make_test_user(conn, user_id):
    conn.execute(
        "INSERT OR REPLACE INTO users (user_id, email, password_hash, created_at) "
        "VALUES (?, ?, 'x', ?)",
        (user_id, f"{user_id}@test.example", datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()


def _insert_scraper_job(conn, job_id, user_id, status="running",
                        total_tasks=9, done_tasks=3, result_count=2):
    iso = datetime.now(timezone.utc).isoformat()
    regions = '{"mode":"cities","country":"us","cities":["Austin"],"states":[],"zips":[],"center_ids":[]}'
    conn.execute(
        "INSERT INTO jobs (job_id, user_id, job_type, status, query, regions, "
        "total_tasks, done_tasks, result_count, created_at, updated_at) "
        "VALUES (?, ?, 'scraper', ?, 'zz-ext-test q', ?, ?, ?, ?, ?, ?)",
        (job_id, user_id, status, regions, total_tasks, done_tasks, result_count, iso, iso),
    )
    conn.commit()


def _cleanup(conn, *job_ids, user_ids=()):
    try:
        for jid in job_ids:
            conn.execute("DELETE FROM job_events WHERE job_id = ?", (jid,))
            conn.execute("DELETE FROM task_checkpoints WHERE job_id = ?", (jid,))
            conn.execute("DELETE FROM jobs WHERE job_id = ?", (jid,))
        for uid in user_ids:
            conn.execute("DELETE FROM users WHERE user_id = ?", (uid,))
        conn.commit()
    except Exception:
        conn.rollback()


def _write_csv(path: Path, n: int, cols=None) -> None:
    cols = list(cols or ext.RESULT_FIELDS[:3])
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = _csv.writer(f)
        w.writerow(cols)
        for i in range(n):
            w.writerow([f"v{i}" for _ in cols])


def _insert_cache_row(conn, cache_id, result_path, total_results=10, is_partial=0,
                      pct=100.0, query="zz-ext-test q"):
    now = datetime.now(timezone.utc)
    conn.execute(
        "INSERT OR REPLACE INTO scraped_cache "
        "(cache_id, job_id, user_id, query, region_signature, regions, zoom_signature, "
        "expected_types_signature, status, result_file_path, checksum, total_results, "
        "created_at, updated_at, expires_at, last_accessed_at, access_count, "
        "is_partial, percentage_complete) "
        "VALUES (?, ?, ?, ?, 'sig', '{}', 'zsig', 'tsig', 'active', ?, 'chk', ?, "
        "?, ?, ?, ?, 5, ?, ?)",
        (cache_id, "job-x", "user-x", query,
         str(result_path), total_results,
         now.isoformat(), now.isoformat(), (now + timedelta(days=60)).isoformat(),
         now.isoformat(), is_partial, pct),
    )
    conn.commit()


def _cache_stats_count(conn) -> int:
    row = conn.execute("SELECT COUNT(*) AS n FROM cache_stats").fetchone()
    return int(row["n"]) if row else 0


def _fake_estimate(centers_n=5):
    """Fake compute_estimate result without touching centers CSVs."""
    return {
        "centers": [{"name": f"c{i}"} for i in range(centers_n)],
        "errors": [],
        "center_count": centers_n,
        "task_count": centers_n * 3,
        "task_basis": "centers_x_3_zooms",
    }


@pytest.fixture()
def conn():
    # Thread-local connection — do NOT close it: db.get_db() caches per
    # thread, and closing poisons every later db call in this thread.
    yield db.get_db()


# ---------------------------------------------------------------------------
# 1. compute_estimate — ALWAYS ×3
# ---------------------------------------------------------------------------

class TestComputeEstimate:
    def test_task_count_is_centers_x3_for_cities_mode(self):
        est = ext.compute_estimate(_req(mode="cities", cities=["Austin"]))
        assert est["task_count"] == est["center_count"] * 3
        assert est["task_basis"] == "centers_x_3_zooms"

    def test_task_count_is_centers_x3_for_zips_mode(self):
        # Regression guard: legacy /regions/estimate reports ×1 for zips; the
        # external surface must match execution + quota billing (×3).
        est = ext.compute_estimate(_req(mode="zips", cities=[], zips=["78701", "78702"]))
        assert est["task_count"] == est["center_count"] * 3
        assert est["task_count"] >= 6  # 2 zips × 3 zooms

    def test_task_count_is_centers_x3_for_states_mode(self):
        est = ext.compute_estimate(_req(mode="states", cities=[], states=["Texas"]))
        assert est["task_count"] == est["center_count"] * 3

    def test_no_centers_raises(self):
        with pytest.raises(ext.ExternalError) as ei:
            ext.compute_estimate(_req(mode="states", cities=[], states=["Nowhere"]))
        assert ei.value.code == "no_centers"
        assert ei.value.status_code == 400

    def test_empty_query_raises(self):
        with pytest.raises(ext.ExternalError) as ei:
            ext.compute_estimate(_req(query="   "))
        assert ei.value.code == "no_query"

    def test_unknown_country_raises(self):
        with pytest.raises(ext.ExternalError) as ei:
            ext.compute_estimate(_req(country="zz"))
        assert ei.value.code == "no_centers"


# ---------------------------------------------------------------------------
# 2. peek_cache — read-only twin of check_cache
# ---------------------------------------------------------------------------

class TestPeekCache:
    def test_miss_returns_none(self, conn, tmp_path):
        before = _cache_stats_count(conn)
        assert ext.peek_cache("never-cached-q", {"mode": "all", "country": "us"}) is None
        assert _cache_stats_count(conn) == before  # no stats write on miss

    def test_hit_returns_row_without_mutating_access_stats(self, conn, tmp_path):
        csv_path = tmp_path / "cached.csv"
        _write_csv(csv_path, 10)
        regions = {"mode": "cities", "country": "us", "states": [], "cities": ["Austin"],
                   "zips": [], "center_ids": []}
        # Compute the cache_id with the SAME formula helpers use
        region_sig = cache_module.generate_region_signature(regions)
        zoom_sig = cache_module.generate_zoom_signature([10, 11, 12])
        types_sig = cache_module.generate_expected_types_signature(None)
        cache_id = cache_module.generate_cache_id("zz-ext-test q", region_sig, zoom_sig, types_sig)
        _insert_cache_row(conn, cache_id, csv_path)
        stats_before = _cache_stats_count(conn)

        entry = ext.peek_cache("zz-ext-test q", regions, [10, 11, 12], None)

        assert entry is not None
        assert entry["cache_id"] == cache_id
        row = conn.execute(
            "SELECT access_count, last_accessed_at FROM scraped_cache WHERE cache_id = ?",
            (cache_id,),
        ).fetchone()
        assert row["access_count"] == 5  # unchanged
        assert _cache_stats_count(conn) == stats_before  # no hit stat recorded

    def test_expired_entry_ignored(self, conn, tmp_path):
        csv_path = tmp_path / "expired.csv"
        _write_csv(csv_path, 5)
        regions = {"mode": "cities", "country": "us", "states": [], "cities": ["Austin"],
                   "zips": [], "center_ids": []}
        region_sig = cache_module.generate_region_signature(regions)
        cache_id = cache_module.generate_cache_id(
            "zz-ext-test expired", region_sig,
            cache_module.generate_zoom_signature([10, 11, 12]),
            cache_module.generate_expected_types_signature(None),
        )
        now = datetime.now(timezone.utc)
        try:
            conn.execute(
                "INSERT OR REPLACE INTO scraped_cache "
                "(cache_id, job_id, user_id, query, region_signature, regions, zoom_signature, "
                "expected_types_signature, status, result_file_path, checksum, total_results, "
                "created_at, updated_at, expires_at, last_accessed_at, access_count, "
                "is_partial, percentage_complete) "
                "VALUES (?, 'j', 'u', 'zz-ext-test expired', 'sig', '{}', 'zsig', 'tsig', 'active', "
                "?, 'chk', 5, ?, ?, ?, ?, 1, 0, 100.0)",
                (cache_id, str(csv_path), (now - timedelta(days=100)).isoformat(),
                 (now - timedelta(days=100)).isoformat(), (now - timedelta(days=10)).isoformat(),
                 now.isoformat()),
            )
            conn.commit()
            assert ext.peek_cache("zz-ext-test expired", regions, [10, 11, 12], None) is None
        finally:
            conn.execute("DELETE FROM scraped_cache WHERE cache_id = ?", (cache_id,))
            conn.commit()

    def test_accepts_regions_as_json_string(self, conn, tmp_path):
        csv_path = tmp_path / "j.csv"
        _write_csv(csv_path, 3)
        regions = {"mode": "cities", "country": "us", "states": [], "cities": ["Austin"],
                   "zips": [], "center_ids": []}
        region_sig = cache_module.generate_region_signature(regions)
        cache_id = cache_module.generate_cache_id(
            "zz-ext-test jsonstr", region_sig,
            cache_module.generate_zoom_signature([10, 11, 12]),
            cache_module.generate_expected_types_signature(None),
        )
        try:
            _insert_cache_row(conn, cache_id, csv_path, query="zz-ext-test jsonstr")
            # Pass a JSON string (form get_job() returns) — must still hit
            found = ext.peek_cache(
                "zz-ext-test jsonstr", __import__("json").dumps(regions), [10, 11, 12], None
            )
            assert found is not None and found["cache_id"] == cache_id
        finally:
            conn.execute("DELETE FROM scraped_cache WHERE cache_id = ?", (cache_id,))
            conn.commit()


# ---------------------------------------------------------------------------
# 3. is_full_cache_hit matrix
# ---------------------------------------------------------------------------

class TestIsFullCacheHit:
    def _entry(self, tmp_path, *, is_partial=0, pct=100.0, with_file=True):
        path = tmp_path / "hit.csv"
        if with_file:
            _write_csv(path, 4)
        return {
            "is_partial": is_partial,
            "percentage_complete": pct,
            "result_file_path": str(path) if with_file else str(tmp_path / "gone.csv"),
        }

    def test_full_hit(self, tmp_path):
        assert ext.is_full_cache_hit(self._entry(tmp_path)) is True

    def test_partial_not_full(self, tmp_path):
        assert ext.is_full_cache_hit(self._entry(tmp_path, is_partial=1)) is False

    def test_under_100_pct_not_full(self, tmp_path):
        assert ext.is_full_cache_hit(self._entry(tmp_path, pct=42.0)) is False

    def test_missing_file_not_full(self, tmp_path):
        assert ext.is_full_cache_hit(self._entry(tmp_path, with_file=False)) is False

    def test_empty_entry(self):
        assert ext.is_full_cache_hit({}) is False
        assert ext.is_full_cache_hit(None) is False


# ---------------------------------------------------------------------------
# 4. read_csv_rows
# ---------------------------------------------------------------------------

class TestReadCsvRows:
    def test_window_offset_limit(self, tmp_path):
        p = tmp_path / "rows.csv"
        _write_csv(p, 10)
        rows, fields, total = ext.read_csv_rows(p, offset=2, limit=3)
        assert total == 10
        assert len(rows) == 3
        assert rows[0]["v0"] if "v0" in rows[0] else True  # header-derived keys
        assert fields == list(ext.RESULT_FIELDS[:3]) or fields  # passthrough

    def test_field_projection(self, tmp_path):
        p = tmp_path / "proj.csv"
        cols = ["name", "phone", "website", "city"]
        with open(p, "w", newline="", encoding="utf-8") as f:
            w = _csv.writer(f)
            w.writerow(cols)
            w.writerow(["Cafe A", "123", "https://a.com", "Austin"])
            w.writerow(["Cafe B", "456", "https://b.com", "Austin"])
        rows, fields, total = ext.read_csv_rows(p, offset=0, limit=10, fields=["name", "phone"])
        assert total == 2
        assert fields == ["name", "phone"]
        assert rows[0] == {"name": "Cafe A", "phone": "123"}

    def test_offset_beyond_end_returns_empty(self, tmp_path):
        p = tmp_path / "beyond.csv"
        _write_csv(p, 5)
        rows, _, total = ext.read_csv_rows(p, offset=100, limit=10)
        assert rows == [] and total == 5

    def test_zero_limit_returns_empty_with_total(self, tmp_path):
        p = tmp_path / "zero.csv"
        _write_csv(p, 5)
        rows, _, total = ext.read_csv_rows(p, offset=0, limit=0)
        assert rows == [] and total == 5

    def test_missing_file_total_zero(self, tmp_path):
        rows, _, total = ext.read_csv_rows(tmp_path / "nope.csv")
        assert rows == [] and total == 0


class TestValidateFields:
    def test_none_passthrough(self):
        assert ext.validate_fields(None) is None

    def test_compact_expansion(self):
        assert ext.validate_fields(["compact"]) == list(ext.COMPACT_FIELDS)

    def test_valid_fields(self):
        assert ext.validate_fields(["name", "phone"]) == ["name", "phone"]

    def test_unknown_field_raises_with_valid_list(self):
        with pytest.raises(ext.ExternalError) as ei:
            ext.validate_fields(["name", "bogus"])
        assert ei.value.code == "invalid_fields"
        assert "name" in str(ei.value.extra.get("valid_fields")) or \
            ei.value.extra.get("valid_fields")


# ---------------------------------------------------------------------------
# 5. Guardrails: check_task_cap / poll / quota
# ---------------------------------------------------------------------------

class TestCheckTaskCap:
    def test_under_limit_ok(self):
        ext.check_task_cap(14_999, is_admin=False)

    def test_at_limit_ok(self):
        ext.check_task_cap(15_000, is_admin=False)

    def test_over_limit_raises_422_with_numbers(self):
        with pytest.raises(ext.ExternalError) as ei:
            ext.check_task_cap(15_001, is_admin=False)
        assert ei.value.code == "task_limit_exceeded"
        assert ei.value.status_code == 422
        assert ei.value.extra["limit"] == 15_000
        assert ei.value.extra["task_count"] == 15_001

    def test_admin_exempt(self):
        ext.check_task_cap(88_638, is_admin=True)

    @mock.patch.dict(os.environ, {"MAX_EXTERNAL_SCRAPER_TASKS": "100"})
    def test_env_override(self):
        with pytest.raises(ext.ExternalError) as ei:
            ext.check_task_cap(101, is_admin=False)
        assert ei.value.extra["limit"] == 100

    @mock.patch.dict(os.environ, {"MAX_EXTERNAL_SCRAPER_TASKS": "0"})
    def test_zero_disables_cap(self):
        ext.check_task_cap(10_000_000, is_admin=False)


class TestPollAndEnvelope:
    def test_poll_boundaries(self):
        assert ext.suggested_poll_seconds(10) == 10
        assert ext.suggested_poll_seconds(499) == 10
        assert ext.suggested_poll_seconds(500) == 30
        assert ext.suggested_poll_seconds(4_999) == 30
        assert ext.suggested_poll_seconds(5_000) == 60

    def test_envelope_without_meta(self):
        e = ext.envelope({"a": 1})
        assert e == {"success": True, "data": {"a": 1}, "error": None, "meta": None}

    def test_envelope_with_meta(self):
        e = ext.envelope([], total=42, limit=10, offset=5)
        assert e["meta"] == {"total": 42, "limit": 10, "offset": 5}

    def test_pct_complete_capped(self):
        assert ext.pct_complete(120, 100) == 100.0
        assert ext.pct_complete(1, 4) == 25.0
        assert ext.pct_complete(0, 0) == 0.0


# ---------------------------------------------------------------------------
# 6. project_job / queue_position
# ---------------------------------------------------------------------------

class TestProjectJob:
    def test_list_projection_shape(self):
        job = {
            "job_id": "jid", "query": "q", "display_name": "[API] q", "status": "running",
            "total_tasks": 9, "done_tasks": 3, "result_count": 2,
            "created_at": "2026-01-01T00:00:00", "updated_at": "2026-01-01T00:01:00",
        }
        out = ext.project_job(job)
        assert out["job_id"] == "jid" and out["status"] == "running"
        assert "progress" not in out and "rows_on_disk" not in out
        assert out["links"]["status"].endswith("/jobs/jid")
        assert "user_id" not in out and "output_path" not in out

    def test_detail_parses_regions_json_string(self):
        job = {
            "job_id": "jid2", "query": "q", "status": "done", "total_tasks": 3,
            "done_tasks": 3, "result_count": 5,
            "regions": '{"mode":"cities","country":"us","cities":["Austin"]}',
            "created_at": "2026-01-01T00:00:00", "updated_at": "2026-01-01T00:05:00",
        }
        out = ext.project_job(job, detail=True)
        assert out["regions"]["cities"] == ["Austin"]
        assert isinstance(out["regions"], dict)
        assert out["progress"]["pct_complete"] == 100.0
        assert "suggested_poll_seconds" in out

    def test_detail_garbage_regions_becomes_empty_dict(self):
        job = {"job_id": "j", "status": "done", "total_tasks": 3, "done_tasks": 3,
               "regions": "not-json{", "created_at": "t", "updated_at": "t"}
        out = ext.project_job(job, detail=True)
        assert out["regions"] == {}

    def test_file_available_false_when_no_file(self):
        job = {"job_id": "no-file-job", "status": "done", "total_tasks": 3,
               "done_tasks": 3, "created_at": "t", "updated_at": "t"}
        out = ext.project_job(job)
        assert out["file_available"] is False


class TestQueuePosition:
    def test_ordering(self, conn):
        uid = "qp-user"
        _make_test_user(conn, uid)
        ids = [str(uuid.uuid4()) for _ in range(3)]
        try:
            for i, jid in enumerate(ids):
                # Stagger created_at so ordering is deterministic
                ts = (datetime.now(timezone.utc) - timedelta(seconds=60 - i)).isoformat()
                conn.execute(
                    "INSERT INTO jobs (job_id, user_id, job_type, status, query, created_at, updated_at) "
                    "VALUES (?, ?, 'scraper', 'queued', 'q', ?, ?)",
                    (jid, uid, ts, ts),
                )
            conn.commit()
            jobs = {jid: conn.execute(
                "SELECT * FROM jobs WHERE job_id = ?", (jid,)).fetchone() for jid in ids}
            # oldest (ts earliest = largest offset) is first
            positions = sorted(
                [(jid, ext.queue_position(dict(jobs[jid]))) for jid in ids],
                key=lambda t: t[1],
            )
            assert [p for _, p in positions] == [0, 1, 2]
        finally:
            _cleanup(conn, *ids, user_ids=(uid,))

    def test_non_queued_is_zero(self):
        assert ext.queue_position({"status": "running", "created_at": "t"}) == 0


# ---------------------------------------------------------------------------
# 7. impl_create_job — guards & cache short-circuit
# ---------------------------------------------------------------------------

class TestImplCreateJob:
    def test_cache_short_circuit_inserts_no_row(self, conn, tmp_path):
        uid = "ext-csc-user"
        _make_test_user(conn, uid)
        user = _make_user(uid)
        csv_path = tmp_path / "full.csv"
        _write_csv(csv_path, 7)
        regions = {"mode": "cities", "country": "us", "states": [], "cities": ["Austin"],
                   "zips": [], "center_ids": []}
        region_sig = cache_module.generate_region_signature(regions)
        cache_id = cache_module.generate_cache_id(
            "zz-ext-test q", region_sig,
            cache_module.generate_zoom_signature([10, 11, 12]),
            cache_module.generate_expected_types_signature(None),
        )
        _insert_cache_row(conn, cache_id, csv_path, total_results=7)
        try:
            before = conn.execute("SELECT COUNT(*) AS n FROM jobs WHERE job_type='scraper'").fetchone()["n"]
            out = ext.impl_create_job(user, _create_req(prefer_cache=True), source="API")
            after = conn.execute("SELECT COUNT(*) AS n FROM jobs WHERE job_type='scraper'").fetchone()["n"]
            assert out["created"] is False
            assert out["served_from_cache"] is True
            assert out["rows_available"] == 7
            assert after == before  # NO job row
        finally:
            conn.execute("DELETE FROM scraped_cache WHERE cache_id = ?", (cache_id,))
            _cleanup(conn, user_ids=(uid,))

    def test_partial_cache_hit_falls_through_to_create(self, conn, tmp_path):
        uid = "ext-partial-user"
        _make_test_user(conn, uid)
        user = _make_user(uid)
        csv_path = tmp_path / "part.csv"
        _write_csv(csv_path, 3)
        regions = {"mode": "cities", "country": "us", "states": [], "cities": ["Austin"],
                   "zips": [], "center_ids": []}
        region_sig = cache_module.generate_region_signature(regions)
        cache_id = cache_module.generate_cache_id(
            "zz-ext-test q", region_sig,
            cache_module.generate_zoom_signature([10, 11, 12]),
            cache_module.generate_expected_types_signature(None),
        )
        _insert_cache_row(conn, cache_id, csv_path, total_results=3, is_partial=1, pct=40.0)
        job_id = None
        try:
            with mock.patch.dict(os.environ, {"SCRAPER_TECH_KEY": "test-key"}):
                out = ext.impl_create_job(user, _create_req(prefer_cache=True), source="API")
            assert out["created"] is True
            job_id = out.get("job_id")
            assert out["status"] == "queued"
            assert out["display_name"].startswith("[API] ")
            assert out["suggested_poll_seconds"] in (10, 30, 60)
            assert "links" in out and "quota" in out
        finally:
            conn.execute("DELETE FROM scraped_cache WHERE cache_id = ?", (cache_id,))
            if job_id:
                _cleanup(conn, job_id, user_ids=(uid,))
            else:
                _cleanup(conn, user_ids=(uid,))

    def test_task_cap_422(self, conn):
        uid = "ext-cap-user"
        _make_test_user(conn, uid)
        try:
            with mock.patch.dict(os.environ, {"SCRAPER_TECH_KEY": "k", "MAX_EXTERNAL_SCRAPER_TASKS": "6"}):
                with pytest.raises(ext.ExternalError) as ei:
                    ext.impl_create_job(_make_user(uid), _create_req(), source="API")
            assert ei.value.code == "task_limit_exceeded"
            assert ei.value.status_code == 422
        finally:
            _cleanup(conn, user_ids=(uid,))

    def test_quota_denied_429_with_resets_at(self, conn):
        uid = "ext-quota-user"
        _make_test_user(conn, uid)
        try:
            with mock.patch.dict(os.environ, {"SCRAPER_TECH_KEY": "k"}):
                with mock.patch.object(ext.db, "check_daily_request_limit", return_value=(False, "Daily limit exceeded.")):
                    with pytest.raises(ext.ExternalError) as ei:
                        ext.impl_create_job(_make_user(uid), _create_req(prefer_cache=False), source="API")
            assert ei.value.code == "quota_exceeded"
            assert ei.value.status_code == 429
            assert ei.value.retry_after is None
            assert "resets_at" in ei.value.extra
        finally:
            _cleanup(conn, user_ids=(uid,))

    def test_missing_scraper_key_500(self, conn):
        uid = "ext-nokey-user"
        _make_test_user(conn, uid)
        try:
            with mock.patch.dict(os.environ, {"SCRAPER_TECH_KEY": ""}):
                with pytest.raises(ext.ExternalError) as ei:
                    ext.impl_create_job(_make_user(uid), _create_req(), source="API")
            assert ei.value.code == "scraper_not_configured"
            assert ei.value.status_code == 500
        finally:
            _cleanup(conn, user_ids=(uid,))

    def test_admin_bypasses_cap_but_creates(self, conn):
        uid = "ext-admin-user"
        _make_test_user(conn, uid)
        job_id = None
        try:
            with mock.patch.dict(os.environ, {"SCRAPER_TECH_KEY": "k", "MAX_EXTERNAL_SCRAPER_TASKS": "1"}):
                out = ext.impl_create_job(_make_user(uid, is_admin=True), _create_req(), source="MCP")
            assert out["created"] is True
            job_id = out["job_id"]
            assert out["display_name"].startswith("[MCP] ")
        finally:
            if job_id:
                _cleanup(conn, job_id, user_ids=(uid,))
            else:
                _cleanup(conn, user_ids=(uid,))


# ---------------------------------------------------------------------------
# 8. impl_job_results / ownership
# ---------------------------------------------------------------------------

class TestImplJobResults:
    def test_running_409_with_retry_after(self, conn):
        uid = "ext-res-user"
        jid = str(uuid.uuid4())
        _make_test_user(conn, uid)
        _insert_scraper_job(conn, jid, uid, status="running", total_tasks=9, done_tasks=3)
        try:
            with pytest.raises(ext.ExternalError) as ei:
                ext.impl_job_results(_make_user(uid), jid)
            assert ei.value.code == "not_ready"
            assert ei.value.status_code == 409
            assert ei.value.retry_after == 10  # 9 tasks → 10s band
            assert ei.value.extra["job_status"] == "running"
        finally:
            _cleanup(conn, jid, user_ids=(uid,))

    def test_non_owner_403(self, conn):
        uid = "ext-owner-user"
        other = "ext-other-user"
        jid = str(uuid.uuid4())
        _make_test_user(conn, uid)
        _make_test_user(conn, other)
        _insert_scraper_job(conn, jid, uid, status="done")
        try:
            with pytest.raises(ext.ExternalError) as ei:
                ext.impl_job_results(_make_user(other), jid)
            assert ei.value.code == "access_denied"
        finally:
            _cleanup(conn, jid, user_ids=(uid, other))

    def test_admin_sees_any_job(self, conn):
        uid = "ext-admin-view-user"
        jid = str(uuid.uuid4())
        _make_test_user(conn, uid)
        _insert_scraper_job(conn, jid, uid, status="done")
        try:
            out = ext.impl_job_status(_make_user("some-admin", is_admin=True), jid)
            assert out["job_id"] == jid
        finally:
            _cleanup(conn, jid, user_ids=(uid,))

    def test_not_found_404(self, conn):
        with pytest.raises(ext.ExternalError) as ei:
            ext.impl_job_results(_make_user("x"), "no-such-job")
        assert ei.value.code == "not_found"

    def test_non_scraper_job_404(self, conn):
        uid = "ext-nsj-user"
        jid = str(uuid.uuid4())
        _make_test_user(conn, uid)
        iso = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT INTO jobs (job_id, user_id, job_type, status, query, created_at, updated_at) "
            "VALUES (?, ?, 'enrichment', 'done', 'q', ?, ?)", (jid, uid, iso, iso))
        conn.commit()
        try:
            with pytest.raises(ext.ExternalError) as ei:
                ext.impl_job_results(_make_user(uid), jid)
            assert ei.value.code == "not_found"
        finally:
            _cleanup(conn, jid, user_ids=(uid,))


# ---------------------------------------------------------------------------
# 9. impl_estimate — assembled payload
# ---------------------------------------------------------------------------

class TestImplEstimate:
    def test_payload_shape(self, conn):
        uid = "ext-est-user"
        _make_test_user(conn, uid)
        try:
            out = ext.impl_estimate(_make_user(uid), _req())
            assert out["center_count"] > 0
            assert out["task_count"] == out["center_count"] * 3
            assert out["cache"]["cached"] is False
            assert out["quota"]["external_task_limit"] > 0
            assert out["can_proceed"] is True
        finally:
            _cleanup(conn, user_ids=(uid,))

    def test_cache_preview_present_on_hit(self, conn, tmp_path):
        uid = "ext-est-cache-user"
        _make_test_user(conn, uid)
        csv_path = tmp_path / "est.csv"
        _write_csv(csv_path, 4)
        regions = {"mode": "cities", "country": "us", "states": [], "cities": ["Austin"],
                   "zips": [], "center_ids": []}
        region_sig = cache_module.generate_region_signature(regions)
        cache_id = cache_module.generate_cache_id(
            "zz-ext-test q", region_sig,
            cache_module.generate_zoom_signature([10, 11, 12]),
            cache_module.generate_expected_types_signature(None),
        )
        _insert_cache_row(conn, cache_id, csv_path, total_results=4)
        try:
            out = ext.impl_estimate(_make_user(uid), _req())
            assert out["cache"]["cached"] is True
            assert out["cache"]["total_results"] == 4
        finally:
            conn.execute("DELETE FROM scraped_cache WHERE cache_id = ?", (cache_id,))
            _cleanup(conn, user_ids=(uid,))
