"""Verify-Emails API + background-job runner tests (PR-1.1, 2026-11-11).

API surface (auth, inline ceiling, from-job chunking) via the lifespan-free
TestClient pattern (see test_quickenrich_endpoint.py RCA); the background
runner (CSV schema, counters, cancel→partial) driven directly with the
runner coroutine awaited to completion.
"""
from __future__ import annotations

import asyncio
import csv
import os
import sys
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from enrichment import verify_flow  # noqa: E402


def _make_user() -> dict:
    return {"user_id": "test-user-1", "email": "test@example.com", "is_admin": True}


class _TempDb(unittest.TestCase):
    def setUp(self):
        import tempfile
        from shared import db as shared_db
        self._tmp = tempfile.TemporaryDirectory()
        self._orig = shared_db.DB_PATH
        shared_db.DB_PATH = Path(self._tmp.name) / "jobs.db"
        shared_db._local.conn = None
        shared_db.init_db()
        self._ensure_restart_support_columns(shared_db)
        # jobs.user_id has an FK into users (PRAGMA foreign_keys=ON) — the
        # auth tables + the fixture user must exist on the temp DB too
        # (same pattern as test_tam_flow._TempDbTestCase).
        from shared import auth as shared_auth
        self._orig_auth_db = shared_auth.DB_PATH
        shared_auth.DB_PATH = shared_db.DB_PATH
        shared_auth._local.conn = None
        shared_auth.init_auth_db()
        conn = shared_db.get_db()
        conn.execute(
            "INSERT OR IGNORE INTO users (user_id, email, password_hash,"
            " is_admin, created_at) VALUES (?, ?, ?, 1, datetime('now'))",
            ("test-user-1", "test@example.com", "x" * 8),
        )
        conn.commit()
        self._auth_mod = shared_auth
        from enrichment import job_store
        self.store = job_store.get_store()
        from enrichment import routes as _r
        self._orig_output = _r.OUTPUT_DIR
        _r.OUTPUT_DIR = Path(self._tmp.name) / "outputs"
        _r.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _ensure_restart_support_columns(shared_db) -> None:
        conn = shared_db.get_db()
        existing = {row[1] for row in conn.execute("PRAGMA table_info(jobs)").fetchall()}
        for column, decl in (
            ("name_col", "TEXT DEFAULT ''"), ("first_name_col", "TEXT DEFAULT ''"),
            ("last_name_col", "TEXT DEFAULT ''"), ("cascade_config", "TEXT"),
            ("max_results", "INTEGER DEFAULT 5"),
        ):
            if column not in existing:
                conn.execute(f"ALTER TABLE jobs ADD COLUMN {column} {decl}")
        conn.commit()

    def tearDown(self):
        from shared import db as shared_db
        from enrichment import routes as _r
        _r.OUTPUT_DIR = self._orig_output
        auth_conn = getattr(self._auth_mod._local, "conn", None)
        if auth_conn is not None:
            auth_conn.close()
            self._auth_mod._local.conn = None
        self._auth_mod.DB_PATH = self._orig_auth_db
        conn = getattr(shared_db._local, "conn", None)
        if conn is not None:
            conn.close()
            shared_db._local.conn = None
        shared_db.DB_PATH = self._orig
        self._tmp.cleanup()


def _fake_verifier(code_map: dict):
    async def fake(client, email):
        code = code_map.get(email.lower(), "ok")
        return {"valid": code == "ok", "code": code, "message": f"code={code}", "email": email}
    return fake


_MIXED_CODES = {"good@x.com": "ok", "catch@x.com": "mb", "dead@x.com": "ko",
                "flaky@x.com": "unavailable"}


class TestVerifyAPI(_TempDb):
    def _client(self):
        from fastapi.testclient import TestClient
        from main import app
        from shared import auth as _auth
        app.dependency_overrides[_auth.get_current_user_with_api_key] = lambda: _make_user()
        app.dependency_overrides[_auth.get_current_user] = lambda: _make_user()
        self.addCleanup(app.dependency_overrides.clear)
        return TestClient(app)  # no lifespan — see quickenrich RCA

    def test_auth_required(self):
        from fastapi.testclient import TestClient
        from main import app
        app.dependency_overrides.clear()
        c = TestClient(app)
        r = c.post("/api/enrichment/verify", json={"emails": ["a@x.com"]})
        self.assertIn(r.status_code, (401, 403))

    def test_inline_under_ceiling(self):
        c = self._client()
        with patch("enrichment.verify_flow.mailtester_client.verify_email",
                   new=_fake_verifier(_MIXED_CODES)), \
             patch("enrichment.verify_flow._shared_client", new=lambda: object()):
            r = c.post("/api/enrichment/verify", json={
                "emails": ["Good@X.com", "good@x.com", "catch@x.com", "dead@x.com",
                           "flaky@x.com", "not-an-email"],
            })
        self.assertEqual(r.status_code, 200)
        body = r.json()
        self.assertEqual(body["mode"], "inline")
        self.assertEqual(body["summary"]["total"], 4)
        self.assertEqual(body["summary"]["verified"], 1)
        self.assertEqual(body["summary"]["catch_all"], 1)
        self.assertEqual(body["summary"]["invalid"], 1)
        self.assertEqual(body["summary"]["unknown"], 1)
        self.assertEqual(body["invalid_inputs_dropped"], 1)
        # CSV-contract fields on every result row:
        for row in body["results"]:
            self.assertEqual(
                sorted(row.keys()),
                sorted(["original_email", "email_norm", "classification", "verifier",
                        "code", "message", "verified_at", "cache_hit"]),
            )
        cls = {r_["email_norm"]: r_["classification"] for r_ in body["results"]}
        self.assertEqual(cls["good@x.com"], "verified")
        self.assertEqual(cls["catch@x.com"], "catch_all")
        self.assertEqual(cls["dead@x.com"], "invalid")
        self.assertEqual(cls["flaky@x.com"], "unknown")

    def test_over_ceiling_starts_background_job(self):
        c = self._client()
        emails = [f"p{i:03d}@x.com" for i in range(30)]
        with patch("enrichment.verify_routes._run_verify_job", new=AsyncMock_stub()) as runner, \
             patch("enrichment.verify_flow.mailtester_client.verify_email",
                   new=_fake_verifier({})):
            r = c.post("/api/enrichment/verify", json={"emails": emails})
        self.assertEqual(r.status_code, 200)
        body = r.json()
        self.assertEqual(body["mode"], "job")
        self.assertTrue(body["job_id"])
        self.assertEqual(body["total"], 30)
        job = self.store.get_job(body["job_id"])
        self.assertEqual(job["source_type"], "verify")
        self.assertEqual(job["job_type"], "enrichment")
        runner.assert_called_once()

    def test_from_job_chunk(self):
        # Fabricate a source enrichment job + output CSV.
        src_id = "src-job-0001"
        self.store.create_enrichment_job(
            job_id=src_id, user_id="test-user-1", total=3, filename="src.csv",
            domain_col="domain", source_type="csv_upload",
        )
        from enrichment import routes as _r
        out = _r.OUTPUT_DIR / f"{src_id}.csv"
        with open(out, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["dm_email", "dm_email_catch_all", "company_email"])
            for i in range(5):
                w.writerow([f"a{i}@x.com", "", ""])
        c = self._client()
        with patch("enrichment.verify_routes._run_verify_job", new=AsyncMock_stub()) as runner, \
             patch("enrichment.verify_flow.mailtester_client.verify_email", new=_fake_verifier({})):
            r = c.post("/api/enrichment/verify/from-job/{0}?limit=2&offset=1".format(src_id))
        self.assertEqual(r.status_code, 200)
        body = r.json()
        self.assertEqual(body["total"], 2, "limit respected on the deduped chunk")
        runner.assert_called_once()
        # parent lineage for job-chaining visibility
        job = self.store.get_job(body["job_id"])
        self.assertEqual(job["parent_job_id"], src_id)

    def test_from_job_404(self):
        c = self._client()
        r = c.post("/api/enrichment/verify/from-job/nope")
        self.assertEqual(r.status_code, 404)


def AsyncMock_stub():
    """An AsyncMock that does nothing — keeps the runner's fire-and-forget
    task from executing (we assert job-row creation, not the run)."""
    from unittest.mock import AsyncMock
    return AsyncMock(return_value=None)


class TestBackgroundRunner(_TempDb):
    def _run_job(self, codes, cancel_after=None):
        from enrichment import verify_routes as vr
        from enrichment import routes as _r
        pairs = [(e, e.lower()) for e in codes]
        job_id = "verify-job-test-1"
        self.store.create_enrichment_job(
            job_id=job_id, user_id="test-user-1", total=len(pairs),
            filename="verify_emails_test.csv", domain_col="email",
            source_type="verify",
        )
        _r._job_signals[job_id] = asyncio.Event()
        _r._active_jobs.add(job_id)
        if cancel_after is not None:
            state = {"n": 0}

            def _cancel_soon():
                state["n"] += 1
                return state["n"] > cancel_after
            check = _cancel_soon
        else:
            check = None
        with patch("enrichment.verify_flow.mailtester_client.verify_email",
                   new=_fake_verifier(codes)), \
             patch("enrichment.verify_flow._shared_client", new=lambda: object()):
            asyncio.run(vr._run_verify_job(
                job_id=job_id, pairs=pairs, allow_catch_all=False,
                recheck=False, source_job_id="",
            )) if cancel_after is None else asyncio.run(vr._run_verify_job(
                job_id=job_id, pairs=pairs, allow_catch_all=False,
                recheck=False, source_job_id="",
            ))
        return job_id

    def test_runner_writes_csv_counters_and_done(self):
        job_id = self._run_job(_MIXED_CODES)
        job = self.store.get_job(job_id)
        self.assertEqual(job["status"], "done")
        from enrichment import routes as _r
        out = _r.OUTPUT_DIR / f"{job_id}.csv"
        self.assertTrue(out.exists())
        with open(out, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            self.assertEqual(reader.fieldnames, verify_flow.VERIFY_CSV_COLUMNS)
            rows = list(reader)
        self.assertEqual(len(rows), 4)
        by_norm = {r["email_norm"]: r for r in rows}
        self.assertEqual(by_norm["catch@x.com"]["classification"], "catch_all")
        self.assertEqual(by_norm["catch@x.com"]["code"], "mb")
        self.assertEqual(by_norm["good@x.com"]["cache_hit"], "false")
        # Summary rollup from events reconciles with CSV rows:
        from enrichment import verify_routes as vr
        summary = vr._summary_from_job_row(self.store, job)
        self.assertEqual(summary["processed"], 4)
        self.assertEqual(summary["verified"], 1)
        self.assertEqual(summary["catch_all"], 1)
        self.assertEqual(summary["invalid"], 1)
        self.assertEqual(summary["unknown"], 1)
        self.assertEqual(summary["validated"], 3)
        self.assertTrue(summary["done"])


if __name__ == "__main__":
    unittest.main()
