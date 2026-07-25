"""Isolated tests for the 6 phone-enrichment reliability fixes.

These deliberately avoid importing the full FastAPI app (``main``) or touching
the production DB. They drive the pipeline + job_store layers with lightweight
fakes and a fresh in-memory SQLite database. A minimal per-router FastAPI app
is used for the HTTP (route) tests so the heavy parent lifespan never runs.

Covers:
  * Fix 1 — SSE emits valid JSON
  * Fix 2 — idempotent schema guard + heartbeat/stale/abandon/cancel store ops
  * Fix 3 — 100% provider failure surfaced as FAILED
  * Fix 4 — cancellation (pipeline stops + endpoint)
  * Fix 5 — incremental CSV write (rows flushed as they complete)
  * Fix 6 — GET /jobs/{id} does not leak the absolute output_path
"""

import asyncio
import csv
import json
import sqlite3
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from phone_enrichment import job_store as phone_job_store
from phone_enrichment import pipeline as phone_pipeline
from phone_enrichment import routes as phone_routes


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _write_input_csv(path: Path, urls) -> Path:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["linkedin_url"])
        for u in urls:
            w.writerow([u])
    return path


class FakeJobStore:
    """Records lifecycle calls; backs OUTPUT_DIR with a temp dir."""

    def __init__(self, output_dir: Path):
        self.OUTPUT_DIR = output_dir
        self.status = None
        self.progress = None
        self.output_path = None
        self.error = None
        self.cancelled = False
        self.cache = {}

    def update_job_status(self, job_id, status):
        self.status = status

    def update_job_progress(self, job_id, processed, phones):
        self.progress = (processed, phones)

    def set_job_output(self, job_id, path):
        self.output_path = path

    def set_job_error(self, job_id, error):
        self.error = error
        self.status = "failed"

    def get_cached_phone(self, url):
        return None

    def cache_phone_enrichment(self, url, number, found):
        self.cache[url] = (number, found)

    def is_job_cancelled(self, job_id):
        return self.cancelled


def _client_with_job(monkeypatch, job_row):
    """Build a minimal app with only the phone router + stubbed auth/store."""
    app = FastAPI()
    app.include_router(phone_routes.router)
    app.dependency_overrides[phone_routes.get_current_user] = lambda: {"user_id": "u1"}
    monkeypatch.setattr(phone_routes.job_store, "get_phone_job", lambda jid: job_row)
    return TestClient(app)


# ---------------------------------------------------------------------------
# Fix 3 + Fix 5: pipeline happy path, failure path, incremental flush
# ---------------------------------------------------------------------------

def test_pipeline_happy_path_done_and_incremental_flush(tmp_path, monkeypatch):
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    fake = FakeJobStore(output_dir)
    monkeypatch.setattr(phone_pipeline, "job_store", fake)
    # Small batches so incremental flush is visible across batch boundaries
    monkeypatch.setattr(phone_pipeline, "BATCH_SIZE", 3)

    seen_line_counts = []

    async def fake_find_phone(client, url):
        # Prove rows are flushed incrementally: the output file already holds
        # the header + all rows completed in earlier batches.
        out = output_dir / "JOB1.csv"
        if out.exists():
            seen_line_counts.append(sum(1 for _ in open(out)))
        found = url.endswith("-yes")
        return {"found": found, "phone": "+1000" if found else None}

    monkeypatch.setattr(phone_pipeline, "find_phone", fake_find_phone)

    urls = [f"https://linkedin.com/in/p{i}-yes" for i in range(6)]
    urls += [f"https://linkedin.com/in/p{i}-no" for i in range(3)]  # 9 rows -> 3 batches
    input_csv = _write_input_csv(tmp_path / "in.csv", urls)

    res = asyncio.run(phone_pipeline.run_phone_enrichment(
        job_id="JOB1", input_path=input_csv, linkedin_col="linkedin_url"))

    assert res["status"] == "done"
    assert fake.status == "done"
    assert res["phones_found"] == 6
    assert res["processed"] == 9

    rows = list(csv.DictReader(open(output_dir / "JOB1.csv")))
    assert len(rows) == 9  # header + 9 data rows, all retained

    # Incremental flush: line count seen during processing grew across batches
    assert seen_line_counts, "find_phone should have observed the output file"
    assert seen_line_counts[0] >= 1  # at least the header
    assert seen_line_counts[-1] > seen_line_counts[0]


def test_pipeline_all_provider_failures_marked_failed(tmp_path, monkeypatch):
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    fake = FakeJobStore(output_dir)
    monkeypatch.setattr(phone_pipeline, "job_store", fake)

    async def boom(client, url):
        raise RuntimeError("blitz is down")

    monkeypatch.setattr(phone_pipeline, "find_phone", boom)

    input_csv = _write_input_csv(
        tmp_path / "in.csv",
        [f"https://linkedin.com/in/p{i}" for i in range(5)],
    )

    res = asyncio.run(phone_pipeline.run_phone_enrichment(
        job_id="JOB1", input_path=input_csv, linkedin_col="linkedin_url"))

    # Fix 3: a 100% provider failure must NOT be a false 'done'
    assert res["status"] == "failed"
    assert fake.status == "failed"
    assert fake.error and "0 phones" in fake.error
    # Output file still written with empty phone columns
    rows = list(csv.DictReader(open(output_dir / "JOB1.csv")))
    assert len(rows) == 5
    assert all(r["phone_found"] == "false" for r in rows)


def test_pipeline_partial_failure_still_done_when_some_phones_found(tmp_path, monkeypatch):
    """If some rows succeed, the job is done (only *all*-failure -> failed)."""
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    fake = FakeJobStore(output_dir)
    monkeypatch.setattr(phone_pipeline, "job_store", fake)
    monkeypatch.setattr(phone_pipeline, "BATCH_SIZE", 10)

    async def mixed(client, url):
        if url.endswith("-boom"):
            raise RuntimeError("transient")
        return {"found": True, "phone": "+1"}

    monkeypatch.setattr(phone_pipeline, "find_phone", mixed)

    urls = ["https://linkedin.com/in/ok-1", "https://linkedin.com/in/ok-2", "https://linkedin.com/in/bad-1-boom"]
    input_csv = _write_input_csv(tmp_path / "in.csv", urls)

    res = asyncio.run(phone_pipeline.run_phone_enrichment(
        job_id="JOB1", input_path=input_csv, linkedin_col="linkedin_url"))
    assert res["status"] == "done"
    assert res["phones_found"] == 2


# ---------------------------------------------------------------------------
# Fix 4: pipeline cancellation
# ---------------------------------------------------------------------------

def test_pipeline_cancellation_marks_cancelled_and_stops_promptly(tmp_path, monkeypatch):
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    fake = FakeJobStore(output_dir)
    monkeypatch.setattr(phone_pipeline, "job_store", fake)

    async def must_not_run(client, url):
        raise AssertionError("find_phone must not run when the job is cancelled")

    monkeypatch.setattr(phone_pipeline, "find_phone", must_not_run)

    input_csv = _write_input_csv(
        tmp_path / "in.csv",
        [f"https://linkedin.com/in/p{i}" for i in range(5)],
    )

    res = asyncio.run(phone_pipeline.run_phone_enrichment(
        job_id="JOB1",
        input_path=input_csv,
        linkedin_col="linkedin_url",
        cancelled_jobs={"JOB1"},
    ))

    assert res["status"] == "cancelled"
    assert fake.status == "cancelled"
    assert res["processed"] == 0  # stopped before any API call


# ---------------------------------------------------------------------------
# Fix 1: SSE emits valid JSON
# ---------------------------------------------------------------------------

def test_sse_event_stream_emits_valid_json(monkeypatch):
    events = [
        {"seq": 0, "type": "progress", "processed": 1, "total": 3},
        {"seq": 1, "type": "progress", "processed": 2, "total": 3},
    ]
    states = iter([{"status": "running"}, {"status": "done"}])
    monkeypatch.setattr(phone_routes.job_store, "get_phone_job", lambda jid: next(states))
    monkeypatch.setattr(
        phone_routes.job_store,
        "get_events_since",
        lambda jid, since: [e for e in events if e["seq"] > since],
    )
    sig = asyncio.Event()
    sig.set()
    phone_routes._job_signals["JOBX"] = sig
    try:

        async def drive():
            return [line async for line in phone_routes._sse_event_stream("JOBX")]

        lines = asyncio.run(drive())
    finally:
        phone_routes._job_signals.pop("JOBX", None)

    assert lines, "expected at least one SSE line"
    payloads = []
    for line in lines:
        assert line.startswith("data: ")
        assert line.endswith("\n\n")
        payload = line[len("data: "):].strip()
        obj = json.loads(payload)  # every payload must be valid JSON
        assert isinstance(obj, dict)
        payloads.append(obj)
    # A terminal status marker must be present
    assert any(o.get("type") == "status" and o.get("status") == "done" for o in payloads)


# ---------------------------------------------------------------------------
# Fix 2: idempotent schema guard + lifecycle store ops
# ---------------------------------------------------------------------------

def _fresh_jobs_conn():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE jobs (
            job_id TEXT PRIMARY KEY, user_id TEXT, job_type TEXT, status TEXT,
            total INTEGER, processed INTEGER, phones_found INTEGER,
            linkedin_col TEXT, error TEXT, output_path TEXT,
            created_at TEXT, updated_at TEXT
        )"""
    )
    return conn


def test_ensure_phone_schema_is_idempotent_and_adds_columns(monkeypatch):
    conn = _fresh_jobs_conn()
    monkeypatch.setattr(phone_job_store, "get_db", lambda: conn)

    phone_job_store._ensure_phone_schema()
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(jobs)")}
    assert "last_heartbeat" in cols
    assert "cancelled_at" in cols

    # Running again must be a safe no-op
    phone_job_store._ensure_phone_schema()
    cols2 = {r["name"] for r in conn.execute("PRAGMA table_info(jobs)")}
    assert cols == cols2


def test_phone_job_store_lifecycle_ops(monkeypatch):
    conn = _fresh_jobs_conn()
    monkeypatch.setattr(phone_job_store, "get_db", lambda: conn)
    phone_job_store._ensure_phone_schema()

    conn.execute(
        "INSERT INTO jobs (job_id, job_type, status, created_at, updated_at, last_heartbeat) "
        "VALUES ('j1','phone_enrichment','running','2020-01-01T00:00:00','2020-01-01T00:00:00','2020-01-01T00:00:00')"
    )
    conn.commit()

    # Stale detection picks up the old-heartbeat running job
    assert phone_job_store.get_stale_running_phone_jobs() == ["j1"]

    # heartbeat updates last_heartbeat
    phone_job_store.heartbeat("j1")
    hb = conn.execute("SELECT last_heartbeat FROM jobs WHERE job_id='j1'").fetchone()["last_heartbeat"]
    assert hb != "2020-01-01T00:00:00"

    # set_abandoned
    phone_job_store.set_abandoned("j1", "crashed")
    assert conn.execute("SELECT status FROM jobs WHERE job_id='j1'").fetchone()["status"] == "abandoned"

    # set_cancelled + is_job_cancelled (requires cancelled_at column added above)
    conn.execute("UPDATE jobs SET status='running' WHERE job_id='j1'")
    conn.commit()
    phone_job_store.set_cancelled("j1")
    assert phone_job_store.is_job_cancelled("j1") is True
    assert conn.execute("SELECT cancelled_at FROM jobs WHERE job_id='j1'").fetchone()["cancelled_at"]


# ---------------------------------------------------------------------------
# Fix 6 + Fix 4 (HTTP): detail serializer + cancel endpoint
# ---------------------------------------------------------------------------

def test_detail_endpoint_does_not_leak_output_path(monkeypatch, tmp_path):
    out = tmp_path / "x.csv"
    out.write_text("linkedin_url,phone_number,phone_found")
    job_row = {
        "job_id": "j1", "user_id": "u1", "status": "done",
        "original_filename": "f.csv", "linkedin_col": "linkedin_url",
        "total": 3, "processed": 3, "phones_found": 1, "error": None,
        "output_path": str(out), "created_at": "t", "updated_at": "t",
    }
    client = _client_with_job(monkeypatch, job_row)
    r = client.get("/api/phone-enrichment/jobs/j1")
    assert r.status_code == 200
    body = r.json()
    assert "output_path" not in body  # Fix 6: absolute path never exposed
    assert body["has_output"] is True


def test_detail_endpoint_has_output_false_when_file_missing(monkeypatch, tmp_path):
    job_row = {
        "job_id": "j1", "user_id": "u1", "status": "running",
        "original_filename": "f.csv", "linkedin_col": "linkedin_url",
        "total": 3, "processed": 1, "phones_found": 0, "error": None,
        "output_path": str(tmp_path / "does-not-exist.csv"),
        "created_at": "t", "updated_at": "t",
    }
    client = _client_with_job(monkeypatch, job_row)
    body = client.get("/api/phone-enrichment/jobs/j1").json()
    assert body["has_output"] is False
    assert "output_path" not in body


def test_cancel_endpoint_sets_flag_and_persists(monkeypatch):
    job_row = {"job_id": "j1", "user_id": "u1", "status": "running"}
    client = _client_with_job(monkeypatch, job_row)

    persisted = {}
    monkeypatch.setattr(phone_routes.job_store, "set_cancelled", lambda jid: persisted.setdefault("jid", jid))
    phone_routes._cancelled_phone_jobs.discard("j1")
    try:
        r = client.post("/api/phone-enrichment/jobs/j1/cancel")
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "cancelled"
        assert "j1" in phone_routes._cancelled_phone_jobs  # in-memory flag set
        assert persisted.get("jid") == "j1"  # DB persistence called
    finally:
        phone_routes._cancelled_phone_jobs.discard("j1")


def test_cancel_endpoint_rejects_non_running(monkeypatch):
    job_row = {"job_id": "j1", "user_id": "u1", "status": "done"}
    client = _client_with_job(monkeypatch, job_row)
    r = client.post("/api/phone-enrichment/jobs/j1/cancel")
    assert r.status_code == 400


# ---------------------------------------------------------------------------
# Download endpoint still works (happy-path preservation)
# ---------------------------------------------------------------------------

def test_download_endpoint_serves_file(monkeypatch, tmp_path):
    out = tmp_path / "dl.csv"
    out.write_text("linkedin_url,phone_number,phone_found\nhttps://li.com,123,true\n")
    job_row = {
        "job_id": "j1", "user_id": "u1", "status": "done",
        "original_filename": "src", "linkedin_col": "linkedin_url",
        "total": 1, "processed": 1, "phones_found": 1, "error": None,
        "output_path": str(out), "created_at": "t", "updated_at": "t",
    }
    client = _client_with_job(monkeypatch, job_row)
    r = client.get("/api/phone-enrichment/jobs/j1/download")
    assert r.status_code == 200
    assert "attachment" in r.headers.get("content-disposition", "")
    assert "123" in r.text
