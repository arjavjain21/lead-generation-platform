"""Tests for enrichment/webscrape_followup.py — waterfall-miss → scrape-emails
followup lifecycle.

Pins:
* Miss extraction: only definitive no-email rows with a valid domain; dedup;
  rows holding any email column (dm_email / company_email / final_email) or a
  non-definitive row_status (skipped / error) are NOT misses.
* Lifecycle: pending_submit → submitted (idempotency, one followup per job),
  submit-failure stays pending with attempts counter, exhausted → failed.
* Poll → finalize: counters updated, CSV written with exact columns, status
  done, notification fired once; finalize is idempotent under double-fire.
* Lease: exactly one poller holder wins; an expired lease can be re-taken.
* Webhook: HMAC-verified batch.completed triggers an advance; bad signature
  and missing secret are rejected without side effects.
* Auto-submit eligibility: website_only and restricted-provider jobs never
  auto-submit; a disabled kill-switch short-circuits everything.

NOTE: pytest-asyncio is NOT installed — async work goes through asyncio.run().
"""

from __future__ import annotations

import asyncio
import csv
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

_BACKEND = Path(__file__).resolve().parents[2]
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

import pytest  # noqa: E402

from enrichment import webscrape_dash_client as dash  # noqa: E402
from enrichment import webscrape_followup as wf  # noqa: E402
from shared import db as shared_db  # noqa: E402


def run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def fresh_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Isolated jobs.db for the followup tables (test_blitz_miss_store pattern)."""
    tmp_db = tmp_path / "followup_jobs.db"
    monkeypatch.setattr(shared_db, "DB_PATH", tmp_db)
    monkeypatch.setattr(shared_db._local, "conn", None, raising=False)
    monkeypatch.setattr(wf, "_schema_initialized", False)
    # Followup CSVs land next to the temp DB, never in the real outputs dir.
    monkeypatch.setattr(wf, "OUTPUT_DIR", tmp_path / "outputs")
    monkeypatch.setenv("WEBSCRAPER_API_KEY", "sk_live_test")
    monkeypatch.setenv("WEBCSCRAPE_FOLLOWUP_ENABLED", "true")
    yield tmp_db
    if getattr(shared_db._local, "conn", None) is not None:
        shared_db._local.conn.close()
        shared_db._local.conn = None


class FakeDashClient:
    """Scriptable stand-in for dash.WebscrapeDashClient (monkeypatched in)."""

    def __init__(self):
        self.submits: list[list[str]] = []
        self.cancels: list[str] = []
        self.estimate_response: dict[str, Any] = {}
        self.submit_error: Optional[Exception] = None
        self.job_states: dict[str, dict[str, Any]] = {}
        self.results: dict[str, list[dict[str, Any]]] = {}

    async def estimate(self, websites, *, client_tag=None):
        out = dict(self.estimate_response or {"total": len(websites), "new": len(websites)})
        return out

    async def submit(self, websites, *, client_tag, idempotency_key, callback_url=None):
        if self.submit_error:
            raise self.submit_error
        self.submits.append(websites)
        job_id = f"dash-{len(self.submits)}"
        state = dict(self.job_states.get(job_id, {}))
        state.setdefault("total", len(websites))
        self.job_states[job_id] = state
        return {
            "job_id": job_id,
            "status": "queued",
            "total": len(websites),
            "new": len(websites),
            "requeued": 0,
            "already_done": 0,
            "invalid": 0,
        }

    async def get_job(self, job_id, *, cursor=None, limit=1000):
        state = self.job_states.get(job_id, {})
        page = state.get("page", [])
        out = {
            "job_id": job_id,
            "status": state.get("status", "processing"),
            "total": state.get("total", 0),
            "processed": state.get("processed", state.get("total", 0)),
            "emails_found": state.get("emails_found", 0),
            "queued": state.get("queued", 0),
            "done": state.get("done", False),
            "eta_min": state.get("eta_min"),
            "next_cursor": None,
            "results": page if cursor is None else [],
        }
        return out

    async def collect_results(self, job_id, *, page_limit=1000, max_rows=200000):
        return self.results.get(job_id, [])

    async def cancel(self, job_id):
        self.cancels.append(job_id)
        return {"cancelled": 1}


@pytest.fixture
def fake_dash(monkeypatch: pytest.MonkeyPatch) -> FakeDashClient:
    fake = FakeDashClient()
    monkeypatch.setattr(wf.dash, "WebscrapeDashClient", lambda: fake)
    return fake


def _write_results_csv(path: Path, rows: list[dict[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(dict.fromkeys(k for r in rows for k in r)) or ["input_domain"]
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return path


def _sample_results_csv(path: Path) -> Path:
    rows = [
        {"input_domain": "hit.com", "dm_email": "ceo@hit.com", "row_status": "enriched"},
        {"input_domain": "miss1.com", "dm_email": "", "row_status": "no_contacts"},
        {"input_domain": "miss2.com", "dm_email": "", "row_status": "not_found"},
        {"input_domain": "miss1.com", "dm_email": "", "row_status": "not_found"},  # dup
        {"input_domain": "company.com", "dm_email": "", "company_email": "info@company.com", "row_status": "enriched"},
        {"input_domain": "skipme.com", "dm_email": "", "row_status": "skipped"},
        {"input_domain": "err.com", "dm_email": "", "row_status": "error"},
        {"input_domain": "https://www.norm.example/deep", "dm_email": "", "row_status": "no_contacts"},
        {"input_domain": "", "dm_email": "", "row_status": "no_contacts"},
    ]
    return _write_results_csv(path, rows)


# ---------------------------------------------------------------------------
# Miss extraction
# ---------------------------------------------------------------------------


def test_extract_miss_domains_filters_and_dedups(tmp_path: Path):
    csv_path = _sample_results_csv(tmp_path / "job.csv")
    misses = wf.extract_miss_domains(csv_path)
    assert misses == ["miss1.com", "miss2.com", "norm.example"]


def test_extract_missing_file_returns_empty(tmp_path: Path):
    assert wf.extract_miss_domains(tmp_path / "nope.csv") == []


# ---------------------------------------------------------------------------
# create_followup / try_submit
# ---------------------------------------------------------------------------


def test_create_followup_submits_and_is_idempotent(fresh_db, fake_dash, tmp_path):
    csv_path = _sample_results_csv(tmp_path / "job.csv")
    first = run(wf.create_followup("job-1", csv_path, origin="auto"))
    assert first["created"] is True
    row = wf.get_followup("job-1")
    assert row["status"] == wf.STATUS_SUBMITTED
    assert row["domains_total"] == 3
    assert json.loads(row["domains_json"]) == ["miss1.com", "miss2.com", "norm.example"]
    assert [b["job_id"] for b in json.loads(row["batches_json"])] == ["dash-1"]
    assert fake_dash.submits == [["miss1.com", "miss2.com", "norm.example"]]

    second = run(wf.create_followup("job-1", csv_path, origin="auto"))
    assert second["created"] is False
    assert len(fake_dash.submits) == 1  # no double submit


def test_create_followup_no_misses(fresh_db, fake_dash, tmp_path):
    csv_path = _write_results_csv(
        tmp_path / "full.csv",
        [{"input_domain": "hit.com", "dm_email": "a@hit.com", "row_status": "enriched"}],
    )
    out = run(wf.create_followup("job-2", csv_path, origin="manual_backfill"))
    assert out == {"created": False, "reason": "no_misses"}
    assert wf.get_followup("job-2") is None


def test_submit_failure_stays_pending_then_exhausts(fresh_db, fake_dash, tmp_path):
    csv_path = _sample_results_csv(tmp_path / "job.csv")
    fake_dash.submit_error = dash.WebscrapeDashBusy("backpressure", retry_after_s=5)
    out = run(wf.create_followup("job-3", csv_path, origin="auto"))
    assert out["created"] is True
    row = wf.get_followup("job-3")
    assert row["status"] == wf.STATUS_PENDING_SUBMIT
    assert row["submit_attempts"] == 1
    assert "backpressure" in row["error"]

    # Simulate the poller retrying until the attempt cap.
    conn = shared_db.get_db()
    conn.execute(
        "UPDATE webscrape_followups SET updated_at=? WHERE parent_job_id='job-3'",
        ((datetime.now(timezone.utc) - timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",),
    )
    conn.commit()
    for _ in range(wf.MAX_SUBMIT_ATTEMPTS):
        run(wf.try_submit("job-3"))
    row = wf.get_followup("job-3")
    assert row["status"] == wf.STATUS_FAILED


def test_try_submit_recovers_after_transient_failure(fresh_db, fake_dash, tmp_path):
    csv_path = _sample_results_csv(tmp_path / "job.csv")
    fake_dash.submit_error = dash.WebscrapeDashError("boom")
    run(wf.create_followup("job-4", csv_path, origin="auto"))
    fake_dash.submit_error = None
    assert run(wf.try_submit("job-4")) is True
    assert wf.get_followup("job-4")["status"] == wf.STATUS_SUBMITTED


# ---------------------------------------------------------------------------
# Poll → finalize
# ---------------------------------------------------------------------------


def _notify_recorder(monkeypatch):
    calls: list[tuple] = []

    async def fake_notify(parent_job_id, emails_found, total):
        calls.append((parent_job_id, emails_found, total))

    monkeypatch.setattr(wf, "_notify_done", fake_notify)
    return calls


def test_poll_tick_runs_to_done_with_csv_and_notification(fresh_db, fake_dash, tmp_path, monkeypatch):
    notified = _notify_recorder(monkeypatch)
    csv_path = _sample_results_csv(tmp_path / "job.csv")
    run(wf.create_followup("job-5", csv_path, origin="auto"))
    batch_job = json.loads(wf.get_followup("job-5")["batches_json"])[0]["job_id"]
    fake_dash.job_states[batch_job] = {
        "total": 3, "processed": 3, "emails_found": 1, "done": True, "status": "done",
    }
    fake_dash.results[batch_job] = [
        {"website": "miss1.com", "email": "info@miss1.com", "found": True,
         "email_type": "domain_generic", "email_source": "homepage",
         "business_name": "Miss1", "status": "completed", "confidence": 0.95},
        {"website": "miss2.com", "email": None, "found": False, "status": "no_email",
         "business_name": "", "confidence": 0.0},
    ]
    advanced = run(wf.poll_tick("w1"))
    assert advanced == 1
    row = wf.get_followup("job-5")
    assert row["status"] == wf.STATUS_DONE
    assert row["emails_found"] == 1
    assert row["no_email_count"] == 1
    assert row["csv_path"].endswith("job-5_website_emails.csv")
    assert Path(row["csv_path"]).exists()
    with open(row["csv_path"], newline="") as fh:
        rows = list(csv.DictReader(fh))
    assert rows[0]["domain"] == "miss1.com" and rows[0]["email"] == "info@miss1.com"
    assert rows[0]["found"] == "true" and rows[0]["email_type"] == "domain_generic"
    assert rows[1]["found"] == "false"
    assert notified == [("job-5", 1, 2)]


def test_finalize_idempotent_under_double_fire(fresh_db, fake_dash, tmp_path, monkeypatch):
    notified = _notify_recorder(monkeypatch)
    csv_path = _sample_results_csv(tmp_path / "job.csv")
    run(wf.create_followup("job-6", csv_path, origin="auto"))
    batch_job = json.loads(wf.get_followup("job-6")["batches_json"])[0]["job_id"]
    fake_dash.job_states[batch_job] = {"total": 3, "processed": 3, "emails_found": 0, "done": True}
    fake_dash.results[batch_job] = []
    run(wf.poll_tick("w1"))
    # Webhook + poller race: a direct finalize after done must be a no-op.
    run(wf._finalize("job-6"))
    assert notified == [("job-6", 0, 0)]


def test_poll_tick_sets_processing_while_in_flight(fresh_db, fake_dash, tmp_path):
    csv_path = _sample_results_csv(tmp_path / "job.csv")
    run(wf.create_followup("job-7", csv_path, origin="auto"))
    batch_job = json.loads(wf.get_followup("job-7")["batches_json"])[0]["job_id"]
    fake_dash.job_states[batch_job] = {
        "total": 3, "processed": 1, "emails_found": 1, "queued": 2,
        "done": False, "status": "processing", "eta_min": 4,
    }
    run(wf.poll_tick("w1"))
    row = wf.get_followup("job-7")
    assert row["status"] == wf.STATUS_PROCESSING
    assert row["processed"] == 1 and row["emails_found"] == 1 and row["eta_min"] == 4


# ---------------------------------------------------------------------------
# Lease
# ---------------------------------------------------------------------------


def test_lease_single_winner_until_expiry(fresh_db):
    assert wf._acquire_lease("worker-a") is True
    assert wf._acquire_lease("worker-b") is False
    # Force expiry, then b can take over.
    conn = shared_db.get_db()
    conn.execute("UPDATE webscrape_followup_poller SET expires_at='2000-01-01T00:00:00.000Z'")
    conn.commit()
    assert wf._acquire_lease("worker-b") is True


def test_poll_tick_respects_kill_switch(fresh_db, fake_dash, monkeypatch):
    monkeypatch.setenv("WEBCSCRAPE_FOLLOWUP_ENABLED", "false")
    assert run(wf.poll_tick("w1")) == 0


# ---------------------------------------------------------------------------
# Webhook
# ---------------------------------------------------------------------------


def _signed(payload: dict[str, Any], secret: str):
    import hashlib
    import hmac as hmac_mod

    raw = json.dumps(payload).encode()
    sig = "sha256=" + hmac_mod.new(secret.encode(), raw, hashlib.sha256).hexdigest()
    return raw, sig


def test_webhook_good_signature_triggers_advance(fresh_db, fake_dash, tmp_path, monkeypatch):
    monkeypatch.setenv("WEBSCRAPER_WEBHOOK_SECRET", "whsec-test")
    csv_path = _sample_results_csv(tmp_path / "job.csv")
    run(wf.create_followup("job-8", csv_path, origin="auto"))
    batch_job = json.loads(wf.get_followup("job-8")["batches_json"])[0]["job_id"]

    advanced: list[str] = []

    async def fake_advance(row):
        advanced.append(row["parent_job_id"])

    monkeypatch.setattr(wf, "_advance_followup", fake_advance)
    raw, sig = _signed({"type": "batch.completed", "job_id": batch_job}, "whsec-test")
    out = run(wf.handle_batch_webhook(raw, sig))
    assert out == {"ok": True, "parent_job_id": "job-8"}
    assert advanced == ["job-8"]


def test_webhook_rejects_bad_signature_and_missing_secret(fresh_db, fake_dash, monkeypatch):
    monkeypatch.setenv("WEBSCRAPER_WEBHOOK_SECRET", "whsec-test")
    raw, _ = _signed({"type": "batch.completed", "job_id": "dash-x"}, "whsec-test")
    assert run(wf.handle_batch_webhook(raw, "sha256=deadbeef"))["ok"] is False
    monkeypatch.delenv("WEBSCRAPER_WEBHOOK_SECRET", raising=False)
    assert run(wf.handle_batch_webhook(raw, "sha256=deadbeef"))["reason"] == "webhook secret not configured"


# ---------------------------------------------------------------------------
# Cancel + summaries
# ---------------------------------------------------------------------------


def test_cancel_followup_calls_remote_and_marks_cancelled(fresh_db, fake_dash, tmp_path):
    csv_path = _sample_results_csv(tmp_path / "job.csv")
    run(wf.create_followup("job-9", csv_path, origin="auto"))
    out = run(wf.cancel_followup("job-9"))
    assert out == {"cancelled": True, "remote_batches_cancelled": 1}
    assert fake_dash.cancels == ["dash-1"]
    assert wf.get_followup("job-9")["status"] == wf.STATUS_CANCELLED
    assert run(wf.cancel_followup("job-9"))["reason"] == "already_cancelled"


def test_cancelled_followup_can_be_revived(fresh_db, fake_dash, tmp_path):
    csv_path = _sample_results_csv(tmp_path / "job.csv")
    run(wf.create_followup("job-11", csv_path, origin="auto"))
    run(wf.cancel_followup("job-11"))
    out = run(wf.create_followup("job-11", csv_path, origin="manual_backfill"))
    assert out.get("created") is True and out.get("revived") is True
    assert wf.get_followup("job-11")["status"] == wf.STATUS_SUBMITTED
    assert len(fake_dash.submits) == 2  # original + revive


def test_summaries_for_jobs_batched(fresh_db, fake_dash, tmp_path):
    csv_path = _sample_results_csv(tmp_path / "job.csv")
    run(wf.create_followup("job-10", csv_path, origin="auto"))
    summaries = wf.summaries_for_jobs(["job-10", "unknown"])
    assert set(summaries) == {"job-10"}
    assert summaries["job-10"]["status"] == wf.STATUS_SUBMITTED
    assert wf.summaries_for_jobs([]) == {}


# ---------------------------------------------------------------------------
# Auto-submit eligibility
# ---------------------------------------------------------------------------


class _FakeStore:
    def __init__(self, job: Optional[dict[str, Any]]):
        self._job = job

    def get_job(self, job_id: str) -> Optional[dict[str, Any]]:
        return self._job


def _patch_job(monkeypatch, job: Optional[dict[str, Any]]) -> None:
    from enrichment import job_store

    monkeypatch.setattr(job_store, "get_store", lambda: _FakeStore(job))


def _eligible_job() -> dict[str, Any]:
    return {
        "job_id": "job-auto", "job_type": "enrichment", "status": "done",
        "website_only": 0, "selected_providers": "", "user_id": "u1",
    }


def test_auto_submit_eligible_job_creates_followup(fresh_db, fake_dash, tmp_path, monkeypatch):
    _patch_job(monkeypatch, _eligible_job())
    csv_path = _sample_results_csv(tmp_path / "job.csv")
    run(wf.auto_submit_for_job("job-auto", csv_path))
    assert wf.get_followup("job-auto")["origin"] == "auto"


def test_auto_submit_skips_website_only(fresh_db, fake_dash, tmp_path, monkeypatch):
    job = {**_eligible_job(), "website_only": 1}
    _patch_job(monkeypatch, job)
    run(wf.auto_submit_for_job("job-auto", _sample_results_csv(tmp_path / "job.csv")))
    assert wf.get_followup("job-auto") is None


def test_auto_submit_skips_restricted_providers(fresh_db, fake_dash, tmp_path, monkeypatch):
    job = {**_eligible_job(), "selected_providers": '["contacts_db"]'}
    _patch_job(monkeypatch, job)
    run(wf.auto_submit_for_job("job-auto", _sample_results_csv(tmp_path / "job.csv")))
    assert wf.get_followup("job-auto") is None


def test_auto_submit_never_raises(fresh_db, fake_dash, tmp_path, monkeypatch):
    _patch_job(monkeypatch, None)  # job vanished
    run(wf.auto_submit_for_job("ghost", tmp_path / "missing.csv"))  # must not raise


def test_auto_submit_kill_switch(fresh_db, fake_dash, tmp_path, monkeypatch):
    monkeypatch.setenv("WEBCSCRAPE_FOLLOWUP_ENABLED", "false")
    _patch_job(monkeypatch, _eligible_job())
    run(wf.auto_submit_for_job("job-auto", _sample_results_csv(tmp_path / "job.csv")))
    assert wf.get_followup("job-auto") is None


# ---------------------------------------------------------------------------
# Merge-back (rung 2, 2026-11-19): curated website emails -> main CSV +
# Contacts DB write-back.
# ---------------------------------------------------------------------------


def _insert_parent_job(job_id: str, output_csv: Path) -> None:
    conn = shared_db.get_db()
    conn.execute(
        "CREATE TABLE IF NOT EXISTS jobs ("
        " job_id TEXT PRIMARY KEY, job_type TEXT, status TEXT,"
        " output_path TEXT DEFAULT '', partial_output_path TEXT DEFAULT '',"
        " created_at TEXT DEFAULT '', updated_at TEXT DEFAULT '')"
    )
    conn.execute(
        "INSERT INTO jobs (job_id, job_type, status, output_path, created_at, updated_at) "
        "VALUES (?, 'enrichment', 'completed', ?, ?, ?)",
        (job_id, str(output_csv), "2026-11-19T00:00:00Z", "2026-11-19T00:00:00Z"),
    )
    conn.commit()


def _main_csv(path: Path) -> Path:
    rows = [
        {"domain": "miss1.com", "dm_email": "", "row_status": "no_contacts"},
        {"domain": "miss1.com", "dm_email": "", "row_status": "not_found"},
        {"domain": "hit.com", "dm_email": "ceo@hit.com", "row_status": "enriched"},
        {"domain": "company.com", "dm_email": "", "company_email": "info@company.com", "row_status": "enriched"},
        {"domain": "nomatch.org", "dm_email": "", "row_status": "no_contacts"},
    ]
    return _write_results_csv(path, rows)


def test_curate_followup_emails_keeps_only_on_domain_role_addresses():
    results = [
        {"website": "a.com", "email": "info@a.com", "found": True, "status": "completed", "business_name": "A"},
        {"website": "b.com", "email": "owner@gmail.com", "found": True, "status": "completed", "business_name": "B"},  # freemail
        {"website": "c.com", "email": "info@other.com", "found": True, "status": "completed", "business_name": "C"},  # off-domain
        {"website": "d.com", "email": "jane@d.com", "found": True, "status": "completed", "business_name": "D"},      # named person
        {"website": "e.com", "email": "contact@e.com", "found": False, "status": "no_email", "business_name": "E"},   # not found
        {"website": "f.com", "email": "frontdesk@f.com", "found": True, "status": "completed", "business_name": "F"},
    ]
    curated = wf._curate_followup_emails(results)
    assert set(curated) == {"a.com", "f.com"}
    assert curated["a.com"]["email"] == "info@a.com"
    assert curated["f.com"]["email"] == "frontdesk@f.com"


def test_merge_into_main_csv_fills_only_eligible_rows_and_is_idempotent(fresh_db, tmp_path):
    main = _main_csv(tmp_path / "main.csv")
    _insert_parent_job("job-merge", main)
    curated = {
        "miss1.com": {"email": "info@miss1.com", "business_name": "Miss1"},
        "hit.com": {"email": "info@hit.com", "business_name": "Hit"},      # row has dm_email -> untouched
        "company.com": {"email": "sales@company.com", "business_name": "Co"},  # row has company_email -> untouched
        "missing.org": {"email": "info@missing.org", "business_name": ""},     # no matching rows
    }
    merged = wf._merge_into_main_csv("job-merge", curated)
    assert merged == 2  # the two miss1.com rows
    with open(main, newline="") as fh:
        rows = list(csv.DictReader(fh))
    by_domain = [r for r in rows if r["domain"] == "miss1.com"]
    assert all(r["company_email"] == "info@miss1.com" for r in by_domain)
    assert all(r["company_email_source"] == "webscrape_followup" for r in by_domain)
    assert all(r["final_email"] == "info@miss1.com" for r in by_domain)
    assert all(r["final_email_level"] == "company" for r in by_domain)
    hit = next(r for r in rows if r["domain"] == "hit.com")
    assert hit["dm_email"] == "ceo@hit.com" and not hit.get("company_email")
    co = next(r for r in rows if r["domain"] == "company.com")
    assert co["company_email"] == "info@company.com"  # not overwritten
    # Idempotent: second run changes nothing and leaves no tmp file.
    assert wf._merge_into_main_csv("job-merge", curated) == 0
    assert not list(tmp_path.glob("*.merging.tmp"))


def test_finalize_merges_back_and_writes_to_contacts_db(fresh_db, fake_dash, tmp_path, monkeypatch):
    notified = _notify_recorder(monkeypatch)
    main = _main_csv(tmp_path / "main.csv")
    _insert_parent_job("job-mb", main)
    # The followup submitter reads the parent CSV for miss domains.
    miss_csv = _write_results_csv(tmp_path / "miss.csv", [
        {"input_domain": "miss1.com", "dm_email": "", "row_status": "no_contacts"},
    ])
    run(wf.create_followup("job-mb", miss_csv, origin="auto"))
    batch_job = json.loads(wf.get_followup("job-mb")["batches_json"])[0]["job_id"]
    fake_dash.job_states[batch_job] = {"total": 2, "processed": 2, "emails_found": 2, "done": True}
    fake_dash.results[batch_job] = [
        {"website": "miss1.com", "email": "info@miss1.com", "found": True, "status": "completed",
         "business_name": "Miss1 Corp", "confidence": 0.95},
        {"website": "miss1.com", "email": "someone@yahoo.com", "found": True, "status": "completed",
         "business_name": "Miss1 Corp", "confidence": 0.5},  # freemail -> not merged
    ]
    written: list[list[dict[str, Any]]] = []

    async def fake_batch(payloads, job_id=None):
        written.append(payloads)
        return {"queued": len(payloads)}

    import enrichment.contacts_writer as cw
    monkeypatch.setattr(cw, "write_enrichment_result_batch", fake_batch)

    run(wf.poll_tick("w1"))
    assert wf.get_followup("job-mb")["status"] == wf.STATUS_DONE
    # Main CSV gained the website email on miss1.com rows.
    with open(main, newline="") as fh:
        rows = list(csv.DictReader(fh))
    miss_rows = [r for r in rows if r["domain"] == "miss1.com"]
    assert len(miss_rows) == 2
    assert all(r["company_email"] == "info@miss1.com" for r in miss_rows)
    # Contacts DB write-back carried exactly the curated domain.
    assert len(written) == 1 and len(written[0]) == 1
    payload = written[0][0]
    assert payload["domain"] == "miss1.com"
    assert payload["company_email"] == "info@miss1.com"
    assert payload["source_name"] == "webscrape_followup"
    assert notified == [("job-mb", 2, 2)]


def test_finalize_merge_failure_never_fails_finalize(fresh_db, fake_dash, tmp_path, monkeypatch):
    notified = _notify_recorder(monkeypatch)
    main = _main_csv(tmp_path / "main.csv")
    _insert_parent_job("job-mbf", main)
    miss_csv = _write_results_csv(tmp_path / "miss.csv", [
        {"input_domain": "miss1.com", "dm_email": "", "row_status": "no_contacts"},
    ])
    run(wf.create_followup("job-mbf", miss_csv, origin="auto"))
    batch_job = json.loads(wf.get_followup("job-mbf")["batches_json"])[0]["job_id"]
    fake_dash.job_states[batch_job] = {"total": 1, "processed": 1, "emails_found": 1, "done": True}
    fake_dash.results[batch_job] = [
        {"website": "miss1.com", "email": "info@miss1.com", "found": True, "status": "completed",
         "business_name": "", "confidence": 0.95},
    ]
    import enrichment.contacts_writer as cw

    async def exploding_batch(payloads, job_id=None):
        raise RuntimeError("outbox down")

    monkeypatch.setattr(cw, "write_enrichment_result_batch", exploding_batch)
    run(wf.poll_tick("w1"))
    # Finalize still completed; the merge happened; only write-back degraded.
    row = wf.get_followup("job-mbf")
    assert row["status"] == wf.STATUS_DONE and row["error"] == ""
    with open(main, newline="") as fh:
        rows = list(csv.DictReader(fh))
    assert any(r["company_email"] == "info@miss1.com" for r in rows if r["domain"] == "miss1.com")
    assert notified == [("job-mbf", 1, 1)]
