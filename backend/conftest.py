"""Pytest configuration — disables MCP oracle and production loops during tests.

Two guards, both load-bearing:

1. ENABLE_MCP_ORACLE=false — the MCP StreamableHTTPSessionManager.run() can
   only be called once per instance (it raises RuntimeError on second entry).
   Tests that use ``with TestClient(app):`` trigger the lifespan, which
   enters the MCP session manager. The second such test would fail. No test
   coverage is lost: the MCP server is a documentation oracle.

2. ENABLE_STARTUP_REAPERS=false — the app's boot sequence runs stale-job
   reapers, auto-resume, the scraper dispatcher and the outbox loop against
   the REAL jobs DB (tests share the prod DB path). On 2026-08-24 a pytest
   boot reaped 6 healthy scraper jobs (marked abandoned at 09:37:12) with
   zero trace in the service logs. Tests must NEVER mutate production job
   state. (main.py defaults this flag to true; only pytest sets it false.)

3. ENABLE_BLITZ_MISS_SKIP=false — the Blitz miss-marker store
   (blitz_miss_store) lives in the REAL jobs DB. The 2026-09 wiring arms it
   inside the production runners (list_builder.run_domain_enrichment,
   pipeline.run_pipeline), so ANY test driving those runners with stubbed
   providers would write bogus 30-day negative-cache markers for test
   domains (acme.com etc.) into production. Only pytest sets it false;
   the wiring itself defaults true. Tests that need the armed path set the
   env explicitly (see test_list_builder_experiences_prepass_phone /
   test_pipeline_prepass_miss_phone).
"""

import os

os.environ.setdefault("ENABLE_MCP_ORACLE", "false")
os.environ.setdefault("ENABLE_STARTUP_REAPERS", "false")
os.environ.setdefault("ENABLE_BLITZ_MISS_SKIP", "false")
os.environ.setdefault("LEADOCEAN_GUARDS_ARMED", "false")
os.environ.setdefault("ENABLE_AUDIT_LOG", "false")

# 4. Env-leak guard (2026-10-15): several modules run ``load_dotenv()`` at
#    IMPORT time — ``main`` (endpoint tests) and the root-level
#    test_wizleads_integration / test_better_enrich_v3 files do it at
#    COLLECTION time, before any test executes. Every backend/.env value
#    (including runtime kill switches like ENABLE_BETTER_ENRICH=false) then
#    leaked into the process env and silently flipped cascade expectations
#    for every later test (~40 order/gate tests failed only in full-suite
#    runs; each passed in isolation). This conftest imports BEFORE test
#    modules are collected, so snapshot the clean baseline here and restore
#    it around EVERY test — no leak, at collection time or runtime, can
#    reach a test's neighbours. Tests set env via monkeypatch (restored) or
#    intentionally inside their own body (always within one test).
import pytest

# Baseline: what the app itself would see (main.py runs load_dotenv() at
# import) — tests legitimately rely on .env tokens (JWT_SECRET,
# CONTACTS_API_TOKEN, provider keys). But .env also carries RUNTIME
# kill-switch state (e.g. ENABLE_BETTER_ENRICH=false while BetterEnrich
# credits are held off) that must NOT flip test expectations — the code
# defaults rule in tests. Scrub exactly those switches from the baseline.
from dotenv import load_dotenv as _load_dotenv

_load_dotenv()
for _k in [k for k in os.environ if k.startswith("ENABLE_")]:
    del os.environ[_k]
# External provider API keys stay OUT of the test baseline: with them, any
# test gap in mocking turns into REAL paid provider calls (2026-10-15: a
# Flow-3 batch test got a real hit — saurabh.kumar@hcl.com — for a
# synthetic linkedin.com/in/u60... row). Clients fail fast without keys
# (the historical pre-leak behaviour); tests that exercise a client set
# their own key via monkeypatch (established pattern: blitz/moltsets/
# quickenrich suites). CONTACTS_API_TOKEN stays: several suites need it
# PRESENT for the client's config gate while mocking its transport.
for _k in (
    "GETLEADS_API_KEY", "BLITZ_API_KEY", "SMARTPROSPECT_API_KEY",
    "QUICKENRICH_API_KEY", "MOLTSETS_API_KEY", "WIZLEADS_API_KEY",
    "BETTER_ENRICH_API_KEY", "LEADOCEAN_API_KEY",
):
    os.environ.pop(_k, None)
# Re-apply pytest's own guards AFTER the scrub (they are intentionally
# test-only and must win over any .env value).
os.environ.setdefault("ENABLE_MCP_ORACLE", "false")
os.environ.setdefault("ENABLE_STARTUP_REAPERS", "false")
os.environ.setdefault("ENABLE_BLITZ_MISS_SKIP", "false")
os.environ.setdefault("LEADOCEAN_GUARDS_ARMED", "false")
# 5. ENABLE_AUDIT_LOG=false — the audit middleware is registered
#    unconditionally in main.py, but self-gates per request on
#    audit.AUDIT_ENABLED (read from this env at shared.audit import).
#    Without this guard, every TestClient POST in every existing suite
#    would flow through the middleware and write audit rows into the REAL
#    jobs.db user_activity_log (tests share the prod DB path). Audit tests
#    flip the module attribute back on with a monkeypatched temp DB.
os.environ.setdefault("ENABLE_AUDIT_LOG", "false")

# 6. Call-tracker backfill no-op (2026-11-27): every TestClient lifespan
#    starts _call_tracker_purge_loop → backfill_call_log_daily, which full-
#    scans the 21M-row provider_call_log (SELECT DISTINCT day) plus one
#    transaction per day — minutes of event-loop-blocking SQLite PER TEST
#    lifespan on this box (observed 2026-11-27: single tests hanging >90s in
#    fixture setup; whole suites freezing). It is telemetry catch-up for the
#    daily rollup; NO test asserts on it (grep-verified). Patched to a no-op
#    for the whole pytest process — production startup is untouched.
import sys as _sys
if os.path.abspath(os.path.dirname(__file__)) not in _sys.path:
    _sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
try:
    from enrichment import call_tracker as _call_tracker
    _call_tracker.backfill_call_log_daily = lambda: {"days": 0, "rows": 0}
except Exception:  # pragma: no cover — import failures shouldn't kill collection
    pass

_SESSION_ENV_START = dict(os.environ)


@pytest.fixture(autouse=True)
def _env_snapshot_restore():
    try:
        yield
    finally:
        # Delta-restore (never clear() — pytest manages PYTEST_* env keys
        # around each test and a wholesale clear() crashes its reporting).
        for key in list(os.environ):
            if key not in _SESSION_ENV_START and not key.startswith("PYTEST"):
                del os.environ[key]
        for key, value in _SESSION_ENV_START.items():
            if os.environ.get(key) != value:
                os.environ[key] = value
