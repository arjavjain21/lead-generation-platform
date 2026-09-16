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
