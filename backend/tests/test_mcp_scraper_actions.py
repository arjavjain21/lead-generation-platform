"""
Tests for mcp_oracle/scraper_actions.py — the 5 MCP ACTION tools.

Covers:
- Registration via register_tools() on a fresh FastMCP + the
  ENABLE_MCP_SCRAPER_TOOLS kill-switch semantics.
- Event-loop safety: every tool is a coroutine function (FastMCP 1.28.1 runs
  SYNC tools directly on the event loop — a sync tool doing SQLite work would
  stall the worker; regression guard).
- Identity: _user_from_ctx reads scope["user"] off ctx.request_context.request;
  missing identity → 401 ExternalError.
- Tool behavior: dry_run default creates nothing; dry_run=false creates with
  [MCP] display name; ownership enforced; not_ready flows through.
- Identity END-TO-END over real streamable HTTP: middleware sets
  scope["user"], the REAL get_scrape_job_status tool (which calls
  _user_from_ctx(ctx)) resolves that identity and answers for that user.
"""
from __future__ import annotations

import asyncio
import contextlib
import inspect
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from unittest import mock

import pytest

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from mcp_oracle import scraper_actions  # noqa: E402
from scraper import external_helpers as ext  # noqa: E402
from shared import db  # noqa: E402

OWNER = "mcp-actions-owner"

TOOL_NAMES = {
    "scrape_local_businesses",
    "get_scrape_job_status",
    "get_scrape_job_results",
    "check_scrape_cache",
    "cancel_scrape_job",
}


def _make_user(user_id=OWNER, is_admin=False):
    return {"user_id": user_id, "email": f"{user_id}@test.example", "is_admin": is_admin}


def _mk_user_row(conn, user_id, is_admin=0):
    conn.execute(
        "INSERT OR REPLACE INTO users (user_id, email, password_hash, is_admin, created_at) "
        "VALUES (?, ?, 'x', ?, ?)",
        (user_id, f"{user_id}@test.example", is_admin, datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()


def _mk_job(conn, job_id, user_id, status="running", total_tasks=9, done_tasks=3):
    iso = datetime.now(timezone.utc).isoformat()
    regions = '{"mode":"cities","country":"us","cities":["Austin"],"states":[],"zips":[],"center_ids":[]}'
    conn.execute(
        "INSERT INTO jobs (job_id, user_id, job_type, status, query, regions, total_tasks, "
        "done_tasks, result_count, created_at, updated_at) "
        "VALUES (?, ?, 'scraper', ?, 'coffee shop', ?, ?, ?, 0, ?, ?)",
        (job_id, user_id, status, regions, total_tasks, done_tasks, iso, iso),
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


class _FakeRequest:
    def __init__(self, user):
        self.scope = {"user": user}


class _FakeRequestContext:
    """Stand-in for mcp's RequestContext (has .request)."""

    def __init__(self, user):
        self.request = _FakeRequest(user)


class _FakeContext:
    """Stand-in for mcp.server.fastmcp.Context — what the tools' ctx param
    receives. Nesting mirrors the SDK: ctx.request_context.request.scope."""

    def __init__(self, user):
        self.request_context = _FakeRequestContext(user)


def _ctx(user):
    return _FakeContext(user)


def _list_tools(mcp_instance):
    """list_tools() is async in mcp 1.28.1 — run on a fresh loop."""
    return asyncio.run(mcp_instance.list_tools())


# ---------------------------------------------------------------------------
# 1. Registration + kill-switch
# ---------------------------------------------------------------------------

class TestRegistration:
    def test_five_tools_registered_on_fresh_instance(self):
        from mcp.server.fastmcp import FastMCP
        m = FastMCP("reg-test", stateless_http=True)
        scraper_actions.register_tools(m)
        names = {t.name for t in _list_tools(m)}
        assert TOOL_NAMES <= names

    def test_enabled_false_registers_nothing(self):
        from mcp.server.fastmcp import FastMCP
        m = FastMCP("kill-test", stateless_http=True)
        scraper_actions.register_tools(m, enabled=False)
        names = {t.name for t in _list_tools(m)}
        assert not (TOOL_NAMES & names)

    def test_real_server_registers_action_tools(self):
        """The gated import in server.py registers on the production singleton.
        Reload BOTH modules in dependency order — scraper_actions caches the
        server's mcp instance at import time, so reloading server alone leaves
        the actions module pointing at the previous singleton."""
        import importlib
        import mcp_oracle.server as server_mod
        import mcp_oracle.scraper_actions as actions_mod
        with mock.patch.dict(os.environ, {"ENABLE_MCP_SCRAPER_TOOLS": "true"}):
            server = importlib.reload(server_mod)
            importlib.reload(actions_mod)
            names = {t.name for t in _list_tools(server.mcp)}
        assert TOOL_NAMES <= names

    def test_kill_switch_off_on_real_server(self):
        import importlib
        import mcp_oracle.server as server_mod
        import mcp_oracle.scraper_actions as actions_mod
        with mock.patch.dict(os.environ, {"ENABLE_MCP_SCRAPER_TOOLS": "false"}):
            server = importlib.reload(server_mod)
            importlib.reload(actions_mod)
            names = {t.name for t in _list_tools(server.mcp)}
        assert not (TOOL_NAMES & names)


# ---------------------------------------------------------------------------
# 2. Event-loop safety
# ---------------------------------------------------------------------------

class TestAsyncSafety:
    def test_all_tools_are_coroutine_functions(self):
        fns = [
            scraper_actions.scrape_local_businesses,
            scraper_actions.get_scrape_job_status,
            scraper_actions.get_scrape_job_results,
            scraper_actions.check_scrape_cache,
            scraper_actions.cancel_scrape_job,
        ]
        for fn in fns:
            assert inspect.iscoroutinefunction(fn), f"{fn.__name__} must be async def"

    def test_run_offloads_sync_fn(self):
        def blocking(x, y=0):
            return x + y
        assert asyncio.run(scraper_actions._run(blocking, 1, y=2)) == 3


# ---------------------------------------------------------------------------
# 3. Identity resolution
# ---------------------------------------------------------------------------

class TestUserFromCtx:
    def test_reads_scope_user(self):
        user = _make_user()
        assert scraper_actions._user_from_ctx(_FakeContext(user)) == user

    def test_missing_user_raises_401(self):
        ctx = _FakeContext(None)
        ctx.request_context.request.scope = {}
        with pytest.raises(ext.ExternalError) as ei:
            scraper_actions._user_from_ctx(ctx)
        assert ei.value.code == "unauthenticated"
        assert ei.value.status_code == 401

    def test_none_ctx_raises_401(self):
        with pytest.raises(ext.ExternalError) as ei:
            scraper_actions._user_from_ctx(None)
        assert ei.value.code == "unauthenticated"


# ---------------------------------------------------------------------------
# 4. Tool behavior (impl layer through the async tool functions)
# ---------------------------------------------------------------------------

class TestToolBehavior:
    def test_dry_run_default_creates_nothing(self):
        conn = db.get_db()
        _mk_user_row(conn, OWNER)
        try:
            with mock.patch.dict(os.environ, {"SCRAPER_TECH_KEY": "k"}):
                out = asyncio.run(scraper_actions.scrape_local_businesses(
                    query="coffee shop", mode="cities", country="us",
                    cities=["Austin"], dry_run=True,
                    ctx=_FakeContext(_make_user()),
                ))
            payload = json.loads(out)
            assert "job_id" not in payload
            assert payload["task_count"] == payload["center_count"] * 3
        finally:
            _cleanup(conn, user_ids=(OWNER,))

    def test_dry_run_false_creates_with_mcp_tag(self):
        conn = db.get_db()
        _mk_user_row(conn, OWNER)
        job_id = None
        try:
            with mock.patch.dict(os.environ, {"SCRAPER_TECH_KEY": "k"}):
                out = asyncio.run(scraper_actions.scrape_local_businesses(
                    query="coffee shop", mode="cities", country="us",
                    cities=["Austin"], dry_run=False, prefer_cache=False,
                    ctx=_FakeContext(_make_user()),
                ))
            payload = json.loads(out)
            assert payload["created"] is True
            job_id = payload["job_id"]
            assert payload["display_name"].startswith("[MCP] ")
        finally:
            if job_id:
                _cleanup(conn, job_id, user_ids=(OWNER,))
            else:
                _cleanup(conn, user_ids=(OWNER,))

    def test_status_tool_returns_projection(self):
        conn = db.get_db()
        _mk_user_row(conn, OWNER)
        jid = str(uuid.uuid4())
        _mk_job(conn, jid, OWNER, status="running")
        try:
            out = asyncio.run(scraper_actions.get_scrape_job_status(
                job_id=jid, ctx=_FakeContext(_make_user())))
            payload = json.loads(out)
            assert payload["job_id"] == jid
            assert "progress" in payload and "user_id" not in payload
        finally:
            _cleanup(conn, jid, user_ids=(OWNER,))

    def test_results_tool_not_ready_error(self):
        conn = db.get_db()
        _mk_user_row(conn, OWNER)
        jid = str(uuid.uuid4())
        _mk_job(conn, jid, OWNER, status="running")
        try:
            with pytest.raises(ext.ExternalError) as ei:
                asyncio.run(scraper_actions.get_scrape_job_results(
                    job_id=jid, limit=999, ctx=_FakeContext(_make_user())))
            assert ei.value.code == "not_ready"
            assert ei.value.retry_after == 10
        finally:
            _cleanup(conn, jid, user_ids=(OWNER,))

    def test_cancel_tool_requires_ownership(self):
        conn = db.get_db()
        _mk_user_row(conn, OWNER)
        _mk_user_row(conn, "mcp-other")
        jid = str(uuid.uuid4())
        _mk_job(conn, jid, OWNER, status="running")
        try:
            with pytest.raises(ext.ExternalError) as ei:
                asyncio.run(scraper_actions.cancel_scrape_job(
                    job_id=jid, ctx=_FakeContext(_make_user("mcp-other"))))
            assert ei.value.code == "access_denied"
        finally:
            _cleanup(conn, jid, user_ids=(OWNER, "mcp-other"))


# ---------------------------------------------------------------------------
# 5. Identity END-TO-END over real streamable HTTP
# ---------------------------------------------------------------------------

class TestIdentityEndToEnd:
    def test_real_tool_resolves_identity_over_streamable_http(self):
        """Full production path: middleware sets scope['user'] → SDK threads
        the Starlette Request into ctx.request_context.request → the REAL
        tool resolves identity and answers for that user's job."""
        import httpx
        from starlette.middleware.base import BaseHTTPMiddleware
        from starlette.requests import Request
        from mcp.server.fastmcp import FastMCP

        conn = db.get_db()
        _mk_user_row(conn, OWNER)
        jid = str(uuid.uuid4())
        _mk_job(conn, jid, OWNER, status="running")

        async def _scenario() -> tuple[int, str]:
            mcp = FastMCP("identity-e2e", stateless_http=True)
            mcp.settings.streamable_http_path = "/"
            mcp.settings.transport_security.enable_dns_rebinding_protection = False
            scraper_actions.register_tools(mcp)
            mcp_app = mcp.streamable_http_app()

            class _Auth(BaseHTTPMiddleware):
                async def dispatch(self, request: Request, call_next):
                    request.scope["user"] = _make_user(OWNER)
                    return await call_next(request)

            mcp_app.add_middleware(_Auth)

            async with contextlib.AsyncExitStack() as stack:
                await stack.enter_async_context(mcp_app.router.lifespan_context(mcp_app))
                transport = httpx.ASGITransport(app=mcp_app)
                async with httpx.AsyncClient(
                    transport=transport, base_url="http://t",
                    headers={"Accept": "application/json, text/event-stream"},
                ) as client:
                    r = await client.post("/", json={
                        "jsonrpc": "2.0", "id": 1, "method": "initialize",
                        "params": {
                            "protocolVersion": "2025-03-26", "capabilities": {},
                            "clientInfo": {"name": "t", "version": "0"},
                        },
                    })
                    assert r.status_code == 200, f"initialize failed: {r.text}"
                    r2 = await client.post("/", json={
                        "jsonrpc": "2.0", "id": 2, "method": "tools/call",
                        "params": {
                            "name": "get_scrape_job_status",
                            "arguments": {"job_id": jid},
                        },
                    })
                    return r2.status_code, r2.text

        try:
            status, text = asyncio.run(_scenario())
            assert status == 200, f"tools/call failed: {text}"
            # The response is SSE-framed (event: message\r\ndata: {...}) —
            # extract the JSON payload from the data frame.
            payload_text = text
            for line in text.splitlines():
                if line.startswith("data: "):
                    payload_text = line[len("data: "):]
                    break
            payload = json.loads(payload_text)
            result = payload["result"]["content"][0]["text"]
            inner = json.loads(result)
            # The tool answered for OUR job → identity resolved through the
            # middleware-set scope user (else the tool would have raised the
            # 401 unauthenticated error instead of returning a payload).
            assert inner["job_id"] == jid
            assert "progress" in inner and "user_id" not in inner
        finally:
            _cleanup(conn, jid, user_ids=(OWNER,))
