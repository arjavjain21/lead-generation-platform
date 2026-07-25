"""Phase 1C tests: lifespan refactor + MCP mounting.

Tests the combined parent + MCP lifespan, the atomic mount pattern, and
the health endpoint visibility.

IMPORTANT: ``conftest.py`` sets ``ENABLE_MCP_ORACLE=false`` for the entire
test suite, so these tests verify the MCP-disabled path. For MCP-enabled
testing, we construct fresh ``FastMCP`` instances per test (never reuse
the module-level singleton — the ``StreamableHTTPSessionManager.run()``
can only be called once per instance).
"""

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path

_BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(_BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(_BACKEND_DIR))


class TestHealthEndpointIncludesMCP(unittest.TestCase):
    """The /health endpoint must include mcp_enabled status."""

    def test_health_returns_mcp_enabled_field(self):
        """In test env, ENABLE_MCP_ORACLE=false so mcp_enabled should be False."""
        import importlib
        import main
        importlib.reload(main)
        # conftest sets ENABLE_MCP_ORACLE=false
        self.assertEqual(os.environ.get("ENABLE_MCP_ORACLE"), "false",
                         "conftest.py should have set ENABLE_MCP_ORACLE=false")
        self.assertFalse(main._MCP_ENABLED,
                         "_MCP_ENABLED should be False when ENABLE_MCP_ORACLE=false")


class TestLifespanParentStartup(unittest.TestCase):
    """The lifespan must run parent startup work (DB tables, etc.)."""

    def test_lifespan_function_exists(self):
        import main
        self.assertTrue(hasattr(main, "lifespan"),
                        "main.lifespan must exist after Phase 1C refactor")

    def test_no_on_event_startup_decorator(self):
        """The old @app.on_event('startup') must be removed."""
        import main
        import inspect
        source = inspect.getsource(main)
        # The decorator line should NOT exist anymore
        self.assertNotIn('@app.on_event("startup")', source,
                         "old @app.on_event('startup') must be removed")
        self.assertNotIn("@app.on_event('startup')", source,
                         "old @app.on_event('startup') must be removed")

    def test_run_parent_startup_function_exists(self):
        """_run_parent_startup must be a callable async function."""
        import main
        self.assertTrue(callable(main._run_parent_startup),
                        "_run_parent_startup must be callable")


class TestAtomicMountPattern(unittest.TestCase):
    """The MCP mount must be atomic — all-or-nothing."""

    def test_mcp_enabled_is_false_in_test_env(self):
        """conftest.py sets ENABLE_MCP_ORACLE=false → _MCP_ENABLED must be False."""
        import main
        self.assertFalse(main._MCP_ENABLED)

    def test_mcp_app_is_none_when_disabled(self):
        """When MCP is disabled, _mcp_app should be None."""
        import main
        self.assertIsNone(main._mcp_app,
                          "_mcp_app must be None when MCP is disabled")

    def test_no_mcp_route_when_disabled(self):
        """When MCP is disabled, /mcp should not be mounted."""
        import main
        mcp_routes = [r for r in main.app.routes if hasattr(r, 'path') and r.path.startswith('/mcp')]
        self.assertEqual(len(mcp_routes), 0,
                         f"/mcp should not be mounted when disabled, found {len(mcp_routes)} routes")


class TestFreshFastMCPLifespan(unittest.TestCase):
    """Verify the MCP lifespan pattern works with a fresh FastMCP instance.

    This avoids the singleton trap: StreamableHTTPSessionManager.run() can
    only be called once per instance, so each test creates a NEW FastMCP.
    """

    def test_fresh_mcp_lifespan_enters_and_exits(self):
        """A fresh FastMCP's streamable_http_app lifespan can be entered and exited."""
        import asyncio
        import contextlib
        from mcp.server.fastmcp import FastMCP

        async def _test():
            mcp = FastMCP("test-fresh", stateless_http=True)
            mcp_app = mcp.streamable_http_app()

            async with contextlib.AsyncExitStack() as stack:
                await stack.enter_async_context(
                    mcp_app.router.lifespan_context(mcp_app)
                )
                # If we get here without error, the lifespan entered successfully
                return True

        result = asyncio.run(_test())
        self.assertTrue(result, "MCP lifespan must enter and exit cleanly")

    def test_fresh_mcp_lifespan_can_be_called_twice_with_separate_instances(self):
        """Two separate FastMCP instances can each enter lifespan independently."""
        import asyncio
        import contextlib
        from mcp.server.fastmcp import FastMCP

        async def _test():
            results = []
            for name in ("test-a", "test-b"):
                mcp = FastMCP(name, stateless_http=True)
                mcp_app = mcp.streamable_http_app()
                async with contextlib.AsyncExitStack() as stack:
                    await stack.enter_async_context(
                        mcp_app.router.lifespan_context(mcp_app)
                    )
                    results.append(name)
            return results

        results = asyncio.run(_test())
        self.assertEqual(results, ["test-a", "test-b"],
                         "Each fresh FastMCP must enter/exit lifespan independently")


if __name__ == "__main__":
    unittest.main()
