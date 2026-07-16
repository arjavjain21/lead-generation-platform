"""FastMCP server instance for the ListBuilding documentation oracle.

Phase 1A: scaffold only — instance + single test resource. Resources, Tools,
and Prompts land in Phases 2–4.
"""

from __future__ import annotations

from mcp.server.fastmcp import FastMCP

# stateless_http=True is required for multi-worker gunicorn deployment.
# Each worker has its own in-memory state; stateful mode would require
# sticky sessions (which we don't have). Stateless is the right call for
# a read-only documentation oracle anyway — there's no per-session work.
mcp = FastMCP(
    name="listbuilding-docs-oracle",
    stateless_http=True,
)


@mcp.resource("health://status")
def health_status() -> str:
    """MCP server health check.

    Returns the literal string "ok" if the MCP server is reachable and the
    FastMCP instance is initialized. Used by Phase 1D end-to-end smoke tests
    to verify the mount + auth + lifespan wiring all work.
    """
    return "ok"
