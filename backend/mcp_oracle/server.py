"""FastMCP server instance for the ListBuilding documentation oracle.

Phase 1A: scaffold only — instance + single test resource. Resources, Tools,
and Prompts land in Phases 2–4.
"""

from __future__ import annotations

from mcp.server.fastmcp import FastMCP

# stateless_http=True is required for multi-worker gunicorn deployment.
mcp = FastMCP(
    name="listbuilding-docs-oracle",
    stateless_http=True,
)

# CRITICAL: set the internal streamable HTTP path to "/" so that when
# mounted at /mcp (via app.mount("/mcp", mcp_app)), the full path is
# /mcp/ — not /mcp/mcp. Without this, POST /mcp/ returns 404 because
# the sub-app's route is at its own /mcp, creating a double prefix.
# See: https://github.com/modelcontextprotocol/python-sdk/issues/951
mcp.settings.streamable_http_path = "/"

# DNS rebinding protection is enabled by default (only allows localhost hosts).
# Since we're behind an Nginx reverse proxy that already validates the Host
# header and TLS certificate, we can safely disable this to accept requests
# forwarded with the production domain (listbuilding.eagleinfoservice.com).
mcp.settings.transport_security.enable_dns_rebinding_protection = False


@mcp.resource("health://status")
def health_status() -> str:
    """MCP server health check.

    Returns the literal string "ok" if the MCP server is reachable and the
    FastMCP instance is initialized. Used by Phase 1D end-to-end smoke tests
    to verify the mount + auth + lifespan wiring all work.
    """
    return "ok"
