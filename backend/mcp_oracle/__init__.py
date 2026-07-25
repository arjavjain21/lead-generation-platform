"""ListBuilding MCP documentation oracle.

Read-only MCP server exposing the platform's API documentation, OpenAPI spec,
and discovery tools to AI assistants (Claude Code, Cursor, etc.).

Design principles:
- **Documentation oracle, not API executor**: this server never makes upstream
  provider API calls. It only reads docs/schemas and serves them.
- **Auto-synced**: Resources read from live sources (FastAPI's openapi(),
  filesystem markdown, runtime introspection) — no manual sync required.
- **Stateless**: configured with ``stateless_http=True`` so any gunicorn worker
  can serve any request without session affinity.
- **Authenticated**: every request requires JWT or API key (enforced by
  :class:`shared.mcp_auth.MCPAuthMiddleware`, mounted on the parent app).

Phase 1A scaffold: just the FastMCP instance + a ``health://status`` resource
to verify mounting works end-to-end. Phase 2 will add the real Resources
(openapi, docs, schemas). Phase 3 will add the discovery Tools.
"""

from .server import mcp

__all__ = ["mcp"]
