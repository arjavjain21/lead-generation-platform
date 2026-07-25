"""Pytest configuration — disables MCP oracle during tests.

The MCP StreamableHTTPSessionManager.run() can only be called once per
instance (it raises RuntimeError on second entry). Tests that use
``with TestClient(app):`` trigger the lifespan, which enters the MCP
session manager. The second such test would fail.

Setting ENABLE_MCP_ORACLE=false here makes the MCP setup in main.py
skip entirely during tests — zero impact on test coverage since the MCP
server is a documentation oracle (no business logic tests depend on it).
"""

import os

os.environ.setdefault("ENABLE_MCP_ORACLE", "false")
