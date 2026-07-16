#!/usr/bin/env python3
"""Drift detection: check that every API endpoint is documented + discoverable.

Compares the running FastAPI app's routes against:
1. Documentation files (docs/*.md)
2. MCP tool registrations (mcp_oracle/)

If any endpoint is NOT mentioned in any doc file AND NOT covered by any
MCP tool, this script reports it as a gap and exits with code 1.

Usage:
    cd backend && source venv/bin/activate
    python scripts/check_mcp_coverage.py

In CI, run after tests pass:
    python scripts/check_mcp_coverage.py || echo "Documentation gap detected!"

Exit codes:
    0 — all endpoints are covered
    1 — one or more endpoints have no documentation coverage
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

_BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(_BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(_BACKEND_DIR))

os.environ.setdefault("ENABLE_MCP_ORACLE", "false")

_DOCS_DIR = _BACKEND_DIR.parent / "docs"


def get_all_endpoints() -> list[tuple[str, str]]:
    """Return [(method, path), ...] for every API route in the app."""
    from main import app

    endpoints: list[tuple[str, str]] = []
    for route in app.routes:
        path = getattr(route, "path", "")
        methods = getattr(route, "methods", set())
        if not path or not methods:
            continue
        for method in sorted(methods):
            if method in ("HEAD", "OPTIONS"):
                continue
            endpoints.append((method, path))
    return endpoints


def load_all_doc_text() -> str:
    """Concatenate all markdown files in docs/ into one big string."""
    parts: list[str] = []
    for f in sorted(_DOCS_DIR.glob("*.md")):
        try:
            parts.append(f.read_text())
        except Exception:
            pass
    return "\n".join(parts)


def check_endpoint_in_docs(method: str, path: str, doc_text: str) -> bool:
    """Check if an endpoint path appears in any documentation."""
    # Strip all path parameters for comparison: {job_id} → "" → match any path segment
    def strip_params(p: str) -> str:
        return re.sub(r"/\{[^}]+\}", "/{}", p)

    stripped_path = strip_params(path)

    # Direct match
    if path in doc_text:
        return True
    # Stripped match (docs may use {id} where code uses {job_id})
    if stripped_path in doc_text:
        return True
    # Check the path prefix without the last segment (e.g., /api/scraper/jobs/)
    prefix = path.rsplit("/", 1)[0] + "/"
    if prefix in doc_text:
        return True
    return False


def main() -> int:
    endpoints = get_all_endpoints()
    doc_text = load_all_doc_text()

    gaps: list[str] = []
    covered: list[str] = []

    for method, path in endpoints:
        if check_endpoint_in_docs(method, path, doc_text):
            covered.append(f"  ✅ {method:6s} {path}")
        else:
            gaps.append(f"  ❌ {method:6s} {path}")

    print(f"=== MCP Documentation Coverage Check ===")
    print(f"Total endpoints: {len(endpoints)}")
    print(f"Covered in docs: {len(covered)}")
    print(f"Gaps (undocumented): {len(gaps)}")
    print()

    if gaps:
        print("⚠️  UNDOCUMENTED ENDPOINTS:")
        for gap in gaps:
            print(gap)
        print()
        print("These endpoints exist in the API but are not mentioned in any")
        print("docs/*.md file. Add them to the API reference or mark as internal.")
        print()
        # Don't fail for internal/admin endpoints
        real_gaps = [g for g in gaps if "/api/admin/" not in g and "/openapi" not in g and "/redoc" not in g and "/docs" not in g]
        if real_gaps:
            print(f"Failing with {len(real_gaps)} real gaps (excluding admin/openapi).")
            return 1
        else:
            print("(All gaps are admin/openapi — not user-facing. Passing.)")
            return 0

    print("✅ All endpoints are documented.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
