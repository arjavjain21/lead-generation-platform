#!/usr/bin/env python3
"""Compare the live MoltSets docs index against this SDK's endpoint manifest.

Run in an environment with internet access:
    python scripts/audit_docs_coverage.py

Exit code 1 means MoltSets documented a current API endpoint that is not present
in the local SDK snapshot, or the snapshot contains a current endpoint no longer
listed by the live docs index.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from urllib.request import Request, urlopen

DOCS_INDEX = "https://developer.moltsets.com/llms.txt"
ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "moltsets_endpoint_manifest.json"


def main() -> int:
    request = Request(DOCS_INDEX, headers={"User-Agent": "moltsets-sdk-coverage-audit/1.0"})
    with urlopen(request, timeout=30) as response:
        text = response.read().decode("utf-8")

    live = set(re.findall(r"\[POST\s+/([a-zA-Z0-9_]+)\]", text))
    manifest = json.loads(MANIFEST.read_text())
    local = {item["name"] for item in manifest["endpoints"]}

    missing = sorted(live - local)
    removed = sorted(local - live)

    print(f"Live documented endpoints: {len(live)}")
    print(f"Local SDK endpoints:        {len(local)}")

    if missing:
        print("\nNEW DOC ENDPOINTS MISSING FROM SDK:")
        for name in missing:
            print(f"  + {name}")

    if removed:
        print("\nLOCAL ENDPOINTS NO LONGER IN CURRENT DOC INDEX:")
        for name in removed:
            print(f"  - {name}")

    if missing or removed:
        return 1

    print("\nCoverage matches the live MoltSets API-reference index.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
