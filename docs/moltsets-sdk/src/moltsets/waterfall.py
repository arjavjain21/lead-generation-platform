from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any

from .client import MoltSetsClient
from .models import MoltSetsResponse


def risk_score_is_sendable(score: str | None, allowed: Sequence[str] = ("A", "B")) -> bool:
    """Conservative helper for outbound. MoltSets documents D as the hard-negative grade."""
    return bool(score and score.upper() in {x.upper() for x in allowed})


def iter_search_people(client: MoltSetsClient, *, page_size: int = 25, max_results: int | None = None, **filters: Any):
    offset = int(filters.pop("offset", 0))
    emitted = 0
    while True:
        limit = min(page_size, 25)
        response = client.search_people(limit=limit, offset=offset, **filters)
        if response.not_found:
            return
        block = response.results or {}
        rows = block.get("results", []) if isinstance(block, dict) else []
        if not rows:
            return
        for row in rows:
            yield row
            emitted += 1
            if max_results is not None and emitted >= max_results:
                return
        total = block.get("total") if isinstance(block, dict) else None
        offset += len(rows)
        if len(rows) < limit or (isinstance(total, int) and offset >= total):
            return


def iter_search_companies(client: MoltSetsClient, *, page_size: int = 25, max_results: int | None = None, **filters: Any):
    offset = int(filters.pop("offset", 0))
    emitted = 0
    while True:
        limit = min(page_size, 25)
        response = client.search_companies(limit=limit, offset=offset, **filters)
        if response.not_found:
            return
        block = response.results or {}
        rows = block.get("results", []) if isinstance(block, dict) else []
        if not rows:
            return
        for row in rows:
            yield row
            emitted += 1
            if max_results is not None and emitted >= max_results:
                return
        total = block.get("total") if isinstance(block, dict) else None
        offset += len(rows)
        if len(rows) < limit or (isinstance(total, int) and offset >= total):
            return


def enrich_best_email(client: MoltSetsClient, linkedin_url: str) -> MoltSetsResponse[Any]:
    """Use MoltSets' highest-coverage email lookup as a single waterfall stage."""
    return client.linkedin_to_best_email(linkedin_url=linkedin_url)
