"""Restart-chain metadata for enrichment job listings.

The grouped jobs UI needs to know, for every enrichment job on a page, which
restart chain it belongs to (``chain_root_id``) and how many attempts that
chain has had (``chain_attempts``). Inferring this client-side from page-local
``parent_job_id`` links breaks across pagination — one upload renders as two
cards when the chain spans a page boundary.

``parent_job_id`` is overloaded: enrichment -> enrichment links are RESTART
chain links, while enrichment -> scraper links are Google-Maps chain roots (a
different concept). A non-enrichment parent is therefore a STOP point: the
enrichment child is itself the restart-chain root.

Batch-efficient: a whole page is resolved in rounds of ONE
``SELECT job_id, job_type, parent_job_id ... WHERE job_id IN (...)`` each
(typically 1 round — parents not already among the input rows), never one
query per job.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Cap on ancestor hops — restart chains in production are shallow (2-4
# attempts); 10 is far beyond anything real and bounds both the query rounds
# and the cycle exposure.
_MAX_HOPS = 10

# SQLite's default SQLITE_MAX_VARIABLE_NUMBER is 999; chunk IN-lists well
# under it so a 200-job page never trips the bind limit.
_CHUNK = 400


def _fetch_jobs_by_ids(conn, ids: set[str]) -> dict[str, dict[str, Optional[str]]]:
    """Return job_id -> {job_id, job_type, parent_job_id} for the given ids.

    Reads only the three columns the chain walk needs. Missing ids are simply
    absent from the result (a dangling parent_job_id is treated as a root by
    the caller).
    """
    found: dict[str, dict[str, Optional[str]]] = {}
    wanted = sorted(i for i in ids if i)
    for start in range(0, len(wanted), _CHUNK):
        chunk = wanted[start:start + _CHUNK]
        placeholders = ",".join("?" for _ in chunk)
        rows = conn.execute(
            f"SELECT job_id, job_type, parent_job_id FROM jobs "
            f"WHERE job_id IN ({placeholders})",
            chunk,
        ).fetchall()
        for row in rows:
            keys = row.keys() if hasattr(row, "keys") else []
            found[row["job_id"]] = {
                "job_id": row["job_id"],
                "job_type": row["job_type"] if "job_type" in keys else None,
                "parent_job_id": row["parent_job_id"] if "parent_job_id" in keys else None,
            }
    return found


def chain_roots_for_jobs(job_rows: list[dict], conn) -> dict[str, str]:
    """Map each input job_id to the root of its restart chain.

    Walks ``parent_job_id`` links upward, cycle-safe, capped at ``_MAX_HOPS``
    rounds. A job is its own root when:
      - it has no parent_job_id,
      - its parent row is missing (dangling link),
      - its parent's job_type is not 'enrichment' (scraper chain root —
        restart chains only link enrichment -> enrichment),
      - the hop cap or a cycle is hit (defensive; not expected in prod).

    Args:
        job_rows: rows as returned by ``store.list_jobs`` (``SELECT *`` — so
            they already carry job_type and parent_job_id; only job_id is
            required).
        conn: sqlite connection with a Row factory (``store.conn`` or
            ``db.get_db()``).

    Returns:
        ``{job_id: root_job_id}`` for every input row that has a job_id.
    """
    # known: id -> (job_type, parent_job_id). Seeded from the input rows so
    # parents already on the page cost zero queries; grows as ancestors are
    # fetched (intermediate ancestors must be RESOLVED too, not just fetched,
    # or a 3-hop chain would never fold down to its root).
    known: dict[str, tuple[Optional[str], Optional[str]]] = {}
    input_ids: list[str] = []
    for row in job_rows:
        job_id = row.get("job_id")
        if not job_id:
            continue
        known[job_id] = (
            row.get("job_type"),
            (row.get("parent_job_id") or None),
        )
        input_ids.append(job_id)

    memo: dict[str, str] = {}
    todo: set[str] = set()
    for job_id, (_jt, parent) in known.items():
        if parent:
            todo.add(job_id)
        else:
            memo[job_id] = job_id

    # Ids we have already asked the DB for (input rows arrive pre-known, so
    # only fetched ancestors land here). Distinguishes "parent not fetched
    # yet" (keep waiting) from "parent fetched and absent" (dangling link).
    requested: set[str] = set()

    for _round in range(_MAX_HOPS):
        if not todo:
            break
        # Batch-fetch parents of unresolved nodes not yet known, in one query.
        need = {
            known[jid][1] for jid in todo
            if known[jid][1] and known[jid][1] not in known
            and known[jid][1] not in requested
        }
        if need:
            requested |= need
            for fid, info in _fetch_jobs_by_ids(conn, need).items():
                known[fid] = (info["job_type"], info["parent_job_id"])
                todo.add(fid)  # fetched ancestors must also be resolved

        # Inner fixpoint: cascade newly-memoized roots down their descendants
        # within the round (E3 waits on E2 which waits on E1 — one round must
        # be enough once all three rows are fetched).
        for _sweep in range(_MAX_HOPS):
            still: set[str] = set()
            progress = False
            for jid in todo:
                parent = known[jid][1]
                if parent is None:
                    root = jid  # no parent link
                elif parent not in known:
                    if parent in requested:
                        root = jid  # fetched and absent → dangling link
                    else:
                        still.add(jid)  # queued for the next fetch round
                        continue
                elif known[parent][0] != "enrichment":
                    root = jid  # scraper (or other) parent: this job IS the root
                elif parent in memo:
                    root = memo[parent]
                elif parent == jid:
                    root = jid  # self-cycle
                else:
                    still.add(jid)
                    continue
                memo[jid] = root
                progress = True
            todo = still
            if not todo or not progress:
                break
        if not todo:
            break

    # Hop cap / cycle leftovers degrade to self-rooted (never crash a listing).
    for jid in todo:
        memo[jid] = jid
    return {jid: memo[jid] for jid in input_ids}


def chain_attempt_counts(job_rows: list[dict], conn) -> dict[str, int]:
    """Return root_job_id -> number of enrichment jobs in that restart chain.

    Members are counted across ALL enrichment jobs (not just the current
    page), so an attempt that fell off the page still counts. Two queries:
    one scan of enrichment ids+parents, one lookup for their non-page
    ancestors. Chains whose root is not represented on the page are omitted.

    Returns:
        ``{root_id: member_count}`` for the roots of the given rows only.
    """
    page_ids = [r.get("job_id") for r in job_rows if r.get("job_id")]
    if not page_ids:
        return {}

    rows = conn.execute(
        "SELECT job_id, job_type, parent_job_id FROM jobs WHERE job_type = 'enrichment'"
    ).fetchall()
    all_enrichment = [
        {
            "job_id": r["job_id"],
            "job_type": r["job_type"],
            "parent_job_id": (r["parent_job_id"] if "parent_job_id" in r.keys() else None),
        }
        for r in rows
    ]
    if not all_enrichment:
        return {}

    all_roots = chain_roots_for_jobs(all_enrichment, conn)
    counts: dict[str, int] = {}
    for root_id in all_roots.values():
        counts[root_id] = counts.get(root_id, 0) + 1

    page_roots = {all_roots[j] for j in page_ids if j in all_roots}
    return {root_id: counts.get(root_id, 1) for root_id in page_roots}
