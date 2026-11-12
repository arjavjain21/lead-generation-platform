"""Job runner for GetLeads "Instant US Database" pulls (scraper-page source).

A places job is a scraper job (job_type='scraper', source_type='getleads_places')
that never touches the scraper.tech crawler:

- Created DIRECTLY in status='running' (never 'queued') so the scraper
  dispatcher — which atomically claims queued jobs and would try to grid-scrape
  them — can never pick it up. The request handler spawns the driver task,
  exactly like enrichment jobs do.
- Result CACHE (phase 2): identical filters within 90 days are served from a
  copy on disk — zero budget, instant. Data refreshes quarterly upstream, so a
  90-day TTL is conservative.
- PARTITIONING (phase 2): pulls larger than PARTITION_TARGET rows are split
  into disjoint ZIP groups via free-count recursive bisection (each GetLeads
  export caps at 25K rows). Each partition's rows land in a durable
  {job_id}.part{i}.csv BEFORE the next starts, so a restart resumes from the
  first unfinished partition without re-billing completed ones.
- Cities are expanded to ZIPs through our us_zips.csv (GetLeads stored-city
  values are unreliable neighborhood strings).
- If the account budget runs out MID-RUN, completed partitions are finalized
  as a clearly-labeled PARTIAL result instead of failing the whole job.
- Output CSV carries a literal `website` column, so the existing
  scraper→enrichment chain endpoint works unchanged.
- Cancel-aware: the standard scraper cancel endpoint flips the DB status; the
  poll loop notices within one tick and stops (remote exports can't be
  cancelled — their rows still bill).
"""

from __future__ import annotations

import asyncio
import csv
import hashlib
import json
import logging
import os
import shutil
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

from shared import db
from scraper import centers as centers_module
from scraper import job_store, places_client
from scraper.places_client import PlacesError

logger = logging.getLogger(__name__)

SOURCE_TYPE = "getleads_places"

# OUTPUT_DIR is owned by scraper/routes.py (single source of truth for where
# job CSVs live; the download endpoints fall back to OUTPUT_DIR/{job_id}.csv).
from scraper.routes import OUTPUT_DIR  # noqa: E402

# Durable cache copies live under the shared cache mount (257 GB free).
CACHE_DIR = Path("/mnt/disk/lead-generation-platform/cache") / "places"
CACHE_TTL_DAYS = 90

# Partitioning: GetLeads caps one export at MAX_EXPORT_ROWS (25K). We target
# below it so a chunk never trips the cap, and bound the number of exports.
PARTITION_TARGET = int(os.getenv("GETLEADS_PLACES_PARTITION_TARGET", "20000"))
MAX_PARTITIONS = 64

# Normalized output columns. `website` is REQUIRED — the chain-to-enrichment
# endpoint (main.py chain_to_enrichment) gates on a literal "website" column.
CSV_COLUMNS = [
    "name", "category", "phone", "website",
    "email", "email_type", "additional_emails",
    "street_address", "city", "state", "zip", "full_address",
    "rating", "review_count", "google_maps_url",
    "facebook", "instagram", "linkedin",
]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Result cache (phase 2)
# ---------------------------------------------------------------------------


def _ensure_places_cache_table(conn: Any) -> None:
    conn.execute(
        """CREATE TABLE IF NOT EXISTS places_cache (
               cache_key       TEXT PRIMARY KEY,
               filters         TEXT NOT NULL,
               csv_path        TEXT NOT NULL,
               row_count       INTEGER NOT NULL,
               created_at      TEXT NOT NULL,
               expires_at      TEXT NOT NULL,
               last_accessed_at TEXT,
               access_count    INTEGER DEFAULT 0
           )"""
    )
    conn.commit()


def places_cache_key(filters: dict[str, Any]) -> str:
    """Stable key over the sanitized filter dict (sorted JSON → sha256[:16])."""
    canonical = json.dumps(filters, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


def places_cache_lookup(filters: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Unexpired hit whose CSV still exists → {csv_path, row_count, cache_key}.
    Updates access stats on hit; None otherwise."""
    key = places_cache_key(filters)
    conn = db.get_db()
    _ensure_places_cache_table(conn)
    row = conn.execute(
        """SELECT * FROM places_cache
           WHERE cache_key=? AND expires_at > ?""",
        (key, _now()),
    ).fetchone()
    if not row:
        return None
    entry = dict(row)
    if not entry.get("csv_path") or not Path(entry["csv_path"]).exists():
        return None  # stale entry — backing file gone
    conn.execute(
        "UPDATE places_cache SET last_accessed_at=?, access_count=access_count+1 WHERE cache_key=?",
        (_now(), key),
    )
    conn.commit()
    return entry


def places_cache_store(filters: dict[str, Any], final_csv: Path, row_count: int) -> str:
    """Copy the final CSV into the cache and upsert the entry."""
    key = places_cache_key(filters)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    dest = CACHE_DIR / f"{key}.csv"
    shutil.copyfile(final_csv, dest)
    now = datetime.now(timezone.utc)
    conn = db.get_db()
    _ensure_places_cache_table(conn)
    conn.execute(
        """INSERT OR REPLACE INTO places_cache
           (cache_key, filters, csv_path, row_count, created_at, expires_at,
            last_accessed_at, access_count)
           VALUES (?, ?, ?, ?, ?, ?, ?, 1)""",
        (key, json.dumps(filters, sort_keys=True), str(dest), row_count,
         now.isoformat(), (now + timedelta(days=CACHE_TTL_DAYS)).isoformat(),
         now.isoformat()),
    )
    conn.commit()
    logger.info("places cache stored: %s (%d rows)", key, row_count)
    return key


# ---------------------------------------------------------------------------
# Filter resolution (cities → ZIPs) + partition planning
# ---------------------------------------------------------------------------


def resolve_filters(filters: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    """Expand `cities` into `zips` via our us_zips.csv — GetLeads city values
    are raw listing strings ("Floor Austin"), while ZIPs match exactly.

    Raises ValueError when a city can't be resolved. Returns (filters, notes).
    """
    resolved = dict(filters)
    cities = resolved.pop("cities", None)
    notes: list[str] = []
    if not cities:
        return resolved, notes

    city_map, errors = centers_module.get_zips_for_cities([str(c) for c in cities])
    if errors and not city_map:
        raise ValueError("Could not resolve cities: " + "; ".join(errors[:3]))
    if errors:
        notes.append("Skipped unresolved cities: " + "; ".join(errors[:3]))

    merged = set(resolved.get("zips") or [])
    for zips in city_map.values():
        merged.update(zips)
    if not merged:
        raise ValueError(f"No ZIPs found for cities: {', '.join(str(c) for c in cities)}")
    resolved["zips"] = sorted(merged)
    notes.append(f"Cities expanded to {len(merged)} ZIPs (exact ZIP matching).")
    return resolved, notes


async def plan_partitions(
    filters: dict[str, Any],
    total: int,
    *,
    count_fn: Callable[[dict[str, Any]], Awaitable[int]] | None = None,
) -> list[dict[str, Any]]:
    """Split a pull larger than PARTITION_TARGET into disjoint ZIP-filtered
    partitions, each expected ≤ target rows, using FREE counts.

    Geo axis: explicit zips → else states expanded to ZIPs → else every state.
    Recursive bisection over the ZIP list; counts are free and memoized by
    the caller's count_fn only (each chunk is counted exactly once).

    Raises PlacesError when partitioning is impossible or would need more
    than MAX_PARTITIONS exports.
    """
    if count_fn is None:
        count_fn = places_client.count_places
    if total <= PARTITION_TARGET:
        return [dict(filters)]

    geo = [str(z) for z in (filters.get("zips") or [])]
    base = {k: v for k, v in filters.items() if k != "zips"}
    if not geo:
        states = filters.get("states")
        geo, errors = centers_module.get_zips_for_states(states)
        if not geo:
            raise PlacesError(
                "plan_partitions", 422,
                "Cannot partition this pull: no ZIP base available. "
                f"({'; '.join(errors) if errors else 'empty ZIP database'})",
            )
        base.pop("states", None)  # partitions are ZIP-keyed from here on

    async def bisect(zips: list[str], depth: int = 0) -> list[dict[str, Any]]:
        if len(zips) == 1:
            count = await count_fn({**base, "zips": zips})
            if count > places_client.MAX_EXPORT_ROWS:
                raise PlacesError(
                    "plan_partitions", 422,
                    f"A single ZIP ({zips[0]}) matches {count:,} businesses — "
                    "over the 25,000-per-export cap. Narrow the business filters.",
                )
            return [{**base, "zips": zips}]
        half = max(1, len(zips) // 2)
        out: list[dict[str, Any]] = []
        for chunk in (zips[:half], zips[half:]):
            count = await count_fn({**base, "zips": chunk})
            if count <= PARTITION_TARGET:
                out.append({**base, "zips": chunk})
            else:
                out.extend(await bisect(chunk, depth + 1))
        return out

    partitions = await bisect(geo)
    if len(partitions) > MAX_PARTITIONS:
        raise PlacesError(
            "plan_partitions", 422,
            f"This pull needs {len(partitions)} separate exports (max {MAX_PARTITIONS}). "
            "Narrow the filters — fewer states, tighter category.",
        )
    return partitions


# ---------------------------------------------------------------------------
# Job creation
# ---------------------------------------------------------------------------


def _insert_places_job(
    job_id: str,
    user_id: str,
    query_label: str,
    display_name: str,
    regions_payload: dict[str, Any],
    group_name: Optional[str],
) -> None:
    """Insert a places job row directly in status='running'.

    Deliberately bypasses JobStoreBase.create_job, which hard-codes
    status='queued' — a queued places row would be claimed by the scraper
    dispatcher and run as a scraper.tech grid job. Inserting as 'running'
    closes that race by construction. is_resumable=0 keeps the UI from
    offering scraper Resume (places jobs resume themselves on boot).
    """
    store = job_store.get_store()
    now = _now()
    store.conn.execute(
        """INSERT INTO jobs (
               job_id, user_id, job_type, status, query, regions,
               total_tasks, done_tasks, result_count,
               source_type, is_resumable, group_name, display_name,
               created_at, updated_at, last_heartbeat
           ) VALUES (?, ?, 'scraper', 'running', ?, ?, ?, 0, 0, ?, 0, ?, ?, ?, ?, ?)""",
        (
            job_id, user_id, query_label,
            json.dumps(regions_payload),
            3, SOURCE_TYPE,
            group_name, display_name,
            now, now, now,
        ),
    )
    store.conn.commit()


def create_and_launch(
    user: dict[str, Any],
    filters: dict[str, Any],
    group: Optional[str] = None,
) -> dict[str, Any]:
    """Resolve (cities→ZIPs) + validate + insert + spawn the driver.

    Raises PlacesError (kill switch / key missing) or ValueError (filterless,
    unresolvable cities, quota).
    """
    if not places_client.is_enabled():
        raise PlacesError("create", 503, "Instant US Database is disabled (ENABLE_GETLEADS_PLACES=false)")
    if not places_client.is_configured():
        raise PlacesError("create", 503, "GETLEADS_API_KEY is not configured")

    filters, _notes = resolve_filters(filters)
    filters = places_client.sanitize_filters(filters)  # ValueError on filterless

    allowed, limit_message = db.check_daily_request_limit(
        user_id=user["user_id"],
        is_admin=user.get("is_admin", False),
        estimated_requests=1,
    )
    if not allowed:
        raise ValueError(limit_message)

    label = places_client.describe_filters(filters)
    job_id = str(uuid.uuid4())
    regions_payload = {
        "mode": "places",
        "country": "us",
        "filters": filters,
        "places": {"partitions": None, "done_parts": [], "export_id": None, "expected": None},
    }
    _insert_places_job(
        job_id=job_id,
        user_id=user["user_id"],
        query_label=label,
        display_name=f"{label} · Instant DB",
        regions_payload=regions_payload,
        group_name=group,
    )
    asyncio.create_task(run_places_job(job_id))
    logger.info("Places job %s created by user %s (%s)", job_id, user.get("user_id"), label)
    return {"job_id": job_id, "status": "running", "total_tasks": 3}


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


class _Emitter:
    """Sequenced append_event + heartbeat + SSE wake for one job."""

    def __init__(self, job_id: str):
        self.job_id = job_id
        self.seq = job_store.get_store().count_events(job_id)

    def emit(self, event: dict[str, Any]) -> None:
        store = job_store.get_store()
        store.append_event(self.job_id, self.seq, event)
        self.seq += 1
        store.heartbeat(self.job_id)
        self._wake()

    def _wake(self) -> None:
        # Best-effort: the SSE stream also polls every 2s, so a missed wake
        # only costs latency, never correctness.
        try:
            from scraper.routes import _job_signals

            sig = _job_signals.get(self.job_id)
            if sig:
                sig.set()
        except Exception:
            pass


def _fail(job_id: str, message: str) -> None:
    logger.warning("Places job %s failed: %s", job_id, message)
    job_store.get_store().set_failed(job_id, message)


def _save_places_state(job_id: str, **state: Any) -> None:
    """Merge keys into the job's regions blob 'places' sub-dict (survives
    restarts — the rearm path reads partitions/done_parts/export_id here)."""
    store = job_store.get_store()
    job = store.get_job(job_id)
    if not job:
        return
    try:
        regions = json.loads(job["regions"]) if job.get("regions") else {}
    except (ValueError, TypeError):
        regions = {}
    places_state = regions.get("places") or {}
    places_state.update(state)
    regions["places"] = places_state
    store.conn.execute(
        "UPDATE jobs SET regions=?, updated_at=? WHERE job_id=?",
        (json.dumps(regions), _now(), job_id),
    )
    store.conn.commit()


def _normalize_row(raw: dict[str, str]) -> dict[str, str]:
    """GetLeads CSV row → our canonical columns (missing fields → '')."""
    def g(key: str) -> str:
        return (raw.get(key) or "").strip()

    extra_emails = "; ".join(
        e for e in (g(f"email_{i}") for i in range(2, 9)) if e
    )
    street, city, state, zip_ = g("street_address"), g("city"), g("state"), g("zip")
    full_address = ", ".join(p for p in (street, city, state, zip_) if p)
    return {
        "name": g("business_name"),
        "category": g("category"),
        "phone": g("phone"),
        "website": g("website"),
        "email": g("primary_email"),
        "email_type": g("email_type"),
        "additional_emails": extra_emails,
        "street_address": street,
        "city": city,
        "state": state,
        "zip": zip_,
        "full_address": full_address,
        "rating": g("rating"),
        "review_count": g("review_count"),
        "google_maps_url": g("google_maps_url"),
        "facebook": g("facebook"),
        "instagram": g("instagram"),
        "linkedin": g("linkedin"),
    }


def _write_part_csv(job_id: str, index: int, rows: list[dict[str, str]]) -> Path:
    path = OUTPUT_DIR / f"{job_id}.part{index}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".csv.tmp")
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    os.replace(tmp, path)
    return path


def _finalize_csv(job_id: str, part_paths: list[Path]) -> tuple[Path, int, int]:
    """Concatenate partition files in order, dedupe (google_maps_url, falling
    back to name+zip). Returns (final_path, row_count, with_email_count)."""
    out_path = OUTPUT_DIR / f"{job_id}.csv"
    tmp = out_path.with_suffix(".csv.tmp")
    seen: set[tuple[str, ...]] = set()
    rows_written = 0
    with_emails = 0
    with open(tmp, "w", newline="", encoding="utf-8") as out_f:
        writer = csv.DictWriter(out_f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for part in part_paths:
            if not part.exists():
                continue
            with open(part, newline="", encoding="utf-8") as in_f:
                for row in csv.DictReader(in_f):
                    key = (
                        ("url", row.get("google_maps_url", ""))
                        if row.get("google_maps_url")
                        else ("nz", row.get("name", ""), row.get("zip", ""))
                    )
                    if key in seen:
                        continue
                    seen.add(key)
                    writer.writerow(row)
                    rows_written += 1
                    if row.get("email"):
                        with_emails += 1
    os.replace(tmp, out_path)
    return out_path, rows_written, with_emails


def _set_total_tasks(job_id: str, total: int) -> None:
    store = job_store.get_store()
    store.conn.execute(
        "UPDATE jobs SET total_tasks=?, updated_at=? WHERE job_id=?",
        (total, _now(), job_id),
    )
    store.conn.commit()


async def run_places_job(job_id: str, *, resume: bool = False) -> None:
    """Drive one places job to a terminal state. Never raises — every failure
    path lands in set_failed so the job card shows the reason."""
    emitter = _Emitter(job_id)
    store = job_store.get_store()
    job = store.get_job(job_id)
    if not job or job.get("source_type") != SOURCE_TYPE:
        logger.error("run_places_job: %s is not a places job", job_id)
        return

    try:
        regions = json.loads(job.get("regions") or "{}")
    except (ValueError, TypeError):
        regions = {}
    filters = regions.get("filters") or {}
    state = regions.get("places") or {}

    if store.is_job_cancelled_or_abandoned(job_id):
        return

    try:
        partitions: Optional[list[dict[str, Any]]] = state.get("partitions")
        done_parts: list[int] = list(state.get("done_parts") or [])
        export_id = state.get("export_id")
        expected = state.get("expected")

        # A phase-1 job (no 'partitions' key) resuming with only an export_id:
        # replan from scratch — its single in-flight export may complete
        # remotely unbilled-to-cache, an accepted one-export re-spend window.
        if resume and partitions is None:
            export_id = None
            done_parts = []

        # ---- Phase 1: precheck + count (+ cache short-circuit) -----------
        if not resume or partitions is None:
            emitter.emit({
                "phase": 1, "task_done": False, "new_results": 0,
                "status": "precheck", "message": "Checking daily budget…",
            })
            fu = await places_client.fair_use()
            daily_remaining = (fu or {}).get("daily", {}).get("remaining")
            resets_at = (fu or {}).get("daily", {}).get("resets_at")
            if daily_remaining is not None and daily_remaining <= 0:
                raise PlacesError(
                    "fair_use", 402,
                    f"GetLeads daily Google Maps budget is used up (resets {resets_at or 'at 00:00 UTC'}). "
                    "Try again after the reset, or use the live Google Maps scrape instead.",
                )

            cached = places_cache_lookup(filters)
            if cached:
                out_path = OUTPUT_DIR / f"{job_id}.csv"
                shutil.copyfile(cached["csv_path"], out_path)
                rows_n = cached.get("row_count") or 0
                emitter.emit({
                    "phase": 1, "task_done": True, "new_results": rows_n,
                    "status": "done",
                    "message": f"Served from cache — {rows_n:,} businesses, 0 budget used.",
                })
                store.update_result_count(job_id, rows_n)
                store._mark_done_and_cleanup(job_id, str(out_path))
                logger.info("Places job %s: cache hit (%s rows)", job_id, rows_n)
                return

            emitter.emit({
                "phase": 1, "task_done": False, "new_results": 0,
                "status": "counting", "message": "Counting matches (free)…",
            })
            total = await places_client.count_places(filters)
            expected = total
            _save_places_state(job_id, expected=total)
            emitter.emit({
                "phase": 1, "task_done": True, "new_results": 0,
                "status": "counted", "message": f"{total:,} businesses match.",
            })

            if total == 0:
                out_path, n, _ = _finalize_csv(job_id, [])
                store.update_result_count(job_id, 0)
                emitter.emit({
                    "phase": 3, "task_done": True, "new_results": 0,
                    "status": "done", "message": "No matches — empty CSV ready.",
                })
                store._mark_done_and_cleanup(job_id, str(out_path))
                return

            if total > places_client.MAX_EXPORT_ROWS:
                emitter.emit({
                    "phase": 1, "task_done": True, "new_results": 0,
                    "status": "planning",
                    "message": f"{total:,} businesses — splitting into {max(2, -(-total // PARTITION_TARGET))}+ fetches (each caps at 25,000)…",
                })
                partitions = await plan_partitions(filters, total)
                _save_places_state(job_id, partitions=partitions)
                _set_total_tasks(job_id, 2 + len(partitions))
                emitter.emit({
                    "phase": 1, "task_done": False, "new_results": 0,
                    "status": "planned",
                    "message": f"Plan: {len(partitions)} ZIP-grouped fetches.",
                })
            else:
                partitions = [dict(filters)]
                _save_places_state(job_id, partitions=partitions)
            done_parts = []

        n_parts = len(partitions or [])
        if n_parts == 0:
            raise PlacesError("plan", 0, "No partitions to fetch — internal error.")

        # ---- Phase 2/3: per-partition export → poll → durable part file ---
        for index in range(n_parts):
            if index in done_parts:
                continue
            if store.is_job_cancelled_or_abandoned(job_id):
                raise PlacesError("cancelled", 0, "Cancelled by user.")

            part_filters = partitions[index]
            if export_id is None:
                emitter.emit({
                    "phase": 2, "task_done": False, "new_results": 0,
                    "status": "exporting",
                    "message": f"Fetching part {index + 1}/{n_parts} from GetLeads…",
                })
                try:
                    export_id = await places_client.start_export(part_filters)
                except PlacesError as exc:
                    if exc.status_code == 402 and done_parts:
                        # Budget exhausted mid-run: finalize what completed.
                        return await _finalize_partial(
                            job_id, emitter, partitions, done_parts,
                            expected, "budget ran out",
                        )
                    raise
                _save_places_state(job_id, export_id=export_id)

            export_url, _rows_avail = await _poll_until_done(job_id, emitter, str(export_id))
            _, raw_rows = await places_client.download_export_csv(export_url)
            rows = [_normalize_row(r) for r in raw_rows]
            _write_part_csv(job_id, index, rows)

            export_id = None
            done_parts.append(index)
            _save_places_state(job_id, export_id=None, done_parts=done_parts)
            emitter.emit({
                "phase": 2, "task_done": True, "new_results": len(rows),
                "status": "part_done",
                "message": f"Part {index + 1}/{n_parts}: {len(rows):,} businesses saved.",
            })

        # ---- Finalize: concat + dedupe + exact count + cache store ---------
        part_paths = [OUTPUT_DIR / f"{job_id}.part{i}.csv" for i in range(n_parts)]
        out_path, n_rows, with_emails = _finalize_csv(job_id, part_paths)
        emitter.emit({
            "phase": 3, "task_done": True, "new_results": n_rows,
            "status": "done",
            "message": f"{n_rows:,} businesses ({with_emails:,} with email) saved.",
        })
        store.update_result_count(job_id, n_rows)
        store._mark_done_and_cleanup(job_id, str(out_path))
        try:
            places_cache_store(filters, out_path, n_rows)
        except Exception as exc:  # cache failure must never fail a done job
            logger.warning("Places job %s: cache store failed: %s", job_id, exc)
        for p in part_paths:  # durable parts are merged; free the space
            try:
                p.unlink(missing_ok=True)
            except OSError:
                pass
        logger.info("Places job %s done: %d rows (%d with email)",
                    job_id, n_rows, with_emails)

    except PlacesError as exc:
        if exc.detail.startswith("Cancelled"):
            return  # cancel core already set the terminal status
        emitter.emit({
            "phase": 0, "task_done": False, "new_results": 0,
            "status": "failed", "message": exc.detail,
        })
        _fail(job_id, exc.detail)
    except Exception as exc:  # never leave a places job hanging in 'running'
        logger.exception("Places job %s crashed", job_id)
        message = f"Unexpected error: {exc}"[:500]
        emitter.emit({
            "phase": 0, "task_done": False, "new_results": 0,
            "status": "failed", "message": message,
        })
        _fail(job_id, message)


async def _finalize_partial(
    job_id: str,
    emitter: _Emitter,
    partitions: list[dict[str, Any]],
    done_parts: list[int],
    expected: Optional[int],
    reason: str,
) -> None:
    """Budget ran out mid-run with ≥1 completed partition → deliver the
    partial result as done + a clearly-labeled PARTIAL message (NOT cached)."""
    store = job_store.get_store()
    part_paths = [OUTPUT_DIR / f"{job_id}.part{i}.csv" for i in sorted(done_parts)]
    out_path, n_rows, with_emails = _finalize_csv(job_id, part_paths)
    expected_str = f" of ~{expected:,}" if expected else ""
    message = (
        f"PARTIAL: {n_rows:,}{expected_str} businesses saved ({with_emails:,} with email) — "
        f"{reason} after part {len(done_parts)}/{len(partitions)}. "
        "Re-run the same filters after the daily reset (00:00 UTC) to top up."
    )
    emitter.emit({
        "phase": 3, "task_done": True, "new_results": n_rows,
        "status": "done", "message": message,
    })
    store.update_result_count(job_id, n_rows)
    store._mark_done_and_cleanup(job_id, str(out_path))
    for p in part_paths:
        try:
            p.unlink(missing_ok=True)
        except OSError:
            pass
    logger.info("Places job %s finalized PARTIAL: %d rows (%s)", job_id, n_rows, reason)


async def _poll_until_done(
    job_id: str, emitter: _Emitter, export_id: str
) -> tuple[str, Optional[int]]:
    """Poll the export until completed/failed/timeout/cancel. Returns
    (export_url, rows_available)."""
    store = job_store.get_store()
    deadline = asyncio.get_event_loop().time() + places_client.EXPORT_TIMEOUT_S
    while True:
        if store.is_job_cancelled_or_abandoned(job_id):
            raise PlacesError(
                "cancelled", 0,
                "Cancelled — the remote export may still complete and use its rows.",
            )
        store.heartbeat(job_id)

        data = await places_client.poll_export(export_id)
        status = str(data.get("job_status") or data.get("status") or "").lower()
        if status == "completed":
            url = data.get("export_url")
            if not url:
                raise PlacesError("poll_export", 0, "completed export returned no export_url")
            return str(url), data.get("rows_exported")
        if status in ("failed", "error", "cancelled"):
            raise PlacesError("poll_export", 0, f"GetLeads export {status}: {str(data)[:200]}")

        if asyncio.get_event_loop().time() > deadline:
            raise PlacesError(
                "poll_export", 0,
                f"Export did not finish within {int(places_client.EXPORT_TIMEOUT_S)}s "
                f"(export_id {export_id}).",
            )
        await asyncio.sleep(places_client.EXPORT_POLL_INTERVAL_S)


# ---------------------------------------------------------------------------
# Restart re-arm (boot path — see main._run_parent_startup)
# ---------------------------------------------------------------------------


def _claim_for_rearm(job_id: str) -> bool:
    """Cross-worker CAS claim so exactly one of the 4 gunicorn workers re-arms
    each running places job. Uses the existing resume_claimed_at column with a
    10-minute stale override (a crashed claim shouldn't block re-arm forever)."""
    store = job_store.get_store()
    now = _now()
    cursor = store.conn.execute(
        """UPDATE jobs SET resume_claimed_at=?, updated_at=?
           WHERE job_id=? AND status='running'
             AND (resume_claimed_at IS NULL
                  OR datetime(resume_claimed_at) < datetime('now', '-10 minutes'))""",
        (now, now, job_id),
    )
    store.conn.commit()
    return cursor.rowcount > 0


def rearm_running_places_jobs() -> int:
    """Re-attach drivers to places jobs still 'running' after a restart.

    Completed partitions (durable {job_id}.part{i}.csv + done_parts state)
    are NOT re-fetched — only the in-flight and remaining partitions run.
    """
    if not places_client.is_enabled():
        return 0
    store = job_store.get_store()
    rows = store.conn.execute(
        """SELECT job_id FROM jobs
           WHERE job_type='scraper' AND source_type=? AND status='running'""",
        (SOURCE_TYPE,),
    ).fetchall()
    rearmed = 0
    for row in rows:
        job_id = row["job_id"]
        if _claim_for_rearm(job_id):
            asyncio.create_task(run_places_job(job_id, resume=True))
            rearmed += 1
            logger.info("Re-armed places job %s after restart", job_id)
    return rearmed
