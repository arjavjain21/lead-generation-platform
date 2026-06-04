"""
Google Maps Scraper API routes.

This module provides all scraper-related endpoints:
- Region discovery (states, cities)
- Job management (create, list, get, stream, download)
- Partial downloads for running jobs
- Contacts sync to leadsdatabase.cc
"""

from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import os
import re
import uuid
from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, File, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

from shared import auth, db
from . import centers as centers_module
from . import crawler as crawler_module
from . import job_store
from . import sync_contacts

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent.parent / "data"
OUTPUT_DIR = DATA_DIR / "outputs"
DATA_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)

router = APIRouter(prefix="/api/scraper", tags=["scraper"])

# Per-job asyncio Events to wake SSE consumers
_job_signals: dict[str, asyncio.Event] = {}
_active_jobs: set[str] = set()
_cancelled_jobs: set[str] = set()


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class StartJobRequest(BaseModel):
    query: str
    mode: str = "all"          # "all" | "states" | "cities" | "zips"
    country: str = "us"        # ISO 2-letter: us, gb, ie, au, ca
    states: list[str] = []
    cities: list[str] = []
    zips: list[str] = []       # US zip codes, UK postcodes, or Canada postal codes (for mode="zips")
    center_ids: list[str] = []  # For non-US: selected center names
    expected_types: list[str] = []


# ---------------------------------------------------------------------------
# Region discovery routes
# ---------------------------------------------------------------------------

@router.get("/regions/countries")
async def list_countries():
    """Return all countries with center data."""
    return {"countries": centers_module.get_countries()}


@router.get("/regions/centers")
async def list_centers(country: str = Query(default="us", description="ISO 2-letter country code")):
    """Return all centers for a country (for center selection)."""
    centers = centers_module.get_centers_for_country(country)
    return {
        "centers": [
            {"name": c["name"], "state": c["state"], "lat": c["lat"], "lng": c["lng"]}
            for c in centers
        ]
    }


@router.get("/regions/states")
async def list_states():
    """Return all canonical state names present in the centers CSV."""
    return {"states": centers_module.CANONICAL_STATES}


@router.get("/regions/cities")
async def search_cities(q: str = Query(default="", max_length=100), country: str = Query(default="us", description="ISO 2-letter country code")):
    """Fuzzy-search anchor cities for autocomplete. Returns up to 20 results."""
    results = centers_module.search_cities(q, country)
    return {
        "cities": [
            {"name": c["name"], "state": c.get("state", ""), "lat": c["lat"], "lng": c["lng"]}
            for c in results
        ]
    }


@router.post("/regions/parse-zip-csv")
async def parse_zip_csv(
    file: UploadFile = File(...),
):
    """
    Parse a CSV file containing zip codes and return the list of zip codes found.
    Looks for any column containing 5-digit zip codes.
    """
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are supported.")

    content = await file.read()
    try:
        text = content.decode('utf-8')
    except UnicodeDecodeError:
        raise HTTPException(status_code=400, detail="File must be UTF-8 encoded.")

    reader = csv.DictReader(io.StringIO(text))
    zips = []

    for row in reader:
        # Check each column for 5-digit zip codes
        for col_name, col_value in row.items():
            if col_value and isinstance(col_value, str):
                value = col_value.strip()
                # Check for 5-digit zip code pattern
                if len(value) == 5 and value.isdigit():
                    if value not in zips:
                        zips.append(value)
                # Check for zip code in format "ZIP - City" or "City, ST ZIP"
                elif len(value) >= 5:
                    # Find all 5-digit patterns
                    found = re.findall(r'\b(\d{5})\b', value)
                    for z in found:
                        if z not in zips:
                            zips.append(z)

    if not zips:
        raise HTTPException(status_code=400, detail="No valid 5-digit zip codes found in the file.")

    # Validate against our zip database
    valid, invalid = centers_module.validate_zip_codes(zips)

    return {
        "zips_found": zips,
        "count_found": len(zips),
        "valid_zips": valid,
        "invalid_zips": invalid,
        "count_valid": len(valid),
        "count_invalid": len(invalid),
    }


@router.post("/regions/parse-uk-postcode-csv")
async def parse_uk_postcode_csv(
    file: UploadFile = File(...),
):
    """
    Parse a CSV file containing UK postcodes and return the list of postcodes found.
    Looks for any column containing UK postcodes (format: AA9A 9AA or similar).
    """
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are supported.")

    content = await file.read()
    try:
        text = content.decode('utf-8')
    except UnicodeDecodeError:
        raise HTTPException(status_code=400, detail="File must be UTF-8 encoded.")

    reader = csv.DictReader(io.StringIO(text))
    postcodes = []

    for row in reader:
        # Check each column for UK postcodes
        for col_name, col_value in row.items():
            if col_value and isinstance(col_value, str):
                value = col_value.strip()
                # UK postcode pattern: A(A)N(N)(A) NAA
                # Match with or without space
                matches = re.findall(r'\b[A-Z]{1,2}[0-9][A-Z0-9]? ?[0-9][A-Z]{2}\b', value.upper())
                for pc in matches:
                    # Normalize: ensure space
                    if " " not in pc and len(pc) >= 5:
                        pc = pc[:-3] + " " + pc[-3:]
                    if pc not in postcodes:
                        postcodes.append(pc)

    if not postcodes:
        raise HTTPException(status_code=400, detail="No valid UK postcodes found in the file.")

    # Validate against our postcode database
    valid, invalid = centers_module.validate_uk_postcodes(postcodes)

    return {
        "postcodes_found": postcodes,
        "count_found": len(postcodes),
        "valid_postcodes": valid,
        "invalid_postcodes": invalid,
        "count_valid": len(valid),
        "count_invalid": len(invalid),
    }


@router.post("/regions/parse-ca-postal-csv")
async def parse_ca_postal_csv(
    file: UploadFile = File(...),
):
    """
    Parse a CSV file containing Canada postal codes and return the list of postal codes found.
    Looks for any column containing Canada postal codes (format: ANA NAN).
    """
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are supported.")

    content = await file.read()
    try:
        text = content.decode('utf-8')
    except UnicodeDecodeError:
        raise HTTPException(status_code=400, detail="File must be UTF-8 encoded.")

    reader = csv.DictReader(io.StringIO(text))
    postal_codes = []

    for row in reader:
        # Check each column for Canada postal codes
        for col_name, col_value in row.items():
            if col_value and isinstance(col_value, str):
                value = col_value.strip()
                # Canada postal code pattern: ANA NAN
                matches = re.findall(r'\b[A-Z][0-9][A-Z] ?[0-9][A-Z][0-9]\b', value.upper())
                for pc in matches:
                    # Normalize: ensure space
                    if " " not in pc:
                        pc = pc[:3] + " " + pc[3:]
                    if pc not in postal_codes:
                        postal_codes.append(pc)

    if not postal_codes:
        raise HTTPException(status_code=400, detail="No valid Canada postal codes found in the file.")

    # Validate against our postal code database
    valid, invalid = centers_module.validate_ca_postal_codes(postal_codes)

    return {
        "postal_codes_found": postal_codes,
        "count_found": len(postal_codes),
        "valid_postal_codes": valid,
        "invalid_postal_codes": invalid,
        "count_valid": len(valid),
        "count_invalid": len(invalid),
    }


@router.get("/regions/cities")
async def list_all_cities(country: str = Query(default="us", description="ISO 2-letter country code")):
    """
    Get all cities available for zip/postal code mapping.

    For US, returns all cities from the zip code database (29,546 cities).
    For other countries, returns the list of centers.
    """
    if country.lower() == "us":
        cities = centers_module.get_all_us_cities()
        # Sort by zip count (most zips first) then alphabetically
        cities.sort(key=lambda x: (-x["zip_count"], x["name"]))
        return {
            "cities": cities,
            "total": len(cities),
            "country": "us"
        }
    else:
        # For other countries, return centers
        centers = centers_module.get_centers_for_country(country)
        city_list = []
        seen_cities = set()
        for center in centers:
            city_state = f"{center.get('name', '')}, {center.get('state', '')}"
            if city_state not in seen_cities and center.get('name'):
                seen_cities.add(city_state)
                city_list.append({
                    "name": center["name"],
                    "state": center.get("state", ""),
                    "lat": center["lat"],
                    "lng": center["lng"]
                })
        city_list.sort(key=lambda x: x["name"])
        return {
            "cities": city_list,
            "total": len(city_list),
            "country": country
        }


class CitiesToZipsRequest(BaseModel):
    cities: list[str]
    country: str = "us"


@router.post("/regions/cities-to-zips")
async def cities_to_zips(req: CitiesToZipsRequest):
    """
    Convert a list of cities to their corresponding zip/postal codes.

    For US, maps cities to all zip codes in those cities.
    For UK/Canada, maps cities to available postcode/FSA areas.

    Returns mapping of cities to their codes and any errors.
    """
    if not req.cities:
        raise HTTPException(status_code=400, detail="No cities provided.")

    if req.country.lower() == "us":
        city_zips_map, errors = centers_module.get_zips_for_cities(req.cities)

        # Flatten all zips
        all_zips = []
        for zips in city_zips_map.values():
            all_zips.extend(zips)

        return {
            "city_zips_map": city_zips_map,
            "all_zips": all_zips,
            "total_zips": len(all_zips),
            "errors": errors
        }
    else:
        # For UK/Canada, try to match city names to postcode areas
        # This is a simple implementation - could be enhanced
        errors = []
        city_codes_map = {}

        if req.country.lower() == "gb":
            pc_db = centers_module._load_uk_postcode_database()
            for city_input in req.cities:
                city_input = city_input.strip().lower()
                matched_codes = []

                for postcode, data in pc_db.items():
                    if city_input in postcode.lower() or city_input in data.get('city', '').lower():
                        matched_codes.append(postcode)

                if matched_codes:
                    # Use the original city name as key
                    city_name = req.cities[req.cities.index(city_input)]
                    city_codes_map[city_name] = matched_codes
                else:
                    errors.append(f"City '{city_input}' not found in UK postcode database")

        elif req.country.lower() == "ca":
            pc_db = centers_module._load_ca_postal_code_database()
            for city_input in req.cities:
                city_input = city_input.strip().lower()
                matched_codes = []

                for postal_code, data in pc_db.items():
                    if city_input in postal_code or city_input in data.get('city', '').lower():
                        matched_codes.append(postal_code)

                if matched_codes:
                    city_name = req.cities[req.cities.index(city_input)]
                    city_codes_map[city_name] = matched_codes
                else:
                    errors.append(f"City '{city_input}' not found in Canada postal code database")

        all_codes = []
        for codes in city_codes_map.values():
            all_codes.extend(codes)

        return {
            "city_codes_map": city_codes_map,
            "all_codes": all_codes,
            "total_codes": len(all_codes),
            "errors": errors,
            "country": req.country
        }


@router.get("/regions/download-template")
async def download_template(template_type: str = Query(default="us_cities")):
    """
    Download a template CSV file for the specified type.

    Available templates:
    - us_cities: US cities template
    - us_zips: US zip codes template
    - uk_postcodes: UK postcode areas template
    - ca_postal_codes: Canada postal areas template
    """
    templates = {
        "us_cities": ("us_cities_template.csv", "text/csv", "US_Cities_Template.csv"),
        "us_zips": ("us_zips_template.csv", "text/csv", "US_Zip_Codes_Template.csv"),
        "uk_postcodes": ("uk_postcodes_template.csv", "text/csv", "UK_Postcode_Areas_Template.csv"),
        "ca_postal_codes": ("ca_postal_codes_template.csv", "text/csv", "Canada_Postal_Areas_Template.csv"),
    }

    if template_type not in templates:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid template type. Available: {list(templates.keys())}"
        )

    template_file, content_type, download_name = templates[template_type]
    template_path = DATA_DIR / "templates" / template_file

    if not template_path.exists():
        raise HTTPException(status_code=404, detail=f"Template file not found: {template_file}")

    return FileResponse(
        path=str(template_path),
        filename=download_name,
        media_type=content_type
    )


@router.post("/regions/estimate")
async def estimate_tasks(req: StartJobRequest, current_user: dict = Depends(auth.get_current_user)):
    """
    Returns the estimated number of API calls for the given region selection.
    Also validates state/city inputs and returns any errors.
    Includes quota status for non-admin users.
    """
    if not req.query.strip():
        raise HTTPException(status_code=400, detail="Query cannot be empty.")

    filtered_centers, errors = centers_module.get_centers_for_job(
        mode=req.mode,
        country=req.country,
        states=req.states,
        cities=req.cities,
        zips=req.zips,
        center_ids=req.center_ids,
    )

    # For zip codes: each zip = 1 task (zoom 12 only)
    if req.mode == "zips":
        task_count = len(filtered_centers)
    else:
        task_count = centers_module.estimate_task_count(filtered_centers)
    is_admin = current_user.get("is_admin", False)
    quota_status = db.get_api_quota_status(current_user["user_id"], is_admin)

    # Check if user has enough quota
    can_proceed = True
    quota_message = ""
    if not is_admin:
        allowed, limit_message = db.check_daily_request_limit(
            user_id=current_user["user_id"],
            is_admin=False,
            estimated_requests=task_count,
        )
        can_proceed = allowed
        quota_message = limit_message if not allowed else ""

    return {
        "center_count": len(filtered_centers),
        "task_count": task_count,
        "errors": errors,
        "quota": quota_status,
        "can_proceed": can_proceed,
        "quota_message": quota_message,
    }


# ---------------------------------------------------------------------------
# Job routes
# ---------------------------------------------------------------------------

def _owns_job(job: dict[str, Any], current_user: dict[str, Any]) -> bool:
    if current_user.get("is_admin"):
        return True
    return job.get("user_id") == current_user["user_id"]


@router.get("/jobs")
async def list_scraper_jobs(current_user: dict = Depends(auth.get_current_user)):
    """List scraper jobs for current user (or all for admin)."""
    store = job_store.get_store()
    if current_user.get("is_admin"):
        jobs = store.list_jobs(job_type="scraper", limit=200)
    else:
        jobs = store.list_jobs(user_id=current_user["user_id"], job_type="scraper", limit=200)
    return {"jobs": jobs}


@router.post("/jobs")
async def start_job(
    req: StartJobRequest,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(auth.get_current_user),
):
    if not req.query.strip():
        raise HTTPException(status_code=400, detail="Query cannot be empty.")

    api_key = os.getenv("SCRAPER_TECH_KEY", "")
    if not api_key:
        raise HTTPException(status_code=500, detail="SCRAPER_TECH_KEY is not configured on the server.")

    filtered_centers, errors = centers_module.get_centers_for_job(
        mode=req.mode,
        country=req.country,
        states=req.states,
        cities=req.cities,
        center_ids=req.center_ids,
    )

    if errors and not filtered_centers:
        raise HTTPException(status_code=400, detail="; ".join(errors))

    if not filtered_centers:
        raise HTTPException(status_code=400, detail="No geographic centers found for the selected region.")

    total_tasks = centers_module.estimate_task_count(filtered_centers)

    # Check daily API request limit for non-admin users
    is_admin = current_user.get("is_admin", False)
    allowed, limit_message = db.check_daily_request_limit(
        user_id=current_user["user_id"],
        is_admin=is_admin,
        estimated_requests=total_tasks,
    )
    if not allowed:
        raise HTTPException(status_code=429, detail=limit_message)

    job_id = str(uuid.uuid4())

    regions_payload = {
        "mode": req.mode,
        "country": req.country,
        "states": req.states,
        "cities": req.cities,
        "zips": req.zips,
        "center_ids": req.center_ids,
    }

    store = job_store.get_store()
    store.create_scraper_job(
        job_id=job_id,
        user_id=current_user["user_id"],
        query=req.query.strip(),
        regions=regions_payload,
        total_tasks=total_tasks,
    )

    _job_signals[job_id] = asyncio.Event()
    _active_jobs.add(job_id)

    output_path = OUTPUT_DIR / f"{job_id}.csv"

    background_tasks.add_task(
        _run_job,
        job_id=job_id,
        user_id=current_user["user_id"],
        is_admin=is_admin,
        query=req.query.strip(),
        filtered_centers=filtered_centers,
        api_key=api_key,
        output_path=output_path,
        expected_types=req.expected_types or [],
    )

    return {
        "job_id": job_id,
        "total_tasks": total_tasks,
        "center_count": len(filtered_centers),
        "warnings": errors,
    }


@router.get("/jobs/{job_id}")
async def get_scraper_job(job_id: str, current_user: dict = Depends(auth.get_current_user)):
    """Get a scraper job by ID."""
    store = job_store.get_store()
    job_data = store.get_job(job_id)
    if not job_data:
        raise HTTPException(status_code=404, detail="Job not found.")
    if job_data.get("job_type") != "scraper":
        raise HTTPException(status_code=404, detail="Scraper job not found.")
    if not _owns_job(job_data, current_user):
        raise HTTPException(status_code=403, detail="Access denied.")
    return job_data


@router.get("/jobs/{job_id}/stream")
async def stream_scraper_job(
    job_id: str,
    token: Optional[str] = Query(default=None),
    current_user: Optional[dict] = Depends(auth.get_current_user_optional),
):
    """
    SSE stream of scraper job progress events.
    Supports reconnection: replays all stored events then streams live.
    """
    if current_user is None:
        if token:
            current_user = auth.decode_token(token)
        else:
            raise HTTPException(status_code=401, detail="Authentication required.")

    store = job_store.get_store()
    job_data = store.get_job(job_id)
    if not job_data:
        raise HTTPException(status_code=404, detail="Job not found.")
    if job_data.get("job_type") != "scraper":
        raise HTTPException(status_code=404, detail="Scraper job not found.")
    if not _owns_job(job_data, current_user):
        raise HTTPException(status_code=403, detail="Access denied.")

    async def event_generator():
        sent = 0
        while True:
            new_events = store.get_events_from(job_id, sent)
            for event in new_events:
                sent += 1
                yield f"data: {json.dumps(event)}\n\n"

            current = store.get_job(job_id)
            if current and current["status"] in ("done", "failed"):
                final = {
                    "done": True,
                    "error": current.get("error"),
                    "total_tasks": current.get("total_tasks", 0),
                    "done_tasks": current.get("done_tasks", 0),
                    "result_count": current.get("result_count", 0),
                }
                yield f"data: {json.dumps(final)}\n\n"
                break

            sig = _job_signals.get(job_id)
            if sig:
                try:
                    await asyncio.wait_for(
                        asyncio.shield(asyncio.ensure_future(_wait_event(sig))),
                        timeout=2.0,
                    )
                except asyncio.TimeoutError:
                    pass
            else:
                await asyncio.sleep(2.0)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/jobs/{job_id}/download")
async def download_scraper_result(job_id: str, current_user: dict = Depends(auth.get_current_user)):
    """Download the full CSV output of a completed scraper job."""
    store = job_store.get_store()
    job_data = store.get_job(job_id)
    if not job_data:
        raise HTTPException(status_code=404, detail="Job not found.")
    if job_data.get("job_type") != "scraper":
        raise HTTPException(status_code=404, detail="Scraper job not found.")
    if not _owns_job(job_data, current_user):
        raise HTTPException(status_code=403, detail="Access denied.")
    if job_data["status"] in ("queued", "running"):
        raise HTTPException(status_code=202, detail="Job not finished yet.")

    # Check if job failed but has partial output available
    if job_data["status"] == "failed":
        output_path = job_data.get("output_path")
        error_msg = job_data.get("error", "")

        # If output_path is not in database, try the standard location
        if not output_path:
            output_path = OUTPUT_DIR / f"{job_id}.csv"

        # If failed but partial output exists and file is not empty
        if Path(output_path).exists() and Path(output_path).stat().st_size > 0:
            # Allow download with a warning
            logger.info("Downloading partial results for failed scraper job %s: %s", job_id, error_msg)
            # Continue to download below (don't raise exception)
        else:
            # No partial output available
            raise HTTPException(status_code=500, detail=f"Job failed: {error_msg}")
    else:
        # For non-failed jobs, get output_path from database
        output_path = job_data.get("output_path")
        if not output_path or not Path(output_path).exists():
            raise HTTPException(status_code=404, detail="Output file not found.")

    query_slug = job_data.get("query", "results")[:30].replace(" ", "-").replace("/", "-")
    filename = f"{query_slug}_{job_id[:8]}.csv"

    return FileResponse(path=output_path, media_type="text/csv", filename=filename)


@router.get("/jobs/{job_id}/partial-download")
async def partial_download_scraper(job_id: str, current_user: dict = Depends(auth.get_current_user)):
    """
    Download partial CSV results from a running scraper job.
    Returns whatever data has been written so far.
    """
    store = job_store.get_store()
    job_data = store.get_job(job_id)
    if not job_data:
        raise HTTPException(status_code=404, detail="Job not found.")
    if job_data.get("job_type") != "scraper":
        raise HTTPException(status_code=404, detail="Scraper job not found.")
    if not _owns_job(job_data, current_user):
        raise HTTPException(status_code=403, detail="Access denied.")

    output_path = OUTPUT_DIR / f"{job_id}.csv"
    if not output_path.exists():
        raise HTTPException(status_code=404, detail="No data available yet.")

    query_slug = job_data.get("query", "partial")[:30].replace(" ", "-").replace("/", "-")
    filename = f"partial_{query_slug}_{job_id[:8]}.csv"

    return FileResponse(path=output_path, media_type="text/csv", filename=filename)


@router.post("/jobs/{job_id}/sync-to-contacts")
async def sync_scraper_job_to_contacts(
    job_id: str,
    current_user: dict = Depends(auth.get_current_user),
):
    """
    Sync scraper job results to the Contacts DB (leadsdatabase.cc).
    Uses place_id for deduplication — existing records are not replaced.
    """
    store = job_store.get_store()
    job_data = store.get_job(job_id)
    if not job_data:
        raise HTTPException(status_code=404, detail="Job not found.")
    if job_data.get("job_type") != "scraper":
        raise HTTPException(status_code=404, detail="Scraper job not found.")
    if not _owns_job(job_data, current_user):
        raise HTTPException(status_code=403, detail="Access denied.")
    if job_data["status"] != "done":
        raise HTTPException(status_code=400, detail="Job must be complete before syncing.")
    output_path = job_data.get("output_path")
    if not output_path or not Path(output_path).exists():
        raise HTTPException(status_code=404, detail="Output file not found.")

    try:
        result = sync_contacts.sync_job_to_contacts(Path(output_path))
        return {"ok": True, **result}
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/jobs/{job_id}/cancel")
async def cancel_scraper_job(
    job_id: str,
    current_user: dict = Depends(auth.get_current_user),
):
    """Cancel a running or queued scraper job."""
    store = job_store.get_store()
    job = store.get_job(job_id)

    if not job:
        raise HTTPException(status_code=404, detail="Job not found.")
    if job.get("job_type") != "scraper":
        raise HTTPException(status_code=404, detail="Scraper job not found.")
    if not _owns_job(job, current_user):
        raise HTTPException(status_code=403, detail="Access denied.")
    if job["status"] not in ("queued", "running"):
        raise HTTPException(status_code=400, detail="Only queued or running jobs can be cancelled.")

    # Mark job as failed with cancellation message
    store.set_failed(job_id, "Job cancelled by user")

    # Add to cancelled set so background task knows to stop
    _cancelled_jobs.add(job_id)

    # Remove from active jobs set
    _active_jobs.discard(job_id)

    # Wake up any SSE listeners
    sig = _job_signals.pop(job_id, None)
    if sig:
        sig.set()

    logger.info("Scraper job %s cancelled by user %s", job_id, current_user.get("user_id"))
    return {"ok": True, "message": "Job cancelled successfully"}


@router.post("/jobs/{job_id}/restart")
async def restart_scraper_job(
    job_id: str,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(auth.get_current_user),
):
    """Restart a failed scraper job with the same configuration."""
    store = job_store.get_store()
    job = store.get_job(job_id)

    if not job:
        raise HTTPException(status_code=404, detail="Job not found.")
    if job.get("job_type") != "scraper":
        raise HTTPException(status_code=404, detail="Scraper job not found.")
    if not _owns_job(job, current_user):
        raise HTTPException(status_code=403, detail="Access denied.")
    if job["status"] != "failed":
        raise HTTPException(status_code=400, detail="Only failed jobs can be restarted.")

    # Get the original job configuration
    query = job.get("query")
    regions_json = job.get("regions")

    if not query or not regions_json:
        raise HTTPException(status_code=500, detail="Original job configuration not found.")

    try:
        regions = json.loads(regions_json)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to parse regions configuration: {str(e)}")

    # Get the filtered centers for the regions
    filtered_centers, errors = centers_module.get_centers_for_job(
        mode=regions.get("mode", "all"),
        country=regions.get("country", "us"),
        states=regions.get("states", []),
        cities=regions.get("cities", []),
        zips=regions.get("zips", []),
        center_ids=regions.get("center_ids", []),
    )

    if errors and not filtered_centers:
        raise HTTPException(status_code=400, detail="; ".join(errors))

    if not filtered_centers:
        raise HTTPException(status_code=400, detail="No geographic centers found for the selected region.")

    total_tasks = centers_module.estimate_task_count(filtered_centers)

    # Check daily API request limit for non-admin users
    is_admin = current_user.get("is_admin", False)
    allowed, limit_message = db.check_daily_request_limit(
        user_id=current_user["user_id"],
        is_admin=is_admin,
        estimated_requests=total_tasks,
    )
    if not allowed:
        raise HTTPException(status_code=429, detail=limit_message)

    # Create new job
    new_job_id = str(uuid.uuid4())
    regions_payload = {
        "mode": regions.get("mode", "all"),
        "country": regions.get("country", "us"),
        "states": regions.get("states", []),
        "cities": regions.get("cities", []),
        "center_ids": regions.get("center_ids", []),
    }

    store.create_scraper_job(
        job_id=new_job_id,
        user_id=current_user["user_id"],
        query=query,
        regions=regions_payload,
        total_tasks=total_tasks,
        parent_job_id=job_id,  # Link to original job
    )

    _job_signals[new_job_id] = asyncio.Event()
    _active_jobs.add(new_job_id)

    output_path = OUTPUT_DIR / f"{new_job_id}.csv"

    # Get API key
    api_key = os.getenv("SCRAPER_TECH_KEY", "")
    if not api_key:
        raise HTTPException(status_code=500, detail="SCRAPER_TECH_KEY is not configured on the server.")

    # Start the background job
    background_tasks.add_task(
        _run_job,
        job_id=new_job_id,
        user_id=current_user["user_id"],
        is_admin=is_admin,
        query=query,
        filtered_centers=filtered_centers,
        api_key=api_key,
        output_path=output_path,
        expected_types=None,  # Could be stored in job if needed
        cancelled_jobs=_cancelled_jobs,  # Pass cancel tracking
    )

    logger.info("Scraper job %s restarted by user %s, new job: %s", job_id, current_user.get("user_id"), new_job_id)
    return {
        "job_id": new_job_id,
        "total": total_tasks,
        "restarted_from": job_id,
    }


# ---------------------------------------------------------------------------
# Background job runner
# ---------------------------------------------------------------------------

async def _run_job(
    job_id: str,
    user_id: str,
    is_admin: bool,
    query: str,
    filtered_centers: list[dict[str, Any]],
    api_key: str,
    output_path: Path,
    expected_types: list[str] | None = None,
    cancelled_jobs: Optional[set[str]] = None,
) -> None:
    store = job_store.get_store()
    store.set_running(job_id)
    seq = [0]
    requests_made = [0]  # Track actual API requests made

    async def on_progress(event: dict[str, Any]) -> None:
        # Get FRESH store instance for this thread
        # This fixes the progress counter bug where background tasks couldn't commit
        progress_store = job_store.get_store()
        progress_store.append_event(job_id, seq[0], event)
        seq[0] += 1
        sig = _job_signals.get(job_id)
        if sig:
            sig.set()
            sig.clear()

        # Track API requests - each completed task = 1 API request
        # Only track for non-admin users
        if event.get("task_done") and not is_admin:
            requests_made[0] += 1
            # Record the request (record in batch every 10 requests to reduce DB writes)
            if requests_made[0] % 10 == 0:
                db.record_api_requests(user_id, 10)

    try:
        result_count = await crawler_module.run_crawl(
            job_id=job_id,
            query=query,
            centers=filtered_centers,
            api_key=api_key,
            output_path=output_path,
            on_progress=on_progress,
            expected_types=expected_types or [],
            cancelled_jobs=cancelled_jobs,  # Pass cancel tracking set
        )

        # Record any remaining requests (non-multiple of 10)
        if not is_admin and requests_made[0] % 10 != 0:
            db.record_api_requests(user_id, requests_made[0] % 10)

        store.set_done(job_id, str(output_path))
        logger.info("Scraper job %s done — %d unique results, %d API requests", job_id[:8], result_count, requests_made[0])

    except RuntimeError as e:
        # Handle job cancellation
        if "was cancelled" in str(e):
            logger.info("Scraper job %s was cancelled by user", job_id[:8])
            # Job already marked as failed by cancel endpoint
            # Just ensure cleanup happens in finally block
            if output_path.exists():
                partial_size = output_path.stat().st_size
                if partial_size > 0:
                    logger.info("Cancelled job %s has partial output available: %d bytes", job_id[:8], partial_size)
        else:
            # Other RuntimeErrors should be handled as normal failures
            logger.exception("Scraper job %s failed with RuntimeError: %s", job_id[:8], e)
            store.set_failed(job_id, f"Job failed: {str(e)}")
    except Exception as e:
        logger.exception("Scraper job %s failed: %s", job_id[:8], e)
        store.set_failed(job_id, str(e))

    finally:
        _active_jobs.discard(job_id)
        sig = _job_signals.pop(job_id, None)
        if sig:
            sig.set()


async def _wait_event(event: asyncio.Event) -> None:
    await event.wait()
    event.clear()


def cleanup_stale_jobs() -> None:
    """Mark jobs as abandoned if they were running when server restarted.

    This status distinguishes from jobs that failed due to errors (user can retry).
    """
    store = job_store.get_store()
    stale = store.get_stale_running_jobs()
    for job_id in stale:
        store.set_abandoned(
            job_id,
            "Job was abandoned: Server restarted or crashed while processing. "
            "The job was interrupted before completion. Please retry from the beginning."
        )
        logger.warning("Marked stale scraper job %s as abandoned", job_id)
