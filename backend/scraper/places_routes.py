"""Routes for the GetLeads "Instant US Database" source (Google Maps Scraper
page, 2026-11-11). All JWT-authed like the rest of /api/scraper/*.

- GET  /categories — FREE category autocomplete
- POST /count      — FREE match count (+ fair-use summary for the UI badge)
- GET  /fair-use   — account google_maps budget gauge
- POST /jobs       — create + launch a places job (lands in the normal
                     scraper jobs list; CSV chains into enrichment as-is)

Kill switch: ENABLE_GETLEADS_PLACES=false omits the router from the app
(main.py) AND every handler short-circuits 503 (defense in depth).
"""

from __future__ import annotations

import logging
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from scraper import places_client, places_flow
from scraper.places_client import PlacesError
from shared import auth

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/scraper/places", tags=["scraper-places"])


class PlacesFilters(BaseModel):
    """Filter set for count + export. At least one business or geo filter is
    required (enforced by places_client.sanitize_filters)."""

    categories: List[str] = Field(default_factory=list)
    category_contains: List[str] = Field(default_factory=list)
    business_name: str = ""
    exclude_categories: List[str] = Field(default_factory=list)
    exclude_category_contains: List[str] = Field(default_factory=list)
    states: List[str] = Field(default_factory=list)
    zips: List[str] = Field(default_factory=list)
    has_email: Optional[bool] = None
    has_phone: Optional[bool] = None
    has_website: Optional[bool] = None

    def to_dict(self) -> dict:
        d = self.model_dump()
        return {k: v for k, v in d.items() if v is not None and v != "" and v != []}


class PlacesCountRequest(BaseModel):
    filters: PlacesFilters


class PlacesJobRequest(BaseModel):
    filters: PlacesFilters
    group: Optional[str] = None


def _guard_enabled() -> None:
    if not places_client.is_enabled() or not places_client.is_configured():
        raise HTTPException(
            status_code=503,
            detail="Instant US Database is disabled or GETLEADS_API_KEY is not configured.",
        )


@router.get("/categories")
async def places_categories(
    q: str = Query(default="", max_length=100),
    page: int = Query(default=1, ge=1),
    current_user: dict = Depends(auth.get_current_user),
):
    """FREE category-name lookup for the autocomplete."""
    _guard_enabled()
    try:
        data = await places_client.search_categories(q=q, page=page)
    except PlacesError as exc:
        raise HTTPException(status_code=502, detail=exc.detail)
    return {
        "categories": data.get("categories", []),
        "total": data.get("total", 0),
        "has_more": bool(data.get("has_more")),
    }


@router.post("/count")
async def places_count(
    req: PlacesCountRequest,
    current_user: dict = Depends(auth.get_current_user),
):
    """FREE match count for the chosen filters + the account's google_maps
    daily budget, so the UI can show "N businesses · M rows left today"."""
    _guard_enabled()
    try:
        filters = places_client.sanitize_filters(req.filters.to_dict())
        total = await places_client.count_places(filters)
        fu = await places_client.fair_use()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except PlacesError as exc:
        status = 402 if exc.status_code == 402 else 502
        raise HTTPException(status_code=status, detail=exc.detail)

    daily = (fu or {}).get("daily", {})
    return {
        "total_matching": total,
        "max_export_rows": places_client.MAX_EXPORT_ROWS,
        "fair_use": {
            "daily_remaining": daily.get("remaining"),
            "daily_cap": daily.get("cap"),
            "resets_at": daily.get("resets_at"),
        },
    }


@router.get("/fair-use")
async def places_fair_use(current_user: dict = Depends(auth.get_current_user)):
    """Account-wide google_maps budget gauge (shared by every consumer of the
    GetLeads places API — including manual MCP pulls)."""
    _guard_enabled()
    fu = await places_client.fair_use()
    if fu is None:
        return {"available": False}
    return {"available": True, "google_maps": fu}


@router.post("/jobs")
async def create_places_job(
    req: PlacesJobRequest,
    current_user: dict = Depends(auth.get_current_user),
):
    """Create an instant places job. Returns immediately; the job runs in the
    background and appears in the normal scraper jobs list."""
    _guard_enabled()
    try:
        result = places_flow.create_and_launch(
            user=current_user,
            filters=req.filters.to_dict(),
            group=req.group,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except PlacesError as exc:
        status = 503 if exc.status_code == 503 else 502
        raise HTTPException(status_code=status, detail=exc.detail)
    return result
