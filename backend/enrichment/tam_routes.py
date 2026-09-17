"""TAM-by-People flow API — POST /api/enrichment/flows/tam (new Flow 2).

Kicks off a background TAM job that pages through Blitz
``/v2/company/tam-by-people`` with whitelisted company + people filters and
writes an incremental company CSV. Job status / SSE / download reuse the
generic enrichment endpoints (``/api/enrichment/jobs``,
``/api/enrichment/stream/{job_id}``, ``/api/enrichment/jobs/{job_id}``,
``.../download``) — the TAM job is a plain ``job_type='enrichment'`` row.

Filter whitelisting: the request models below enumerate every filter this
surface forwards; both filter models are ``extra="forbid"`` so an unknown
key is a 422 with a clear error instead of a silently-dropped or
forwarded-raw field. The Blitz payload is built ONLY from these known
fields (``to_payload``), never from a raw client dict.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field, field_validator

from enrichment import job_store
from enrichment import list_builder
from enrichment import tam_flow
from enrichment.routes import _active_jobs, _cancelled_jobs, _job_signals
from shared import auth

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/enrichment", tags=["enrichment"])

# Blitz job_level taxonomy (verified against the blitz-api-py SDK enums).
# Exact-match only: a typo returns a clear 422, not a silent empty search.
_VALID_JOB_LEVELS = frozenset({"C-Team", "Director", "Manager", "Other", "Staff", "VP"})


# ---------------------------------------------------------------------------
# Filter payload helpers (pure, no mutation)
# ---------------------------------------------------------------------------

def _keyword_payload(
    include: Optional[list[str]], exclude: Optional[list[str]]
) -> Optional[dict[str, Any]]:
    """Build a Blitz KeywordFilter {include, exclude} from optional lists."""
    payload: dict[str, Any] = {}
    if include:
        payload["include"] = list(include)
    if exclude:
        payload["exclude"] = list(exclude)
    return payload or None


def _range_payload(
    min_value: Optional[int], max_value: Optional[int]
) -> Optional[dict[str, Any]]:
    """Build a Blitz RangeFilter {min, max} from optional bounds."""
    payload: dict[str, Any] = {}
    if min_value is not None:
        payload["min"] = min_value
    if max_value is not None:
        payload["max"] = max_value
    return payload or None


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------

class TamCompanyFilters(BaseModel):
    """Company-side TAM filters — maps 1:1 onto the Blitz CompanyFilter
    surface accepted by ``/v2/company/tam-by-people``.

    Every field is optional; ``to_payload()`` emits only the set ones.
    ``extra="forbid"`` so unknown keys 422 instead of being forwarded.
    """
    model_config = ConfigDict(extra="forbid")

    # Restrict the TAM to specific company LinkedIn pages (max 50).
    linkedin_url: Optional[list[str]] = Field(None, max_length=50)
    # Company-name keyword include/exclude.
    name_include: Optional[list[str]] = None
    name_exclude: Optional[list[str]] = None
    # Fixed industry taxonomy include/exclude.
    industry_include: Optional[list[str]] = None
    industry_exclude: Optional[list[str]] = None
    # Company type include/exclude (e.g. "Privately Held", "Public Company").
    type_include: Optional[list[str]] = None
    type_exclude: Optional[list[str]] = None
    # LinkedIn size-band labels (e.g. "11-50", "51-200").
    employee_range: Optional[list[str]] = None
    # Exact headcount bounds (numeric; use employee_range for bands).
    employee_count_min: Optional[int] = Field(None, ge=0)
    employee_count_max: Optional[int] = Field(None, ge=0)
    # Minimum LinkedIn follower count.
    min_linkedin_followers: Optional[int] = Field(None, ge=0)
    # Estimated revenue bounds (USD).
    revenue_min: Optional[int] = Field(None, ge=0)
    revenue_max: Optional[int] = Field(None, ge=0)
    # Free-text keyword include/exclude over the company profile.
    keywords_include: Optional[list[str]] = None
    keywords_exclude: Optional[list[str]] = None
    # Founding year bounds.
    founded_year_min: Optional[int] = Field(None, ge=0)
    founded_year_max: Optional[int] = Field(None, ge=0)
    # Headquarters location filters (nested under hq in the Blitz payload).
    hq_country_code: Optional[list[str]] = None
    hq_continent: Optional[list[str]] = None
    hq_sales_region: Optional[list[str]] = None

    def to_payload(self) -> dict[str, Any]:
        """Build the Blitz CompanyFilter dict from the whitelisted fields."""
        payload: dict[str, Any] = {}
        if self.linkedin_url:
            payload["linkedin_url"] = list(self.linkedin_url)
        for include, exclude, key in (
            (self.name_include, self.name_exclude, "name"),
            (self.industry_include, self.industry_exclude, "industry"),
            (self.type_include, self.type_exclude, "type"),
            (self.keywords_include, self.keywords_exclude, "keywords"),
        ):
            keyword = _keyword_payload(include, exclude)
            if keyword:
                payload[key] = keyword
        if self.employee_range:
            payload["employee_range"] = list(self.employee_range)
        for min_value, max_value, key in (
            (self.employee_count_min, self.employee_count_max, "employee_count"),
            (self.revenue_min, self.revenue_max, "revenue"),
            (self.founded_year_min, self.founded_year_max, "founded_year"),
        ):
            rng = _range_payload(min_value, max_value)
            if rng:
                payload[key] = rng
        if self.min_linkedin_followers is not None:
            payload["min_linkedin_followers"] = self.min_linkedin_followers
        hq: dict[str, Any] = {}
        if self.hq_country_code:
            hq["country_code"] = list(self.hq_country_code)
        if self.hq_continent:
            hq["continent"] = list(self.hq_continent)
        if self.hq_sales_region:
            hq["sales_region"] = list(self.hq_sales_region)
        if hq:
            payload["hq"] = hq
        return payload


class TamPeopleFilters(BaseModel):
    """People-side TAM filters — maps 1:1 onto the Blitz PeopleFilter surface
    accepted by ``/v2/company/tam-by-people``.

    Every field is optional; ``to_payload()`` emits only the set ones.
    ``extra="forbid"`` so unknown keys 422 instead of being forwarded.
    """
    model_config = ConfigDict(extra="forbid")

    # Job-title include/exclude. Wrap-free plain tokens are fuzzy; the
    # request-level exact_titles flag bracket-wraps them server-side.
    job_title_include: Optional[list[str]] = None
    job_title_exclude: Optional[list[str]] = None
    # Seniority levels — must be one of the Blitz job_level taxonomy.
    job_levels: Optional[list[str]] = None
    # Drop companies with fewer matched people than this (0-25).
    min_per_company: Optional[int] = Field(None, ge=0, le=25)
    # Person-location country codes (nested under location).
    location_country_code: Optional[list[str]] = None

    @field_validator("job_levels")
    @classmethod
    def _validate_job_levels(cls, value: Optional[list[str]]) -> Optional[list[str]]:
        """Reject unknown seniority values with a clear 422."""
        if value is None:
            return None
        invalid = [level for level in value if level not in _VALID_JOB_LEVELS]
        if invalid:
            raise ValueError(
                f"job_levels must be from {sorted(_VALID_JOB_LEVELS)}; got {invalid}"
            )
        return value

    def to_payload(self) -> dict[str, Any]:
        """Build the Blitz PeopleFilter dict from the whitelisted fields."""
        payload: dict[str, Any] = {}
        keyword = _keyword_payload(self.job_title_include, self.job_title_exclude)
        if keyword:
            payload["job_title"] = keyword
        if self.job_levels:
            payload["job_level"] = list(self.job_levels)
        if self.min_per_company is not None:
            payload["min_per_company"] = self.min_per_company
        if self.location_country_code:
            payload["location"] = {"country_code": list(self.location_country_code)}
        return payload


class TamRequest(BaseModel):
    """Request model for POST /flows/tam (TAM-by-People, new Flow 2).

    Fields:
        company: company-side filters (TamCompanyFilters).
        people: people-side filters (TamPeopleFilters).
        max_companies: budget ceiling on companies pulled (default 1000,
            hard cap 10000). Blitz bills 1 FUP record per returned result,
            so this is a cost ceiling, not just a page limit.
        create_enrichment_job: when True and the run found companies with
            domains, a Flow-1 domain-enrichment job is chained over them
            (same creation/execution path as /flows/domain-enrich).
        titles: titles for the chained Flow-1 job (max 50). Bracket-wrapped
            for exact matching when exact_titles is True.
        max_decision_makers: max contacts per domain in the chained job.
        providers: provider allowlist for the chained job (validated against
            list_builder.VALID_PROVIDERS; None = all enabled).
        exact_titles: send titles as exact ([CEO]) matches instead of fuzzy.
    """
    company: TamCompanyFilters = Field(default_factory=TamCompanyFilters)
    people: TamPeopleFilters = Field(default_factory=TamPeopleFilters)
    max_companies: int = Field(1000, ge=1, le=10_000)
    create_enrichment_job: bool = False
    titles: Optional[list[str]] = Field(None, max_length=50)
    max_decision_makers: int = Field(5, ge=1, le=25)
    providers: Optional[list[str]] = None
    exact_titles: bool = False


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------

def _tam_display_summary(req: "TamRequest") -> str:
    """One-line plain-language summary of what was submitted, for the job's
    display_name (Jobs page title). Kept short — first filter of each kind,
    ' · ' separated."""
    parts: list[str] = []
    titles = (req.people.job_title_include or [])[:2]
    parts.append("/".join(titles) if titles else "any role")
    industry = (req.company.industry_include or [])[:1]
    parts.append(industry[0] if industry else "any industry")
    size = (req.company.employee_range or [])[:1]
    parts.append(size[0] + " emp" if size else "any size")
    country = (req.company.hq_country_code or [])[:1]
    parts.append(country[0] if country else "all countries")
    summary = " · ".join(parts)
    return summary[:100]


@router.post("/flows/tam")
async def start_tam_flow(
    req: TamRequest,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(auth.get_current_user_with_api_key),
):
    """
    Flow 2 (new): TAM-by-People — persona + firmographic company discovery.

    Pages through Blitz tam-by-people with the given filters and writes an
    incremental company CSV. Optionally chains a Flow-1 enrichment job over
    the discovered domains when ``create_enrichment_job`` is true.

    Auth: same dependency as the other flow endpoints (JWT or API key).

    Cost: 1 Blitz FUP record per returned company; an empty page costs 0.
    ``max_companies`` caps the total (default 1000, hard cap 10000).
    """
    company_filters = req.company.to_payload()
    people_filters = req.people.to_payload()
    if not company_filters and not people_filters:
        raise HTTPException(
            status_code=400,
            detail=(
                "At least one company or people filter is required — an "
                "unbounded TAM pull would bill up to max_companies Blitz "
                "records with no targeting."
            ),
        )

    if req.providers:
        invalid = [p for p in req.providers if p not in list_builder.VALID_PROVIDERS]
        if invalid:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Invalid providers: {invalid}. "
                    f"Valid: {sorted(list_builder.VALID_PROVIDERS)}"
                ),
            )

    job_id = str(uuid.uuid4())
    store = job_store.get_store()
    # Human-readable identity: the Jobs page title comes from display_name
    # (what was submitted), the downloaded file from the timestamped filename.
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    store.create_enrichment_job(
        job_id=job_id,
        user_id=current_user["user_id"],
        # total is the budget ceiling — the real count is unknown until the
        # cursor runs dry; processed tracks pages fetched.
        total=req.max_companies,
        filename=f"find_companies_{stamp}.csv",
        domain_col="domain",
        original_filename=f"find_companies_{stamp}.csv",
        max_results=req.max_decision_makers,
        selected_providers=req.providers,
        source_type="tam_flow",
        display_name=f"Find Companies — {_tam_display_summary(req)}",
    )

    _job_signals[job_id] = asyncio.Event()
    _active_jobs.add(job_id)

    def check_cancelled() -> bool:
        if job_id in _cancelled_jobs:
            return True
        return job_store.get_store().is_job_cancelled_or_abandoned(job_id)

    params: dict[str, Any] = {
        "company_filters": company_filters,
        "people_filters": people_filters,
        "max_companies": req.max_companies,
        "create_enrichment_job": req.create_enrichment_job,
        "user_id": current_user["user_id"],
        "titles": req.titles,
        "max_decision_makers": req.max_decision_makers,
        "providers": req.providers,
        "exact_titles": req.exact_titles,
        "display_name": _tam_display_summary(req),
        "should_cancel": check_cancelled,
    }

    background_tasks.add_task(_run_tam_job, job_id=job_id, params=params)

    logger.info(
        "TAM job %s queued for user %s (max_companies=%d, chain=%s)",
        job_id, current_user["user_id"], req.max_companies, req.create_enrichment_job,
    )
    return {
        "job_id": job_id,
        "flow": "tam",
        "total": req.max_companies,
    }


# ---------------------------------------------------------------------------
# Background runner (skeleton mirrors routes._run_domain_enrich_job)
# ---------------------------------------------------------------------------

async def _run_tam_job(job_id: str, params: dict[str, Any]) -> None:
    """Background task to run a TAM flow job.

    Skeleton copied from ``routes._run_domain_enrich_job``: set_running ->
    30s heartbeat loop -> on_progress with append_event + commit +
    ``_job_signals`` wake -> cancel/partial handling -> ``_mark_done_and_cleanup``.

    No contacts write-back: TAM output is companies, not contacts (the
    chained Flow-1 job does its own contacts_writer sync), so there is no
    ``_run_background_sync`` drain here.
    """
    store = job_store.get_store()
    store.set_running(job_id)
    # Initial heartbeat so the stale-job reaper doesn't mark us abandoned early.
    store.heartbeat(job_id)
    seq = [0]

    async def heartbeat_loop():
        try:
            while True:
                await asyncio.sleep(30)
                try:
                    job_store.get_store().heartbeat(job_id)
                except Exception as hb_err:
                    logger.warning("Heartbeat failed for %s: %s", job_id, hb_err)
        except asyncio.CancelledError:
            pass

    heartbeat_task = asyncio.create_task(heartbeat_loop())

    async def on_progress(event: dict[str, Any]):
        try:
            progress_store = job_store.get_store()
            progress_store.append_event(job_id, seq[0], event)
            progress_store.conn.commit()
            seq[0] += 1
            sig = _job_signals.get(job_id)
            if sig:
                sig.set()
                sig.clear()
        except Exception as prog_err:
            logger.error("Progress callback failed for job %s: %s", job_id, prog_err)

    try:
        summary = await tam_flow.run_tam_flow(job_id, params, on_progress=on_progress)

        if summary.get("status") == "cancelled":
            _cancelled_jobs.discard(job_id)
            output_path = Path(summary.get("csv_path") or "")
            has_partial = summary.get("companies_found", 0) > 0
            if has_partial and output_path.exists():
                store.set_status(job_id, "partial")
                try:
                    store.set_partial_output_path(job_id, str(output_path))
                except Exception as partial_err:
                    logger.warning(
                        "Job %s: set_partial_output_path failed: %s",
                        job_id, partial_err,
                    )
                logger.info(
                    "TAM job %s stopped mid-run: partial CSV at %s (%d companies)",
                    job_id, output_path, summary.get("companies_found", 0),
                )
            else:
                store.set_failed(job_id, "Job was cancelled by user.")
                logger.info(
                    "TAM job %s cancelled before any companies completed", job_id
                )
            return

        output_path = Path(summary["csv_path"])
        store._mark_done_and_cleanup(job_id, output_path)
        logger.info(
            "TAM job %s completed: %d companies over %d pages (csv=%s)",
            job_id, summary.get("companies_found", 0),
            summary.get("pages", 0), output_path,
        )
        if summary.get("chained_job_id"):
            logger.info(
                "TAM job %s chained Flow-1 job %s",
                job_id, summary["chained_job_id"],
            )
        elif summary.get("chain_skipped"):
            logger.info(
                "TAM job %s chain skipped: %s", job_id, summary["chain_skipped"]
            )

    except Exception as exc:
        logger.exception("TAM job %s failed: %s", job_id, exc)
        store.set_failed(job_id, f"Job failed: {exc}")
        # Best-effort: if pages already flushed, keep the CSV downloadable.
        try:
            fallback_path = tam_flow.OUTPUT_DIR / f"{job_id}.csv"
            if fallback_path.exists() and fallback_path.stat().st_size > 0:
                store.set_partial_output_path(job_id, str(fallback_path))
        except Exception as reg_err:
            logger.warning(
                "Job %s: failed to register partial output: %s", job_id, reg_err
            )

    finally:
        heartbeat_task.cancel()
        try:
            job_store.get_store().remove_job_state(job_id)
        except Exception as cleanup_err:
            logger.warning(
                "Failed to clean job state for %s: %s", job_id, cleanup_err
            )
