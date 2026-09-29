"""Thin async client for the webscrapedash scrape-emails API.

The scraper VPS (ssh alias ``webscraper-vps``) exposes a programmatic surface at
``https://webscrapedash.eagleinfoservice.com`` (docs: /opt/enrichment-api/API.md
on that box). This client covers exactly what the website-email followup flow
needs — estimate, submit, poll, paged results, cancel — and nothing else.

Design pins (enrichment/tests/test_webscrape_dash_client.py):
* Auth: ``Authorization: Bearer <WEBSCRAPER_API_KEY>`` (sk_live_…). The key
  lives in backend/.env; ``is_configured()`` gates every caller so an unset
  key degrades to "followup disabled", never to a crashing enrichment job.
* Idempotency: callers pass a deterministic Idempotency-Key (derived from the
  parent job id) so a retry after a network blip never double-submits.
* Backpressure: 429/503 are retried up to MAX_AUTO_RETRIES honouring the
  ``Retry-After`` header (seconds); exhaustion raises WebscrapeDashBusy so the
  followup poller can retry the whole batch later.
* Batch size: their hard cap is 10,000 websites per submit — callers chunk.
* Never raises bare httpx errors: everything is mapped to WebscrapeDashError
  subclasses carrying the response snippet for logs.

Env vars:
* WEBSCRAPER_API_BASE  — API origin (default https://webscrapedash.eagleinfoservice.com)
* WEBSCRAPER_API_KEY   — sk_live_… bearer key ('leadgen-prod' on their box)
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any, Optional

import httpx

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://webscrapedash.eagleinfoservice.com"
SUBMIT_PATH = "/api/scrape-emails"
ESTIMATE_PATH = "/api/scrape-emails/estimate"

#: Their documented per-request cap (API.md "Submit websites").
MAX_WEBSITES_PER_BATCH = 10_000

#: Automatic retries for 429/503 (backpressure). Beyond this the caller's
#: poller/retry loop takes over with much longer delays.
MAX_AUTO_RETRIES = 3

#: Cap on a single Retry-After sleep so a hostile/buggy header can't stall a
#: worker for minutes.
MAX_RETRY_AFTER_SLEEP_S = 30.0

_STATUS_ERR_SNIPPET = 300


class WebscrapeDashError(Exception):
    """Base error — message is safe for logs and job error columns."""


class WebscrapeDashAuthError(WebscrapeDashError):
    """401/403 — key missing, revoked, or wrong scope."""


class WebscrapeDashBusy(WebscrapeDashError):
    """429/503 persisted past retries. ``retry_after_s`` carries the hint."""

    def __init__(self, message: str, retry_after_s: Optional[float] = None):
        super().__init__(message)
        self.retry_after_s = retry_after_s


def is_configured() -> bool:
    """True when WEBSCRAPER_API_KEY is present and non-empty."""
    return bool(os.getenv("WEBSCRAPER_API_KEY", "").strip())


def api_base_url() -> str:
    return os.getenv("WEBSCRAPER_API_BASE", DEFAULT_BASE_URL).rstrip("/")


def _err_snippet(response: httpx.Response) -> str:
    try:
        return response.text[:_STATUS_ERR_SNIPPET]
    except Exception:  # noqa: BLE001 — never mask the original failure
        return "<unreadable body>"


class WebscrapeDashClient:
    """Stateless-per-call async client. Use as an async context manager, or
    call the coroutine methods directly (a short-lived client is created per
    call — volumes here are one submit + a poll per 30s, nowhere near the
    300 req/min key limit)."""

    def __init__(
        self,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        timeout_s: float = 30.0,
    ):
        self._base_url = (base_url or api_base_url()).rstrip("/")
        self._api_key = (api_key or os.getenv("WEBSCRAPER_API_KEY", "")).strip()
        if not self._api_key:
            raise WebscrapeDashAuthError(
                "WEBSCRAPER_API_KEY is not configured — website-email followup disabled"
            )
        self._timeout_s = timeout_s

    # -- transport ---------------------------------------------------------

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: Optional[dict[str, Any]] = None,
        params: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> dict[str, Any]:
        """One request with bounded 429/503 retry. Returns parsed JSON."""
        merged_headers = {"Authorization": f"Bearer {self._api_key}"}
        if headers:
            merged_headers.update(headers)
        attempt = 0
        last_busy: Optional[WebscrapeDashBusy] = None
        while True:
            try:
                async with httpx.AsyncClient(timeout=self._timeout_s) as client:
                    response = await client.request(
                        method,
                        f"{self._base_url}{path}",
                        json=json_body,
                        params=params,
                        headers=merged_headers,
                    )
            except httpx.HTTPError as exc:
                raise WebscrapeDashError(f"webscrapedash transport error: {exc}") from exc

            if response.status_code in (429, 503):
                retry_after = _parse_retry_after(response.headers.get("Retry-After"))
                last_busy = WebscrapeDashBusy(
                    f"webscrapedash backpressure {response.status_code}: "
                    f"{_err_snippet(response)}",
                    retry_after_s=retry_after,
                )
                if attempt >= MAX_AUTO_RETRIES:
                    raise last_busy
                sleep_s = min(retry_after if retry_after else 2.0 * (attempt + 1),
                              MAX_RETRY_AFTER_SLEEP_S)
                await asyncio.sleep(sleep_s)
                attempt += 1
                continue

            if response.status_code in (401, 403):
                raise WebscrapeDashAuthError(
                    f"webscrapedash auth failed {response.status_code}: {_err_snippet(response)}"
                )
            if response.status_code >= 400:
                raise WebscrapeDashError(
                    f"webscrapedash {method} {path} -> {response.status_code}: "
                    f"{_err_snippet(response)}"
                )
            try:
                return response.json()
            except ValueError as exc:
                raise WebscrapeDashError(
                    f"webscrapedash returned non-JSON for {path}: {_err_snippet(response)}"
                ) from exc

    # -- API surface -------------------------------------------------------

    async def estimate(
        self, websites: list[str], *, client_tag: Optional[str] = None
    ) -> dict[str, Any]:
        """Dry-run quote. Free; never queues anything."""
        body: dict[str, Any] = {"websites": websites}
        if client_tag:
            body["client"] = client_tag
        return await self._request("POST", ESTIMATE_PATH, json_body=body)

    async def submit(
        self,
        websites: list[str],
        *,
        client_tag: str,
        idempotency_key: str,
        callback_url: Optional[str] = None,
    ) -> dict[str, Any]:
        """Submit one batch (≤ MAX_WEBSITES_PER_BATCH — callers chunk)."""
        if len(websites) > MAX_WEBSITES_PER_BATCH:
            raise WebscrapeDashError(
                f"batch of {len(websites)} exceeds per-request cap "
                f"{MAX_WEBSITES_PER_BATCH}; caller must chunk"
            )
        body: dict[str, Any] = {
            "websites": websites,
            "client": client_tag,
        }
        if callback_url:
            body["callback_url"] = callback_url
        return await self._request(
            "POST",
            SUBMIT_PATH,
            json_body=body,
            headers={"Idempotency-Key": idempotency_key},
        )

    async def get_job(
        self,
        job_id: str,
        *,
        cursor: Optional[str] = None,
        limit: int = 1000,
    ) -> dict[str, Any]:
        """Progress + one page of results for a batch."""
        params: dict[str, Any] = {"limit": limit}
        if cursor:
            params["cursor"] = cursor
        return await self._request("GET", f"{SUBMIT_PATH}/{job_id}", params=params)

    async def collect_results(
        self, job_id: str, *, page_limit: int = 1000, max_rows: int = 200_000
    ) -> list[dict[str, Any]]:
        """Page through every result row of a batch (cursor = last domain)."""
        results: list[dict[str, Any]] = []
        cursor: Optional[str] = None
        while True:
            page = await self.get_job(job_id, cursor=cursor, limit=page_limit)
            results.extend(page.get("results") or [])
            cursor = page.get("next_cursor")
            if not cursor:
                break
            if len(results) >= max_rows:
                logger.warning(
                    "webscrapedash results for %s truncated at %d rows", job_id, len(results)
                )
                break
        return results

    async def cancel(self, job_id: str) -> dict[str, Any]:
        """Cancel a batch: un-started rows are removed from their queue."""
        return await self._request("POST", f"{SUBMIT_PATH}/{job_id}/cancel")


def _parse_retry_after(raw: Optional[str]) -> Optional[float]:
    """Parse a Retry-After header that is seconds (their API) — tolerates
    HTTP-date form by falling back to None."""
    if not raw:
        return None
    try:
        return max(0.0, float(raw.strip()))
    except ValueError:
        return None
