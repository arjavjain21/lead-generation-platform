from __future__ import annotations

import os
import time
import warnings
from collections.abc import Sequence
from typing import Any

import httpx

from .exceptions import MoltSetsAPIError, MoltSetsError
from .manifest import BASE_URL
from .models import MoltSetsResponse

ScalarOrList = str | Sequence[str]


def _clean_payload(payload: dict[str, Any]) -> dict[str, Any]:
    cleaned: dict[str, Any] = {}
    for key, value in payload.items():
        if value is None:
            continue
        if isinstance(value, tuple):
            value = list(value)
        cleaned[key] = value
    return cleaned


class _EndpointsMixin:
    """Endpoint surface shared by sync and async clients."""

    # Search
    def search_people(
        self,
        *,
        query: str | None = None,
        title: ScalarOrList | None = None,
        company: ScalarOrList | None = None,
        company_domain: ScalarOrList | None = None,
        country: ScalarOrList | None = None,
        state: ScalarOrList | None = None,
        city: ScalarOrList | None = None,
        seniority: ScalarOrList | None = None,
        industry: ScalarOrList | None = None,
        department: ScalarOrList | None = None,
        functional_area: ScalarOrList | None = None,
        employee_range: ScalarOrList | None = None,
        revenue_range: ScalarOrList | None = None,
        naics_code: ScalarOrList | None = None,
        linkedin_industry: ScalarOrList | None = None,
        exclude_company_domain: Sequence[str] | None = None,
        exclude_country: Sequence[str] | None = None,
        exclude_state: Sequence[str] | None = None,
        exclude_industry: Sequence[str] | None = None,
        exclude_seniority: Sequence[str] | None = None,
        exclude_employee_range: Sequence[str] | None = None,
        limit: int = 10,
        offset: int = 0,
    ):
        return self._request("search_people", _clean_payload(locals() | {}))

    def search_companies(
        self,
        *,
        query: str | None = None,
        domain: ScalarOrList | None = None,
        industry: ScalarOrList | None = None,
        employee_range: ScalarOrList | None = None,
        revenue_range: ScalarOrList | None = None,
        naics_code: ScalarOrList | None = None,
        linkedin_industry: ScalarOrList | None = None,
        country: ScalarOrList | None = None,
        state: ScalarOrList | None = None,
        exclude_domain: Sequence[str] | None = None,
        exclude_industry: Sequence[str] | None = None,
        exclude_employee_range: Sequence[str] | None = None,
        exclude_revenue_range: Sequence[str] | None = None,
        limit: int = 10,
        offset: int = 0,
    ):
        return self._request("search_companies", _clean_payload(locals() | {}))

    def search_linkedin_profile(
        self,
        *,
        name: str | None = None,
        first_name: str | None = None,
        last_name: str | None = None,
        company: str | None = None,
        company_domain: str | None = None,
        count_only: bool = False,
    ):
        return self._request("search_linkedin_profile", _clean_payload(locals() | {}))

    def search_business_email_by_name(
        self,
        *,
        name: str | None = None,
        first_name: str | None = None,
        last_name: str | None = None,
        company: str | None = None,
        company_domain: str | None = None,
    ):
        return self._request("search_business_email_by_name", _clean_payload(locals() | {}))

    def search_business_profile_by_name(
        self,
        *,
        name: str | None = None,
        first_name: str | None = None,
        last_name: str | None = None,
        company: str | None = None,
        company_domain: str | None = None,
    ):
        return self._request("search_business_profile_by_name", _clean_payload(locals() | {}))

    # Emails
    def linkedin_to_best_email(self, *, linkedin_url: str | None = None, linkedin_urls: Sequence[str] | None = None):
        return self._request("linkedin_to_best_email", _clean_payload(locals() | {}))

    def linkedin_to_business_email(self, *, linkedin_url: str | None = None, linkedin_urls: Sequence[str] | None = None):
        return self._request("linkedin_to_business_email", _clean_payload(locals() | {}))

    def linkedin_to_personal_email_usa(self, *, linkedin_url: str | None = None, linkedin_urls: Sequence[str] | None = None):
        return self._request("linkedin_to_personal_email_usa", _clean_payload(locals() | {}))

    def linkedin_to_best_personal_email_usa(self, *, linkedin_url: str | None = None, linkedin_urls: Sequence[str] | None = None):
        return self._request("linkedin_to_best_personal_email_usa", _clean_payload(locals() | {}))

    def linkedin_to_mobile_phone(self, *, linkedin_url: str | None = None, linkedin_urls: Sequence[str] | None = None):
        return self._request("linkedin_to_mobile_phone", _clean_payload(locals() | {}))

    # Reverse lookups
    def reverse_email_lookup(self, *, email: str):
        return self._request("reverse_email_lookup", {"email": email})

    def reverse_linkedin_lookup(self, *, linkedin_url: str):
        return self._request("reverse_linkedin_lookup", {"linkedin_url": linkedin_url})

    # Ad audience
    def email_to_maid(self, *, email: str):
        return self._request("email_to_maid", {"email": email})

    def business_email_to_sha256(self, *, email: str):
        return self._request("business_email_to_sha256", {"email": email})

    def linkedin_to_sha256(self, *, linkedin_url: str):
        return self._request("linkedin_to_sha256", {"linkedin_url": linkedin_url})

    # LinkedIn / company
    def email_to_linkedin(self, *, email: str):
        return self._request("email_to_linkedin", {"email": email})

    def ip_to_company(self, *, ip_address: str):
        return self._request("ip_to_company", {"ip_address": ip_address})

    # Account
    def get_account(self):
        return self._request("get_account", {})

    def get_billing(self):
        return self._request("get_billing", {})

    def get_usage(self, *, period: str = "today"):
        return self._request("get_usage", {"period": period})

    # Deprecated documented route aliases, intentionally kept for migration completeness.
    def linkedin_to_personal_email(self, *, linkedin_url: str | None = None, linkedin_urls: Sequence[str] | None = None):
        warnings.warn(
            "linkedin_to_personal_email is deprecated by MoltSets; use linkedin_to_personal_email_usa",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._request("linkedin_to_personal_email", _clean_payload(locals() | {}))

    def linkedin_to_best_personal_email(self, *, linkedin_url: str | None = None, linkedin_urls: Sequence[str] | None = None):
        warnings.warn(
            "linkedin_to_best_personal_email is deprecated by MoltSets; use linkedin_to_best_personal_email_usa",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._request("linkedin_to_best_personal_email", _clean_payload(locals() | {}))


class MoltSetsClient(_EndpointsMixin):
    def __init__(
        self,
        api_key: str | None = None,
        *,
        base_url: str = BASE_URL,
        user_agent: str = "moltsets-sdk/0.1.0",
        timeout: float = 30.0,
        max_retries: int = 3,
        max_retry_after: float = 30.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.api_key = api_key or os.getenv("MOLTSETS_API_KEY")
        if not self.api_key:
            raise MoltSetsError("Missing API key. Pass api_key=... or set MOLTSETS_API_KEY.")
        self.base_url = base_url.rstrip("/")
        self.user_agent = user_agent
        self.max_retries = max_retries
        self.max_retry_after = max_retry_after
        self._client = httpx.Client(timeout=timeout, transport=transport)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def close(self) -> None:
        self._client.close()

    def call(self, tool: str, /, **payload: Any) -> MoltSetsResponse[Any]:
        """Call any MoltSets tool, including future tools not yet wrapped by this SDK."""
        return self._request(tool, payload)

    def _request(self, tool: str, payload: dict[str, Any]) -> MoltSetsResponse[Any]:
        payload = {k: v for k, v in _clean_payload(payload).items() if k != "self"}
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "User-Agent": self.user_agent,
        }
        url = f"{self.base_url}/{tool}"
        last_error: MoltSetsAPIError | None = None

        for attempt in range(self.max_retries + 1):
            try:
                response = self._client.post(url, json=payload, headers=headers)
            except httpx.HTTPError as exc:
                if attempt >= self.max_retries:
                    raise MoltSetsError(f"Network error calling {tool}: {exc}") from exc
                time.sleep(min(0.2 * (2**attempt), self.max_retry_after))
                continue

            try:
                body = response.json()
            except ValueError:
                body = {}

            if 200 <= response.status_code < 300:
                status = body.get("status", "ok")
                return MoltSetsResponse(
                    results=body.get("results"),
                    status=status,
                    metadata=body.get("metadata") or {},
                    outcome=response.headers.get("X-MoltSets-Outcome"),
                    partial_error=body.get("error"),
                    headers={k.lower(): v for k, v in response.headers.items()},
                    raw=body,
                )

            error = body.get("error") or {}
            code = error.get("code") or f"http_{response.status_code}"
            message = error.get("message") or response.text or "MoltSets request failed"
            metadata = body.get("metadata") or {}
            retry_header = response.headers.get("Retry-After")
            retry_after = metadata.get("retry_after", retry_header)
            try:
                retry_after_f = float(retry_after) if retry_after is not None else None
            except (TypeError, ValueError):
                retry_after_f = None

            last_error = MoltSetsAPIError(
                code=code,
                message=message,
                status_code=response.status_code,
                retry_after=retry_after_f,
                metadata=metadata,
                body=body,
            )

            retryable = code in {"rate_limited", "internal_error"}
            if code in {"fair_use_limit_exceeded", "fair_use_requests_exceeded"}:
                retryable = retry_after_f is not None and retry_after_f <= self.max_retry_after

            if not retryable or attempt >= self.max_retries:
                raise last_error

            delay = retry_after_f if retry_after_f is not None else min(0.2 * (2**attempt), self.max_retry_after)
            if delay > self.max_retry_after:
                raise last_error
            time.sleep(max(0.0, delay))

        assert last_error is not None
        raise last_error


class AsyncMoltSetsClient(_EndpointsMixin):
    def __init__(
        self,
        api_key: str | None = None,
        *,
        base_url: str = BASE_URL,
        user_agent: str = "moltsets-sdk/0.1.0",
        timeout: float = 30.0,
        max_retries: int = 3,
        max_retry_after: float = 30.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.api_key = api_key or os.getenv("MOLTSETS_API_KEY")
        if not self.api_key:
            raise MoltSetsError("Missing API key. Pass api_key=... or set MOLTSETS_API_KEY.")
        self.base_url = base_url.rstrip("/")
        self.user_agent = user_agent
        self.max_retries = max_retries
        self.max_retry_after = max_retry_after
        self._client = httpx.AsyncClient(timeout=timeout, transport=transport)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()

    async def call(self, tool: str, /, **payload: Any) -> MoltSetsResponse[Any]:
        return await self._request(tool, payload)

    async def _request(self, tool: str, payload: dict[str, Any]) -> MoltSetsResponse[Any]:
        import asyncio

        payload = {k: v for k, v in _clean_payload(payload).items() if k != "self"}
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "User-Agent": self.user_agent,
        }
        url = f"{self.base_url}/{tool}"
        last_error: MoltSetsAPIError | None = None

        for attempt in range(self.max_retries + 1):
            try:
                response = await self._client.post(url, json=payload, headers=headers)
            except httpx.HTTPError as exc:
                if attempt >= self.max_retries:
                    raise MoltSetsError(f"Network error calling {tool}: {exc}") from exc
                await asyncio.sleep(min(0.2 * (2**attempt), self.max_retry_after))
                continue

            try:
                body = response.json()
            except ValueError:
                body = {}

            if 200 <= response.status_code < 300:
                return MoltSetsResponse(
                    results=body.get("results"),
                    status=body.get("status", "ok"),
                    metadata=body.get("metadata") or {},
                    outcome=response.headers.get("X-MoltSets-Outcome"),
                    partial_error=body.get("error"),
                    headers={k.lower(): v for k, v in response.headers.items()},
                    raw=body,
                )

            error = body.get("error") or {}
            code = error.get("code") or f"http_{response.status_code}"
            message = error.get("message") or response.text or "MoltSets request failed"
            metadata = body.get("metadata") or {}
            retry_header = response.headers.get("Retry-After")
            retry_after = metadata.get("retry_after", retry_header)
            try:
                retry_after_f = float(retry_after) if retry_after is not None else None
            except (TypeError, ValueError):
                retry_after_f = None

            last_error = MoltSetsAPIError(
                code=code,
                message=message,
                status_code=response.status_code,
                retry_after=retry_after_f,
                metadata=metadata,
                body=body,
            )
            retryable = code in {"rate_limited", "internal_error"}
            if code in {"fair_use_limit_exceeded", "fair_use_requests_exceeded"}:
                retryable = retry_after_f is not None and retry_after_f <= self.max_retry_after
            if not retryable or attempt >= self.max_retries:
                raise last_error
            delay = retry_after_f if retry_after_f is not None else min(0.2 * (2**attempt), self.max_retry_after)
            if delay > self.max_retry_after:
                raise last_error
            await asyncio.sleep(max(0.0, delay))

        assert last_error is not None
        raise last_error
