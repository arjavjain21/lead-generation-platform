from __future__ import annotations

from typing import Any


class MoltSetsError(Exception):
    """Base SDK exception."""


class MoltSetsAPIError(MoltSetsError):
    def __init__(
        self,
        *,
        code: str,
        message: str,
        status_code: int | None = None,
        retry_after: float | None = None,
        metadata: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
        self.status_code = status_code
        self.retry_after = retry_after
        self.metadata = metadata or {}
        self.body = body or {}

    @property
    def is_retryable(self) -> bool:
        return self.code in {
            "rate_limited",
            "fair_use_limit_exceeded",
            "fair_use_requests_exceeded",
            "internal_error",
        }
