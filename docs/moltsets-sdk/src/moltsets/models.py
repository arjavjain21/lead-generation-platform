from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

T = TypeVar("T")


@dataclass(slots=True)
class MoltSetsResponse(Generic[T]):
    results: T | None
    status: str
    metadata: dict[str, Any] = field(default_factory=dict)
    outcome: str | None = None
    partial_error: dict[str, Any] | None = None
    headers: dict[str, str] = field(default_factory=dict)
    raw: dict[str, Any] = field(default_factory=dict)

    @property
    def found(self) -> bool:
        return self.status == "ok"

    @property
    def not_found(self) -> bool:
        return self.status == "not_found"

    @property
    def retry_after(self) -> float | None:
        value = self.metadata.get("retry_after")
        if value is None:
            value = self.headers.get("retry-after")
        try:
            return float(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    @property
    def fair_use(self) -> dict[str, Any]:
        return self.metadata.get("fair_use", {}) or {}
