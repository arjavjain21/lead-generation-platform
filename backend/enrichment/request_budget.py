"""Per-request cascade deadline for interactive enrichment lookups.

2026-10-15 RCA: ``/api/enrichment/enrich`` had no per-request ceiling. A cold
domain cascading through 8 provider tiers (with retry backoffs) measured
56s typical / 132s worst-case while calling clients aborted (nginx logged
1,038 HTTP 499s in 3h). This module gives every interactive request a wall
clock budget (``ENRICH_REQUEST_BUDGET_S``, default 20s, ``0`` disables).
Each cascade tier consults :func:`budget_expired` via the three mirror
``_should_skip_provider`` gates (pipeline / list_builder / routes) and skips
to returning partial results once the budget is spent.

Background jobs (CSV Flow-1/3, google_maps_chain, runner auto-resume) never
start a budget, so their per-row deep cascades are unaffected. ContextVar
scoping means the deadline is confined to the HTTP request task that set it
(sub-tasks spawned for the same request inherit it, separate requests and
background jobs do not).
"""

from __future__ import annotations

import logging
from contextvars import ContextVar
from time import monotonic
from typing import Optional, Union

from shared.env_utils import env_float

logger = logging.getLogger(__name__)

_DEFAULT_BUDGET_S = env_float("ENRICH_REQUEST_BUDGET_S", "20")

_deadline_var: ContextVar[Optional[float]] = ContextVar(
    "enrich_request_deadline", default=None
)


def start_request_budget(seconds: Optional[float] = None) -> bool:
    """Arm the deadline for the current request task.

    Args:
        seconds: Budget in seconds. ``None`` uses ``ENRICH_REQUEST_BUDGET_S``
            (default 20). Values ``<= 0`` disable the budget entirely.

    Returns:
        True if a deadline was armed, False if budgets are disabled.
    """
    budget = _DEFAULT_BUDGET_S if seconds is None else float(seconds)
    if budget <= 0:
        return False
    _deadline_var.set(monotonic() + budget)
    return True


def clear_request_budget() -> None:
    """Disarm any deadline set in the current context.

    Needed by tests (pytest runs test functions in one thread, so a ContextVar
    set in one test otherwise leaks into the next) and by any long-lived task
    that wants to explicitly drop its budget.
    """
    _deadline_var.set(None)


def budget_expired() -> bool:
    """Whether the current request's cascade budget is spent.

    Always False when no budget is armed (background jobs, tests, routing
    diagnostics).
    """
    deadline = _deadline_var.get()
    return deadline is not None and monotonic() > deadline


def budget_remaining_s() -> Union[float, float]:
    """Seconds left on the current budget (``inf`` when unarmed)."""
    deadline = _deadline_var.get()
    if deadline is None:
        return float("inf")
    return max(0.0, deadline - monotonic())
