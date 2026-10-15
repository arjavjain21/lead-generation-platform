"""Regression tests for the 2026-10-15 latency RCA fixes.

Covers the three shipped guards:
1. ``company_fallback.better_enrich_allowed`` honors the ENABLE_BETTER_ENRICH
   env kill switch — the dict-only check leaked ~9.6K dead-key 403s/day while
   ENABLE_BETTER_ENRICH=false (2026-10-15 RCA, cause #1).
2. ``request_budget`` ContextVar deadline: unarmed never expires, armed
   expires, 0 disables, and task isolation keeps background jobs immune.
3. All three mirror ``_should_skip_provider`` gates (pipeline / list_builder /
   routes) skip providers once the interactive budget is spent.
4. WizLeads 429 handling: skip the tier immediately — no retry sleep (RCA
   cause #2: up to 3 backoffs x 30s inside a single request).
5. WizLeads circuit breaker opens after consecutive upstream failures.
"""

from __future__ import annotations

import asyncio
import importlib
import time

import pytest

from enrichment import request_budget
from enrichment import wizleads_client
from enrichment.company_fallback import better_enrich_allowed
from shared import rate_limiter
from shared.circuit_breaker import get_circuit_breaker


@pytest.fixture(autouse=True)
def _clean_budget_and_limiter(tmp_path):
    """Isolate the ContextVar budget and the shared limiter per test."""
    request_budget.clear_request_budget()
    rate_limiter.configure_db_path(str(tmp_path / "rate_limiter_test.db"))
    yield
    request_budget.clear_request_budget()
    rate_limiter.configure_db_path(None)


class TestBetterEnrichEnvGate:
    """The exact production leak: dict True + env false must mean OFF."""

    def test_env_false_beats_dict_true(self, monkeypatch):
        monkeypatch.setenv("ENABLE_BETTER_ENRICH", "false")
        monkeypatch.setattr(
            "enrichment.providers.ENABLED_PROVIDERS",
            {**{"better_enrich": True}},
        )
        assert better_enrich_allowed(None) is False

    def test_env_absent_dict_true_allows(self, monkeypatch):
        monkeypatch.delenv("ENABLE_BETTER_ENRICH", raising=False)
        monkeypatch.setattr(
            "enrichment.providers.ENABLED_PROVIDERS",
            {**{"better_enrich": True}},
        )
        assert better_enrich_allowed(None) is True

    def test_dict_false_blocks_even_with_env_true(self, monkeypatch):
        monkeypatch.setenv("ENABLE_BETTER_ENRICH", "true")
        monkeypatch.setattr(
            "enrichment.providers.ENABLED_PROVIDERS",
            {**{"better_enrich": False}},
        )
        assert better_enrich_allowed(None) is False


class TestRequestBudget:
    def test_unarmed_never_expires(self):
        assert request_budget.budget_expired() is False
        assert request_budget.budget_remaining_s() == float("inf")

    def test_armed_budget_expires(self):
        armed = request_budget.start_request_budget(seconds=0.05)
        assert armed is True
        assert request_budget.budget_expired() is False
        time.sleep(0.06)
        assert request_budget.budget_expired() is True
        assert request_budget.budget_remaining_s() == 0.0

    def test_zero_disables(self):
        armed = request_budget.start_request_budget(seconds=0)
        assert armed is False
        assert request_budget.budget_expired() is False

    def test_task_isolation(self):
        """A budget armed inside a spawned task must not leak to its parent —
        this is what keeps background jobs immune to interactive deadlines."""

        async def main():
            async def arms_in_child_task():
                request_budget.start_request_budget(seconds=60)
                await asyncio.sleep(0.01)
                return request_budget.budget_expired()

            child = asyncio.create_task(arms_in_child_task())
            child_expired = await child
            parent_expired = request_budget.budget_expired()
            return child_expired, parent_expired

        child_expired, parent_expired = asyncio.run(main())
        assert child_expired is False, "budget should be live inside the child"
        assert parent_expired is False, "budget must not leak to the parent"


class TestSkipGatesHonorBudget:
    """All three mirror _should_skip_provider gates skip on expired budget."""

    @pytest.mark.parametrize(
        "module_name", ["pipeline", "list_builder", "routes"]
    )
    def test_expired_budget_skips_provider(self, module_name):
        mod = importlib.import_module(f"enrichment.{module_name}")
        request_budget.start_request_budget(seconds=0.05)
        time.sleep(0.06)
        assert mod._should_skip_provider("blitz") is True
        assert mod._should_skip_provider("wizleads") is True

    def test_unarmed_budget_does_not_skip(self):
        from enrichment import pipeline as pl

        assert pl._should_skip_provider("blitz") is False


class _StubResponse:
    def __init__(self, status_code: int):
        self.status_code = status_code
        self.headers = {}
        self.text = ""


class _StubClient:
    """Minimal httpx.AsyncClient stand-in: records calls, returns canned status."""

    def __init__(self, status_code: int):
        self.status_code = status_code
        self.calls = 0

    async def get(self, *args, **kwargs):
        self.calls += 1
        return _StubResponse(self.status_code)


class TestWizLeads429Skip:
    def test_429_returns_none_without_retry(self, monkeypatch):
        monkeypatch.setattr(wizleads_client, "API_KEY", "test-key")
        monkeypatch.setattr(
            wizleads_client,
            "_wizleads_circuit",
            get_circuit_breaker("wizleads_test_429_skip"),
        )
        stub = _StubClient(429)

        started = time.monotonic()
        result = asyncio.run(
            wizleads_client.find_email(stub, "Patrick", "Collison", "stripe.com")
        )
        elapsed = time.monotonic() - started

        assert result is None
        assert stub.calls == 1, "429 must NOT be retried inside one request"
        assert elapsed < 2.0, f"429 path slept {elapsed:.1f}s — skip-tier is instant"


class TestWizLeadsRetryPolicy:
    def test_429_not_retryable(self):
        assert wizleads_client._should_retry(429) is False

    def test_5xx_retryable(self):
        assert wizleads_client._should_retry(500) is True
        assert wizleads_client._should_retry(504) is True

    def test_4xx_not_retryable(self):
        assert wizleads_client._should_retry(402) is False
        assert wizleads_client._should_retry(422) is False
        assert wizleads_client._should_retry(404) is False


class TestWizLeadsCircuitBreaker:
    def test_opens_after_consecutive_failures(self, monkeypatch):
        test_breaker = get_circuit_breaker(
            "wizleads_test_breaker_open",
            failure_threshold=3,
            recovery_timeout=60.0,
            half_open_max_calls=1,
        )
        monkeypatch.setattr(wizleads_client, "_wizleads_circuit", test_breaker)
        monkeypatch.setattr(wizleads_client, "API_KEY", "test-key")

        async def drain_failures():
            for _ in range(3):
                await test_breaker.record_failure()
            return await test_breaker.can_proceed()

        assert asyncio.run(drain_failures()) is False

        # With the breaker open, find_email fails fast without any HTTP call.
        stub = _StubClient(200)
        result = asyncio.run(
            wizleads_client.find_email(stub, "Patrick", "Collison", "stripe.com")
        )
        assert result is None
        assert stub.calls == 0, "open breaker must prevent the HTTP call"
