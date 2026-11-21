"""Email Finder provider integration (terminal tier, flag-gated).

The internal Email Finder service (127.0.0.1:8910) is a final person-level
fallback after the whole paid cascade misses. It must be:
  - disabled by default (ENABLE_EMAIL_FINDER env, default false)
  - terminal (last step) in the name+domain route when armed
  - fail-open in the executor (service down = miss, never job failure)
"""

import asyncio
import os
from unittest.mock import AsyncMock, patch

import pytest

from enrichment import providers
from enrichment.pipeline import route_enrichment, VALID_PROVIDERS as PIPELINE_PROVIDERS
from enrichment.list_builder import VALID_PROVIDERS as LIST_BUILDER_PROVIDERS
from enrichment.routes import VALID_PROVIDERS as ROUTES_PROVIDERS


class TestProviderFlag:
    def test_disabled_by_default(self):
        """Hard-off: not in ENABLED_PROVIDERS until the rollout gate flips it."""
        assert not providers.is_provider_enabled("email_finder")

    def test_env_is_kill_switch_once_armed(self):
        with patch.dict(providers.ENABLED_PROVIDERS, {"email_finder": True}):
            assert providers.is_provider_enabled("email_finder")
            with patch.dict(os.environ, {"ENABLE_EMAIL_FINDER": "false"}):
                assert not providers.is_provider_enabled("email_finder")

    def test_registered_in_all_three_provider_sets(self):
        """The three mirror VALID_PROVIDERS sets must stay identical (rule 6)."""
        assert PIPELINE_PROVIDERS == LIST_BUILDER_PROVIDERS == ROUTES_PROVIDERS
        assert "email_finder" in PIPELINE_PROVIDERS


class TestRouteConstruction:
    def test_no_step_when_disabled(self):
        os.environ.pop("ENABLE_EMAIL_FINDER", None)
        route = route_enrichment(
            first_name="John", last_name="Doe", domain="acme.com",
        )
        assert route["mode"] == "name_domain"
        assert all(s["provider"] != "email_finder" for s in route["steps"])

    def test_terminal_step_when_armed(self):
        with patch.dict(providers.ENABLED_PROVIDERS, {"email_finder": True}):
            route = route_enrichment(
                first_name="John", last_name="Doe", domain="acme.com",
            )
        providers_steps = [s["provider"] for s in route["steps"]]
        assert "email_finder" in providers_steps
        assert providers_steps[-1] == "email_finder", \
            "email_finder must be the terminal tier, after wizleads"


class TestClient:
    def test_verified_hit_parsed(self):
        from enrichment import email_finder_client

        class FakeResp:
            def raise_for_status(self):
                pass

            def json(self):
                return {
                    "person": {"first_name": "john", "last_name": "doe", "domain": "acme.com"},
                    "valid_emails": [
                        {"email": "jdoe@acme.com", "pattern": "firstinitiallastname", "confidence": 0.7}
                    ],
                    "statistics": {
                        "total_tested": 2, "valid_count": 1, "invalid_count": 1,
                        "skipped_reason": None, "learned_pattern": None,
                        "hint_status": None, "hint_pattern": None,
                        "mailtester_calls": 3, "millionverifier_calls": 0,
                    },
                }

        client = AsyncMock()
        client.post.return_value = FakeResp()
        result = asyncio.run(
            email_finder_client.find_email(client, "John", "Doe", "acme.com")
        )

        assert result["email"] == "jdoe@acme.com"
        assert result["source"] == "email_finder_email"
        assert result["verified"] is True
        assert result["mailtester_calls"] == 3
        assert result["pattern"] == "firstinitiallastname"

    def test_miss_reports_skipped_reason(self):
        from enrichment import email_finder_client

        class FakeResp:
            def raise_for_status(self):
                pass

            def json(self):
                return {
                    "person": {"first_name": "jane", "last_name": "smith", "domain": "acme.com"},
                    "valid_emails": [],
                    "statistics": {
                        "total_tested": 0, "valid_count": 0, "invalid_count": 0,
                        "skipped_reason": "catch_all", "learned_pattern": None,
                        "hint_status": None, "hint_pattern": None,
                        "mailtester_calls": 1, "millionverifier_calls": 0,
                    },
                }

        client = AsyncMock()
        client.post.return_value = FakeResp()
        result = asyncio.run(
            email_finder_client.find_email(client, "Jane", "Smith", "acme.com")
        )

        assert result["email"] == ""
        assert result["skipped_reason"] == "catch_all"
