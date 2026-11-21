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
    def test_armed_with_kill_switch(self):
        """Armed since the approved 2026-11-21 canary; env kill switch disables."""
        assert providers.is_provider_enabled("email_finder")
        with patch.dict(os.environ, {"ENABLE_EMAIL_FINDER": "false"}):
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
    def test_no_step_when_kill_switched(self):
        with patch.dict(os.environ, {"ENABLE_EMAIL_FINDER": "false"}):
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


class TestCanaryGate:
    def test_bucket_is_deterministic(self):
        from enrichment.providers import email_finder_canary_bucket
        a = email_finder_canary_bucket("john|doe|acme.com")
        b = email_finder_canary_bucket("john|doe|acme.com")
        assert a == b and 0 <= a < 100

    def test_canary_off_by_default(self):
        import os
        from enrichment.providers import email_finder_rollout_allows
        os.environ.pop("EMAIL_FINDER_CANARY_PERCENT", None)
        with patch.dict(providers.ENABLED_PROVIDERS, {"email_finder": True}):
            # even enabled, percent default 0 -> nobody passes
            assert not email_finder_rollout_allows("anyone")

    def test_five_percent_cohort_boundary(self):
        from enrichment.providers import email_finder_canary_bucket, email_finder_rollout_allows
        with patch.dict(providers.ENABLED_PROVIDERS, {"email_finder": True}), \
             patch.dict({"EMAIL_FINDER_CANARY_PERCENT": "5"}):
            import os
            os.environ["EMAIL_FINDER_CANARY_PERCENT"] = "5"
            inside = f"cohort-member-{email_finder_canary_bucket('x')}"  # sanity var
            # deterministic members: bucket<5 pass, >=5 don't
            for i in range(200):
                ident = f"person{i}"
                b = email_finder_canary_bucket(ident)
                assert email_finder_rollout_allows(ident) == (b < 5)
            os.environ.pop("EMAIL_FINDER_CANARY_PERCENT", None)

    def test_full_percent_and_force_override(self):
        from enrichment.providers import email_finder_rollout_allows
        with patch.dict(providers.ENABLED_PROVIDERS, {"email_finder": True}):
            import os
            os.environ["EMAIL_FINDER_CANARY_PERCENT"] = "100"
            assert email_finder_rollout_allows("anyone")
            os.environ["EMAIL_FINDER_CANARY_PERCENT"] = "0"
            assert not email_finder_rollout_allows("anyone")
            # force_provider bypasses the canary but not the kill switch
            assert email_finder_rollout_allows("anyone", force_provider="email_finder")
            os.environ.pop("EMAIL_FINDER_CANARY_PERCENT", None)

    def test_kill_switch_beats_everything(self):
        from enrichment.providers import email_finder_rollout_allows
        import os
        with patch.dict(providers.ENABLED_PROVIDERS, {"email_finder": True}), \
             patch.dict(os.environ, {"ENABLE_EMAIL_FINDER": "false",
                                     "EMAIL_FINDER_CANARY_PERCENT": "100"}):
            assert not email_finder_rollout_allows("anyone")
            assert not email_finder_rollout_allows("anyone", force_provider="email_finder")
