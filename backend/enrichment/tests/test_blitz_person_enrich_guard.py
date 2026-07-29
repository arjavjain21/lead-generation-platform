"""
TDD tests for disabling Blitz person_enrich (/v2/enrichment/person).

Telemetry (24h, by endpoint) showed /v2/enrichment/person returns 422/401 on
EVERY call — 19,616 failures, ZERO successes — because (a) the endpoint requires
a `linkedin_profile_url` field and (b) our Blitz plan returns 401 "You do not
have access to this route" even with it. So person_enrich is entirely dead and
was wasting ~5 calls/sec. The working Blitz paths are /v2/enrichment/email
(person_enrich_by_linkedin), /domain-to-linkedin, and /search/waterfall-icp-keyword.

Fix: person_enrich returns not-found WITHOUT calling /person unless explicitly
re-enabled via BLITZ_PERSON_ENRICH=1 (e.g. after a Blitz plan upgrade). When
enabled, the linkedin payload uses the correct field name `linkedin_profile_url`.
"""
from __future__ import annotations

import asyncio
import os
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest  # noqa: F401

_BE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BE not in sys.path:
    sys.path.insert(0, _BE)

from enrichment import blitz_client  # noqa: E402


class TestPersonEnrichDisabledByDefault:
    def test_no_call_for_any_input_without_env_flag(self):
        # /person is inaccessible on our plan -> skip for ALL inputs by default.
        with patch.object(blitz_client, "_post_with_retry",
                          AsyncMock(return_value={"found": True})) as post:
            for kwargs in (
                {"domain": "acme.com"},
                {"full_name": "Jane Doe", "domain": "acme.com"},
                {"linkedin_url": "https://www.linkedin.com/in/x"},
            ):
                result = asyncio.run(blitz_client.person_enrich(MagicMock(), **kwargs))
                assert result.get("found") is False, f"expected not-found for {kwargs}"
        post.assert_not_called()


class TestPersonEnrichEnabledViaEnv:
    def test_name_plus_domain_calls_blitz_when_enabled(self):
        with patch.dict(os.environ, {"BLITZ_PERSON_ENRICH": "1"}), \
                patch.object(blitz_client, "_post_with_retry",
                             AsyncMock(return_value={"found": False})) as post:
            asyncio.run(
                blitz_client.person_enrich(MagicMock(), full_name="Jane Doe", domain="acme.com")
            )
        post.assert_called_once()
        payload = post.call_args.args[2]  # _post_with_retry(client, url, payload, timeout)
        assert payload.get("first_name") == "Jane"
        assert payload.get("domain") == "acme.com"

    def test_linkedin_uses_correct_field_name_when_enabled(self):
        # Blitz requires `linkedin_profile_url` (not `linkedin_url`).
        with patch.dict(os.environ, {"BLITZ_PERSON_ENRICH": "1"}), \
                patch.object(blitz_client, "_post_with_retry",
                             AsyncMock(return_value={"found": False})) as post:
            asyncio.run(
                blitz_client.person_enrich(MagicMock(), linkedin_url="https://www.linkedin.com/in/x")
            )
        post.assert_called_once()
        payload = post.call_args.args[2]
        assert payload.get("linkedin_profile_url") == "https://www.linkedin.com/in/x"
