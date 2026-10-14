"""
Tests for ``response_normalizer.normalize_moltsets_contact`` (2026-10-14).

Covers:
- Full payload shape from ``moltsets_client.find_person_email`` + the
  identity fields stitched at cascade call sites (full_name/domain)
- ``extra`` passthrough: risk_score / email_type / verified_hint /
  source_endpoint preserved
- Dispatch registry accepts every source spelling (moltsets,
  moltsets_email, molt_sets, molt-sets); unknown sources return None
- Junk filter: no email/name/linkedin -> None
"""

from __future__ import annotations

import os
import sys

import pytest

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from enrichment import response_normalizer as rn  # noqa: E402


def _payload(**overrides) -> dict:
    base = {
        "found": True,
        "email": "jane@acme.com",
        "risk_score": "A",
        "email_type": "business",
        "first_name": "Jane",
        "last_name": "Doe",
        "full_name": "Jane Doe",
        "linkedin_url": "https://www.linkedin.com/in/janedoe",
        "verified_hint": "yes",
        "source_endpoint": "linkedin_to_best_email",
        "domain": "acme.com",
    }
    base.update(overrides)
    return base


class TestNormalizeMoltSetsContact:
    def test_full_payload(self):
        rec = rn.normalize_moltsets_contact(_payload())
        assert rec is not None
        assert rec["source"] == "moltsets"
        assert rec["email"] == "jane@acme.com"
        assert rec["first_name"] == "Jane"
        assert rec["last_name"] == "Doe"
        # House canonical LinkedIn form strips the scheme + www prefix.
        assert rec["linkedin_url"] == "linkedin.com/in/janedoe"
        assert rec["domain"] == "acme.com"
        assert rec["risk_score"] == "A"
        assert rec["email_type"] == "business"
        assert rec["verified_hint"] == "yes"
        assert rec["source_endpoint"] == "linkedin_to_best_email"

    def test_personal_email_type_passthrough(self):
        rec = rn.normalize_moltsets_contact(_payload(email_type="personal", risk_score="B"))
        assert rec is not None
        assert rec["email_type"] == "personal"
        assert rec["risk_score"] == "B"

    def test_non_dict_returns_none(self):
        assert rn.normalize_moltsets_contact("nope") is None
        assert rn.normalize_moltsets_contact(None) is None

    def test_junk_contact_returns_none(self):
        # No email, no name, no linkedin -> junk.
        assert rn.normalize_moltsets_contact({"risk_score": "A"}) is None


class TestDispatch:
    @pytest.mark.parametrize("source", [
        "moltsets",
        "moltsets_email",
        "molt_sets",
        "molt-sets",
        "MoltSets",       # case-insensitive
    ])
    def test_all_spellings_dispatch(self, source):
        rec = rn.normalize_provider_contact(source, _payload())
        assert rec is not None
        assert rec["source"] == "moltsets"

    def test_unknown_source_returns_none(self):
        assert rn.normalize_provider_contact("moltset", _payload()) is None

    def test_exported(self):
        assert "normalize_moltsets_contact" in rn.__all__
