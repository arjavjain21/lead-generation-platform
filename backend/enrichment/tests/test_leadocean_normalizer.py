"""
Unit tests for ``response_normalizer.normalize_leadocean_person`` — the
LeadOcean domain→people DISCOVERY row shape (phase 2, 2026-11-07).

Unlike the quickenrich/getleads discovery normalizers there is NO
email-required gate: discovery rows carry identity + the vendor's has_email /
email_status flags but NO address by design (the per-person email cascade
resolves it). The junk filter still applies — name or LinkedIn URL must
survive.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from enrichment.response_normalizer import (  # noqa: E402
    normalize_leadocean_person,
    normalize_provider_contact,
)


_ROW = {
    "full_name": "Aakash Sasikumar",
    "first_name": "Aakash",
    "last_name": "Sasikumar",
    "headline": "ML @ Stripe",
    "linkedin_url": "https://www.linkedin.com/in/aakash-sasikumar",
    "title": "Engineering, Ml Infra",
    "job_level": "Other",
    "has_email": True,
    "email_status": "catch_all",
    "email_types": ["work"],
    "has_phone": False,
    "person_id": "25615657",
    "company_name": "Stripe",
    "company_domain": "stripe.com",
    "company_industry": "Technology; Information and Internet",
    "city": "Edmonton",
    "country_code": "CA",
}


class TestNormalizeLeadOceanPerson:
    def test_identity_row_no_email_required(self):
        rec = normalize_leadocean_person(dict(_ROW))
        assert rec is not None
        assert rec["full_name"] == "Aakash Sasikumar"
        assert rec["linkedin_url"] == "linkedin.com/in/aakash-sasikumar"
        assert rec["email"] == ""  # by design — no address on discovery rows
        assert rec["title"] == "Engineering, Ml Infra"
        assert rec["domain"] == "stripe.com"
        assert rec["company_name"] == "Stripe"
        assert rec["job_level"] == "Other"

    def test_flags_ride_via_extra(self):
        rec = normalize_leadocean_person(dict(_ROW))
        assert rec is not None
        assert rec["has_email"] is True
        assert rec["email_status"] == "catch_all"
        assert rec["person_id"] == "25615657"

    def test_dispatch_keys(self):
        for source in ("leadocean_people", "leadocean_discovery", "leadocean_person"):
            rec = normalize_provider_contact(source, dict(_ROW))
            assert rec is not None, f"dispatch key {source} must route to the person normalizer"

    def test_junk_row_returns_none(self):
        assert normalize_leadocean_person({"title": "CEO"}) is None
        assert normalize_leadocean_person("not-a-dict") is None

    def test_linkedin_only_row_survives(self):
        rec = normalize_leadocean_person({"linkedin_url": _ROW["linkedin_url"]})
        assert rec is not None and rec["email"] == ""
