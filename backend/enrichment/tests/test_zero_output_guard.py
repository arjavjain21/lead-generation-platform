"""
Smoke test for the 2026-07-11..13 silent-failure bug class.

The original bug: ``run_domain_enrichment`` referenced ``linkedin_url_col``
in its body but never declared it as a parameter. For any CSV without a
``company_linkedin_col`` (the common case), the function raised
``NameError: name 'linkedin_url_col' is not defined`` on every row.
``asyncio.gather(return_exceptions=True)`` swallowed the error and the
job finished as ``status=done`` with a 0-byte output CSV.

These tests exercise:
  * Group A: Regression — a no-optional-columns CSV must produce rows,
    not silently fail with an empty list.
  * Group B: When every row DOES fail (forced error), ``run_domain_enrichment``
    raises RuntimeError instead of returning []. This is the loud-failure
    contract the caller relies on to mark the job as ``failed``.

Run:
    cd backend && source venv/bin/activate
    python -m pytest enrichment/tests/test_zero_output_guard.py -v
"""

from __future__ import annotations

import asyncio
import sys
from unittest.mock import AsyncMock, patch

import pytest

sys.path.insert(0, "/var/www/lead-generation-platform/backend")

from enrichment import list_builder


def _stub_providers():
    """Return a list of patches that replace every provider HTTP call with
    a no-op that returns a minimal valid shape. The cascade still runs end
    to end; we just don't spend real API credits or hit the network."""
    return [
        patch("enrichment.list_builder.contacts_client.company_by_domain",
              new=AsyncMock(return_value={"linkedin_url": "https://linkedin.com/company/acme"})),
        patch("enrichment.list_builder.contacts_client.company_contacts_enriched",
              new=AsyncMock(return_value=[])),
        patch("enrichment.list_builder.contacts_client.person_by_name_and_domain",
              new=AsyncMock(return_value=None)),
        patch("enrichment.list_builder.blitz_client.domain_to_linkedin",
              new=AsyncMock(return_value={"found": False, "company_linkedin_url": ""})),
        patch("enrichment.list_builder.blitz_client.waterfall_icp_search",
              new=AsyncMock(return_value={"results": []})),
    ]


class TestNoOptionalColumnsCsv:
    """Group A: The exact scenario that broke on 2026-07-11.

    A CSV with only a ``domain`` column — no ``company_linkedin_col``,
    no ``linkedin_url_col``. The original bug fired NameError on every row
    of this input."""

    def test_domain_only_csv_produces_rows(self):
        """A 3-row CSV with just ``domain`` must produce 3+ output rows."""
        rows = [
            {"domain": "acme.com"},
            {"domain": "example.com"},
            {"domain": "stripe.com"},
        ]
        patches = _stub_providers()
        for p in patches:
            p.start()
        try:
            result = asyncio.run(list_builder.run_domain_enrichment(
                rows=rows,
                domain_col="domain",
                max_decision_makers=2,
                job_id="test_domain_only_csv",
            ))
        finally:
            for p in patches:
                p.stop()

        assert isinstance(result, list), "run_domain_enrichment must return a list"
        assert len(result) >= 3, (
            f"Expected ≥3 output rows from 3 input rows, got {len(result)}. "
            f"If this is 0, the linkedin_url_col NameError regression likely returned."
        )

    def test_explicit_none_for_optional_cols(self):
        """Pass None for every optional col — equivalent to a fresh user
        uploading a bare domain CSV. Must not NameError."""
        rows = [{"domain": "acme.com"}]
        patches = _stub_providers()
        for p in patches:
            p.start()
        try:
            result = asyncio.run(list_builder.run_domain_enrichment(
                rows=rows,
                domain_col="domain",
                name_col=None,
                first_name_col=None,
                last_name_col=None,
                company_linkedin_col=None,
                linkedin_url_col=None,
                max_decision_makers=2,
                job_id="test_explicit_none",
            ))
        finally:
            for p in patches:
                p.stop()

        assert len(result) >= 1, "Got 0 rows — silent failure regression"


class TestZeroOutputGuard:
    """Group B: When every row fails, the function must RAISE — not return [].

    This is the safety net so callers mark the job as ``failed`` instead
    of ``done`` with an empty CSV. Mirrors the contract the
    ``_run_domain_enrich_job`` defensive guard relies on."""

    def test_all_rows_failing_raises(self):
        """Force process_row itself to raise (not the cascade — the cascade
        catches provider errors gracefully). We patch normalize_domain which
        is called at the TOP of process_row, mimicking the exact pattern of
        the 2026-07-11 bug (NameError inside process_row before any provider
        call). The function must surface this as RuntimeError, not silently
        return []."""

        def boom(*args, **kwargs):
            raise RuntimeError("forced per-row failure")

        with patch("enrichment.list_builder.identifier_utils.normalize_domain",
                   side_effect=boom):
            with pytest.raises(RuntimeError, match="All .* rows failed"):
                asyncio.run(list_builder.run_domain_enrichment(
                    rows=[
                        {"domain": "acme.com"},
                        {"domain": "stripe.com"},
                    ],
                    domain_col="domain",
                    max_decision_makers=1,
                    job_id="test_all_rows_failing",
                    normalize_domains=True,
                ))

    def test_partial_failure_still_returns_successes(self):
        """If some rows succeed and some fail, the function returns the
        successes (does NOT raise). Only ALL-fail raises."""
        # Two rows. First succeeds (stubs return valid), second fails (bad
        # domain triggers an early no_linkedin — counts as a successful
        # "no data" row, not an exception). So output is non-empty.
        patches = _stub_providers()
        for p in patches:
            p.start()
        try:
            result = asyncio.run(list_builder.run_domain_enrichment(
                rows=[
                    {"domain": "acme.com"},
                    {"domain": ""},
                ],
                domain_col="domain",
                max_decision_makers=1,
                job_id="test_partial_failure",
            ))
        finally:
            for p in patches:
                p.stop()

        assert len(result) >= 1, "Partial failure should still return successful rows"
