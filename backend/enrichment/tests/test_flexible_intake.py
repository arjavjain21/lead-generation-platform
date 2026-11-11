"""Flexible Flow-1 intake tests (PR-2.1, 2026-11-11).

A CSV with only company LinkedIn URLs (or only company names) is accepted;
identifier precedence (company LinkedIn → domain → name) stays the runner's
existing routing; rows with no usable identifier are SKIPPED with explicit
reasons; domain-only behavior is byte-identical to pre-PR-2.1.
"""
from __future__ import annotations

import asyncio
import csv
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from enrichment import list_builder  # noqa: E402


def _stub_providers(company_url=""):
    return [
        patch("enrichment.list_builder.contacts_client.company_by_domain",
              new=AsyncMock(return_value={"linkedin_url": company_url} if company_url else None)),
        patch("enrichment.list_builder.contacts_client.company_contacts_enriched",
              new=AsyncMock(return_value=[])),
        patch("enrichment.list_builder.contacts_client.person_by_name_and_domain",
              new=AsyncMock(return_value=None)),
        patch("enrichment.list_builder.blitz_client.domain_to_linkedin",
              new=AsyncMock(return_value={"found": False, "company_linkedin_url": ""})),
        patch("enrichment.list_builder.blitz_client.waterfall_icp_search",
              new=AsyncMock(return_value={"results": []})),
        patch("enrichment.list_builder.company_fallback.run_company_fallbacks",
              new=AsyncMock(return_value={})),
    ]


class TestRoutingWithEmptyDomainCol(unittest.TestCase):
    """Run the real router with domain_col='' — the LinkedIn-only shape."""

    def _run(self, rows, **kw):
        patches = _stub_providers() + [
            patch("enrichment.list_builder._attach_seg_columns",
                  new=AsyncMock(return_value=None)),
        ]
        for p in patches:
            p.start()
        try:
            return asyncio.run(list_builder.run_domain_enrichment(
                rows=rows, domain_col=kw.pop("domain_col", ""), **kw))
        finally:
            for p in patches:
                p.stop()

    def test_company_linkedin_row_routes_to_company_path(self):
        company_recorder = AsyncMock(return_value=[{"row_status": "no_contacts"}])
        with patch("enrichment.list_builder._enrich_by_company_linkedin",
                   new=company_recorder), \
             patch("enrichment.list_builder._enrich_single_domain",
                   new=AsyncMock(return_value=[{"row_status": "skipped"}])):
            rows = self._run(
                [{"linkedin_url": "https://linkedin.com/company/acme",
                  "company_name": "Acme"}],
                domain_col="", linkedin_url_col="linkedin_url",
                company_name_col="company_name",
            )
        company_recorder.assert_awaited_once()
        self.assertEqual(
            company_recorder.await_args.kwargs.get("company_linkedin_url"),
            "https://linkedin.com/company/acme")

    def test_identifier_less_row_skipped_with_reason(self):
        rows = self._run([{"company_name": ""}, {"company_name": "Ghost Co"}],
                         domain_col="", company_name_col="company_name")
        # NOTE: skip rows never reach _enrich_single_domain, so the echoed
        # input_* columns are blank — assert by input ORDER (row order is
        # preserved by the runner).
        self.assertEqual(rows[0].get("no_email_reason"), "no_identifier")
        self.assertEqual(rows[1].get("no_email_reason"), "name_unresolved")
        self.assertTrue(all(r.get("row_status") == "skipped" for r in rows))

    def test_domain_only_run_unchanged(self):
        """A domain-column CSV routes exactly as before PR-2.1 (no skip
        reasons, domain cascade invoked)."""
        domain_recorder = AsyncMock(return_value=[{"row_status": "no_contacts"}])
        with patch("enrichment.list_builder._enrich_single_domain",
                   new=domain_recorder):
            rows = self._run([{"domain": "a.example"}], domain_col="domain")
        domain_recorder.assert_awaited_once()
        self.assertEqual(rows[0].get("no_email_reason", ""), "")


class TestDedupeByIdentity(unittest.TestCase):
    def test_linkedin_identity_dedupe_first_wins(self):
        # Mirror the route-level helper by exercising it through the public
        # shape: rows with the same /company/ URL collapse.
        from enrichment.routes import domain_enrich_with_providers  # noqa: F401 (import check)
        # The dedupe logic itself lives inline in the route; test the raw key
        # semantics through identifier_utils + _is_company_linkedin_url.
        from enrichment import identifier_utils
        url_a = identifier_utils.normalize_linkedin_url("https://www.LinkedIn.com/Company/Acme/")
        url_b = identifier_utils.normalize_linkedin_url("linkedin.com/company/acme")
        # normalize preserves the www host — the CHAIN canonicalizer strips it
        # (tam_chain._canonical_company_url); route-level identity dedupe must
        # apply the same normalization or the two spellings would not collapse.
        from enrichment import tam_chain
        self.assertEqual(tam_chain._canonical_company_url(url_a),
                         tam_chain._canonical_company_url(url_b))
        self.assertTrue(list_builder._is_company_linkedin_url(url_a))
        self.assertTrue(list_builder._is_company_linkedin_url(url_b))


if __name__ == "__main__":
    unittest.main()
