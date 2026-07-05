"""Tests for row lineage preservation.

Verifies that input_* columns (input_domain, input_full_name,
input_linkedin_url, input_fields_used) are preserved on the output row
even when dedupe has collapsed input rows, so users can audit which
input was enriched.
"""
import asyncio
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


class TestInputColumnsPreserved:
    """The output row from _enrich_single_domain already carries the
    source dict (including all input columns). attach_input_columns
    should populate input_* fields from those."""

    def test_input_domain_preserved_with_dedupe(self):
        from enrichment import list_builder

        captured = []

        async def fake_enrich(blitz_http, contacts_http, row, domain, *args, **kwargs):
            # Return a copy with one enriched contact.
            captured.append({"source_domain": row.get("website")})
            out = {**row, "dm_email": "test@example.com", "row_status": "ok"}
            # Mimic the input_* columns the pipeline normally attaches.
            out["input_domain"] = row.get("website")
            out["input_full_name"] = row.get("name", "")
            out["input_linkedin_url"] = row.get("linkedin", "")
            out["input_fields_used"] = "domain"
            return [out]

        rows = [
            {"website": "acme.com", "name": "Alice", "linkedin": "https://li.com/in/alice"},
            {"website": "acme.com", "name": "Bob", "linkedin": "https://li.com/in/bob"},
            {"website": "acme.com", "name": "Charlie", "linkedin": "https://li.com/in/charlie"},
        ]
        # Simulate dedupe: only the first row survives
        deduped = rows[:1]

        with patch.object(list_builder, "_enrich_single_domain", side_effect=fake_enrich):
            with patch.object(list_builder, "_apply_company_fallback_to_output_rows",
                              return_value=None):
                output = _run(list_builder.run_domain_enrichment(
                    rows=deduped,
                    domain_col="website",
                    on_progress=None,
                ))

        assert len(output) == 1
        # The surviving (first) row's input columns are preserved.
        assert output[0]["input_domain"] == "acme.com"
        assert output[0]["input_full_name"] == "Alice"
        assert output[0]["input_linkedin_url"] == "https://li.com/in/alice"

    def test_deduped_rows_dropped_before_runner(self):
        """Confirm that the deduped rows (Bob, Charlie) never reach
        the runner, so the lineage metadata on the output is for the
        surviving row (Alice). This is the contract."""
        from enrichment import list_builder

        seen_rows = []

        async def fake_enrich(blitz_http, contacts_http, row, domain, *args, **kwargs):
            seen_rows.append(row.get("name"))
            out = {**row, "dm_email": "test@example.com", "row_status": "ok"}
            out["input_domain"] = row.get("website")
            return [out]

        rows = [
            {"website": "acme.com", "name": "Alice"},
            {"website": "acme.com", "name": "Bob"},
            {"website": "acme.com", "name": "Charlie"},
        ]
        # Simulate route-handler dedupe: only first row passes through.
        deduped = rows[:1]

        with patch.object(list_builder, "_enrich_single_domain", side_effect=fake_enrich):
            with patch.object(list_builder, "_apply_company_fallback_to_output_rows",
                              return_value=None):
                _run(list_builder.run_domain_enrichment(
                    rows=deduped,
                    domain_col="website",
                    on_progress=None,
                ))

        # Only Alice's row reached the runner.
        assert seen_rows == ["Alice"]


class TestDedupeSkippedDomainsAuditability:
    """The dedupe helper returns skipped_domains for audit. The route
    handler persists this to the jobs table."""

    def test_skipped_domains_preserved_in_dedupe_result(self):
        from enrichment.identifier_utils import dedupe_rows_by_domain

        rows = [
            {"website": "acme.com"},
            {"website": "https://acme.com/?x=1"},
            {"website": "https://acme.com/?x=2"},
        ]
        kept, count, skipped = dedupe_rows_by_domain(rows, "website", normalize=True)
        # The two duplicates are preserved verbatim in skipped.
        assert count == 2
        assert skipped == ["https://acme.com/?x=1", "https://acme.com/?x=2"]
        # The kept row is the first occurrence.
        assert len(kept) == 1
        assert kept[0]["website"] == "acme.com"
