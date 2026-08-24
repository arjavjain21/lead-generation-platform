"""Tests for the normalize_domains gate inside list_builder.run_domain_enrichment.

Verifies:
  * normalize_domains=True (default) → bare lowercase domain
  * normalize_domains=False → raw stripped value
  * DEDUPE_ON + NORMALIZE_OFF combo: dedupe in route uses raw key, but
    the runner still passes the raw value to the provider.
"""
import asyncio
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))


def _run(coro):
    # asyncio.run() instead of the deprecated get_event_loop(): other test
    # modules (e.g. test_domain_checkpoints) close the thread's loop via
    # asyncio.run(), after which get_event_loop() raises — order pollution.
    return asyncio.run(coro)


@pytest.fixture(autouse=True)
def _reset_loop():
    yield
    # Ensure a clean loop per test
    pass


class TestNormalizeGate:
    """Verify that the normalize_domains flag gates the per-row
    normalize_domain() call inside process_row."""

    def test_normalize_on_default(self):
        """Default behavior: normalize_domain() is called."""
        from enrichment import list_builder

        captured = []

        async def fake_enrich(blitz_http, contacts_http, row, domain, *args, **kwargs):
            captured.append({"row": row, "domain": domain})
            return [{**row, **list_builder._empty_enriched(), "row_status": "ok"}]

        rows = [{"website": "https://Acme.com/?utm_source=x"}]

        with patch.object(list_builder, "_enrich_single_domain", side_effect=fake_enrich):
            with patch.object(list_builder, "_apply_company_fallback_to_output_rows",
                              return_value=None):
                _run(list_builder.run_domain_enrichment(
                    rows=rows,
                    domain_col="website",
                    on_progress=None,
                    # normalize_domains default is True
                ))

        assert len(captured) == 1
        assert captured[0]["domain"] == "acme.com"

    def test_normalize_off_passes_raw(self):
        """With normalize_domains=False, the raw stripped value is sent."""
        from enrichment import list_builder

        captured = []

        async def fake_enrich(blitz_http, contacts_http, row, domain, *args, **kwargs):
            captured.append({"row": row, "domain": domain})
            return [{**row, **list_builder._empty_enriched(), "row_status": "ok"}]

        rows = [{"website": "https://Acme.com/?utm_source=x"}]

        with patch.object(list_builder, "_enrich_single_domain", side_effect=fake_enrich):
            with patch.object(list_builder, "_apply_company_fallback_to_output_rows",
                              return_value=None):
                _run(list_builder.run_domain_enrichment(
                    rows=rows,
                    domain_col="website",
                    on_progress=None,
                    normalize_domains=False,
                ))

        assert len(captured) == 1
        # Raw value with original casing/format is passed through.
        assert captured[0]["domain"] == "https://Acme.com/?utm_source=x"

    def test_normalize_off_empty_row_still_skipped(self):
        """Empty rows are still skipped when normalize is off."""
        from enrichment import list_builder

        captured = []

        async def fake_enrich(blitz_http, contacts_http, row, domain, *args, **kwargs):
            captured.append({"row": row, "domain": domain})
            return [{**row, **list_builder._empty_enriched(), "row_status": "ok"}]

        rows = [{"website": ""}, {"website": "   "}]

        with patch.object(list_builder, "_enrich_single_domain", side_effect=fake_enrich):
            with patch.object(list_builder, "_apply_company_fallback_to_output_rows",
                              return_value=None):
                _run(list_builder.run_domain_enrichment(
                    rows=rows,
                    domain_col="website",
                    on_progress=None,
                    normalize_domains=False,
                ))

        # Empty rows should NOT be passed to _enrich_single_domain.
        assert captured == []

    def test_normalize_off_preserves_path(self):
        """When normalize is off, sub-paths survive — useful for franchise
        locations with unique URL paths."""
        from enrichment import list_builder

        captured = []

        async def fake_enrich(blitz_http, contacts_http, row, domain, *args, **kwargs):
            captured.append({"row": row, "domain": domain})
            return [{**row, **list_builder._empty_enriched(), "row_status": "ok"}]

        rows = [{"website": "mcdonalds.com/location/001"}]

        with patch.object(list_builder, "_enrich_single_domain", side_effect=fake_enrich):
            with patch.object(list_builder, "_apply_company_fallback_to_output_rows",
                              return_value=None):
                _run(list_builder.run_domain_enrichment(
                    rows=rows,
                    domain_col="website",
                    on_progress=None,
                    normalize_domains=False,
                ))

        assert captured[0]["domain"] == "mcdonalds.com/location/001"

    def test_normalize_on_strips_path(self):
        """When normalize is on (default), sub-paths are stripped."""
        from enrichment import list_builder

        captured = []

        async def fake_enrich(blitz_http, contacts_http, row, domain, *args, **kwargs):
            captured.append({"row": row, "domain": domain})
            return [{**row, **list_builder._empty_enriched(), "row_status": "ok"}]

        rows = [{"website": "mcdonalds.com/location/001"}]

        with patch.object(list_builder, "_enrich_single_domain", side_effect=fake_enrich):
            with patch.object(list_builder, "_apply_company_fallback_to_output_rows",
                              return_value=None):
                _run(list_builder.run_domain_enrichment(
                    rows=rows,
                    domain_col="website",
                    on_progress=None,
                ))

        assert captured[0]["domain"] == "mcdonalds.com"


class TestDedupeOnNormalizeOffCombo:
    """DEDUPE_ON + NORMALIZE_OFF: dedupe uses raw (lowercased) key, the
    provider receives the raw value. Two rows with the same domain in
    different formats are NOT deduped."""

    def test_protocol_variant_not_deduped_by_helper(self):
        """The dedupe_rows_by_domain helper uses raw lowercased key when
        normalize=False, so 'acme.com' and 'https://acme.com' are distinct."""
        from enrichment.identifier_utils import dedupe_rows_by_domain

        rows = [
            {"domain": "acme.com"},
            {"domain": "https://acme.com"},
        ]
        kept, count, skipped = dedupe_rows_by_domain(rows, "domain", normalize=False)
        assert len(kept) == 2
        assert count == 0
        assert skipped == []

    def test_protocol_variant_deduped_by_helper_when_normalize_on(self):
        """Same data with normalize=True collapses to one."""
        from enrichment.identifier_utils import dedupe_rows_by_domain

        rows = [
            {"domain": "acme.com"},
            {"domain": "https://acme.com"},
        ]
        kept, count, skipped = dedupe_rows_by_domain(rows, "domain", normalize=True)
        assert len(kept) == 1
        assert count == 1
        assert skipped == ["https://acme.com"]
