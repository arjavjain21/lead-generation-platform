"""B1 + A1 tests (2026-11-25).

B1 — per-request name-resolution opt-in (tri-state with ENABLE_NAME_RESOLVER):
    env unset    -> the request flag decides
    env "false"  -> hard kill-switch (flag ignored)
    env "true"   -> always on (pre-B1 behavior)
A1 — cross-leg person-location OUTPUT filter:
    rows with a KNOWN non-matching dm_location_country are dropped from the
    export; unknown-location rows are kept; a fully filtered domain leaves
    one audit row (no_email_reason=location_filtered_out); the location is
    read from the persisted cascade so restart/resume keeps the filter.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import unittest
from unittest.mock import AsyncMock, patch

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from enrichment import company_resolver, list_builder  # noqa: E402


def _stub_providers():
    return [
        patch("enrichment.list_builder.contacts_client.company_by_domain",
              new=AsyncMock(return_value=None)),
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
        patch("enrichment.list_builder._attach_seg_columns",
              new=AsyncMock(return_value=None)),
    ]


def _run(rows, **kw):
    patches = _stub_providers()
    for p in patches:
        p.start()
    try:
        return asyncio.run(list_builder.run_domain_enrichment(
            rows=rows, domain_col=kw.pop("domain_col", ""), **kw))
    finally:
        for p in patches:
            p.stop()


_CASCADE_US = json.dumps([{"titles": ["CEO"], "location": ["US"]}])


class TestResolverTriState(unittest.TestCase):
    def test_env_unset_follows_request_flag(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ENABLE_NAME_RESOLVER", None)
            self.assertTrue(company_resolver.resolution_allowed(True))
            self.assertFalse(company_resolver.resolution_allowed(False))

    def test_env_false_is_hard_kill_switch(self):
        with patch.dict(os.environ, {"ENABLE_NAME_RESOLVER": "false"}):
            self.assertFalse(company_resolver.resolution_allowed(True))
            self.assertFalse(company_resolver.resolution_allowed(False))

    def test_env_true_resolves_regardless(self):
        with patch.dict(os.environ, {"ENABLE_NAME_RESOLVER": "true"}):
            self.assertTrue(company_resolver.resolution_allowed(True))
            self.assertTrue(company_resolver.resolution_allowed(False))

    def test_legacy_resolver_enabled_unchanged(self):
        # Pre-B1 helper semantics preserved (default-off reading).
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ENABLE_NAME_RESOLVER", None)
            self.assertFalse(company_resolver.resolver_enabled())

    def test_real_resolve_not_shortcircuited_when_env_unset(self):
        # Regression: resolve_company_name's INTERNAL gate used to require
        # env ENABLE_NAME_RESOLVER=true, which silently neutered the
        # per-request opt-in (env unset). The lookup itself is mocked; the
        # gate behavior under test is the real function's.
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ENABLE_NAME_RESOLVER", None)
            with patch("enrichment.company_resolver._resolve_uncached",
                       new=AsyncMock(return_value=company_resolver.ResolveResult(
                           status="high", domain="acme.com"))):
                result = asyncio.run(company_resolver.resolve_company_name(
                    None, "Acme Corp", conn=None))
        self.assertEqual((result.status, result.domain), ("high", "acme.com"))

    def test_real_resolve_blocked_by_explicit_env_false(self):
        # The hard kill-switch still holds inside the function.
        with patch.dict(os.environ, {"ENABLE_NAME_RESOLVER": "false"}):
            with patch("enrichment.company_resolver._resolve_uncached",
                       new=AsyncMock(return_value=company_resolver.ResolveResult(
                           status="high", domain="acme.com"))):
                result = asyncio.run(company_resolver.resolve_company_name(
                    None, "Acme Corp", conn=None))
        self.assertEqual(result.status, "none")


class TestResolveNamesRequestFlag(unittest.TestCase):
    """Name-only rows through the real router with the request flag."""

    def _resolver_patch(self, status, domain="acme.com"):
        result = company_resolver.ResolveResult(
            status=status, domain=domain if status in ("high", "medium") else "",
            candidates=[{"name": "Acme", "domain": domain}] if status == "ambiguous" else [])
        return patch("enrichment.list_builder.company_resolver.resolve_company_name",
                     new=AsyncMock(return_value=result))

    def test_flag_on_resolves_high_confidence(self):
        domain_recorder = AsyncMock(return_value=[{"row_status": "no_contacts"}])
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ENABLE_NAME_RESOLVER", None)
            with self._resolver_patch("high") as _rp, \
                 patch("enrichment.list_builder._resolver_conn", new=lambda: None), \
                 patch("enrichment.list_builder._enrich_single_domain", new=domain_recorder):
                rows = _run([{"company_name": "Acme Corp"}],
                            domain_col="", company_name_col="company_name",
                            resolve_names=True)
        domain_recorder.assert_awaited_once()
        # _enrich_single_domain(blitz_http, contacts_http, row, domain, ...) —
        # the resolved domain rides the row (with its provenance stamps) and
        # the 4th positional arg. The mocked return can't carry the stamps, so
        # prove them on the row the router PASSED IN.
        self.assertEqual(domain_recorder.await_args.args[2].get("domain"), "acme.com")
        self.assertEqual(domain_recorder.await_args.args[3], "acme.com")
        self.assertEqual(domain_recorder.await_args.args[2].get("name_resolution_status"), "high")

    def test_flag_off_skips_name_unresolved(self):
        # Same env-unset state, request flag NOT set: legacy behavior preserved.
        resolver_spy = AsyncMock(return_value=company_resolver.ResolveResult(status="high", domain="acme.com"))
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ENABLE_NAME_RESOLVER", None)
            with patch("enrichment.list_builder.company_resolver.resolve_company_name",
                       new=resolver_spy), \
                 patch("enrichment.list_builder._resolver_conn", new=lambda: None):
                rows = _run([{"company_name": "Acme Corp"}],
                            domain_col="", company_name_col="company_name")
        resolver_spy.assert_not_awaited()
        self.assertEqual(rows[0].get("no_email_reason"), "name_unresolved")

    def test_kill_switch_beats_request_flag(self):
        resolver_spy = AsyncMock(return_value=company_resolver.ResolveResult(status="high", domain="acme.com"))
        with patch.dict(os.environ, {"ENABLE_NAME_RESOLVER": "false"}):
            with patch("enrichment.list_builder.company_resolver.resolve_company_name",
                       new=resolver_spy), \
                 patch("enrichment.list_builder._resolver_conn", new=lambda: None):
                rows = _run([{"company_name": "Acme Corp"}],
                            domain_col="", company_name_col="company_name",
                            resolve_names=True)
        resolver_spy.assert_not_awaited()
        self.assertEqual(rows[0].get("no_email_reason"), "name_unresolved")

    def test_ambiguous_never_autojoins_even_when_armed(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ENABLE_NAME_RESOLVER", None)
            with self._resolver_patch("ambiguous"), \
                 patch("enrichment.list_builder._resolver_conn", new=lambda: None):
                rows = _run([{"company_name": "Acme Corp"}],
                            domain_col="", company_name_col="company_name",
                            resolve_names=True)
        self.assertEqual(rows[0].get("no_email_reason"), "ambiguous_company_name")
        self.assertIn("Acme", rows[0].get("name_candidates", ""))


class TestLocationOutputFilter(unittest.TestCase):
    def _domain_rows(self):
        return [
            {"row_status": "enriched", "dm_full_name": "A", "dm_email": "a@x.com",
             "dm_location_country": "United States"},
            {"row_status": "enriched", "dm_full_name": "B", "dm_email": "b@x.com",
             "dm_location_country": "Germany"},
            {"row_status": "enriched", "dm_full_name": "C", "dm_email": "",
             "dm_location_country": ""},
        ]

    def test_unit_helpers(self):
        from enrichment.list_builder import (
            _cascade_location_country, _filter_rows_by_person_location, _location_matches,
        )
        self.assertTrue(_location_matches("United States", "US"))
        self.assertTrue(_location_matches("united kingdom", "UK"))
        self.assertFalse(_location_matches("Russia", "US"))  # never substring
        self.assertFalse(_location_matches("", "US"))        # unknown never matches-known
        kept, dropped = _filter_rows_by_person_location(self._domain_rows(), "US")
        self.assertEqual(dropped, 1)
        self.assertEqual([r["dm_full_name"] for r in kept], ["A", "C"])
        self.assertEqual(_cascade_location_country(_CASCADE_US), "US")
        self.assertEqual(_cascade_location_country(None), "")
        self.assertEqual(_cascade_location_country("not-json"), "")

    def test_runner_filters_output_rows_and_counts(self):
        events: list[dict] = []

        def on_progress(e):
            events.append(e)

        domain_mock = AsyncMock(return_value=self._domain_rows())
        with patch("enrichment.list_builder._enrich_single_domain", new=domain_mock):
            rows = _run([{"domain": "mix.example"}], domain_col="domain",
                        cascade_config=_CASCADE_US, on_progress=on_progress)
        self.assertEqual([r.get("dm_full_name") for r in rows], ["A", "C"])
        self.assertEqual(rows[0].get("no_email_reason", ""), "")
        # Event counters reflect the FILTERED rows (reconciliation guarantee).
        self.assertEqual(events[-1]["contacts_found"], 2)
        self.assertEqual(events[-1]["location_filtered"], 1)

    def test_all_filtered_domain_leaves_audit_row(self):
        only_germany = [{"row_status": "enriched", "dm_full_name": "B",
                         "dm_email": "b@x.com", "dm_location_country": "Germany"}]
        events: list[dict] = []

        def on_progress(e):
            events.append(e)

        with patch("enrichment.list_builder._enrich_single_domain",
                   new=AsyncMock(return_value=only_germany)):
            rows = _run([{"domain": "de.example"}], domain_col="domain",
                        cascade_config=_CASCADE_US, on_progress=on_progress)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].get("no_email_reason"), "location_filtered_out")
        self.assertEqual(events[-1]["location_filtered"], 1)

    def test_no_location_no_filter(self):
        """Without a location in the cascade nothing changes (regression guard)."""
        cascade_plain = json.dumps([{"titles": ["CEO"]}])
        domain_mock = AsyncMock(return_value=self._domain_rows())
        with patch("enrichment.list_builder._enrich_single_domain", new=domain_mock):
            rows = _run([{"domain": "mix.example"}], domain_col="domain",
                        cascade_config=cascade_plain)
        self.assertEqual(len(rows), 3)


if __name__ == "__main__":
    unittest.main()
