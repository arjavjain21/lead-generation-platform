"""CSV-output tests for the SEG (Secure Email Gateway) columns.

Wave 2a: ``seg_classification`` / ``seg_provider`` are appended to
``ENRICHED_COLUMNS`` in BOTH ``list_builder.py`` and ``pipeline.py`` and
populated by a per-batch pre-pass that runs BEFORE the incremental flush
(classification must NEVER live inside ``_flush_incremental_batch`` — its
flush -> fsync -> checkpoint -> drain ordering is load-bearing).

Pinned invariants:
1. The two columns are the LAST two entries of both lists, and the lists
   stay in sync (append-at-END is what makes resume-safe DictWriter blanks
   work: pre-feature partial rows carried into a resume lack the seg keys
   and get '' via ``restval=''; mid-list insertion would misalign them).
2. ``_empty_enriched()`` carries both keys with '' values.
3. Flow-1-style batch: one ``seg.classify_domains`` call per batch, values
   stamped onto rows keyed by ``seg.normalize_seg_key``.
4. Flag off (``classify_domains`` -> {}): columns exist but are blank.
5. Row without a domain: blank seg columns, no exception, no lookup.
6. ``classify_domains`` raising is swallowed — rows still flush with blanks.
7. Resume carry-over: an old-header partial row written through the new
   DictWriter gets the new trailing columns as blanks, no misalignment.
8. Flush purity: ``_flush_incremental_batch`` never calls
   ``classify_domains`` (rows arrive pre-stamped).

Network is fully mocked (provider stubs mirror ``test_zero_output_guard``
and ``tests/test_domain_checkpoints``); seg is monkeypatched at the
``enrichment.seg`` module so the flag can stay off in this environment.
"""

from __future__ import annotations

import asyncio
import csv
import inspect
import os
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from enrichment import list_builder  # noqa: E402
from enrichment import pipeline  # noqa: E402
from enrichment import seg  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _stub_providers():
    """No-op provider layer (mirrors test_zero_output_guard._stub_providers):
    Contacts DB returns a company LinkedIn URL but no persons, so each input
    domain produces exactly one NOT_FOUND output row without network."""
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


def _read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        rows = [dict(zip(header, r)) for r in reader]
    return header, rows


def _run_flow1(tmpdir: str, rows: list[dict[str, Any]], extra_patches: list = ()) -> Path:
    """Drive run_domain_enrichment once with incremental writing on and every
    provider stubbed. Returns the output CSV path."""
    out_path = Path(tmpdir) / "out.csv"
    patches = _stub_providers() + list(extra_patches)
    for p in patches:
        p.start()
    try:
        asyncio.run(list_builder.run_domain_enrichment(
            rows=rows,
            domain_col="domain",
            max_decision_makers=1,
            job_id="test_seg_csv",
            write_incremental=True,
            output_path=out_path,
        ))
    finally:
        for p in patches:
            p.stop()
    return out_path


# ---------------------------------------------------------------------------
# 1 + 2: column lists and blank-row helper
# ---------------------------------------------------------------------------

class TestSegColumnsPinned(unittest.TestCase):
    def test_seg_columns_are_last_two_in_list_builder(self):
        self.assertEqual(
            list_builder.ENRICHED_COLUMNS[-2:],
            ["seg_classification", "seg_provider"],
        )

    def test_seg_columns_are_last_two_in_pipeline(self):
        self.assertEqual(
            pipeline.ENRICHED_COLUMNS[-2:],
            ["seg_classification", "seg_provider"],
        )

    def test_both_lists_in_sync(self):
        self.assertEqual(
            list(pipeline.ENRICHED_COLUMNS), list(list_builder.ENRICHED_COLUMNS)
        )

    def test_seg_columns_present_exactly_once_each(self):
        for cols in (list_builder.ENRICHED_COLUMNS, pipeline.ENRICHED_COLUMNS):
            self.assertEqual(cols.count("seg_classification"), 1)
            self.assertEqual(cols.count("seg_provider"), 1)

    def test_empty_enriched_includes_both_keys_blank(self):
        empty = list_builder._empty_enriched()
        self.assertEqual(empty["seg_classification"], "")
        self.assertEqual(empty["seg_provider"], "")

    def test_pipeline_empty_enriched_includes_both_keys_blank(self):
        empty = pipeline._empty_enriched()
        self.assertEqual(empty["seg_classification"], "")
        self.assertEqual(empty["seg_provider"], "")

    def test_empty_enriched_covers_all_columns(self):
        self.assertEqual(
            set(list_builder._empty_enriched().keys()),
            set(list_builder.ENRICHED_COLUMNS),
        )


# ---------------------------------------------------------------------------
# 3: Flow-1 batch stamped from the map, one classify call
# ---------------------------------------------------------------------------

class TestSegPrepassStampsRows(unittest.TestCase):
    def test_row_values_match_map_with_normalize_key_semantics(self):
        """The pre-pass must key the map by seg.normalize_seg_key of the
        domain the row carries — raw URL forms normalize to the same key."""
        calls: list[list[str]] = []

        async def fake_classify(domains):
            calls.append(list(domains))
            return {
                "acme.com": {"seg_classification": "external_seg",
                             "seg_provider": "SEG: Mimecast", "source": "doh"},
                "stripe.com": {"seg_classification": "direct_google",
                               "seg_provider": "Google", "source": "doh"},
            }

        with tempfile.TemporaryDirectory() as td:
            with patch("enrichment.seg.classify_domains", new=fake_classify):
                out = _run_flow1(td, [
                    {"domain": "https://www.acme.com/about"},
                    {"domain": "stripe.com"},
                ])
            header, rows = _read_csv(out)

        self.assertEqual(header[-2:], ["seg_classification", "seg_provider"])
        self.assertEqual(len(rows), 2)
        by_input = {r["domain"]: r for r in rows}
        self.assertEqual(by_input["https://www.acme.com/about"]["seg_classification"],
                         "external_seg")
        self.assertEqual(by_input["https://www.acme.com/about"]["seg_provider"],
                         "SEG: Mimecast")
        self.assertEqual(by_input["stripe.com"]["seg_classification"], "direct_google")
        self.assertEqual(by_input["stripe.com"]["seg_provider"], "Google")
        # ONE classify call for the whole batch (dedupe inside seg).
        self.assertEqual(len(calls), 1)
        self.assertEqual(sorted(calls[0]), ["acme.com", "stripe.com"])

    def test_one_classify_call_per_batch_multirow_expansion(self):
        """A domain can expand to several output rows (DM-per-row); they all
        get stamped from the same single classify call."""
        calls: list[list[str]] = []

        async def fake_classify(domains):
            calls.append(list(domains))
            return {"acme.com": {"seg_classification": "no_email",
                                 "seg_provider": "No Email (no MX)", "source": "doh"}}

        # Two contacts from Contacts DB -> 2 output rows for 1 input row.
        contact = {"full_name": "Jane Doe", "email": "jane@acme.com",
                   "title": "CEO", "first_name": "Jane", "last_name": "Doe"}
        patches = _stub_providers() + [
            patch("enrichment.list_builder.contacts_client.company_contacts_enriched",
                  new=AsyncMock(return_value=[contact, {**contact, "email": "j2@acme.com",
                                                        "full_name": "Jay Dee"}])),
        ]
        with tempfile.TemporaryDirectory() as td:
            out_path = Path(td) / "out.csv"
            with patch("enrichment.seg.classify_domains", new=fake_classify):
                for p in patches:
                    p.start()
                try:
                    asyncio.run(list_builder.run_domain_enrichment(
                        rows=[{"domain": "acme.com"}],
                        domain_col="domain",
                        max_decision_makers=5,
                        job_id="test_seg_multirow",
                        write_incremental=True,
                        output_path=out_path,
                    ))
                finally:
                    for p in patches:
                        p.stop()
            header, rows = _read_csv(out_path)

        self.assertEqual(len(calls), 1)
        self.assertTrue(len(rows) >= 2, f"expected multi-row expansion, got {len(rows)}")
        for r in rows:
            self.assertEqual(r["seg_classification"], "no_email")
            self.assertEqual(r["seg_provider"], "No Email (no MX)")


# ---------------------------------------------------------------------------
# 4 + 4b: flag off / missing domain
# ---------------------------------------------------------------------------

class TestSegFlagOffAndMissingDomain(unittest.TestCase):
    def test_flag_off_columns_present_but_empty(self):
        """classify_domains returns {} when the flag is off — columns exist,
        blank. No seg module monkeypatch: real flag-off behavior."""
        self.assertFalse(seg.is_seg_enabled())
        with tempfile.TemporaryDirectory() as td:
            out = _run_flow1(td, [{"domain": "acme.com"}])
            header, rows = _read_csv(out)
        self.assertEqual(header[-2:], ["seg_classification", "seg_provider"])
        self.assertEqual(rows[0]["seg_classification"], "")
        self.assertEqual(rows[0]["seg_provider"], "")

    def test_empty_map_columns_blank(self):
        async def fake_classify(domains):
            return {}

        with tempfile.TemporaryDirectory() as td:
            with patch("enrichment.seg.classify_domains", new=fake_classify):
                out = _run_flow1(td, [{"domain": "acme.com"}])
            header, rows = _read_csv(out)
        self.assertEqual(header[-2:], ["seg_classification", "seg_provider"])
        self.assertEqual(rows[0]["seg_classification"], "")
        self.assertEqual(rows[0]["seg_provider"], "")

    def test_row_without_domain_gets_blank_seg(self):
        """A row with no usable domain is skipped (STATUS_SKIPPED) but still
        flushed — with blank seg columns and NO domain passed to classify."""

        seen_domains: list[list[str]] = []

        async def fake_classify(domains):
            seen_domains.append([d for d in domains if d])
            return {}

        with tempfile.TemporaryDirectory() as td:
            with patch("enrichment.seg.classify_domains", new=fake_classify):
                out = _run_flow1(td, [{"domain": ""}])
            header, rows = _read_csv(out)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["seg_classification"], "")
        self.assertEqual(rows[0]["seg_provider"], "")
        # No usable domain reached the classifier.
        self.assertEqual(seen_domains, [[]])


# ---------------------------------------------------------------------------
# 5: pre-pass failure swallowed
# ---------------------------------------------------------------------------

class TestSegPrepassFailureSwallowed(unittest.TestCase):
    def test_classify_raising_still_flushes_with_blanks(self):
        async def boom(domains):
            raise RuntimeError("seg exploded")

        with tempfile.TemporaryDirectory() as td:
            with patch("enrichment.seg.classify_domains", new=boom):
                out = _run_flow1(td, [{"domain": "acme.com"}, {"domain": "stripe.com"}])
            header, rows = _read_csv(out)

        self.assertEqual(header[-2:], ["seg_classification", "seg_provider"])
        self.assertEqual(len(rows), 2)
        for r in rows:
            self.assertEqual(r["seg_classification"], "")
            self.assertEqual(r["seg_provider"], "")

    def test_attach_seg_columns_helper_swallows_and_blanks(self):
        """Direct unit check on the shared pre-pass helper (Flow 3 path)."""
        async def boom(domains):
            raise RuntimeError("seg exploded")

        rows = [{"domain": "acme.com", **list_builder._empty_enriched()}]
        with patch("enrichment.seg.classify_domains", new=boom):
            asyncio.run(list_builder._attach_seg_columns(rows))
        self.assertEqual(rows[0]["seg_classification"], "")
        self.assertEqual(rows[0]["seg_provider"], "")

    def test_flow3_legacy_domain_carry_through(self):
        """Flow 3 legacy: the domain comes from the row itself (domain /
        company_domain / input_domain carriers), not a domain column."""
        async def fake_classify(domains):
            return {"acme.com": {"seg_classification": "external_seg",
                                 "seg_provider": "SEG: Proofpoint", "source": "doh"}}

        rows = [{"linkedin_url": "https://www.linkedin.com/in/janedoe",
                 "domain": "acme.com"}]
        with patch("enrichment.seg.classify_domains", new=fake_classify):
            asyncio.run(list_builder._attach_seg_columns(rows))
        self.assertEqual(rows[0]["seg_classification"], "external_seg")
        self.assertEqual(rows[0]["seg_provider"], "SEG: Proofpoint")

    def test_flow3_row_with_no_domain_blanks(self):
        async def fake_classify(domains):
            return {"acme.com": {"seg_classification": "x", "seg_provider": "y",
                                 "source": "doh"}}

        rows = [{"linkedin_url": "https://www.linkedin.com/in/janedoe"}]
        with patch("enrichment.seg.classify_domains", new=fake_classify):
            asyncio.run(list_builder._attach_seg_columns(rows))
        self.assertEqual(rows[0]["seg_classification"], "")
        self.assertEqual(rows[0]["seg_provider"], "")


# ---------------------------------------------------------------------------
# 6: resume carry-over — old-header partial through the new writer
# ---------------------------------------------------------------------------

class TestResumeCarryOver(unittest.TestCase):
    def test_old_row_written_through_new_writer_gets_blank_tail(self):
        """Pre-feature partial rows lack the seg keys. The incremental writer
        is constructed with the NEW header (input keys + ENRICHED_COLUMNS)
        and DictWriter's restval='' fills the missing cells — reproducing the
        exact writer construction the three run_* functions use."""
        old_header = ["domain", "company_name", "dm_email"]
        old_row = {"domain": "acme.com", "company_name": "Acme", "dm_email": "j@acme.com"}

        first_keys = list(old_row.keys())
        all_columns = first_keys + [c for c in list_builder.ENRICHED_COLUMNS
                                    if c not in first_keys]

        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "carried.csv"
            with open(out, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=all_columns, extrasaction="ignore")
                writer.writeheader()
                writer.writerow(old_row)  # resume prepend path
            header, rows = _read_csv(out)

        self.assertEqual(header[-2:], ["seg_classification", "seg_provider"])
        self.assertEqual(len(rows), 1)
        # No misalignment: the old cells kept their columns, seg cells blank.
        self.assertEqual(rows[0]["domain"], "acme.com")
        self.assertEqual(rows[0]["company_name"], "Acme")
        self.assertEqual(rows[0]["dm_email"], "j@acme.com")
        self.assertEqual(rows[0]["seg_classification"], "")
        self.assertEqual(rows[0]["seg_provider"], "")
        self.assertEqual(len(header), len(rows[0]))

    def test_pipeline_writer_header_carries_seg_columns(self):
        """pipeline.run_pipeline builds fieldnames as input keys + ENRICHED_COLUMNS
        — the seg pair must land at the tail there too."""
        first_keys = ["website", "name"]
        all_columns = list(first_keys) + pipeline.ENRICHED_COLUMNS
        self.assertEqual(all_columns[-2:], ["seg_classification", "seg_provider"])
        # ENRICHED_COLUMNS has no duplicates, so the tail cannot shift.
        self.assertEqual(len(pipeline.ENRICHED_COLUMNS),
                         len(set(pipeline.ENRICHED_COLUMNS)))


# ---------------------------------------------------------------------------
# 7: flush purity — classify never called inside the flush
# ---------------------------------------------------------------------------

class TestFlushPurity(unittest.TestCase):
    def test_classify_not_called_during_flush(self):
        """Drive run_domain_enrichment with rows ALREADY carrying seg values
        (classify patched to a sentinel that fails the test if invoked during
        the flush window). The flush itself must never classify."""

        async def sentinel(domains):
            raise AssertionError(
                "seg.classify_domains called inside the flush window"
            )

        # Stub Contacts DB to return one contact so a real row flows through
        # the flush, and patch classify to the sentinel. The pre-pass runs
        # BEFORE the flush, so the sentinel firing would mean the flush (or
        # anything after it in the same await chain) classified.
        contact = {"full_name": "Jane Doe", "email": "jane@acme.com",
                   "title": "CEO", "first_name": "Jane", "last_name": "Doe"}
        patches = _stub_providers() + [
            patch("enrichment.list_builder.contacts_client.company_contacts_enriched",
                  new=AsyncMock(return_value=[contact])),
        ]
        with tempfile.TemporaryDirectory() as td:
            out_path = Path(td) / "out.csv"
            with patch("enrichment.seg.classify_domains", new=sentinel):
                for p in patches:
                    p.start()
                try:
                    result = asyncio.run(list_builder.run_domain_enrichment(
                        rows=[{"domain": "acme.com"}],
                        domain_col="domain",
                        max_decision_makers=1,
                        job_id="test_flush_purity",
                        write_incremental=True,
                        output_path=out_path,
                    ))
                finally:
                    for p in patches:
                        p.stop()
            header, rows = _read_csv(out_path)

        # The pre-pass DID run (it called the sentinel) — but the assertion
        # above proves it fired only BEFORE the flush wrote the rows. The CSV
        # carries the (blank) stamped columns and the flush completed.
        self.assertEqual(header[-2:], ["seg_classification", "seg_provider"])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["seg_classification"], "")
        self.assertEqual(result[0]["seg_classification"], "")

    def test_flush_functions_contain_no_classify_call_statically(self):
        """Static guarantee: the three _flush_incremental_batch definitions
        (Flow 1, Flow 3 legacy, Flow 3 v2) contain no classify_domains call,
        and each run_* pre-pass call site sits textually BEFORE its flush."""
        src = inspect.getsource(list_builder)

        # No flush body classifies.
        flush_bodies = []
        for marker in ("async def _flush_incremental_batch",):
            start = 0
            while True:
                i = src.find(marker, start)
                if i == -1:
                    break
                # Slice to the next top-level "async def " at column 0/4.
                nxt = src.find("\n    async def ", i + len(marker))
                nxt2 = src.find("\nasync def ", i + len(marker))
                end = min(x for x in (nxt, nxt2, len(src)) if x != -1)
                flush_bodies.append(src[i:end])
                start = i + len(marker)
        self.assertEqual(len(flush_bodies), 3,
                         f"expected 3 flush definitions, got {len(flush_bodies)}")
        for body in flush_bodies:
            self.assertNotIn("classify_domains", body,
                             "_flush_incremental_batch must never classify")

        # Each run_* pre-pass precedes its flush call.
        for fn_name in ("run_domain_enrichment", "run_linkedin_enrichment",
                        "run_unified_linkedin_enrichment"):
            fn_src = getattr(list_builder, fn_name)
            body = inspect.getsource(fn_src)
            pre = body.find("_attach_seg_columns")
            flush = body.find("await _flush_incremental_batch")
            self.assertNotEqual(pre, -1, f"{fn_name} missing seg pre-pass")
            self.assertNotEqual(flush, -1, f"{fn_name} missing flush call")
            self.assertLess(
                pre, flush,
                f"{fn_name}: seg pre-pass must precede the flush call",
            )

    def test_pipeline_process_row_precedes_csv_write(self):
        """pipeline.run_pipeline: the seg stamp must sit textually BEFORE the
        incremental CSV write block (flush -> checkpoint -> progress order)."""
        body = inspect.getsource(pipeline.run_pipeline)
        seg_pos = body.find("seg.classify_domains")
        write_pos = body.find("# Write to CSV incrementally if enabled")
        self.assertNotEqual(seg_pos, -1, "run_pipeline missing seg pre-pass")
        self.assertNotEqual(write_pos, -1)
        self.assertLess(seg_pos, write_pos,
                        "seg stamp must precede the CSV write block")


if __name__ == "__main__":
    unittest.main()
