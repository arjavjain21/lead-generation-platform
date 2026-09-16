"""Blitz Find People batch prepass (enrichment/blitz_batch.py).

The prepass covers a job's domains in bulk before the per-domain waterfall:
domain-to-linkedin per domain, then /v2/search/people per 50-company chunk.
These tests pin the contract the caller (pipeline/list_builder wiring) relies
on:

- miss recording happens ONLY on conclusive answers (domain-to-linkedin
  found=false -> kind "company"; zero persons AFTER a conclusive pagination
  end -> kind "contacts") and NEVER on exceptions — a transient 429/5xx must
  not poison a domain for the miss store's TTL — and never when the chunk's
  pagination was TRUNCATED at the max_pages cap with a pending cursor
  (matches may exist past the last page; suppression is per chunk);
- domains that fail (per-domain lookup error, chunk-level error, cancel
  between chunks) are absent from prepass_domains -> the caller falls back
  to the per-domain waterfall;
- is_recent_miss domains short-circuit (skipped_miss) and are never touched;
- exact_titles wraps the include list via bracket_exact; excludes stay raw;
- chunks of 50, one find_people_batch call each, progress + cancel hooks
  fire between chunks;
- default max_pages is adaptive per chunk — min(30, max(6,
  ceil(target_per_company * chunk_size / 50) + 1)) — so target>5 jobs page
  enough to actually fill their targets; an explicit max_pages passes
  through verbatim (see TestPrepassAdaptiveMaxPages).

Async pattern: no pytest-asyncio — asyncio.run(...) in sync tests, matching
the suite convention. Blitz client functions are monkeypatched on the
enrichment.blitz_client module (blitz_batch calls them through that
reference), so no HTTP or rate limiting is exercised here.
"""
from __future__ import annotations

import asyncio
import os
import sys
from unittest.mock import MagicMock

import pytest

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from enrichment import blitz_batch  # noqa: E402
from enrichment import blitz_client as bc  # noqa: E402


def _flat_row(name: str, company_url: str) -> dict:
    """Waterfall-flat row as produced by blitz_client.find_people_batch."""
    return {
        "first_name": name,
        "last_name": "Doe",
        "full_name": f"{name} Doe",
        "title": "CEO",
        "job_level": None,
        "linkedin_url": f"https://www.linkedin.com/in/{name.lower()}",
        "email": None,
        "verified_email": None,
        "headline": "CEO",
        "location_city": "Austin",
        "location_country": "US",
        "icp_tier": 1,
        "ranking": 1,
        "experiences": [
            {
                "company_linkedin_url": company_url,
                "job_title": "CEO",
                "job_is_current": True,
            }
        ],
    }


def _wire_domain_lookup(monkeypatch, mapping: dict, fail: set[str] = set()) -> dict:
    """Patch blitz_client.domain_to_linkedin.

    mapping: domain -> company url (or None for found=false).
    fail: domains whose lookup raises (transient error).
    Returns a call counter dict.
    """
    calls = {"n": 0, "domains": []}

    async def fake_lookup(http, domain):
        calls["n"] += 1
        calls["domains"].append(domain)
        if domain in fail:
            raise RuntimeError("simulated blitz outage")
        url = mapping.get(domain)
        if url is None:
            return {"found": False, "company_linkedin_url": None}
        return {"found": True, "company_linkedin_url": url}

    monkeypatch.setattr(bc, "domain_to_linkedin", fake_lookup)
    return calls


def _wire_find_people(
    monkeypatch,
    grouped_by_url: dict,
    fail_urls: set[str] = set(),
    truncated_urls: set[str] = set(),
) -> dict:
    """Patch blitz_client.find_people_batch.

    grouped_by_url: normalized company url -> [rows].
    fail_urls: any chunk CONTAINING one of these urls raises (chunk failure).
    truncated_urls: any chunk CONTAINING one of these urls reports
        stats={"truncated": True, ...} — pagination stopped at the page cap
        with a pending cursor (matches may exist past the last page).
    Returns a call counter dict capturing the URL batches.
    """
    calls = {"n": 0, "batches": [], "kwargs": []}

    async def fake_batch(http, urls, **kwargs):
        calls["n"] += 1
        calls["batches"].append(list(urls))
        calls["kwargs"].append(kwargs)
        normalized = {bc._normalize_company_url(u) for u in urls}
        if normalized & fail_urls:
            raise RuntimeError("429 after retries")
        stats = kwargs.get("stats")
        if stats is not None:
            stats["truncated"] = bool(normalized & truncated_urls)
            stats["pages"] = 4
        return {
            key: list(rows)
            for key, rows in grouped_by_url.items()
            if key in normalized
        }

    monkeypatch.setattr(bc, "find_people_batch", fake_batch)
    return calls


def _run(**kwargs):
    defaults = dict(
        http=MagicMock(),
        domains=[],
        is_recent_miss=lambda domain: False,
        record_miss=lambda domain, kind: None,
        title_include=["CEO"],
        title_exclude=["intern"],
        exact_titles=False,
    )
    defaults.update(kwargs)
    return asyncio.run(blitz_batch.blitz_find_people_prepass(**defaults))


class TestPrepassHappyPath:
    def test_all_domains_covered_with_persons(self, monkeypatch):
        _wire_domain_lookup(
            monkeypatch,
            {
                "acme.com": "https://www.linkedin.com/company/acme",
                "beta.com": "https://www.linkedin.com/company/beta",
            },
        )
        misses = []
        _wire_find_people(
            monkeypatch,
            {
                "linkedin.com/company/acme": [_flat_row("A", "acme")],
                "linkedin.com/company/beta": [_flat_row("B", "beta")],
            },
        )

        result = _run(
            domains=["acme.com", "beta.com"],
            record_miss=lambda domain, kind: misses.append((domain, kind)),
        )

        assert result["company_url_by_domain"] == {
            "acme.com": "https://www.linkedin.com/company/acme",
            "beta.com": "https://www.linkedin.com/company/beta",
        }
        assert [row["first_name"] for row in result["persons_by_domain"]["acme.com"]] == ["A"]
        assert [row["first_name"] for row in result["persons_by_domain"]["beta.com"]] == ["B"]
        assert result["prepass_domains"] == {"acme.com", "beta.com"}
        assert result["skipped_miss"] == set()
        assert misses == [], "hits must never be recorded as misses"


class TestPrepassMissRecording:
    def test_company_miss_recorded_and_still_covered(self, monkeypatch):
        _wire_domain_lookup(monkeypatch, {"acme.com": None, "beta.com": "https://www.linkedin.com/company/beta"})
        misses = []
        _wire_find_people(monkeypatch, {"linkedin.com/company/beta": [_flat_row("B", "beta")]})

        result = _run(
            domains=["acme.com", "beta.com"],
            record_miss=lambda domain, kind: misses.append((domain, kind)),
        )

        assert ("acme.com", "company") in misses
        assert "acme.com" in result["prepass_domains"], "confirmed miss IS covered"
        assert result["persons_by_domain"]["acme.com"] == []
        assert "acme.com" not in result["company_url_by_domain"]

    def test_zero_persons_recorded_as_contacts_miss_and_covered(self, monkeypatch):
        _wire_domain_lookup(monkeypatch, {"acme.com": "https://www.linkedin.com/company/acme"})
        misses = []
        _wire_find_people(monkeypatch, {})  # company found, zero matching people

        result = _run(
            domains=["acme.com"],
            record_miss=lambda domain, kind: misses.append((domain, kind)),
        )

        assert misses == [("acme.com", "contacts")]
        assert "acme.com" in result["prepass_domains"], (
            "zero-persons domain is conclusively covered (GetLeads DM fallback proceeds)"
        )
        assert result["persons_by_domain"]["acme.com"] == []

    def test_lookup_exception_never_records_miss(self, monkeypatch):
        _wire_domain_lookup(
            monkeypatch,
            {"ok.com": "https://www.linkedin.com/company/ok"},
            fail={"boom.com"},
        )
        misses = []
        _wire_find_people(monkeypatch, {"linkedin.com/company/ok": [_flat_row("O", "ok")]})

        result = _run(
            domains=["boom.com", "ok.com"],
            record_miss=lambda domain, kind: misses.append((domain, kind)),
        )

        assert misses == [], "transient errors must not poison the miss store"
        assert "boom.com" not in result["prepass_domains"], "failed domain falls back"
        assert "boom.com" not in result["company_url_by_domain"]
        assert "boom.com" not in result["persons_by_domain"]
        assert "ok.com" in result["prepass_domains"], "healthy domain unaffected"

    def test_chunk_exception_never_records_miss(self, monkeypatch):
        # acme + 49 fillers fill chunk 1 (sorted); zeta lands in chunk 2.
        fillers = {f"f{i:02d}.com": f"https://www.linkedin.com/company/f{i:02d}" for i in range(49)}
        _wire_domain_lookup(
            monkeypatch,
            {
                "acme.com": "https://www.linkedin.com/company/acme",
                "zeta.com": "https://www.linkedin.com/company/zeta",
                **fillers,
            },
        )
        misses = []
        _wire_find_people(
            monkeypatch,
            {"linkedin.com/company/zeta": [_flat_row("Z", "zeta")]},
            fail_urls={"linkedin.com/company/acme"},
        )

        result = _run(
            domains=["acme.com", "zeta.com", *fillers],
            record_miss=lambda domain, kind: misses.append((domain, kind)),
        )

        assert misses == [], "chunk failure is not a conclusive answer"
        assert "acme.com" not in result["prepass_domains"], "chunk-failed domain falls back"
        assert "zeta.com" in result["prepass_domains"], "other chunk unaffected"
        assert "f00.com" not in result["prepass_domains"], "whole failed chunk falls back"

    def test_record_miss_raising_does_not_break_prepass(self, monkeypatch):
        _wire_domain_lookup(monkeypatch, {"acme.com": None})

        def exploding_miss(domain, kind):
            raise RuntimeError("miss store unavailable")

        _wire_find_people(monkeypatch, {})
        result = _run(domains=["acme.com"], record_miss=exploding_miss)
        assert "acme.com" in result["prepass_domains"]


class TestPrepassSkippedMiss:
    def test_recent_miss_short_circuits_everything(self, monkeypatch):
        lookup = _wire_domain_lookup(
            monkeypatch,
            {
                "known-miss.com": "https://www.linkedin.com/company/x",
                "fresh.com": "https://www.linkedin.com/company/fresh",
            },
        )
        _wire_find_people(
            monkeypatch,
            {
                "linkedin.com/company/x": [_flat_row("X", "x")],
                "linkedin.com/company/fresh": [_flat_row("F", "fresh")],
            },
        )

        result = _run(
            domains=["known-miss.com", "fresh.com"],
            is_recent_miss=lambda domain: domain == "known-miss.com",
        )

        assert result["skipped_miss"] == {"known-miss.com"}
        assert "known-miss.com" not in result["prepass_domains"], (
            "skipped misses are never touched, disjoint from covered"
        )
        assert "known-miss.com" not in result["persons_by_domain"]
        assert "fresh.com" in result["prepass_domains"]
        assert lookup["domains"].count("known-miss.com") == 0, "no Blitz call for stored miss"

    def test_all_domains_missed_means_no_find_people_call(self, monkeypatch):
        _wire_domain_lookup(monkeypatch, {})
        batch = _wire_find_people(monkeypatch, {})

        result = _run(domains=["a.com", "b.com"], is_recent_miss=lambda domain: True)

        assert result["skipped_miss"] == {"a.com", "b.com"}
        assert batch["n"] == 0
        assert result["prepass_domains"] == set()


class TestPrepassChunkingAndOptions:
    def test_chunks_of_fifty_companies(self, monkeypatch):
        domains = [f"d{i}.com" for i in range(101)]
        mapping = {d: f"https://www.linkedin.com/company/c{i}" for i, d in enumerate(domains)}
        _wire_domain_lookup(monkeypatch, mapping)
        batch = _wire_find_people(monkeypatch, {})

        result = _run(domains=domains)

        assert batch["n"] == 3, "101 companies -> 3 chunks"
        assert len(batch["batches"][0]) == 50
        assert len(batch["batches"][1]) == 50
        assert len(batch["batches"][2]) == 1
        assert len(result["prepass_domains"]) == 101

    def test_exact_titles_wraps_includes_only(self, monkeypatch):
        _wire_domain_lookup(monkeypatch, {"acme.com": "https://www.linkedin.com/company/acme"})
        batch = _wire_find_people(monkeypatch, {})

        _run(
            domains=["acme.com"],
            title_include=["CEO", "Founder"],
            title_exclude=["intern"],
            exact_titles=True,
        )
        kwargs = batch["kwargs"][0]
        assert kwargs["job_title_include"] == ["[CEO]", "[Founder]"]
        assert kwargs["job_title_exclude"] == ["intern"], "excludes stay fuzzy"

        batch2 = _wire_find_people(monkeypatch, {})
        _run(
            domains=["acme.com"],
            title_include=["CEO", "Founder"],
            title_exclude=["intern"],
            exact_titles=False,
        )
        assert batch2["kwargs"][0]["job_title_include"] == ["CEO", "Founder"]

    def test_target_and_max_pages_forwarded(self, monkeypatch):
        _wire_domain_lookup(monkeypatch, {"acme.com": "https://www.linkedin.com/company/acme"})
        batch = _wire_find_people(monkeypatch, {})

        _run(domains=["acme.com"], target_per_company=7, max_pages=2)
        assert batch["kwargs"][0]["target_per_company"] == 7
        assert batch["kwargs"][0]["max_pages"] == 2

    def test_progress_event_per_chunk(self, monkeypatch):
        mapping = {f"d{i}.com": f"https://www.linkedin.com/company/c{i}" for i in range(60)}
        _wire_domain_lookup(monkeypatch, mapping)
        _wire_find_people(monkeypatch, {})
        events = []

        async def progress(message):
            events.append(message)

        _run(domains=list(mapping), on_progress=progress)
        assert events == [
            "blitz find-people prepass chunk 1/2",
            "blitz find-people prepass chunk 2/2",
        ]

    def test_progress_callback_failure_is_swallowed(self, monkeypatch):
        _wire_domain_lookup(monkeypatch, {"acme.com": "https://www.linkedin.com/company/acme"})
        _wire_find_people(monkeypatch, {})

        async def bad_progress(message):
            raise RuntimeError("sse closed")

        result = _run(domains=["acme.com"], on_progress=bad_progress)
        assert "acme.com" in result["prepass_domains"]

    def test_cancel_between_chunks_stops_early(self, monkeypatch):
        mapping = {f"d{i}.com": f"https://www.linkedin.com/company/c{i}" for i in range(80)}
        _wire_domain_lookup(monkeypatch, mapping)
        batch = _wire_find_people(monkeypatch, {})

        result = _run(domains=list(mapping), should_cancel=lambda: batch["n"] >= 1)

        assert batch["n"] == 1, "cancel checked between chunks stops chunk 2"
        assert len(result["prepass_domains"]) == 50
        assert "d79.com" not in result["prepass_domains"], (
            "unprocessed chunk domains fall back to the waterfall"
        )


class TestPrepassAdaptiveMaxPages:
    """Default max_pages (None) scales with target x chunk so target>5 jobs
    page enough to actually FILL targets; explicit caps pass through
    verbatim. Formula: min(30, max(6, ceil(target * chunk / 50) + 1))."""

    @pytest.mark.parametrize(
        "n_companies,target,expected",
        [
            (1, 5, 6),     # floor: small chunks never page below the legacy 6
            (50, 5, 6),    # legacy default exactly reproduced (target 5)
            (50, 10, 11),  # target 10 over a full chunk needs ~10 pages
            (3, 100, 7),   # small chunk still scales: ceil(300/50)+1
            (50, 200, 30), # ceiling cap
        ],
    )
    def test_default_max_pages_is_adaptive(self, monkeypatch, n_companies, target, expected):
        mapping = {f"d{i}.com": f"https://www.linkedin.com/company/c{i}" for i in range(n_companies)}
        _wire_domain_lookup(monkeypatch, mapping)
        batch = _wire_find_people(monkeypatch, {})

        _run(domains=list(mapping), target_per_company=target)
        assert batch["kwargs"][0]["max_pages"] == expected

    def test_explicit_max_pages_respected_verbatim(self, monkeypatch):
        # An explicit cap is a hard cap — even when the adaptive default for
        # this target/chunk would be larger (target 100, 1 company -> 3).
        _wire_domain_lookup(monkeypatch, {"acme.com": "https://www.linkedin.com/company/acme"})
        batch = _wire_find_people(monkeypatch, {})

        _run(domains=["acme.com"], target_per_company=100, max_pages=2)
        assert batch["kwargs"][0]["max_pages"] == 2

    def test_stats_out_param_forwarded(self, monkeypatch):
        # blitz_batch must pass the mutable stats dict so truncation is
        # observable per chunk (find_people_batch fills it).
        _wire_domain_lookup(monkeypatch, {"acme.com": "https://www.linkedin.com/company/acme"})
        batch = _wire_find_people(monkeypatch, {})

        _run(domains=["acme.com"])
        assert isinstance(batch["kwargs"][0].get("stats"), dict)


class TestPrepassTruncationMissSuppression:
    """Regression (2026-09-16): a chunk whose pagination stopped at the
    max_pages cap WITH a pending cursor is TRUNCATED, not conclusive —
    zero-person domains there must NOT get a 'contacts' miss marker (30-day
    poison on an unanswered question). They are still covered: the persons
    the prepass did get are used, empty list and all."""

    def test_truncated_chunk_records_no_contacts_miss_but_covers(self, monkeypatch):
        _wire_domain_lookup(monkeypatch, {"acme.com": "https://www.linkedin.com/company/acme"})
        misses = []
        _wire_find_people(
            monkeypatch,
            {},  # zero persons
            truncated_urls={"linkedin.com/company/acme"},  # cap hit, cursor pending
        )

        result = _run(
            domains=["acme.com"],
            record_miss=lambda domain, kind: misses.append((domain, kind)),
        )

        assert misses == [], "truncation is not a conclusive answer — no poison"
        assert "acme.com" in result["prepass_domains"], "domain still counts as covered"
        assert result["persons_by_domain"]["acme.com"] == []

    def test_truncation_suppression_is_per_chunk(self, monkeypatch):
        # Chunk 1 (sorted: acme + 49 fillers) truncated; chunk 2 (zeta)
        # conclusive zero -> ONLY zeta gets the contacts miss.
        fillers = {f"f{i:02d}.com": f"https://www.linkedin.com/company/f{i:02d}" for i in range(49)}
        _wire_domain_lookup(
            monkeypatch,
            {
                "acme.com": "https://www.linkedin.com/company/acme",
                "zeta.com": "https://www.linkedin.com/company/zeta",
                **fillers,
            },
        )
        misses = []
        _wire_find_people(
            monkeypatch,
            {},
            truncated_urls={"linkedin.com/company/acme"},
        )

        result = _run(
            domains=["acme.com", "zeta.com", *fillers],
            record_miss=lambda domain, kind: misses.append((domain, kind)),
        )

        assert misses == [("zeta.com", "contacts")], (
            "conclusive chunk still records; truncated chunk does not"
        )
        assert result["prepass_domains"] == {"acme.com", "zeta.com", *fillers}

    def test_truncated_chunk_with_persons_never_missed_anyway(self, monkeypatch):
        # A truncated chunk that DID return persons: hits were never misses;
        # the empty sibling is suppressed. Both covered.
        _wire_domain_lookup(
            monkeypatch,
            {
                "acme.com": "https://www.linkedin.com/company/acme",
                "beta.com": "https://www.linkedin.com/company/beta",
            },
        )
        misses = []
        _wire_find_people(
            monkeypatch,
            {"linkedin.com/company/acme": [_flat_row("A", "acme")]},
            truncated_urls={"linkedin.com/company/acme"},
        )

        result = _run(
            domains=["acme.com", "beta.com"],
            record_miss=lambda domain, kind: misses.append((domain, kind)),
        )

        assert misses == []
        assert [row["first_name"] for row in result["persons_by_domain"]["acme.com"]] == ["A"]
        assert result["persons_by_domain"]["beta.com"] == []
        assert result["prepass_domains"] == {"acme.com", "beta.com"}


class TestPrepassAttachmentEdgeCases:
    def test_persons_attached_via_normalized_url(self, monkeypatch):
        # domain_to_linkedin returns a trailing-slash URL; find_people_batch
        # keys are normalized — attachment must bridge the two shapes.
        _wire_domain_lookup(monkeypatch, {"acme.com": "https://www.linkedin.com/company/acme/"})
        _wire_find_people(
            monkeypatch, {"linkedin.com/company/acme": [_flat_row("A", "acme")]}
        )

        result = _run(domains=["acme.com"])
        assert [row["first_name"] for row in result["persons_by_domain"]["acme.com"]] == ["A"]

    def test_two_domains_same_company_both_covered(self, monkeypatch):
        _wire_domain_lookup(
            monkeypatch,
            {
                "acme.com": "https://www.linkedin.com/company/acme",
                "acme.co": "https://www.linkedin.com/company/acme/",
            },
        )
        _wire_find_people(monkeypatch, {"linkedin.com/company/acme": [_flat_row("A", "acme")]})

        result = _run(domains=["acme.com", "acme.co"])
        assert result["prepass_domains"] == {"acme.com", "acme.co"}
        assert result["persons_by_domain"]["acme.com"] == result["persons_by_domain"]["acme.co"]

    def test_empty_domain_list_is_a_noop(self, monkeypatch):
        lookup = _wire_domain_lookup(monkeypatch, {})
        batch = _wire_find_people(monkeypatch, {})
        result = _run(domains=[])
        assert result == {
            "company_url_by_domain": {},
            "persons_by_domain": {},
            "prepass_domains": set(),
            "skipped_miss": set(),
        }
        assert lookup["n"] == 0 and batch["n"] == 0
