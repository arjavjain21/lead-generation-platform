"""
Wave 2/3 tests: find-people prepass consumption + Blitz miss markers + DM
phone bundle in ``pipeline._enrich_domain`` / ``pipeline.run_pipeline``.

Scope:
  * Prepass consumption (``_enrich_domain``): a domain covered by the
    job-level ``blitz_prepass`` map reuses its company URL (d2l NOT called)
    and its persons (waterfall NOT called), feeding them through the SAME
    title gate + collector capture as waterfall results. A covered domain
    with zero persons is a confirmed DM miss -> 'contacts' marker recorded
    when absent, flow continues to the Step-2.4 GetLeads fallback.
  * Miss recording (legacy path, ``blitz_miss_skip=True``): clean d2l
    found=false -> 'company'; clean empty waterfall -> 'contacts'; NEVER on
    exceptions, NEVER when the title gate (not the provider) dropped the
    persons, and nothing at all when the wiring is not armed.
  * Miss skipping (legacy path): an unexpired marker short-circuits d2l AND
    the waterfall; the row takes the no-LI path (Contacts DB name+domain
    fallback when full_name is present, else a no_linkedin row).
  * Phone bundle: first eligible row only (US/unknown country + final email
    + dm LinkedIn URL), ``phone_for_all`` lifts the cap, exceptions are
    swallowed, include_phone=False and ENABLE_PHONE_BUNDLE=false never call.
  * Prepass launch (``run_pipeline``): fires only for >5 unique domain-only
    domains with no force_provider and the env flag on; ``exact_titles``
    reaches blitz_batch which applies bracket_exact to the title includes;
    a prepass failure degrades to the legacy per-domain waterfall; the
    result map is threaded into every ``_enrich_domain`` call.

DB isolation: the Blitz miss store writes to the shared jobs database. Every
test here patches ``pipeline._record_blitz_miss`` / ``pipeline._recent_blitz_miss``
(the pipeline-level seams) or disables ``ENABLE_BLITZ_MISS_SKIP`` — NOTHING in
this file ever touches the real blitz_domain_miss table.

Async pattern: the project does NOT use ``pytest-asyncio``. We wrap the code
under test in ``asyncio.run(...)`` inside synchronous test functions,
matching ``test_smartprospect_batch_prepass.py``.

Run:
    python -m pytest enrichment/tests/test_pipeline_prepass_miss_phone.py -v
"""

from __future__ import annotations

import asyncio
import inspect
import os
import sys
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Make sure the backend root is on sys.path so `enrichment` is importable
# regardless of where pytest is invoked from.
_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

os.environ.setdefault("CONTACTS_API_TOKEN", "test-token-from-suite")

from enrichment import pipeline as pipeline_mod  # noqa: E402
from enrichment import blitz_client as bc  # noqa: E402
from enrichment import blitz_batch  # noqa: E402
from enrichment import contacts_client as cc  # noqa: E402
from enrichment import better_enrich_client as bec  # noqa: E402
from enrichment import getleads_client as glc  # noqa: E402
from phone_enrichment import client as phone_client_mod  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DOMAIN = "prepass-acme.com"
_COMPANY_URL = "https://linkedin.com/company/prepass-acme"


def _flat_person(
    first: str = "Alice",
    last: str = "Alpha",
    title: str = "CEO",
    linkedin: str = "https://linkedin.com/in/alice-alpha",
    country: str = "US",
) -> dict[str, Any]:
    """One waterfall-FLAT person row, the shape blitz_batch.persons_by_domain
    carries (blitz_client.find_people_batch output)."""
    return {
        "first_name": first,
        "last_name": last,
        "full_name": f"{first} {last}",
        "title": title,
        "job_level": None,
        "linkedin_url": linkedin,
        "email": None,
        "verified_email": None,
        "headline": f"{title} at Acme",
        "location_city": "NYC",
        "location_country": country,
        "icp_tier": 1,
        "ranking": 1,
        "experiences": [
            {"job_title": title, "job_is_current": True,
             "company_linkedin_url": _COMPANY_URL},
        ],
    }


def _waterfall_person(
    first: str = "Alice",
    last: str = "Alpha",
    title: str = "CEO",
    linkedin: str = "https://linkedin.com/in/alice-alpha",
    country: str = "US",
) -> dict[str, Any]:
    """One waterfall result item (``{"person": {...}, "icp": N}`` shape)."""
    return {
        "person": {
            "first_name": first,
            "last_name": last,
            "full_name": f"{first} {last}",
            "title": title,
            "headline": f"{title} at Acme",
            "linkedin_url": linkedin,
            "location": {"city": "NYC", "country_code": country},
            "experiences": [],
        },
        "icp": 0,
    }


def _prepass_map(
    persons: list[dict[str, Any]] | None,
    *,
    covered: bool = True,
) -> dict[str, Any]:
    """A blitz_batch result map covering exactly ``_DOMAIN``."""
    persons_by_domain: dict[str, list[dict[str, Any]]] = {}
    company_url_by_domain: dict[str, str] = {}
    prepass_domains: set[str] = set()
    if covered:
        prepass_domains.add(_DOMAIN)
        if persons is not None:
            persons_by_domain[_DOMAIN] = persons
            company_url_by_domain[_DOMAIN] = _COMPANY_URL
    return {
        "company_url_by_domain": company_url_by_domain,
        "persons_by_domain": persons_by_domain,
        "prepass_domains": prepass_domains,
        "skipped_miss": set(),
    }


def _wire_no_company(monkeypatch: pytest.MonkeyPatch) -> None:
    """Patch Contacts DB so Step 1 falls through to the Blitz/prepass arm and
    Step 2 finds nobody: company_by_domain -> None, contacts -> [], the
    no-LI name+domain fallback -> None, BetterEnrich company email -> miss,
    GetLeads decision-makers -> [] (overridden per-test)."""

    async def fake_company_by_domain(http, domain):
        return None

    async def fake_company_contacts_enriched(http, domain, limit):
        return []

    async def fake_person_by_name_and_domain(http, full_name, domain):
        return None

    async def fake_find_company_email(http, website=None, **kw):
        return {"email": ""}

    async def fake_lookup_decision_makers(http, domain, limit=25):
        return []

    monkeypatch.setattr(cc, "company_by_domain", fake_company_by_domain)
    monkeypatch.setattr(cc, "company_contacts_enriched", fake_company_contacts_enriched)
    monkeypatch.setattr(cc, "person_by_name_and_domain", fake_person_by_name_and_domain)
    monkeypatch.setattr(bec, "find_company_email", fake_find_company_email)
    monkeypatch.setattr(glc, "lookup_decision_makers", fake_lookup_decision_makers)


def _patch_miss_seams(
    monkeypatch: pytest.MonkeyPatch,
    recent: Any = None,
) -> list[tuple[str, str]]:
    """Patch the pipeline miss seams; returns the recorded (domain, kind)
    pairs. ``recent`` is what _recent_blitz_miss returns (None = no marker)."""
    recorded: list[tuple[str, str]] = []

    def fake_record(domain: str, kind: str) -> None:
        recorded.append((domain, kind))

    async def _nothing_recent(domain: str):  # never used; clarity only
        return recent

    monkeypatch.setattr(pipeline_mod, "_record_blitz_miss", fake_record)
    monkeypatch.setattr(
        pipeline_mod, "_recent_blitz_miss", lambda domain: recent,
    )
    return recorded


def _run(coro):
    return asyncio.run(coro)


async def _enrich(**kwargs) -> list[dict[str, Any]]:
    """Invoke _enrich_domain with the standard argument skeleton."""
    defaults = dict(
        blitz_http=MagicMock(),
        contacts_http=MagicMock(),
        base_row={"domain": _DOMAIN},
        domain=_DOMAIN,
        full_name="",
        cascade=bc.DEFAULT_CASCADE,
        max_results=10,
        domain_semaphore=asyncio.Semaphore(1),
        email_semaphore=asyncio.Semaphore(1),
        validate_email=False,
    )
    defaults.update(kwargs)
    return await pipeline_mod._enrich_domain(**defaults)


# ---------------------------------------------------------------------------
# 0. Parameter plumbing
# ---------------------------------------------------------------------------


class TestParameterPlumbing:
    """Mechanical checks: new parameters exist with backward-compatible
    defaults so existing callers (routes, tests) are unaffected."""

    def test_enrich_domain_params_default_off(self):
        sig = inspect.signature(pipeline_mod._enrich_domain)
        for name in ("blitz_prepass", "blitz_miss_skip", "include_phone", "phone_for_all"):
            assert name in sig.parameters, f"_enrich_domain missing {name!r}"
            assert sig.parameters[name].default in (None, False), (
                f"{name!r} must default to off/None for backward compat"
            )

    def test_run_pipeline_params_default_off(self):
        sig = inspect.signature(pipeline_mod.run_pipeline)
        for name in ("exact_titles", "include_phone", "phone_for_all"):
            assert name in sig.parameters, f"run_pipeline missing {name!r}"
            assert sig.parameters[name].default is False, (
                f"{name!r} must default to False for backward compat"
            )

    def test_prepass_min_domains_constant(self):
        assert pipeline_mod._PREPASS_MIN_DOMAINS == 5


# ---------------------------------------------------------------------------
# 1. Prepass consumption (_enrich_domain)
# ---------------------------------------------------------------------------


class TestPrepassConsumption:
    """Domains covered by the job-level prepass reuse its answers."""

    def test_prepass_persons_injected_and_waterfall_not_called(self, monkeypatch):
        """Covered domain with persons: d2l AND waterfall are skipped; rows
        are built from the prepass persons; company URL comes from the map."""
        _wire_no_company(monkeypatch)
        d2l_mock = AsyncMock(return_value={"found": False})
        waterfall_mock = AsyncMock(return_value={"results": []})
        monkeypatch.setattr(bc, "domain_to_linkedin", d2l_mock)
        monkeypatch.setattr(bc, "waterfall_icp_search", waterfall_mock)
        per_row = AsyncMock(return_value=("alice@x.com", "contacts_db_email", {}))
        used: list[str] = []

        async def go():
            with patch.object(pipeline_mod, "_resolve_email_for_person", per_row):
                return await _enrich(
                    blitz_prepass=_prepass_map([_flat_person()]),
                    record_provider_use=used.append,
                )

        rows = _run(go())
        assert d2l_mock.await_count == 0, "prepass-covered domain must skip d2l"
        assert waterfall_mock.await_count == 0, "prepass-covered domain must skip the waterfall"
        assert len(rows) == 1
        row = rows[0]
        assert row["dm_full_name"] == "Alice Alpha"
        assert row["dm_linkedin_url"] == "https://linkedin.com/in/alice-alpha"
        assert row["dm_title"] == "CEO"
        assert row["company_linkedin_url"] == _COMPANY_URL
        assert row["dm_location_country"] == "US"
        assert "blitz" in used, "blitz was attempted (at the job level)"

    def test_prepass_persons_pass_through_title_gate(self, monkeypatch):
        """Prepass persons flow through the SAME local title gate as
        waterfall results: an off-ICP title is dropped, no API fallback."""
        _wire_no_company(monkeypatch)
        waterfall_mock = AsyncMock(return_value={"results": []})
        monkeypatch.setattr(bc, "waterfall_icp_search", waterfall_mock)
        per_row = AsyncMock(return_value=("alice@x.com", "contacts_db_email", {}))
        user_cascade = [{"include_title": ["Owner"], "exclude_title": []}]

        async def go():
            with patch.object(pipeline_mod, "_resolve_email_for_person", per_row):
                return await _enrich(
                    blitz_prepass=_prepass_map([
                        _flat_person("Alice", "Alpha", title="Owner"),
                        _flat_person("Bob", "Beta", title="Sales Representative"),
                    ]),
                    cascade=user_cascade,
                )

        rows = _run(go())
        assert len(rows) == 1, "off-ICP prepass person must be gated out"
        assert rows[0]["dm_first_name"] == "Alice"
        assert waterfall_mock.await_count == 0

    def test_prepass_persons_captured_by_collector(self, monkeypatch):
        """Collector capture fires for prepass persons exactly like waterfall
        results (source='blitz', one capture per person)."""
        from enrichment.raw_contact_collector import RawContactCollector

        _wire_no_company(monkeypatch)
        collector = RawContactCollector(job_id="prepass-cap")
        original_capture = collector.capture_company_contact
        blitz_captures: list[dict[str, Any]] = []

        def counting_capture(*, source, **kwargs):
            if source == "blitz":
                blitz_captures.append(kwargs)
            return original_capture(source=source, **kwargs)

        collector.capture_company_contact = counting_capture  # type: ignore[assignment]
        per_row = AsyncMock(return_value=("", "not_found", {}))

        async def go():
            with patch.object(pipeline_mod, "_resolve_email_for_person", per_row):
                return await _enrich(
                    blitz_prepass=_prepass_map([
                        _flat_person("Alice", "Alpha"),
                        _flat_person("Bob", "Beta", linkedin="https://linkedin.com/in/bob-beta"),
                    ]),
                    collector=collector,
                )

        rows = _run(go())
        assert len(rows) == 2
        assert len(blitz_captures) == 2, (
            f"expected 2 blitz captures from the prepass path, got {len(blitz_captures)}"
        )
        assert all(c.get("domain") == _DOMAIN for c in blitz_captures)

    def test_prepass_confirmed_zero_records_contacts_miss_and_falls_to_getleads(self, monkeypatch):
        """Covered domain with zero persons: 'contacts' miss recorded when no
        marker exists, and the Step-2.4 GetLeads DM fallback fires exactly
        like the waterfall-empty path."""
        _wire_no_company(monkeypatch)
        recorded = _patch_miss_seams(monkeypatch, recent=None)
        waterfall_mock = AsyncMock(return_value={"results": []})
        monkeypatch.setattr(bc, "waterfall_icp_search", waterfall_mock)

        async def fake_dm(http, domain, limit=25):
            return [{
                "person_full_name": "Grace Getleads",
                "first_name": "Grace",
                "last_name": "Getleads",
                "linkedin_url": "https://linkedin.com/in/grace",
                "job_title": "CEO",
                "email": "grace@gl.com",
            }]

        monkeypatch.setattr(glc, "lookup_decision_makers", fake_dm)
        per_row = AsyncMock(return_value=("grace@gl.com", "getleads", {}))

        async def go():
            with patch.object(pipeline_mod, "_resolve_email_for_person", per_row):
                return await _enrich(
                    blitz_prepass=_prepass_map([]),
                    blitz_miss_skip=True,
                )

        rows = _run(go())
        assert waterfall_mock.await_count == 0
        assert (_DOMAIN, "contacts") in recorded, (
            "confirmed-zero prepass domain must record a contacts miss when unmarked"
        )
        assert len(rows) == 1
        assert rows[0]["dm_full_name"] == "Grace Getleads"

    def test_prepass_zero_persons_keeps_existing_marker(self, monkeypatch):
        """When a 'contacts' marker already exists the belt-and-suspenders
        re-record is skipped (strictly no duplicate writes)."""
        _wire_no_company(monkeypatch)
        recorded = _patch_miss_seams(monkeypatch, recent="contacts")

        async def go():
            return await _enrich(
                blitz_prepass=_prepass_map([]),
                blitz_miss_skip=True,
            )

        _run(go())
        assert recorded == [], "existing marker must not be re-recorded"

    def test_prepass_uncovered_domain_uses_legacy_waterfall(self, monkeypatch):
        """A domain NOT in prepass_domains keeps the legacy behavior: its own
        d2l + waterfall calls."""
        _wire_no_company(monkeypatch)
        recorded = _patch_miss_seams(monkeypatch, recent=None)
        d2l_mock = AsyncMock(return_value={
            "found": True, "company_linkedin_url": _COMPANY_URL,
        })
        waterfall_mock = AsyncMock(return_value={
            "results": [_waterfall_person()],
        })
        monkeypatch.setattr(bc, "domain_to_linkedin", d2l_mock)
        monkeypatch.setattr(bc, "waterfall_icp_search", waterfall_mock)
        per_row = AsyncMock(return_value=("alice@x.com", "contacts_db_email", {}))

        async def go():
            with patch.object(pipeline_mod, "_resolve_email_for_person", per_row):
                return await _enrich(
                    blitz_prepass=_prepass_map([_flat_person()], covered=False),
                    blitz_miss_skip=True,
                )

        rows = _run(go())
        assert d2l_mock.await_count == 1, "uncovered domain must run its own d2l"
        assert waterfall_mock.await_count == 1, "uncovered domain must run its own waterfall"
        assert len(rows) == 1
        assert rows[0]["dm_full_name"] == "Alice Alpha"
        assert recorded == [], "non-empty waterfall must not record a miss"


# ---------------------------------------------------------------------------
# 2. Miss recording (legacy path)
# ---------------------------------------------------------------------------


class TestMissRecording:
    """Definitive-miss markers are recorded on CLEAN not-found responses
    only — never on exceptions, never on gate drops, never when disarmed."""

    def _wire_legacy(self, monkeypatch, d2l_return, waterfall_return=None, waterfall_raises=False):
        _wire_no_company(monkeypatch)
        d2l_mock = AsyncMock(return_value=d2l_return)
        monkeypatch.setattr(bc, "domain_to_linkedin", d2l_mock)
        if waterfall_raises:
            monkeypatch.setattr(
                bc, "waterfall_icp_search",
                AsyncMock(side_effect=RuntimeError("blitz 503")),
            )
        else:
            monkeypatch.setattr(
                bc, "waterfall_icp_search",
                AsyncMock(return_value=waterfall_return or {"results": []}),
            )
        per_row = AsyncMock(return_value=("", "not_found", {}))
        return d2l_mock, per_row

    def test_clean_d2l_found_false_records_company_miss(self, monkeypatch):
        recorded = _patch_miss_seams(monkeypatch, recent=None)
        d2l_mock, per_row = self._wire_legacy(monkeypatch, {"found": False})

        async def go():
            with patch.object(pipeline_mod, "_resolve_email_for_person", per_row):
                return await _enrich(blitz_miss_skip=True)

        rows = _run(go())
        assert (_DOMAIN, "company") in recorded
        assert all(kind != "contacts" for _d, kind in recorded)
        assert rows[0]["row_status"] == pipeline_mod.STATUS_NO_LINKEDIN

    def test_d2l_found_true_records_nothing(self, monkeypatch):
        recorded = _patch_miss_seams(monkeypatch, recent=None)
        d2l_mock, per_row = self._wire_legacy(
            monkeypatch,
            {"found": True, "company_linkedin_url": _COMPANY_URL},
            waterfall_return={"results": [_waterfall_person()]},
        )

        async def go():
            with patch.object(pipeline_mod, "_resolve_email_for_person", per_row):
                await _enrich(blitz_miss_skip=True)

        _run(go())
        assert recorded == [], "successful lookups must never be marked as misses"

    def test_clean_empty_waterfall_records_contacts_miss(self, monkeypatch):
        recorded = _patch_miss_seams(monkeypatch, recent=None)
        _d2l, per_row = self._wire_legacy(
            monkeypatch,
            {"found": True, "company_linkedin_url": _COMPANY_URL},
            waterfall_return={"results": []},
        )

        async def go():
            with patch.object(pipeline_mod, "_resolve_email_for_person", per_row):
                await _enrich(blitz_miss_skip=True)

        _run(go())
        assert (_DOMAIN, "contacts") in recorded

    def test_waterfall_exception_records_nothing(self, monkeypatch):
        recorded = _patch_miss_seams(monkeypatch, recent=None)
        _d2l, per_row = self._wire_legacy(
            monkeypatch,
            {"found": True, "company_linkedin_url": _COMPANY_URL},
            waterfall_raises=True,
        )

        async def go():
            with patch.object(pipeline_mod, "_resolve_email_for_person", per_row):
                return await _enrich(blitz_miss_skip=True)

        rows = _run(go())
        assert recorded == [], "a transient waterfall error is NOT a miss"
        assert rows[0]["row_status"] == pipeline_mod.STATUS_ERROR

    def test_gate_dropped_persons_record_nothing(self, monkeypatch):
        """Waterfall found people but the user's title gate dropped them all:
        the provider DID answer, so no miss marker."""
        recorded = _patch_miss_seams(monkeypatch, recent=None)
        _d2l, per_row = self._wire_legacy(
            monkeypatch,
            {"found": True, "company_linkedin_url": _COMPANY_URL},
            waterfall_return={"results": [_waterfall_person(title="Sales Rep")]},
        )
        user_cascade = [{"include_title": ["Owner"], "exclude_title": []}]

        async def go():
            with patch.object(pipeline_mod, "_resolve_email_for_person", per_row):
                await _enrich(blitz_miss_skip=True, cascade=user_cascade)

        _run(go())
        assert recorded == [], "gate-dropped (non-empty raw) results are not misses"

    def test_miss_wiring_inert_when_not_armed(self, monkeypatch):
        """blitz_miss_skip=False (the default for direct callers): a clean
        empty waterfall records NOTHING (protects test isolation)."""
        recorded = _patch_miss_seams(monkeypatch, recent=None)
        _d2l, per_row = self._wire_legacy(
            monkeypatch,
            {"found": True, "company_linkedin_url": _COMPANY_URL},
            waterfall_return={"results": []},
        )

        async def go():
            with patch.object(pipeline_mod, "_resolve_email_for_person", per_row):
                await _enrich()  # blitz_miss_skip defaults to False

        _run(go())
        assert recorded == []


# ---------------------------------------------------------------------------
# 3. Miss skipping (legacy path)
# ---------------------------------------------------------------------------


class TestMissSkipping:
    """An unexpired marker short-circuits the whole Blitz arm."""

    def test_recent_miss_skips_d2l_and_waterfall(self, monkeypatch):
        _wire_no_company(monkeypatch)
        _patch_miss_seams(monkeypatch, recent="company")
        d2l_mock = AsyncMock(return_value={"found": True, "company_linkedin_url": _COMPANY_URL})
        waterfall_mock = AsyncMock(return_value={"results": [_waterfall_person()]})
        monkeypatch.setattr(bc, "domain_to_linkedin", d2l_mock)
        monkeypatch.setattr(bc, "waterfall_icp_search", waterfall_mock)
        used: list[str] = []

        async def go():
            return await _enrich(blitz_miss_skip=True, record_provider_use=used.append)

        rows = _run(go())
        assert d2l_mock.await_count == 0, "stored miss must skip d2l"
        assert waterfall_mock.await_count == 0, "stored miss must skip the waterfall"
        assert "blitz" not in used, "miss-skip must not record blitz as attempted"
        assert rows[0]["row_status"] == pipeline_mod.STATUS_NO_LINKEDIN

    def test_recent_miss_with_full_name_uses_contacts_db_fallback(self, monkeypatch):
        """Documented ordering: a stored miss lands on the no-LI path, whose
        Contacts DB name+domain fallback still runs when full_name is set."""
        _wire_no_company(monkeypatch)
        _patch_miss_seams(monkeypatch, recent="contacts")
        d2l_mock = AsyncMock(return_value={"found": True, "company_linkedin_url": _COMPANY_URL})
        waterfall_mock = AsyncMock(return_value={"results": [_waterfall_person()]})
        monkeypatch.setattr(bc, "domain_to_linkedin", d2l_mock)
        monkeypatch.setattr(bc, "waterfall_icp_search", waterfall_mock)

        async def fake_pbnd(http, full_name, domain):
            return {
                "full_name": full_name,
                "first_name": "Jane",
                "last_name": "Doe",
                "email": "jane@contacts-db.com",
                "linkedin_url": "https://linkedin.com/in/jane",
                "headline": "CEO",
                "city": "NYC",
                "country_code": "US",
            }

        monkeypatch.setattr(cc, "person_by_name_and_domain", fake_pbnd)

        async def go():
            return await _enrich(blitz_miss_skip=True, full_name="Jane Doe")

        rows = _run(go())
        assert d2l_mock.await_count == 0
        assert waterfall_mock.await_count == 0
        assert len(rows) == 1
        assert rows[0]["dm_email"] == "jane@contacts-db.com"
        assert rows[0]["dm_email_source"] == pipeline_mod.SOURCE_CONTACTS_NAME


# ---------------------------------------------------------------------------
# 4. Phone bundle (_enrich_domain)
# ---------------------------------------------------------------------------


class TestPhoneBundle:
    """Blitz Direct Phone on eligible rows; never fails the domain."""

    def _wire_two_us_persons(self, monkeypatch):
        _wire_no_company(monkeypatch)
        d2l_mock = AsyncMock(return_value={
            "found": True, "company_linkedin_url": _COMPANY_URL,
        })
        waterfall_mock = AsyncMock(return_value={"results": [
            _waterfall_person("Alice", "Alpha", linkedin="https://linkedin.com/in/alice"),
            _waterfall_person("Bob", "Beta", linkedin="https://linkedin.com/in/bob"),
        ]})
        monkeypatch.setattr(bc, "domain_to_linkedin", d2l_mock)
        monkeypatch.setattr(bc, "waterfall_icp_search", waterfall_mock)
        per_row = AsyncMock(return_value=("x@example.com", "contacts_db_email", {}))
        return per_row

    def _patch_find_phone(self, monkeypatch, payload=None, raises=False):
        calls: list[str] = []

        async def fake_find_phone(http, linkedin_url):
            calls.append(linkedin_url)
            if raises:
                raise RuntimeError("phone api down")
            return payload if payload is not None else {
                "found": True, "phone": "+15550001000",
            }

        monkeypatch.setattr(phone_client_mod, "find_phone", fake_find_phone)
        return calls

    def _rows(self, per_row, **kwargs):
        async def go():
            with patch.object(pipeline_mod, "_resolve_email_for_person", per_row):
                return await _enrich(**kwargs)

        return _run(go())

    def test_phone_happy_path_first_eligible_row_only(self, monkeypatch):
        per_row = self._wire_two_us_persons(monkeypatch)
        calls = self._patch_find_phone(monkeypatch)

        rows = self._rows(per_row, include_phone=True)

        assert len(calls) == 1, "default cap is ONE phone call per domain"
        assert calls[0] == "https://linkedin.com/in/alice", "first eligible row wins"
        assert rows[0]["dm_phone"] == "+15550001000"
        assert rows[1]["dm_phone"] == "", "second row must stay unphoned under the cap"

    def test_phone_for_all_lifts_cap(self, monkeypatch):
        per_row = self._wire_two_us_persons(monkeypatch)
        calls = self._patch_find_phone(monkeypatch)

        rows = self._rows(per_row, include_phone=True, phone_for_all=True)

        assert len(calls) == 2
        assert rows[0]["dm_phone"] == "+15550001000"
        assert rows[1]["dm_phone"] == "+15550001000"

    def test_non_us_country_skipped(self, monkeypatch):
        _wire_no_company(monkeypatch)
        d2l_mock = AsyncMock(return_value={
            "found": True, "company_linkedin_url": _COMPANY_URL,
        })
        monkeypatch.setattr(bc, "domain_to_linkedin", d2l_mock)
        monkeypatch.setattr(bc, "waterfall_icp_search", AsyncMock(return_value={"results": [
            _waterfall_person(country="GB"),
        ]}))
        per_row = AsyncMock(return_value=("x@example.com", "contacts_db_email", {}))
        calls = self._patch_find_phone(monkeypatch)

        rows = self._rows(per_row, include_phone=True)

        assert calls == [], "non-US rows are skipped for free (US-only coverage)"
        assert rows[0]["dm_phone"] == ""

    def test_unknown_country_is_eligible(self, monkeypatch):
        _wire_no_company(monkeypatch)
        monkeypatch.setattr(bc, "domain_to_linkedin", AsyncMock(return_value={
            "found": True, "company_linkedin_url": _COMPANY_URL,
        }))
        monkeypatch.setattr(bc, "waterfall_icp_search", AsyncMock(return_value={"results": [
            _waterfall_person(country=""),
        ]}))
        per_row = AsyncMock(return_value=("x@example.com", "contacts_db_email", {}))
        calls = self._patch_find_phone(monkeypatch)

        rows = self._rows(per_row, include_phone=True)

        assert len(calls) == 1, "empty/unknown country counts as eligible"
        assert rows[0]["dm_phone"] == "+15550001000"

    def test_no_linkedin_url_skipped(self, monkeypatch):
        _wire_no_company(monkeypatch)
        monkeypatch.setattr(bc, "domain_to_linkedin", AsyncMock(return_value={
            "found": True, "company_linkedin_url": _COMPANY_URL,
        }))
        monkeypatch.setattr(bc, "waterfall_icp_search", AsyncMock(return_value={"results": [
            _waterfall_person(linkedin=""),
        ]}))
        per_row = AsyncMock(return_value=("x@example.com", "contacts_db_email", {}))
        calls = self._patch_find_phone(monkeypatch)

        rows = self._rows(per_row, include_phone=True)

        assert calls == []
        assert rows[0]["dm_phone"] == ""

    def test_no_final_email_skipped(self, monkeypatch):
        per_row_mock = self._wire_two_us_persons(monkeypatch)
        per_row = AsyncMock(return_value=("", "not_found", {}))
        calls = self._patch_find_phone(monkeypatch)

        rows = self._rows(per_row, include_phone=True)

        assert calls == [], "rows without a final email are not phoned"
        assert all(r["dm_phone"] == "" for r in rows)

    def test_find_phone_exception_swallowed(self, monkeypatch):
        per_row = self._wire_two_us_persons(monkeypatch)
        calls = self._patch_find_phone(monkeypatch, raises=True)

        rows = self._rows(per_row, include_phone=True)

        assert len(calls) == 1, "one attempt before the swallow"
        assert len(rows) == 2, "phone failure must never fail the rows"
        assert all(r["dm_phone"] == "" for r in rows)
        assert rows[0]["dm_email"] == "x@example.com"

    def test_include_phone_false_never_calls(self, monkeypatch):
        per_row = self._wire_two_us_persons(monkeypatch)
        calls = self._patch_find_phone(monkeypatch)

        rows = self._rows(per_row)  # include_phone defaults to False

        assert calls == []
        assert all(r["dm_phone"] == "" for r in rows)

    def test_env_kill_switch_blocks_bundle(self, monkeypatch):
        monkeypatch.setenv("ENABLE_PHONE_BUNDLE", "false")
        per_row = self._wire_two_us_persons(monkeypatch)
        calls = self._patch_find_phone(monkeypatch)

        rows = self._rows(per_row, include_phone=True)

        assert calls == [], "ENABLE_PHONE_BUNDLE=false must suppress the calls"
        assert all(r["dm_phone"] == "" for r in rows)


# ---------------------------------------------------------------------------
# 5. Prepass launch (run_pipeline)
# ---------------------------------------------------------------------------


def _run_pipeline_rows(n: int = 6) -> list[dict[str, Any]]:
    return [{"website": f"pp{i}.com"} for i in range(n)]


def _fake_enrich_domain(rows_out: dict[str, int] | None = None):
    calls: list[dict[str, Any]] = []

    async def fake(*args, **kwargs):
        calls.append(kwargs)
        return [{**pipeline_mod._empty_enriched(), "input_domain": "x",
                 "row_status": pipeline_mod.STATUS_ENRICHED}]

    return fake, calls


async def _fake_route(*args, **kwargs):
    return {
        "email": "", "source": "", "provider_attempts": [],
        "provider_attempts_json": [], "providers_called": [],
        "providers_skipped": [], "no_email_reason": "",
        "final_email_status": "", "source_path": "",
    }


def _drive(monkeypatch, tmp_path, rows, **pipeline_kwargs):
    """Run run_pipeline with every downstream side effect stubbed."""
    fake_enrich, enrich_calls = _fake_enrich_domain()
    progress: list[dict[str, Any]] = []

    async def on_progress(e):
        progress.append(e)

    monkeypatch.setattr(pipeline_mod, "AUDIT_SIDECAR_DIR", tmp_path)
    full_kwargs = dict(
        cascade=bc.DEFAULT_CASCADE,
        linkedin_url_col=None,
    )
    full_kwargs.update(pipeline_kwargs)
    with patch.object(pipeline_mod, "_enrich_domain", fake_enrich), \
         patch.object(pipeline_mod, "run_enrichment_route", _fake_route), \
         patch.object(pipeline_mod, "_maybe_apply_company_fallbacks",
                      new=AsyncMock(return_value=None)):
        result = asyncio.run(pipeline_mod.run_pipeline(
            rows,
            domain_col="website",
            name_col=None,
            first_name_col=None,
            last_name_col=None,
            max_results=5,
            on_progress=on_progress,
            use_email_cache=False,
            **full_kwargs,
        ))
    return result, enrich_calls, progress


class TestPrepassLaunch:
    """The job-level prepass fires under the documented gates only."""

    def test_launches_above_five_domain_only_domains(self, monkeypatch, tmp_path):
        launched: dict[str, Any] = {}

        async def fake_prepass(http, domains, **kwargs):
            launched["domains"] = list(domains)
            launched["kwargs"] = kwargs
            return {
                "company_url_by_domain": {},
                "persons_by_domain": {},
                "prepass_domains": set(),
                "skipped_miss": set(),
            }

        monkeypatch.setattr(blitz_batch, "blitz_find_people_prepass", fake_prepass)
        result, enrich_calls, _p = _drive(monkeypatch, tmp_path, _run_pipeline_rows(6))
        assert "domains" in launched, "6 domain-only domains must arm the prepass"
        assert launched["domains"] == [f"pp{i}.com" for i in range(6)]
        assert len(result) == 6
        assert all(kw.get("blitz_prepass") == {
            "company_url_by_domain": {}, "persons_by_domain": {},
            "prepass_domains": set(), "skipped_miss": set(),
        } for kw in enrich_calls), "the prepass map must thread into _enrich_domain"

    def test_not_launched_at_five_domains(self, monkeypatch, tmp_path):
        launched = {"n": 0}

        async def fake_prepass(http, domains, **kwargs):
            launched["n"] += 1
            return {"company_url_by_domain": {}, "persons_by_domain": {},
                    "prepass_domains": set(), "skipped_miss": set()}

        monkeypatch.setattr(blitz_batch, "blitz_find_people_prepass", fake_prepass)
        _drive(monkeypatch, tmp_path, _run_pipeline_rows(5))
        assert launched["n"] == 0, "5 domains is not > 5 — no prepass"

    def test_not_launched_with_force_provider(self, monkeypatch, tmp_path):
        launched = {"n": 0}

        async def fake_prepass(http, domains, **kwargs):
            launched["n"] += 1
            return {"company_url_by_domain": {}, "persons_by_domain": {},
                    "prepass_domains": set(), "skipped_miss": set()}

        monkeypatch.setattr(blitz_batch, "blitz_find_people_prepass", fake_prepass)
        _drive(monkeypatch, tmp_path, _run_pipeline_rows(6), force_provider="blitz")
        assert launched["n"] == 0, "force_provider must exclude the prepass"

    def test_not_launched_when_env_disabled(self, monkeypatch, tmp_path):
        monkeypatch.setenv("ENABLE_BLITZ_FIND_PEOPLE_BATCH", "false")
        launched = {"n": 0}

        async def fake_prepass(http, domains, **kwargs):
            launched["n"] += 1
            return {"company_url_by_domain": {}, "persons_by_domain": {},
                    "prepass_domains": set(), "skipped_miss": set()}

        monkeypatch.setattr(blitz_batch, "blitz_find_people_prepass", fake_prepass)
        _drive(monkeypatch, tmp_path, _run_pipeline_rows(6))
        assert launched["n"] == 0

    def test_prepass_failure_degrades_to_legacy(self, monkeypatch, tmp_path):
        """A raising prepass is logged and swallowed: every row still runs
        through _enrich_domain with blitz_prepass=None."""

        async def failing_prepass(http, domains, **kwargs):
            raise RuntimeError("simulated prepass outage")

        monkeypatch.setattr(blitz_batch, "blitz_find_people_prepass", failing_prepass)
        result, enrich_calls, _p = _drive(monkeypatch, tmp_path, _run_pipeline_rows(6))
        assert len(result) == 6, "rows must still complete after a prepass failure"
        assert all(kw.get("blitz_prepass") is None for kw in enrich_calls)

    def test_person_rows_never_prepassed(self, monkeypatch, tmp_path):
        """Rows carrying a strong identifier route to the person path — their
        domains must NOT be prepassed (records nobody would consume)."""
        launched: list[list[str]] = []

        async def fake_prepass(http, domains, **kwargs):
            launched.append(list(domains))
            return {"company_url_by_domain": {}, "persons_by_domain": {},
                    "prepass_domains": set(), "skipped_miss": set()}

        monkeypatch.setattr(blitz_batch, "blitz_find_people_prepass", fake_prepass)
        rows = [
            {"website": f"r{i}.com", "linkedin_url": f"https://linkedin.com/in/u{i}"}
            for i in range(6)
        ]
        _drive(monkeypatch, tmp_path, rows, linkedin_url_col="linkedin_url")
        assert launched == [], "routed rows must not arm the prepass"


class TestExactTitlesReachesPrepass:
    """exact_titles flows run_pipeline -> blitz_find_people_prepass ->
    blitz_client.bracket_exact -> find_people_batch(job_title_include)."""

    def _drive_real_prepass(self, monkeypatch, tmp_path, *, exact_titles, cascade=None):
        monkeypatch.setenv("ENABLE_BLITZ_MISS_SKIP", "false")  # no store access
        captured: dict[str, Any] = {}

        async def fake_d2l(http, domain):
            return {
                "found": True,
                "company_linkedin_url": f"https://linkedin.com/company/{domain.split('.')[0]}",
            }

        async def fake_find_people_batch(http, urls, **kwargs):
            captured["urls"] = list(urls)
            captured["kwargs"] = kwargs
            return {}  # nobody found -> empty persons everywhere

        monkeypatch.setattr(bc, "domain_to_linkedin", fake_d2l)
        monkeypatch.setattr(bc, "find_people_batch", fake_find_people_batch)
        result, enrich_calls, _p = _drive(
            monkeypatch, tmp_path, _run_pipeline_rows(6),
            exact_titles=exact_titles, cascade=cascade or bc.DEFAULT_CASCADE,
        )
        return captured, result

    def test_exact_titles_true_brackets_the_includes(self, monkeypatch, tmp_path):
        captured, _result = self._drive_real_prepass(monkeypatch, tmp_path, exact_titles=True)
        default_t12 = [
            t for tier in bc.DEFAULT_CASCADE[:2] for t in tier["include_title"]
        ]
        assert captured["kwargs"]["job_title_include"] == [f"[{t}]" for t in default_t12], (
            "exact_titles=True must wrap every include in [...] via bracket_exact"
        )
        assert captured["kwargs"]["job_title_exclude"] == list(dict.fromkeys(
            t for tier in bc.DEFAULT_CASCADE for t in tier["exclude_title"]
        )), "excludes stay fuzzy/unbracketed, deduped, with DEFAULT_CASCADE excludes included"

    def test_exact_titles_false_keeps_plain_includes(self, monkeypatch, tmp_path):
        captured, _result = self._drive_real_prepass(monkeypatch, tmp_path, exact_titles=False)
        default_t12 = [
            t for tier in bc.DEFAULT_CASCADE[:2] for t in tier["include_title"]
        ]
        assert captured["kwargs"]["job_title_include"] == default_t12

    def test_user_titles_win_over_defaults(self, monkeypatch, tmp_path):
        user_cascade = [{
            "include_title": ["VP Marketing"],
            "exclude_title": ["consultant"],
        }]
        captured, _result = self._drive_real_prepass(
            monkeypatch, tmp_path, exact_titles=True, cascade=user_cascade,
        )
        assert captured["kwargs"]["job_title_include"] == ["[VP Marketing]"]
        assert "consultant" in captured["kwargs"]["job_title_exclude"]
