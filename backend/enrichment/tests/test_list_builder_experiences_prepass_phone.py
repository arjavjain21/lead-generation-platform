"""Wave-3 wiring tests: past-experiences end-to-end + Flow-1 prepass /
miss-skip / phone bundle.

Covers the 2026-09-16 list_builder wave:
  1. ``response_normalizer.previous_companies_titles`` — the ONE canonical
     extraction (order, current-job exclusion, title dedup, 500-char cap,
     total-function behavior).
  2. ``_build_record`` canonical keys — every-key-always-present invariant
     now includes previous_companies / previous_titles.
  3. ``RawContactCollector.capture_company_contact`` — optional
     dm_previous_companies / dm_previous_titles payload keys (present only
     when non-empty; both the flat find-people shape and the waterfall
     ``{"person": ...}`` shape feed it).
  4. ``contacts_writer`` — the custom_fields carry (payload key ->
     custom_fields key) for the new pair.
  5. ``ENRICHED_COLUMNS`` append-only ordering (seg pair stays the
     last-but-one pair, the experiences pair is appended AFTER it) +
     ``_empty_enriched`` lockstep.
  6. Miss-store wiring in ``_enrich_single_domain`` — is_recent_miss skips
     the paid d2l replay (no-LI path); clean found=false records
     'company'; clean waterfall-empty records 'contacts'; exceptions
     NEVER record.
  7. Prepass consumption — covered domains skip the waterfall, prepass
     persons flow through the SAME collector capture + local title gate,
     experiences reach the output rows; uncovered domains keep the
     waterfall.
  8. ``run_domain_enrichment`` prepass launch gating (>5 domains, env
     flag, provider selection, website_only; exception -> legacy path;
     exact_titles forwarded; prepass progress mapped onto on_progress).
  9. Phone bundle — happy path, non-US skip, unknown-country eligibility,
     fill-only, single-attempt-on-exception, phone_for_all, and the
     run_domain_enrichment include_phone wiring.

Network is fully mocked (provider stubs mirror test_seg_csv /
test_cascade_collector_wiring); the miss store is monkeypatched at the
module so no sqlite is touched.

Run:
    python -m pytest enrichment/tests/test_list_builder_experiences_prepass_phone.py -v
"""

from __future__ import annotations

import asyncio
import os
import sys
import unittest
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

os.environ.setdefault("CONTACTS_API_TOKEN", "test-token-from-suite")

from enrichment import list_builder as lb  # noqa: E402
from enrichment import blitz_miss_store  # noqa: E402
from enrichment import contacts_writer as cw  # noqa: E402
from enrichment import response_normalizer as rn  # noqa: E402
from enrichment.raw_contact_collector import RawContactCollector  # noqa: E402

# A cascade carrying user titles — differs from blitz_client.DEFAULT_CASCADE,
# so title_filter.gate_title_filter activates the local gate.
_TITLES_CASCADE = (
    '[{"include_title": ["CEO"], "exclude_title": ["assistant"], '
    '"location": ["WORLD"], "include_headline_search": false}]'
)


def _exp(
    company: str,
    title: str,
    *,
    current: bool = False,
) -> dict[str, Any]:
    """One Blitz-style experiences[] entry."""
    return {
        "company_name": company,
        "job_title": title,
        "job_is_current": current,
    }


def _flat_person(name: str = "Pat Doe", *, title: str = "CEO") -> dict[str, Any]:
    """One find_people_batch waterfall-flat row (blitz_client._append_batch_person)."""
    parts = name.split(" ")
    return {
        "first_name": parts[0],
        "last_name": parts[1] if len(parts) > 1 else "",
        "full_name": name,
        "title": title,
        "job_level": None,
        "linkedin_url": f"https://www.linkedin.com/in/{name.lower().replace(' ', '-')}",
        "email": None,
        "verified_email": None,
        "headline": f"{title} at somewhere",
        "location_city": "Austin",
        "location_country": "US",
        "icp_tier": 1,
        "ranking": 1,
        "experiences": [
            _exp("Current Corp", title, current=True),
            _exp("Past One", "Founder"),
            _exp("Past Two", "founder"),
        ],
    }


def _waterfall_person(name: str = "Blitz Person") -> dict[str, Any]:
    """One waterfall_icp_search result ({"person": ..., "icp": N})."""
    parts = name.split(" ")
    return {
        "person": {
            "first_name": parts[0],
            "last_name": parts[1] if len(parts) > 1 else "",
            "full_name": name,
            "title": "CEO",
            "headline": "Chief Executive",
            "linkedin_url": "https://www.linkedin.com/in/blitz-person",
            "location": {"city": "SF", "country_code": "US"},
            "experiences": [
                _exp("Now Inc", "CEO", current=True),
                _exp("Before Inc", "CTO"),
            ],
        },
        "icp": 0,
    }


def _miss_store_stubs(recorded: list[tuple[str, str]], miss_kind: Any = None):
    """Patch pair for blitz_miss_store: is_recent_miss -> miss_kind (constant),
    record_miss -> append (domain, kind). Returns the two patch objects."""
    return (
        patch.object(blitz_miss_store, "is_recent_miss", lambda domain: miss_kind),
        patch.object(
            blitz_miss_store,
            "record_miss",
            lambda domain, kind: recorded.append((domain, kind)),
        ),
    )


def _domain_stubs(
    *,
    company: Any = None,
    contacts: Any = None,
    d2l: Any = None,
    waterfall: Any = None,
    getleads_dm: Any = None,
):
    """Standard _enrich_single_domain surface mocks."""
    return [
        patch.object(
            lb.contacts_client, "company_by_domain",
            new=AsyncMock(return_value=company if company is not None else {}),
        ),
        patch.object(
            lb.contacts_client, "company_contacts_enriched",
            new=AsyncMock(return_value=contacts if contacts is not None else []),
        ),
        patch.object(
            lb.contacts_client, "person_by_name_and_domain",
            new=AsyncMock(return_value=None),
        ),
        patch.object(
            lb.contacts_client, "person_by_linkedin",
            new=AsyncMock(return_value=None),
        ),
        patch.object(
            lb.blitz_client, "domain_to_linkedin",
            new=d2l if d2l is not None else AsyncMock(
                return_value={"found": False, "company_linkedin_url": ""}
            ),
        ),
        patch.object(
            lb.blitz_client, "waterfall_icp_search",
            new=waterfall if waterfall is not None else AsyncMock(return_value={"results": []}),
        ),
        patch.object(
            lb.getleads_client, "lookup_decision_makers",
            new=getleads_dm if getleads_dm is not None else AsyncMock(return_value=[]),
        ),
        patch.object(
            lb, "_resolve_person_email",
            new=AsyncMock(return_value=("found@acme.test", "", "blitz_name", "yes", "", "", {})),
        ),
    ]


# ---------------------------------------------------------------------------
# 1-2: canonical extraction + normalizer keys
# ---------------------------------------------------------------------------


class TestPreviousCompaniesTitles(unittest.TestCase):
    def test_order_and_current_exclusion(self):
        experiences = [
            _exp("Now Inc", "CEO", current=True),
            _exp("Past A", "CTO"),
            _exp("Past B", "Head of Sales"),
        ]
        companies, titles = rn.previous_companies_titles(experiences)
        self.assertEqual(companies, "Past A | Past B")
        self.assertEqual(titles, "CTO | Head of Sales")

    def test_titles_deduped_case_insensitively_first_casing_wins(self):
        experiences = [
            _exp("A", "Founder"),
            _exp("B", "founder"),
            _exp("C", "COO"),
        ]
        companies, titles = rn.previous_companies_titles(experiences)
        self.assertEqual(titles, "Founder | COO")
        # companies are NOT deduped — two stints at one company is signal
        self.assertEqual(companies, "A | B | C")

    def test_cap_500_chars_each_field(self):
        many = [_exp(f"Company {i:03d}", f"Title {i:03d}") for i in range(100)]
        companies, titles = rn.previous_companies_titles(many)
        self.assertLessEqual(len(companies), 500)
        self.assertLessEqual(len(titles), 500)
        # the cap trims the JOINED string, never mangles an entry
        self.assertTrue(companies.startswith("Company 000 | Company 001"))

    def test_custom_max_chars(self):
        experiences = [_exp("A", "T1"), _exp("B", "T2")]
        companies, titles = rn.previous_companies_titles(experiences, max_chars=3)
        self.assertEqual(companies, "A |")
        self.assertEqual(titles, "T1 ")

    def test_total_function_junk_inputs(self):
        for junk in (None, "nope", 42, {}, [None, 42, "str"], [{"job_is_current": False}]):
            self.assertEqual(rn.previous_companies_titles(junk), ("", ""))

    def test_non_string_fields_skipped(self):
        experiences = [
            {"company_name": 42, "job_title": None, "job_is_current": False},
            _exp("Real", "Role"),
        ]
        companies, titles = rn.previous_companies_titles(experiences)
        self.assertEqual(companies, "Real")
        self.assertEqual(titles, "Role")

    def test_build_record_keys_always_present(self):
        record = rn.normalize_blitz_contact({
            "first_name": "A",
            "last_name": "B",
            "linkedin_url": "https://www.linkedin.com/in/a-b",
        })
        self.assertIn("previous_companies", record)
        self.assertIn("previous_titles", record)
        self.assertEqual(record["previous_companies"], "")
        self.assertEqual(record["previous_titles"], "")

    def test_normalize_blitz_contact_carries_experiences(self):
        record = rn.normalize_blitz_contact({
            "first_name": "A",
            "last_name": "B",
            "linkedin_url": "https://www.linkedin.com/in/a-b",
            "experiences": [_exp("Past", "CTO")],
        })
        self.assertEqual(record["previous_companies"], "Past")
        self.assertEqual(record["previous_titles"], "CTO")


# ---------------------------------------------------------------------------
# 3-4: collector + writer carry
# ---------------------------------------------------------------------------


class TestCollectorAndWriterCarry(unittest.TestCase):
    def test_collector_flat_person_carries_previous_fields(self):
        collector = RawContactCollector(job_id="job-x")
        captured = collector.capture_company_contact(
            source="blitz",
            domain="acme.com",
            company_linkedin_url="https://www.linkedin.com/company/acme",
            contact=_flat_person(),
        )
        self.assertTrue(captured)
        payload = collector.to_payloads()[0]
        self.assertEqual(payload["dm_previous_companies"], "Past One | Past Two")
        self.assertEqual(payload["dm_previous_titles"], "Founder")

    def test_collector_waterfall_shape_carries_previous_fields(self):
        collector = RawContactCollector()
        captured = collector.capture_company_contact(
            source="blitz",
            domain="acme.com",
            company_linkedin_url="",
            contact=_waterfall_person(),
        )
        self.assertTrue(captured)
        payload = collector.to_payloads()[0]
        self.assertEqual(payload["dm_previous_companies"], "Before Inc")
        self.assertEqual(payload["dm_previous_titles"], "CTO")

    def test_collector_omits_keys_without_experiences(self):
        collector = RawContactCollector()
        collector.capture_company_contact(
            source="contacts_db",
            domain="acme.com",
            company_linkedin_url="",
            contact={"full_name": "No History", "email": "n@acme.com"},
        )
        payload = collector.to_payloads()[0]
        self.assertNotIn("dm_previous_companies", payload)
        self.assertNotIn("dm_previous_titles", payload)

    def test_firmographic_tuples_include_previous_pair(self):
        entries = cw._FIRMOGRAPHIC_CUSTOM_FIELDS
        self.assertIn(("previous_companies", "dm_previous_companies"), entries)
        self.assertIn(("previous_titles", "dm_previous_titles"), entries)
        # FIRST element is the custom_fields key, SECOND the payload key.
        cf_keys = [e[0] for e in entries]
        self.assertIn("previous_companies", cf_keys)

    def test_writer_payload_carries_previous_pair_into_custom_fields(self):
        bodies: list[dict] = []

        async def fake_do_upsert(client, body, payload, job_id=None, row_index=None, kind="person"):
            bodies.append(body)
            return cw.WriteStatus.SYNCED

        payload = {
            "dm_email": "a@acme.com",
            "domain": "acme.com",
            "dm_previous_companies": "Past One | Past Two",
            "dm_previous_titles": "Founder",
        }
        with patch.object(cw, "_do_upsert", new=fake_do_upsert):
            asyncio.run(cw.write_enrichment_result(payload, job_id="j", row_index=0))
        cf = bodies[0].get("custom_fields") or {}
        self.assertEqual(cf.get("previous_companies"), "Past One | Past Two")
        self.assertEqual(cf.get("previous_titles"), "Founder")

    def test_writer_blank_previous_pair_not_written(self):
        bodies: list[dict] = []

        async def fake_do_upsert(client, body, payload, job_id=None, row_index=None, kind="person"):
            bodies.append(body)
            return cw.WriteStatus.SYNCED

        with patch.object(cw, "_do_upsert", new=fake_do_upsert):
            asyncio.run(cw.write_enrichment_result(
                {"dm_email": "a@acme.com", "domain": "acme.com",
                 "dm_previous_companies": "", "dm_previous_titles": ""},
                job_id="j", row_index=0,
            ))
        cf = bodies[0].get("custom_fields") or {}
        self.assertNotIn("previous_companies", cf)
        self.assertNotIn("previous_titles", cf)


# ---------------------------------------------------------------------------
# 5: ENRICHED_COLUMNS append-only ordering
# ---------------------------------------------------------------------------


class TestEnrichedColumnsAppendOnly(unittest.TestCase):
    def test_experiences_pair_is_last_seg_pair_before_it(self):
        cols = lb.ENRICHED_COLUMNS
        self.assertEqual(
            cols[-2:], ["dm_previous_companies", "dm_previous_titles"]
        )
        self.assertEqual(cols[-4:-2], ["seg_classification", "seg_provider"])

    def test_columns_present_exactly_once(self):
        for col in ("dm_previous_companies", "dm_previous_titles"):
            self.assertEqual(lb.ENRICHED_COLUMNS.count(col), 1)

    def test_empty_enriched_lockstep(self):
        empty = lb._empty_enriched()
        self.assertEqual(set(empty.keys()), set(lb.ENRICHED_COLUMNS))
        self.assertEqual(empty["dm_previous_companies"], "")
        self.assertEqual(empty["dm_previous_titles"], "")

    def test_waterfall_flat_rows_carry_previous_fields(self):
        """_enrich_by_company_waterfall derives the pair onto its flat rows."""

        async def run() -> list[dict]:
            return await lb._enrich_by_company_waterfall(
                blitz_http=MagicMock(),
                company_url="https://www.linkedin.com/company/acme",
                cascade=lb.blitz_client.DEFAULT_CASCADE,
                max_dms=2,
                semaphore=asyncio.Semaphore(1),
            )

        with patch.object(
            lb.blitz_client, "waterfall_icp_search",
            new=AsyncMock(return_value={"results": [_waterfall_person()]}),
        ):
            flat = asyncio.run(run())
        self.assertEqual(flat[0]["previous_companies"], "Before Inc")
        self.assertEqual(flat[0]["previous_titles"], "CTO")


# ---------------------------------------------------------------------------
# 6: miss-store wiring semantics
# ---------------------------------------------------------------------------


class TestMissStoreWiring(unittest.TestCase):
    def test_recent_miss_skips_domain_to_linkedin(self):
        recorded: list[tuple[str, str]] = []
        d2l = AsyncMock(return_value={"found": True, "company_linkedin_url": "https://x"})
        patches = _domain_stubs(d2l=d2l)
        miss_patches = _miss_store_stubs(recorded, miss_kind="company")
        for p in patches + list(miss_patches):
            p.start()
        try:
            rows = asyncio.run(lb._enrich_single_domain(
                blitz_http=MagicMock(), contacts_http=MagicMock(),
                base_row={"domain": "acme.com"}, domain="acme.com",
                max_decision_makers=2, domain_semaphore=asyncio.Semaphore(1),
                email_semaphore=asyncio.Semaphore(1), validate_email=False,
                blitz_miss_skip=True,
            ))
        finally:
            for p in patches + list(miss_patches):
                p.stop()
        d2l.assert_not_awaited()
        self.assertEqual(rows[0]["row_status"], lb.STATUS_NO_LINKEDIN)
        # nothing recorded on a skip — the marker already exists
        self.assertEqual(recorded, [])

    def test_clean_company_miss_is_recorded(self):
        recorded: list[tuple[str, str]] = []
        patches = _domain_stubs()  # d2l default: found=False
        miss_patches = _miss_store_stubs(recorded, miss_kind=None)
        for p in patches + list(miss_patches):
            p.start()
        try:
            asyncio.run(lb._enrich_single_domain(
                blitz_http=MagicMock(), contacts_http=MagicMock(),
                base_row={"domain": "acme.com"}, domain="acme.com",
                max_decision_makers=2, domain_semaphore=asyncio.Semaphore(1),
                email_semaphore=asyncio.Semaphore(1), validate_email=False,
                blitz_miss_skip=True,
            ))
        finally:
            for p in patches + list(miss_patches):
                p.stop()
        self.assertIn(("acme.com", "company"), recorded)

    def test_d2l_exception_never_records(self):
        recorded: list[tuple[str, str]] = []
        patches = _domain_stubs(d2l=AsyncMock(side_effect=RuntimeError("boom")))
        miss_patches = _miss_store_stubs(recorded, miss_kind=None)
        for p in patches + list(miss_patches):
            p.start()
        try:
            asyncio.run(lb._enrich_single_domain(
                blitz_http=MagicMock(), contacts_http=MagicMock(),
                base_row={"domain": "acme.com"}, domain="acme.com",
                max_decision_makers=2, domain_semaphore=asyncio.Semaphore(1),
                email_semaphore=asyncio.Semaphore(1), validate_email=False,
                blitz_miss_skip=True,
            ))
        finally:
            for p in patches + list(miss_patches):
                p.stop()
        self.assertEqual(recorded, [])

    def test_clean_waterfall_empty_records_contacts(self):
        recorded: list[tuple[str, str]] = []
        company = {"linkedin_url": "https://www.linkedin.com/company/acme"}
        patches = _domain_stubs(company=company, waterfall=AsyncMock(return_value={"results": []}))
        miss_patches = _miss_store_stubs(recorded, miss_kind=None)
        for p in patches + list(miss_patches):
            p.start()
        try:
            rows = asyncio.run(lb._enrich_single_domain(
                blitz_http=MagicMock(), contacts_http=MagicMock(),
                base_row={"domain": "acme.com"}, domain="acme.com",
                max_decision_makers=2, domain_semaphore=asyncio.Semaphore(1),
                email_semaphore=asyncio.Semaphore(1), validate_email=False,
                blitz_miss_skip=True,
            ))
        finally:
            for p in patches + list(miss_patches):
                p.stop()
        self.assertIn(("acme.com", "contacts"), recorded)
        self.assertEqual(rows[0]["row_status"], lb.STATUS_NO_CONTACTS)

    def test_waterfall_exception_never_records(self):
        recorded: list[tuple[str, str]] = []
        company = {"linkedin_url": "https://www.linkedin.com/company/acme"}
        patches = _domain_stubs(company=company, waterfall=AsyncMock(side_effect=RuntimeError("wf down")))
        miss_patches = _miss_store_stubs(recorded, miss_kind=None)
        for p in patches + list(miss_patches):
            p.start()
        try:
            rows = asyncio.run(lb._enrich_single_domain(
                blitz_http=MagicMock(), contacts_http=MagicMock(),
                base_row={"domain": "acme.com"}, domain="acme.com",
                max_decision_makers=2, domain_semaphore=asyncio.Semaphore(1),
                email_semaphore=asyncio.Semaphore(1), validate_email=False,
                blitz_miss_skip=True,
            ))
        finally:
            for p in patches + list(miss_patches):
                p.stop()
        self.assertEqual(recorded, [])
        self.assertEqual(rows[0]["row_status"], lb.STATUS_ERROR)

    def test_waterfall_hits_are_not_a_miss(self):
        recorded: list[tuple[str, str]] = []
        company = {"linkedin_url": "https://www.linkedin.com/company/acme"}
        patches = _domain_stubs(
            company=company,
            waterfall=AsyncMock(return_value={"results": [_waterfall_person()]}),
        )
        miss_patches = _miss_store_stubs(recorded, miss_kind=None)
        for p in patches + list(miss_patches):
            p.start()
        try:
            rows = asyncio.run(lb._enrich_single_domain(
                blitz_http=MagicMock(), contacts_http=MagicMock(),
                base_row={"domain": "acme.com"}, domain="acme.com",
                max_decision_makers=2, domain_semaphore=asyncio.Semaphore(1),
                email_semaphore=asyncio.Semaphore(1), validate_email=False,
                blitz_miss_skip=True,
            ))
        finally:
            for p in patches + list(miss_patches):
                p.stop()
        self.assertEqual(recorded, [])
        self.assertEqual(rows[0]["row_status"], lb.STATUS_ENRICHED)
        self.assertEqual(rows[0]["dm_previous_companies"], "Before Inc")
        self.assertEqual(rows[0]["dm_previous_titles"], "CTO")

    def test_default_disarmed_never_touches_the_store(self):
        """blitz_miss_skip defaults to False so direct-call tests never
        touch the shared jobs.db (mirrors the pipeline wave's parameter) —
        clean misses are NOT recorded and recent-miss markers are NOT
        consulted (the paid d2l runs normally)."""
        recorded: list[tuple[str, str]] = []
        recent_reads: list[str] = []

        def tracking_recent(domain):
            recent_reads.append(domain)
            return "company"

        # company={} forces the domain_to_linkedin branch; found=False is a
        # clean miss that a disarmed call must NOT record.
        patches = _domain_stubs(
            company={},
            d2l=AsyncMock(return_value={"found": False}),
            waterfall=AsyncMock(return_value={"results": []}),
        )
        miss_patches = (
            patch.object(blitz_miss_store, "is_recent_miss", tracking_recent),
            patch.object(
                blitz_miss_store,
                "record_miss",
                lambda domain, kind: recorded.append((domain, kind)),
            ),
        )
        for p in patches + list(miss_patches):
            p.start()
        try:
            rows = asyncio.run(lb._enrich_single_domain(
                blitz_http=MagicMock(), contacts_http=MagicMock(),
                base_row={"domain": "acme.com"}, domain="acme.com",
                max_decision_makers=2, domain_semaphore=asyncio.Semaphore(1),
                email_semaphore=asyncio.Semaphore(1), validate_email=False,
                # blitz_miss_skip intentionally OMITTED (default False)
            ))
        finally:
            for p in patches + list(miss_patches):
                p.stop()
        self.assertEqual(recorded, [])
        self.assertEqual(recent_reads, [])
        self.assertEqual(rows[0]["row_status"], lb.STATUS_NO_LINKEDIN)

    def test_miss_wiring_env_gate(self):
        """ENABLE_BLITZ_MISS_SKIP (default true) is the kill-switch the
        production runner arms the wiring from."""
        with patch.dict(os.environ, {"ENABLE_BLITZ_MISS_SKIP": "false"}):
            self.assertFalse(lb._blitz_miss_wiring_armed())
        with patch.dict(os.environ, {"ENABLE_BLITZ_MISS_SKIP": "true"}):
            self.assertTrue(lb._blitz_miss_wiring_armed())
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ENABLE_BLITZ_MISS_SKIP", None)
            self.assertTrue(lb._blitz_miss_wiring_armed())


# ---------------------------------------------------------------------------
# 7: prepass consumption in _enrich_single_domain
# ---------------------------------------------------------------------------


class TestPrepassConsumption(unittest.TestCase):
    def _prepass(self, domain: str, flat_persons: list) -> dict:
        return {
            "company_url_by_domain": {
                domain: "https://www.linkedin.com/company/acme"
            },
            "persons_by_domain": {domain: flat_persons},
            "prepass_domains": {domain},
            "skipped_miss": set(),
        }

    def test_covered_domain_skips_waterfall_and_d2l(self):
        waterfall = AsyncMock(return_value={"results": [_waterfall_person("Decoy")]})
        d2l = AsyncMock(return_value={"found": True, "company_linkedin_url": "https://x"})
        collector = RawContactCollector(job_id="job-p")
        recorded: list[tuple[str, str]] = []
        patches = _domain_stubs(d2l=d2l, waterfall=waterfall) + list(
            _miss_store_stubs(recorded, miss_kind=None)
        )
        for p in patches:
            p.start()
        try:
            rows = asyncio.run(lb._enrich_single_domain(
                blitz_http=MagicMock(), contacts_http=MagicMock(),
                base_row={"domain": "acme.com"}, domain="acme.com",
                max_decision_makers=3, domain_semaphore=asyncio.Semaphore(1),
                email_semaphore=asyncio.Semaphore(1), validate_email=False,
                collector=collector,
                blitz_prepass=self._prepass("acme.com", [_flat_person()]),
            ))
        finally:
            for p in patches:
                p.stop()
        waterfall.assert_not_awaited()
        d2l.assert_not_awaited()  # prepass supplied the company URL
        self.assertEqual(rows[0]["row_status"], lb.STATUS_ENRICHED)
        self.assertEqual(rows[0]["company_linkedin_url"], "https://www.linkedin.com/company/acme")
        self.assertEqual(rows[0]["dm_previous_companies"], "Past One | Past Two")
        self.assertEqual(rows[0]["dm_previous_titles"], "Founder")
        # SAME collector capture as waterfall persons
        self.assertEqual(len(collector), 1)

    def test_covered_domain_empty_persons_no_waterfall_replay(self):
        waterfall = AsyncMock(return_value={"results": [_waterfall_person()]})
        recorded: list[tuple[str, str]] = []
        patches = _domain_stubs(waterfall=waterfall) + list(
            _miss_store_stubs(recorded, miss_kind=None)
        )
        for p in patches:
            p.start()
        try:
            rows = asyncio.run(lb._enrich_single_domain(
                blitz_http=MagicMock(), contacts_http=MagicMock(),
                base_row={"domain": "acme.com"}, domain="acme.com",
                max_decision_makers=2, domain_semaphore=asyncio.Semaphore(1),
                email_semaphore=asyncio.Semaphore(1), validate_email=False,
                blitz_prepass=self._prepass("acme.com", []),
            ))
        finally:
            for p in patches:
                p.stop()
        waterfall.assert_not_awaited()
        # GetLeads DM fallback stubbed [] -> single no_contacts row
        self.assertEqual(rows[0]["row_status"], lb.STATUS_NO_CONTACTS)

    def test_prepass_persons_pass_the_title_gate(self):
        off_icp = _flat_person("Marketeer May", title="Marketing Manager")
        on_icp = _flat_person("Ceo Sid", title="CEO")
        waterfall = AsyncMock(return_value={"results": []})
        recorded: list[tuple[str, str]] = []
        patches = _domain_stubs(waterfall=waterfall) + list(
            _miss_store_stubs(recorded, miss_kind=None)
        )
        for p in patches:
            p.start()
        try:
            rows = asyncio.run(lb._enrich_single_domain(
                blitz_http=MagicMock(), contacts_http=MagicMock(),
                base_row={"domain": "acme.com"}, domain="acme.com",
                max_decision_makers=5, domain_semaphore=asyncio.Semaphore(1),
                email_semaphore=asyncio.Semaphore(1), validate_email=False,
                cascade_config=_TITLES_CASCADE,
                blitz_prepass=self._prepass("acme.com", [off_icp, on_icp]),
            ))
        finally:
            for p in patches:
                p.stop()
        waterfall.assert_not_awaited()
        names = [r["dm_full_name"] for r in rows]
        self.assertIn("Ceo Sid", names)
        self.assertNotIn("Marketeer May", names)

    def test_uncovered_domain_keeps_legacy_waterfall(self):
        waterfall = AsyncMock(return_value={"results": [_waterfall_person("Legacy Path")]})
        recorded: list[tuple[str, str]] = []
        patches = _domain_stubs(
            d2l=AsyncMock(return_value={
                "found": True,
                "company_linkedin_url": "https://www.linkedin.com/company/acme",
            }),
            waterfall=waterfall,
        ) + list(_miss_store_stubs(recorded, miss_kind=None))
        prepass = {
            "company_url_by_domain": {"other.com": "https://www.linkedin.com/company/other"},
            "persons_by_domain": {"other.com": [_flat_person()]},
            "prepass_domains": {"other.com"},
            "skipped_miss": set(),
        }
        for p in patches:
            p.start()
        try:
            rows = asyncio.run(lb._enrich_single_domain(
                blitz_http=MagicMock(), contacts_http=MagicMock(),
                base_row={"domain": "acme.com"}, domain="acme.com",
                max_decision_makers=2, domain_semaphore=asyncio.Semaphore(1),
                email_semaphore=asyncio.Semaphore(1), validate_email=False,
                blitz_prepass=prepass,
            ))
        finally:
            for p in patches:
                p.stop()
        waterfall.assert_awaited_once()
        self.assertEqual(rows[0]["dm_full_name"], "Legacy Path")


# ---------------------------------------------------------------------------
# 8: run_domain_enrichment prepass launch gating
# ---------------------------------------------------------------------------


def _flow1_rows(n: int) -> list[dict]:
    return [{"domain": f"d{i}.com"} for i in range(n)]


class TestRunDomainEnrichmentPrepassLaunch(unittest.TestCase):
    def _run(self, rows, *, extra_run_kwargs=None, prepass_return=None,
             prepass_exc: Exception | None = None, env: dict | None = None,
             on_progress=None):
        captured: dict[str, Any] = {}

        async def fake_prepass(http, domains, **kwargs):
            captured["domains"] = list(domains)
            captured["kwargs"] = kwargs
            if prepass_exc is not None:
                raise prepass_exc
            # simulate one chunk progress event so the on_progress mapping
            # is exercised (the real prepass emits these between chunks)
            if kwargs.get("on_progress") is not None:
                await kwargs["on_progress"]("blitz find-people prepass chunk 1/1")
            return prepass_return if prepass_return is not None else {
                "company_url_by_domain": {}, "persons_by_domain": {},
                "prepass_domains": set(), "skipped_miss": set(),
            }

        patches = [
            patch.object(lb.contacts_client, "company_by_domain",
                         new=AsyncMock(return_value={})),
            patch.object(lb.contacts_client, "company_contacts_enriched",
                         new=AsyncMock(return_value=[])),
            patch.object(lb.contacts_client, "person_by_name_and_domain",
                         new=AsyncMock(return_value=None)),
            patch.object(lb.contacts_client, "person_by_linkedin",
                         new=AsyncMock(return_value=None)),
            patch.object(lb.blitz_client, "domain_to_linkedin",
                         new=AsyncMock(return_value={"found": False})),
            patch.object(lb.blitz_client, "waterfall_icp_search",
                         new=AsyncMock(return_value={"results": []})),
            patch.object(lb.getleads_client, "lookup_decision_makers",
                         new=AsyncMock(return_value=[])),
            patch.object(lb.blitz_batch, "blitz_find_people_prepass",
                         new=fake_prepass),
            patch.object(blitz_miss_store, "is_recent_miss", lambda d: None),
            patch.object(blitz_miss_store, "record_miss", lambda d, k: None),
        ]
        env_patches = [
            patch.dict(os.environ, env, clear=False),
        ] if env else []
        run_kwargs = {
            "rows": rows,
            "domain_col": "domain",
            "max_decision_makers": 2,
        }
        if on_progress is not None:
            run_kwargs["on_progress"] = on_progress
        run_kwargs.update(extra_run_kwargs or {})
        for p in patches + env_patches:
            p.start()
        try:
            result = asyncio.run(lb.run_domain_enrichment(**run_kwargs))
        finally:
            for p in patches + env_patches:
                p.stop()
        return result, captured

    def test_prepass_launched_over_five_domains(self):
        events: list[dict] = []

        def on_progress(e: dict) -> None:
            events.append(e)

        prepass = {
            "company_url_by_domain": {
                "d0.com": "https://www.linkedin.com/company/d0",
            },
            "persons_by_domain": {"d0.com": []},
            "prepass_domains": {"d0.com"},
            "skipped_miss": set(),
        }
        rows, captured = self._run(
            _flow1_rows(6), prepass_return=prepass, on_progress=on_progress,
            # conftest disarms the miss wiring suite-wide; this test wants
            # the ARMED production shape (real store callbacks passed through)
            env={"ENABLE_BLITZ_MISS_SKIP": "true"},
        )
        self.assertEqual(
            captured["domains"],
            [f"d{i}.com" for i in range(6)],
        )
        # exact_titles forwarded; miss-store callbacks wired (armed -> the
        # store's functions, patched here, NOT the disarmed no-ops)
        self.assertFalse(captured["kwargs"]["exact_titles"])
        self.assertIsNot(captured["kwargs"]["is_recent_miss"], lb._noop_recent_miss)
        self.assertIsNot(captured["kwargs"]["record_miss"], lb._noop_record_miss)
        self.assertTrue(callable(captured["kwargs"]["is_recent_miss"]))
        self.assertTrue(callable(captured["kwargs"]["record_miss"]))
        self.assertTrue(callable(captured["kwargs"]["should_cancel"]))
        self.assertEqual(captured["kwargs"]["target_per_company"], 2)
        # prepass progress events were mapped onto on_progress
        self.assertTrue(any(
            e.get("stage") == "blitz_prepass" for e in events
        ))

    def test_prepass_miss_store_disarmed_passes_noops(self):
        """With ENABLE_BLITZ_MISS_SKIP=false (the conftest default), the
        prepass still launches but receives the no-op callbacks — a stubbed
        test run can never write markers to the shared jobs.db."""
        _, captured = self._run(
            _flow1_rows(6),
            env={"ENABLE_BLITZ_MISS_SKIP": "false"},
        )
        self.assertIs(captured["kwargs"]["is_recent_miss"], lb._noop_recent_miss)
        self.assertIs(captured["kwargs"]["record_miss"], lb._noop_record_miss)

    def test_exact_titles_flag_forwarded(self):
        _, captured = self._run(
            _flow1_rows(6),
            extra_run_kwargs={"exact_titles": True},
        )
        self.assertTrue(captured["kwargs"]["exact_titles"])

    def test_prepass_not_launched_with_five_or_fewer_domains(self):
        _, captured = self._run(_flow1_rows(5))
        self.assertEqual(captured, {})

    def test_prepass_disabled_via_env(self):
        _, captured = self._run(
            _flow1_rows(6), env={"ENABLE_BLITZ_FIND_PEOPLE_BATCH": "false"},
        )
        self.assertEqual(captured, {})

    def test_prepass_skipped_for_website_only(self):
        _, captured = self._run(
            _flow1_rows(6),
            extra_run_kwargs={"website_only": True},
        )
        self.assertEqual(captured, {})

    def test_prepass_skipped_when_blitz_not_selected(self):
        _, captured = self._run(
            _flow1_rows(6),
            extra_run_kwargs={"force_provider": "contacts_db"},
        )
        self.assertEqual(captured, {})

    def test_prepass_exception_falls_back_to_legacy(self):
        waterfall = None  # set inside _run patches; assert via captured only
        _, captured = self._run(
            _flow1_rows(6), prepass_exc=RuntimeError("prepass exploded"),
        )
        # the prepass WAS attempted (domains captured) and its exception
        # did not fail the run — every domain took the legacy path
        self.assertEqual(len(captured["domains"]), 6)

    def test_company_url_rows_excluded_from_prepass_candidates(self):
        rows = [
            {"domain": f"d{i}.com", "company_url": "https://www.linkedin.com/company/x"}
            for i in range(6)
        ]
        _, captured = self._run(
            rows, extra_run_kwargs={"company_linkedin_col": "company_url"},
        )
        self.assertEqual(captured, {})


# ---------------------------------------------------------------------------
# 9: phone bundle
# ---------------------------------------------------------------------------


class TestPhoneBundle(unittest.TestCase):
    def _row(self, *, email="a@b.c", linkedin="https://www.linkedin.com/in/a",
             country="US", phone="") -> dict:
        return {
            "dm_email": email,
            "dm_linkedin_url": linkedin,
            "dm_location_country": country,
            "dm_phone": phone,
        }

    def _run_bundle(self, rows, find_phone=None, *, phone_for_all=False):
        finder = find_phone if find_phone is not None else AsyncMock(
            return_value={"found": True, "phone": "+15550001111"}
        )
        with patch("phone_enrichment.client.find_phone", new=finder):
            asyncio.run(lb._attach_phone_bundle(
                MagicMock(), rows, phone_for_all=phone_for_all,
            ))
        return rows, finder

    def test_happy_path_first_qualifying_row_gets_phone(self):
        rows = [self._row(), self._row()]
        rows, finder = self._run_bundle(rows)
        self.assertEqual(finder.await_count, 1)
        self.assertEqual(rows[0]["dm_phone"], "+15550001111")
        self.assertEqual(rows[1]["dm_phone"], "")

    def test_non_us_row_skipped(self):
        rows = [self._row(country="DE")]
        rows, finder = self._run_bundle(rows)
        finder.assert_not_awaited()
        self.assertEqual(rows[0]["dm_phone"], "")

    def test_unknown_country_is_eligible(self):
        rows = [self._row(country="")]
        rows, finder = self._run_bundle(rows)
        finder.assert_awaited_once()
        self.assertEqual(rows[0]["dm_phone"], "+15550001111")

    def test_row_without_email_or_linkedin_skipped(self):
        rows = [self._row(email=""), self._row(linkedin="")]
        _, finder = self._run_bundle(rows)
        finder.assert_not_awaited()

    def test_existing_phone_never_overwritten(self):
        rows = [self._row(phone="+1999")]
        rows, finder = self._run_bundle(rows)
        finder.assert_not_awaited()
        self.assertEqual(rows[0]["dm_phone"], "+1999")

    def test_exception_swallowed_single_attempt(self):
        rows = [self._row(), self._row()]
        rows, finder = self._run_bundle(
            rows, find_phone=AsyncMock(side_effect=RuntimeError("phone down")),
        )
        # exactly ONE attempt even though two rows qualify — an exception
        # must not cascade into more paid calls
        self.assertEqual(finder.await_count, 1)
        self.assertEqual(rows[0]["dm_phone"], "")
        self.assertEqual(rows[1]["dm_phone"], "")

    def test_phone_for_all_targets_every_qualifying_row(self):
        rows = [self._row(), self._row(), self._row(country="FR")]
        rows, finder = self._run_bundle(rows, phone_for_all=True)
        self.assertEqual(finder.await_count, 2)
        self.assertEqual(rows[0]["dm_phone"], "+15550001111")
        self.assertEqual(rows[1]["dm_phone"], "+15550001111")
        self.assertEqual(rows[2]["dm_phone"], "")

    def test_us_country_aliases_eligible(self):
        for alias in ("US", "us", "USA", "United States"):
            self.assertTrue(lb._is_us_or_unknown_country(alias))
        for other in ("DE", "France", "Canada", "GB"):
            self.assertFalse(lb._is_us_or_unknown_country(other))
        for unknown in ("", None, 42):
            self.assertTrue(lb._is_us_or_unknown_country(unknown))

    def test_flow1_include_phone_wiring(self):
        finder = AsyncMock(return_value={"found": True, "phone": "+15552223333"})
        patches = _domain_stubs(
            company={"linkedin_url": "https://www.linkedin.com/company/acme"},
            waterfall=AsyncMock(return_value={"results": [_waterfall_person()]}),
        )
        extra = [
            patch("phone_enrichment.client.find_phone", new=finder),
            patch.object(blitz_miss_store, "is_recent_miss", lambda d: None),
            patch.object(blitz_miss_store, "record_miss", lambda d, k: None),
            patch.object(lb, "_merge_by_company_contacts",
                         new=AsyncMock(side_effect=lambda http, rows_, *a, **k: rows_)),
            patch.object(lb.company_fallback, "run_company_fallbacks",
                         new=AsyncMock(return_value=MagicMock())),
            patch.object(lb.company_fallback, "apply_company_fallbacks_to_row",
                         lambda row, fb, **k: None),
        ]
        rows_in = [{"domain": "acme.com"}]
        for p in patches + extra:
            p.start()
        try:
            out = asyncio.run(lb.run_domain_enrichment(
                rows=rows_in, domain_col="domain",
                max_decision_makers=2,
                include_phone=True,
            ))
        finally:
            for p in patches + extra:
                p.stop()
        finder.assert_awaited_once()
        self.assertEqual(out[0]["dm_phone"], "+15552223333")

    def test_flow1_phone_off_by_default(self):
        finder = AsyncMock(return_value={"found": True, "phone": "+1"})
        patches = _domain_stubs(
            company={"linkedin_url": "https://www.linkedin.com/company/acme"},
            waterfall=AsyncMock(return_value={"results": [_waterfall_person()]}),
        )
        extra = [
            patch("phone_enrichment.client.find_phone", new=finder),
            patch.object(blitz_miss_store, "is_recent_miss", lambda d: None),
            patch.object(blitz_miss_store, "record_miss", lambda d, k: None),
            patch.object(lb, "_merge_by_company_contacts",
                         new=AsyncMock(side_effect=lambda http, rows_, *a, **k: rows_)),
            patch.object(lb.company_fallback, "run_company_fallbacks",
                         new=AsyncMock(return_value=MagicMock())),
            patch.object(lb.company_fallback, "apply_company_fallbacks_to_row",
                         lambda row, fb, **k: None),
        ]
        for p in patches + extra:
            p.start()
        try:
            asyncio.run(lb.run_domain_enrichment(
                rows=[{"domain": "acme.com"}], domain_col="domain",
                max_decision_makers=2,
            ))
        finally:
            for p in patches + extra:
                p.stop()
        finder.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
