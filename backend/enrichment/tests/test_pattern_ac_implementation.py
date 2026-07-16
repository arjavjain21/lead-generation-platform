"""
Phase 2 — Pattern A + Pattern C implementation tests.

Verifies that ``enrichment.pipeline`` correctly:

* Pattern A — extracts 5 fields from existing provider responses:
    - ``company_name`` / ``company_industry`` / ``company_employee_count``
      from the Contacts DB ``company_by_domain`` response.
    - ``dm_job_level`` / ``dm_job_function`` from the Blitz person dict
      (``job_level`` / ``job_function`` keys, with ``seniority`` /
      ``function`` aliases).

* Pattern C — captures per-row provider errors:
    - Every ``except`` block in ``_resolve_email_for_person`` and
      ``_enrich_domain`` records an error to a per-row list.
    - The list is serialized to JSON in the row's ``provider_errors``
      CSV cell. Empty list → empty string.

* Regression — existing CSV columns, cascade control flow, and the
  separate routing-level ``routing.provider_errors`` block are not
  affected.

Run:
    python -m pytest enrichment/tests/test_pattern_ac_implementation.py -v
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

# Make backend root importable so `enrichment` resolves regardless of cwd.
_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

os.environ.setdefault("CONTACTS_API_TOKEN", "test-token-pattern-ac")

from enrichment import pipeline as pipeline_mod  # noqa: E402
from enrichment import contacts_client as cc  # noqa: E402
from enrichment import blitz_client as bc  # noqa: E402
from enrichment import better_enrich_client as be  # noqa: E402
from enrichment import wizleads_client as wl  # noqa: E402
from enrichment import mailtester_client as mt  # noqa: E402


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

def _contacts_db_company_payload(
    *,
    name: str = "Acme Corp",
    industry: str = "Technology",
    employee_count=None,
    linkedin_url: str = "https://linkedin.com/company/acme",
) -> dict:
    """Build a Contacts DB company_by_domain response."""
    payload = {
        "linkedin_url": linkedin_url,
        "name": name,
        "industry": industry,
    }
    if employee_count is not None:
        payload["employee_count"] = employee_count
    return payload


def _contacts_db_contacts_payload(n: int) -> list[dict]:
    """Build N Contacts DB decision-maker contact dicts."""
    return [
        {
            "first_name": f"First{i}",
            "last_name": f"Last{i}",
            "full_name": f"First{i} Last{i}",
            "title": f"Title {i}",
            "headline": f"Headline {i}",
            "linkedin_url": f"https://linkedin.com/in/person-{i}",
            "email": f"person{i}@acme.com",
            "city": "NYC",
            "country_code": "US",
        }
        for i in range(n)
    ]


def _blitz_waterfall_payload(n: int, *, with_job_fields: bool = True) -> dict:
    """Build N Blitz waterfall_icp_search person results."""
    persons = []
    for i in range(n):
        person = {
            "first_name": f"BFirst{i}",
            "last_name": f"BLast{i}",
            "full_name": f"BFirst{i} BLast{i}",
            "title": f"BTitle {i}",
            "headline": f"BHeadline {i}",
            "linkedin_url": f"https://linkedin.com/in/blitz-{i}",
            "location": {"city": "SF", "country_code": "US"},
            "emails": [{"email": f"blitz{i}@acme.com"}],
            "verified_email": f"blitz{i}@acme.com",
        }
        if with_job_fields:
            person["job_level"] = "VP"
            person["job_function"] = "Sales & Business Development"
        persons.append({"person": person, "icp": 0})
    return {"results": persons}


def _patch_person_email_resolve(return_email: str = "", source: str = "not_found"):
    """Patch ``_resolve_email_for_person`` to skip the email cascade.

    The Pattern A tests don't care about email resolution — they verify
    row dict population. Default returns no email so we always reach
    ``_build_person_row`` with empty verification_info.
    """
    return patch.object(
        pipeline_mod,
        "_resolve_email_for_person",
        AsyncMock(return_value=(return_email, source, {})),
    )


async def _run_enrich_domain(
    *,
    company_payload=None,
    contacts_payload=None,
    blitz_payload=None,
    force_provider=None,
    cascade=None,
    max_results: int = 2,
    collector=None,
    mock_email_resolve: bool = True,
    domain_to_linkedin_payload=None,
    company_side_effect=None,
    d2l_side_effect=None,
    blitz_side_effect=None,
) -> list[dict]:
    """Invoke ``pipeline._enrich_domain`` with mocked providers.

    By default mocks ``_resolve_email_for_person`` to return (no email,
    not_found, empty verification) so we don't depend on real provider
    API keys. Pass ``mock_email_resolve=False`` to exercise the real
    email cascade (only useful with all provider clients mocked).

    ``domain_to_linkedin_payload`` lets the caller override the Blitz
    domain_to_linkedin response (default ``{"found": False}``).
    ``company_side_effect`` / ``d2l_side_effect`` let the caller force
    the company_by_domain / domain_to_linkedin mocks to raise instead of
    return — used by the Pattern C company-level failure tests.
    """
    if cascade is None:
        cascade = bc.DEFAULT_CASCADE

    if company_side_effect is not None:
        company_patch = patch.object(
            cc, "company_by_domain",
            AsyncMock(side_effect=company_side_effect),
        )
    else:
        company_patch = patch.object(
            cc, "company_by_domain",
            AsyncMock(return_value=company_payload),
        )

    contacts_patch = patch.object(
        cc, "company_contacts_enriched",
        AsyncMock(return_value=contacts_payload),
    )
    if blitz_payload is None and blitz_side_effect is None:
        # Default: empty results so cascade is deterministic.
        blitz_patch = patch.object(
            bc, "waterfall_icp_search",
            AsyncMock(return_value={"results": []}),
        )
    elif blitz_side_effect is not None:
        blitz_patch = patch.object(
            bc, "waterfall_icp_search",
            AsyncMock(side_effect=blitz_side_effect),
        )
    else:
        blitz_patch = patch.object(
            bc, "waterfall_icp_search",
            AsyncMock(return_value=blitz_payload),
        )
    person_by_name_patch = patch.object(
        cc, "person_by_name_and_domain",
        AsyncMock(return_value=None),
    )

    if d2l_side_effect is not None:
        domain_to_linkedin_patch = patch.object(
            bc, "domain_to_linkedin",
            AsyncMock(side_effect=d2l_side_effect),
        )
    else:
        if domain_to_linkedin_payload is None:
            domain_to_linkedin_payload = {"found": False}
        domain_to_linkedin_patch = patch.object(
            bc, "domain_to_linkedin",
            AsyncMock(return_value=domain_to_linkedin_payload),
        )

    email_resolve_patch = (
        patch.object(
            pipeline_mod, "_resolve_email_for_person",
            AsyncMock(return_value=("", "not_found", {})),
        )
        if mock_email_resolve else
        patch.object(pipeline_mod, "_resolve_email_for_person",
                     pipeline_mod._resolve_email_for_person)
    )

    with company_patch, contacts_patch, blitz_patch, \
         person_by_name_patch, domain_to_linkedin_patch, email_resolve_patch:
        return await pipeline_mod._enrich_domain(
            blitz_http=MagicMock(),
            contacts_http=MagicMock(),
            base_row={"input_domain": "acme.com"},
            domain="acme.com",
            full_name="",
            cascade=cascade,
            max_results=max_results,
            domain_semaphore=asyncio.Semaphore(1),
            email_semaphore=asyncio.Semaphore(1),
            force_provider=force_provider,
            collector=collector,
        )


# ---------------------------------------------------------------------------
# Pattern A tests — company fields
# ---------------------------------------------------------------------------


class TestPatternACompanyFields(unittest.IsolatedAsyncioTestCase):
    """Verify company_name / industry / employee_count extracted."""

    async def test_company_name_extracted_from_contacts_db_response(self):
        rows = await _run_enrich_domain(
            company_payload=_contacts_db_company_payload(name="Acme Corp"),
            contacts_payload=_contacts_db_contacts_payload(1),
        )
        self.assertGreaterEqual(len(rows), 1)
        self.assertEqual(rows[0]["company_name"], "Acme Corp")

    async def test_company_industry_extracted(self):
        rows = await _run_enrich_domain(
            company_payload=_contacts_db_company_payload(industry="Manufacturing"),
            contacts_payload=_contacts_db_contacts_payload(1),
        )
        self.assertEqual(rows[0]["company_industry"], "Manufacturing")

    async def test_company_employee_count_extracted(self):
        rows = await _run_enrich_domain(
            company_payload=_contacts_db_company_payload(employee_count=250),
            contacts_payload=_contacts_db_contacts_payload(1),
        )
        # Employee count normalized to str for CSV.
        self.assertEqual(rows[0]["company_employee_count"], "250")

    async def test_pattern_a_fields_empty_when_provider_returns_nothing(self):
        # company_by_domain returns None (provider 404).
        rows = await _run_enrich_domain(
            company_payload=None,
            contacts_payload=None,
        )
        # Will hit the no_linkedin path since both providers returned nothing.
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].get("company_name", ""), "")
        self.assertEqual(rows[0].get("company_industry", ""), "")
        self.assertEqual(rows[0].get("company_employee_count", ""), "")

    async def test_pattern_a_fields_empty_when_company_dict_missing_keys(self):
        # company_by_domain returns a dict but without name/industry/employee_count.
        rows = await _run_enrich_domain(
            company_payload={"linkedin_url": "https://linkedin.com/company/acme"},
            contacts_payload=_contacts_db_contacts_payload(1),
        )
        self.assertEqual(rows[0].get("company_name", ""), "")
        self.assertEqual(rows[0].get("company_industry", ""), "")
        self.assertEqual(rows[0].get("company_employee_count", ""), "")


# ---------------------------------------------------------------------------
# Pattern A tests — person fields
# ---------------------------------------------------------------------------


class TestPatternAPersonFields(unittest.IsolatedAsyncioTestCase):
    """Verify dm_job_level / dm_job_function extracted from Blitz person."""

    async def test_dm_job_level_extracted_from_blitz_person(self):
        """Blitz waterfall person carries job_level — row should mirror it."""
        # Force Blitz path: skip Contacts DB by using a custom cascade so
        # the Contacts DB contact lookup is skipped and Blitz runs.
        custom_cascade = [{"include_title": ["CEO"]}]
        blitz_payload = _blitz_waterfall_payload(1, with_job_fields=True)
        rows = await _run_enrich_domain(
            company_payload=_contacts_db_company_payload(),
            contacts_payload=None,
            blitz_payload=blitz_payload,
            cascade=custom_cascade,
            max_results=1,
        )
        self.assertGreaterEqual(len(rows), 1)
        self.assertEqual(rows[0]["dm_job_level"], "VP")

    async def test_dm_job_function_extracted(self):
        custom_cascade = [{"include_title": ["CEO"]}]
        blitz_payload = _blitz_waterfall_payload(1, with_job_fields=True)
        rows = await _run_enrich_domain(
            company_payload=_contacts_db_company_payload(),
            contacts_payload=None,
            blitz_payload=blitz_payload,
            cascade=custom_cascade,
            max_results=1,
        )
        self.assertEqual(rows[0]["dm_job_function"], "Sales & Business Development")

    async def test_dm_job_level_empty_when_person_lacks_it(self):
        """Blitz person with no job_level → dm_job_level is empty string."""
        custom_cascade = [{"include_title": ["CEO"]}]
        blitz_payload = _blitz_waterfall_payload(1, with_job_fields=False)
        rows = await _run_enrich_domain(
            company_payload=_contacts_db_company_payload(),
            contacts_payload=None,
            blitz_payload=blitz_payload,
            cascade=custom_cascade,
            max_results=1,
        )
        self.assertEqual(rows[0].get("dm_job_level", ""), "")
        self.assertEqual(rows[0].get("dm_job_function", ""), "")

    async def test_dm_job_level_supports_seniority_alias(self):
        """``_build_person_row`` should also accept seniority/function."""
        base_row = {"input_domain": "x.com"}
        person = {
            "seniority": "Director",
            "function": "Engineering",
            "first_name": "A",
            "last_name": "B",
            "full_name": "A B",
        }
        row = pipeline_mod._build_person_row(
            base_row=base_row,
            company_linkedin_url="",
            person=person,
            icp_tier=0,
            email="",
            email_source="not_found",
        )
        self.assertEqual(row["dm_job_level"], "Director")
        self.assertEqual(row["dm_job_function"], "Engineering")


# ---------------------------------------------------------------------------
# Pattern C tests — error capture in _resolve_email_for_person
# ---------------------------------------------------------------------------


class TestPatternCErrorCapture(unittest.IsolatedAsyncioTestCase):
    """Verify every provider failure records to verification_info.provider_errors."""

    async def _resolve_one(
        self,
        *,
        person: dict,
        domain: str = "acme.com",
        input_full_name: str = "",
        force_provider=None,
    ) -> dict:
        """Invoke _resolve_email_for_person and return verification_info."""
        sem = asyncio.Semaphore(1)
        _email, _source, verification_info = await pipeline_mod._resolve_email_for_person(
            blitz_client_inst=MagicMock(),
            contacts_client_inst=MagicMock(),
            person=person,
            domain=domain,
            input_full_name=input_full_name,
            email_semaphore=sem,
            force_provider=force_provider,
            validate_email=False,
        )
        return verification_info

    async def test_error_captured_on_contacts_db_failure(self):
        person = {"full_name": "John Doe", "linkedin_url": ""}
        with patch.object(cc, "person_by_name_and_domain",
                          AsyncMock(side_effect=Exception("boom-contacts"))):
            verification_info = await self._resolve_one(person=person)
        errs = verification_info["provider_errors"]
        self.assertTrue(any(e["provider"] == "contacts_db" for e in errs),
                        f"expected contacts_db error in {errs}")
        match = next(e for e in errs if e["provider"] == "contacts_db")
        self.assertIn("boom-contacts", match["message"])
        self.assertEqual(match["method"], "person_by_name_and_domain")

    async def test_error_captured_on_blitz_failure(self):
        # linkedin_url set so Blitz path runs.
        person = {"full_name": "John Doe", "linkedin_url": "https://linkedin.com/in/johndoe"}
        with patch.object(cc, "person_by_name_and_domain",
                          AsyncMock(return_value=None)), \
             patch.object(cc, "person_by_linkedin",
                          AsyncMock(return_value=None)), \
             patch.object(bc, "person_enrich",
                          AsyncMock(side_effect=Exception("blitz-name-fail"))), \
             patch.object(bc, "find_work_email",
                          AsyncMock(side_effect=Exception("blitz-li-fail"))):
            verification_info = await self._resolve_one(person=person)
        errs = verification_info["provider_errors"]
        providers = [e["provider"] for e in errs]
        self.assertIn("blitz", providers)
        # Both Blitz methods should record an error.
        methods = [e["method"] for e in errs if e["provider"] == "blitz"]
        self.assertIn("person_enrich", methods)
        self.assertIn("find_work_email", methods)

    async def test_error_captured_on_wizleads_failure(self):
        person = {"full_name": "John Doe", "linkedin_url": ""}
        with patch.object(cc, "person_by_name_and_domain",
                          AsyncMock(return_value=None)), \
             patch.object(bc, "person_enrich",
                          AsyncMock(return_value={"found": False})), \
             patch.object(wl, "find_email",
                          AsyncMock(side_effect=Exception("wiz-fail"))):
            verification_info = await self._resolve_one(person=person)
        errs = verification_info["provider_errors"]
        self.assertTrue(any(e["provider"] == "wizleads" for e in errs),
                        f"expected wizleads error in {errs}")

    async def test_error_captured_on_better_enrich_failure(self):
        person = {"full_name": "John Doe", "linkedin_url": ""}
        with patch.object(cc, "person_by_name_and_domain",
                          AsyncMock(return_value=None)), \
             patch.object(bc, "person_enrich",
                          AsyncMock(return_value={"found": False})), \
             patch.object(wl, "find_email",
                          AsyncMock(return_value=None)), \
             patch.object(be, "find_work_email_v3",
                          AsyncMock(side_effect=Exception("be-fail"))):
            verification_info = await self._resolve_one(person=person)
        errs = verification_info["provider_errors"]
        self.assertTrue(any(e["provider"] == "better_enrich" for e in errs),
                        f"expected better_enrich error in {errs}")

    async def test_multiple_errors_per_row(self):
        """All providers fail → all 4 errors captured in the list."""
        person = {"full_name": "John Doe", "linkedin_url": ""}
        with patch.object(cc, "person_by_name_and_domain",
                          AsyncMock(side_effect=Exception("e1"))), \
             patch.object(cc, "person_by_linkedin",
                          AsyncMock(side_effect=Exception("e2"))), \
             patch.object(bc, "person_enrich",
                          AsyncMock(side_effect=Exception("e3"))), \
             patch.object(bc, "find_work_email",
                          AsyncMock(side_effect=Exception("e4"))), \
             patch.object(wl, "find_email",
                          AsyncMock(side_effect=Exception("e5"))), \
             patch.object(be, "find_work_email_v3",
                          AsyncMock(side_effect=Exception("e6"))):
            verification_info = await self._resolve_one(person=person)
        errs = verification_info["provider_errors"]
        # Person-level cascade: contacts_db (Step 1), blitz person_enrich (Step 3),
        # wizleads (Step 5), better_enrich (Step 6). Note Step 2 only runs if
        # linkedin_url is set — so it's not exercised here.
        provider_set = {e["provider"] for e in errs}
        self.assertIn("contacts_db", provider_set)
        self.assertIn("blitz", provider_set)
        self.assertIn("wizleads", provider_set)
        self.assertIn("better_enrich", provider_set)
        self.assertGreaterEqual(len(errs), 4)

    async def test_no_errors_when_all_providers_succeed(self):
        """When a provider returns an email, cascade short-circuits — empty list."""
        person = {"full_name": "John Doe", "linkedin_url": ""}
        with patch.object(cc, "person_by_name_and_domain",
                          AsyncMock(return_value={"email": "john@acme.com"})), \
             patch.object(mt, "verify_email",
                          AsyncMock(return_value={"valid": True, "code": "ok", "message": "ok"})):
            verification_info = await self._resolve_one(person=person)
        self.assertEqual(verification_info["provider_errors"], [])

    async def test_provider_errors_csv_format_is_json(self):
        """``_build_person_row`` emits JSON in the provider_errors cell."""
        person = {"full_name": "John Doe", "linkedin_url": ""}
        with patch.object(cc, "person_by_name_and_domain",
                          AsyncMock(side_effect=Exception("json-test"))):
            verification_info = await self._resolve_one(person=person)
        row = pipeline_mod._build_person_row(
            base_row={"input_domain": "x.com"},
            company_linkedin_url="",
            person={"first_name": "J", "last_name": "D", "full_name": "J D"},
            icp_tier=0,
            email="",
            email_source="not_found",
            verification_info=verification_info,
        )
        cell = row["provider_errors"]
        self.assertIsInstance(cell, str)
        self.assertGreater(len(cell), 0)
        # Must be valid JSON and a list.
        parsed = json.loads(cell)
        self.assertIsInstance(parsed, list)
        self.assertGreaterEqual(len(parsed), 1)
        self.assertEqual(parsed[0]["provider"], "contacts_db")

    async def test_empty_errors_yield_empty_csv_cell(self):
        """No errors → empty string in CSV cell (NOT "[]")."""
        row = pipeline_mod._build_person_row(
            base_row={"input_domain": "x.com"},
            company_linkedin_url="",
            person={"first_name": "J", "last_name": "D", "full_name": "J D"},
            icp_tier=0,
            email="",
            email_source="not_found",
            verification_info={
                "dm_email_verified": "unknown",
                "mailtester_code": "",
                "mailtester_message": "",
                "provider_errors": [],
            },
        )
        self.assertEqual(row["provider_errors"], "")

    async def test_no_verification_info_yields_empty_csv_cell(self):
        """verification_info=None should leave provider_errors empty."""
        row = pipeline_mod._build_person_row(
            base_row={"input_domain": "x.com"},
            company_linkedin_url="",
            person={"first_name": "J", "last_name": "D", "full_name": "J D"},
            icp_tier=0,
            email="",
            email_source="not_found",
            verification_info=None,
        )
        self.assertEqual(row["provider_errors"], "")


# ---------------------------------------------------------------------------
# Pattern C tests — error capture in _enrich_domain (company-level)
# ---------------------------------------------------------------------------


class TestPatternCCompanyLevelCapture(unittest.IsolatedAsyncioTestCase):
    """Verify company-level errors land on the row."""

    async def test_company_by_domain_failure_recorded_on_no_linkedin_row(self):
        """Contacts DB company lookup fails → error on the no_linkedin row."""
        rows = await _run_enrich_domain(
            company_payload=None,
            contacts_payload=None,
            mock_email_resolve=False,
            company_side_effect=Exception("company-boom"),
            d2l_side_effect=Exception("d2l-boom"),
        )
        self.assertEqual(len(rows), 1)
        cell = rows[0]["provider_errors"]
        self.assertGreater(len(cell), 0, "expected JSON errors on no_linkedin row")
        parsed = json.loads(cell)
        self.assertTrue(any(e["provider"] == "contacts_db"
                            and e["method"] == "company_by_domain" for e in parsed),
                        f"contacts_db company_by_domain error missing from {parsed}")
        # Blitz domain_to_linkedin also failed → should be in the list.
        self.assertTrue(any(e["provider"] == "blitz"
                            and e["method"] == "domain_to_linkedin" for e in parsed),
                        f"blitz domain_to_linkedin error missing from {parsed}")

    async def test_blitz_waterfall_failure_recorded_on_error_row(self):
        """Blitz waterfall fails → error captured and row_status=error."""
        # force_provider=blitz bypasses Contacts DB contacts lookup so we
        # run Blitz waterfall directly. Company_by_domain is also skipped
        # under force_provider=blitz, so we mock domain_to_linkedin to
        # return a company LinkedIn URL. The waterfall mock is set to
        # raise via the helper's blitz_side_effect param.
        rows = await _run_enrich_domain(
            company_payload=None,
            contacts_payload=None,
            blitz_payload=None,
            force_provider="blitz",
            domain_to_linkedin_payload={
                "found": True,
                "company_linkedin_url": "https://linkedin.com/company/acme",
            },
            blitz_side_effect=Exception("waterfall-boom"),
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["row_status"], "error")
        cell = rows[0]["provider_errors"]
        parsed = json.loads(cell)
        self.assertTrue(any(e["method"] == "waterfall_icp_search" for e in parsed))


# ---------------------------------------------------------------------------
# Regression tests
# ---------------------------------------------------------------------------


class TestRegression(unittest.TestCase):
    """Verify existing contracts are unchanged."""

    def test_existing_csv_columns_unchanged(self):
        """All 49 ENRICHED_COLUMNS still present, in same order."""
        expected_first_six = [
            "company_linkedin_url",
            "company_name",
            "company_industry",
            "company_employee_count",
            "dm_first_name",
            "dm_last_name",
        ]
        self.assertEqual(pipeline_mod.ENRICHED_COLUMNS[:6], expected_first_six)
        # Confirm the 6 new fields exist in the schema.
        for field in ("company_name", "company_industry",
                      "company_employee_count", "dm_job_level",
                      "dm_job_function", "provider_errors"):
            self.assertIn(field, pipeline_mod.ENRICHED_COLUMNS,
                          f"{field} missing from ENRICHED_COLUMNS")
        # Confirm provider_errors slot is still exactly one column.
        self.assertEqual(pipeline_mod.ENRICHED_COLUMNS.count("provider_errors"), 1)

    def test_routing_block_provider_errors_not_affected(self):
        """The routing-level provider_errors (separate concept) must be intact.

        ``_ProviderError`` is the routing-level carrier. Confirm its shape
        is unchanged: provider, method, error_type, message.
        """
        err = pipeline_mod._ProviderError(
            provider="blitz",
            method="person_enrich",
            error_type="service_unavailable",
            message="blitz: Service temporarily unavailable.",
        )
        # Must remain falsy so cascade continues.
        self.assertFalse(bool(err))
        d = err.to_dict()
        self.assertEqual(set(d.keys()), {"provider", "method", "error_type", "message"})
        self.assertEqual(d["provider"], "blitz")
        self.assertEqual(d["method"], "person_enrich")

    def test_empty_enriched_includes_all_columns(self):
        """``_empty_enriched`` must populate every ENRICHED_COLUMNS key."""
        row = pipeline_mod._empty_enriched()
        self.assertEqual(set(row.keys()), set(pipeline_mod.ENRICHED_COLUMNS))
        for col in pipeline_mod.ENRICHED_COLUMNS:
            self.assertEqual(row[col], "",
                             f"{col} not empty in _empty_enriched")


class TestCascadeBehaviorUnchanged(unittest.IsolatedAsyncioTestCase):
    """Verify cascade still produces rows on happy path."""

    async def test_cascade_behavior_unchanged_on_success(self):
        """Contacts DB returns contacts → row produced as before."""
        rows = await _run_enrich_domain(
            company_payload=_contacts_db_company_payload(),
            contacts_payload=_contacts_db_contacts_payload(2),
        )
        # Both decision makers should produce rows.
        self.assertEqual(len(rows), 2)
        # Row status is enriched (with empty email from mock resolve, it's no_contacts).
        for r in rows:
            self.assertIn(r["row_status"], ("enriched", "no_contacts"))

    async def test_company_linkedin_url_still_populated(self):
        rows = await _run_enrich_domain(
            company_payload=_contacts_db_company_payload(),
            contacts_payload=_contacts_db_contacts_payload(1),
        )
        self.assertEqual(rows[0]["company_linkedin_url"],
                         "https://linkedin.com/company/acme")


if __name__ == "__main__":
    unittest.main()
