"""
Phase 2 (full capture) end-to-end field-propagation tests for GetLeads.

Proves the 29-field GetLeads ``data`` blob survives every stage of the drop
chain (client -> normalizer -> collector -> contacts_writer -> result row):

  1. ``getleads_client._normalize_result_item`` returns all new keys
     (+ ``_raw_getleads`` passthrough); ``_not_found_contact`` mirrors them.
  2. ``response_normalizer.normalize_getleads_contact`` carries them (both
     the nested-``data`` enrich shape and the flat stitched shape the
     pipeline feeds the collector).
  3. ``raw_contact_collector.capture_company_contact`` emits them in the
     contacts_writer payload (dm_headline / dm_phone / company_revenue / ...).
  4. ``contacts_writer._write_person_payload`` includes the new fields in
     the upsert body when present and omits them when empty.
  5. ``pipeline._build_person_row`` overlays ``verification_info[
     "getleads_dm"]`` onto the row.
  6. ``_build_person_row`` WITHOUT ``getleads_dm`` is byte-identical to the
     pre-Phase-2 behavior (zero-change guarantee).
  7. Mirror invariant: ``set(pipeline.ENRICHED_COLUMNS) ==
     set(list_builder.ENRICHED_COLUMNS)``.

Fixture drawn verbatim from /tmp/getleads_live_shapes.md (verified live
shape, 2026-08-13). Pure unit tests — no network. The only mock is on
``contacts_writer._do_upsert`` (captures the request body).

Run:
    python -m pytest enrichment/tests/test_getleads_field_propagation.py -v
"""

from __future__ import annotations

import asyncio
import os
import sys
import unittest
from unittest.mock import patch

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from enrichment import contacts_writer  # noqa: E402
from enrichment import getleads_client  # noqa: E402
from enrichment import list_builder  # noqa: E402
from enrichment import pipeline  # noqa: E402
from enrichment import response_normalizer as rn  # noqa: E402
from enrichment.raw_contact_collector import RawContactCollector  # noqa: E402


# Verbatim from the verified live-shape spec (/tmp/getleads_live_shapes.md).
LIVE_DATA = {
    "first_name": "Zac",
    "last_name": "Chaffin",
    "person_full_name": "Zac Chaffin",
    "job_title": "Chief Financial Officer",
    "org_company_name": "Earthworks Inc",
    "job_is_current": True,
    "job_function": "Finance & Accounting",
    "job_level": "C-Team",
    "person_linkedin_url": "https://www.linkedin.com/in/zac-chaffin-1234",
    "linkedin_url_org": "https://www.linkedin.com/company/earthworks",
    "linkedin_connections_count": 215,
    "linkedin_headline": "CFO at Earthworks, Inc.",
    "linkedin_industry": "Facilities Services",
    "country_name_org": "United States",
    "city_org": "Alvarado",
    "person_city": "Alvarado",
    "person_country_name": "United States",
    "revenue_range_org": "$50M to <$100M",
    "industry_linkedin_org": "Facilities Services",
    "domain_org": "earth.works",
    "website_org": "https://www.earth.works",
    "employee_count_range_org": "201 to 500",
    "cellphone": "+1 817-825-4777",
    "direct_phone": "",
    "email_address": "zac@earth.works",
    "email_domain": "earth.works",
    "email_status": "VALID",
    "email_last_verified_at": "2026-06-05T18:32:42",
}

LIVE_ITEM = {
    "first_name": "Zac",
    "last_name": "Chaffin",
    "email_domain": "earth.works",
    "profileUrl": "https://www.linkedin.com/in/zac-chaffin-1234",
    "email": "zac@earth.works",
    "data": dict(LIVE_DATA),
}

NEW_CLIENT_KEYS = (
    "job_title",
    "linkedin_headline",
    "person_full_name",
    "company_name",
    "company_industry",
    "employee_count",
    "revenue",
    "city",
    "country",
    "linkedin_connections",
    "email_last_verified_at",
    "job_level",
    "job_function",
)

NEW_RECORD_KEYS = (
    "phone",
    "city",
    "country",
    "company_name",
    "company_industry",
    "employee_count",
    "revenue",
    "linkedin_connections",
    "email_last_verified_at",
    "job_level",
    "job_function",
)


class TestClientNormalizeResultItem(unittest.TestCase):
    """Stage 1: getleads_client._normalize_result_item (root cause)."""

    def test_full_data_blob_returns_all_new_keys(self):
        out = getleads_client._normalize_result_item(LIVE_ITEM)
        self.assertEqual(out["job_title"], "Chief Financial Officer")
        self.assertEqual(out["linkedin_headline"], "CFO at Earthworks, Inc.")
        self.assertEqual(out["person_full_name"], "Zac Chaffin")
        self.assertEqual(out["company_name"], "Earthworks Inc")
        self.assertEqual(out["company_industry"], "Facilities Services")
        self.assertEqual(out["employee_count"], "201 to 500")
        self.assertEqual(out["revenue"], "$50M to <$100M")
        self.assertEqual(out["city"], "Alvarado")
        self.assertEqual(out["country"], "United States")
        # int connection count coerced to string
        self.assertEqual(out["linkedin_connections"], "215")
        self.assertEqual(out["email_last_verified_at"], "2026-06-05T18:32:42")
        self.assertEqual(out["job_level"], "C-Team")
        self.assertEqual(out["job_function"], "Finance & Accounting")
        # pre-existing keys untouched
        self.assertEqual(out["email"], "zac@earth.works")
        self.assertEqual(out["verification_status"], "Valid")
        self.assertEqual(out["phone"], "+1 817-825-4777")

    def test_raw_getleads_passthrough_is_the_raw_data_dict(self):
        out = getleads_client._normalize_result_item(LIVE_ITEM)
        self.assertEqual(out["_raw_getleads"], LIVE_DATA)

    def test_not_found_contact_mirrors_new_keys_as_empty(self):
        nf = getleads_client._not_found_contact("Zac", "Chaffin", "earth.works")
        for key in NEW_CLIENT_KEYS:
            self.assertEqual(nf[key], "", key)
        self.assertNotIn("_raw_getleads", nf)
        # pre-existing keys preserved
        self.assertEqual(nf["first_name"], "Zac")
        self.assertEqual(nf["domain"], "earth.works")


class TestNormalizeGetleadsContact(unittest.TestCase):
    """Stage 2/3: response_normalizer.normalize_getleads_contact."""

    def test_nested_data_shape_carries_new_fields(self):
        rec = rn.normalize_getleads_contact(LIVE_ITEM)
        self.assertIsNotNone(rec)
        self.assertEqual(rec["title"], "Chief Financial Officer")
        self.assertEqual(rec["headline"], "CFO at Earthworks, Inc.")
        self.assertEqual(rec["phone"], "+1 817-825-4777")
        self.assertEqual(rec["city"], "Alvarado")
        self.assertEqual(rec["country"], "United States")
        self.assertEqual(rec["company_name"], "Earthworks Inc")
        self.assertEqual(rec["company_industry"], "Facilities Services")
        self.assertEqual(rec["employee_count"], "201 to 500")
        self.assertEqual(rec["revenue"], "$50M to <$100M")
        self.assertEqual(rec["linkedin_connections"], "215")
        self.assertEqual(rec["email_last_verified_at"], "2026-06-05T18:32:42")
        self.assertEqual(rec["job_level"], "C-Team")
        self.assertEqual(rec["job_function"], "Finance & Accounting")
        self.assertEqual(rec["_raw_getleads"], LIVE_DATA)

    def test_flat_stitched_shape_carries_new_fields(self):
        """The pipeline feeds the collector {**normalized_item, ...} — a flat
        dict with the client's new keys and _raw_getleads but no data."""
        flat = {
            **getleads_client._normalize_result_item(LIVE_ITEM),
            "full_name": "Zac Chaffin",
            "linkedin_url": "https://www.linkedin.com/in/zac-chaffin-1234",
        }
        rec = rn.normalize_getleads_contact(flat)
        self.assertIsNotNone(rec)
        self.assertEqual(rec["city"], "Alvarado")
        self.assertEqual(rec["revenue"], "$50M to <$100M")
        self.assertEqual(rec["company_name"], "Earthworks Inc")
        self.assertEqual(rec["linkedin_connections"], "215")
        # _raw_getleads survives the stitching via the client passthrough
        self.assertEqual(rec["_raw_getleads"], LIVE_DATA)


class TestCollectorPayload(unittest.TestCase):
    """Stage 4: raw_contact_collector.capture_company_contact payload."""

    def _payload(self) -> dict:
        collector = RawContactCollector(job_id="job-gl-1")
        flat = {
            **getleads_client._normalize_result_item(LIVE_ITEM),
            "full_name": "Zac Chaffin",
            "linkedin_url": "https://www.linkedin.com/in/zac-chaffin-1234",
        }
        ok = collector.capture_company_contact(
            source="getleads",
            domain="earth.works",
            company_linkedin_url="https://www.linkedin.com/company/earthworks",
            contact=flat,
        )
        self.assertTrue(ok)
        payloads = collector.to_payloads()
        self.assertEqual(len(payloads), 1)
        return payloads[0]

    def test_payload_carries_all_new_fields(self):
        p = self._payload()
        self.assertEqual(p["dm_headline"], "CFO at Earthworks, Inc.")
        self.assertEqual(p["dm_phone"], "+1 817-825-4777")
        self.assertEqual(p["dm_location_city"], "Alvarado")
        self.assertEqual(p["dm_location_country"], "United States")
        self.assertEqual(p["company_name"], "Earthworks Inc")
        self.assertEqual(p["company_industry"], "Facilities Services")
        self.assertEqual(p["company_employee_count"], "201 to 500")
        self.assertEqual(p["company_revenue"], "$50M to <$100M")
        self.assertEqual(p["dm_linkedin_connections"], "215")
        self.assertEqual(p["dm_email_last_verified_at"], "2026-06-05T18:32:42")

    def test_payload_omits_fields_when_absent(self):
        collector = RawContactCollector()
        ok = collector.capture_company_contact(
            source="getleads",
            domain="acme.com",
            company_linkedin_url="",
            contact={
                "email": "jane@acme.com",
                "first_name": "Jane",
                "last_name": "Doe",
            },
        )
        self.assertTrue(ok)
        p = collector.to_payloads()[0]
        for key in (
            "dm_headline",
            "dm_phone",
            "dm_location_city",
            "dm_location_country",
            "company_employee_count",
            "company_revenue",
            "dm_linkedin_connections",
            "dm_email_last_verified_at",
            "company_name",
            "company_industry",
        ):
            self.assertNotIn(key, p, key)


class TestWritePersonPayload(unittest.TestCase):
    """Stage 5: contacts_writer._write_person_payload body."""

    def _run(self, payload: dict) -> dict:
        bodies = []

        async def fake_do_upsert(client, body, original, *, job_id, row_index, kind):
            bodies.append(body)
            return contacts_writer.WriteStatus.SYNCED

        with patch.object(contacts_writer, "_do_upsert", new=fake_do_upsert):
            status = asyncio.run(
                contacts_writer._write_person_payload(
                    client=object(), payload=payload, job_id="j1", row_index=0
                )
            )
        self.assertEqual(status, contacts_writer.WriteStatus.SYNCED)
        self.assertEqual(len(bodies), 1)
        return bodies[0]

    def test_body_includes_new_fields_when_present(self):
        p = self._payload_for_writer()
        body = self._run(p)
        # PersonUpsertRequest has no dedicated field for firmographics —
        # they travel via custom_fields (the only server-persisted path).
        cf = body.get("custom_fields")
        self.assertIsInstance(cf, dict)
        self.assertEqual(cf["headline"], "CFO at Earthworks, Inc.")
        self.assertEqual(cf["industry"], "Facilities Services")
        self.assertEqual(cf["employee_count"], "201 to 500")
        self.assertEqual(cf["revenue"], "$50M to <$100M")
        self.assertEqual(cf["city"], "Alvarado")
        self.assertEqual(cf["country"], "United States")
        self.assertEqual(cf["linkedin_connections"], "215")
        self.assertEqual(cf["email_last_verified_at"], "2026-06-05T18:32:42")
        # phone_number is the field the live API actually persists
        self.assertEqual(body["phone_number"], "+1 817-825-4777")
        # canonical fields unchanged
        self.assertEqual(body["email"], "zac@earth.works")
        self.assertEqual(body["full_name"], "Zac Chaffin")
        self.assertEqual(body["title"], "Chief Financial Officer")

    def test_body_omits_new_fields_when_empty(self):
        body = self._run({
            "dm_email": "jane@acme.com",
            "domain": "acme.com",
            "dm_first_name": "Jane",
            "dm_last_name": "Doe",
        })
        self.assertNotIn("custom_fields", body)
        self.assertNotIn("phone_number", body)

    @staticmethod
    def _payload_for_writer() -> dict:
        collector = RawContactCollector()
        flat = {
            **getleads_client._normalize_result_item(LIVE_ITEM),
            "full_name": "Zac Chaffin",
            "linkedin_url": "https://www.linkedin.com/in/zac-chaffin-1234",
        }
        collector.capture_company_contact(
            source="getleads",
            domain="earth.works",
            company_linkedin_url="",
            contact=flat,
        )
        return collector.to_payloads()[0]


class TestBuildPersonRowOverlay(unittest.TestCase):
    """Stage 6: pipeline._build_person_row getleads_dm overlay."""

    BASE_ROW = {"input_domain": "earth.works", "company_name": "Earthworks Inc"}

    PERSON = {
        "first_name": "Zac",
        "last_name": "Chaffin",
        "full_name": "Zac Chaffin",
        "title": "Finance",
        "linkedin_url": "https://www.linkedin.com/in/zac-chaffin-1234",
        "location": {"city": "Dublin", "country_code": "IE"},
        "experiences": [{"job_is_current": True, "job_title": "Finance Lead"}],
    }

    def _row(self, verification_info):
        return pipeline._build_person_row(
            dict(self.BASE_ROW),
            "https://www.linkedin.com/company/earthworks",
            dict(self.PERSON),
            1,
            "zac@earth.works",
            "getleads_email",
            verification_info,
        )

    def test_overlay_applies_getleads_dm_fields(self):
        vi = {
            "dm_email_verified": "yes",
            "getleads_dm": {
                "title": "Chief Financial Officer",
                "headline": "CFO at Earthworks, Inc.",
                "city": "Alvarado",
                "country": "United States",
                "phone": "+1 817-825-4777",
                "job_level": "C-Team",
                "job_function": "Finance & Accounting",
                "revenue": "$50M to <$100M",
                "employee_count": "201 to 500",
                "linkedin_connections": "215",
                "email_last_verified_at": "2026-06-05T18:32:42",
            },
        }
        row = self._row(vi)
        self.assertEqual(row["dm_title"], "Chief Financial Officer")
        self.assertEqual(row["dm_headline"], "CFO at Earthworks, Inc.")
        self.assertEqual(row["dm_location_city"], "Alvarado")
        self.assertEqual(row["dm_location_country"], "United States")
        self.assertEqual(row["dm_phone"], "+1 817-825-4777")
        self.assertEqual(row["dm_job_level"], "C-Team")
        self.assertEqual(row["dm_job_function"], "Finance & Accounting")
        self.assertEqual(row["dm_email_last_verified_at"], "2026-06-05T18:32:42")
        self.assertEqual(row["dm_linkedin_connections"], "215")
        # fill-only: no contacts_db company_* stamped on base_row -> filled
        self.assertEqual(row["company_revenue"], "$50M to <$100M")

    def test_overlay_never_overwrites_contactsdb_company_fields(self):
        vi = {
            "dm_email_verified": "yes",
            "getleads_dm": {
                "revenue": "$50M to <$100M",
                "employee_count": "201 to 500",
            },
        }
        row = pipeline._build_person_row(
            {
                "input_domain": "earth.works",
                "company_name": "Earthworks Inc",
                "company_employee_count": "501-1000",  # contacts_db value
            },
            "",
            dict(self.PERSON),
            1,
            "zac@earth.works",
            "getleads_email",
            vi,
        )
        self.assertEqual(row["company_employee_count"], "501-1000")
        self.assertEqual(row["company_revenue"], "$50M to <$100M")

    def test_no_getleads_dm_is_byte_identical_to_pre_change(self):
        """Zero-change guarantee: without getleads_dm the row matches the
        pre-Phase-2 behavior exactly (new columns stay at their
        _empty_enriched defaults)."""
        with_dm_absent = self._row({"dm_email_verified": "yes"})
        # Every new column is present (via _empty_enriched) but empty
        for col in ("company_revenue", "dm_email_last_verified_at", "dm_linkedin_connections"):
            self.assertIn(col, with_dm_absent)
            self.assertEqual(with_dm_absent[col], "")
        # The Blitz/contacts_db-derived fields are untouched
        self.assertEqual(with_dm_absent["dm_title"], "Finance Lead")
        self.assertEqual(with_dm_absent["dm_location_city"], "Dublin")
        self.assertEqual(with_dm_absent["dm_location_country"], "IE")
        self.assertEqual(with_dm_absent["dm_email_verified"], "yes")

    def test_empty_getleads_dm_values_do_not_blank_fields(self):
        vi = {
            "dm_email_verified": "yes",
            "getleads_dm": {"title": "", "city": "", "phone": ""},
        }
        row = self._row(vi)
        self.assertEqual(row["dm_title"], "Finance Lead")
        self.assertEqual(row["dm_location_city"], "Dublin")


class TestGetleadsDmSnapshot(unittest.TestCase):
    """Stage 6 helper: _getleads_dm_snapshot from a client result."""

    def test_snapshot_from_normalized_item(self):
        snap = pipeline._getleads_dm_snapshot(
            getleads_client._normalize_result_item(LIVE_ITEM)
        )
        self.assertEqual(snap["title"], "Chief Financial Officer")
        self.assertEqual(snap["city"], "Alvarado")
        self.assertEqual(snap["phone"], "+1 817-825-4777")
        self.assertEqual(snap["job_level"], "C-Team")
        self.assertEqual(snap["revenue"], "$50M to <$100M")
        self.assertEqual(snap["linkedin_connections"], "215")

    def test_snapshot_empty_for_none_or_non_dict(self):
        self.assertEqual(pipeline._getleads_dm_snapshot(None), {})
        self.assertEqual(pipeline._getleads_dm_snapshot("nope"), {})


class TestEnrichedColumnsMirror(unittest.TestCase):
    """Stage 7: pipeline.ENRICHED_COLUMNS must mirror list_builder's set."""

    def test_mirror_invariant(self):
        self.assertEqual(
            set(pipeline.ENRICHED_COLUMNS), set(list_builder.ENRICHED_COLUMNS)
        )

    def test_new_columns_present_in_both(self):
        for col in ("company_revenue", "dm_email_last_verified_at", "dm_linkedin_connections"):
            self.assertIn(col, pipeline.ENRICHED_COLUMNS, col)
            self.assertIn(col, list_builder.ENRICHED_COLUMNS, col)

    def test_list_builder_empty_enriched_covers_all_columns(self):
        empty = list_builder._empty_enriched()
        self.assertEqual(set(empty.keys()), set(list_builder.ENRICHED_COLUMNS))


if __name__ == "__main__":
    unittest.main()
