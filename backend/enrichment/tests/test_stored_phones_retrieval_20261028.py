"""Stored-phone retrieval wiring tests (2026-10-28).

The Contacts DB (leadsdatabase.cc) holds ~2.64M phone numbers in
``core.phone`` (≈432K Blitz-sourced) that the enrichment cascade paid for
but never surfaced: the contacts-api read endpoints the cascade calls
(``/v1/company/contacts/enriched``, ``/v1/person/by-linkedin``,
``/v1/person/by-name-and-domain``, ``/v1/company/persons/by-domain``) did
not SELECT from ``core.phone``, so stored phones never reached output rows.

This file pins the listbuilding side of the fix:

  1. ``response_normalizer.normalize_contacts_db_contact`` — maps the new
     ``phone`` passthrough (absent key → "", never None; write-back
     round-trip preserved via RawContactCollector → contacts_writer).
  2. ``pipeline._build_person_row`` — a contacts_db discovery person's
     ``phone`` fills ``dm_phone``; the GetLeads DM overlay still overwrites
     only on non-empty values (existing semantics untouched); a stored
     phone makes the paid Blitz phone bundle skip the row (fill-only).
  3. ``list_builder._merge_by_company_contacts`` — by-company rows carry
     ``bc["phone"]`` into ``dm_phone``.
  4. ``list_builder._resolve_person_email`` Strategy 1 (contacts_db by
     name+domain) — REGRESSION PIN: it already returned the response's
     ``phone`` in the result tuple before this change; keep it that way.

Pure unit tests — network fully mocked (AsyncMock on contacts_client).
Run:
    python -m pytest enrichment/tests/test_stored_phones_retrieval_20261028.py -v
"""

from __future__ import annotations

import os
import sys
import unittest
from unittest.mock import AsyncMock, patch

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

os.environ.setdefault("CONTACTS_API_TOKEN", "test-token-from-suite")

from enrichment import list_builder as lb  # noqa: E402
from enrichment import pipeline  # noqa: E402
from enrichment import response_normalizer as rn  # noqa: E402
from enrichment import contacts_client as cc  # noqa: E402


class TestNormalizeContactsDbPhone(unittest.TestCase):
    """1. contacts_db normalizer maps the stored phone."""

    def test_phone_mapped(self):
        raw = {
            "person_id": "abc",
            "full_name": "Walter White",
            "email": "walter@acme.com",
            "linkedin_url": "https://linkedin.com/in/walterwhite",
            "domain": "acme.com",
            "phone": "+14155551234",
        }
        result = rn.normalize_contacts_db_contact(raw)
        self.assertIsNotNone(result)
        self.assertEqual(result["phone"], "+14155551234")

    def test_phone_absent_is_empty_string(self):
        raw = {"email": "someone@acme.com"}
        result = rn.normalize_contacts_db_contact(raw)
        self.assertIsNotNone(result)
        self.assertEqual(result["phone"], "")


class TestBuildPersonRowStoredPhone(unittest.TestCase):
    """2. pipeline._build_person_row fills dm_phone from the person dict."""

    BASE_ROW = {"input_domain": "acme.com", "company_name": "Acme"}

    def _person(self, with_phone: bool) -> dict:
        person = {
            "first_name": "Walter",
            "last_name": "White",
            "full_name": "Walter White",
            "title": "CEO",
            "linkedin_url": "https://www.linkedin.com/in/walterwhite",
            "location": {"city": "Austin", "country_code": "US"},
            "experiences": [],
        }
        if with_phone:
            person["phone"] = "+14155551234"
        return person

    def _row(self, person, verification_info=None):
        return pipeline._build_person_row(
            dict(self.BASE_ROW),
            "https://www.linkedin.com/company/acme",
            person,
            1,
            "walter@acme.com",
            "contacts_db_email",
            verification_info,
        )

    def test_stored_phone_fills_dm_phone(self):
        row = self._row(self._person(with_phone=True))
        self.assertEqual(row["dm_phone"], "+14155551234")

    def test_no_phone_is_empty_string(self):
        # Blitz waterfall persons carry no phone key — row stays "".
        row = self._row(self._person(with_phone=False))
        self.assertEqual(row["dm_phone"], "")

    def test_getleads_overlay_still_wins_on_non_empty(self):
        # Existing overlay semantics: a fresh GetLeads phone (non-empty)
        # overwrites the stored one; empty overlay values never blank it.
        row = self._row(
            self._person(with_phone=True),
            verification_info={
                "dm_email_verified": "yes",
                "getleads_dm": {"phone": "+19998887777"},
            },
        )
        self.assertEqual(row["dm_phone"], "+19998887777")

    def test_getleads_empty_overlay_keeps_stored(self):
        row = self._row(
            self._person(with_phone=True),
            verification_info={
                "dm_email_verified": "yes",
                "getleads_dm": {"phone": ""},
            },
        )
        self.assertEqual(row["dm_phone"], "+14155551234")


class TestMergeByCompanyPhone(unittest.TestCase):
    """3. by-company rows carry the stored phone into dm_phone."""

    def test_by_company_phone_mapped(self):
        contact = {
            "person_id": "p1",
            "full_name": "Saul Goodman",
            "email": "saul@goodman.law",
            "title": "Attorney",
            "phone": "+15055550101",
        }
        base_row = {"input_domain": "goodman.law", "company_name": "Goodman"}
        with patch.object(lb.contacts_client, "company_persons_by_domain",
                          AsyncMock(return_value=[contact])), \
             patch.dict(os.environ, {"ENABLE_COMPANY_LOOKUP": "1"}):
            rows = asyncio_run(
                lb._merge_by_company_contacts(
                    http_client_stub(), [], "goodman.law", dict(base_row), None,
                )
            )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["dm_phone"], "+15055550101")
        self.assertEqual(rows[0]["dm_email_source"], "contacts_db_by_company")

    def test_by_company_without_phone_is_empty(self):
        contact = {"person_id": "p2", "full_name": "Kim Wexler",
                   "email": "kim@goodman.law"}
        base_row = {"input_domain": "goodman.law", "company_name": "Goodman"}
        with patch.object(lb.contacts_client, "company_persons_by_domain",
                          AsyncMock(return_value=[contact])), \
             patch.dict(os.environ, {"ENABLE_COMPANY_LOOKUP": "1"}):
            rows = asyncio_run(
                lb._merge_by_company_contacts(
                    http_client_stub(), [], "goodman.law", dict(base_row), None,
                )
            )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["dm_phone"], "")


class TestResolvePersonEmailStrategy1PhonePin(unittest.TestCase):
    """4. REGRESSION PIN: Strategy 1 already threaded the response phone.

    ``_resolve_person_email`` (list_builder) extracted
    ``contacts_data["phone"]`` into its result tuple long before this
    change — with the contacts-api now actually returning the key, the
    wiring must keep working. Pins the contract so a future refactor of
    the tuple shape notices this test.
    """

    def test_strategy1_returns_stored_phone(self):
        contacts_data = {
            "person_id": "p3",
            "full_name": "Mike Ehrmantraut",
            "email": "mike@acme.com",
            "phone": "+15055550202",
        }
        with patch.object(cc, "person_by_name_and_domain",
                          AsyncMock(return_value=contacts_data)), \
             patch.object(lb.mailtester_client, "verify_email",
                          AsyncMock(return_value={"valid": True, "code": "ok",
                                                  "message": "ok"})):
            result = asyncio_run(
                lb._resolve_person_email(
                    http_client_stub(),
                    http_client_stub(),
                    {"full_name": "Mike Ehrmantraut",
                     "linkedin_url": "https://linkedin.com/in/mikee"},
                    "acme.com",
                )
            )
        email, phone, source = result[0], result[1], result[2]
        self.assertEqual(email, "mike@acme.com")
        self.assertEqual(phone, "+15055550202")
        self.assertEqual(source, lb.SOURCE_CONTACTS_DB_NAME)


def asyncio_run(coro):
    import asyncio
    return asyncio.run(coro)


def http_client_stub():
    from unittest.mock import MagicMock
    return MagicMock()


if __name__ == "__main__":
    unittest.main()
