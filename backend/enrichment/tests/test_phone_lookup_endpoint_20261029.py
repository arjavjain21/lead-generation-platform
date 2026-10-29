"""Dedicated phone-lookup endpoint tests (2026-10-29).

``GET/POST /api/enrichment/phone`` — the lean Clay-friendly surface: give a
LinkedIn URL (or domain + full name), get back ONLY the phone (stored →
QuickEnrich ladder), never the full enrichment payload.

Pins:
  1. stored phone by LinkedIn URL (free, fastest path)
  2. stored phone by name + domain
  3. no stored phone → QuickEnrich fresh hit (default fresh=true)
  4. no stored phone → QE miss → found:false, empty phone (miss is free)
  5. fresh=false → QE never called (free-only mode)
  6. missing identity inputs → 400
  7. response stays LEAN (no contacts[] payload)

Network fully mocked. Run:
    python -m pytest enrichment/tests/test_phone_lookup_endpoint_20261029.py -v
"""

from __future__ import annotations

import os
import sys
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

os.environ.setdefault("CONTACTS_API_TOKEN", "test-token-from-suite")

from enrichment import routes as er  # noqa: E402
from enrichment import contacts_client as cc  # noqa: E402
from enrichment import quickenrich_client as qe  # noqa: E402


def run(coro):
    import asyncio
    return asyncio.run(coro)


def _stub_client():
    return MagicMock()


class TestPhoneLookup(unittest.TestCase):
    def _lookup(self, *, linkedin_url=None, domain=None, full_name=None, fresh=None):
        kwargs = {}
        if linkedin_url:
            kwargs["linkedin_url"] = linkedin_url
        if domain:
            kwargs["domain"] = domain
        if full_name:
            kwargs["full_name"] = full_name
        if fresh is not None:
            kwargs["fresh"] = fresh
        return run(er._phone_lookup_core(**kwargs))

    def test_stored_phone_by_linkedin(self):
        data = {"full_name": "Sanjeev Kumar", "email": "skumar@servefed.com",
                "phone": "+17035178527"}
        with patch.object(er.contacts_client, "person_by_linkedin",
                          AsyncMock(return_value=data)), \
             patch.object(er.quickenrich_client, "find_person_phone", AsyncMock()) as qe_mock:
            r = self._lookup(linkedin_url="https://linkedin.com/in/sanjeev-kumar-330965148")
        self.assertTrue(r["found"])
        self.assertEqual(r["phone"], "+17035178527")
        self.assertEqual(r["phone_source"], "stored")
        self.assertEqual(r["looked_up_by"], "linkedin_url")
        self.assertEqual(r["person_name"], "Sanjeev Kumar")
        qe_mock.assert_not_called()

    def test_stored_phone_by_name_and_domain(self):
        data = {"full_name": "Sanjeev Kumar", "email": "skumar@servefed.com",
                "phone": "+17035178527"}
        with patch.object(er.contacts_client, "person_by_name_and_domain",
                          AsyncMock(return_value=data)), \
             patch.object(er.quickenrich_client, "find_person_phone", AsyncMock()) as qe_mock:
            r = self._lookup(domain="servefed.com", full_name="Sanjeev Kumar")
        self.assertTrue(r["found"])
        self.assertEqual(r["phone"], "+17035178527")
        self.assertEqual(r["phone_source"], "stored")
        self.assertEqual(r["looked_up_by"], "name_and_domain")
        qe_mock.assert_not_called()

    def test_qe_fresh_hit_when_no_stored(self):
        with patch.object(er.contacts_client, "person_by_linkedin",
                          AsyncMock(return_value={"full_name": "Shannon Long",
                                                  "email": "s@x.com", "phone": ""})), \
             patch.object(er.quickenrich_client, "find_person_phone",
                          AsyncMock(return_value={"found": True, "phone": "+17576192853",
                                                  "phone_type": "mobile"})):
            r = self._lookup(linkedin_url="https://linkedin.com/in/shannon-long-09bb387")
        self.assertTrue(r["found"])
        self.assertEqual(r["phone"], "+17576192853")
        self.assertEqual(r["phone_source"], "quickenrich")

    def test_qe_miss_clean_not_found(self):
        with patch.object(er.contacts_client, "person_by_linkedin",
                          AsyncMock(return_value=None)), \
             patch.object(er.quickenrich_client, "find_person_phone",
                          AsyncMock(return_value={"found": False, "phone": ""})):
            r = self._lookup(linkedin_url="https://linkedin.com/in/nobody-here")
        self.assertFalse(r["found"])
        self.assertEqual(r["phone"], "")
        self.assertEqual(r["phone_source"], "")

    def test_fresh_false_skips_qe(self):
        with patch.object(er.contacts_client, "person_by_name_and_domain",
                          AsyncMock(return_value={"full_name": "X", "email": "x@y.com",
                                                  "phone": ""})), \
             patch.object(er.quickenrich_client, "find_person_phone", AsyncMock()) as qe_mock:
            r = self._lookup(domain="acme.com", full_name="John Doe", fresh=False)
        self.assertFalse(r["found"])
        qe_mock.assert_not_called()

    def test_qe_error_never_fails_lookup(self):
        with patch.object(er.contacts_client, "person_by_linkedin",
                          AsyncMock(side_effect=Exception("boom"))), \
             patch.object(er.quickenrich_client, "find_person_phone",
                          AsyncMock(side_effect=Exception("net"))):
            r = self._lookup(linkedin_url="https://linkedin.com/in/x")
        self.assertFalse(r["found"])
        self.assertEqual(r["phone"], "")

    def test_response_is_lean(self):
        data = {"full_name": "A", "email": "a@b.com", "phone": "+1"}
        with patch.object(er.contacts_client, "person_by_linkedin",
                          AsyncMock(return_value=data)):
            r = self._lookup(linkedin_url="https://linkedin.com/in/a")
        self.assertEqual(
            set(r.keys()),
            {"found", "phone", "phone_type", "phone_source", "looked_up_by",
             "person_name", "email"},
        )


class TestPhoneLookupValidation(unittest.TestCase):
    def test_no_identity_inputs_raises(self):
        with self.assertRaises(Exception):
            run(er._phone_lookup_core())

    def test_name_without_domain_raises(self):
        with self.assertRaises(Exception):
            run(er._phone_lookup_core(full_name="John Doe"))


if __name__ == "__main__":
    unittest.main()
