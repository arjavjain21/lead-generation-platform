"""QuickEnrich phone-search fallback tier tests (2026-10-28, wave 2).

Phone resolution order on phone-bundle rows (``include_phone=true``):

  1. stored phone already on the row (free — the 2026-10-28 stored-phone
     fill) → skip everything
  2. QuickEnrich ``/api/employees/phone-search`` — 1 credit ONLY on a hit
     (miss is free), NOT US-gated (provider docs claim no geo restriction)
  3. Blitz Direct Phone — bills per call even on a miss, US-only (last
     resort, unchanged behavior)

This file pins:

  1. ``quickenrich_client.find_person_phone`` — request gating, hit/miss
     parse, phone normalization ("+1-555-0123" → "+15550123"), disabled
     short-circuit.
  2. ``pipeline._attach_dm_phones`` — QE hit (no Blitz call), QE miss →
     Blitz fallback (US row), QE miss on non-US row (no Blitz), stored
     phone skips both, kill switch off → QE skipped.
  3. ``list_builder._attach_phone_bundle`` — the same ladder (mirror).

Network fully mocked. Run:
    python -m pytest enrichment/tests/test_quickenrich_phone_search_20261028.py -v
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

from enrichment import list_builder as lb  # noqa: E402
from enrichment import pipeline  # noqa: E402
from enrichment import quickenrich_client as qe  # noqa: E402


def _http_stub() -> MagicMock:
    return MagicMock()


class TestFindPersonPhoneParse(unittest.TestCase):
    """1. client-level parse + gating."""

    def test_hit_normalizes_dashes(self):
        response = MagicMock(status_code=200)
        response.json.return_value = {
            "success": True,
            "data": {
                "first_name": "John",
                "last_name": "Doe",
                "employee_phone": "+1-555-0123",
                "employee_phone_type": "direct",
                "company_url": "https://techcorp.com",
            },
            "meta": {"credits_used": 1, "remaining_credits": 99},
        }
        with patch.object(qe, "_is_enabled", return_value=True), \
             patch.object(qe, "is_configured", return_value=True), \
             patch.object(qe, "_request_with_retries", AsyncMock(return_value=response)):
            result = AsyncMock_runner.run(
                qe.find_person_phone(_http_stub(), linkedin_url="https://linkedin.com/in/johndoe")
            )
        self.assertTrue(result["found"])
        self.assertEqual(result["phone"], "+15550123")
        self.assertEqual(result["phone_type"], "direct")

    def test_hit_bare_10_digit_gets_us_prefix(self):
        # Live-observed vendor shape 2026-10-28: "757-619-2853" (no country
        # code) must become +17576192853, not the undialable "+7576192853".
        response = MagicMock(status_code=200)
        response.json.return_value = {
            "success": True,
            "data": {"employee_phone": "757-619-2853", "employee_phone_type": "mobile"},
            "meta": {"credits_used": 1, "remaining_credits": 98},
        }
        with patch.object(qe, "_is_enabled", return_value=True), \
             patch.object(qe, "is_configured", return_value=True), \
             patch.object(qe, "_request_with_retries", AsyncMock(return_value=response)):
            result = AsyncMock_runner.run(
                qe.find_person_phone(_http_stub(), linkedin_url="https://linkedin.com/in/x")
            )
        self.assertTrue(result["found"])
        self.assertEqual(result["phone"], "+17576192853")

    def test_clean_miss_free(self):
        response = MagicMock(status_code=200)
        response.json.return_value = {
            "success": True, "data": [],
            "meta": {"credits_used": 0, "reason": "PHONE_NOT_FOUND"},
        }
        with patch.object(qe, "_is_enabled", return_value=True), \
             patch.object(qe, "is_configured", return_value=True), \
             patch.object(qe, "_request_with_retries", AsyncMock(return_value=response)):
            result = AsyncMock_runner.run(
                qe.find_person_phone(_http_stub(), linkedin_url="https://linkedin.com/in/johndoe")
            )
        self.assertFalse(result["found"])
        self.assertEqual(result["phone"], "")

    def test_no_identity_inputs_returns_none(self):
        with patch.object(qe, "_is_enabled", return_value=True), \
             patch.object(qe, "is_configured", return_value=True):
            result = AsyncMock_runner.run(qe.find_person_phone(_http_stub()))
        self.assertIsNone(result)

    def test_disabled_returns_none(self):
        with patch.object(qe, "_is_enabled", return_value=False), \
             patch.object(qe, "is_configured", return_value=True):
            result = AsyncMock_runner.run(
                qe.find_person_phone(_http_stub(), linkedin_url="https://linkedin.com/in/x")
            )
        self.assertIsNone(result)


class AsyncMock_runner:
    """Tiny wrapper so test methods can stay sync-flavored."""

    @staticmethod
    def run(coro):
        import asyncio
        return asyncio.run(coro)


def run(coro):
    import asyncio
    return asyncio.run(coro)


def _row(**over):
    base = {
        "dm_email": "a@b.com",
        "dm_linkedin_url": "https://linkedin.com/in/johndoe",
        "dm_location_country": "US",
        "dm_phone": "",
    }
    base.update(over)
    return base


def _qe_hit(url="https://linkedin.com/in/johndoe", phone="+15550123"):
    return AsyncMock(return_value={"found": True, "phone": phone, "phone_type": "direct"})


def _qe_miss():
    return AsyncMock(return_value={"found": False, "phone": ""})


class TestPipelineAttachDmPhones(unittest.TestCase):
    """2. pipeline._attach_dm_phones — QE-first ladder."""

    def _attach(self, rows, qe_mock, blitz_mock, phone_for_all=False):
        with patch.object(pipeline.quickenrich_client, "find_person_phone", qe_mock), \
             patch("phone_enrichment.client.find_phone", blitz_mock):
            run(pipeline._attach_dm_phones(
                _http_stub(), rows, domain="b.com", phone_for_all=phone_for_all,
            ))

    def test_qe_hit_no_blitz_call(self):
        blitz = AsyncMock(return_value={"found": True, "phone": "+1999"})
        rows = [_row()]
        self._attach(rows, _qe_hit(), blitz)
        self.assertEqual(rows[0]["dm_phone"], "+15550123")
        blitz.assert_not_called()

    def test_qe_miss_us_row_falls_to_blitz(self):
        blitz = AsyncMock(return_value={"found": True, "phone": "+18887776666"})
        rows = [_row()]
        self._attach(rows, _qe_miss(), blitz)
        self.assertEqual(rows[0]["dm_phone"], "+18887776666")
        blitz.assert_called_once()

    def test_qe_miss_non_us_row_no_blitz(self):
        blitz = AsyncMock(return_value={"found": True, "phone": "+1999"})
        rows = [_row(dm_location_country="DE")]
        self._attach(rows, _qe_hit(phone="+4915112345678"), blitz)
        # QE is not US-gated — a German row can resolve via QE alone.
        self.assertEqual(rows[0]["dm_phone"], "+4915112345678")
        blitz.assert_not_called()

    def test_qe_miss_non_us_row_stays_empty(self):
        blitz = AsyncMock()
        rows = [_row(dm_location_country="DE")]
        self._attach(rows, _qe_miss(), blitz)
        self.assertEqual(rows[0]["dm_phone"], "")
        blitz.assert_not_called()

    def test_stored_phone_skips_both(self):
        blitz = AsyncMock()
        qe_mock = AsyncMock()
        rows = [_row(dm_phone="+17035178527")]
        self._attach(rows, qe_mock, blitz)
        self.assertEqual(rows[0]["dm_phone"], "+17035178527")
        qe_mock.assert_not_called()
        blitz.assert_not_called()

    def test_kill_switch_off_skips_qe(self):
        blitz = AsyncMock(return_value={"found": True, "phone": "+18887776666"})
        rows = [_row()]
        with patch.dict(os.environ, {"ENABLE_QUICKENRICH_PHONE_SEARCH": "false"}), \
             patch.object(pipeline.quickenrich_client, "find_person_phone", AsyncMock()) as qe_mock, \
             patch("phone_enrichment.client.find_phone", blitz):
            run(pipeline._attach_dm_phones(_http_stub(), rows, domain="b.com"))
        self.assertEqual(rows[0]["dm_phone"], "+18887776666")
        qe_mock.assert_not_called()

    def test_budget_shape_unchanged_one_unit_per_row(self):
        # Default cap = 1 row per domain: two eligible rows, only the first
        # resolves (one budget unit = one row's QE-then-Blitz resolution —
        # a QE hit means Blitz is never reached, and row 2 is never attempted).
        blitz = AsyncMock(return_value={"found": False, "phone": ""})
        rows = [_row(), _row(dm_linkedin_url="https://linkedin.com/in/other")]
        self._attach(rows, _qe_hit(), blitz)
        self.assertEqual(rows[0]["dm_phone"], "+15550123")
        self.assertEqual(rows[1]["dm_phone"], "")
        blitz.assert_not_called()


class TestListBuilderAttachPhoneBundle(unittest.TestCase):
    """3. list_builder._attach_phone_bundle — mirror ladder."""

    def _attach(self, rows, qe_mock, blitz_mock, phone_for_all=False):
        with patch.object(lb.quickenrich_client, "find_person_phone", qe_mock), \
             patch("phone_enrichment.client.find_phone", blitz_mock):
            run(lb._attach_phone_bundle(
                _http_stub(), rows, phone_for_all=phone_for_all,
            ))

    def test_qe_hit_no_blitz_call(self):
        blitz = AsyncMock(return_value={"found": True, "phone": "+1999"})
        rows = [_row()]
        self._attach(rows, _qe_hit(), blitz)
        self.assertEqual(rows[0]["dm_phone"], "+15550123")
        blitz.assert_not_called()

    def test_qe_miss_falls_to_blitz(self):
        blitz = AsyncMock(return_value={"found": True, "phone": "+18887776666"})
        rows = [_row()]
        self._attach(rows, _qe_miss(), blitz)
        self.assertEqual(rows[0]["dm_phone"], "+18887776666")
        blitz.assert_called_once()

    def test_stored_phone_skips_both(self):
        blitz = AsyncMock()
        qe_mock = AsyncMock()
        rows = [_row(dm_phone="+17035178527")]
        self._attach(rows, qe_mock, blitz)
        qe_mock.assert_not_called()
        blitz.assert_not_called()


if __name__ == "__main__":
    unittest.main()
