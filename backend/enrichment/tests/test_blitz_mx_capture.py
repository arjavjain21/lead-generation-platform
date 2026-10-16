"""
Tests for the Blitz summer-followup MX capture (2026-10-15).

Live-verified contract (probe session 2026-10-15):
- /v2/enrichment/email + /v2/enrichment/company return raw MX records
  ``mx: [{priority, host}]`` top-level (+ per ``all_emails[]`` entry on the
  email endpoint). No catch-all boolean observed live — parsed defensively.
- ``_stamp_mx_fields`` classifies the hosts via the SEG classifier so
  Blitz-sourced labels match our own DoH SEG vocabulary, and returns a NEW
  dict (immutability rule).
"""

from __future__ import annotations

import os
import sys

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from enrichment import blitz_client as bc  # noqa: E402


_GOOGLE_MX = [{"priority": 10, "host": "aspmx.l.google.com"}]
_MS_MX = [{"priority": 10, "host": "microsoft-com.mail.protection.outlook.com"}]


class TestMxProviderFromRecords:
    def test_google(self):
        assert bc._mx_provider_from_records(_GOOGLE_MX) != ""

    def test_microsoft(self):
        assert bc._mx_provider_from_records(_MS_MX) != ""

    def test_junk_shapes(self):
        assert bc._mx_provider_from_records(None) == ""
        assert bc._mx_provider_from_records("not-a-list") == ""
        assert bc._mx_provider_from_records([]) == ""
        assert bc._mx_provider_from_records([{"priority": 10}]) == ""  # no host
        assert bc._mx_provider_from_records(["plain-string"]) == ""


class TestStampMxFields:
    def test_stamps_provider_new_dict(self):
        result = {"found": True, "email": "a@b.com", "mx": _GOOGLE_MX}
        stamped = bc._stamp_mx_fields(result)
        assert stamped is not result  # immutability: new dict
        assert stamped["mx_provider"]
        assert result.get("mx_provider") is None  # original untouched

    def test_no_mx_passthrough(self):
        result = {"found": False, "email": None, "all_emails": []}
        assert bc._stamp_mx_fields(result) is result  # nothing to stamp

    def test_non_dict_passthrough(self):
        assert bc._stamp_mx_fields(None) is None
        assert bc._stamp_mx_fields("x") == "x"

    def test_catch_all_defensive_parse(self):
        result = {
            "found": True,
            "email": "a@b.com",
            "mx": _MS_MX,
            "all_emails": [{"email": "a@b.com", "is_catch_all": True}],
        }
        stamped = bc._stamp_mx_fields(result)
        assert stamped["domain_catch_all"] is True

    def test_seg_vocabulary_match(self):
        """The stamp must equal what our own SEG classifier derives for the
        same hosts — one vocabulary across DoH-side and Blitz-side MX."""
        from enrichment import seg

        for mx in (_GOOGLE_MX, _MS_MX):
            hosts = [r["host"] for r in mx]
            _classification, provider = seg._classify_mx_hosts(hosts)
            assert bc._mx_provider_from_records(mx) == provider
