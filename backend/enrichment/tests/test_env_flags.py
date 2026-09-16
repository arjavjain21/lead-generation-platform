"""
Tests for enrichment.env_flags — the single boolean env-flag parser.

Fix-wave context (2026-09-16): the flag vocabulary was inconsistent —
``list_builder._env_flag`` treated ``off`` as false while the inline gates
in ``pipeline.py`` accepted only ``false``/``0``/``no``, so
``ENABLE_PHONE_BUNDLE=off`` silently meant ON in the pipeline. One helper
now serves every gate; these tests pin the vocabulary and one gate per
consumer module.

Run:
    python -m pytest enrichment/tests/test_env_flags.py -v
"""

from __future__ import annotations

import os
import sys
import unittest
from unittest.mock import patch

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from enrichment import list_builder as lb  # noqa: E402
from enrichment import pipeline as pipeline_mod  # noqa: E402
from enrichment.env_flags import env_flag  # noqa: E402


class TestEnvFlagHelper(unittest.TestCase):
    def test_unset_uses_default(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ENV_FLAG_TEST_X", None)
            self.assertTrue(env_flag("ENV_FLAG_TEST_X"))
            self.assertIs(env_flag("ENV_FLAG_TEST_X", default=True), True)
            self.assertIs(env_flag("ENV_FLAG_TEST_X", default=False), False)

    def test_blank_uses_default(self):
        for blank in ("", "   ", "\t"):
            with patch.dict(os.environ, {"ENV_FLAG_TEST_X": blank}):
                self.assertTrue(env_flag("ENV_FLAG_TEST_X"))
                self.assertIs(env_flag("ENV_FLAG_TEST_X", default=False), False)

    def test_true_vocabulary(self):
        for value in ("true", "1", "yes", "on"):
            with patch.dict(os.environ, {"ENV_FLAG_TEST_X": value}):
                self.assertIs(env_flag("ENV_FLAG_TEST_X"), True, value)

    def test_false_vocabulary(self):
        for value in ("false", "0", "no", "off"):
            with patch.dict(os.environ, {"ENV_FLAG_TEST_X": value}):
                self.assertIs(env_flag("ENV_FLAG_TEST_X"), False, value)

    def test_case_and_whitespace_insensitive(self):
        for value in ("OFF", "Off", " off ", "TRUE", "Yes", "ON", "No", "0"):
            expected = value.strip().lower() not in ("false", "0", "no", "off")
            with patch.dict(os.environ, {"ENV_FLAG_TEST_X": value}):
                self.assertIs(env_flag("ENV_FLAG_TEST_X"), expected, value)

    def test_garbage_falls_back_to_default(self):
        for garbage in ("banana", "2", "enable", "-1"):
            with patch.dict(os.environ, {"ENV_FLAG_TEST_X": garbage}):
                self.assertTrue(env_flag("ENV_FLAG_TEST_X"), garbage)
                self.assertIs(
                    env_flag("ENV_FLAG_TEST_X", default=False), False, garbage,
                )


class TestPipelineGates(unittest.TestCase):
    """pipeline.py consumes env_flag for ENABLE_BLITZ_MISS_SKIP,
    ENABLE_BLITZ_FIND_PEOPLE_BATCH and ENABLE_PHONE_BUNDLE."""

    def test_miss_skip_gate_off_is_false(self):
        """The ops footgun this module fixed: 'off' must disable the gate in
        the pipeline too (it previously evaluated true there)."""
        with patch.dict(os.environ, {"ENABLE_BLITZ_MISS_SKIP": "off"}):
            self.assertFalse(pipeline_mod._blitz_miss_wiring_armed())
        with patch.dict(os.environ, {"ENABLE_BLITZ_MISS_SKIP": "true"}):
            self.assertTrue(pipeline_mod._blitz_miss_wiring_armed())
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ENABLE_BLITZ_MISS_SKIP", None)
            self.assertTrue(pipeline_mod._blitz_miss_wiring_armed())


class TestListBuilderGates(unittest.TestCase):
    """list_builder.py consumes env_flag for the same three flags."""

    def test_prepass_gate_off_is_false(self):
        with patch.dict(os.environ, {"ENABLE_BLITZ_FIND_PEOPLE_BATCH": "off"}):
            self.assertFalse(lb._blitz_find_people_prepass_enabled())
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ENABLE_BLITZ_FIND_PEOPLE_BATCH", None)
            self.assertTrue(lb._blitz_find_people_prepass_enabled())

    def test_miss_skip_gate_off_is_false(self):
        with patch.dict(os.environ, {"ENABLE_BLITZ_MISS_SKIP": "off"}):
            self.assertFalse(lb._blitz_miss_wiring_armed())

    def test_phone_bundle_gate_off_is_false(self):
        with patch.dict(os.environ, {"ENABLE_PHONE_BUNDLE": "off"}):
            self.assertFalse(lb._phone_bundle_enabled())
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ENABLE_PHONE_BUNDLE", None)
            self.assertTrue(lb._phone_bundle_enabled())


if __name__ == "__main__":
    unittest.main()
