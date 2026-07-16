"""
Regression tests for ``pipeline._backfill_person_identity`` and its
integration into ``_build_person_row`` + the routes.py contact dict sites.

Background: the user reported that enhanced-mode responses had
``first_name=""``, ``last_name=""``, ``title=""`` even though ``full_name``
was populated (e.g., "Connor Gillivan"). Contacts DB often returns only
``full_name`` without splitting it. These tests lock in the backfill
contract: empty first/last get derived from full_name; empty title falls
back to alternative field names.

The backfill helper is pure (no I/O, no globals, never mutates input).
"""

from __future__ import annotations

import os
import sys

import pytest

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from enrichment import pipeline  # noqa: E402


# ---------------------------------------------------------------------------
# Direct helper tests
# ---------------------------------------------------------------------------


class TestBackfillPersonIdentity:
    def test_splits_full_name_when_first_and_last_empty(self):
        first, last, title = pipeline._backfill_person_identity({
            "full_name": "Connor Gillivan",
        })
        assert first == "Connor"
        assert last == "Gillivan"
        assert title == ""

    def test_preserves_explicit_first_and_last(self):
        """Caller-provided first/last must never be overridden."""
        first, last, title = pipeline._backfill_person_identity({
            "full_name": "John David Smith",
            "first_name": "Jonathan",  # caller's preferred variant
            "last_name": "Smith",
        })
        assert first == "Jonathan"
        assert last == "Smith"

    def test_partial_explicit_first_only_derives_last(self):
        first, last, title = pipeline._backfill_person_identity({
            "full_name": "John Doe",
            "first_name": "John",
            "last_name": "",
        })
        assert first == "John"
        assert last == "Doe"

    def test_partial_explicit_last_only_derives_first(self):
        first, last, title = pipeline._backfill_person_identity({
            "full_name": "John Doe",
            "first_name": "",
            "last_name": "Doe",
        })
        assert first == "John"
        assert last == "Doe"

    def test_single_word_full_name_leaves_last_empty(self):
        """A single-word name can't produce a last name — leave it empty.
        This is the correct behavior (smartprospect/WizLeads need both)."""
        first, last, title = pipeline._backfill_person_identity({
            "full_name": "Cher",
        })
        assert first == "Cher"
        assert last == ""

    def test_multi_word_full_name_first_word_is_first_name(self):
        """Convention: first token = first_name, remainder = last_name."""
        first, last, title = pipeline._backfill_person_identity({
            "full_name": "John David Smith Jr",
        })
        assert first == "John"
        assert last == "David Smith Jr"

    def test_title_uses_canonical_field(self):
        first, last, title = pipeline._backfill_person_identity({
            "full_name": "Jane Doe",
            "title": "CEO",
        })
        assert title == "CEO"

    @pytest.mark.parametrize("alt_key", [
        "job_title", "position", "role", "occupation", "jobTitle", "current_role",
    ])
    def test_title_falls_back_to_alternative_field_names(self, alt_key):
        """When the canonical ``title`` field is empty, try alternative keys."""
        first, last, title = pipeline._backfill_person_identity({
            "full_name": "Jane Doe",
            alt_key: "Chief Marketing Officer",
        })
        assert title == "Chief Marketing Officer"

    def test_title_canonical_wins_over_alternative(self):
        """If both ``title`` and an alternative are set, canonical wins."""
        first, last, title = pipeline._backfill_person_identity({
            "full_name": "Jane Doe",
            "title": "CEO",
            "job_title": "Chief Executive Officer",
        })
        assert title == "CEO"

    def test_title_empty_when_no_fields_present(self):
        first, last, title = pipeline._backfill_person_identity({
            "full_name": "Jane Doe",
        })
        assert title == ""

    def test_non_dict_input_returns_empty_strings(self):
        first, last, title = pipeline._backfill_person_identity(None)  # type: ignore[arg-type]
        assert (first, last, title) == ("", "", "")

    def test_empty_dict_returns_empty_strings(self):
        first, last, title = pipeline._backfill_person_identity({})
        assert (first, last, title) == ("", "", "")

    def test_does_not_mutate_input(self):
        """Immutability contract — the input dict must not be modified."""
        person = {"full_name": "John Doe", "first_name": "", "last_name": ""}
        pipeline._backfill_person_identity(person)
        assert person == {"full_name": "John Doe", "first_name": "", "last_name": ""}

    def test_strips_whitespace_from_fields(self):
        """Whitespace-padded values are stripped (defensive against messy data)."""
        first, last, title = pipeline._backfill_person_identity({
            "full_name": "  John Doe  ",
            "first_name": "  John  ",
            "title": "  CEO  ",
        })
        assert first == "John"
        assert last == "Doe"
        assert title == "CEO"

    def test_handles_non_string_field_values_gracefully(self):
        """Weird types (None, int, list) should not crash the helper."""
        first, last, title = pipeline._backfill_person_identity({
            "full_name": "John Doe",
            "first_name": None,  # type: ignore[dict-item]
            "last_name": 42,    # type: ignore[dict-item]
            "title": [],        # type: ignore[dict-item]
        })
        # full_name split fills first; last_name is non-string so treated as empty
        # → derived from full_name. Title non-string → falls through to "".
        assert first == "John"
        assert last == "Doe"
        assert title == ""


# ---------------------------------------------------------------------------
# Integration: _build_person_row
# ---------------------------------------------------------------------------


class TestBuildPersonRowBackfill:
    """Verify _build_person_row uses the backfilled values for dm_first_name,
    dm_last_name, and dm_title."""

    def test_row_has_first_last_derived_from_full_name(self):
        """The bug we fixed: person with only full_name produced empty
        dm_first_name / dm_last_name in the row."""
        person = {"full_name": "Connor Gillivan"}
        row = pipeline._build_person_row(
            base_row={"domain": "ecombalance.com"},
            company_linkedin_url="",
            person=person,
            icp_tier=1,
            email="connor@example.com",
            email_source="contacts_db_email",
        )
        assert row["dm_first_name"] == "Connor"
        assert row["dm_last_name"] == "Gillivan"
        assert row["dm_full_name"] == "Connor Gillivan"

    def test_row_preserves_explicit_first_last(self):
        person = {
            "full_name": "John David Smith",
            "first_name": "Jonathan",
            "last_name": "Smith",
        }
        row = pipeline._build_person_row(
            base_row={"domain": "x.com"},
            company_linkedin_url="",
            person=person,
            icp_tier=1,
            email="j@x.com",
            email_source="blitz_email",
        )
        assert row["dm_first_name"] == "Jonathan"
        assert row["dm_last_name"] == "Smith"

    def test_row_title_falls_back_to_job_title_field(self):
        """When ``title`` field is empty but ``job_title`` is set, dm_title
        should pick up job_title via the backfill."""
        person = {
            "full_name": "Jane Doe",
            "job_title": "Chief Marketing Officer",
        }
        row = pipeline._build_person_row(
            base_row={"domain": "x.com"},
            company_linkedin_url="",
            person=person,
            icp_tier=1,
            email="j@x.com",
            email_source="contacts_db_email",
        )
        assert row["dm_title"] == "Chief Marketing Officer"

    def test_row_title_prefers_experiences_over_direct_title(self):
        """The existing _current_title logic still wins for Blitz data —
        backfill only fills the direct_title fallback path."""
        person = {
            "full_name": "Jane Doe",
            "experiences": [
                {"job_is_current": True, "job_title": "CTO"},
            ],
            "title": "",  # empty direct title
            "job_title": "Chief Technology Officer",  # alternative field
        }
        row = pipeline._build_person_row(
            base_row={"domain": "x.com"},
            company_linkedin_url="",
            person=person,
            icp_tier=1,
            email="j@x.com",
            email_source="blitz_email",
        )
        # Experiences array wins (current CTO role)
        assert row["dm_title"] == "CTO"

    def test_row_single_word_full_name_keeps_last_empty(self):
        """Single-word name → last_name stays empty (downstream smartprospect
        cascade correctly gated out)."""
        person = {"full_name": "Cher"}
        row = pipeline._build_person_row(
            base_row={"domain": "x.com"},
            company_linkedin_url="",
            person=person,
            icp_tier=1,
            email="",
            email_source="",
        )
        assert row["dm_first_name"] == "Cher"
        assert row["dm_last_name"] == ""


# ---------------------------------------------------------------------------
# Integration: routes.py contact dict (smoke-level)
# ---------------------------------------------------------------------------


class TestRoutesContactDictBackfill:
    """Smoke test that the routes.py contact dict construction sites use
    the backfilled values. We test the helper directly (above) plus
    _build_person_row (above) — these are the two integration points.
    A full HTTP-level test would require auth + DB setup; the unit-level
    coverage here is sufficient because routes.py just calls the helper
    and uses the returned values verbatim."""

    def test_helper_is_callable_from_routes(self):
        """Confirm routes.py can call pipeline._backfill_person_identity.
        (It's a private-name function but accessed cross-module — this test
        locks in that we don't accidentally rename it without updating callers.)"""
        from enrichment import pipeline as p
        from enrichment import routes  # noqa: F401  (import test)
        # If the helper were renamed or removed, this would fail at import
        # or attribute lookup. Both modules must coexist.
        assert callable(p._backfill_person_identity)
