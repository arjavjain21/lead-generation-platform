"""
Comprehensive unit tests for ``enrichment.response_normalizer``.

Covers:
  * All four field normalizers (email, name, linkedin, domain).
  * Junk filter rule.
  * Per-provider dispatchers for Contacts DB, Blitz, BetterEnrich,
    WizLeads.
  * Generic dispatcher with known and unknown providers.
  * Edge cases: None, empty strings, whitespace, placeholders, mixed
    case, unicode, display-name emails, LinkedIn URL variants, domain
    variants.

Every public function in the module has at least one dedicated test
class. Every per-provider normalizer has at least 5 tests.
"""

from __future__ import annotations

import os
import sys

import pytest

# Make sure the backend root is on sys.path so `enrichment` is importable
# regardless of where pytest is invoked from.
_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from enrichment import response_normalizer as rn  # noqa: E402


# ---------------------------------------------------------------------------
# normalize_email
# ---------------------------------------------------------------------------

class TestNormalizeEmail:
    def test_none(self):
        assert rn.normalize_email(None) == ""

    def test_empty(self):
        assert rn.normalize_email("") == ""

    def test_whitespace_only(self):
        assert rn.normalize_email("    ") == ""
        assert rn.normalize_email("\t\n") == ""

    @pytest.mark.parametrize("placeholder", [
        "no_email", "no-email", "noemail", "n/a", "na", "n.a.",
        "none", "null", "nil", "unknown", "-", "--", "...",
        "not_found", "notfound", "false", "undefined",
    ])
    def test_placeholders(self, placeholder):
        assert rn.normalize_email(placeholder) == ""
        # Placeholders are case-insensitive.
        assert rn.normalize_email(placeholder.upper()) == ""
        assert rn.normalize_email(placeholder.lower()) == ""

    def test_valid_email(self):
        assert rn.normalize_email("john@acme.com") == "john@acme.com"

    def test_mixed_case(self):
        assert rn.normalize_email("John@AcMe.Com") == "john@acme.com"

    def test_leading_trailing_whitespace(self):
        assert rn.normalize_email("  john@acme.com  ") == "john@acme.com"

    def test_display_name_format(self):
        # "John Doe <john@x.com>" should extract just john@x.com
        assert rn.normalize_email("John Doe <john@x.com>") == "john@x.com"

    def test_just_angle_brackets(self):
        assert rn.normalize_email("<john@x.com>") == "john@x.com"

    def test_no_at_sign(self):
        assert rn.normalize_email("not-an-email") == ""
        assert rn.normalize_email("john.example.com") == ""

    def test_no_domain(self):
        assert rn.normalize_email("john@") == ""

    def test_no_local_part(self):
        assert rn.normalize_email("@acme.com") == ""

    def test_naked_tld_less_host(self):
        # No dot in host -> not a valid email.
        assert rn.normalize_email("john@localhost") == ""

    def test_non_string_input(self):
        # Numbers should not raise.
        assert rn.normalize_email(123) == ""
        assert rn.normalize_email(0) == ""

    def test_object_input(self):
        # An object without a clean __str__ fallback shouldn't raise.
        assert rn.normalize_email(object()) in {"", "..."}  # defensive


# ---------------------------------------------------------------------------
# normalize_person_name
# ---------------------------------------------------------------------------

class TestNormalizePersonName:
    def test_none(self):
        assert rn.normalize_person_name(None) == ""

    def test_empty(self):
        assert rn.normalize_person_name("") == ""

    def test_whitespace_only(self):
        assert rn.normalize_person_name("   ") == ""
        assert rn.normalize_person_name("\n\t") == ""

    @pytest.mark.parametrize("placeholder", [
        "unknown", "n/a", "na", "none", "null",
        "-", "--", "...",
        "test", "test user", "test test",
        "not found", "not_found",
    ])
    def test_placeholders(self, placeholder):
        assert rn.normalize_person_name(placeholder) == ""
        # Case-insensitive.
        assert rn.normalize_person_name(placeholder.upper()) == ""

    def test_valid_name(self):
        assert rn.normalize_person_name("Walter White") == "Walter White"

    def test_preserves_case(self):
        assert rn.normalize_person_name("WALTER WHITE") == "WALTER WHITE"
        assert rn.normalize_person_name("McKenzie O'Brien") == "McKenzie O'Brien"

    def test_strips_whitespace(self):
        assert rn.normalize_person_name("  Walter White  ") == "Walter White"

    def test_collapses_internal_whitespace(self):
        assert rn.normalize_person_name("Walter   White") == "Walter White"
        assert rn.normalize_person_name("Walter\tWhite") == "Walter White"
        assert rn.normalize_person_name("Walter\nWhite") == "Walter White"

    def test_unicode(self):
        # Names with diacritics are preserved.
        assert rn.normalize_person_name("José Ramírez") == "José Ramírez"
        assert rn.normalize_person_name("François Müller") == "François Müller"

    def test_single_name(self):
        # A single name is still meaningful (some cultures use mononyms).
        assert rn.normalize_person_name("Madonna") == "Madonna"

    def test_non_string_input(self):
        assert rn.normalize_person_name(42) == ""


# ---------------------------------------------------------------------------
# normalize_linkedin_url
# ---------------------------------------------------------------------------

class TestNormalizeLinkedinUrl:
    def test_none(self):
        assert rn.normalize_linkedin_url(None) == ""

    def test_empty(self):
        assert rn.normalize_linkedin_url("") == ""

    def test_whitespace(self):
        assert rn.normalize_linkedin_url("   ") == ""

    @pytest.mark.parametrize("placeholder", [
        "n/a", "none", "null", "unknown",
        "-", "--", "...",
        "not found", "not_found", "no_linkedin", "no-linkedin",
    ])
    def test_placeholders(self, placeholder):
        assert rn.normalize_linkedin_url(placeholder) == ""

    def test_not_linkedin(self):
        assert rn.normalize_linkedin_url("https://example.com/in/john") == ""
        assert rn.normalize_linkedin_url("https://facebook.com/john") == ""

    def test_with_protocol(self):
        assert (
            rn.normalize_linkedin_url("https://linkedin.com/in/johndoe")
            == "linkedin.com/in/johndoe"
        )
        assert (
            rn.normalize_linkedin_url("http://linkedin.com/in/johndoe")
            == "linkedin.com/in/johndoe"
        )

    def test_with_www(self):
        assert (
            rn.normalize_linkedin_url("https://www.linkedin.com/in/johndoe")
            == "linkedin.com/in/johndoe"
        )

    def test_with_trailing_slash(self):
        assert (
            rn.normalize_linkedin_url("https://linkedin.com/in/johndoe/")
            == "linkedin.com/in/johndoe"
        )

    def test_uppercase(self):
        # Should lowercase host + path.
        assert (
            rn.normalize_linkedin_url("HTTP://Linkedin.com/in/JohnDoe/")
            == "linkedin.com/in/johndoe"
        )

    def test_with_query(self):
        assert (
            rn.normalize_linkedin_url("https://linkedin.com/in/johndoe?utm_source=x")
            == "linkedin.com/in/johndoe"
        )

    def test_bare_no_protocol(self):
        assert (
            rn.normalize_linkedin_url("linkedin.com/in/johndoe")
            == "linkedin.com/in/johndoe"
        )

    def test_company_url(self):
        # Company URLs should also be canonicalized.
        assert (
            rn.normalize_linkedin_url("https://www.linkedin.com/company/acme")
            == "linkedin.com/company/acme"
        )

    def test_just_host(self):
        # No path -> empty.
        assert rn.normalize_linkedin_url("https://linkedin.com") == ""
        assert rn.normalize_linkedin_url("https://linkedin.com/") == ""

    def test_non_string_input(self):
        assert rn.normalize_linkedin_url(42) == ""


# ---------------------------------------------------------------------------
# normalize_domain
# ---------------------------------------------------------------------------

class TestNormalizeDomain:
    def test_none(self):
        assert rn.normalize_domain(None) == ""

    def test_empty(self):
        assert rn.normalize_domain("") == ""

    def test_whitespace(self):
        assert rn.normalize_domain("   ") == ""

    @pytest.mark.parametrize("placeholder", [
        "n/a", "none", "null", "unknown",
        "-", "--", "...",
        "no_domain", "no-domain", "not found",
    ])
    def test_placeholders(self, placeholder):
        assert rn.normalize_domain(placeholder) == ""

    def test_bare_domain(self):
        assert rn.normalize_domain("acme.com") == "acme.com"

    def test_uppercase(self):
        assert rn.normalize_domain("Acme.COM") == "acme.com"
        assert rn.normalize_domain("ACME.COM") == "acme.com"

    def test_with_protocol(self):
        assert rn.normalize_domain("https://acme.com") == "acme.com"
        assert rn.normalize_domain("http://acme.com") == "acme.com"

    def test_with_www(self):
        assert rn.normalize_domain("https://www.acme.com") == "acme.com"
        assert rn.normalize_domain("www.acme.com") == "acme.com"

    def test_with_path(self):
        assert rn.normalize_domain("https://acme.com/path/to/page") == "acme.com"
        assert rn.normalize_domain("acme.com/path") == "acme.com"

    def test_with_query(self):
        assert (
            rn.normalize_domain("https://acme.com/?utm_source=gmb")
            == "acme.com"
        )

    def test_with_port(self):
        assert rn.normalize_domain("acme.com:8080") == "acme.com"

    def test_email_is_not_domain(self):
        assert rn.normalize_domain("john@acme.com") == ""

    def test_no_dot(self):
        assert rn.normalize_domain("localhost") == ""
        assert rn.normalize_domain("notadomain") == ""

    def test_trailing_dot(self):
        assert rn.normalize_domain("acme.com.") == "acme.com"

    def test_with_credentials(self):
        assert rn.normalize_domain("user:pass@acme.com") == "acme.com"

    def test_non_string_input(self):
        assert rn.normalize_domain(42) == ""

    def test_real_world_mesterh(self):
        # The case from the 2026-06-13 fix: URL with utm_source should
        # be reduced to the bare domain.
        assert (
            rn.normalize_domain("https://mesterh-service.de/?utm_source=gmb")
            == "mesterh-service.de"
        )


# ---------------------------------------------------------------------------
# Predicates
# ---------------------------------------------------------------------------

class TestPredicates:
    def test_is_meaningful_email_true(self):
        assert rn.is_meaningful_email("john@acme.com") is True

    def test_is_meaningful_email_false(self):
        assert rn.is_meaningful_email(None) is False
        assert rn.is_meaningful_email("") is False
        assert rn.is_meaningful_email("no_email") is False
        assert rn.is_meaningful_email("no-at-sign") is False

    def test_is_meaningful_person_true(self):
        assert rn.is_meaningful_person("Walter White") is True

    def test_is_meaningful_person_false(self):
        assert rn.is_meaningful_person(None) is False
        assert rn.is_meaningful_person("") is False
        assert rn.is_meaningful_person("unknown") is False
        assert rn.is_meaningful_person("test") is False


# ---------------------------------------------------------------------------
# Per-provider: Contacts DB
# ---------------------------------------------------------------------------

class TestNormalizeContactsDbContact:
    def test_valid_full_contact(self):
        raw = {
            "person_id": "abc",
            "full_name": "Walter White",
            "first_name": "Walter",
            "last_name": "White",
            "email": "walter@acme.com",
            "title": "CEO",
            "headline": "CEO at Acme",
            "linkedin_url": "https://linkedin.com/in/walterwhite",
            "domain": "acme.com",
        }
        result = rn.normalize_contacts_db_contact(raw)
        assert result is not None
        assert result["source"] == "contacts_db"
        assert result["email"] == "walter@acme.com"
        assert result["first_name"] == "Walter"
        assert result["last_name"] == "White"
        assert result["full_name"] == "Walter White"
        assert result["title"] == "CEO"
        assert result["linkedin_url"] == "linkedin.com/in/walterwhite"
        assert result["domain"] == "acme.com"

    def test_junk_all_empty(self):
        raw = {"person_id": "abc"}
        assert rn.normalize_contacts_db_contact(raw) is None

    def test_junk_placeholders_only(self):
        raw = {
            "email": "no_email",
            "full_name": "unknown",
            "linkedin_url": "n/a",
        }
        assert rn.normalize_contacts_db_contact(raw) is None

    def test_email_only_kept(self):
        # No name, no LinkedIn — should still be kept (email is meaningful).
        raw = {"email": "someone@acme.com"}
        result = rn.normalize_contacts_db_contact(raw)
        assert result is not None
        assert result["email"] == "someone@acme.com"
        assert result["full_name"] == ""

    def test_name_only_kept(self):
        raw = {"full_name": "Saul Goodman"}
        result = rn.normalize_contacts_db_contact(raw)
        assert result is not None
        assert result["full_name"] == "Saul Goodman"
        assert result["email"] == ""

    def test_linkedin_only_kept(self):
        raw = {"linkedin_url": "https://linkedin.com/in/saulgoodman"}
        result = rn.normalize_contacts_db_contact(raw)
        assert result is not None
        assert result["linkedin_url"] == "linkedin.com/in/saulgoodman"

    def test_first_last_combined_to_full(self):
        # If full_name missing but first/last present, full_name is built.
        raw = {"first_name": "Jesse", "last_name": "Pinkman"}
        result = rn.normalize_contacts_db_contact(raw)
        assert result is not None
        assert result["full_name"] == "Jesse Pinkman"

    def test_non_dict_returns_none(self):
        assert rn.normalize_contacts_db_contact(None) is None  # type: ignore[arg-type]
        assert rn.normalize_contacts_db_contact([]) is None  # type: ignore[arg-type]
        assert rn.normalize_contacts_db_contact("string") is None  # type: ignore[arg-type]

    def test_dirty_values_cleaned(self):
        raw = {
            "email": "  WALTER@ACME.com  ",
            "full_name": "  Walter   White  ",
            "linkedin_url": "https://www.linkedin.com/in/walterwhite/",
            "domain": "https://acme.com/?ref=foo",
        }
        result = rn.normalize_contacts_db_contact(raw)
        assert result is not None
        assert result["email"] == "walter@acme.com"
        assert result["full_name"] == "Walter White"
        assert result["linkedin_url"] == "linkedin.com/in/walterwhite"
        assert result["domain"] == "acme.com"


# ---------------------------------------------------------------------------
# Per-provider: Blitz
# ---------------------------------------------------------------------------

class TestNormalizeBlitzContact:
    def test_valid_waterfall_result_with_person_key(self):
        raw = {
            "icp": 95,
            "ranking": 1,
            "person": {
                "first_name": "Walter",
                "last_name": "White",
                "full_name": "Walter White",
                "headline": "CEO at Acme",
                "linkedin_url": "https://linkedin.com/in/walterwhite",
                "verified_email": "walter@acme.com",
                "experiences": [
                    {"job_title": "CEO", "job_is_current": True},
                ],
            },
        }
        result = rn.normalize_blitz_contact(raw)
        assert result is not None
        assert result["source"] == "blitz"
        assert result["email"] == "walter@acme.com"
        assert result["full_name"] == "Walter White"
        assert result["title"] == "CEO"
        assert result["headline"] == "CEO at Acme"
        assert result["linkedin_url"] == "linkedin.com/in/walterwhite"
        # Blitz person responses don't carry a domain.
        assert result["domain"] == ""

    def test_bare_person_dict(self):
        # Some Blitz responses are bare person dicts without "person" wrapper.
        raw = {
            "first_name": "Walter",
            "last_name": "White",
            "linkedin_url": "https://linkedin.com/in/walterwhite",
            "verified_email": "walter@acme.com",
        }
        result = rn.normalize_blitz_contact(raw)
        assert result is not None
        assert result["email"] == "walter@acme.com"

    def test_email_from_emails_list_dict_form(self):
        raw = {
            "person": {
                "full_name": "Jesse Pinkman",
                "emails": [{"email": "jesse@acme.com", "verified": False}],
            },
        }
        result = rn.normalize_blitz_contact(raw)
        assert result is not None
        assert result["email"] == "jesse@acme.com"

    def test_email_from_emails_list_string_form(self):
        raw = {
            "person": {
                "full_name": "Jesse Pinkman",
                "emails": ["jesse@acme.com"],
            },
        }
        result = rn.normalize_blitz_contact(raw)
        assert result is not None
        assert result["email"] == "jesse@acme.com"

    def test_title_from_experiences_no_current_flag(self):
        # If no experience is marked current, fall back to first.
        raw = {
            "person": {
                "full_name": "Mike",
                "experiences": [
                    {"job_title": "Senior Enforcer"},
                    {"job_title": "Enforcer"},
                ],
            },
        }
        result = rn.normalize_blitz_contact(raw)
        assert result is not None
        assert result["title"] == "Senior Enforcer"

    def test_title_direct_field_wins_when_no_experiences(self):
        raw = {"person": {"full_name": "Mike", "title": "Head of Security"}}
        result = rn.normalize_blitz_contact(raw)
        assert result is not None
        assert result["title"] == "Head of Security"

    def test_junk_person_empty(self):
        raw = {"icp": 50, "ranking": 1, "person": {}}
        assert rn.normalize_blitz_contact(raw) is None

    def test_junk_no_person_key(self):
        raw = {"icp": 50}
        assert rn.normalize_blitz_contact(raw) is None

    def test_junk_placeholders_only(self):
        raw = {
            "person": {
                "email": "no_email",
                "full_name": "unknown",
                "linkedin_url": "n/a",
            }
        }
        assert rn.normalize_blitz_contact(raw) is None

    def test_name_only_kept(self):
        raw = {"person": {"full_name": "Gus Fring"}}
        result = rn.normalize_blitz_contact(raw)
        assert result is not None
        assert result["full_name"] == "Gus Fring"
        assert result["email"] == ""

    def test_non_dict_returns_none(self):
        assert rn.normalize_blitz_contact(None) is None  # type: ignore[arg-type]
        assert rn.normalize_blitz_contact("string") is None  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Per-provider: BetterEnrich
# ---------------------------------------------------------------------------

class TestNormalizeBetterEnrichContact:
    def test_valid_email_only(self):
        raw = {"email": "john@acme.com", "email_status": "verified"}
        result = rn.normalize_better_enrich_contact(raw)
        assert result is not None
        assert result["source"] == "better_enrich"
        assert result["email"] == "john@acme.com"
        # Status / verifier are intentionally not part of the canonical shape.
        assert result["full_name"] == ""

    def test_v3_response_shape(self):
        raw = {
            "email": "john@acme.com",
            "email_status": "verified",
            "verifier": "Google",
            "esp": "Gmail",
        }
        result = rn.normalize_better_enrich_contact(raw)
        assert result is not None
        assert result["email"] == "john@acme.com"

    def test_work_email_key(self):
        raw = {"work_email": "jane@acme.com"}
        result = rn.normalize_better_enrich_contact(raw)
        assert result is not None
        assert result["email"] == "jane@acme.com"

    def test_nested_data_shape(self):
        raw = {"data": {"email": "nested@acme.com"}}
        result = rn.normalize_better_enrich_contact(raw)
        assert result is not None
        assert result["email"] == "nested@acme.com"

    def test_junk_placeholder_email(self):
        raw = {"email": "no_email"}
        assert rn.normalize_better_enrich_contact(raw) is None

    def test_junk_empty(self):
        raw = {}
        assert rn.normalize_better_enrich_contact(raw) is None

    def test_junk_invalid_email(self):
        # Email without @ is normalized to "" and there's no name/linkedin
        # to fall back on — junk.
        raw = {"email": "not-an-email"}
        assert rn.normalize_better_enrich_contact(raw) is None

    def test_name_fields_carried_through(self):
        raw = {
            "email": "john@acme.com",
            "first_name": "John",
            "last_name": "Doe",
        }
        result = rn.normalize_better_enrich_contact(raw)
        assert result is not None
        assert result["first_name"] == "John"
        assert result["last_name"] == "Doe"
        assert result["full_name"] == "John Doe"

    def test_non_dict_returns_none(self):
        assert rn.normalize_better_enrich_contact(None) is None  # type: ignore[arg-type]
        assert rn.normalize_better_enrich_contact([]) is None  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Per-provider: WizLeads
# ---------------------------------------------------------------------------

class TestNormalizeWizleadsContact:
    def test_valid_full(self):
        raw = {
            "email": "john@acme.com",
            "catchall": "YES",
            "provider": "Google",
            "normalized_fname": "John",
            "normalized_lname": "Doe",
            "website": "acme.com",
        }
        result = rn.normalize_wizleads_contact(raw)
        assert result is not None
        assert result["source"] == "wizleads"
        assert result["email"] == "john@acme.com"
        assert result["first_name"] == "John"
        assert result["last_name"] == "Doe"
        assert result["domain"] == "acme.com"

    def test_email_only(self):
        raw = {"email": "jane@acme.com"}
        result = rn.normalize_wizleads_contact(raw)
        assert result is not None
        assert result["email"] == "jane@acme.com"

    def test_first_name_fallback_to_input(self):
        # If normalized_fname is missing, fall back to first_name.
        raw = {
            "email": "jane@acme.com",
            "first_name": "Jane",
            "last_name": "Roe",
        }
        result = rn.normalize_wizleads_contact(raw)
        assert result is not None
        assert result["first_name"] == "Jane"
        assert result["last_name"] == "Roe"

    def test_junk_placeholder_email(self):
        raw = {"email": "no_email"}
        assert rn.normalize_wizleads_contact(raw) is None

    def test_junk_empty(self):
        raw = {}
        assert rn.normalize_wizleads_contact(raw) is None

    def test_website_used_as_domain(self):
        raw = {
            "email": "jane@acme.com",
            "website": "https://www.acme.com/?ref=x",
        }
        result = rn.normalize_wizleads_contact(raw)
        assert result is not None
        assert result["domain"] == "acme.com"

    def test_domain_key_used_as_fallback(self):
        raw = {
            "email": "jane@acme.com",
            "domain": "acme.com",
        }
        result = rn.normalize_wizleads_contact(raw)
        assert result is not None
        assert result["domain"] == "acme.com"

    def test_non_dict_returns_none(self):
        assert rn.normalize_wizleads_contact(None) is None  # type: ignore[arg-type]
        assert rn.normalize_wizleads_contact("string") is None  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Per-provider: GetLeads
# ---------------------------------------------------------------------------

class TestNormalizeGetLeadsContact:
    """GetLeads (app.getleads.io) — handles enrich items + decision-makers.

    The full per-shape suite lives in test_getleads_normalizer.py; this class
    covers the dispatch + key contract here so regressions are caught by the
    main normalizer suite too.
    """

    def test_enrich_item_with_nested_data(self):
        raw = {
            "email": "zac@earth.works",
            "profileUrl": "https://www.linkedin.com/in/zac-chaffin-0475a023",
            "data": {
                "first_name": "Zac", "last_name": "Chaffin",
                "person_full_name": "Zac Chaffin",
                "job_title": "Chief Financial Officer",
                "linkedin_headline": "CFO at Earthworks, Inc.",
                "person_linkedin_url": "https://www.linkedin.com/in/zac-chaffin-0475a023",
                "domain_org": "earth.works",
                "email_address": "zac@earth.works",
            },
        }
        result = rn.normalize_getleads_contact(raw)
        assert result is not None
        assert result["source"] == "getleads"
        assert result["email"] == "zac@earth.works"
        assert result["full_name"] == "Zac Chaffin"
        assert result["title"] == "Chief Financial Officer"
        assert result["domain"] == "earth.works"

    def test_decision_makers_flat_record(self):
        raw = {
            "first_name": "Brad", "last_name": "Bobenrieth",
            "email_address": "bbobenrieth@meadowsfarms.com",
            "job_title": "Vice President", "org_domain": "meadowsfarms.com",
            "person_linkedin_url": "https://www.linkedin.com/in/brad-bobenrieth-84a45695",
        }
        result = rn.normalize_getleads_contact(raw)
        assert result is not None
        assert result["email"] == "bbobenrieth@meadowsfarms.com"
        assert result["domain"] == "meadowsfarms.com"

    def test_not_found_returns_none(self):
        # success:true but email:null + data:null — must be skipped.
        raw = {"success": True, "email": None, "data": None}
        assert rn.normalize_getleads_contact(raw) is None

    def test_partial_no_email_returns_none(self):
        # data populated with identity but no email_address — skipped.
        raw = {
            "email": None,
            "data": {
                "person_full_name": "Troy Nelson",
                "person_linkedin_url": "https://www.linkedin.com/in/troy-nelson",
            },
        }
        assert rn.normalize_getleads_contact(raw) is None

    def test_junk_empty(self):
        assert rn.normalize_getleads_contact({}) is None

    def test_non_dict_returns_none(self):
        assert rn.normalize_getleads_contact(None) is None  # type: ignore[arg-type]
        assert rn.normalize_getleads_contact("string") is None  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Generic dispatcher
# ---------------------------------------------------------------------------

class TestNormalizeProviderContact:
    def test_contacts_db(self):
        raw = {"full_name": "John", "email": "john@acme.com"}
        result = rn.normalize_provider_contact("contacts_db", raw)
        assert result is not None
        assert result["source"] == "contacts_db"

    def test_blitz(self):
        raw = {"person": {"full_name": "John", "email": "john@acme.com"}}
        result = rn.normalize_provider_contact("blitz", raw)
        assert result is not None
        assert result["source"] == "blitz"

    def test_better_enrich(self):
        raw = {"email": "john@acme.com"}
        result = rn.normalize_provider_contact("better_enrich", raw)
        assert result is not None
        assert result["source"] == "better_enrich"

    def test_wizleads(self):
        raw = {"email": "john@acme.com", "normalized_fname": "John"}
        result = rn.normalize_provider_contact("wizleads", raw)
        assert result is not None
        assert result["source"] == "wizleads"

    def test_getleads(self):
        raw = {"email": "john@acme.com", "data": {"email_address": "john@acme.com"}}
        result = rn.normalize_provider_contact("getleads", raw)
        assert result is not None
        assert result["source"] == "getleads"

    def test_getleads_aliases(self):
        raw = {"email": "john@acme.com"}
        assert rn.normalize_provider_contact("get_leads", raw) is not None
        assert rn.normalize_provider_contact("get-leads", raw) is not None
        assert rn.normalize_provider_contact("GETLEADS", raw) is not None

    def test_unknown_provider(self):
        raw = {"email": "john@acme.com"}
        assert rn.normalize_provider_contact("unknown_provider", raw) is None

    def test_case_insensitive(self):
        raw = {"email": "john@acme.com"}
        assert rn.normalize_provider_contact("CONTACTS_DB", raw) is not None
        assert rn.normalize_provider_contact("Blitz", raw) is not None
        assert rn.normalize_provider_contact("WIZLEADS", raw) is not None

    def test_alias_dash_underscore(self):
        raw = {"email": "john@acme.com"}
        assert rn.normalize_provider_contact("better-enrich", raw) is not None
        assert rn.normalize_provider_contact("wiz_leads", raw) is not None

    def test_junk_returns_none_via_dispatcher(self):
        # Even for a known provider, junk returns None.
        raw = {"email": "no_email"}
        assert rn.normalize_provider_contact("contacts_db", raw) is None

    def test_non_dict_raw(self):
        assert rn.normalize_provider_contact("blitz", None) is None  # type: ignore[arg-type]
        assert rn.normalize_provider_contact("blitz", []) is None  # type: ignore[arg-type]

    def test_non_string_source(self):
        assert rn.normalize_provider_contact(None, {"email": "a@b.com"}) is None  # type: ignore[arg-type]
        assert rn.normalize_provider_contact(42, {"email": "a@b.com"}) is None  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Output-shape invariants
# ---------------------------------------------------------------------------

class TestOutputShapeInvariants:
    """The normalized dict must always have exactly these keys, all str.

    Phase 2 (full capture, 2026-08-14) widened the canonical record shared
    by ALL providers: the 9 original identity keys plus 11 passthrough
    firmographic fields ("" when the provider doesn't emit them). Providers
    that pass an ``extra`` blob (GetLeads: ``_raw_getleads``) add that key
    too — these fixtures carry no blob.
    """

    EXPECTED_KEYS = {
        "email", "first_name", "last_name", "full_name",
        "title", "headline", "linkedin_url", "domain", "source",
        "phone", "city", "country", "company_name", "company_industry",
        "employee_count", "revenue", "linkedin_connections",
        "email_last_verified_at", "job_level", "job_function",
    }

    def test_contacts_db_keys(self):
        result = rn.normalize_contacts_db_contact({"email": "a@b.com"})
        assert set(result.keys()) == self.EXPECTED_KEYS
        # Every value must be a str (never None).
        for v in result.values():
            assert isinstance(v, str)

    def test_blitz_keys(self):
        result = rn.normalize_blitz_contact({"person": {"full_name": "x"}})
        assert result is not None
        assert set(result.keys()) == self.EXPECTED_KEYS
        for v in result.values():
            assert isinstance(v, str)

    def test_better_enrich_keys(self):
        result = rn.normalize_better_enrich_contact({"email": "a@b.com"})
        assert set(result.keys()) == self.EXPECTED_KEYS
        for v in result.values():
            assert isinstance(v, str)

    def test_wizleads_keys(self):
        result = rn.normalize_wizleads_contact({"email": "a@b.com"})
        assert set(result.keys()) == self.EXPECTED_KEYS
        for v in result.values():
            assert isinstance(v, str)

    def test_getleads_keys(self):
        result = rn.normalize_getleads_contact({"email": "a@b.com"})
        assert result is not None
        assert set(result.keys()) == self.EXPECTED_KEYS
        for v in result.values():
            assert isinstance(v, str)


# ---------------------------------------------------------------------------
# Placeholder sets are immutable frozensets
# ---------------------------------------------------------------------------

class TestPlaceholderSets:
    def test_email_is_frozenset(self):
        assert isinstance(rn.EMAIL_PLACEHOLDERS, frozenset)

    def test_name_is_frozenset(self):
        assert isinstance(rn.NAME_PLACEHOLDERS, frozenset)

    def test_linkedin_is_frozenset(self):
        assert isinstance(rn.LINKEDIN_PLACEHOLDERS, frozenset)

    def test_domain_is_frozenset(self):
        assert isinstance(rn.DOMAIN_PLACEHOLDERS, frozenset)

    def test_empty_string_in_all_sets(self):
        # The empty string must be a member of every placeholder set
        # so the normalizers treat "" as missing uniformly.
        assert "" in rn.EMAIL_PLACEHOLDERS
        assert "" in rn.NAME_PLACEHOLDERS
        assert "" in rn.LINKEDIN_PLACEHOLDERS
        assert "" in rn.DOMAIN_PLACEHOLDERS
