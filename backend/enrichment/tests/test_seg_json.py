"""Wave 2b — SEG classification surfaced in JSON API responses.

Pins the contract for the four response sites wired in routes.py:

  1. POST /api/enrichment/enrich           (top-level seg_classification/seg_provider)
  2. GET  /api/enrichment/enrich           (top-level, both _unified_enrich_logic returns)
  3. GET  /api/enrichment/enrich/{domain}  (top-level, normalized path-param domain)
  4. POST /api/enrichment/search/employees (PER-PERSON seg_classification/seg_provider)

Contract decisions under test:
  - Flag OFF  -> the two keys are ABSENT from every response (byte-for-byte
    JSON Response Freeze parity for external consumers while dark).
  - Flag ON   -> keys present at TOP LEVEL ("" when the domain is
    unclassifiable), and NEVER inside contacts[*] on sites 1-3 (the
    _strip_internal_fields_from_response freeze contract).
  - /search/employees hydrates the keys ON each person dict (this endpoint
    does NOT run the strip filter, so per-person keys reach the frontend
    verbatim); the envelope and pagination fields stay identical.
  - seg.classify_domains NEVER blocks the response: even a raising mock
    degrades to blank seg fields with HTTP 200.

All network is hermetic: pipeline/providers/seg are monkeypatched, no
provider HTTP and no DNS. The seg cache table is never touched (classify is
mocked at the routes boundary).

Run:
    python -m pytest enrichment/tests/test_seg_json.py -v
"""

from __future__ import annotations

import os
import sys
import unittest
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

# Make backend root importable so `enrichment` resolves regardless of cwd.
_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

os.environ.setdefault("CONTACTS_API_TOKEN", "test-token-from-suite")

from fastapi.testclient import TestClient  # noqa: E402

from enrichment import contacts_client as cc  # noqa: E402
from enrichment import contacts_writer  # noqa: E402
from enrichment import pipeline as pipeline_mod  # noqa: E402
from enrichment import routes as routes_mod  # noqa: E402
from enrichment import seg  # noqa: E402
from main import app  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

SEG_KEYS: frozenset[str] = frozenset({"seg_classification", "seg_provider"})

_SEG_MAP_MIMECAST: dict[str, dict] = {
    "stripe.com": {
        "seg_classification": "external_seg",
        "seg_provider": "SEG: Mimecast",
        "source": "doh",
    }
}


def _make_user() -> dict:
    return {
        "user_id": "test-user-1",
        "email": "test@example.com",
        "is_admin": True,
    }


def _override_auth():
    """Dependency overrides covering both auth flavors used by the endpoints.

    sites 1-3 use get_current_user_with_api_key; /search/employees uses
    get_current_user.
    """
    from shared import auth as _auth
    return {
        _auth.get_current_user_with_api_key: lambda: _make_user(),
        _auth.get_current_user: lambda: _make_user(),
    }


def _patch_flag(on: bool):
    """Patch seg.is_seg_enabled as seen by routes_mod (it holds its own ref)."""
    return patch.object(routes_mod.seg, "is_seg_enabled", MagicMock(return_value=on))


def _patch_classify(map_to_return: dict[str, dict]):
    """Patch seg.classify_domains as seen by routes_mod."""
    return patch.object(
        routes_mod.seg, "classify_domains", AsyncMock(return_value=map_to_return)
    )


def _patch_classify_raising():
    async def _boom(_domains):
        raise RuntimeError("classify exploded")

    return patch.object(routes_mod.seg, "classify_domains", _boom)


def _clear_enrich_response_cache():
    """The GET /enrich response cache (300s TTL, module-level dict) survives
    across tests — a later test with the same cache key would receive the
    first test's cached body and assert against the wrong seg fields."""
    routes_mod._enrich_response_cache.clear()


def _patch_v2_off():
    """Legacy sync path is never taken (contacts_writer v2 off + no contacts)."""
    return patch.object(
        contacts_writer, "is_v2_enabled", MagicMock(return_value=False)
    )


def _patch_post_cascade(contact_email: str = "aaron@stripe.com"):
    """Patch pipeline._enrich_domain so POST /enrich domain_only returns one
    contact deterministically (same surface the freeze tests use)."""
    async def fake_enrich_domain(*_args, **_kwargs):
        return [{
            "domain": "stripe.com",
            "row_status": pipeline_mod.STATUS_ENRICHED,
            "dm_full_name": "Aaron Harris",
            "dm_first_name": "Aaron",
            "dm_last_name": "Harris",
            "dm_title": "CEO",
            "dm_email": contact_email,
            "dm_linkedin_url": "aaronh",
            "company_linkedin_url": "https://li/company/stripe",
            "dm_email_source": "contacts_db_email",
            "dm_email_verified": "yes",
            "mailtester_code": "",
            "mailtester_message": "",
        }]

    return patch.object(routes_mod.pipeline, "_enrich_domain", fake_enrich_domain)


# ---------------------------------------------------------------------------
# Site 1 — POST /api/enrichment/enrich
# ---------------------------------------------------------------------------


class TestPostEnrichSeg(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)
        app.dependency_overrides.update(_override_auth())

    def tearDown(self):
        app.dependency_overrides.clear()

    def _post(self, domain: str = "stripe.com"):
        return self.client.post(
            "/api/enrichment/enrich",
            json={"domain": domain, "max_results": 1},
        )

    def test_flag_on_top_level_seg_fields_present(self):
        with _patch_v2_off(), _patch_flag(True), \
             _patch_classify(_SEG_MAP_MIMECAST), _patch_post_cascade():
            resp = self._post()
        self.assertEqual(resp.status_code, 200, resp.text)
        body = resp.json()
        self.assertEqual(body.get("seg_classification"), "external_seg")
        self.assertEqual(body.get("seg_provider"), "SEG: Mimecast")

    def test_flag_on_contacts_do_not_carry_seg_keys(self):
        with _patch_v2_off(), _patch_flag(True), \
             _patch_classify(_SEG_MAP_MIMECAST), _patch_post_cascade():
            resp = self._post()
        self.assertEqual(resp.status_code, 200, resp.text)
        for contact in resp.json().get("contacts", []):
            for key in SEG_KEYS:
                self.assertNotIn(key, contact, f"{key} leaked into contacts[*]")

    def test_flag_off_keys_absent(self):
        with _patch_v2_off(), _patch_flag(False), \
             _patch_classify(_SEG_MAP_MIMECAST), _patch_post_cascade():
            resp = self._post()
        self.assertEqual(resp.status_code, 200, resp.text)
        body = resp.json()
        for key in SEG_KEYS:
            self.assertNotIn(key, body, f"{key} must be absent when flag off")

    def test_flag_on_unclassifiable_domain_blank_values(self):
        with _patch_v2_off(), _patch_flag(True), \
             _patch_classify({}), _patch_post_cascade():
            resp = self._post()
        self.assertEqual(resp.status_code, 200, resp.text)
        body = resp.json()
        self.assertEqual(body.get("seg_classification"), "")
        self.assertEqual(body.get("seg_provider"), "")

    def test_classify_raising_still_200_with_blank_seg(self):
        with _patch_v2_off(), _patch_flag(True), \
             _patch_classify_raising(), _patch_post_cascade():
            resp = self._post()
        self.assertEqual(resp.status_code, 200, resp.text)
        body = resp.json()
        self.assertEqual(body.get("seg_classification"), "")
        self.assertEqual(body.get("seg_provider"), "")


# ---------------------------------------------------------------------------
# Site 2 — GET /api/enrichment/enrich
# ---------------------------------------------------------------------------


class TestGetEnrichSeg(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)
        app.dependency_overrides.update(_override_auth())
        _clear_enrich_response_cache()

    def tearDown(self):
        app.dependency_overrides.clear()
        _clear_enrich_response_cache()

    def _get(self, domain: str = "stripe.com"):
        return self.client.get(
            "/api/enrichment/enrich",
            params={"domain": domain, "max_results": 1},
        )

    def test_flag_on_top_level_seg_fields_present(self):
        # domain_only GET path: stub contacts + blitz so the endpoint returns
        # 200 without network (same surface the freeze tests use).
        with _patch_v2_off(), _patch_flag(True), \
             _patch_classify(_SEG_MAP_MIMECAST), \
             patch.object(cc, "company_by_domain",
                          AsyncMock(return_value={"linkedin_url": "https://li/company/stripe"})), \
             patch.object(cc, "company_contacts_enriched", AsyncMock(return_value=[])), \
             patch.object(routes_mod.pipeline, "run_enrichment_route",
                          AsyncMock(return_value={"email": "", "source": "not_found",
                                                  "mode": "domain_person",
                                                  "source_path": "", "steps": []})):
            resp = self._get()
        self.assertEqual(resp.status_code, 200, resp.text)
        body = resp.json()
        self.assertEqual(body.get("seg_classification"), "external_seg")
        self.assertEqual(body.get("seg_provider"), "SEG: Mimecast")

    def test_flag_on_contacts_do_not_carry_seg_keys(self):
        with _patch_v2_off(), _patch_flag(True), \
             _patch_classify(_SEG_MAP_MIMECAST), \
             patch.object(cc, "company_by_domain",
                          AsyncMock(return_value={"linkedin_url": "https://li/company/stripe"})), \
             patch.object(cc, "company_contacts_enriched", AsyncMock(return_value=[])), \
             patch.object(routes_mod.pipeline, "run_enrichment_route",
                          AsyncMock(return_value={"email": "", "source": "not_found",
                                                  "mode": "domain_person",
                                                  "source_path": "", "steps": []})):
            resp = self._get()
        self.assertEqual(resp.status_code, 200, resp.text)
        for contact in resp.json().get("contacts", []):
            for key in SEG_KEYS:
                self.assertNotIn(key, contact)

    def test_flag_off_keys_absent(self):
        with _patch_v2_off(), _patch_flag(False), \
             _patch_classify(_SEG_MAP_MIMECAST), \
             patch.object(cc, "company_by_domain",
                          AsyncMock(return_value={"linkedin_url": "https://li/company/stripe"})), \
             patch.object(cc, "company_contacts_enriched", AsyncMock(return_value=[])), \
             patch.object(routes_mod.pipeline, "run_enrichment_route",
                          AsyncMock(return_value={"email": "", "source": "not_found",
                                                  "mode": "domain_person",
                                                  "source_path": "", "steps": []})):
            resp = self._get()
        self.assertEqual(resp.status_code, 200, resp.text)
        body = resp.json()
        for key in SEG_KEYS:
            self.assertNotIn(key, body)

    def test_classify_raising_still_200_with_blank_seg(self):
        with _patch_v2_off(), _patch_flag(True), \
             _patch_classify_raising(), \
             patch.object(cc, "company_by_domain",
                          AsyncMock(return_value={"linkedin_url": "https://li/company/stripe"})), \
             patch.object(cc, "company_contacts_enriched", AsyncMock(return_value=[])), \
             patch.object(routes_mod.pipeline, "run_enrichment_route",
                          AsyncMock(return_value={"email": "", "source": "not_found",
                                                  "mode": "domain_person",
                                                  "source_path": "", "steps": []})):
            resp = self._get()
        self.assertEqual(resp.status_code, 200, resp.text)
        body = resp.json()
        self.assertEqual(body.get("seg_classification"), "")
        self.assertEqual(body.get("seg_provider"), "")

    def test_linkedin_only_no_domain_keys_absent_even_flag_on(self):
        """linkedin_only has no domain to classify — seg keys stay absent."""
        with _patch_v2_off(), _patch_flag(True), _patch_classify(_SEG_MAP_MIMECAST), \
             patch.object(cc, "person_by_linkedin", AsyncMock(return_value=None)), \
             patch.object(routes_mod.pipeline, "route_enrichment",
                          MagicMock(return_value={"mode": "linkedin", "steps": []})), \
             patch.object(routes_mod.pipeline, "run_enrichment_route",
                          AsyncMock(return_value={"email": "", "source": "not_found",
                                                  "mode": "linkedin", "source_path": "",
                                                  "steps": []})):
            resp = self.client.get(
                "/api/enrichment/enrich",
                params={"linkedin_url": "https://www.linkedin.com/in/aaronh",
                        "max_results": 1},
            )
        self.assertEqual(resp.status_code, 200, resp.text)
        body = resp.json()
        for key in SEG_KEYS:
            self.assertNotIn(key, body)


# ---------------------------------------------------------------------------
# Site 3 — GET /api/enrichment/enrich/{domain}
# ---------------------------------------------------------------------------


class TestGetEnrichByDomainSeg(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)
        app.dependency_overrides.update(_override_auth())

    def tearDown(self):
        app.dependency_overrides.clear()

    def _get(self, path_domain: str = "stripe.com"):
        return self.client.get(f"/api/enrichment/enrich/{path_domain}")

    def test_flag_on_top_level_seg_fields_present(self):
        with _patch_v2_off(), _patch_flag(True), \
             _patch_classify(_SEG_MAP_MIMECAST), _patch_post_cascade():
            resp = self._get()
        self.assertEqual(resp.status_code, 200, resp.text)
        body = resp.json()
        self.assertEqual(body.get("seg_classification"), "external_seg")
        self.assertEqual(body.get("seg_provider"), "SEG: Mimecast")

    def test_flag_on_contacts_do_not_carry_seg_keys(self):
        with _patch_v2_off(), _patch_flag(True), \
             _patch_classify(_SEG_MAP_MIMECAST), _patch_post_cascade():
            resp = self._get()
        self.assertEqual(resp.status_code, 200, resp.text)
        for contact in resp.json().get("contacts", []):
            for key in SEG_KEYS:
                self.assertNotIn(key, contact)

    def test_flag_off_keys_absent(self):
        with _patch_v2_off(), _patch_flag(False), \
             _patch_classify(_SEG_MAP_MIMECAST), _patch_post_cascade():
            resp = self._get()
        self.assertEqual(resp.status_code, 200, resp.text)
        body = resp.json()
        for key in SEG_KEYS:
            self.assertNotIn(key, body)

    def test_normalized_domain_passed_to_classifier(self):
        """The endpoint normalizes the path param before enriching; the
        classifier must receive the NORMALIZED domain so its result-map key
        matches. Path params cannot contain '/', so a 'www.'-prefixed host
        exercises the normalization."""
        captured: list[list[str]] = []

        async def fake_classify(domains):
            captured.append(list(domains))
            return _SEG_MAP_MIMECAST

        with _patch_v2_off(), _patch_flag(True), \
             patch.object(routes_mod.seg, "classify_domains", fake_classify), \
             _patch_post_cascade():
            resp = self._get("www.stripe.com")
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertEqual(captured, [["stripe.com"]])
        self.assertEqual(resp.json().get("seg_provider"), "SEG: Mimecast")

    def test_classify_raising_still_200_with_blank_seg(self):
        with _patch_v2_off(), _patch_flag(True), \
             _patch_classify_raising(), _patch_post_cascade():
            resp = self._get()
        self.assertEqual(resp.status_code, 200, resp.text)
        body = resp.json()
        self.assertEqual(body.get("seg_classification"), "")
        self.assertEqual(body.get("seg_provider"), "")


# ---------------------------------------------------------------------------
# Site 4 — POST /api/enrichment/search/employees
# ---------------------------------------------------------------------------


_PEOPLE_PAGE: list[dict[str, Any]] = [
    {
        "person_id": "p1",
        "full_name": "Aaron Harris",
        "title": "CEO",
        "company_name": "Stripe",
        "email": "aaron@stripe.com",
    },
    {
        "person_id": "p2",
        "full_name": "No Email Person",
        "title": "CTO",
        "company_name": "Acme",
        "email": None,
    },
    {
        "person_id": "p3",
        "full_name": "Unclassified Person",
        "title": "CFO",
        "company_name": "Unknown Co",
        "email": "cfo@unclassifiable.invalid",
    },
]


def _patch_search_people(people: list[dict[str, Any]], total: int = 3):
    return patch.object(
        cc,
        "search_people",
        AsyncMock(return_value={"total": total, "limit": 50, "offset": 0,
                                "people": people}),
    )


class TestSearchEmployeesSeg(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)
        app.dependency_overrides.update(_override_auth())

    def tearDown(self):
        app.dependency_overrides.clear()

    def _post(self, payload: dict | None = None):
        return self.client.post(
            "/api/enrichment/search/employees",
            json=payload or {},
        )

    def test_flag_on_people_hydrated_per_person(self):
        seg_map = {
            "stripe.com": {
                "seg_classification": "external_seg",
                "seg_provider": "SEG: Mimecast",
                "source": "doh",
            }
        }
        with _patch_flag(True), _patch_classify(seg_map), \
             _patch_search_people(_PEOPLE_PAGE):
            resp = self._post({"limit": 50, "offset": 0})
        self.assertEqual(resp.status_code, 200, resp.text)
        body = resp.json()
        people = body["people"]
        self.assertEqual(len(people), 3)
        # Classified domain (email-derived).
        self.assertEqual(people[0]["seg_classification"], "external_seg")
        self.assertEqual(people[0]["seg_provider"], "SEG: Mimecast")
        # No email -> no derivable domain -> blank fields, still present.
        self.assertEqual(people[1]["seg_classification"], "")
        self.assertEqual(people[1]["seg_provider"], "")
        # Email present but domain unclassifiable -> blank fields.
        self.assertEqual(people[2]["seg_classification"], "")
        self.assertEqual(people[2]["seg_provider"], "")
        # Original fields untouched.
        self.assertEqual(people[0]["full_name"], "Aaron Harris")
        self.assertEqual(people[0]["email"], "aaron@stripe.com")

    def test_flag_on_envelope_and_pagination_unchanged(self):
        seg_map = {"stripe.com": {"seg_classification": "external_seg",
                                  "seg_provider": "SEG: Mimecast",
                                  "source": "doh"}}
        with _patch_flag(True), _patch_classify(seg_map), \
             _patch_search_people(_PEOPLE_PAGE, total=1234):
            resp = self._post({"limit": 50, "offset": 100})
        self.assertEqual(resp.status_code, 200, resp.text)
        body = resp.json()
        self.assertEqual(body["total"], 1234)
        self.assertEqual(body["limit"], 50)
        self.assertEqual(body["offset"], 100)
        self.assertEqual(body["flow"], "people_search")
        self.assertEqual(
            set(body.keys()),
            {"total", "limit", "offset", "people", "flow"},
        )

    def test_flag_off_people_verbatim_no_seg_keys(self):
        with _patch_flag(False), _patch_classify(_SEG_MAP_MIMECAST), \
             _patch_search_people(_PEOPLE_PAGE):
            resp = self._post()
        self.assertEqual(resp.status_code, 200, resp.text)
        people = resp.json()["people"]
        self.assertEqual(people, _PEOPLE_PAGE)
        for person in people:
            for key in SEG_KEYS:
                self.assertNotIn(key, person)

    def test_single_batched_call_for_duplicate_domains(self):
        captured: list[list[str]] = []

        async def fake_classify(domains):
            captured.append(list(domains))
            return {"stripe.com": {"seg_classification": "direct_google",
                                   "seg_provider": "Google", "source": "doh"}}

        dupes = [
            {"full_name": f"P{i}", "email": f"p{i}@stripe.com"} for i in range(5)
        ]
        with _patch_flag(True), \
             patch.object(routes_mod.seg, "classify_domains", fake_classify), \
             _patch_search_people(dupes):
            resp = self._post()
        self.assertEqual(resp.status_code, 200, resp.text)
        # One call, deduped to the single unique domain.
        self.assertEqual(captured, [["stripe.com"]])
        people = resp.json()["people"]
        for person in people:
            self.assertEqual(person["seg_classification"], "direct_google")
            self.assertEqual(person["seg_provider"], "Google")

    def test_explicit_domain_field_preferred_over_email(self):
        """Forward-compat: if the contacts API ever returns a `domain`
        column, it wins over the email-derived one."""
        page = [{"full_name": "X", "email": "x@gmail.com", "domain": "stripe.com"}]
        seg_map = {"stripe.com": {"seg_classification": "external_seg",
                                  "seg_provider": "SEG: Mimecast", "source": "doh"}}
        with _patch_flag(True), _patch_classify(seg_map), \
             _patch_search_people(page):
            resp = self._post()
        self.assertEqual(resp.status_code, 200, resp.text)
        person = resp.json()["people"][0]
        self.assertEqual(person["seg_classification"], "external_seg")
        self.assertEqual(person["seg_provider"], "SEG: Mimecast")

    def test_classify_raising_still_200_without_seg_keys_on_people(self):
        """A raising classify must not fail the endpoint; people that had no
        verdict yet keep blank seg fields (uniform row shape)."""
        with _patch_flag(True), _patch_classify_raising(), \
             _patch_search_people(_PEOPLE_PAGE):
            resp = self._post()
        self.assertEqual(resp.status_code, 200, resp.text)
        people = resp.json()["people"]
        self.assertEqual(len(people), 3)
        for person in people:
            self.assertEqual(person.get("seg_classification"), "")
            self.assertEqual(person.get("seg_provider"), "")


# ---------------------------------------------------------------------------
# Helper-level unit tests (no TestClient)
# ---------------------------------------------------------------------------


class TestSegHelpers(unittest.TestCase):
    def test_seg_fields_for_domain_empty_domain_returns_empty(self):
        result = asyncio_run(routes_mod._seg_fields_for_domain(""))
        self.assertEqual(result, {})

    def test_seg_fields_for_domain_flag_off_returns_empty(self):
        with _patch_flag(False):
            result = asyncio_run(routes_mod._seg_fields_for_domain("stripe.com"))
        self.assertEqual(result, {})

    def test_hydrate_flag_off_returns_input_identity(self):
        people: list[dict[str, Any]] = [{"full_name": "A", "email": "a@x.com"}]
        with _patch_flag(False):
            out = asyncio_run(routes_mod._hydrate_people_with_seg(people))
        self.assertIs(out, people)

    def test_hydrate_empty_people_returns_empty(self):
        with _patch_flag(True):
            out = asyncio_run(routes_mod._hydrate_people_with_seg([]))
        self.assertEqual(out, [])

    def test_hydrate_does_not_mutate_input(self):
        people = [{"full_name": "A", "email": "a@stripe.com"}]
        with _patch_flag(True), _patch_classify(_SEG_MAP_MIMECAST):
            out = asyncio_run(routes_mod._hydrate_people_with_seg(people))
        self.assertNotIn("seg_classification", people[0])
        self.assertIn("seg_classification", out[0])

    def test_hydrate_cap_guard_skips_classify_over_500_domains(self):
        people = [{"full_name": f"P{i}", "email": f"p{i}@d{i}.com"} for i in range(501)]
        classify_mock = AsyncMock(return_value=_SEG_MAP_MIMECAST)
        with _patch_flag(True), \
             patch.object(routes_mod.seg, "classify_domains", classify_mock):
            out = asyncio_run(routes_mod._hydrate_people_with_seg(people))
        # classify never called past the cap; everyone gets blank fields.
        classify_mock.assert_not_awaited()
        self.assertEqual(out[0]["seg_classification"], "")


def asyncio_run(coro):
    import asyncio
    return asyncio.run(coro)


if __name__ == "__main__":
    unittest.main()
