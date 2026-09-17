"""Tests for the TAM flow API surface (enrichment/tam_routes.py) and the
exact_titles / include_phone / phone_for_all request-field plumbing added to
enrichment/routes.py.

Coverage:
  * POST /api/enrichment/flows/tam
      - 401 without credentials; 200 with the test-auth dependency override.
      - Creates a job_type='enrichment' row with source_type='tam_flow' and
        runs the background job to completion (TestClient awaits background
        tasks), leaving status='done' + a downloadable CSV.
      - Filter whitelisting: unknown company/people filter key -> 422.
      - Invalid job_level value -> 422.
      - Zero filters -> 400 (unbounded TAM pull refused).
      - Invalid provider name -> 400.
  * Request-model defaults: the new fields on UnifiedEnrichRequest /
      ProviderToggleRequest / LinkedInV2Request / TamRequest all default off.
  * routes._forward_compat_kwargs: drops unknown kwargs, forwards known,
      passes everything to **kwargs callees.
"""

from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

os.environ.setdefault("CONTACTS_API_TOKEN", "test-token-from-suite")

from fastapi.testclient import TestClient  # noqa: E402

from shared import db as shared_db  # noqa: E402
from shared import auth as shared_auth  # noqa: E402

from enrichment import job_store  # noqa: E402
from enrichment import routes as routes_mod  # noqa: E402
from enrichment import tam_flow  # noqa: E402
from enrichment import tam_routes  # noqa: E402
from main import app  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_user() -> dict:
    return {"user_id": "tam-test-user", "email": "tam@example.com", "is_admin": True}


def _override_auth() -> dict:
    return {
        shared_auth.get_current_user_with_api_key: lambda: _make_user(),
        shared_auth.get_current_user: lambda: _make_user(),
    }


def _tam_entry(index: int) -> dict:
    return {
        "company": {
            "name": f"TAM Co {index}",
            "domain": f"tamco{index}.example",
            "linkedin_url": f"https://linkedin.com/company/tamco-{index}",
            "industry": "Software Development",
            "hq": {"city": "Berlin", "country_code": "DE", "region": "Berlin"},
            "followers": 100 + index,
        },
        "matched_people": 2,
    }


async def _one_page_tam(_http, *, company_filters, people_filters,
                        max_results, cursor):
    """Single page, cursor exhausted."""
    return {"results": [_tam_entry(0), _tam_entry(1)], "cursor": None}


class _TamRouteTestCase(unittest.TestCase):
    """Temp jobs DB + temp TAM output dir per test (no prod writes)."""

    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self._orig_db_path = shared_db.DB_PATH
        shared_db.DB_PATH = Path(self._tmpdir.name) / "jobs_test.db"
        shared_db._local.conn = None
        shared_db.init_db()
        self._ensure_restart_support_columns()
        # jobs.user_id has an FK into users (get_db sets PRAGMA foreign_keys=ON),
        # so the auth tables must exist on the temp DB too. NOTE: shared.auth
        # keeps its OWN DB_PATH module attr + thread-local conn — patch both.
        self._orig_auth_db_path = shared_auth.DB_PATH
        shared_auth.DB_PATH = shared_db.DB_PATH
        shared_auth._local.conn = None
        shared_auth.init_auth_db()
        # FK target row (jobs.user_id -> users.user_id is enforced by
        # PRAGMA foreign_keys=ON in get_db()).
        conn = shared_db.get_db()
        conn.execute(
            "INSERT OR IGNORE INTO users (user_id, email, password_hash,"
            " is_admin, created_at) VALUES (?, ?, ?, 1, datetime('now'))",
            ("tam-test-user", "tam-route@example.com", "x" * 8),
        )
        conn.commit()
        self._orig_output_dir = tam_flow.OUTPUT_DIR
        tam_flow.OUTPUT_DIR = Path(self._tmpdir.name) / "outputs"
        app.dependency_overrides.update(_override_auth())
        self._client = TestClient(app)

    @staticmethod
    def _ensure_restart_support_columns() -> None:
        """A fresh init_db() lacks columns prod gained from one-time
        migrations / manual ALTERs: add_restart_support.py (name_col /
        first_name_col / last_name_col / cascade_config / max_results) and
        hidden_from_ui (queried by list_jobs, created outside init_db).
        Add them idempotently so job creation + listing work on the temp DB."""
        conn = shared_db.get_db()
        existing = {
            row[1] for row in conn.execute("PRAGMA table_info(jobs)").fetchall()
        }
        for column, decl in (
            ("name_col", "TEXT"),
            ("first_name_col", "TEXT"),
            ("last_name_col", "TEXT"),
            ("cascade_config", "TEXT"),
            ("max_results", "INTEGER DEFAULT 5"),
            ("hidden_from_ui", "INTEGER DEFAULT 0"),
        ):
            if column not in existing:
                conn.execute(f"ALTER TABLE jobs ADD COLUMN {column} {decl}")
        conn.commit()

    def tearDown(self):
        app.dependency_overrides.clear()
        conn = getattr(shared_db._local, "conn", None)
        if conn is not None:
            conn.close()
            shared_db._local.conn = None
        auth_conn = getattr(shared_auth._local, "conn", None)
        if auth_conn is not None:
            auth_conn.close()
            shared_auth._local.conn = None
        shared_auth.DB_PATH = self._orig_auth_db_path
        shared_db.DB_PATH = self._orig_db_path
        tam_flow.OUTPUT_DIR = self._orig_output_dir
        self._tmpdir.cleanup()


# ---------------------------------------------------------------------------
# Route tests
# ---------------------------------------------------------------------------

class TestTamRoute(_TamRouteTestCase):

    def test_unauthenticated_request_is_401(self):
        app.dependency_overrides.clear()
        with TestClient(app) as client:
            resp = client.post("/api/enrichment/flows/tam", json={
                "company": {"employee_range": ["11-50"]},
                "people": {"job_title_include": ["CEO"]},
            })
        self.assertEqual(resp.status_code, 401)

    def test_authenticated_request_runs_job_to_done(self):
        with patch("enrichment.blitz_client.tam_by_people", new=_one_page_tam):
            resp = self._client.post("/api/enrichment/flows/tam", json={
                "company": {"employee_range": ["11-50"]},
                "people": {"job_title_include": ["CEO"], "job_levels": ["C-Team"]},
                "max_companies": 10,
            })

        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertIn("job_id", body)
        self.assertEqual(body["flow"], "tam")
        job_id = body["job_id"]

        # TestClient awaits background tasks, so the job has finished.
        job = job_store.get_store().get_job(job_id)
        self.assertIsNotNone(job)
        self.assertEqual(job["job_type"], "enrichment", "never a new job_type")
        self.assertEqual(job["source_type"], "tam_flow")
        self.assertEqual(job["status"], "done")
        self.assertEqual(job["result_count"], 2)
        csv_path = Path(job["output_path"])
        self.assertTrue(csv_path.exists())
        self.assertIn("TAM Co 0", csv_path.read_text(encoding="utf-8"))


    def test_lookalike_analyze_mode_returns_profile_no_job(self):
        async def fake_gl(client, *, domains, limit=1, **kw):
            return {"ok": True, "contacts": [{
                "org_company_name": "Acme", "org_domain": domains[0],
                "org_industry_linkedin": "Software Development",
                "employee_count_range": "11 to 50",
            }]}

        async def miss_d2l(client, domain):
            return {"found": False, "company_linkedin_url": None}

        with patch("enrichment.getleads_client.search_contacts_companies",
                   new=fake_gl), \
             patch("enrichment.blitz_client.domain_to_linkedin", new=miss_d2l):
            resp = self._client.post("/api/enrichment/flows/tam", json={
                "seed_companies": ["acme.com", "acme2.com"],
                "analyze_seeds": True,
            })
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertTrue(body["ok"])
        self.assertEqual(len(body["seeds"]), 2)
        self.assertEqual(body["profile"]["industries"], ["Software Development"])
        self.assertEqual(body["profile"]["size_band"], "11-50")

    def test_lookalike_run_merges_filters_and_excludes_seeds(self):
        captured = {}

        async def fake_tam(_http, *, company_filters, people_filters,
                           max_results, cursor):
            captured["company"] = company_filters
            return {"results": [
                {"company": {"name": "Lookalike Co", "domain": "lookalike.com",
                             "linkedin_url": "https://linkedin.com/company/lk",
                             "industry": "Software Development", "size": "11-50"},
                 "matched_people": 1},
                {"company": {"name": "Seed Echo", "domain": "acme.com",
                             "linkedin_url": "https://linkedin.com/company/acme",
                             "industry": "Software Development", "size": "11-50"},
                 "matched_people": 1},
            ], "cursor": None}

        async def fake_gl(client, *, domains, limit=1, **kw):
            return {"ok": True, "contacts": [{
                "org_company_name": "Acme", "org_domain": domains[0],
                "org_industry_linkedin": "Software Development",
                "employee_count_range": "11 to 50",
            }]}

        async def miss_d2l(client, domain):
            return {"found": False, "company_linkedin_url": None}

        with patch("enrichment.blitz_client.tam_by_people", new=fake_tam), \
             patch("enrichment.getleads_client.search_contacts_companies",
                   new=fake_gl), \
             patch("enrichment.blitz_client.domain_to_linkedin", new=miss_d2l):
            resp = self._client.post("/api/enrichment/flows/tam", json={
                "seed_companies": ["acme.com"],
                "people": {"job_title_include": ["CEO"]},
                "max_companies": 10,
            })
        self.assertEqual(resp.status_code, 200)
        job_id = resp.json()["job_id"]
        job = job_store.get_store().get_job(job_id)
        self.assertEqual(job["status"], "done")
        self.assertEqual(captured["company"].get("industry"),
                         {"include": ["Software Development"]})
        self.assertEqual(captured["company"].get("employee_range"), ["11-50"])
        csv_text = Path(job["output_path"]).read_text(encoding="utf-8")
        self.assertIn("Lookalike Co", csv_text)
        self.assertNotIn("Seed Echo", csv_text)
        self.assertIn("Lookalikes", job["display_name"])
        self.assertIn("source", csv_text.splitlines()[0])

    def test_lookalike_getleads_only_source_runs_gl_leg(self):
        # ONE fake serves both call sites (profile-by-domain + the pull leg)
        # — patching the module attribute covers lookalike's reference too.
        async def fake_gl(client, *, domains=None, limit=1, offset=0, **kw):
            if domains:
                return {"ok": True, "contacts": [{
                    "org_company_name": "Acme", "org_domain": domains[0],
                    "org_industry_linkedin": "Software Development",
                    "employee_count_range": "11 to 50",
                }]}
            return {"ok": True, "contacts": [
                {"org_company_name": "GL Co", "org_domain": "glco.com",
                 "org_industry_linkedin": "Software Development",
                 "employee_count_range": "11 to 50"},
            ], "has_more": False, "next_offset": 1, "query_credits_used": 1}

        async def fake_tam(*a, **kw):
            raise AssertionError("blitz leg must not run when getleads-only")

        with patch("enrichment.blitz_client.tam_by_people", new=fake_tam), \
             patch("enrichment.getleads_client.search_contacts_companies",
                   new=fake_gl):
            resp = self._client.post("/api/enrichment/flows/tam", json={
                "seed_companies": ["acme.com"],
                "sources": ["getleads"],
                "max_companies": 10,
            })
        self.assertEqual(resp.status_code, 200)
        job = job_store.get_store().get_job(resp.json()["job_id"])
        self.assertEqual(job["status"], "done")
        csv_text = Path(job["output_path"]).read_text(encoding="utf-8")
        self.assertIn("GL Co", csv_text)
        self.assertIn("getleads", csv_text)

    def test_unknown_company_filter_key_is_422(self):
        resp = self._client.post("/api/enrichment/flows/tam", json={
            "company": {"bogus_filter": ["x"], "employee_range": ["11-50"]},
            "people": {"job_title_include": ["CEO"]},
        })
        self.assertEqual(resp.status_code, 422)
        self.assertIn("bogus_filter", resp.text)

    def test_unknown_people_filter_key_is_422(self):
        resp = self._client.post("/api/enrichment/flows/tam", json={
            "company": {"employee_range": ["11-50"]},
            "people": {"also_bogus": 1},
        })
        self.assertEqual(resp.status_code, 422)
        self.assertIn("also_bogus", resp.text)

    def test_invalid_job_level_is_422(self):
        resp = self._client.post("/api/enrichment/flows/tam", json={
            "company": {"employee_range": ["11-50"]},
            "people": {"job_levels": ["Chief Executive"]},
        })
        self.assertEqual(resp.status_code, 422)
        self.assertIn("job_levels", resp.text)

    def test_zero_filters_is_400(self):
        resp = self._client.post("/api/enrichment/flows/tam", json={})
        self.assertEqual(resp.status_code, 400)
        self.assertIn("filter", resp.json()["detail"].lower())

    def test_invalid_provider_is_400(self):
        resp = self._client.post("/api/enrichment/flows/tam", json={
            "company": {"employee_range": ["11-50"]},
            "people": {"job_title_include": ["CEO"]},
            "providers": ["not_a_provider"],
        })
        self.assertEqual(resp.status_code, 400)
        self.assertIn("not_a_provider", resp.json()["detail"])

    def test_job_visible_via_generic_enrichment_jobs_endpoint(self):
        with patch("enrichment.blitz_client.tam_by_people", new=_one_page_tam):
            created = self._client.post("/api/enrichment/flows/tam", json={
                "company": {"employee_range": ["11-50"]},
                "people": {"job_title_include": ["CEO"]},
            }).json()
        listed = self._client.get(
            "/api/enrichment/jobs",
            params={"search": created["job_id"]},
        )
        self.assertEqual(listed.status_code, 200)
        job_ids = [j.get("job_id") for j in listed.json().get("jobs", [])]
        self.assertIn(created["job_id"], job_ids)


# ---------------------------------------------------------------------------
# Filter payload whitelisting (unit)
# ---------------------------------------------------------------------------

class TestFilterPayloads(unittest.TestCase):

    def test_company_payload_only_emits_set_fields(self):
        filters = tam_routes.TamCompanyFilters(
            employee_range=["11-50"],
            hq_country_code=["DE", "AT"],
            min_linkedin_followers=500,
        )
        self.assertEqual(
            filters.to_payload(),
            {
                "employee_range": ["11-50"],
                "hq": {"country_code": ["DE", "AT"]},
                "min_linkedin_followers": 500,
            },
        )

    def test_company_payload_keyword_and_range_groups(self):
        filters = tam_routes.TamCompanyFilters(
            name_include=["dental", "ortho"],
            name_exclude=["lab"],
            revenue_min=1000,
            founded_year_max=2020,
        )
        payload = filters.to_payload()
        self.assertEqual(payload["name"], {"include": ["dental", "ortho"], "exclude": ["lab"]})
        self.assertEqual(payload["revenue"], {"min": 1000})
        self.assertEqual(payload["founded_year"], {"max": 2020})

    def test_people_payload_shape(self):
        filters = tam_routes.TamPeopleFilters(
            job_title_include=["CEO"],
            job_levels=["C-Team", "VP"],
            min_per_company=2,
            location_country_code=["US"],
        )
        self.assertEqual(
            filters.to_payload(),
            {
                "job_title": {"include": ["CEO"]},
                "job_level": ["C-Team", "VP"],
                "min_per_company": 2,
                "location": {"country_code": ["US"]},
            },
        )

    def test_empty_filters_emit_empty_payloads(self):
        self.assertEqual(tam_routes.TamCompanyFilters().to_payload(), {})
        self.assertEqual(tam_routes.TamPeopleFilters().to_payload(), {})


# ---------------------------------------------------------------------------
# Request-model defaults (new fields default off)
# ---------------------------------------------------------------------------

class TestNewRequestFieldDefaults(unittest.TestCase):

    def test_unified_enrich_request_defaults(self):
        req = routes_mod.UnifiedEnrichRequest(domain="example.com")
        self.assertFalse(req.exact_titles)
        self.assertFalse(req.include_phone)
        self.assertFalse(req.phone_for_all)

    def test_provider_toggle_request_defaults(self):
        req = routes_mod.ProviderToggleRequest(upload_id="u", domain_col="domain")
        self.assertFalse(req.exact_titles)
        self.assertFalse(req.include_phone)
        self.assertFalse(req.phone_for_all)

    def test_linkedin_v2_request_defaults(self):
        req = routes_mod.LinkedInV2Request(upload_id="u")
        self.assertFalse(req.include_phone)

    def test_tam_request_defaults(self):
        req = tam_routes.TamRequest()
        self.assertFalse(req.exact_titles)
        self.assertFalse(req.create_enrichment_job)
        self.assertEqual(req.max_companies, 1000)
        self.assertEqual(req.max_decision_makers, 5)
        self.assertIsNone(req.providers)
        self.assertEqual(req.company.to_payload(), {})
        self.assertEqual(req.people.to_payload(), {})


# ---------------------------------------------------------------------------
# Forward-compat kwarg helper
# ---------------------------------------------------------------------------

class TestForwardCompatKwargs(unittest.TestCase):

    def test_drops_kwargs_the_callee_lacks(self):
        def target(alpha: int) -> None:
            pass

        forwarded = routes_mod._forward_compat_kwargs(
            target, exact_titles=True, include_phone=False, phone_for_all=True
        )
        self.assertEqual(forwarded, {})

    def test_forwards_known_kwargs_only(self):
        def target(alpha: int, include_phone: bool = False) -> None:
            pass

        forwarded = routes_mod._forward_compat_kwargs(
            target, exact_titles=True, include_phone=True
        )
        self.assertEqual(forwarded, {"include_phone": True})

    def test_var_keyword_callee_gets_everything(self):
        def target(alpha: int, **kwargs) -> None:
            pass

        forwarded = routes_mod._forward_compat_kwargs(
            target, exact_titles=False, include_phone=True
        )
        self.assertEqual(
            forwarded, {"exact_titles": False, "include_phone": True}
        )

    def test_post_merge_pipeline_call_still_works(self):
        """Simulate the parallel wave's signature: once run_domain_enrichment
        grows the params, the Flow-1 call site forwards them unchanged."""
        sentinel = object()

        def merged(rows, domain_col, exact_titles=False, include_phone=False,
                   phone_for_all=False):
            return exact_titles, include_phone, phone_for_all

        forwarded = routes_mod._forward_compat_kwargs(
            merged, exact_titles=sentinel, include_phone=True, phone_for_all=False
        )
        self.assertEqual(
            forwarded,
            {"exact_titles": sentinel, "include_phone": True, "phone_for_all": False},
        )


if __name__ == "__main__":
    unittest.main()
