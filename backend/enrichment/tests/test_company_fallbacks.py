"""
Acceptance tests for BetterEnrich company / page-level fallbacks.

These tests cover the goal contract in LOOP_BETTERENRICH_FALLBACKS.md:

  1. Person email success -> company/page fallbacks are NOT called.
  2. Person waterfall fails, Facebook URL exists -> company_email
     populated, final_email populated only if allow_company_email_as_final.
  3. Person waterfall fails, no Facebook, website exists -> company_email
     populated, company_email_type=generic.
  4. Generic company email rejected (allow_generic_company_email=False) ->
     final_email blank, no_email_reason=generic_company_email_rejected.
  5. Franchise duplicate domain -> find-company-email called once.
  6. Facebook duplicate page -> facebook endpoint called once.
  7. Rate limit -> V3 + Facebook + company together do not exceed 5 RPS.
  8. Output separation -> dm_email blank if only company/page email found.

All provider calls are mocked — no production credits are spent.
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from enrichment import pipeline as pipeline_mod  # noqa: E402
from enrichment import better_enrich_client as bec  # noqa: E402
from enrichment import company_fallback as cf  # noqa: E402
from enrichment import fallback_config as fb_cfg  # noqa: E402
from enrichment import identifier_utils as idu  # noqa: E402
from enrichment import mailtester_client as mt  # noqa: E402
from enrichment import blitz_client as bc  # noqa: E402
from enrichment import wizleads_client as wc  # noqa: E402
from enrichment import contacts_client as cc  # noqa: E402


def _enable_company_fallbacks(
    *,
    allow_generic: bool = False,
    allow_as_final: bool = False,
    enable_company: bool = True,
    enable_facebook: bool = True,
):
    """Return a contextmanager stack that patches the four flags."""
    from contextlib import ExitStack
    stack = ExitStack()
    stack.enter_context(patch.object(fb_cfg, "ENABLE_COMPANY_EMAIL_FALLBACK", enable_company, create=True))
    stack.enter_context(patch.object(fb_cfg, "ENABLE_FACEBOOK_EMAIL_FALLBACK", enable_facebook, create=True))
    stack.enter_context(patch.object(fb_cfg, "ALLOW_GENERIC_COMPANY_EMAIL", allow_generic, create=True))
    stack.enter_context(patch.object(fb_cfg, "ALLOW_COMPANY_EMAIL_AS_FINAL", allow_as_final, create=True))
    return stack


# ---------------------------------------------------------------------------
# Acceptance 1: Person email success. Company/page fallbacks MUST NOT run.
# ---------------------------------------------------------------------------


class TestAcceptance1PersonSuccessSkipsCompanyFallbacks(unittest.IsolatedAsyncioTestCase):
    """When a person-level cascade returns a verified email, the company
    and Facebook page fallback endpoints MUST NOT be called."""

    async def test_person_email_short_circuits_company_fallbacks(self):
        calls = {"company": 0, "facebook": 0}

        async def fake_cc_name_domain(http, full_name, domain):
            return None

        async def fake_blitz_person_enrich(http, full_name, domain, include_phone):
            return {"found": False, "person": {}}

        async def fake_wizleads(http, first_name, last_name, website):
            return {"email": "jane@acme.com", "catchall": True}

        async def fake_find_work_email_v3(http, full_name, company_domain, linkedin_url):
            # Should NOT be called because wizleads returned an email.
            return None

        async def fake_find_company_email(http, website):
            calls["company"] += 1
            return {"email": "contact@acme.com", "email_status": "verified"}

        async def fake_find_email_from_facebook_page(http, page_url):
            calls["facebook"] += 1
            return {"email": "page@acme.com", "email_status": "verified"}

        with _enable_company_fallbacks(), \
             patch.object(cc, "person_by_name_and_domain", fake_cc_name_domain), \
             patch.object(cc, "extract_email_from_contacts_response", MagicMock(return_value=None)), \
             patch.object(bc, "person_enrich", fake_blitz_person_enrich), \
             patch.object(bc, "find_work_email", AsyncMock(return_value={"found": False, "email": ""})), \
             patch.object(wc, "find_email", fake_wizleads), \
             patch.object(bec, "find_work_email_v3", fake_find_work_email_v3), \
             patch.object(bec, "find_company_email", fake_find_company_email), \
             patch.object(bec, "find_email_from_facebook_page", fake_find_email_from_facebook_page), \
             patch.object(mt, "verify_email", AsyncMock(return_value={"valid": True, "code": "ok", "message": "ok"})):
            route = pipeline_mod.route_enrichment(
                full_name="Jane Doe", first_name="Jane", last_name="Doe", domain="acme.com",
            )
            result = await pipeline_mod.run_enrichment_route(
                route, AsyncMock(), AsyncMock(), asyncio.Semaphore(1),
                validate_email=False, job_id="job_acc1", row_index=0, emit_logs=False,
                record_provider_use=lambda p: None,
            )

        # Person email is the WizLeads hit.
        self.assertEqual(result["email"], "jane@acme.com")
        self.assertEqual(result["source"], pipeline_mod.SOURCE_WIZLEADS)
        # Company + Facebook fallbacks were NOT called.
        self.assertEqual(calls["company"], 0)
        self.assertEqual(calls["facebook"], 0)


# ---------------------------------------------------------------------------
# Acceptance 2: Person fails, Facebook URL exists, company_email populated.
# ---------------------------------------------------------------------------


class TestAcceptance2PersonFailsFacebookPopulatesCompanyEmail(unittest.IsolatedAsyncioTestCase):
    """When the person-level cascade fails AND a Facebook URL is present,
    the company_email column is populated and final_email depends on the
    allow_company_email_as_final flag."""

    async def test_facebook_email_filled_and_final_email_gated(self):
        calls = {"facebook": 0, "company": 0}

        async def fake_find_email_from_facebook_page(http, page_url):
            calls["facebook"] += 1
            return {"email": "page@acme.com", "email_status": "verified"}

        async def fake_find_company_email(http, website):
            calls["company"] += 1
            return None

        with _enable_company_fallbacks(allow_as_final=False), \
             patch.object(bec, "find_email_from_facebook_page", fake_find_email_from_facebook_page), \
             patch.object(bec, "find_company_email", fake_find_company_email), \
             patch.object(mt, "verify_email", AsyncMock(return_value={"valid": True, "code": "ok", "message": "ok"})):
            fb_result = await cf.run_company_fallbacks(
                AsyncMock(),
                domain="acme.com",
                facebook_url="https://facebook.com/AcmePage",
                source_path_prefix="contacts_db -> blitz -> wizleads -> better_enrich",
                validate_email=False,
            )

        # Facebook endpoint called exactly once.
        self.assertEqual(calls["facebook"], 1)
        # Company endpoint NOT called (facebook already yielded).
        self.assertEqual(calls["company"], 0)
        # company_email populated.
        self.assertEqual(fb_result["company_email"], "page@acme.com")
        self.assertEqual(fb_result["company_email_source"], pipeline_mod.SOURCE_BETTER_ENRICH_FACEBOOK)
        self.assertEqual(fb_result["company_email_type"], "specific")
        self.assertEqual(fb_result["company_email_verified"], "yes")
        # final_email blank when allow_company_email_as_final=False.
        self.assertEqual(fb_result["final_email"], "")
        self.assertEqual(fb_result["final_email_level"], "")
        self.assertEqual(
            fb_result["no_email_reason"],
            pipeline_mod.NO_EMAIL_REASON_COMPANY_EMAIL_FOUND_BUT_NOT_ALLOWED,
        )

        # Now flip the flag and verify final_email is populated.
        with _enable_company_fallbacks(allow_as_final=True), \
             patch.object(bec, "find_email_from_facebook_page", fake_find_email_from_facebook_page), \
             patch.object(bec, "find_company_email", fake_find_company_email), \
             patch.object(mt, "verify_email", AsyncMock(return_value={"valid": True, "code": "ok", "message": "ok"})):
            fb_result2 = await cf.run_company_fallbacks(
                AsyncMock(),
                domain="acme.com",
                facebook_url="https://facebook.com/AcmePage",
                source_path_prefix="",
                validate_email=False,
            )
        self.assertEqual(fb_result2["final_email"], "page@acme.com")
        self.assertEqual(fb_result2["final_email_level"], "company")
        self.assertIn("better_enrich_facebook", fb_result2["final_email_source_path"])


# ---------------------------------------------------------------------------
# Acceptance 3: Person fails, no Facebook, website exists, generic email.
# ---------------------------------------------------------------------------


class TestAcceptance3PersonFailsWebsitePopulatesGenericCompanyEmail(unittest.IsolatedAsyncioTestCase):
    """When the person-level cascade fails, no Facebook URL exists, but a
    website does, the company endpoint is called and company_email_type
    is 'generic' for an info@/contact@/etc. email."""

    async def test_company_email_generic_classified(self):
        async def fake_find_company_email(http, website):
            return {"email": "contact@acme.com", "email_status": "verified"}

        with _enable_company_fallbacks(allow_generic=True), \
             patch.object(bec, "find_company_email", fake_find_company_email), \
             patch.object(mt, "verify_email", AsyncMock(return_value={"valid": True, "code": "ok", "message": "ok"})):
            fb_result = await cf.run_company_fallbacks(
                AsyncMock(),
                domain="acme.com",
                facebook_url="",
                source_path_prefix="",
                validate_email=False,
            )

        self.assertEqual(fb_result["company_email"], "contact@acme.com")
        self.assertEqual(fb_result["company_email_source"], pipeline_mod.SOURCE_BETTER_ENRICH_COMPANY_V2)
        self.assertEqual(fb_result["company_email_type"], "generic")
        self.assertEqual(fb_result["company_email_verified"], "yes")


# ---------------------------------------------------------------------------
# Acceptance 4: Generic company email rejected.
# ---------------------------------------------------------------------------


class TestAcceptance4GenericCompanyEmailRejected(unittest.IsolatedAsyncioTestCase):
    """When allow_generic_company_email=False, a generic company email
    MUST be rejected: final_email blank and no_email_reason=
    generic_company_email_rejected."""

    async def test_generic_email_rejected_when_flag_off(self):
        async def fake_find_company_email(http, website):
            return {"email": "info@acme.com", "email_status": "verified"}

        with _enable_company_fallbacks(allow_generic=False, allow_as_final=True), \
             patch.object(bec, "find_company_email", fake_find_company_email), \
             patch.object(mt, "verify_email", AsyncMock(return_value={"valid": True, "code": "ok", "message": "ok"})):
            fb_result = await cf.run_company_fallbacks(
                AsyncMock(),
                domain="acme.com",
                facebook_url="",
                source_path_prefix="",
                validate_email=False,
            )

        # final_email is blank.
        self.assertEqual(fb_result["final_email"], "")
        self.assertEqual(fb_result["final_email_level"], "")
        # Reason is set.
        self.assertEqual(
            fb_result["no_email_reason"],
            pipeline_mod.NO_EMAIL_REASON_GENERIC_COMPANY_EMAIL_REJECTED,
        )
        # company_email fields are cleared (per spec).
        self.assertEqual(fb_result["company_email"], "")

    async def test_generic_email_kept_when_flag_on(self):
        async def fake_find_company_email(http, website):
            return {"email": "info@acme.com", "email_status": "verified"}

        with _enable_company_fallbacks(allow_generic=True, allow_as_final=True), \
             patch.object(bec, "find_company_email", fake_find_company_email), \
             patch.object(mt, "verify_email", AsyncMock(return_value={"valid": True, "code": "ok", "message": "ok"})):
            fb_result = await cf.run_company_fallbacks(
                AsyncMock(),
                domain="acme.com",
                facebook_url="",
                source_path_prefix="",
                validate_email=False,
            )

        self.assertEqual(fb_result["company_email"], "info@acme.com")
        self.assertEqual(fb_result["company_email_type"], "generic")
        self.assertEqual(fb_result["final_email"], "info@acme.com")
        self.assertEqual(fb_result["final_email_level"], "company")
        self.assertNotEqual(
            fb_result["no_email_reason"],
            pipeline_mod.NO_EMAIL_REASON_GENERIC_COMPANY_EMAIL_REJECTED,
        )


# ---------------------------------------------------------------------------
# Acceptance 5: Franchise duplicate domain. find-company-email called once.
# ---------------------------------------------------------------------------


class TestAcceptance5FranchiseDuplicateDomain(unittest.IsolatedAsyncioTestCase):
    """When 100 rows share the same website, find-company-email MUST be
    called exactly once."""

    async def test_company_endpoint_called_once_for_duplicate_domain(self):
        calls = {"company": 0}

        async def fake_find_company_email(http, website):
            calls["company"] += 1
            await asyncio.sleep(0.01)  # simulate network
            return {"email": "info@franchise.com", "email_status": "verified"}

        with _enable_company_fallbacks(allow_generic=True), \
             patch.object(bec, "find_company_email", fake_find_company_email), \
             patch.object(mt, "verify_email", AsyncMock(return_value={"valid": True, "code": "ok", "message": "ok"})):
            dedupe = cf.CompanyFallbackDedupe()
            results = await asyncio.gather(*[
                cf.run_company_fallbacks(
                    AsyncMock(), domain="franchise.com", facebook_url="",
                    source_path_prefix="", validate_email=False, dedupe=dedupe,
                )
                for _ in range(100)
            ])

        self.assertEqual(calls["company"], 1)
        # All 100 rows got the same email.
        self.assertTrue(all(r["company_email"] == "info@franchise.com" for r in results))


# ---------------------------------------------------------------------------
# Acceptance 6: Facebook duplicate page. Called once.
# ---------------------------------------------------------------------------


class TestAcceptance6FacebookDuplicatePage(unittest.IsolatedAsyncioTestCase):
    """When 50 rows share the same Facebook page URL, the Facebook endpoint
    MUST be called exactly once."""

    async def test_facebook_endpoint_called_once_for_duplicate_page(self):
        calls = {"facebook": 0}

        async def fake_find_email_from_facebook_page(http, page_url):
            calls["facebook"] += 1
            await asyncio.sleep(0.01)
            return {"email": "page@brand.com", "email_status": "verified"}

        with _enable_company_fallbacks(), \
             patch.object(bec, "find_email_from_facebook_page", fake_find_email_from_facebook_page), \
             patch.object(mt, "verify_email", AsyncMock(return_value={"valid": True, "code": "ok", "message": "ok"})):
            dedupe = cf.CompanyFallbackDedupe()
            results = await asyncio.gather(*[
                cf.run_company_fallbacks(
                    AsyncMock(), domain="brand.com",
                    facebook_url="https://facebook.com/BrandPage",
                    source_path_prefix="", validate_email=False, dedupe=dedupe,
                )
                for _ in range(50)
            ])

        self.assertEqual(calls["facebook"], 1)
        self.assertTrue(all(r["company_email"] == "page@brand.com" for r in results))


# ---------------------------------------------------------------------------
# Acceptance 7: Rate limit. V3 + Facebook + Company together <= 5 RPS.
# ---------------------------------------------------------------------------


class TestAcceptance7SharedRateLimit5RPS(unittest.IsolatedAsyncioTestCase):
    """V3, Facebook, and company endpoints MUST share a single 5 RPS
    limiter. We fire concurrent calls and verify the elapsed time is
    consistent with at most 5 RPS."""

    def test_all_three_endpoints_use_shared_limiter(self):
        """All three BetterEnrich endpoints must call the shared 5 RPS
        limiter — not their own independent ones."""
        import inspect
        v3_src = inspect.getsource(bec.find_work_email_v3)
        company_src = inspect.getsource(bec.find_company_email)
        facebook_src = inspect.getsource(bec.find_email_from_facebook_page)

        for name, src in (
            ("find_work_email_v3", v3_src),
            ("find_company_email", company_src),
            ("find_email_from_facebook_page", facebook_src),
        ):
            self.assertIn(
                "_acquire_shared_rate_limit", src,
                f"{name} does not use the shared 5 RPS limiter",
            )
            # None should use the legacy per-endpoint limiter.
            self.assertNotIn(
                "_acquire_rate_limit_v3", src,
                f"{name} uses the legacy _acquire_rate_limit_v3 (should be _acquire_shared_rate_limit)",
            )
            # None should use the 10 RPS V1/V2 limiter.
            self.assertNotIn(
                "_acquire_rate_limit(", src,
                f"{name} uses the V1/V2 10 RPS limiter (should be the shared 5 RPS limiter)",
            )

    async def test_shared_5rps_limiter(self):
        # Verify the shared limiter enforces 5 RPS (0.2s between calls).
        # Reset the shared limiter state.
        bec._last_request_time_shared = 0.0
        async with bec._rate_limiter_lock_shared:
            bec._last_request_time_shared = 0.0

        # Measure time for 6 calls through the shared limiter
        call_times = []
        for i in range(6):
            t0 = time.monotonic()
            await bec._acquire_shared_rate_limit()
            elapsed = time.monotonic() - t0
            call_times.append(elapsed)

        # The first call should be instant (no delay)
        self.assertLess(call_times[0], 0.1)

        # Subsequent calls should be at least 0.2 seconds apart (5 RPS = 0.2s per call)
        for i in range(1, 6):
            self.assertGreater(call_times[i], 0.15,
                             f"Call {i} took only {call_times[i]:.3f}s, expected >0.15s due to rate limiting")

        # Total time should be around 1 second (5 calls * 0.2s)
        total_time = sum(call_times)
        self.assertGreater(total_time, 0.8,
                          f"Total time {total_time:.3f}s is too short, expected >0.8s")


# ---------------------------------------------------------------------------
# Acceptance 8: Output separation. dm_email blank if only company/page.
# ---------------------------------------------------------------------------


class TestAcceptance8OutputSeparation(unittest.IsolatedAsyncioTestCase):
    """When only a company/page email is found, dm_email MUST remain
    blank and company_email MUST be populated."""

    async def test_dm_email_blank_company_email_populated(self):
        async def fake_find_company_email(http, website):
            return {"email": "specific@acme.com", "email_status": "verified"}

        with _enable_company_fallbacks(), \
             patch.object(bec, "find_company_email", fake_find_company_email), \
             patch.object(mt, "verify_email", AsyncMock(return_value={"valid": True, "code": "ok", "message": "ok"})):
            fb_result = await cf.run_company_fallbacks(
                AsyncMock(),
                domain="acme.com",
                facebook_url="",
                source_path_prefix="",
                validate_email=False,
            )

        # Build a row that already has a person-row shape with no dm_email.
        row = {
            "dm_email": "",
            "dm_email_source": "",
            "source_path": "contacts_db -> blitz -> wizleads -> better_enrich -> not_found",
            "providers_called": "[]",
            "no_email_reason": "all_providers_called_no_email",
        }
        cf.apply_company_fallbacks_to_row(
            row, fb_result, person_email="", person_source_path="",
        )

        # dm_email is STILL blank.
        self.assertEqual(row["dm_email"], "")
        self.assertEqual(row["dm_email_source"], "")
        # company_email populated.
        self.assertEqual(row["company_email"], "specific@acme.com")
        self.assertEqual(row["company_email_source"], pipeline_mod.SOURCE_BETTER_ENRICH_COMPANY_V2)
        # final_email blank (allow_company_email_as_final is False in the patch).
        self.assertEqual(row["final_email"], "")
        # no_email_reason preserved (company fallback did not override).
        self.assertEqual(row["no_email_reason"], "all_providers_called_no_email")

    async def test_apply_person_wins(self):
        """When a person email is also present, final_email reflects the
        person email and the company email sits separately."""
        async def fake_find_company_email(http, website):
            return {"email": "info@acme.com", "email_status": "verified"}

        with _enable_company_fallbacks(allow_generic=True, allow_as_final=True), \
             patch.object(bec, "find_company_email", fake_find_company_email), \
             patch.object(mt, "verify_email", AsyncMock(return_value={"valid": True, "code": "ok", "message": "ok"})):
            fb_result = await cf.run_company_fallbacks(
                AsyncMock(),
                domain="acme.com",
                facebook_url="",
                source_path_prefix="",
                validate_email=False,
            )

        row = {
            "dm_email": "jane@acme.com",
            "dm_email_source": pipeline_mod.SOURCE_BETTER_ENRICH_PERSON,
            "source_path": "name_domain -> better_enrich_v3",
            "providers_called": "[]",
            "no_email_reason": "",
        }
        cf.apply_company_fallbacks_to_row(
            row, fb_result, person_email="jane@acme.com", person_source_path="name_domain -> better_enrich_v3",
        )

        # final_email is the person email.
        self.assertEqual(row["final_email"], "jane@acme.com")
        self.assertEqual(row["final_email_level"], "person")
        # company_email still populated.
        self.assertEqual(row["company_email"], "info@acme.com")
        # dm_email untouched.
        self.assertEqual(row["dm_email"], "jane@acme.com")


# ---------------------------------------------------------------------------
# Identifier facebook_url extraction tests
# ---------------------------------------------------------------------------


class TestFacebookIdentifierPropagation(unittest.TestCase):
    """facebook_url is plumbed through build_row_identifier_payload
    and properly normalized."""

    def test_facebook_url_extracted_and_normalized(self):
        row = {
            "domain": "acme.com",
            "full_name": "Jane Doe",
            "facebook": "https://www.facebook.com/AcmePage",
        }
        payload = idu.build_row_identifier_payload(
            row,
            domain_col="domain",
            name_col="full_name",
            facebook_url_col="facebook",
        )
        self.assertEqual(payload["facebook_url"], "https://www.facebook.com/AcmePage")
        self.assertEqual(payload["normalized_facebook_url"], "https://facebook.com/acmepage")
        self.assertIn("facebook_url", payload["input_fields_used"])
        self.assertEqual(payload["input_facebook_url"], "https://www.facebook.com/AcmePage")

    def test_facebook_url_rejects_non_facebook(self):
        self.assertEqual(idu.normalize_facebook_url("https://example.com/page"), "")
        self.assertEqual(idu.normalize_facebook_url(""), "")
        self.assertEqual(idu.normalize_facebook_url(None), "")

    def test_facebook_suggest_column(self):
        self.assertEqual(idu.suggest_facebook_column(["a", "facebook_url", "b"]), "facebook_url")
        self.assertEqual(idu.suggest_facebook_column(["a", "fb_url", "b"]), "fb_url")
        self.assertEqual(idu.suggest_facebook_column(["a", "page_url", "b"]), "page_url")
        self.assertIsNone(idu.suggest_facebook_column(["a", "b"]))


# ---------------------------------------------------------------------------
# Generic email classifier tests
# ---------------------------------------------------------------------------


class TestGenericEmailClassifier(unittest.TestCase):
    """is_generic_email recognises the seven documented prefix tokens."""

    def test_generic_prefixes(self):
        for prefix in ("info", "contact", "support", "hello", "sales", "admin", "office"):
            with self.subTest(prefix=prefix):
                self.assertTrue(fb_cfg.is_generic_email(f"{prefix}@acme.com"))
                self.assertTrue(fb_cfg.is_generic_email(f"  {prefix.upper()}@acme.com  "))

    def test_specific_prefixes(self):
        for prefix in ("jane", "ceo", "team", "billing", "press"):
            with self.subTest(prefix=prefix):
                self.assertFalse(fb_cfg.is_generic_email(f"{prefix}@acme.com"))

    def test_invalid_inputs(self):
        self.assertFalse(fb_cfg.is_generic_email(""))
        self.assertFalse(fb_cfg.is_generic_email("not-an-email"))
        self.assertFalse(fb_cfg.is_generic_email(None))


# ---------------------------------------------------------------------------
# Source-path / provider-attempts accounting
# ---------------------------------------------------------------------------


class TestProviderAttemptsAccounting(unittest.IsolatedAsyncioTestCase):
    """The new sources 'better_enrich_facebook_email' and
    'better_enrich_company_email' must appear in providers_called and
    be mapped to the canonical 'better_enrich' group in stats_store."""

    def test_source_groups_contain_new_sources(self):
        from enrichment import stats_store as ss
        self.assertEqual(ss.normalize_source("better_enrich_facebook_email"), "better_enrich")
        self.assertEqual(ss.normalize_source("better_enrich_company_email"), "better_enrich")

    async def test_providers_called_records_new_attempts(self):
        async def fake_find_company_email(http, website):
            return {"email": "specific@acme.com", "email_status": "verified"}

        recorded = []
        with _enable_company_fallbacks(allow_as_final=False), \
             patch.object(bec, "find_company_email", fake_find_company_email), \
             patch.object(mt, "verify_email", AsyncMock(return_value={"valid": True, "code": "ok", "message": "ok"})):
            await cf.run_company_fallbacks(
                AsyncMock(), domain="acme.com", facebook_url="",
                source_path_prefix="", validate_email=False,
                record_provider_use=recorded.append,
            )

        # better_enrich was recorded (as a fallback attempt).
        self.assertIn("better_enrich", recorded)


if __name__ == "__main__":
    unittest.main()
