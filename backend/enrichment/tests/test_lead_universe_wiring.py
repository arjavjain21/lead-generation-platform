"""Regression tests for lead_universe write-back wiring (fixes #2 + #3).

#2: the synchronous /enrich handlers now copy ``company_industry`` onto each
    contact dict (it already lives on the pipeline row), so
    ``_build_contacts_writer_payloads`` -> ``classify_industry`` tags
    ``lead_universe`` on write-back instead of leaving new contacts NULL.
#3: ``EmployeeSearchRequest.universe`` is validated (case-normalized; unknown
    buckets rejected with 422) so a typo no longer silently returns no leads.
"""
import unittest

from pydantic import ValidationError


class TestUniverseValidation(unittest.TestCase):
    def test_normalizes_case(self):
        from enrichment.routes import EmployeeSearchRequest

        self.assertEqual(EmployeeSearchRequest(universe="SaaS").universe, "saas")
        self.assertEqual(
            EmployeeSearchRequest(universe="LOCAL_BUSINESS").universe,
            "local_business",
        )

    def test_empty_and_omitted_mean_all(self):
        from enrichment.routes import EmployeeSearchRequest

        self.assertIsNone(EmployeeSearchRequest(universe="").universe)
        self.assertIsNone(EmployeeSearchRequest(universe=None).universe)
        self.assertIsNone(EmployeeSearchRequest().universe)

    def test_all_four_buckets_accepted(self):
        from enrichment.routes import EmployeeSearchRequest

        for u in ("local_business", "b2b_agency", "saas", "ecom"):
            self.assertEqual(EmployeeSearchRequest(universe=u).universe, u)

    def test_unknown_bucket_rejected(self):
        from enrichment.routes import EmployeeSearchRequest

        for bad in ("bogus", "local", "saas2", "ecommerce", "agency"):
            with self.assertRaises(ValidationError):
                EmployeeSearchRequest(universe=bad)


class TestCompanyIndustryPropagation(unittest.TestCase):
    def test_payload_carries_company_industry(self):
        from enrichment.routes import _build_contacts_writer_payloads

        contacts = [
            {"email": "jane@acme.com", "full_name": "Jane", "company_industry": "plumber"}
        ]
        payloads = _build_contacts_writer_payloads(contacts, "acme.com")
        self.assertEqual(len(payloads), 1)
        self.assertEqual(payloads[0]["company_industry"], "plumber")

    def test_payload_blank_when_absent(self):
        from enrichment.routes import _build_contacts_writer_payloads

        contacts = [{"email": "jane@acme.com", "full_name": "Jane"}]
        payloads = _build_contacts_writer_payloads(contacts, "acme.com")
        self.assertEqual(payloads[0]["company_industry"], "")


if __name__ == "__main__":
    unittest.main()
