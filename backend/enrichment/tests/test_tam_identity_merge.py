"""Cross-provider identity + survivorship fixtures (all-source audit stage 2).

Deterministic fixtures for every identity collision class from the review:
bare vs www domains, scheme/path/query variants, LinkedIn variants,
same-domain different names, same-name different companies, LinkedIn-only,
name-only, missing-domain rows, and survivorship merge semantics
(fill-not-overwrite, matched_people basis, source accumulation).
"""
from __future__ import annotations

import os
import sys
import unittest

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from enrichment import tam_flow  # noqa: E402


class TestIdentityKey(unittest.TestCase):
    def test_domain_variants_collapse(self):
        variants = ["nike.com", "www.nike.com", "https://nike.com",
                    "http://www.nike.com", "https://www.Nike.com/jordan?x=1",
                    "WWW.NIKE.COM/", "nike.com:443"]
        keys = {tam_flow.tam_identity_key({"domain": v}) for v in variants}
        self.assertEqual(keys, {"d:nike.com"})

    def test_deep_url_domains_bare(self):
        self.assertEqual(tam_flow.tam_identity_key({"domain": "https://acme.com/?utm_source=gmb"}),
                         "d:acme.com")

    def test_linkedin_variants_collapse(self):
        variants = ["https://www.linkedin.com/company/acme/",
                    "linkedin.com/company/acme",
                    "https://www.linkedin.com/company/acme?trk=xyz",
                    "WWW.LINKEDIN.COM/COMPANY/ACME/"]
        keys = {tam_flow.tam_identity_key({"linkedin_url": v}) for v in variants}
        self.assertEqual(keys, {"l:linkedin.com/company/acme"})

    def test_personal_linkedin_not_a_company_key_via_domain_first(self):
        # A personal /in/ URL still keys as l:... when it's all the row has —
        # the chain separately refuses non-/company/ URLs; here we only assert
        # canonicalization, not business validity.
        k = tam_flow.tam_identity_key({"linkedin_url": "https://www.linkedin.com/in/jane/"})
        self.assertEqual(k, "l:linkedin.com/in/jane")

    def test_domain_wins_over_linkedin(self):
        k = tam_flow.tam_identity_key({"domain": "nike.com",
                                       "linkedin_url": "https://linkedin.com/company/nike"})
        self.assertEqual(k, "d:nike.com")

    def test_linkedin_only(self):
        self.assertEqual(tam_flow.tam_identity_key({"linkedin_url": "https://linkedin.com/company/x"}),
                         "l:linkedin.com/company/x")

    def test_name_only_and_missing(self):
        self.assertEqual(tam_flow.tam_identity_key({"name": "Acme Corp"}), "n:acme corp")
        self.assertIsNone(tam_flow.tam_identity_key({"name": " ", "domain": ""}))

    def test_same_name_different_companies_do_not_merge(self):
        # Two companies named "Acme" with different domains -> distinct keys
        # (name-keying only when BOTH domain and linkedin are absent).
        a = tam_flow.tam_identity_key({"domain": "acme.us", "name": "Acme"})
        b = tam_flow.tam_identity_key({"domain": "acme.de", "name": "Acme"})
        self.assertNotEqual(a, b)

    def test_cross_provider_reconciliation(self):
        # Provider A: www domain; Provider B: bare domain + different name
        # spelling; Provider C: LinkedIn only -> A==B by domain; C stays
        # separate until it supplies the domain (no name-based merging).
        a = tam_flow.tam_identity_key({"domain": "www.stripe.com", "name": "Stripe"})
        b = tam_flow.tam_identity_key({"domain": "stripe.com", "name": "Stripe, Inc."})
        c = tam_flow.tam_identity_key({"linkedin_url": "https://linkedin.com/company/stripe"})
        self.assertEqual(a, b)
        self.assertNotEqual(a, c)


class _Shared:
    rows: list = None


class TestSurvivorshipMerge(unittest.TestCase):
    def _row(self, **kw):
        base = {"name": "", "domain": "", "website": "", "linkedin_url": "",
                "industry": "", "type": "", "size": "", "matched_people": 1,
                "matched_people_basis": "placeholder", "source": "",
                "sources": "", "revenue": "", "hq_country_code": "", "slogan": ""}
        base.update(kw)
        return base

    def test_fill_not_overwrite(self):
        survivor = self._row(name="Acme", domain="acme.com", type="Privately Held",
                             source="blitz", matched_people=7,
                             matched_people_basis="observed", linkedin_url="https://linkedin.com/company/acme")
        newcomer = self._row(name="Acme Inc", domain="acme.com", industry="Retail",
                             revenue="$10M", source="leadocean", matched_people=1,
                             matched_people_basis="placeholder")
        tam_flow._merge_company_row(survivor, newcomer)
        self.assertEqual(survivor["name"], "Acme")          # NOT overwritten
        self.assertEqual(survivor["type"], "Privately Held")  # NOT overwritten
        self.assertEqual(survivor["matched_people"], 7)      # real count kept
        self.assertEqual(survivor["matched_people_basis"], "observed")
        self.assertEqual(survivor["industry"], "Retail")     # blank filled
        self.assertEqual(survivor["revenue"], "$10M")        # blank filled
        self.assertEqual(survivor["sources"], "blitz;leadocean")

    def test_larger_observed_count_promotes(self):
        survivor = self._row(domain="acme.com", matched_people=2,
                             matched_people_basis="observed", source="blitz")
        newcomer = self._row(domain="acme.com", matched_people=9,
                             matched_people_basis="observed", source="quickenrich")
        tam_flow._merge_company_row(survivor, newcomer)
        self.assertEqual(survivor["matched_people"], 9)

    def test_placeholder_never_beats_observed(self):
        survivor = self._row(domain="acme.com", matched_people=3,
                             matched_people_basis="observed", source="quickenrich")
        newcomer = self._row(domain="acme.com", matched_people=99,
                             matched_people_basis="placeholder", source="leadocean")
        tam_flow._merge_company_row(survivor, newcomer)
        self.assertEqual(survivor["matched_people"], 3)

    def test_keyword_confirmation_propagates(self):
        survivor = self._row(domain="acme.com", keyword_match="unverified", source="getleads")
        newcomer = self._row(domain="acme.com", keyword_match="confirmed", source="leadocean")
        tam_flow._merge_company_row(survivor, newcomer)
        self.assertEqual(survivor["keyword_match"], "confirmed")

    def test_merge_newcomers_appends_and_merges(self):
        shared = _Shared()
        shared.rows = [self._row(domain="acme.com", source="blitz", industry="Software")]
        newcomers = [
            self._row(domain="acme.com", source="leadocean", revenue="$5M"),
            self._row(domain="beta.com", source="leadocean"),
        ]
        tam_flow._merge_newcomers(shared, newcomers)
        self.assertEqual(len(shared.rows), 2)  # one merge + one append
        self.assertEqual(shared.rows[0]["revenue"], "$5M")
        self.assertEqual(shared.rows[0]["sources"], "blitz;leadocean")
        self.assertEqual(shared.rows[1]["domain"], "beta.com")


class TestCapabilityMatrix(unittest.TestCase):
    CF = {"employee_range": ["11-50"], "keywords": {"include": ["jewelry"]},
          "type": {"include": ["Privately Held"]}}
    PF = {"job_title": {"include": ["CEO"]}, "min_per_company": 3}

    def test_getleads_cannot_enforce_keywords_type_minmatches(self):
        dims = tam_flow.provider_unverified_dims("getleads", self.CF, self.PF)
        self.assertIn("keywords", dims)
        self.assertIn("type", dims)         # (typed searches skip the GL leg outright)
        self.assertIn("min_matches", dims)
        self.assertNotIn("size", dims)      # GL bands are exact

    def test_quickenrich_size_and_keywords_in_persona_mode(self):
        dims = tam_flow.provider_unverified_dims("quickenrich", self.CF, self.PF)
        self.assertIn("size", dims)         # taxonomy-incompatible
        self.assertIn("keywords", dims)     # contact-finder mode (titles set)
        self.assertIn("min_matches", dims)

    def test_quickenrich_keywords_ok_without_titles(self):
        dims = tam_flow.provider_unverified_dims("quickenrich", self.CF, {})
        self.assertNotIn("keywords", dims)  # company-finder mode
        self.assertIn("size", dims)

    def test_leadocean_type_and_minmatches(self):
        dims = tam_flow.provider_unverified_dims("leadocean", self.CF, self.PF)
        self.assertIn("type", dims)
        self.assertIn("min_matches", dims)
        self.assertNotIn("keywords", dims)
        self.assertNotIn("size", dims)

    def test_blitz_enforces_everything(self):
        self.assertEqual(tam_flow.provider_unverified_dims("blitz", self.CF, self.PF), [])


if __name__ == "__main__":
    unittest.main()
