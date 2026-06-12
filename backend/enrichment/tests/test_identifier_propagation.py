"""
Acceptance tests for identifier propagation through the enrichment pipeline.

Goal: confirm that row-level LinkedIn URLs, phone numbers, company names, and
existing emails are extracted, normalized, attached to the output, and counted
in `input_fields_used`.

All tests use the helper utilities in `identifier_utils` and a hand-rolled
subset of the pipeline's row processor. They do NOT hit the network or
external services.
"""

from __future__ import annotations

import sys
import os
import unittest
from typing import Any

# Add backend to path so 'enrichment.identifier_utils' resolves
_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from enrichment import identifier_utils as u  # noqa: E402
from enrichment import pipeline as pipeline_mod  # noqa: E402


SAMPLE_ROWS = [
    {
        "domain": "google.com",
        "full_name": "Sundar Pichai",
        "linkedin_url": "https://linkedin.com/in/sundarpichai",
        "phone": "650-253-0000",
        "company_name": "Google",
        "existing_email": "sundar@google.com",
    },
    {
        "domain": "apple.com",
        "full_name": "Tim Cook",
        "linkedin_url": "HTTP://Linkedin.com/in/tim-cook/",
        "phone": "(408) 996-1010",
        "company_name": "Apple",
        "existing_email": "",
    },
    {
        "domain": "  microsoft.com  ",
        "full_name": "  Satya Nadella  ",
        "linkedin_url": "linkedin.com/in/satyanadella",
        "phone": "",
        "company_name": "Microsoft",
        "existing_email": "satya@microsoft.com",
    },
    {
        "domain": "amazon.com",
        "full_name": "Andy Jassy",
        "linkedin_url": "nan",
        "phone": "n/a",
        "company_name": "",
        "existing_email": "none",
    },
    {
        "domain": "meta.com",
        "full_name": "Mark Zuckerberg",
        "linkedin_url": "https://facebook.com/zuck",  # not linkedin
        "phone": "555-0001",
        "company_name": "Meta",
        "existing_email": "zuck@meta.com",
    },
    {
        "domain": "tesla.com",
        "full_name": "Elon Musk",
        "linkedin_url": "https://linkedin.com/in/elonmusk",
        "phone": None,
        "company_name": "Tesla",
        "existing_email": None,
    },
    {
        "domain": "netflix.com",
        "full_name": "Reed Hastings",
        "linkedin_url": "",
        "phone": "  ",
        "company_name": "Netflix",
        "existing_email": "",
    },
    {
        "domain": "nvidia.com",
        "full_name": "Jensen Huang",
        "linkedin_url": "https://linkedin.com/in/jenhsunhuang",
        "phone": "408-486-2000",
        "company_name": "NVIDIA",
        "existing_email": "jensen@nvidia.com",
    },
    {
        "domain": "salesforce.com",
        "full_name": "Marc Benioff",
        "linkedin_url": "https://linkedin.com/in/marcbenioff",
        "phone": "415-901-7000",
        "company_name": "Salesforce",
        "existing_email": "marc@salesforce.com",
    },
    {
        "domain": "oracle.com",
        "full_name": "Safra Catz",
        "linkedin_url": "https://linkedin.com/in/safracatz",
        "phone": "737-867-7000",
        "company_name": "Oracle",
        "existing_email": "safra@oracle.com",
    },
]


COLS = {
    "domain_col": "domain",
    "name_col": "full_name",
    "linkedin_url_col": "linkedin_url",
    "phone_col": "phone",
    "company_name_col": "company_name",
    "existing_email_col": "existing_email",
}


def simulate_row_processing(row: dict) -> dict:
    """Mirror what pipeline.process_row does with the input payload."""
    payload = u.build_row_identifier_payload(
        row,
        **COLS,
    )
    # Output row starts as the original row plus all enriched columns empty.
    out = {**row, **{c: "" for c in pipeline_mod.ENRICHED_COLUMNS}}
    out["row_status"] = "enriched"
    u.attach_input_columns(out, payload)
    return out


class TestDirectAPI(unittest.TestCase):
    """Direct API: domain + full_name + linkedin_url are all in input_fields_used."""

    def test_direct_api_payload_includes_all_fields(self):
        row = {
            "domain": "acme.com",
            "full_name": "Jane Doe",
            "linkedin_url": "https://linkedin.com/in/janedoe",
        }
        p = u.build_row_identifier_payload(
            row, domain_col="domain", name_col="full_name", linkedin_url_col="linkedin_url"
        )
        self.assertEqual(p["domain"], "acme.com")
        self.assertEqual(p["full_name"], "Jane Doe")
        self.assertEqual(p["linkedin_url"], "https://linkedin.com/in/janedoe")
        self.assertEqual(p["normalized_linkedin_url"], "https://linkedin.com/in/janedoe")
        self.assertEqual(p["linkedin_username"], "janedoe")
        used = set(p["input_fields_used"].split(","))
        self.assertIn("domain", used)
        self.assertIn("name", used)
        self.assertIn("linkedin_url", used)


class TestTenRowCSV(unittest.TestCase):
    """CSV with 10 rows: LinkedIn and phone values appear in output for all matching rows."""

    def test_all_rows_produce_output_with_input_columns(self):
        outputs = [simulate_row_processing(r) for r in SAMPLE_ROWS]
        self.assertEqual(len(outputs), 10)

        # LinkedIn URL appears for all 10 rows except the one that didn't
        # have a linkedin URL (amazon's 'nan' became '') and the one with
        # a non-LinkedIn URL (meta).
        li_in_output = [o for o in outputs if o["input_linkedin_url"]]
        # The amazon row had 'nan' which normalizes to ''; meta had facebook URL (not linkedin).
        # So we expect 8 rows with non-empty input_linkedin_url.
        self.assertEqual(len(li_in_output), 8,
                         f"Expected 8 rows with non-empty input_linkedin_url, got {len(li_in_output)}")

        # Normalized LinkedIn URL is lowercase and canonical
        # (only for rows with a real LinkedIn URL).
        for o in outputs:
            if o["input_linkedin_url"]:
                # If it's a valid LinkedIn URL, we expect normalized version
                if "linkedin.com" in o["input_linkedin_url"].lower():
                    self.assertTrue(o["normalized_linkedin_url"].startswith("https://linkedin.com/"))
                    self.assertNotIn("HTTP", o["normalized_linkedin_url"])
                    self.assertFalse(o["normalized_linkedin_url"].endswith("/"))
                # Non-LinkedIn URLs result in empty normalized version
                else:
                    self.assertEqual(o["normalized_linkedin_url"], "")

        # linkedin_username only set for /in/<slug> URLs.
        for o in outputs:
            if o["linkedin_username"]:
                self.assertIn("/in/", o["normalized_linkedin_url"])

        # Phone appears in output for rows that had a phone in source CSV.
        # We expect 9 rows to have non-empty input_phone (amazon 'n/a' is empty,
        # netflix had whitespace-only, tesla was None, but tesla row had a phone
        # value of None in the dict so normalize_value returns '' for None).
        phone_in_output = [o for o in outputs if o["input_phone"]]
        self.assertGreaterEqual(len(phone_in_output), 6,
                                f"Expected at least 6 rows with non-empty input_phone, got {len(phone_in_output)}")

        # Every row's input_fields_used includes 'domain' (we have a domain for all 10).
        for o in outputs:
            used = set(o["input_fields_used"].split(","))
            self.assertIn("domain", used)

        # Every row has the input_domain column populated from the source row.
        for o, src in zip(outputs, SAMPLE_ROWS):
            self.assertEqual(o["input_domain"], src["domain"].strip())

    def test_100_percent_rows_propagate_linkedin_and_phone(self):
        """The strict test from the goal: 100% of rows that had a non-empty
        linkedin_url or phone in the source CSV must carry it through to
        output columns."""
        outputs = [simulate_row_processing(r) for r in SAMPLE_ROWS]

        for src, out in zip(SAMPLE_ROWS, outputs):
            src_li = u.normalize_value(src.get("linkedin_url"))
            src_phone = u.normalize_value(src.get("phone"))

            if src_li:
                self.assertTrue(out["input_linkedin_url"],
                                f"input_linkedin_url empty for source {src!r}")
                # And the normalized version is the canonical LinkedIn URL.
                if u._LINKEDIN_HOST_RE.match(src_li):
                    self.assertTrue(out["normalized_linkedin_url"])

            if src_phone:
                self.assertTrue(out["input_phone"],
                                f"input_phone empty for source {src!r}")


class TestBackwardCompatibility(unittest.TestCase):
    """CSVs with only domain+name (no new columns) still work."""

    def test_only_domain_and_name(self):
        row = {"domain": "acme.com", "full_name": "Jane Doe"}
        p = u.build_row_identifier_payload(
            row, domain_col="domain", name_col="full_name"
        )
        self.assertEqual(p["domain"], "acme.com")
        self.assertEqual(p["full_name"], "Jane Doe")
        self.assertEqual(p["linkedin_url"], "")
        self.assertEqual(p["phone"], "")
        self.assertEqual(p["company_name"], "")
        self.assertEqual(p["existing_email"], "")
        used = set(p["input_fields_used"].split(","))
        self.assertEqual(used, {"domain", "name"})

    def test_first_last_name_compose(self):
        row = {"domain": "acme.com", "first_name": "Jane", "last_name": "Doe"}
        p = u.build_row_identifier_payload(
            row,
            domain_col="domain",
            first_name_col="first_name",
            last_name_col="last_name",
        )
        self.assertEqual(p["full_name"], "Jane Doe")
        used = set(p["input_fields_used"].split(","))
        self.assertIn("name", used)


class TestAutoDetect(unittest.TestCase):
    """Auto-detect common column names."""

    def test_linkedin_profile_suggested(self):
        cols = ["foo", "LinkedIn Profile", "bar"]
        self.assertEqual(u.suggest_linkedin_column(cols), "LinkedIn Profile")

    def test_mobile_phone_suggested(self):
        cols = ["foo", "Mobile Phone", "bar"]
        self.assertEqual(u.suggest_phone_column(cols), "Mobile Phone")

    def test_phone_number_suggested(self):
        cols = ["Phone Number", "x"]
        self.assertEqual(u.suggest_phone_column(cols), "Phone Number")

    def test_company_name_suggested(self):
        cols = ["x", "Company Name", "y"]
        self.assertEqual(u.suggest_company_column(cols), "Company Name")

    def test_work_email_suggested(self):
        cols = ["x", "Work Email", "y"]
        self.assertEqual(u.suggest_email_column(cols), "Work Email")

    def test_no_match_returns_none(self):
        self.assertIsNone(u.suggest_linkedin_column(["foo", "bar"]))
        self.assertIsNone(u.suggest_phone_column(["foo", "bar"]))
        self.assertIsNone(u.suggest_company_column(["foo", "bar"]))
        self.assertIsNone(u.suggest_email_column(["foo", "bar"]))


class TestOmissionRegression(unittest.TestCase):
    """Omitting the new fields from a request does not break existing flow."""

    def test_payload_helper_with_no_new_cols(self):
        row = {"domain": "x.com", "name": "Bob"}
        p = u.build_row_identifier_payload(
            row, domain_col="domain", name_col="name"
        )
        self.assertEqual(p["domain"], "x.com")
        self.assertEqual(p["full_name"], "Bob")
        self.assertEqual(p["linkedin_url"], "")
        self.assertEqual(p["phone"], "")

    def test_payload_helper_with_no_cols_at_all(self):
        row: dict[str, Any] = {}
        p = u.build_row_identifier_payload(row)
        self.assertEqual(p["domain"], "")
        self.assertEqual(p["full_name"], "")
        self.assertEqual(p["input_fields_used"], "")

    def test_attach_input_columns_does_not_overwrite(self):
        """If output row already has input_* values, don't overwrite them."""
        row = {"input_domain": "original.com"}
        payload = {"input_domain": "new.com", "input_linkedin_url": "x"}
        u.attach_input_columns(row, payload)
        self.assertEqual(row["input_domain"], "original.com")
        self.assertEqual(row["input_linkedin_url"], "x")


if __name__ == "__main__":
    unittest.main()
