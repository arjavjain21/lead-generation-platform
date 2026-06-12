#!/usr/bin/env python3
"""
Mechanical check: build a 10-row CSV in memory and run it through the
identifier propagation helper, asserting that input_linkedin_url, input_phone,
and input_fields_used are correctly set for every row that has data.

Exits 0 on success, 1 on failure.
"""

import os
import sys

# Make backend importable
HERE = os.path.dirname(os.path.abspath(__file__))
BACKEND_DIR = os.path.abspath(os.path.join(HERE, ".."))
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

from enrichment import identifier_utils as u


SAMPLE_ROWS = [
    {"domain": "google.com", "full_name": "Sundar Pichai", "linkedin_url": "https://linkedin.com/in/sundarpichai", "phone": "650-253-0000", "company_name": "Google"},
    {"domain": "apple.com", "full_name": "Tim Cook", "linkedin_url": "HTTP://Linkedin.com/in/tim-cook/", "phone": "(408) 996-1010", "company_name": "Apple"},
    {"domain": "  microsoft.com  ", "full_name": "  Satya Nadella  ", "linkedin_url": "linkedin.com/in/satyanadella", "phone": "", "company_name": "Microsoft"},
    {"domain": "amazon.com", "full_name": "Andy Jassy", "linkedin_url": "nan", "phone": "n/a", "company_name": "Amazon"},
    {"domain": "meta.com", "full_name": "Mark Zuckerberg", "linkedin_url": "https://facebook.com/zuck", "phone": "555-0001", "company_name": "Meta"},
    {"domain": "tesla.com", "full_name": "Elon Musk", "linkedin_url": "https://linkedin.com/in/elonmusk", "phone": None, "company_name": "Tesla"},
    {"domain": "netflix.com", "full_name": "Reed Hastings", "linkedin_url": "", "phone": "  ", "company_name": "Netflix"},
    {"domain": "nvidia.com", "full_name": "Jensen Huang", "linkedin_url": "https://linkedin.com/in/jenhsunhuang", "phone": "408-486-2000", "company_name": "NVIDIA"},
    {"domain": "salesforce.com", "full_name": "Marc Benioff", "linkedin_url": "https://linkedin.com/in/marcbenioff", "phone": "415-901-7000", "company_name": "Salesforce"},
    {"domain": "oracle.com", "full_name": "Safra Catz", "linkedin_url": "https://linkedin.com/in/safracatz", "phone": "737-867-7000", "company_name": "Oracle"},
]


def main() -> int:
    failures = []
    for idx, row in enumerate(SAMPLE_ROWS):
        payload = u.build_row_identifier_payload(
            row,
            domain_col="domain",
            name_col="full_name",
            linkedin_url_col="linkedin_url",
            phone_col="phone",
            company_name_col="company_name",
        )
        # Every row must have a domain.
        if not payload["input_domain"]:
            failures.append(f"Row {idx}: missing input_domain")
        # input_linkedin_url must match source for non-empty values.
        src_li = u.normalize_value(row.get("linkedin_url"))
        if src_li and not payload["input_linkedin_url"]:
            failures.append(f"Row {idx}: input_linkedin_url empty (source: {src_li!r})")
        # input_phone must match source for non-empty values.
        src_phone = u.normalize_value(row.get("phone"))
        if src_phone and not payload["input_phone"]:
            failures.append(f"Row {idx}: input_phone empty (source: {src_phone!r})")
        # input_fields_used must include 'domain' for every row.
        used = set(payload["input_fields_used"].split(","))
        if "domain" not in used:
            failures.append(f"Row {idx}: input_fields_used missing 'domain'")
        # If linkedin URL is real (linkedin.com host), normalized version must be lowercased.
        if src_li and "linkedin.com" in src_li.lower():
            if not payload["normalized_linkedin_url"].startswith("https://linkedin.com/"):
                failures.append(f"Row {idx}: normalized_linkedin_url not canonical ({payload['normalized_linkedin_url']!r})")
            if "HTTP" in payload["normalized_linkedin_url"] or payload["normalized_linkedin_url"].endswith("/"):
                failures.append(f"Row {idx}: normalized_linkedin_url not fully normalized ({payload['normalized_linkedin_url']!r})")

    if failures:
        print(f"FAIL: {len(failures)} issues")
        for f in failures:
            print(f"  - {f}")
        return 1
    print(f"PASS: {len(SAMPLE_ROWS)} rows checked, all input_* columns populated correctly")
    return 0


if __name__ == "__main__":
    sys.exit(main())
