from __future__ import annotations

BASE_URL = "https://api.moltsets.com/api/v1/tools"

# Current API-reference pages exposed by MoltSets' documentation index.
CURRENT_ENDPOINTS = (
    "search_people",
    "search_companies",
    "search_linkedin_profile",
    "search_business_email_by_name",
    "search_business_profile_by_name",
    "linkedin_to_best_email",
    "linkedin_to_business_email",
    "linkedin_to_personal_email_usa",
    "linkedin_to_best_personal_email_usa",
    "linkedin_to_mobile_phone",
    "reverse_email_lookup",
    "reverse_linkedin_lookup",
    "email_to_maid",
    "business_email_to_sha256",
    "linkedin_to_sha256",
    "email_to_linkedin",
    "ip_to_company",
    "get_account",
    "get_billing",
    "get_usage",
)

# Documented migration aliases that still work at the time of this SDK snapshot.
LEGACY_ENDPOINTS = (
    "linkedin_to_personal_email",
    "linkedin_to_best_personal_email",
)

ALL_KNOWN_ENDPOINTS = CURRENT_ENDPOINTS + LEGACY_ENDPOINTS
