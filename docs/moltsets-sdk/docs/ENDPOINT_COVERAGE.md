# MoltSets Endpoint Coverage

Snapshot: **2026-10-14**

Current API-reference endpoints covered: **20/20**
Documented legacy routes covered: **2/2**

| Category | Method | SDK method | Endpoint |
|---|---|---|---|
| search | POST | `search_people()` | `/search_people` |
| search | POST | `search_companies()` | `/search_companies` |
| search | POST | `search_linkedin_profile()` | `/search_linkedin_profile` |
| search | POST | `search_business_email_by_name()` | `/search_business_email_by_name` |
| search | POST | `search_business_profile_by_name()` | `/search_business_profile_by_name` |
| email | POST | `linkedin_to_best_email()` | `/linkedin_to_best_email` |
| email | POST | `linkedin_to_business_email()` | `/linkedin_to_business_email` |
| email | POST | `linkedin_to_personal_email_usa()` | `/linkedin_to_personal_email_usa` |
| email | POST | `linkedin_to_best_personal_email_usa()` | `/linkedin_to_best_personal_email_usa` |
| phone | POST | `linkedin_to_mobile_phone()` | `/linkedin_to_mobile_phone` |
| reverse_lookup | POST | `reverse_email_lookup()` | `/reverse_email_lookup` |
| reverse_lookup | POST | `reverse_linkedin_lookup()` | `/reverse_linkedin_lookup` |
| ad_audience | POST | `email_to_maid()` | `/email_to_maid` |
| ad_audience | POST | `business_email_to_sha256()` | `/business_email_to_sha256` |
| ad_audience | POST | `linkedin_to_sha256()` | `/linkedin_to_sha256` |
| linkedin | POST | `email_to_linkedin()` | `/email_to_linkedin` |
| company | POST | `ip_to_company()` | `/ip_to_company` |
| account | POST | `get_account()` | `/get_account` |
| account | POST | `get_billing()` | `/get_billing` |
| account | POST | `get_usage()` | `/get_usage` |

## Legacy compatibility routes

- `linkedin_to_personal_email()` -> `linkedin_to_personal_email_usa()` migration target
- `linkedin_to_best_personal_email()` -> `linkedin_to_best_personal_email_usa()` migration target

## Cross-cutting behavior encoded in the SDK

- Bearer authentication on every call.
- Mandatory non-empty `User-Agent` supplied automatically.
- HTTP 200 + `status: not_found` is returned as a normal miss, not an exception.
- API errors are raised with machine-readable `error.code`.
- `Retry-After` / `metadata.retry_after` are preserved on rate-limit errors.
- Long Fair Use waits are not slept automatically by default; the exception carries the retry time so the pipeline can requeue.
- 200 partial-batch errors are retained in `response.partial_error` rather than discarded.
- `X-MoltSets-Outcome` is exposed as `response.outcome`.
- Generic `client.call(tool, **payload)` remains available for future/unknown tools.
