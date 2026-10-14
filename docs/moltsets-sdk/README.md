# MoltSets Python SDK

A complete Python SDK snapshot of the public MoltSets REST API documentation as of **2026-10-14**, built for internal enrichment and waterfall pipelines.

This package is not an official MoltSets SDK. It was reconstructed from the public developer documentation and includes a coverage manifest so endpoint drift can be audited.

## Coverage

- 20 current API-reference endpoints: covered 20/20
- 2 documented legacy personal-email routes: covered 2/2
- Sync client and async client
- Generic future-tool caller
- Search pagination helpers
- Machine-readable endpoint manifest
- Reconstructed OpenAPI 3.1 file for code generation in other languages
- Documentation inventory containing all 45 pages listed by the MoltSets `llms.txt` index at the snapshot date
- Library-informed recipe layer for internal waterfall orchestration
- GitHub Actions test matrix for Python 3.10-3.13

See `docs/ENDPOINT_COVERAGE.md` and `moltsets_endpoint_manifest.json`.

## Install

```bash
pip install -e .
```

Set the API key:

```bash
export MOLTSETS_API_KEY="ms_xxxxxxxxxxx"
```

## Basic use

```python
from moltsets import MoltSetsClient

with MoltSetsClient(user_agent="eis-waterfall/1.0") as moltsets:
    response = moltsets.linkedin_to_best_email(
        linkedin_url="https://linkedin.com/in/example"
    )

    if response.not_found:
        print("clean miss, continue waterfall")
    else:
        print(response.results)
```

MoltSets requires both Bearer authentication and a non-empty `User-Agent`. The SDK sends both automatically.

## Async use

```python
import asyncio
from moltsets import AsyncMoltSetsClient

async def main():
    async with AsyncMoltSetsClient(user_agent="eis-waterfall/1.0") as moltsets:
        response = await moltsets.reverse_email_lookup(email="person@example.com")
        print(response.status, response.results)

asyncio.run(main())
```

## Waterfall behavior

MoltSets uses three materially different outcomes:

1. `status == "ok"`: data found.
2. `status == "not_found"`: clean miss. The HTTP response is still 200. Continue to the next provider.
3. HTTP 4xx/5xx with `error.code`: operational, account, plan, input, or rate-limit error.

Do not use HTTP 404 as your miss signal. A documented data miss is HTTP 200 with `status: "not_found"`.

```python
from moltsets import MoltSetsAPIError, MoltSetsClient

client = MoltSetsClient(user_agent="eis-waterfall/1.0")

try:
    result = client.linkedin_to_business_email(
        linkedin_url="https://linkedin.com/in/example"
    )

    if result.not_found:
        # Advance to next provider.
        pass
    else:
        email = result.results.get("email")
        risk = result.results.get("risk_score")

except MoltSetsAPIError as exc:
    if exc.code in {
        "rate_limited",
        "fair_use_limit_exceeded",
        "fair_use_requests_exceeded",
    }:
        # Requeue based on exc.retry_after when present.
        print(exc.retry_after)
    else:
        raise
```

The SDK automatically retries short transient `rate_limited` and `internal_error` responses. It does not sleep for long Fair Use windows. If MoltSets returns a long `Retry-After`, the SDK raises `MoltSetsAPIError` with `exc.retry_after` so your queue can reschedule the job.

## Email safety helper

```python
from moltsets import risk_score_is_sendable

risk_score_is_sendable("A")  # True
risk_score_is_sendable("B")  # True
risk_score_is_sendable("D")  # False
```

The helper is conservative and defaults to A/B only. You can choose your own allowed grades:

```python
risk_score_is_sendable("C", allowed=("A", "B", "C"))
```

## Search pagination

MoltSets search endpoints return at most 25 rows per page. The SDK includes generators:

```python
from moltsets import MoltSetsClient, iter_search_people

client = MoltSetsClient(user_agent="eis-waterfall/1.0")

for person in iter_search_people(
    client,
    title="Head of Sales",
    country=["United States", "Canada"],
    employee_range=["51-200", "201-500"],
    max_results=250,
):
    print(person)
```

## Batch LinkedIn enrichment

The documented LinkedIn email and mobile endpoints accept batches of up to 100 LinkedIn profiles per call:

```python
response = client.linkedin_to_best_email(
    linkedin_urls=[
        "https://linkedin.com/in/a",
        "https://linkedin.com/in/b",
    ]
)
```

Each item still counts separately against MoltSets Fair Use request accounting.

## All current endpoint methods

### Search

```text
search_people
search_companies
search_linkedin_profile
search_business_email_by_name
search_business_profile_by_name
```

### Valid emails

```text
linkedin_to_best_email
linkedin_to_business_email
linkedin_to_personal_email_usa
linkedin_to_best_personal_email_usa
```

### Mobile phone

```text
linkedin_to_mobile_phone
```

### Reverse lookups

```text
reverse_email_lookup
reverse_linkedin_lookup
```

### Ad audience

```text
email_to_maid
business_email_to_sha256
linkedin_to_sha256
```

### LinkedIn and company

```text
email_to_linkedin
ip_to_company
```

### Account

```text
get_account
get_billing
get_usage
```

### Documented legacy migration routes

```text
linkedin_to_personal_email
linkedin_to_best_personal_email
```

The legacy methods call the legacy API route and emit a Python `DeprecationWarning`. New code should use the `_usa` methods.

## Library-informed recipe layer

The public MoltSets Skills Library is an orchestration layer, not a second REST API surface. The SDK keeps all 20 current raw API methods and adds a small `moltsets.recipes` layer for the MoltSets-native workflows that are useful inside an internal waterfall.

```python
from moltsets import MoltSetsClient, find_best_email

with MoltSetsClient(user_agent="internal-waterfall/1.0") as client:
    result = find_best_email(
        client,
        name="Jane Doe",
        company_domain="example.com",
    )

    if result.found and result.accepted:
        print(result.email, result.risk_score, result.source_tool)
    elif result.found:
        print("Found but rejected by risk policy:", result.risk_score)
    else:
        print("Clean miss, continue provider waterfall")
```

Included recipes:

- `find_best_email()` - endpoint-confirmed email fallback chain
- `probe_search_people()` - safe `limit=1` sizing probe
- `enrich_linkedin_profile()` - reverse profile plus optional mobile
- `chunked_mobile_lookup()` - native 100-URL mobile batching

See `docs/LIBRARY_WORKFLOWS.md` for the library-to-SDK mapping and a documented inconsistency around `count_only`.

## Future endpoint escape hatch

If MoltSets adds a route before this SDK is updated:

```python
response = client.call("new_tool_name", foo="bar")
```

This uses the same authentication, headers, response parsing, error parsing, and retry behavior.

## Response object

Every successful HTTP call returns `MoltSetsResponse`:

```python
response.results
response.status
response.metadata
response.fair_use
response.outcome
response.partial_error
response.headers
response.raw
response.found
response.not_found
response.retry_after
```

`partial_error` matters for batch calls because MoltSets documents that a batch can return HTTP 200 with partial results plus an error block when a Fair Use cap is reached.

## Endpoint manifest

`moltsets_endpoint_manifest.json` is the source-of-truth snapshot used for coverage audits. It records:

- REST method
- full URL
- docs URL
- category
- documented input fields
- important plan/batch notes
- deprecated route replacements

## Reconstructed OpenAPI

`openapi.reconstructed.json` provides all current paths and documented request fields. Response bodies intentionally stay permissive because MoltSets tools return different result shapes and the docs do not expose a single downloadable official OpenAPI spec through the public index.

You can use this file to generate Go, TypeScript, Java, or other language clients while keeping the Python package as the behavioral reference.

## Tests

```bash
pytest -q
```

Coverage tests fail if one of the 20 current endpoint names in the SDK manifest has no client method.

## Recommended production integration

For a waterfall pipeline, keep MoltSets as a provider stage with this decision model:

```text
HTTP 200 + status=ok
    -> normalize provider result
    -> enforce your risk-score policy
    -> stop or continue based on field completeness

HTTP 200 + status=not_found
    -> clean miss
    -> next provider

HTTP 200 + partial_error
    -> keep returned results
    -> requeue only dropped items

429 with retry_after
    -> requeue for retry_after seconds

plan_restricted / insufficient_tokens / insufficient_phone_tokens
    -> provider configuration or billing route

invalid_input
    -> dead-letter / developer alert

internal_error / rate_limited
    -> short retry, then requeue
```

## Live documentation drift audit

Run this periodically in CI:

```bash
python scripts/audit_docs_coverage.py
```

It fetches `https://developer.moltsets.com/llms.txt`, extracts every documented `POST /tool_name` entry, compares that set with `moltsets_endpoint_manifest.json`, and exits non-zero if the live API-reference surface has changed.
