# MoltSets Library Workflow Notes

Snapshot date: **2026-10-14**

Sources:

- https://moltsets.com/library
- https://moltsets.com/library/all-skills/
- https://moltsets.com/library/skills/email-finder-find-anyones-email-address/
- https://moltsets.com/library/skills/build-target-list/
- https://support.moltsets.com/en/articles/16036734-searching-for-people
- https://support.moltsets.com/en/articles/16038163-getting-valid-emails
- https://support.moltsets.com/en/articles/16038481-getting-carrier-verified-mobile-phones

## What the library is

The MoltSets library is an orchestration layer made of pre-built Claude skills. It is not a second REST API surface. At this snapshot, the public library page showed **75 skills**. Many combine MoltSets with external systems such as HubSpot, Attio, Clay, RB2B, Ahrefs, Google Search Console, LinkedIn, Apify, CSV/Sheets/Excel, and Claude Chat.

The SDK therefore keeps two boundaries:

1. `MoltSetsClient` and `AsyncMoltSetsClient` wrap the documented MoltSets REST endpoints.
2. `moltsets.recipes` implements high-value MoltSets-native orchestration patterns without pretending each library skill is a new endpoint.

## Library patterns encoded in `moltsets.recipes`

### `find_best_email`

Implements the useful part of the library's Email Finder chain while only returning endpoint-confirmed addresses:

1. LinkedIn URL -> `linkedin_to_best_email`
2. If that misses -> `reverse_linkedin_lookup`
3. Name + company/domain -> `search_business_email_by_name`
4. If needed -> `search_linkedin_profile`
5. If the profile resolves a LinkedIn URL -> `linkedin_to_best_email`

The library describes pattern guessing as a last resort. This SDK intentionally does **not** fabricate candidate email addresses inside the default recipe. A caller can add such a stage separately if their compliance and verification policy permits it.

### `probe_search_people`

Runs `search_people(limit=1)` and returns the sample plus `results.total`.

This is deliberate. Some MoltSets library skill pages currently say `count_only: true` can size `search_people`. MoltSets' support article **Searching for People** is more explicit and states that `count_only` is only available on `search_linkedin_profile`, while the recommended cheap probe for `search_people` is `limit: 1`.

The SDK follows the stricter support documentation.

### `enrich_linkedin_profile`

Combines:

- `reverse_linkedin_lookup` for identity, current role/company, firmographics, and known emails
- `linkedin_to_mobile_phone` for the optional carrier-verified mobile number

The valid-email support documentation recommends checking reverse lookup results before making redundant email calls because reverse LinkedIn lookup can already return business and personal emails.

### `chunked_mobile_lookup`

Uses `linkedin_to_mobile_phone` with native batches of up to 100 LinkedIn URLs. The support documentation explicitly recommends batching for request-efficiency and rate-limit conservation.

## Risk-score policy

MoltSets documents grades as:

- `A`: validated deliverable
- `B`: known send with no bounce recorded
- `C`: catch-all / uncertain
- `D`: hard negative, such as bounce, complaint, or spam-trap signal
- `F`: no signal either way

The recipe default is `A/B/C/F`, with `D` excluded. Results are not silently discarded. `EmailRecipeResult.accepted` tells the caller whether the returned address passed the configured policy.

For a stricter outbound policy, pass `accepted_risk_scores=("A", "B")`.

## Recommended internal architecture

```text
Your provider waterfall
        |
        +-- Provider A
        +-- Provider B
        +-- MoltSets recipe layer
        |       +-- raw 20-endpoint SDK
        |       +-- risk policy
        |       +-- search probe / pagination
        |       +-- enrichment fallbacks
        +-- Provider D
```

Keep provider-specific fallback logic inside this package. Your central waterfall should consume normalized outcomes such as found, not found, retry later, and hard error rather than knowing every MoltSets endpoint.
