# Contacts DB (leadsdatabase.cc) — Read-Path Bug Report

**From:** Lead Generation Platform team (`listbuilding.eagleinfoservice.com`)
**To:** Contacts DB team (`leadsdatabase.cc`)
**Date:** 2026-07-05
**Severity:** High — production cascade degraded, all reads falling through to paid providers

---

## TL;DR

Writes to `/v1/persons/upsert` succeed (`200 {"success":true, "action":"created", "person_id":"..."}`), but **the records we just wrote cannot be found by any read endpoint**. In addition, three production-read endpoints (`/v1/person/by-email`, `/v1/decision-makers/search`, `/v1/company/contacts/enriched`) are currently returning `500 Internal Server Error` for every request. We need you to confirm whether writes are actually persisting and to restore the read endpoints.

---

## Our setup (so you can find our traffic)

| | |
|---|---|
| Base URL | `https://leadsdatabase.cc` |
| Auth header | `Authorization: Bearer <CONTACTS_API_TOKEN>` (token starts with `eSKdxjQo…Tyk=`, 43 chars — same token as always; we can re-share privately if helpful) |
| Client user-agent | `httpx 0.x` from Python 3.12, gunicorn workers on `listbuilding.eagleinfoservice.com` (server IP `vps-7eba81a6`) |
| Write rate | ≤ 75 RPS (we honour your limiter — see `_acquire_upsert_rate_limit()` in our client) |
| Write volume (last 30d) | ~10,954 successful `INSERT` calls accepted by your API |
| Use case | (1) Write-back of every enriched contact we produce. (2) Read-as-provider: your `/v1/company/contacts/enriched` is the **first free lookup** in our enrichment cascade. When it fails, every row falls through to paid providers (Blitz / WizLeads / BetterEnrich / Prospeo). |

---

## Test 1 — Write succeeds, lookup-by-email returns "not found"

Reproduce (token redacted — replace `$TOK`):

```bash
TOK="<our existing Bearer token>"

# 1a. Write a record
curl -s -X POST "https://leadsdatabase.cc/v1/persons/upsert" \
  -H "Authorization: Bearer $TOK" \
  -H "Content-Type: application/json" \
  -d '{"email":"report-probe@example.org","domain":"example.org","company_domain":"example.org","full_name":"Report Probe","title":"Audit"}'

# 1b. Wait a few seconds, then look the same email up
sleep 3
curl -s -X GET "https://leadsdatabase.cc/v1/person/lookup?email=report-probe@example.org" \
  -H "Authorization: Bearer $TOK"
```

Observed (today, 2026-07-05 ~07:50 UTC):

```http
POST /v1/persons/upsert                                    → 200
{"success":true,
 "person_id":"983607bd-ddd7-49e9-bcb8-919febcae82b",
 "company_id":"1b467e67-de8c-4c3b-bc53-00dc87d5d118",
 "email_id":"21ddd33f-2b0d-40f4-be5b-648370d6b6db",
 "action":"created",
 "message":"Person record created successfully"}

GET  /v1/person/lookup?email=report-probe@example.org      → 200
{"found":false,
 "email":"report-probe@example.org",
 "message":"No person found with this email address"}
```

**Expected:** The record we just upserted (we got `person_id` + `email_id` back) should be findable by `lookup`.

### Variations also tried

```bash
# 1c. Try fetching the person_id we just got back from the upsert
curl -s -X GET "https://leadsdatabase.cc/v1/person/983607bd-ddd7-49e9-bcb8-919febcae82b" \
  -H "Authorization: Bearer $TOK"
# → 404 {"detail":"Not Found"}

# 1d. Try the dedicated by-email endpoint
curl -s -X GET "https://leadsdatabase.cc/v1/person/by-email?email=report-probe@example.org" \
  -H "Authorization: Bearer $TOK"
# → 500 Internal Server Error
```

A second probe (`final-audit-test@example.org`) and a third (`direct-probe@example.org`) showed the **same pattern**: upsert returns 200 with `action:"created"`, then `lookup` says `found:false`. The `person_id` returned by the upsert 404s when fetched directly.

**Note:** `lookup` does work for *some* emails. e.g. `GET /v1/person/lookup?email=test@example.com` returns `{"found":true, "person":{...}}`. So the endpoint is up; it just can't see the records we wrote.

---

## Test 2 — `/v1/company/contacts/enriched` 500s for every domain

```bash
for D in stripe.com deloitte.com godaddy.com salesforce.com hubspot.com thequietus.com; do
  printf "%-20s " "$D"
  curl -s -o /dev/null -w "%{http_code}\n" -X GET \
    "https://leadsdatabase.cc/v1/company/contacts/enriched?domain=$D" \
    -H "Authorization: Bearer $TOK"
done
```

Observed (2026-07-05):

```
stripe.com           500
deloitte.com         500
godaddy.com          500
salesforce.com       500
hubspot.com          500
thequietus.com       500
```

`500 Internal Server Error` (empty body) for **every** domain, including ones we know you have data for.

### Other 500-ing endpoints

| Endpoint | Status |
|---|---|
| `GET /v1/person/by-email?email=...` | 500 |
| `GET /v1/decision-makers/search?domain=...` | 500 |
| `GET /v1/decision-makers/by-linkedin?...` | not tested (cascade path doesn't reach it) |

### Working endpoints (sanity check)

| Endpoint | Status |
|---|---|
| `GET /health` | 200 |
| `GET /v1/analytics/contact-types` | 200 — reports **5,080,204 total contacts, 4,501,915 typed, 1,614,300 enriched** |
| `GET /v1/companies/search` | 200 — reports **1,821,642 companies** |
| `GET /v1/person/lookup?email=test@example.com` | 200 `found:true` |
| `POST /v1/persons/upsert` | 200 (see Test 1) |

So the service is up and the data store is populated, but specific read paths fail.

---

## Test 3 — Real-world: 60% of records we wrote on Jul 4 are missing from `lookup`

We ran enrichment **job `7a6566ec-2531-4939-ae86-0c712aab5cfd`** on 2026-07-04 23:01–23:36 UTC. The output CSV has **790 emails**. Our write-back log shows:

```
INFO  enrichment.routes: contacts_writer v2 sync done for job
      7a6566ec-2531-4939-ae86-0c712aab5cfd:
      {'inserted': 715, 'updated': 0, 'skipped': 75,
       'failed': 0, 'queued_for_retry': 0, 'no_data': 0, 'total': 790}
```

So your API accepted **715 inserts + 75 already-exists** (all 200 OK, `action:"created"` or duplicate-key).

We then probed 20 random emails from that same CSV via `GET /v1/person/lookup?email=...`:

| Status | Count | Sample emails |
|---|---|---|
| ✅ `found:true` | 8 / 20 | `gwells@forthepeople.com`, `armstronga@seminolestate.edu`, `aaron.frankel@cybercoders.com`, `rpiantosi@atlassearchllc.com`, `lindsay.drebenstedt@emilyprogram.com`, `m_barlow@synectics.com`, `stevej@bachrachgroup.com`, `joycet@howardsloan.com` |
| ❌ `found:false` | 12 / 20 | `sophia.brauer@larsonmaddox.com`, `sunpreet.bhatia@cetera.com`, `marc@careerslaunch.com`, `miglesias@burr.com`, `jessica@burnettspecialists.com`, `mwhittier@cacgroc.org`, `spencer.skeen@ogletree.com`, `dbeaty@voa.org`, `isaac@deloitte.com`, `richard.cox@huschblackwell.com`, `rebecca.gray@huschblackwell.com`, `dave.muller@tandymgroup.com` |

**Reproduce any of the missing ones:**

```bash
for E in sophia.brauer@larsonmaddox.com isaac@deloitte.com \
         spencer.skeen@ogletree.com rebecca.gray@huschblackwell.com; do
  echo "=== $E ==="
  curl -s -X GET "https://leadsdatabase.cc/v1/person/lookup?email=$E" \
    -H "Authorization: Bearer $TOK"
  echo
done
```

**Expected:** Every one of those emails (and the other ~700 from that CSV) should return `found:true`, because your API returned 200 + `action:"created"` for each of them on 2026-07-04 around 23:34–23:36 UTC.

The CSV (790 rows) is available at:
```
/var/www/lead-generation-platform/backend/data/outputs/7a6566ec-2531-4939-ae86-0c712aab5cfd.csv
```
Happy to share if you want the full set.

---

## Test 4 — Production logs: `/v1/company/contacts/enriched` has been failing all of Jul 4–5

Sample log lines from our gunicorn journal (full command below if you want the raw stream):

```
Jul 04 07:13:00 vps-7eba81a6 gunicorn[3419441]: ERROR:enrichment.contacts_client: Contacts DB
       https://leadsdatabase.cc/v1/company/contacts/enriched returned 500, exhausted retries
Jul 04 07:13:02 vps-7eba81a6 gunicorn[3419458]: ERROR:enrichment.contacts_client: Contacts DB
       https://leadsdatabase.cc/v1/company/contacts/enriched returned 500, exhausted retries
Jul 04 07:14:10 vps-7eba81a6 gunicorn[3419459]: ERROR:enrichment.contacts_client: Contacts DB
       https://leadsdatabase.cc/v1/company/contacts/enriched returned 500, exhausted retries
... (continued all day Jul 4 and into Jul 5)
```

**Total occurrences in last 30 days: 615**, **all clustered on 2026-07-04 and 2026-07-05**. The endpoint was working before July 4 — this looks like a recent regression on your side.

Reproduce the count from our side:

```bash
journalctl -u lead-generation-platform.service --since "30 days ago" --no-pager \
  | grep -c "Contacts DB.*returned 500"
# → 615
```

---

## Test 5 — `/v1/person/by-name-and-domain` requires `name`, not `full_name`

Minor, but worth flagging while you're in there. OpenAPI declares the param as `name`, but the conventional shape (and what other endpoints use) is `full_name`. We worked around it, but wanted to surface the inconsistency.

```bash
# Returns 422 — wants "name" not "full_name"
curl -s "https://leadsdatabase.cc/v1/person/by-name-and-domain?full_name=Test&domain=example.org" \
  -H "Authorization: Bearer $TOK"
```

---

## What we've ruled out on our side

To save you the back-and-forth:

1. **Token is valid.** `GET /v1/analytics/contact-types` and `GET /v1/companies/search` both return 200 with the same Bearer token. We're not auth-failing.
2. **We're not over the rate limit.** Our client calls `_acquire_upsert_rate_limit()` before every upsert (75 RPS cap). The 500s come back instantly — they're not preceeded by 429s.
3. **We're not sending malformed bodies.** The exact payload in Test 1 is a flat object with `email`, `domain`, `company_domain`, `full_name`, `title`. The upsert returns 200 with a real `person_id`. The body is fine.
4. **No eventual-consistency window we can wait out.** We waited 3 seconds between write and read in Test 1 — still `found:false`. We have records we wrote **days ago** (job `7a6566ec`, Jul 4) that still aren't readable via `lookup`.
5. **Our write path isn't silently failing.** Outbox table is empty, gunicorn logs show only 200/`action:created` responses. There are zero LoudFailure or outbox-retry entries for the test jobs.

---

## Our hypotheses (you know your system better — please sanity-check)

In rough order of suspicion:

1. **Write and read paths hit different stores.** Upsert persists to one table/index, `lookup` and `/v1/company/contacts/enriched` query another. The `person_id` returned by upsert 404s on direct fetch — that strongly suggests the read path can't see the row at all.
2. **Indexing pipeline stalled.** `lookup` works for *old* records (`test@example.com` is found) but not for records written in the last few days. If writes go to an OLTP table and reads go through a search index (e.g. Elasticsearch / materialised view), the indexer may have stopped or fallen behind.
3. **Recent deploy regression.** The 500s on `/v1/company/contacts/enriched` started on Jul 4 — the same endpoint worked before. If you shipped something that day, that's the prime suspect.
4. **Schema migration gone wrong.** The 500s are unhandled (no JSON error body, just `Internal Server Error`). If the endpoint expects a column that no longer exists, that pattern fits.

---

## What we need from you

In priority order:

1. **Confirm whether the 715 records we wrote for job `7a6566ec` (Jul 4, 23:34–23:36 UTC) are in your database.** If yes, why can't `lookup` find 12 of the 20 we sampled? If no, why did upsert return `action:"created"` for them?
2. **Restore `/v1/company/contacts/enriched`, `/v1/person/by-email`, `/v1/decision-makers/search`** — all return 500 right now. We need at least the first one for our cascade.
3. **Tell us which read endpoint you'd recommend as the canonical "does this email exist / fetch me this person" call**, given `lookup` is unreliable for fresh rows.
4. **If there's indexing lag, what's the expected SLA** for a freshly-upserted row to become readable? We'll add appropriate waits/retries on our side.
5. **Status page / incident note** if this is a known issue, so we know when it's resolved.

---

## How to reach us

Reply on this thread, or ping the Lead Generation Platform team. We can provide:
- Full CSV of any specific job's writes (790 rows for `7a6566ec`, more available)
- Specific `person_id` / `email_id` / `company_id` triples returned by upserts
- Coordinated timestamps if you want to cross-reference your logs

Thanks!

---

## Appendix — full reproduction script

Save as `probe.sh` and run with `TOK` set:

```bash
#!/usr/bin/env bash
set -u
: "${TOK:?Set TOK to your Contacts DB bearer token}"
BASE="https://leadsdatabase.cc"
AUTH="Authorization: Bearer $TOK"

echo "===== Test 1: write-then-read ====="
EMAIL="report-probe-$(date +%s)@example.org"
echo "Writing $EMAIL ..."
UPSERT_RESP=$(curl -s -X POST "$BASE/v1/persons/upsert" \
  -H "$AUTH" -H "Content-Type: application/json" \
  -d "{\"email\":\"$EMAIL\",\"domain\":\"example.org\",\"company_domain\":\"example.org\",\"full_name\":\"Probe\",\"title\":\"Audit\"}")
echo "  upsert: $UPSERT_RESP"
PID=$(echo "$UPSERT_RESP" | python3 -c "import json,sys;print(json.load(sys.stdin).get('person_id',''))")
sleep 3
echo "  lookup by email:"
curl -s -X GET "$BASE/v1/person/lookup?email=$EMAIL" -H "$AUTH"; echo
echo "  fetch by person_id ($PID):"
curl -s -o /dev/null -w "  http %{http_code}\n" -X GET "$BASE/v1/person/$PID" -H "$AUTH"
echo "  by-email endpoint:"
curl -s -o /dev/null -w "  http %{http_code}\n" -X GET "$BASE/v1/person/by-email?email=$EMAIL" -H "$AUTH"

echo
echo "===== Test 2: company/contacts/enriched across domains ====="
for D in stripe.com deloitte.com godaddy.com salesforce.com hubspot.com thequietus.com; do
  printf "  %-20s " "$D"
  curl -s -o /dev/null -w "http %{http_code}\n" -X GET "$BASE/v1/company/contacts/enriched?domain=$D" -H "$AUTH"
done

echo
echo "===== Test 3: known-missing emails from job 7a6566ec (Jul 4) ====="
for E in sophia.brauer@larsonmaddox.com isaac@deloitte.com \
         spencer.skeen@ogletree.com rebecca.gray@huschblackwell.com \
         dbeaty@voa.org; do
  printf "  %-50s " "$E"
  curl -s "$BASE/v1/person/lookup?email=$E" -H "$AUTH" \
    | python3 -c "import json,sys;d=json.load(sys.stdin);print('found='+str(d.get('found')))"
done

echo
echo "===== Sanity: working endpoints ====="
printf "  %-50s " "/health"
curl -s -o /dev/null -w "http %{http_code}\n" "$BASE/health" -H "$AUTH"
printf "  %-50s " "/v1/analytics/contact-types"
curl -s -o /dev/null -w "http %{http_code}\n" "$BASE/v1/analytics/contact-types" -H "$AUTH"
printf "  %-50s " "/v1/person/lookup?email=test@example.com"
curl -s "$BASE/v1/person/lookup?email=test@example.com" -H "$AUTH" \
  | python3 -c "import json,sys;d=json.load(sys.stdin);print('found='+str(d.get('found')))"
```
