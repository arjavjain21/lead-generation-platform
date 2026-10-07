# BetterEnrich Billing Review & Reconciliation Request

**Date:** 2026-09-27
**From:** Eagle Info Service — ListBuilding platform (listbuilding.eagleinfoservice.com)
**API key (prefix):** `870823…` (full key available on request through a secure channel)
**Subject:** 7 × $240 charges in the last 14 days — request for itemization, usage export, and credit for the ongoing auth outage

---

## 1. Summary of the situation

Our card was charged **$240 seven times in the last 14 days** (~09-13 → 09-27), totaling **$1,680**.
Over the same window we made **734,412 API requests** to your endpoints, of which **683,344 returned
HTTP 200** and delivered **~228,000 email results** (internally ledgered at receipt time).

We are not disputing that heavy usage occurred — it did, and our own records confirm it. We are asking
you to reconcile the charges against that usage, because three things do not add up:

1. **Seven identical $240 charges in 14 days.** Your Professional plan is $240/month (10,000 credits).
   If these are subscription charges rather than one-time credit packs, several appear to be duplicates.
   If they are auto-recharges of 10K-credit packs, the implied consumption (~70,000 credits) does not
   match any published rate applied to our delivered volume (see §5).
2. **Your API has been hard-failing our key since 2026-09-26 08:19:32 UTC** — every request returns
   `401 {"message":"Authorization information is invalid"}` (46,700+ requests and counting, zero
   successes). Any charge landing on or after that timestamp paid for **no service at all**.
3. **Your own docs define 403 as "Insufficient credits" and 401 as invalid auth.** Our key was
   returning 200s with (to our knowledge) remaining credits at 08:19 and 401s minutes later, with no
   change to the key on our side (unchanged since 2026-09-07).

---

## 2. What we are requesting

1. **Itemization of all 7 charges**: date/time (UTC), what each charge purchased (subscription vs.
   one-time credit pack), credits added, and invoice/receipt IDs.
2. **A usage export for our API key covering 2026-09-01 → present**: per-day billed hits, credits
   consumed per endpoint (especially `find-work-email-low-cost-v3-alt` and `find-company-email`),
   and the credit cost per unit you apply to each endpoint.
3. **Confirmation that error responses are never billed**: HTTP 429 (4,284 in window), HTTP 401
   (46,727 in window), and the 403 period of 09-07 → 09-10 (367,316 requests, all rejected).
4. **Credit or refund for any charge timestamped after 2026-09-26 08:20 UTC**, plus an explanation
   of why the key became invalid at that moment (suspended account? failed auto-recharge? key
   rotation on your side?) and what is needed to restore service.
5. **Your duplicate-hit policy**: 64,107 of our requests returned emails we had already received
   earlier in the window (our own re-enrichment traffic — we accept responsibility and are fixing
   client-side dedupe). If credits are charged per returning request rather than per unique result,
   we ask whether idempotency-window credits can be applied.

---

## 3. Our usage record (2026-09-13 → 2026-09-27 05:35 UTC)

All numbers from our independent request-level observability (every outbound HTTP call and every
email extracted from a response body is logged at receipt time, before any processing).

### 3.1 Requests by endpoint and status

| Endpoint | 200s | 401s | 429s | Total |
|---|---:|---:|---:|---:|
| `POST /api/v1/find-company-email` | 405,299 | 501 | 4,146 | 409,997 |
| `POST /api/v1/find-work-email-low-cost-v3-alt` | 275,182 | 46,226 | 138 | 321,552 |
| `POST /api/v1/find-email-from-facebook-page` | 2,863 | 0 | 0 | 2,863 |
| **Total** | **683,344** | **46,727** | **4,284** | **734,412** |

### 3.2 Daily breakdown

| Date (UTC) | 200s | 401s | 429s | Total | Emails delivered |
|---|---:|---:|---:|---:|---:|
| 09-13 | 65,908 | 0 | 993 | 66,901 | ~24,300* |
| 09-14 | 88,140 | 0 | 1,663 | 89,859 | 1,506* |
| 09-15 | 57,124 | 0 | 235 | 57,359 | 14,081 |
| 09-16 | 5,521 | 0 | 0 | 5,521 | 1,958 |
| 09-17 | 44,363 | 0 | 53 | 44,416 | 16,675 |
| 09-18 | 64,228 | 0 | 188 | 64,416 | 23,515 |
| 09-19 | 40,332 | 0 | 0 | 40,333 | 16,599 |
| 09-21 | 46,283 | 0 | 13 | 46,296 | 12,800 |
| 09-22 | 6,528 | 0 | 0 | 6,528 | 805 |
| 09-23 | 61,064 | 0 | 48 | 61,112 | 22,792 |
| 09-24 | 74,710 | 0 | 58 | 74,768 | 49,080 |
| 09-25 | 87,326 | 0 | 555 | 87,881 | 46,202 |
| 09-26 | 41,817 | 34,702 | 478 | 76,997 | 21,798 (all before 08:19) |
| 09-27 (to 05:35) | 0 | 12,018 | 0 | 12,018 | 0 |

\* Our email ledger began logging 2026-09-14 10:25 UTC; 09-13 is estimated from the window's
emails-per-successful-call ratio (0.369) and 09-14 is partial-day.

**Emails delivered, total:** 227,811 ledgered rows; **163,704 unique email addresses**
(64,107 repeat deliveries caused by our own duplicate requests).

### 3.3 Rate profile

Steady-state ~3,650 requests/hour on peak days (~88K/day). Our client throttles per-process to the
documented 5 req/s on the shared v3/company/facebook budget, but we run multiple server processes,
so our aggregate occasionally exceeds 10 req/s — that is the likely cause of the 4,284 × 429s.
We are correcting this. Please confirm 429-rejected requests are never charged.

---

## 4. Service-incident timeline (our records)

| When (UTC) | Event |
|---|---|
| 09-07 00:00 → 09-10 ~20:00 | **403 storm**: 367,316 requests across all 3 endpoints, every one rejected `403` (your docs: "Insufficient credits"). Zero service delivered for 4 days. Our key and code were unchanged. |
| 09-11 04:00 | 200s resume with no change on our side (consistent with a top-up/plan activation on your side ~09-11). |
| 09-13 → 09-25 | Continuous heavy but clean operation (see §3.2). |
| **09-26 08:19:32** | **Last successful response ever received.** Last email delivered 08:19:23. |
| 09-26 08:20 → present (09-27 05:35+) | **Every request returns 401 "Authorization information is invalid"** — 46,727 and counting. No key/config change on our side (key unchanged since 09-07; no code deploy since 09-25). |

---

## 5. Reconciliation analysis (why we need your itemization)

Applying your published pricing to our delivered volume:

- Published: work-email waterfall = **1.25 credits per verified hit**, Professional pack =
  10,000 credits for $240 ($0.024/credit) → ≈ $0.03 per delivered email.
- Our delivered volume: 228K email results (164K unique). At 1.25 cr/hit that implies
  **205,000–285,000 credits ≈ $4,900–$6,840** — far more than the $1,680 charged.
- Conversely, $1,680 for 228K emails implies an effective **0.28–0.34 credits per hit**.

Both can be true only if the `low-cost` endpoints are metered at a substantially lower rate than the
1.25-credit waterfall (which the endpoint name suggests), or if only a verified subset of results is
billed. We cannot verify either from outside your system — hence the request for the per-endpoint
unit price and the usage export (§2, items 1–2). If your records instead show ~70,000 credits
consumed at waterfall rates, the charge count and our volume cannot both be correct and we need to
understand what was actually metered.

---

## 6. Questions (numbered for your reply)

1. What exactly did each of the 7 × $240 charges purchase (subscription vs pack, credits, timestamps)?
2. What is the per-unit credit cost of `/api/v1/find-work-email-low-cost-v3-alt` and
   `/api/v1/find-company-email`, and is billing per request, per returned email, or per verified email?
3. Are 401/403/429 responses ever charged? Please confirm zero credits were consumed during
   09-07→09-10 (403 period) and 09-26 08:20→present (401 period).
4. Why did our key start returning 401 at 2026-09-26 08:20 UTC while returning 200s at 08:19?
   Was the account suspended (e.g., failed auto-recharge), or was the key invalidated on your side?
5. Will you credit/refund any charge timestamped after 2026-09-26 08:20 UTC, during which we
   received zero service?
6. Do you bill repeated identical lookups (same person/domain) multiple times? Is there an
   idempotency window?
7. Can you provide the usage export described in §2.2?

---

*Prepared from our internal request logs (`provider_call_log`, `provider_email_ledger`), which record
every outbound HTTP call and every email parsed from your responses at receipt time. Raw extracts
available on request.*
