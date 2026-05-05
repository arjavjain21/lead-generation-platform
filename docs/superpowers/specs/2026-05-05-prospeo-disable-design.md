# Design: Disable Prospeo + Centralized Provider Config

**Date:** 2026-05-05
**Status:** Approved

---

## Context

Prospeo is the final fallback in the 4-tier enrichment cascade (Contacts DB → Blitz → BetterEnrich → Prospeo). It is a paid provider and the team wants to temporarily disable it. Additionally, the long-term goal is a single-source-of-truth config so any provider can be enabled/disabled from one place without touching cascade logic across multiple files.

---

## Phase 1: Immediate Disable — No Downtime, Low Risk

### Goal
Surgically remove Prospeo from all cascade chains. No new files, no architecture changes.

### Files to change

| File | Change |
|------|--------|
| `backend/enrichment/pipeline.py` | Remove `step_6_prospeo` and company fallback call |
| `backend/enrichment/routes.py` | Remove Prospeo from linkedin-only cascade, enhanced cascades, and `VALID_PROVIDERS` |
| `backend/enrichment/list_builder.py` | Remove Strategy 5 (Prospeo fallback) |
| `frontend/index.html` | Remove Prospeo from homepage description, data sources list, and help section |

### Approach
Delete Prospeo from if/elif chains only. All other logic remains intact. This is a pure removal change — no new code paths introduced.

---

## Phase 2: Centralized Provider Config

### Goal
One file (`providers.py`) that controls which enrichment providers are enabled. All cascade logic reads from it. Future provider toggles = edit one line.

### New File: `backend/enrichment/providers.py`

```python
# Single source of truth for which enrichment providers are enabled.
# Toggle a provider by changing its value here — no cascade logic changes needed.
ENABLED_PROVIDERS = {
    "contacts_db": True,
    "blitz": True,
    "better_enrich": True,
    "prospeo": False,   # ← one place to toggle
}
```

### Cascade Files Updated

| File | Change |
|------|--------|
| `pipeline.py` | Import `ENABLED_PROVIDERS`; wrap Prospeo call in `if ENABLED_PROVIDERS.get("prospeo"):` |
| `routes.py` | Same pattern for all Prospeo call sites |
| `list_builder.py` | Same pattern for all Prospeo call sites |

### New API Endpoint

- `GET /api/enrichment/providers` — returns `{"providers": ["contacts_db", "blitz", "better_enrich"]}` (enabled only)
- Used by frontend to dynamically render the data sources list (no more hardcoded UI strings)

### UI: Frontend reads from API

- `frontend/index.html` — fetch `/api/enrichment/providers` and render the list dynamically
- Removes the need to update frontend when providers change

---

## Cascade Reference (Updated)

```
Contacts DB (75 RPS, free)     ← enabled
Blitz API (25 RPS)             ← enabled
BetterEnrich (10 RPS, paid)    ← enabled
Prospeo (30 RPS, paid)         ← DISABLED (ENABLED_PROVIDERS["prospeo"] = False)
```

---

## Verification Plan

1. **Phase 1:** Run existing enrichment tests, verify no Prospeo calls in logs
2. **Phase 2:** Integration test — toggle `prospeo: True`, confirm Prospeo is called; toggle `False`, confirm it's skipped
3. **End-to-end:** Run a small enrichment job, verify results return without Prospeo
4. **UI:** Verify homepage data sources list matches enabled providers from API

---

## Out of Scope

- Adding new providers (future work)
- Per-user provider toggles (future work)
- Environment variable overrides (future work)
- Database-backed toggle (future work)