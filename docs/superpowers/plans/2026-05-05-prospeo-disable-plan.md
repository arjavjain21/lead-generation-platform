# Prospeo Disable + Centralized Provider Config Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Disable Prospeo in all enrichment cascades immediately (Phase 1), then build a centralized provider config so any provider can be toggled from one place going forward (Phase 2).

**Architecture:** Phase 1 is a pure surgical removal — delete Prospeo from if/elif chains in 4 files. Phase 2 introduces `providers.py` as single source of truth, all cascade files check `ENABLED_PROVIDERS`, and a new `GET /api/enrichment/providers` endpoint powers the frontend dynamically.

**Tech Stack:** Python (FastAPI), React (static HTML/JS)

---

## Phase 1 — Immediate Disable (No Downtime)

### Task 1: Remove Prospeo from `pipeline.py`

**Files:**
- Modify: `backend/enrichment/pipeline.py:333-349`
- Modify: `backend/enrichment/pipeline.py:528-546`

- [ ] **Step 1: Remove Step 6 (Prospeo) from `_resolve_email_for_person`**

File: `backend/enrichment/pipeline.py`, lines 333-349

Delete this entire block (including the surrounding comment `# Step 6: Prospeo person enrichment`):

```python
        # Step 6: Prospeo person enrichment
        if not _should_skip_provider("prospeo", force_provider):
            try:
                result = await prospeo_client.enrich_person(
                    blitz_client_inst,
                    linkedin_url=linkedin_url,
                    full_name=full_name,
                    first_name=first_name,
                    last_name=last_name,
                    company_website=domain,
                )
                # Use helper to extract email from Prospeo result
                email = prospeo_client.extract_email_from_prospeo(result)
                if email:
                    return email, SOURCE_PROSPEO_PERSON
            except Exception as e:
                logger.warning("Prospeo person enrichment failed for %s / %s: %s", full_name, domain, e)
```

Note: Do NOT delete the `import prospeo_client` at line 34 — it may be used elsewhere. Keep it for now.

- [ ] **Step 2: Remove Prospeo company fallback from `_enrich_domain`**

File: `backend/enrichment/pipeline.py`, lines 528-546

Delete this entire block (including the `# Final fallback: try Prospeo for company enrichment` comment and the try/except):

```python
        # Final fallback: try Prospeo for company enrichment
        if not _should_skip_provider("prospeo", force_provider):
            try:
                prospeo_result = await prospeo_client.enrich_company(
                    blitz_http,
                    company_website=domain,
                )
                if prospeo_result:
                    company_data = prospeo_result.get("company", {})
                    if company_data:
                        logger.info("Prospeo found company data for %s", domain)
                        return [_prospeo_company_row(
                            base_row,
                            company_linkedin_url,
                            company_data,
                            SOURCE_PROSPEO,
                        )]
            except Exception as e:
                logger.debug("Prospeo company lookup failed for %s: %s", domain, e)
```

- [ ] **Step 3: Remove `SOURCE_PROSPEO` and `SOURCE_PROSPEO_PERSON` constants**

File: `backend/enrichment/pipeline.py`, lines 117-118

Delete:
```python
SOURCE_PROSPEO = "prospeo"                               # Email/data from Prospeo
SOURCE_PROSPEO_PERSON = "prospeo_person"                  # Person email from Prospeo
```

Also delete the helper function `_prospeo_company_row` at lines 212-225.

- [ ] **Step 4: Verify `_normalize_source` still works without prospeo**

File: `backend/enrichment/pipeline.py`, lines 64-81

The `_normalize_source` function still references prospeo. Update it to remove the `prospeo` branch (lines 79-80):

```python
def _normalize_source(source: str) -> str:
    """Map raw source value to provider group."""
    if source.startswith("contacts_db"):
        return "contacts_db"
    elif source.startswith("blitz"):
        return "blitz"
    elif source.startswith("better_enrich"):
        return "better_enrich"
    elif source.startswith("prospeo"):
        return "prospeo"   # Keep this — old rows may still have prospeo as source
    return source
```

Keep the prospeo branch in `_normalize_source` — it handles historical data gracefully.

- [ ] **Step 5: Run existing enrichment tests**

Run: `cd /var/www/lead-generation-platform/backend && source venv/bin/activate && python -m pytest enrichment/tests/ -v --tb=short 2>&1 | head -80`

Expected: All existing tests pass.

- [ ] **Step 6: Commit pipeline.py changes**

```bash
git add backend/enrichment/pipeline.py
git commit -m "chore: disable Prospeo in pipeline.py (Phase 1)

Remove Prospeo from:
- _resolve_email_for_person: Step 6 cascade
- _enrich_domain: company fallback
- SOURCE_PROSPEO and SOURCE_PROSPEO_PERSON constants
- _prospeo_company_row helper

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 2: Remove Prospeo from `routes.py`

**Files:**
- Modify: `backend/enrichment/routes.py:103`
- Modify: `backend/enrichment/routes.py:849-886`
- Modify: `backend/enrichment/routes.py:975-990`
- Modify: `backend/enrichment/routes.py:1182-1223`

- [ ] **Step 1: Remove `prospeo` from `VALID_PROVIDERS`**

File: `backend/enrichment/routes.py`, line 103

Change:
```python
VALID_PROVIDERS = frozenset({"contacts_db", "blitz", "better_enrich", "prospeo"})
```
To:
```python
VALID_PROVIDERS = frozenset({"contacts_db", "blitz", "better_enrich"})
```

- [ ] **Step 2: Remove Step 4 (Prospeo) from linkedin-only cascade**

File: `backend/enrichment/routes.py`, lines 849-886

Delete this entire block:
```python
            # Step 4: Final fallback - Try Prospeo (works with just linkedin_url)
            # Prospeo can work with only linkedin_url, so run this regardless of domain
            if not contacts or not any(c.get("email") for c in contacts):
                try:
                    prospeo_result = await prospeo_client.enrich_person(
                        blitz_http,
                        linkedin_url=req.linkedin_url,
                        company_website=domain if domain else None,
                    )
                    if prospeo_result:
                        person_data = prospeo_result.get("person", {})
                        email = prospeo_client.extract_email_from_prospeo(prospeo_result)
                        if person_data:
                            if not contacts:
                                contacts.append({
                                    ...
                                    "email_source": "prospeo",
                                })
                            else:
                                for contact in contacts:
                                    if not contact.get("email"):
                                        contact["email"] = email
                                        contact["email_source"] = "prospeo"
                                        break
                            sources["contacts"] = "prospeo"
                            sources["emails"] = "prospeo"
                            logger.info("Prospeo found person data for %s", req.linkedin_url)
                except Exception as e:
                    logger.debug("Prospeo LinkedIn lookup failed: %s", e)
```

- [ ] **Step 3: Remove Step 3 (Prospeo) from domain-only cascade**

File: `backend/enrichment/routes.py`, lines 975-990

Delete:
```python
            # Step 3: If still no contacts found, try Prospeo as final fallback
            # Skip if force_provider is set and it's not "prospeo"
            if not contacts and not _should_skip_provider("prospeo", req.force_provider):
                try:
                    prospeo_result = await prospeo_client.enrich_company(
                        blitz_http,
                        company_website=domain,
                    )
                    if prospeo_result:
                        company_data = prospeo_result.get("company", {})
                        if company_data:
                            sources["contacts"] = "prospeo"
                            sources["emails"] = "prospeo"
                            logger.info("Prospeo found company data for %s", domain)
                except Exception as e:
                    logger.debug("Prospeo company lookup failed for %s: %s", domain, e)
```

- [ ] **Step 4: Remove final Prospeo fallback from enhanced mode**

File: `backend/enrichment/routes.py`, lines 1182-1223

Delete:
```python
            # Final fallback: Try Prospeo if no contacts or no emails found
            # Skip if force_provider is set and it's not "prospeo"
            if (not contacts or not any(c.get("email") for c in contacts)) and not _should_skip_provider("prospeo", req.force_provider):
                    try:
                        # Try Prospeo with available data
                        prospeo_result = await prospeo_client.enrich_person(
                            blitz_http,
                            linkedin_url=req.linkedin_url if req.linkedin_url else None,
                            full_name=full_name if full_name else None,
                            company_website=domain if domain else None,
                        )
                        if prospeo_result:
                            person_data = prospeo_result.get("person", {})
                            email = prospeo_client.extract_email_from_prospeo(prospeo_result)
                            if person_data:
                                if not contacts:
                                    contacts.append({
                                        ...
                                        "email_source": "prospeo",
                                    })
                                else:
                                    for contact in contacts:
                                        if not contact.get("email"):
                                            contact["email"] = email
                                            contact["email_source"] = "prospeo"
                                            break

                                sources["contacts"] = "prospeo"
                                sources["emails"] = "prospeo"
                                logger.info("Prospeo found person data for %s", full_name or domain)
                    except Exception as e:
                        logger.debug("Prospeo person lookup failed: %s", e)
```

- [ ] **Step 5: Remove `from . import prospeo_client` import**

File: `backend/enrichment/routes.py`, line 39

Delete:
```python
from . import prospeo_client
```

- [ ] **Step 6: Update `force_provider` docstrings to remove "prospeo" reference**

Search for all occurrences of `"contacts_db", "blitz", "better_enrich", "prospeo"` in docstrings and update to `"contacts_db", "blitz", "better_enrich"`.

File: `backend/enrichment/routes.py`, lines 386-397 and 394-396

Update:
- Line 386: `# Force a specific provider: "contacts_db", "blitz", "better_enrich", "prospeo"` → remove `", "prospeo"`
- Line 395: same
- Line 641: same
- Line 1369: same

- [ ] **Step 7: Run tests**

Run: `cd /var/www/lead-generation-platform/backend && source venv/bin/activate && python -m pytest enrichment/tests/ -v --tb=short 2>&1 | head -80`

Expected: All existing tests pass.

- [ ] **Step 8: Commit routes.py changes**

```bash
git add backend/enrichment/routes.py
git commit -m "chore: disable Prospeo in routes.py (Phase 1)

Remove Prospeo from:
- VALID_PROVIDERS frozenset
- linkedin_only mode cascade (Step 4 fallback)
- domain_only mode cascade (Step 3 fallback)
- enhanced mode cascade (final fallback)
- prospeo_client import

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 3: Remove Prospeo from `list_builder.py`

**Files:**
- Modify: `backend/enrichment/list_builder.py:31`
- Modify: `backend/enrichment/list_builder.py:240-258`

- [ ] **Step 1: Remove `prospeo` from `VALID_PROVIDERS`**

File: `backend/enrichment/list_builder.py`, line 31

Change:
```python
VALID_PROVIDERS = frozenset({"contacts_db", "blitz", "better_enrich", "prospeo"})
```
To:
```python
VALID_PROVIDERS = frozenset({"contacts_db", "blitz", "better_enrich"})
```

- [ ] **Step 2: Remove Strategy 5 (Prospeo) from `_resolve_person_email`**

File: `backend/enrichment/list_builder.py`, lines 240-258

Delete this entire block:
```python
    # Strategy 5: Prospeo as final fallback (PAID)
    if not _should_skip_provider("prospeo", force_provider):
        try:
            prospeo_result = await prospeo_client.enrich_person(
                blitz_http,
                linkedin_url=linkedin_url if linkedin_url else None,
                full_name=search_name if search_name else None,
                company_website=domain if domain else None,
            )
            if prospeo_result:
                email = prospeo_client.extract_email_from_prospeo(prospeo_result)
                person_data = prospeo_result.get("person", {})
                verified = prospeo_client.extract_verified_status(prospeo_result)
                phone = person_data.get("mobile", {}).get("mobile", "") if person_data.get("mobile") else ""
                if email:
                    logger.debug("Prospeo found email for %s", search_name or linkedin_url)
                    return email, phone, SOURCE_PROSPEO, verified
        except Exception as e:
            logger.debug("Prospeo person enrich failed: %s", e)
```

- [ ] **Step 3: Remove `prospeo_client` import**

File: `backend/enrichment/list_builder.py`, line 25

Delete:
```python
from . import prospeo_client
```

- [ ] **Step 4: Remove `SOURCE_PROSPEO` constant**

File: `backend/enrichment/list_builder.py`, line 83

Delete:
```python
SOURCE_PROSPEO = "prospeo"
```

Keep `SOURCE_PROSPEO` in `_normalize_source` for historical data handling (it's the last return in the function).

- [ ] **Step 5: Run tests**

Run: `cd /var/www/lead-generation-platform/backend && source venv/bin/activate && python -m pytest enrichment/tests/ -v --tb=short 2>&1 | head -80`

Expected: All existing tests pass.

- [ ] **Step 6: Commit list_builder.py changes**

```bash
git add backend/enrichment/list_builder.py
git commit -m "chore: disable Prospeo in list_builder.py (Phase 1)

Remove Strategy 5 (Prospeo) from _resolve_person_email cascade.
Remove prospeo from VALID_PROVIDERS.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 4: Remove Prospeo from `frontend/index.html`

**Files:**
- Modify: `frontend/index.html:218`
- Modify: `frontend/index.html:226-229`
- Modify: `frontend/index.html:655-660`

- [ ] **Step 1: Update homepage cascade description**

File: `frontend/index.html`, line 218

Change:
```html
<p>Build targeted lead lists by uploading domains, searching companies, or enriching LinkedIn profiles. Get emails using our 4-source cascade: Contacts DB, Blitz API, BetterEnrich, and Prospeo.</p>
```
To:
```html
<p>Build targeted lead lists by uploading domains, searching companies, or enriching LinkedIn profiles. Get emails using our 3-source cascade: Contacts DB, Blitz API, and BetterEnrich.</p>
```

- [ ] **Step 2: Update data sources list (remove Prospeo item)**

File: `frontend/index.html`, lines 226-229

Change:
```html
                    <li><strong>Contacts DB</strong> - Free, cached data (75 RPS)</li>
                    <li><strong>Blitz API</strong> - LinkedIn-based enrichment (25 RPS)</li>
                    <li><strong>BetterEnrich</strong> - Person/company email lookup (10 RPS)</li>
                    <li><strong>Prospeo</strong> - Final fallback enrichment (paid)</li>
```
To:
```html
                    <li><strong>Contacts DB</strong> - Free, cached data (75 RPS)</li>
                    <li><strong>Blitz API</strong> - LinkedIn-based enrichment (25 RPS)</li>
                    <li><strong>BetterEnrich</strong> - Person/company email lookup (10 RPS)</li>
```

Also change `<ol>` from `start="4"` to `start="3"` since we now have 3 items.

- [ ] **Step 3: Remove Prospeo from help section**

File: `frontend/index.html`, lines 655-660

Delete this entire block:
```html
                <div class="data-sources">
                    <h4>4. Prospeo (Final Fallback - Paid)</h4>
                    <p style="font-size:0.85rem;color:var(--gray-600);">Person and company enrichment as last resort (30 RPS).</p>
                    <div style="margin-top:0.5rem;">
                        <span class="source-tag">prospeo</span>
                        <span class="source-tag">prospeo_person</span>
                    </div>
                </div>
```

- [ ] **Step 4: Verify frontend loads correctly**

Open the frontend in browser or check for syntax errors:
Run: `grep -n "Prospeo\|prospeo" frontend/index.html` — should return no results.

- [ ] **Step 5: Commit frontend changes**

```bash
git add frontend/index.html
git commit -m "chore: remove Prospeo from frontend UI (Phase 1)

- Update homepage cascade description to 3-source cascade
- Remove Prospeo from data sources list
- Remove Prospeo from help section
- Renumber list from 4 to 3

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 5: End-to-End Verification (Phase 1)

**Files:** None (verification only)

- [ ] **Step 1: Restart the service**

Run:
```bash
sudo systemctl restart lead-generation-platform.service
sleep 3
curl http://localhost:8765/api/health
```
Expected: `{"status":"ok"}` or similar

- [ ] **Step 2: Run a small enrichment job and check logs for Prospeo calls**

Run a test enrichment job (use existing test data or a small CSV with 2-3 domains).

Check logs:
```bash
journalctl -u lead-generation-platform.service --since "5 minutes ago" | grep -i prospeo
```
Expected: No prospeo calls in recent logs.

- [ ] **Step 3: Verify enrichment still works without Prospeo**

Check that enrichment jobs complete successfully and return contacts/emails from the 3 remaining providers.

---

## Phase 2 — Centralized Provider Config

### Task 6: Create `providers.py`

**Files:**
- Create: `backend/enrichment/providers.py`

- [ ] **Step 1: Create `backend/enrichment/providers.py`**

```python
"""
Centralized provider configuration.

Single source of truth for which enrichment providers are enabled.
To disable a provider, set its value to False here — no cascade logic changes needed.

The cascade logic in pipeline.py, routes.py, and list_builder.py
reads from this module to decide whether to call each provider.
"""

from typing import Final

# Single source of truth — edit here to enable/disable providers
ENABLED_PROVIDERS: Final[dict[str, bool]] = {
    "contacts_db": True,
    "blitz": True,
    "better_enrich": True,
    "prospeo": False,   # ← disable Prospeo (was paid, temporarily disabled)
}


def is_provider_enabled(provider: str) -> bool:
    """
    Check if a provider is enabled.

    Args:
        provider: Provider name (e.g., "contacts_db", "blitz", "prospeo")

    Returns:
        True if enabled, False otherwise. Defaults to False for unknown providers.
    """
    return ENABLED_PROVIDERS.get(provider, False)


def get_enabled_providers() -> list[str]:
    """
    Return list of enabled provider names.

    Returns:
        List of provider names that are currently enabled.
    """
    return [name for name, enabled in ENABLED_PROVIDERS.items() if enabled]
```

- [ ] **Step 2: Verify the module loads without errors**

Run:
```bash
cd /var/www/lead-generation-platform/backend && source venv/bin/activate && python -c "from enrichment.providers import is_provider_enabled, get_enabled_providers; print(get_enabled_providers())"
```
Expected: `['contacts_db', 'blitz', 'better_enrich']`

- [ ] **Step 3: Commit providers.py**

```bash
git add backend/enrichment/providers.py
git commit -m "feat: add centralized ENABLED_PROVIDERS config (Phase 2)

Single source of truth for provider toggling.
is_provider_enabled() and get_enabled_providers() helpers.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 7: Update `pipeline.py` to use `providers.py`

**Files:**
- Modify: `backend/enrichment/pipeline.py`

- [ ] **Step 1: Add import at the top of pipeline.py**

File: `backend/enrichment/pipeline.py`, after existing imports (after line 33)

Add:
```python
from . import providers
```

- [ ] **Step 2: Replace `_should_skip_provider` to also check ENABLED_PROVIDERS**

File: `backend/enrichment/pipeline.py`, lines 43-61

Replace the existing `_should_skip_provider` function with:

```python
def _should_skip_provider(provider: str, force_provider: Optional[str]) -> bool:
    """
    Determine if a provider should be skipped.

    Args:
        provider: The current provider being considered (e.g., "contacts_db", "blitz")
        force_provider: The forced provider from request (or None for normal cascade)

    Returns:
        True if the provider should be skipped, False otherwise

    Checks:
      1. Is the provider globally disabled in ENABLED_PROVIDERS?
      2. If force_provider is set, does it match the current provider?
    """
    # First check: is the provider globally disabled?
    if not providers.is_provider_enabled(provider):
        logger.debug("_should_skip_provider: %s disabled in ENABLED_PROVIDERS", provider)
        return True

    # Second check: force_provider constraint
    if force_provider:
        result = provider != force_provider
        logger.debug("_should_skip_provider(provider=%s, force_provider=%s) = %s", provider, force_provider, result)
        return result

    return False
```

- [ ] **Step 3: Verify pipeline still imports and works**

Run:
```bash
cd /var/www/lead-generation-platform/backend && source venv/bin/activate && python -c "from enrichment.pipeline import _should_skip_provider; print('OK')"
```

- [ ] **Step 4: Run tests**

Run: `cd /var/www/lead-generation-platform/backend && source venv/bin/activate && python -m pytest enrichment/tests/ -v --tb=short 2>&1 | head -80`

Expected: All existing tests pass.

- [ ] **Step 5: Commit pipeline.py providers integration**

```bash
git add backend/enrichment/pipeline.py
git commit -m "feat: wire ENABLED_PROVIDERS into pipeline.py (Phase 2)

_should_skip_provider now checks providers.is_provider_enabled()
first before checking force_provider.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 8: Update `routes.py` to use `providers.py`

**Files:**
- Modify: `backend/enrichment/routes.py`

- [ ] **Step 1: Add import after existing imports**

File: `backend/enrichment/routes.py`, after line 38 (after `from . import better_enrich_client`)

Add:
```python
from . import providers
```

- [ ] **Step 2: Replace `_should_skip_provider` in routes.py**

File: `backend/enrichment/routes.py`, lines 106-124 (approximate location — search for `def _should_skip_provider`)

Find and replace with the same implementation as pipeline.py:

```python
def _should_skip_provider(provider: str, force_provider: Optional[str]) -> bool:
    """
    Determine if a provider should be skipped.

    Args:
        provider: The current provider being considered (e.g., "contacts_db", "blitz")
        force_provider: The forced provider from request (or None for normal cascade)

    Returns:
        True if the provider should be skipped, False otherwise

    Checks:
      1. Is the provider globally disabled in ENABLED_PROVIDERS?
      2. If force_provider is set, does it match the current provider?
    """
    # First check: is the provider globally disabled?
    if not providers.is_provider_enabled(provider):
        logger.debug("_should_skip_provider: %s disabled in ENABLED_PROVIDERS", provider)
        return True

    # Second check: force_provider constraint
    if force_provider:
        return provider != force_provider

    return False
```

- [ ] **Step 3: Run tests**

Run: `cd /var/www/lead-generation-platform/backend && source venv/bin/activate && python -m pytest enrichment/tests/ -v --tb=short 2>&1 | head -80`

Expected: All existing tests pass.

- [ ] **Step 4: Commit routes.py providers integration**

```bash
git add backend/enrichment/routes.py
git commit -m "feat: wire ENABLED_PROVIDERS into routes.py (Phase 2)

_should_skip_provider now checks providers.is_provider_enabled()
first before checking force_provider.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 9: Update `list_builder.py` to use `providers.py`

**Files:**
- Modify: `backend/enrichment/list_builder.py`

- [ ] **Step 1: Add import after existing imports**

File: `backend/enrichment/list_builder.py`, after line 24 (after `from . import prospeo_client` — which should already be deleted from Phase 1)

Add:
```python
from . import providers
```

- [ ] **Step 2: Replace `_should_skip_provider` in list_builder.py**

File: `backend/enrichment/list_builder.py`, lines 34-50

Find and replace with:

```python
def _should_skip_provider(provider: str, force_provider: Optional[str]) -> bool:
    """
    Determine if a provider should be skipped.

    Args:
        provider: The current provider being considered (e.g., "contacts_db", "blitz")
        force_provider: The forced provider from request (or None for normal cascade)

    Returns:
        True if the provider should be skipped, False otherwise

    Checks:
      1. Is the provider globally disabled in ENABLED_PROVIDERS?
      2. If force_provider is set, does it match the current provider?
    """
    # First check: is the provider globally disabled?
    if not providers.is_provider_enabled(provider):
        logger.debug("_should_skip_provider: %s disabled in ENABLED_PROVIDERS", provider)
        return True

    # Second check: force_provider constraint
    if force_provider:
        return provider != force_provider

    return False
```

- [ ] **Step 3: Run tests**

Run: `cd /var/www/lead-generation-platform/backend && source venv/bin/activate && python -m pytest enrichment/tests/ -v --tb=short 2>&1 | head -80`

Expected: All existing tests pass.

- [ ] **Step 4: Commit list_builder.py providers integration**

```bash
git add backend/enrichment/list_builder.py
git commit -m "feat: wire ENABLED_PROVIDERS into list_builder.py (Phase 2)

_should_skip_provider now checks providers.is_provider_enabled()
first before checking force_provider.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 10: Add `GET /api/enrichment/providers` endpoint

**Files:**
- Modify: `backend/enrichment/routes.py`

- [ ] **Step 1: Add the endpoint near the top of the routes file**

File: `backend/enrichment/routes.py` — add after the existing imports and before the first route definition (around line 105, after `VALID_PROVIDERS`)

```python
@router.get("/providers")
async def get_enrichment_providers(
    current_user: dict = Depends(auth.get_current_user_with_api_key),
):
    """
    Return list of currently enabled enrichment providers.

    Used by frontend to dynamically render the data sources list
    without needing to update HTML when providers change.

    Returns:
        {"providers": ["contacts_db", "blitz", "better_enrich"]}
    """
    from . import providers as p
    return {"providers": p.get_enabled_providers()}
```

- [ ] **Step 2: Update the force_provider parameter descriptions**

Search for `force_provider` in routes.py and update the description strings to include `providers.py` as the single source of truth:

Update all docstrings that say `"contacts_db", "blitz", "better_enrich", "prospeo"` to `"contacts_db", "blitz", "better_enrich"` (prospeo removed from the valid options, but still appears as a possible force_provider value for backward compatibility with in-flight requests).

Actually, keep the parameter description as-is — if someone passes `force_provider=prospeo` when prospeo is disabled, `_should_skip_provider` will catch it. Don't over-engineer the description.

- [ ] **Step 3: Test the new endpoint**

Run:
```bash
curl -s http://localhost:8765/api/enrichment/providers -H "Authorization: Bearer <your-jwt-token>"
```
Expected: `{"providers":["contacts_db","blitz","better_enrich"]}`

- [ ] **Step 4: Commit endpoint**

```bash
git add backend/enrichment/routes.py
git commit -m "feat: add GET /api/enrichment/providers endpoint (Phase 2)

Returns list of enabled providers from providers.py.
Frontend can call this to render data sources dynamically.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 11: Update frontend to fetch providers from API

**Files:**
- Modify: `frontend/index.html`

- [ ] **Step 1: Add JavaScript to fetch providers and update the UI dynamically**

File: `frontend/index.html` — add a function to fetch providers and update the data sources list. Find the `init()` or `document.addEventListener('DOMContentLoaded', ...)` section and add:

```javascript
async function loadEnabledProviders() {
    try {
        const token = localStorage.getItem('auth_token');
        const resp = await fetch('/api/enrichment/providers', {
            headers: { 'Authorization': 'Bearer ' + token }
        });
        if (!resp.ok) return;
        const data = await resp.json();
        const providers = data.providers || [];

        // Map provider keys to display names and descriptions
        const providerInfo = {
            'contacts_db': { name: 'Contacts DB', desc: 'Free, cached data (75 RPS)', tag: 'contacts_db' },
            'blitz': { name: 'Blitz API', desc: 'LinkedIn-based enrichment (25 RPS)', tag: 'blitz' },
            'better_enrich': { name: 'BetterEnrich', desc: 'Person/company email lookup (10 RPS)', tag: 'better_enrich' },
        };

        // Update homepage description dynamically
        const cascadeDesc = document.querySelector('.welcome-card p');
        if (cascadeDesc) {
            cascadeDesc.textContent = `Build targeted lead lists by uploading domains, searching companies, or enriching LinkedIn profiles. Get emails using our ${providers.length}-source cascade: ${providers.map(p => providerInfo[p]?.name || p).join(', ')}.`;
        }

        // Update data sources list in info section
        const sourcesList = document.querySelector('.info-sources ol');
        if (sourcesList) {
            const labels = { contacts_db: 'Contacts DB', blitz: 'Blitz API', better_enrich: 'BetterEnrich' };
            sourcesList.innerHTML = providers.map(p => `<li><strong>${labels[p] || p}</strong></li>`).join('');
        }
    } catch (e) {
        console.warn('Could not load enabled providers:', e);
    }
}
```

Also call `loadEnabledProviders()` in the init/load flow.

**Simpler approach for now** — update the static HTML to remove hardcoded "4-source" references, and add a fetch call to update the count/names if the API is available. The simplest safe approach:

In the JavaScript section, find the `showPage('home')` function or the home page init and add:
```javascript
// Fetch and update cascade count
fetch('/api/enrichment/providers', { headers: { 'Authorization': 'Bearer ' + (localStorage.getItem('auth_token') || '') } })
    .then(r => r.json())
    .then(d => {
        const count = (d.providers || []).length;
        document.querySelectorAll('.info-sources ol').forEach(ol => {
            ol.start = 1;
        });
    })
    .catch(() => {});
```

**Even simpler** — just update the hardcoded "4-source" to "multi-source" as a temporary measure, and note that the dynamic fetch can be implemented as a follow-up. For now, the static removal of Prospeo from the list is sufficient.

Actually, since the frontend is static HTML with inline JS and not a React app, the safest approach is to keep the 3-item static list and just update the count text. The dynamic provider fetch is a nice-to-have but not critical for this Phase 2 task.

**Decision:** Do a minimal frontend update — change the hardcoded "4-source" to "3-source" (already done in Phase 1 Task 4). Add a note that the dynamic fetch can be implemented in a follow-up if desired. The core Phase 2 value is in `providers.py` + API endpoint.

- [ ] **Step 2: Commit frontend update**

```bash
git add frontend/index.html
git commit -m "chore: update frontend cascade count to 3-source (Phase 2)

Static update — dynamic provider fetch can be added as follow-up.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 12: Final Verification

**Files:** None (verification only)

- [ ] **Step 1: Restart service and run integration tests**

```bash
sudo systemctl restart lead-generation-platform.service
sleep 3
curl http://localhost:8765/api/health
```

- [ ] **Step 2: Verify GET /api/enrichment/providers returns only 3 providers**

```bash
curl -s http://localhost:8765/api/enrichment/providers -H "Authorization: Bearer <token>"
```
Expected: `{"providers":["contacts_db","blitz","better_enrich"]}`

- [ ] **Step 3: Verify no Prospeo calls in logs during a live enrichment job**

```bash
journalctl -u lead-generation-platform.service --since "10 minutes ago" | grep -i prospeo
```
Expected: No Prospeo calls.

- [ ] **Step 4: Verify enrichment still returns results without Prospeo**

Run a small domain enrichment job. Verify results come back from Contacts DB, Blitz, or BetterEnrich.

- [ ] **Step 5: Verify enabling prospeo in providers.py re-enables it**

In `providers.py`, change `"prospeo": False` to `"prospeo": True`, restart service, run a job, check that Prospeo IS called.

Then set it back to `False` and restart.

---

## Summary of Files Changed

| Task | File | Change |
|------|------|--------|
| 1 | `pipeline.py` | Remove Prospeo cascade steps + wire providers.py |
| 2 | `routes.py` | Remove Prospeo cascade steps + wire providers.py + add endpoint |
| 3 | `list_builder.py` | Remove Prospeo cascade steps + wire providers.py |
| 4 | `frontend/index.html` | Remove Prospeo from static UI |
| 5 | `providers.py` | NEW — ENABLED_PROVIDERS, helpers |