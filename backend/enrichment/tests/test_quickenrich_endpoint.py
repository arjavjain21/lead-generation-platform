"""Tests for POST /api/enrichment/quickenrich (QE-only fast path).

Covers: availability gate (503), input validation (400/422), single-domain
happy path via the shared _unified_enrich_logic executor (force_provider=
"quickenrich" semantics — no other provider called), linkedin_url mode,
optional MailTester validation (drop "ko"/"mb", poison "ko" only, fail-open
on unavailability), and bounded batch fan-out with per-row error isolation.

Run:
    cd /var/www/lead-generation-platform/backend && source venv/bin/activate
    python -m pytest enrichment/tests/test_quickenrich_endpoint.py -v
"""

import asyncio
import os
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
os.environ.setdefault("CONTACTS_API_TOKEN", "test-token-from-suite")

from enrichment import routes as routes_mod  # noqa: E402
from enrichment import blitz_client as bc  # noqa: E402
from enrichment import contacts_client as cc  # noqa: E402
from enrichment import contacts_writer  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures (mirror test_selected_providers.py conventions)
# ---------------------------------------------------------------------------


def _make_user() -> dict:
    return {"user_id": "test-user-1", "email": "test@example.com", "is_admin": True}


def _override_auth():
    from shared import auth as _auth

    return {
        _auth.get_current_user_with_api_key: lambda: _make_user(),
        _auth.get_current_user: lambda: _make_user(),
    }


@pytest.fixture
def client():
    from fastapi.testclient import TestClient
    from main import app

    app.dependency_overrides.update(_override_auth())
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()


def _qe_employee(
    full_name="Jane Founder",
    email="jane@example.com",
    title="CEO",
    verified="2026-01-01",
):
    """One parsed employee record, matching quickenrich_client._parse_employee_record."""
    parts = full_name.split(" ", 1)
    return {
        "found": True,
        "email": email,
        "first_name": parts[0],
        "last_name": parts[1] if len(parts) > 1 else "",
        "full_name": full_name,
        "title": title,
        "linked_url": "https://www.linkedin.com/in/jane-founder",
        "email_verification_date": verified,
        "company_name": "Example Inc",
        "company_url": "https://example.com",
    }


def _qe_domain_page(employees):
    return {"employees": employees, "credits_used": len(employees), "remaining_credits": None}


class _Patches:
    """Hermetic stubs for the shared executor + write-back path."""

    def __init__(self, qe_page=None, qe_person=None):
        self.qe_page = qe_page
        self.qe_person = qe_person
        self.contacts_by_domain = AsyncMock(return_value=None)
        self.waterfall = AsyncMock(return_value={"results": []})
        self.d2l = AsyncMock(return_value=None)
        self.sp = AsyncMock(return_value=None)
        self.gl = AsyncMock(return_value=None)
        self.mark_invalid = AsyncMock(return_value={"ok": True})
        self._live = []

    def start(self):
        fake_sync = MagicMock()
        fake_sync.sync_enrichment_to_contacts = MagicMock(
            return_value={"synced": 0, "skipped": 0, "failed": 0}
        )
        self.sync = fake_sync
        patches = [
            patch.object(
                routes_mod,
                "_quickenrich_endpoint_available",
                MagicMock(return_value=True),
            ),
            patch.object(contacts_writer, "is_v2_enabled", MagicMock(return_value=False)),
            patch.object(routes_mod, "sync_contacts", fake_sync),
            patch.object(routes_mod, "_record_unified_enrich_stats", MagicMock()),
            patch.object(cc, "company_by_domain", self.contacts_by_domain),
            patch.object(cc, "mark_email_invalid", self.mark_invalid),
            patch.object(bc, "waterfall_icp_search", self.waterfall),
            patch.object(bc, "domain_to_linkedin", self.d2l),
            patch.object(routes_mod.smartprospect_client, "find_email", self.sp),
            patch.object(routes_mod.getleads_client, "find_email_by_linkedin", self.gl),
            patch.object(
                routes_mod.quickenrich_client,
                "domain_search",
                AsyncMock(return_value=self.qe_page),
            ),
            patch.object(
                routes_mod.quickenrich_client,
                "find_person_email",
                AsyncMock(return_value=self.qe_person),
            ),
        ]
        for p in patches:
            p.start()
        self._live = patches
        return self

    def stop(self):
        for p in getattr(self, "_live", []):
            p.stop()


@pytest.fixture
def hermetic():
    stubs = _Patches(qe_page=_qe_domain_page([_qe_employee()])).start()
    yield stubs
    stubs.stop()


# ---------------------------------------------------------------------------
# Availability + input validation (no provider stubs needed)
# ---------------------------------------------------------------------------


def test_503_when_quickenrich_disabled(client):
    with patch.object(
        routes_mod, "_quickenrich_endpoint_available", MagicMock(return_value=False)
    ):
        resp = client.post("/api/enrichment/quickenrich", json={"domain": "example.com"})
    assert resp.status_code == 503
    assert "disabled" in resp.json()["detail"]


def test_400_when_no_input(client, hermetic):
    resp = client.post("/api/enrichment/quickenrich", json={})
    assert resp.status_code == 400


def test_400_when_batch_combined_with_single(client, hermetic):
    resp = client.post(
        "/api/enrichment/quickenrich",
        json={"domains": ["a.com"], "domain": "b.com"},
    )
    assert resp.status_code == 400


def test_400_when_full_name_without_domain(client, hermetic):
    resp = client.post(
        "/api/enrichment/quickenrich",
        json={"linkedin_url": "https://www.linkedin.com/in/x", "full_name": "X Y"},
    )
    assert resp.status_code == 400


def test_422_when_batch_too_large(client, hermetic):
    domains = [f"d{i}.com" for i in range(26)]
    resp = client.post("/api/enrichment/quickenrich", json={"domains": domains})
    assert resp.status_code == 422


def test_400_when_batch_all_empty(client, hermetic):
    resp = client.post("/api/enrichment/quickenrich", json={"domains": ["  ", ""]})
    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# Single-domain happy path
# ---------------------------------------------------------------------------


def test_single_domain_returns_qe_contacts_and_sync_receipt(client, hermetic):
    resp = client.post("/api/enrichment/quickenrich", json={"domain": "example.com"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["contact_count"] >= 1
    emails = [c.get("email") for c in body["contacts"]]
    assert "jane@example.com" in emails
    # Write-back receipt present (persistence contract)
    assert "sync_to_contacts_db" in body
    # Validation block absent unless requested
    assert "validation" not in body


def test_single_domain_calls_only_quickenrich(client, hermetic):
    resp = client.post("/api/enrichment/quickenrich", json={"domain": "example.com"})
    assert resp.status_code == 200
    assert routes_mod.quickenrich_client.domain_search.call_count >= 1
    assert hermetic.waterfall.call_count == 0
    assert hermetic.d2l.call_count == 0
    assert hermetic.sp.call_count == 0
    assert hermetic.gl.call_count == 0


def test_linkedin_url_mode_uses_employee_search(client, hermetic):
    hermetic.qe_person = {
        "found": True,
        "email": "jane@example.com",
        "full_name": "Jane Founder",
        "title": "CEO",
        "email_verification_date": "2026-01-01",
    }
    with patch.object(
        routes_mod.quickenrich_client,
        "find_person_email",
        AsyncMock(return_value=hermetic.qe_person),
    ):
        resp = client.post(
            "/api/enrichment/quickenrich",
            json={"linkedin_url": "https://www.linkedin.com/in/jane-founder"},
        )
    assert resp.status_code == 200
    body = resp.json()
    assert any(
        c.get("email") == "jane@example.com" for c in body.get("contacts") or []
    )


# ---------------------------------------------------------------------------
# Optional MailTester validation
# ---------------------------------------------------------------------------


def test_validate_drops_ko_and_poisons_only_ko(client, hermetic):
    hermetic.qe_page = _qe_domain_page(
        [
            _qe_employee(full_name="Jane Founder", email="good@example.com", title="CEO"),
            _qe_employee(full_name="Bob Dead", email="dead@example.com", title="Founder"),
        ]
    )

    async def fake_verify(http, email):
        dead = email.startswith("dead")
        return {
            "valid": not dead,
            "code": "ko" if dead else "ok",
            "message": "",
            "email": email,
        }

    with patch.object(
        routes_mod.quickenrich_client, "domain_search", AsyncMock(return_value=hermetic.qe_page)
    ), patch.object(
        routes_mod.mailtester_client, "verify_email", fake_verify
    ):
        resp = client.post(
            "/api/enrichment/quickenrich",
            json={"domain": "example.com", "validate_emails": True},
        )
    assert resp.status_code == 200
    body = resp.json()
    emails = [c.get("email") for c in body["contacts"]]
    assert "good@example.com" in emails
    assert "dead@example.com" not in emails
    assert body["validation"]["dropped_invalid"] == 1
    assert body["validation"]["accepted"] >= 1
    # Only the hard-invalid email is poisoned
    hermetic.mark_invalid.assert_awaited_once()
    call_kwargs = hermetic.mark_invalid.await_args.kwargs
    assert call_kwargs.get("email") == "dead@example.com"


def test_validate_catch_all_kept_only_when_allowed(client, hermetic):
    hermetic.qe_page = _qe_domain_page(
        [_qe_employee(full_name="Catch All", email="catchall@example.com", title="CEO")]
    )

    async def fake_verify(http, email):
        # Catch-all server: accepted mailbox unknown
        return {"valid": False, "code": "mb", "message": "Catch-All", "email": email}

    base_kwargs = {"domain": "example.com", "validate_emails": True}
    with patch.object(
        routes_mod.quickenrich_client, "domain_search", AsyncMock(return_value=hermetic.qe_page)
    ), patch.object(routes_mod.mailtester_client, "verify_email", fake_verify):
        # Default policy: catch-all dropped (not poisoned)
        resp = client.post("/api/enrichment/quickenrich", json=base_kwargs)
        assert resp.status_code == 200
        body = resp.json()
        assert body["contact_count"] == 0
        assert body["validation"]["dropped_policy_rejected"] == 1
        hermetic.mark_invalid.assert_not_awaited()

        # allow_catch_all: kept with mb code annotated, status NOT "invalid"
        resp = client.post(
            "/api/enrichment/quickenrich",
            json={**base_kwargs, "allow_catch_all": True},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["contact_count"] == 1
        assert body["contacts"][0]["mailtester_code"] == "mb"
        assert body["contacts"][0]["validation_status"] == "valid_catch_all"
        assert body["validation"]["accepted"] == 1


def test_response_dedupes_repeated_emails(client, hermetic):
    hermetic.qe_page = _qe_domain_page(
        [
            _qe_employee(full_name="Dup One", email="dup@example.com", title="CEO"),
            _qe_employee(full_name="Dup Again", email="DUP@example.com", title="CEO"),
            _qe_employee(full_name="Unique", email="uniq@example.com", title="CTO"),
        ]
    )
    with patch.object(
        routes_mod.quickenrich_client,
        "domain_search",
        AsyncMock(return_value=hermetic.qe_page),
    ):
        resp = client.post(
            "/api/enrichment/quickenrich", json={"domain": "example.com"}
        )
    assert resp.status_code == 200
    body = resp.json()
    emails = [c.get("email") for c in body["contacts"]]
    assert len(emails) == len(set(e.lower() for e in emails))
    assert body["contact_count"] == len(emails)


def test_validate_fails_open_when_mailtester_unavailable(client, hermetic):
    with patch.object(
        routes_mod.mailtester_client,
        "verify_email",
        AsyncMock(side_effect=RuntimeError("mailtester unavailable")),
    ):
        resp = client.post(
            "/api/enrichment/quickenrich",
            json={"domain": "example.com", "validate_emails": True},
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["contact_count"] >= 1  # contact kept (fail-open)
    assert body["validation"]["unavailable"] >= 1
    hermetic.mark_invalid.assert_not_awaited()


# ---------------------------------------------------------------------------
# Batch mode
# ---------------------------------------------------------------------------


def test_batch_fan_out_with_per_row_isolation(client, hermetic):
    pages = {
        "a.com": _qe_domain_page([_qe_employee(email="founder@a.com")]),
        "b.com": _qe_domain_page([_qe_employee(email="founder@b.com")]),
    }

    async def _qe_side_effect(http, company_url, titles=None, max_pages=None):
        return pages.get(company_url, _qe_domain_page([]))

    # Provider-level failures are swallowed by the pipeline by design (row
    # returns 0 contacts); a row only reports an error when the EXECUTOR
    # itself raises (e.g. capacity 429). Simulate that for boom.com.
    real_executor = routes_mod.unified_enrich

    async def _executor(sub, debug=False, current_user=None):
        if sub.domain == "boom.com":
            raise routes_mod.HTTPException(status_code=429, detail="at capacity")
        return await real_executor(sub, debug, current_user)

    with patch.object(
        routes_mod.quickenrich_client, "domain_search", AsyncMock(side_effect=_qe_side_effect)
    ), patch.object(routes_mod, "unified_enrich", _executor):
        resp = client.post(
            "/api/enrichment/quickenrich",
            json={"domains": ["a.com", "boom.com", "b.com", "a.com"]},
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is True
    # Dedup: 4 inputs -> 3 unique
    assert body["batch"]["total"] == 3
    assert body["batch"]["succeeded"] == 2
    assert body["batch"]["failed"] == 1
    by_domain = {r["domain"]: r for r in body["results"]}
    assert by_domain["a.com"]["contact_count"] == 1
    assert by_domain["boom.com"].get("error") == "at capacity"
    assert by_domain["b.com"]["contact_count"] == 1
