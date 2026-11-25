"""2026-11-25 four-pack UX/UI pins.

B1  — 'Resolve company names' checkbox on the Scope panel + resolve_names in
      the Flow-1 request body; banner no longer claims admin-wide shutdown.
A1  — cross-leg person-location output filter copy (see also
      test_people_company_mode.test_location_caveat_present).
UX1 — TAM industry is a searchable chip MULTI-select feeding industry_include
      as an OR-array; the old single <select id="searchIndustry"> is gone.
UX2 — '→ People' handoff button on TAM runs + window-exported
      handoffTamToPeople (inline onclick trap — see window-exports history).
"""
from pathlib import Path

import pytest

INDEX = Path(__file__).parent.parent / "index.html"


@pytest.fixture(scope="module")
def html():
    return INDEX.read_text(encoding="utf-8")


# --- B1: resolve-names opt-in ------------------------------------------------

def test_resolve_names_checkbox_present(html):
    assert 'id="peopleScopeResolveNames"' in html


def test_resolve_names_sent_in_request_body(html):
    assert "resolve_names: !!(document.getElementById('peopleScopeResolveNames')" in html


def test_resolver_checkbox_defaults_off(html):
    assert 'id="peopleScopeResolveNames" checked' not in html


def test_banner_no_longer_claims_admin_shutdown(html):
    assert "switched off by the administrator" not in html
    assert "Resolve company names" in html


# --- UX1: industry chip multi-select ------------------------------------------

def test_industry_chip_widgets_present(html):
    for el in ("tamIndustrySearch", "tamIndustryDrop", "tamIndustryChips"):
        assert f'id="{el}"' in html


def test_old_industry_select_gone(html):
    assert 'id="searchIndustry"' not in html


def test_industry_chips_populated_from_options(html):
    assert "setupTamIndustryChips(data.industries || [])" in html


def test_industry_include_feeds_from_chip_set(html):
    assert "company.industry_include = Array.from(tamIndustrySelected)" in html


def test_employee_and_country_selects_still_populated(html):
    # The options loader still fills the two surviving selects.
    assert "['EmployeeRange', 'Country'].forEach" in html


# --- UX2: TAM → People handoff -------------------------------------------------

def test_handoff_button_in_runs_table(html):
    assert "onclick=\"handoffTamToPeople(" in html
    assert "→ People</button>" in html


def test_handoff_function_defined_and_exported(html):
    assert "async function handoffTamToPeople(jobId, btn)" in html
    assert "window.handoffTamToPeople = handoffTamToPeople" in html


def test_handoff_uses_download_endpoint_and_scope_panel(html):
    assert "/api/enrichment/jobs/' + jobId + '/download?token=" in html
    assert "peopleScopeList" in html.split("async function handoffTamToPeople")[1][:2200]


def test_handoff_prefers_linkedin_then_domain(html):
    body = html.split("async function handoffTamToPeople")[1][:2600]
    assert "linkedin\\.com\\/company\\/" in body or "linkedin.com/company/" in body
    assert "hdr.indexOf('domain')" in body
