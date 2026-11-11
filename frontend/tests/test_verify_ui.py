"""Verify-Emails UI structural assertions (PR-1.3, 2026-11-11).

Served-HTML convention (test_tam_page_ui.py): pin the page contract — nav
entry, hash routes, page elements, option defaults, Jobs-page wiring (source
chip + filter option + Verify-emails button), and the catch-all semantics
copy — so future hand edits of the single-file frontend cannot silently
regress Stage 3.
"""
from pathlib import Path

import pytest

INDEX = Path(__file__).parent.parent / "index.html"


@pytest.fixture(scope="module")
def html():
    return INDEX.read_text(encoding="utf-8")


# --- Navigation + routing ---------------------------------------------------

def test_nav_item_present(html):
    assert 'data-page="verify"' in html
    assert "Verify Emails" in html


def test_hash_routes_mapped(html):
    assert "'verify': 'verify'" in html
    assert "'verify-emails': 'verify'" in html


def test_page_present_with_controls(html):
    assert 'id="page-verify"' in html
    assert 'id="verifyPaste"' in html
    assert 'id="verifyFile"' in html
    assert 'id="verifySubmit"' in html
    assert 'id="verifyJobName"' in html
    assert 'id="verifyDownload"' in html
    assert 'id="verifyCancel"' in html
    assert 'id="verifyResults"' in html


def test_option_defaults_preserve_policy(html):
    # allow_catch_all must ship UNCHECKED (sendability posture unchanged)…
    assert 'id="verifyAllowCatchAll" style="width:16px;height:16px;">' in html
    assert 'id="verifyAllowCatchAll" style="width:16px;height:16px;" checked>' not in html
    # …and recheck unchecked (cache-first by default).
    assert 'id="verifyRecheck" style="width:16px;height:16px;">' in html
    assert 'id="verifyRecheck" style="width:16px;height:16px;" checked>' not in html


def test_catch_all_copy_is_honest(html):
    # The page must explain catch-all ≠ verified and that results are retained.
    assert "accepts <em>everything</em>" in html
    assert "never deletes results" in html
    assert "read-only" in html


def test_js_handlers_wired(html):
    assert "verifyFromJob" in html
    assert "window.verifyFromJob = verifyFromJob" in html
    assert "/api/enrichment/verify/from-job/" in html


# --- Jobs page wiring --------------------------------------------------------

def test_jobs_source_filter_has_verify_option(html):
    assert '<option value="verify">✓ Verify Emails</option>' in html


def test_jobs_source_chip_map_has_verify(html):
    assert "verify: { t: '✓ Verify Emails'" in html


def test_jobs_card_verify_button_only_for_email_jobs(html):
    assert "verifyFromJob(\\''+job.job_id+'\\', this)" in html
    assert "job.source_type !== 'verify'" in html  # no self-verify


def test_verify_job_meta_line(html):
    assert "emails checked" in html


def test_catch_all_badge_on_job_cards(html):
    assert "catch-all retained" in html
    assert "job.catch_all_retained" in html
