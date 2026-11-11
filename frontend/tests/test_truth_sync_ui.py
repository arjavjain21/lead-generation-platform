"""Truth-sync assertions for operator-facing copy (PR-0.2, 2026-11-11).

Follows the repo's served-HTML assertion convention (see
test_tam_page_ui.py): the shipped index.html is the product, so the
operator-facing claims — active source count/list, the internal-DB size on
the Find People banner, the Help page's Last-Updated stamp, and the Jobs
page source filter — are pinned here to catch regressions from future hand
edits of the single-file frontend.

Truth basis (verified 2026-11-11):
  * 8 active cascade tiers: contacts_db → smartprospect → quickenrich →
    leadocean → getleads → blitz → moltsets → wizleads (better_enrich
    env-off in prod, prospeo disabled end-to-end) — mirrors
    enrichment/providers.py + backend/.env kill switches and the executor
    order in pipeline.py / list_builder.py.
  * Internal contacts DB: 30,848,494 persons measured 2026-11-10
    (leadsdatabase.cc /stats.json); "30M+" is the conservative round-down.
"""
from pathlib import Path

import pytest

INDEX = Path(__file__).parent.parent / "index.html"


@pytest.fixture(scope="module")
def html():
    return INDEX.read_text(encoding="utf-8")


# --- Help page: sources ----------------------------------------------------

def test_help_lists_eight_active_sources(html):
    assert "We use 8 active data sources" in html


def test_help_includes_leadocean_and_moltsets_tiers(html):
    assert "4. LeadOcean (Paid)" in html
    assert "7. MoltSets (Paid)" in html


def test_help_cascade_numbering_matches_live_order(html):
    # Order pins: GetLeads 5th, Blitz 6th, WizLeads 8th (terminal).
    assert "5. GetLeads (Paid)" in html
    assert "6. Blitz API (Paid)" in html
    assert "8. WizLeads (Paid)" in html


def test_help_disabled_block_distinguishes_kill_switch_from_end_to_end(html):
    assert "switched off by an admin kill switch" in html  # BetterEnrich (env)
    assert "disabled end-to-end" in html                    # Prospeo


def test_help_stale_copy_removed(html):
    assert "We use 6 data sources" not in html
    # Old blanket claim — BetterEnrich was re-enabled in code 2026-10-14
    # (env kill switch), so "are currently disabled" was wrong for Prospeo too.
    assert "and Prospeo are currently disabled" not in html


def test_help_last_updated_stamped_this_change(html):
    assert "Last Updated: 2026-11-11" in html
    assert "Last Updated: 2026-07-05" not in html


# --- Find People banner: internal DB size ----------------------------------

def test_people_banner_uses_verified_person_count(html):
    assert "30M+ people" in html
    assert "6.6M+ leads" not in html  # stale figure (pre-30.8M measurement)


def test_help_contacts_db_copy_mentions_database_scale(html):
    assert "database of 30M+ people" in html


# --- Jobs page: source filter covers TAM job types --------------------------

def test_jobs_source_filter_includes_tam_options(html):
    assert '<option value="tam_flow">' in html
    assert '<option value="tam_chain">' in html


# --- Home page: cascade copy consistent with the live 8-active order -------

def test_home_cascade_copy_lists_active_sources_only(html):
    assert "8-source cascade" in html
    assert "9-source cascade" not in html  # counts the env-off BetterEnrich tier


def test_home_data_sources_card_lists_eight_in_order(html):
    card = html.split("We use 8 active data sources", 1)[1][:1200]
    for expected in (
        "Contacts DB", "smartprospect", "QuickEnrich", "LeadOcean",
        "GetLeads", "Blitz API", "MoltSets", "WizLeads",
    ):
        assert expected in card
