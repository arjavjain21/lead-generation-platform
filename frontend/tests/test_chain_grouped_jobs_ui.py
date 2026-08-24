"""Chain-grouped enrichment job cards ("one card per upload").

P2 UI: server restarts + auto-resume create one child job per attempt, which
used to fan a single upload out into N near-identical job cards with
per-attempt counters. The candidate artifact (index.chain_grouping.html —
pending promotion to index.html) collapses a restart chain into ONE card with
file-level progress and an "Attempt history" disclosure.

These tests follow the repo's served-HTML assertion convention (see
test_latam_dropdown_options.py). They are deliberately structural: the numeric
grouping math is exercised in the node harness documented in the commit that
introduced this file.
"""
from pathlib import Path

import pytest

FRONTEND = Path(__file__).parent.parent
CANDIDATE = FRONTEND / "index.chain_grouping.html"
INDEX = FRONTEND / "index.html"


@pytest.fixture(scope="module")
def html():
    """The chain-grouping candidate, falling back to index.html once promoted."""
    path = CANDIDATE if CANDIDATE.exists() else INDEX
    return path.read_text(encoding="utf-8")


def test_chain_grouping_artifact_exists(html):
    assert "buildEnrichmentChainUnits" in html, "chain grouping entry point missing"


def test_card_renderer_split_out(html):
    assert "function enrichmentJobCardHtml" in html
    assert "enrichmentJobCardHtml(u.job, u)" in html


def test_restart_badge_copy(html):
    assert "↻ Restarted " in html
    # The reassuring tooltip from the spec.
    assert "automatically resumed after a server restart" in html


def test_attempt_history_disclosure(html):
    assert "Attempt history" in html
    assert "toggleEnrichmentChainHistory" in html
    # Must be exported for the inline onclick handler (closure-scoped fns).
    assert "window.toggleEnrichmentChainHistory = toggleEnrichmentChainHistory" in html


def test_superseded_state_present(html):
    assert "Superseded" in html


def test_approx_marker_for_fragmented_chains(html):
    # Root off-page (pagination) → approximate file-level progress.
    assert "'≈'" in html or '"≈"' in html or "≈" in html


def test_existing_single_job_render_path_preserved(html):
    # Ungrouped jobs keep the classic per-job meta line.
    assert "'/' + (job.total||0) + ' rows'" in html
    assert "renderEnrichmentPagination" in html


def test_scraper_surface_untouched(html):
    # The scraper list still renders its own cards (no chain grouping there).
    assert "function renderScraperJobs" in html
    assert "toggleScraperShards" in html


def test_no_nul_bytes(html):
    assert "\x00" not in html
