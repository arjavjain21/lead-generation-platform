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
    # M5: plain-language badge (was "↻ Restarted N×", jargon + hover-only).
    assert "↻ Resumed " in html
    assert " times</span>" in html
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


# ── Review fixes (page-split continuations, head download, copy, XSS) ──


def test_continuation_state_present(html):
    """C1: a root-topped bucket whose chain continues off-page is rendered as a
    CONTINUATION (badge + demoted actions), not an actionable head."""
    assert "ctx.continuation" in html
    assert "Continued on another page" in html
    # Off-page proof compares the AGGREGATE attempt budget (Σ restart_count)
    # against on-page edges (members - 1). Per-member comparison breaks under
    # the root-counted claim bump: auto-resume increments the ROOT's counter
    # while the child attaches to the HEAD, so a fully-on-page chain can show
    # rc(root)=2 vs 1 child and would be falsely demoted (Stop button lost).
    assert "members.reduce((s, m) => s + (m.restart_count || 0), 0)" in html
    assert "> members.length - 1" in html


def test_approx_extended_to_off_page_chains(html):
    """C2: the ≈ condition also fires when the chain continues off-page."""
    assert "chainContinuesOffPage" in html
    assert "|| chainContinuesOffPage" in html


def test_head_download_gated_on_output_exists(html):
    """H1: grouped cards base partial-download visibility on output_exists
    (/recover-partial falls back to outputs/{id}.csv)."""
    assert "ctx.chainMeta ? job.output_exists : job.partial_output_path" in html
    # The head is always reachable as a distinct history row.
    assert "Current attempt" in html
    assert "att-current" in html


def test_attempt_history_sorted_oldest_first(html):
    """M1: history is labelled oldest-first so "Attempt 1" is the first try."""
    assert "OLDEST FIRST" in html or "oldest first" in html


def test_superseded_and_continuation_keep_download(html):
    """M4: demoted cards keep their partial download (data on disk is real)."""
    # Both demotion branches render a Download partial button gated on
    # output_exists.
    assert "const dlPartial = job.output_exists" in html
    assert "actionsHtml = dlPartial" in html


def test_estimated_marker_visible(html):
    """M5: ≈ numbers carry a visible "(estimated)" note, not hover-only."""
    assert "concat(approx ? ['(estimated)'] : [])" in html


def test_progress_tooltip_plain_language(html):
    """H2: de-jargonned tooltip copy."""
    assert (
        "Progress adds up every time this upload was resumed. ≈ means some "
        "attempts overlapped, so the number is an estimate."
    ) in html
    # The old jargon must be gone.
    assert "head lineage vs the deduped upload size" not in html


def test_job_error_escaped(html):
    """H4: job.error is HTML-escaped in BOTH the text and the title attribute."""
    assert "const fullError = esc(job.error);" in html
    assert "const shortError = esc(" in html
    # The raw (unescaped) interpolation must not remain.
    assert "errorInfo = '<div class=\"job-error\" title=\"' + fullError + '\">' + shortError" in html


def test_fragment_reunion_age_gate(html):
    """H3: same-filename fragments only reunite when close in age."""
    assert "FRAGMENT_REUNION_MAX_GAP_MS" in html
    assert "24 * 60 * 60 * 1000" in html


def test_origin_chip_resolution(html):
    """M2: grouped cards resolve the true origin; 'restart' is never shown."""
    assert "originMember" in html
    assert "m.source_type === 'csv_upload' || m.source_type === 'google_maps_chain'" in html
