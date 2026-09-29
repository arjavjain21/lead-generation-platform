"""Website-email followup UI (2026-09-29) — served-HTML structure assertions.

The followup is the second phase of domain enrichment jobs: the waterfall's
without-email domains are submitted to the webscrapedash scrape-emails API
(auto for eligible jobs, manual backfill button for historical ones); while
the remote batch processes the job card shows a progress chip, and once done
a second download button appears. These tests pin the structures that make
that work, mirroring the convention in test_inline_onclick_exports.py /
test_tam_page_ui.py:

1. Card builder branches on job.webscrape_followup: done → second download
   button; processing → status chip (+ETA); cancelled/failed handled.
2. Backfill button appears for done jobs with output and no existing followup
   (and is suppressed for website_only jobs).
3. The three handler functions exist AND are window-exported (the dead-button
   trap that shipped three times — see test_inline_onclick_exports.py).
4. The download uses the native ?token= pattern (2026-09-25 truncation fix),
   not fetch+blob.
"""

import re
from pathlib import Path

import pytest

INDEX_HTML = Path(__file__).parent.parent / "index.html"


@pytest.fixture(scope="module")
def html():
    return INDEX_HTML.read_text(encoding="utf-8")


def test_card_builder_reads_followup_payload(html):
    assert "job.webscrape_followup" in html


def test_second_download_button_when_done(html):
    card_start = html.index("const wf = job.webscrape_followup;")
    block = html[card_start - 1500 : card_start + 2500]
    assert "downloadWebsiteEmails(" in block
    assert "wf.status === 'done'" in block
    assert "found)" in block  # "(N found)" label


def test_progress_chip_with_eta_while_processing(html):
    card_start = html.index("const wf = job.webscrape_followup;")
    block = html[card_start - 1500 : card_start + 2500]
    assert "wf.status === 'processing'" in block
    assert "ETA" in block
    assert "cancelWebsiteEmailFollowup(" in block


def test_backfill_button_guards(html):
    card_start = html.index("const wf = job.webscrape_followup;")
    block = html[card_start - 1500 : card_start + 2500]
    assert "startWebsiteEmailFollowup(" in block
    # Only for finished jobs with a downloadable file, never website_only.
    assert "job.status === 'done'" in block
    assert "job.output_exists" in block
    assert "!job.website_only" in block


def test_download_uses_native_token_pattern(html):
    block = html[html.index("function downloadWebsiteEmails"):][:1200]
    assert "/webscrape-followup/download?token=" in block
    assert "triggerNativeDownload" in block


def test_estimate_dialog_shows_cached_vs_fresh_split(html):
    block = html[html.index("function startWebsiteEmailFollowup"):][:2500]
    assert "/webscrape-followup/estimate" in block
    assert "already_done" in block
    assert "confirm(" in block


@pytest.mark.parametrize(
    "fn",
    ["downloadWebsiteEmails", "startWebsiteEmailFollowup", "cancelWebsiteEmailFollowup"],
)
def test_handlers_defined_and_window_exported(html, fn):
    assert f"function {fn}(" in html, f"{fn} must be defined"
    assert f"window.{fn} = {fn};" in html, (
        f"{fn} is used from inline onclick but not exported to window — "
        "its button will be dead on click (see test_inline_onclick_exports.py)"
    )


def test_no_stray_followup_duplicate_definitions(html):
    assert len(re.findall(r"function downloadWebsiteEmails\(", html)) == 1
    assert len(re.findall(r"function startWebsiteEmailFollowup\(", html)) == 1
