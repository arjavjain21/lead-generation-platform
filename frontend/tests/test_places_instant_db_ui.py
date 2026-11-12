"""UI tests for the Instant US Database source on the Google Maps Scraper
page (GetLeads places, 2026-11-11 phase 1).

Validates against the served single-file app (frontend/index.html):
- The Data Source selector exists with the live + instant options.
- The free match-preview badge group + has-email checkbox exist.
- The JS functions for the instant path are defined (source toggle, count
  refresh, GetLeads category suggest, job start, progress stream, retry).
- retryPlacesJob is exported to window (inline onclick resolves via window —
  the 3-outbreak bug class this repo test-guards).
- The scraper start handler branches to the places endpoint BEFORE the
  scraper.tech payload build, and the job card renders a places-aware
  branch (source_type check + retryPlacesJob wiring).
- Tag balance sanity (a missing </div> blanked whole pages before — 9daa4b2).
"""

import re
from pathlib import Path

import pytest

INDEX_HTML = Path(__file__).parent.parent / "index.html"


@pytest.fixture(scope="module")
def html_content():
    return INDEX_HTML.read_text()


class TestPlacesSourceUI:
    def test_source_selector_present(self, html_content):
        assert 'id="scraperSource"' in html_content
        assert '<option value="live" selected>' in html_content
        assert '<option value="instant">⚡ Instant US Database (emails included)</option>' in html_content

    def test_count_badge_group_present(self, html_content):
        assert 'id="placesCountGroup"' in html_content
        assert 'id="placesCountBadge"' in html_content
        assert 'id="placesHasEmail"' in html_content
        assert 'id="placesSourceHint"' in html_content

    def test_source_toggle_functions_defined(self, html_content):
        for fn in (
            "function scraperSourceIsInstant",
            "function updateScraperSourceUI",
            "function placesCurrentFilters",
            "function loadPlacesFairUse",
            "function refreshPlacesCount",
            "function placesCategorySuggest",
            "function startPlacesJobFromForm",
            "function startPlacesProgressStream",
            "function retryPlacesJob",
        ):
            assert fn in html_content, f"missing {fn}"

    def test_retry_places_window_export(self, html_content):
        assert "window.retryPlacesJob = retryPlacesJob" in html_content

    def test_start_handler_branches_before_scraper_payload(self, html_content):
        # The instant branch must appear inside the start click handler but
        # BEFORE the scraper.tech payload build (the payload line is unique
        # to the click handler; the mode-read line appears 4x in the file).
        instant_branch = html_content.find("if (scraperSourceIsInstant())")
        payload_build = html_content.find("const payload = { query: query, mode: mode, country: country };")
        click_handler = html_content.find("document.getElementById('startScraperJob').addEventListener('click'")
        assert click_handler > 0
        assert instant_branch > 0
        assert payload_build > 0
        assert click_handler < instant_branch < payload_build

    def test_places_post_endpoint_used(self, html_content):
        assert "'/api/scraper/places/jobs'" in html_content
        assert "'/api/scraper/places/count'" in html_content
        assert "'/api/scraper/places/categories?q='" in html_content

    def test_job_card_places_branch(self, html_content):
        assert "job.source_type === 'getleads_places'" in html_content
        assert "retryPlacesJob(\\''+job.job_id+'\\')" in html_content

    def test_exact_category_tracking(self, html_content):
        # Picked suggestions send exact categories; typed text degrades to
        # category_contains. Both the tracker and the picker must exist.
        assert "let placesExactCategory = null;" in html_content
        assert "placesExactCategory = cat;" in html_content
        assert "filters.categories = [placesExactCategory]" in html_content
        assert "filters.category_contains = [queryText.trim()]" in html_content

    def test_tag_balance(self, html_content):
        opens = len(re.findall(r"<div\b", html_content))
        closes = len(re.findall(r"</div>", html_content))
        assert opens == closes, f"div imbalance: {opens} open vs {closes} close"

    def test_script_tag_balance(self, html_content):
        assert html_content.count("<script") == html_content.count("</script>")


class TestPlacesPhase23UI:
    """Phase 2/3 (2026-11-12): city input, cached badge, live fallback."""

    def test_city_input_present(self, html_content):
        assert 'id="placesCitiesInput"' in html_content

    def test_cities_sent_in_filters(self, html_content):
        assert "filters.cities = cities" in html_content

    def test_cached_badge_text(self, html_content):
        assert "⚡ CACHED — instant & free" in html_content
        assert "data.cached" in html_content

    def test_partition_message_not_an_error(self, html_content):
        # Oversize pulls are now auto-split — the badge must not read as a
        # dead end anymore.
        assert "auto-split into parts" in html_content

    def test_live_fallback_function_and_export(self, html_content):
        assert "function retryPlacesJobLive" in html_content
        assert "window.retryPlacesJobLive = retryPlacesJobLive" in html_content

    def test_live_fallback_button_on_budget_failures(self, html_content):
        assert "budget is used up" in html_content
        assert "Scrape Live Instead" in html_content

    def test_stream_honors_replanned_totals(self, html_content):
        # Multi-partition jobs raise total_tasks mid-run; the progress bar
        # must follow the server's number.
        assert "if (data.total_tasks) {" in html_content
