"""Sidebar scroll fix tests (2026-11-27).

Served-HTML assertion convention (see test_tam_page_ui.py). Pins the flex
scroll-trap fix: with 13 nav items the sidebar outgrew shorter viewports
(MacBook @100% zoom) and — because .sidebar used min-height instead of
height — the fixed element grew past the viewport instead of letting
.sidebar-nav scroll, hiding the user badge + Sign out button entirely.

The fix: .sidebar height 100vh (clamped) + .sidebar-nav flex/overflow +
.sidebar-footer flex-shrink 0 (pinned). These tests keep anyone from
reintroducing min-height or dropping the scroll container.
"""

import re
from pathlib import Path

import pytest

INDEX = Path(__file__).parent.parent / "index.html"


@pytest.fixture(scope="module")
def html() -> str:
    return INDEX.read_text(encoding="utf-8")


def _rule(html: str, selector: str) -> str:
    m = re.search(re.escape(selector) + r"\s*\{([^}]*)\}", html)
    assert m, f"rule not found: {selector}"
    return m.group(1)


class TestSidebarScroll:
    def test_sidebar_clamped_to_viewport_height(self, html):
        rule = _rule(html, ".sidebar")
        assert "height: 100vh" in rule
        # The bug: min-height lets the fixed sidebar grow past the viewport,
        # disabling the nav's overflow scroll and pushing the footer away.
        assert "min-height" not in rule
        assert "display: flex" in rule and "flex-direction: column" in rule
        assert "position: fixed" in rule

    def test_nav_is_the_scroll_container(self, html):
        rule = _rule(html, ".sidebar-nav")
        assert "overflow-y: auto" in rule
        assert "flex: 1" in rule
        assert "min-height: 0" in rule  # without it, flex children refuse to shrink

    def test_footer_pinned_not_scrolled_away(self, html):
        rule = _rule(html, ".sidebar-footer")
        assert "flex-shrink: 0" in rule
