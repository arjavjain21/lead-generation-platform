"""Guard: every function invoked via a inline onclick/onchange/oninput handler
must be exported to window.

The whole app lives inside a DOMContentLoaded closure (index.html ~line 2423),
so inline HTML attribute handlers resolve against window scope, not the
closure. A function referenced by onclick that lacks a ``window.<fn> =``
export throws ReferenceError on click and its button is silently dead —
no request ever leaves the browser.

This trap has shipped to production three times:
- scraper Stop / Retry buttons (2026-07, noted in the export block itself)
- campaign bucket "Download combined CSV" + "Show jobs" buttons (2026-09-27)

Mirrors the served-HTML assertion convention (test_latam_dropdown_options.py).
"""
import re
from pathlib import Path

import pytest

INDEX_HTML = Path(__file__).parent.parent / "index.html"

# Inline handler attributes the app actually uses (keep in sync if new ones appear).
_HANDLER_ATTRS = ("onclick", "onchange", "oninput")


@pytest.fixture(scope="module")
def html():
    return INDEX_HTML.read_text(encoding="utf-8")


def _strip_full_line_comments(text: str) -> str:
    """Drop // full-line comments.

    Code comments legitimately mention onclick="fn(...)" (the export-block
    warning at the bottom of the script) and must not be scanned as handlers.
    Trailing // comments are kept: they cannot start an onclick= attribute.
    """
    return "\n".join(
        line for line in text.splitlines() if not line.lstrip().startswith("//")
    )


def _invoked_functions(code: str) -> set:
    invoked = set()
    for attr in _HANDLER_ATTRS:
        for quote in ('"', "'"):
            invoked |= set(
                re.findall(rf"{attr}={quote}([A-Za-z_$][\w$]*)\(", code)
            )
    return invoked


def test_inline_handlers_exist(html):
    """Sanity: the scan must find handlers, else the regex drifted silently."""
    invoked = _invoked_functions(_strip_full_line_comments(html))
    assert invoked, "no inline onclick/onchange/oninput handlers found — scan pattern broken?"


def test_every_inline_handler_function_is_window_exported(html):
    code = _strip_full_line_comments(html)
    invoked = _invoked_functions(code)
    exported = set(re.findall(r"window\.([A-Za-z_$][\w$]*)\s*=", code))
    missing = sorted(invoked - exported)
    assert not missing, (
        f"inline handler functions missing a window export "
        f"(their buttons are dead on click): {missing}"
    )


def test_campaign_bucket_buttons_exported(html):
    """2026-09-27 regression: the combined-CSV + show/hide buttons were the
    third silent-dead-button outbreak; pin them explicitly."""
    assert "window.downloadScraperGroup = downloadScraperGroup;" in html
    assert "window.toggleScraperGroup = toggleScraperGroup;" in html
