"""Blitz client 422 URL guard + find_people_batch + tam_by_people + bracket_exact.

Why the guard exists: 30-day telemetry recorded 215,898 HTTP 422s on
POST /v2/enrichment/email — every one a malformed person LinkedIn URL
(company pages, non-LinkedIn hosts, whitespace-padded strings) that reached
the endpoint. The guard rejects them at the client boundary for ALL call
sites, returning the not-found shape at zero cost and logging the REASON
only (never the URL itself).

find_people_batch is the batch DM-discovery endpoint (POST /v2/search/people,
max 50 companies/call, cursor-paginated, 1 FUP record/result). These tests
pin: the waterfall-flat row mapping (title from the CURRENT experience),
grouping under normalized company URLs, and all three pagination stop
conditions (cursor null / every company at target / max_pages).

tam_by_people is a payload passthrough (POST /v2/company/tam-by-people).

Async pattern: this project does NOT use pytest-asyncio — async code is
driven with ``asyncio.run(...)`` inside sync test functions, matching
``test_blitz_person_enrich_guard.py``. HTTP is faked by patching
``blitz_client._post_with_retry``.
"""
from __future__ import annotations

import asyncio
import os
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from enrichment import blitz_client as bc  # noqa: E402


# ---------------------------------------------------------------------------
# 1. _valid_person_linkedin_url — the 422 guard matrix
# ---------------------------------------------------------------------------


class TestValidPersonLinkedinUrl:
    @pytest.mark.parametrize(
        "url",
        [
            "https://www.linkedin.com/in/johndoe",
            "https://linkedin.com/in/johndoe",
            "https://uk.linkedin.com/in/johndoe",
            "https://www.linkedin.com/in/johndoe/",
            "https://www.linkedin.com/in/john-doe-123abc?trk=public_profile",
        ],
    )
    def test_valid_urls_pass(self, url):
        valid, reason = bc._valid_person_linkedin_url(url)
        assert valid is True, f"{url!r} must be valid, got reason {reason!r}"

    @pytest.mark.parametrize(
        "url,expected_reason_fragment",
        [
            ("", "empty"),
            ("   ", "empty"),
            (None, "not a string"),
            (123, "not a string"),
            ("https://www.linkedin.com/company/acme", "/in/"),
            ("https://www.linkedin.com/school/stanford", "/in/"),
            ("https://www.linkedin.com/in/john doe", "whitespace"),
            (" https://www.linkedin.com/in/johndoe", "whitespace"),
            ("https://facebook.com/in/johndoe", "host"),
            ("https://notlinkedin.com/in/johndoe", "host"),
            ("linkedin.com/in/johndoe", "host"),  # no scheme -> empty netloc
        ],
    )
    def test_invalid_urls_rejected_with_reason(self, url, expected_reason_fragment):
        valid, reason = bc._valid_person_linkedin_url(url)
        assert valid is False, f"{url!r} must be rejected"
        assert expected_reason_fragment in reason, (
            f"reason {reason!r} must mention {expected_reason_fragment!r}"
        )

    def test_reason_never_contains_the_url(self):
        secret = "https://www.linkedin.com/company/leaky-url-xyz"
        _valid, reason = bc._valid_person_linkedin_url(secret)
        assert "leaky-url-xyz" not in reason


# ---------------------------------------------------------------------------
# 2. Guard applied at the top of both /v2/enrichment/email call sites
# ---------------------------------------------------------------------------


class TestGuardApplication:
    def _posted(self):
        posted = AsyncMock(return_value={"found": True, "email": "x@y.com"})
        patcher = patch.object(bc, "_post_with_retry", posted)
        return posted, patcher

    def test_find_work_email_invalid_returns_not_found_without_http(self):
        posted, patcher = self._posted()
        with patcher:
            result = asyncio.run(
                bc.find_work_email(MagicMock(), "https://www.linkedin.com/company/acme")
            )
        assert result == {"found": False, "email": None, "all_emails": []}
        posted.assert_not_called()

    def test_find_work_email_valid_still_calls_blitz(self):
        posted, patcher = self._posted()
        with patcher:
            result = asyncio.run(
                bc.find_work_email(MagicMock(), "https://www.linkedin.com/in/johndoe")
            )
        assert result["found"] is True
        posted.assert_called_once()

    def test_person_enrich_by_linkedin_invalid_returns_not_found_without_http(self):
        posted, patcher = self._posted()
        with patcher:
            result = asyncio.run(
                bc.person_enrich_by_linkedin(MagicMock(), "https://facebook.com/in/johndoe")
            )
        assert result == {"found": False, "email": None, "all_emails": []}
        posted.assert_not_called()

    def test_person_enrich_by_linkedin_valid_still_calls_blitz(self):
        posted, patcher = self._posted()
        with patcher:
            result = asyncio.run(
                bc.person_enrich_by_linkedin(MagicMock(), "https://uk.linkedin.com/in/jane")
            )
        assert result["found"] is True
        posted.assert_called_once()

    def test_guard_logs_reason_but_never_the_url(self, caplog):
        secret = "https://www.linkedin.com/in/hidden-person-42"
        with patch.object(bc, "_post_with_retry", AsyncMock()):
            with caplog.at_level("WARNING", logger="enrichment.blitz_client"):
                asyncio.run(bc.find_work_email(MagicMock(), f"{secret} with space"))
        joined = "\n".join(record.getMessage() for record in caplog.records)
        assert "blitz email skipped invalid linkedin url" in joined
        assert "whitespace" in joined
        assert "hidden-person-42" not in joined, "the URL itself must never be logged"


# ---------------------------------------------------------------------------
# 3. bracket_exact
# ---------------------------------------------------------------------------


class TestBracketExact:
    def test_wraps_each_title(self):
        assert bc.bracket_exact(["CEO", "VP Sales"]) == ["[CEO]", "[VP Sales]"]

    def test_empty_list(self):
        assert bc.bracket_exact([]) == []

    def test_does_not_mutate_input(self):
        titles = ["CEO"]
        _result = bc.bracket_exact(titles)
        assert titles == ["CEO"]

    def test_double_wrap_is_callers_choice(self):
        # bracket_exact is a pure wrapper: it does not inspect whether a
        # value is already bracketed (callers pass raw titles).
        assert bc.bracket_exact(["[CEO]"]) == ["[[CEO]]"]


# ---------------------------------------------------------------------------
# 4. find_people_batch
# ---------------------------------------------------------------------------


def _person(
    linkedin_url: str,
    company_url: str,
    title: str = "CEO",
    first: str = "John",
    last: str = "Doe",
    past_company_url: str = "https://www.linkedin.com/company/old-co/",
) -> dict:
    """One /v2/search/people result: CURRENT experience second in the list,
    to prove current-company inference is not just experiences[0]."""
    return {
        "first_name": first,
        "last_name": last,
        "full_name": f"{first} {last}",
        "headline": f"{title} doing things",
        "linkedin_url": linkedin_url,
        "location": {"city": "Austin", "state_code": "TX", "country_code": "US"},
        "experiences": [
            {
                "company_name": "Old Co",
                "company_linkedin_url": past_company_url,
                "job_title": "Founder",
                "job_is_current": False,
            },
            {
                "company_name": "Acme",
                "company_linkedin_url": company_url,
                "job_title": title,
                "job_is_current": True,
            },
        ],
    }


class TestFindPeopleBatchMapping:
    def test_row_mapping_is_waterfall_flat(self):
        acme = "https://www.linkedin.com/company/acme/"
        page = {"results": [_person("https://www.linkedin.com/in/john", acme)], "cursor": None}
        with patch.object(bc, "_post_with_retry", AsyncMock(return_value=page)):
            grouped = asyncio.run(bc.find_people_batch(MagicMock(), [acme]))

        rows = grouped["www.linkedin.com/company/acme"]
        assert len(rows) == 1
        row = rows[0]
        assert row["first_name"] == "John"
        assert row["last_name"] == "Doe"
        assert row["full_name"] == "John Doe"
        # Title must come from the CURRENT experience (Acme/CEO), not the
        # stale first entry (Old Co/Founder).
        assert row["title"] == "CEO"
        assert row["job_level"] is None
        assert row["linkedin_url"] == "https://www.linkedin.com/in/john"
        assert row["email"] is None
        assert row["verified_email"] is None
        assert row["headline"] == "CEO doing things"
        assert row["location_city"] == "Austin"
        assert row["location_country"] == "US"
        assert row["icp_tier"] == 1
        assert row["ranking"] == 1
        # Raw experiences kept verbatim for downstream extraction.
        assert row["experiences"][0]["company_name"] == "Old Co"
        assert row["experiences"][1]["job_is_current"] is True

    def test_full_name_falls_back_to_first_plus_last(self):
        acme = "https://www.linkedin.com/company/acme"
        person = _person("https://www.linkedin.com/in/john", acme)
        person["full_name"] = None
        page = {"results": [person], "cursor": None}
        with patch.object(bc, "_post_with_retry", AsyncMock(return_value=page)):
            grouped = asyncio.run(bc.find_people_batch(MagicMock(), [acme]))
        assert grouped["www.linkedin.com/company/acme"][0]["full_name"] == "John Doe"

    def test_grouping_normalizes_host_case_and_trailing_slash(self):
        requested = "https://WWW.LinkedIn.com/company/acme/"
        person = _person("https://www.linkedin.com/in/john", "https://www.linkedin.com/company/acme")
        page = {"results": [person], "cursor": None}
        with patch.object(bc, "_post_with_retry", AsyncMock(return_value=page)):
            grouped = asyncio.run(bc.find_people_batch(MagicMock(), [requested]))
        assert len(grouped["www.linkedin.com/company/acme"]) == 1, (
            "host case + trailing slash must collapse to one key"
        )

    def test_grouping_preserves_path_case(self):
        # Only the HOST is lowercased (per contract); a mixed-case path is a
        # different key, so a lowercase person href stays unattributable.
        requested = "https://www.linkedin.com/Company/Acme/"
        person = _person("https://www.linkedin.com/in/john", "https://www.linkedin.com/company/acme")
        page = {"results": [person], "cursor": None}
        with patch.object(bc, "_post_with_retry", AsyncMock(return_value=page)):
            grouped = asyncio.run(bc.find_people_batch(MagicMock(), [requested]))
        assert grouped["www.linkedin.com/Company/Acme"] == []

    def test_person_without_current_experience_is_skipped(self):
        acme = "https://www.linkedin.com/company/acme"
        person = _person("https://www.linkedin.com/in/john", acme)
        person["experiences"] = [
            {"company_linkedin_url": acme, "job_title": "Ex-CEO", "job_is_current": False}
        ]
        page = {"results": [person], "cursor": None}
        with patch.object(bc, "_post_with_retry", AsyncMock(return_value=page)):
            grouped = asyncio.run(bc.find_people_batch(MagicMock(), [acme]))
        assert grouped["www.linkedin.com/company/acme"] == []

    def test_person_from_unrequested_company_is_skipped(self):
        acme = "https://www.linkedin.com/company/acme"
        person = _person("https://www.linkedin.com/in/john", "https://www.linkedin.com/company/other")
        page = {"results": [person], "cursor": None}
        with patch.object(bc, "_post_with_retry", AsyncMock(return_value=page)):
            grouped = asyncio.run(bc.find_people_batch(MagicMock(), [acme]))
        assert grouped["www.linkedin.com/company/acme"] == []

    def test_ranking_is_per_company_one_based(self):
        acme = "https://www.linkedin.com/company/acme"
        beta = "https://www.linkedin.com/company/beta"
        page = {
            "results": [
                _person("https://www.linkedin.com/in/p1", acme, title="CEO"),
                _person("https://www.linkedin.com/in/p2", beta, title="CTO"),
                _person("https://www.linkedin.com/in/p3", acme, title="CFO", first="J", last="D"),
            ],
            "cursor": None,
        }
        with patch.object(bc, "_post_with_retry", AsyncMock(return_value=page)):
            grouped = asyncio.run(bc.find_people_batch(MagicMock(), [acme, beta]))
        assert [row["ranking"] for row in grouped["www.linkedin.com/company/acme"]] == [1, 2]
        assert [row["ranking"] for row in grouped["www.linkedin.com/company/beta"]] == [1]

    def test_empty_company_list_returns_empty_dict_without_http(self):
        posted = AsyncMock()
        with patch.object(bc, "_post_with_retry", posted):
            grouped = asyncio.run(bc.find_people_batch(MagicMock(), []))
        assert grouped == {}
        posted.assert_not_called()


class TestFindPeopleBatchPayload:
    def _capture(self, pages):
        payloads = []

        async def fake_post(_client, url, payload, timeout):
            payloads.append({"url": url, "payload": payload, "timeout": timeout})
            return pages.pop(0)

        return payloads, fake_post

    def test_payload_shape_and_people_filters(self):
        acme = "https://www.linkedin.com/company/acme"
        payloads, fake = self._capture([{"results": [], "cursor": None}])
        with patch.object(bc, "_post_with_retry", fake):
            asyncio.run(
                bc.find_people_batch(
                    MagicMock(),
                    [acme],
                    job_title_include=["[CEO]"],
                    job_title_exclude=["intern"],
                    job_levels=["owner", "c_suite"],
                )
            )
        assert len(payloads) == 1
        request = payloads[0]
        assert request["url"] == f"{bc.BLITZ_BASE_URL}/v2/search/people"
        assert request["timeout"] == 60.0
        assert request["payload"]["company"] == {"linkedin_url": [acme]}
        assert request["payload"]["max_results"] == 50
        assert request["payload"]["people"] == {
            "job_title": {"include": ["[CEO]"], "exclude": ["intern"]},
            "job_level": ["owner", "c_suite"],
        }
        assert "cursor" not in request["payload"], "first page must omit the cursor"

    def test_no_people_filters_omits_people_key(self):
        payloads, fake = self._capture([{"results": [], "cursor": None}])
        with patch.object(bc, "_post_with_retry", fake):
            asyncio.run(bc.find_people_batch(MagicMock(), ["https://www.linkedin.com/company/x"]))
        assert "people" not in payloads[0]["payload"]

    def test_cursor_sent_on_second_page(self):
        acme = "https://www.linkedin.com/company/acme"
        page1 = {"results": [], "cursor": "cursor-abc"}
        page2 = {"results": [], "cursor": None}
        payloads, fake = self._capture([page1, page2])
        with patch.object(bc, "_post_with_retry", fake):
            asyncio.run(bc.find_people_batch(MagicMock(), [acme]))
        assert len(payloads) == 2
        assert payloads[1]["payload"]["cursor"] == "cursor-abc"


class TestFindPeopleBatchPaginationStops:
    def _acme_pages(self, n_results_page1: int, cursor_after_page1):
        acme = "https://www.linkedin.com/company/acme"
        page1 = {
            "results": [
                _person(f"https://www.linkedin.com/in/p{i}", acme, title=f"T{i}")
                for i in range(n_results_page1)
            ],
            "cursor": cursor_after_page1,
        }
        return acme, page1

    def test_stops_when_cursor_null(self):
        acme, page1 = self._acme_pages(1, None)
        posted = AsyncMock(return_value=page1)
        with patch.object(bc, "_post_with_retry", posted):
            grouped = asyncio.run(bc.find_people_batch(MagicMock(), [acme]))
        assert posted.await_count == 1
        assert len(grouped["www.linkedin.com/company/acme"]) == 1

    def test_stops_when_every_company_reaches_target(self):
        acme, page1 = self._acme_pages(5, "cursor-more")
        beta = "https://www.linkedin.com/company/beta"
        page1 = {**page1, "results": page1["results"] + [
            _person(f"https://www.linkedin.com/in/q{i}", beta, title=f"Q{i}") for i in range(5)
        ]}
        posted = AsyncMock(return_value=page1)
        with patch.object(bc, "_post_with_retry", posted):
            grouped = asyncio.run(
                bc.find_people_batch(MagicMock(), [acme, beta], target_per_company=5)
            )
        assert posted.await_count == 1, "all companies at target -> no second page"
        assert len(grouped["www.linkedin.com/company/acme"]) == 5
        assert len(grouped["www.linkedin.com/company/beta"]) == 5

    def test_paginates_while_any_company_below_target(self):
        acme, page1 = self._acme_pages(5, "cursor-more")
        beta = "https://www.linkedin.com/company/beta"
        page1 = {**page1, "results": page1["results"] + [
            _person("https://www.linkedin.com/in/q1", beta, title="Q")
        ]}
        page2 = {
            "results": [_person("https://www.linkedin.com/in/q2", beta, title="Q2", first="A", last="B")],
            "cursor": None,
        }
        posted = AsyncMock(side_effect=[page1, page2])
        with patch.object(bc, "_post_with_retry", posted):
            grouped = asyncio.run(
                bc.find_people_batch(MagicMock(), [acme, beta], target_per_company=5)
            )
        assert posted.await_count == 2, "beta below target -> one more page"
        assert len(grouped["www.linkedin.com/company/beta"]) == 2

    def test_stops_at_max_pages_even_with_cursor_and_deficit(self):
        acme = "https://www.linkedin.com/company/acme"
        pages = [
            {
                "results": [_person("https://www.linkedin.com/in/p1", acme)],
                "cursor": f"cursor-{i}",
            }
            for i in range(9)  # server always has more
        ]
        pages[-1] = {"results": [], "cursor": "cursor-last"}
        posted = AsyncMock(side_effect=pages)
        with patch.object(bc, "_post_with_retry", posted):
            grouped = asyncio.run(
                bc.find_people_batch(MagicMock(), [acme], target_per_company=5, max_pages=3)
            )
        assert posted.await_count == 3, "max_pages is a hard cap"
        assert len(grouped["www.linkedin.com/company/acme"]) == 3

    def test_error_propagates_for_caller_fallback(self):
        posted = AsyncMock(side_effect=httpx.ConnectError("blitz unreachable"))
        with patch.object(bc, "_post_with_retry", posted):
            with pytest.raises(httpx.ConnectError):
                asyncio.run(bc.find_people_batch(MagicMock(), ["https://www.linkedin.com/company/x"]))
        # Documented contract: unwrapped propagation, no swallowing here —
        # blitz_batch's prepass owns the per-chunk fallback.


# ---------------------------------------------------------------------------
# 5. tam_by_people — payload passthrough
# ---------------------------------------------------------------------------


class TestTamByPeople:
    def test_payload_passthrough(self):
        posted = AsyncMock(return_value={"results": [], "cursor": None})
        with patch.object(bc, "_post_with_retry", posted):
            result = asyncio.run(
                bc.tam_by_people(
                    MagicMock(),
                    company_filters={"employee_range": ["11-50"]},
                    people_filters={"job_title": {"include": ["[CEO]"]}},
                    max_results=25,
                    cursor="page-2",
                )
            )
        posted.assert_called_once()
        args = posted.call_args.args  # _post_with_retry(client, url, payload, timeout)
        assert args[1] == f"{bc.BLITZ_BASE_URL}/v2/company/tam-by-people"
        assert args[2] == {
            "company": {"employee_range": ["11-50"]},
            "people": {"job_title": {"include": ["[CEO]"]}},
            "max_results": 25,
            "cursor": "page-2",
        }
        assert posted.call_args.kwargs.get("timeout") == 60.0
        assert result == {"results": [], "cursor": None}

    def test_cursor_omitted_when_none(self):
        posted = AsyncMock(return_value={"results": [], "cursor": None})
        with patch.object(bc, "_post_with_retry", posted):
            asyncio.run(
                bc.tam_by_people(
                    MagicMock(),
                    company_filters={},
                    people_filters={},
                )
            )
        payload = posted.call_args.args[2]
        assert "cursor" not in payload
        assert payload["max_results"] == 50
