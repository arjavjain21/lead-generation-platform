"""Tests for enrichment/seed_text.py — Lookalike 2.0 seed-text primitives.

All network interaction is mocked via httpx.MockTransport (fetch tests) —
no test ever hits a real homepage.
"""

from __future__ import annotations

import asyncio
import os
import sys
import unittest

import httpx

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from enrichment import seed_text


class TestTokenizeAndKeywords(unittest.TestCase):
    def test_tokenize_drops_short_numeric_and_stopwords(self):
        tokens = seed_text.tokenize(
            "We are the BEST prediction platform for 12345 fans who love DATA"
        )
        # we/are/the/for/who dropped by length; best/platform/data are
        # stopwords; 12345 is pure digits. Only real tokens survive.
        self.assertEqual(tokens, ["prediction", "fans", "love"])

    def test_multi_seed_requires_shared_tokens(self):
        # "prediction", "markets", "sports" appear in >= 2 of 3 texts;
        # "derivatives" appears in only one -> niche, dropped.
        keywords = seed_text.extract_niche_keywords([
            "prediction markets trading app for sports fans",
            "prediction markets platform sports odds exchange",
            "sports prediction markets events trading",
            "derivatives brokerage research",
        ])
        self.assertIn("prediction", keywords)
        self.assertIn("markets", keywords)
        self.assertIn("sports", keywords)
        self.assertNotIn("derivatives", keywords)
        self.assertNotIn("platform", keywords)  # stopword even though shared

    def test_single_seed_takes_top_frequency(self):
        keywords = seed_text.extract_niche_keywords([
            "trading trading trading odds odds sweepstakes"
        ])
        self.assertEqual(keywords[:3], ["trading", "odds", "sweepstakes"])

    def test_deterministic_ordering_and_cap(self):
        keywords = seed_text.extract_niche_keywords(
            ["alpha beta gamma delta", "alpha beta gamma delta"], max_k=3
        )
        self.assertEqual(len(keywords), 3)
        # Equal counts -> alphabetical tie-break, stable across calls.
        self.assertEqual(keywords, seed_text.extract_niche_keywords(
            ["alpha beta gamma delta", "alpha beta gamma delta"], max_k=3
        ))

    def test_empty_inputs(self):
        self.assertEqual(seed_text.extract_niche_keywords([]), [])
        self.assertEqual(seed_text.extract_niche_keywords(["", ""]), [])


class TestTrigramSimilarity(unittest.TestCase):
    def test_identical_is_one(self):
        self.assertEqual(
            seed_text.trigram_similarity(
                "prediction markets for sports", "prediction markets for sports"
            ),
            1.0,
        )

    def test_disjoint_is_zero(self):
        self.assertEqual(
            seed_text.trigram_similarity("zzzz qqqq vvvv", "bbbb wwwww jjjj"),
            0.0,
        )

    def test_partial_is_between(self):
        sim = seed_text.trigram_similarity(
            "prediction markets", "prediction market"
        )
        self.assertGreater(sim, 0.0)
        self.assertLess(sim, 1.0)

    def test_empty_sides_are_zero(self):
        self.assertEqual(seed_text.trigram_similarity("", ""), 0.0)
        self.assertEqual(seed_text.trigram_similarity("real text", ""), 0.0)

    def test_case_and_punctuation_normalized(self):
        self.assertEqual(
            seed_text.trigram_similarity("Prediction-Markets!", "prediction markets"),
            1.0,
        )


class TestStripHtml(unittest.TestCase):
    def test_scripts_styles_tags_entities_removed(self):
        html = (
            "<html><head><style>body{color:red}</style></head>"
            "<body><script>tracker()</script>"
            "<h1>Kalshi &amp; Prediction</h1><p>Markets for sports</p>"
            "</body></html>"
        )
        text = seed_text.strip_html(html)
        self.assertEqual(text, "kalshi and prediction markets for sports")
        self.assertNotIn("<", text)


def _client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


class TestFetchHomepageText(unittest.TestCase):
    def _run(self, coro):
        return asyncio.run(coro)

    def test_200_html_is_cleaned_and_lowercased(self):
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.host == "kalshi.com"
            return httpx.Response(
                200, text="<html><body><h1>PREDICTION Markets</h1></body></html>"
            )

        async def go():
            async with _client(handler) as client:
                return await seed_text.fetch_homepage_text(client, "kalshi.com")

        self.assertEqual(self._run(go()), "prediction markets")

    def test_www_fallback_after_failure(self):
        seen: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request.url.host)
            if request.url.host == "acme.com":
                raise httpx.ConnectError("refused")
            return httpx.Response(200, text="<p>Acme tools</p>")

        async def go():
            async with _client(handler) as client:
                return await seed_text.fetch_homepage_text(client, "acme.com")

        self.assertEqual(self._run(go()), "acme tools")
        self.assertEqual(seen, ["acme.com", "www.acme.com"])

    def test_exception_never_raises_returns_empty(self):
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ReadTimeout("too slow")

        async def go():
            async with _client(handler) as client:
                return await seed_text.fetch_homepage_text(client, "down.com")

        self.assertEqual(self._run(go()), "")

    def test_non_200_returns_empty(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(403, text="forbidden")

        async def go():
            async with _client(handler) as client:
                return await seed_text.fetch_homepage_text(client, "blocked.com")

        self.assertEqual(self._run(go()), "")

    def test_invalid_domains_short_circuit(self):
        async def go():
            async with _client(lambda req: httpx.Response(200, text="x")) as client:
                return [
                    await seed_text.fetch_homepage_text(client, ""),
                    await seed_text.fetch_homepage_text(client, "not a domain"),
                    await seed_text.fetch_homepage_text(client, "localhost"),
                ]

        self.assertEqual(self._run(go()), ["", "", ""])

    def test_text_capped_at_max_chars(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, text="word " * 100_000)

        async def go():
            async with _client(handler) as client:
                return await seed_text.fetch_homepage_text(client, "big.com")

        self.assertEqual(len(self._run(go())), seed_text.MAX_TEXT_CHARS)


class TestCombinedSha(unittest.TestCase):
    def test_stable_and_distinct(self):
        self.assertEqual(
            seed_text.combined_sha(["a", "b"]),
            seed_text.combined_sha(["a", "b"]),
        )
        self.assertNotEqual(
            seed_text.combined_sha(["a"]), seed_text.combined_sha(["b"])
        )
        self.assertEqual(len(seed_text.combined_sha([])), 12)


if __name__ == "__main__":
    unittest.main()
