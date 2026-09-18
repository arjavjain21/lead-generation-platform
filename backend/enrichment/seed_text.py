"""Deterministic seed-text utilities for Lookalike 2.0 (no LLM, no embeddings).

Three pure-ish primitives used by the ranked lookalike flow:

* ``fetch_homepage_text`` — best-effort GET of a company homepage, reduced
  to clean lowercase prose. NEVER raises: any failure returns ``""`` so a
  dead/blocked site can never fail a lookalike run.
* ``extract_niche_keywords`` — tokens shared by >= 2 seed homepages (or
  top-frequency for a single seed), minus a generic business/web stopword
  list. Deterministic ordering (doc count, total frequency, alphabetical).
* ``trigram_similarity`` — character-3-gram set Jaccard (0..1), the cheap
  "do these two companies describe themselves similarly" signal.
"""

from __future__ import annotations

import hashlib
import logging
import re
from collections import Counter
from typing import Iterable, Optional

import httpx

logger = logging.getLogger(__name__)

# Fetch profile: 8s hard timeout, browser-ish UA (some sites 403 bare clients),
# redirect-following (homepages love -> marketing / redirects).
HOMEPAGE_TIMEOUT_S = 8.0
HOMEPAGE_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0 Safari/537.36"
)

# Cap on the extracted text (chars). 20KB of prose is far more than the
# keyword/trigram signals need, and keeps memory flat for 5 seeds.
MAX_TEXT_CHARS = 20_000

_TOKEN_RE = re.compile(r"[a-z0-9]+")
_SCRIPT_STYLE_RE = re.compile(r"<(script|style|noscript)\b[^>]*>.*?</\1>",
                              re.IGNORECASE | re.DOTALL)
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")

# Generic business/web boilerplate that appears on virtually every company
# site. These carry zero niche signal, so they never qualify as "niche
# keywords" no matter how many seeds share them. Short tokens (<4 chars) are
# dropped separately by length; they are kept here for clarity/parity.
STOPWORDS: frozenset[str] = frozenset({
    # --- spec-mandated core -------------------------------------------------
    "company", "companies", "platform", "team", "teams", "customer",
    "customers", "solution", "solutions", "service", "services", "product",
    "products", "business", "businesses", "learn", "more", "contact", "us",
    "get", "privacy", "terms", "cookie", "sign", "login", "work", "working",
    "best", "leading", "provider", "providers", "data", "software",
    "technology", "help", "need", "make", "use", "using", "new", "also",
    "like", "including", "based", "inc", "llc", "ltd", "home", "page",
    "site", "website", "menu", "search", "subscribe", "newsletter",
    "copyright", "rights", "reserved",
    # --- web chrome / navigation --------------------------------------------
    "about", "here", "read", "view", "click", "find", "please", "email",
    "phone", "address", "links", "policy", "legal", "information",
    "navigation", "skip", "content", "browser", "javascript", "enable",
    "account", "password", "forgot", "create", "reset", "dashboard",
    "download", "settings", "profile", "logout", "register", "signup",
    # --- generic marketing filler -------------------------------------------
    "world", "people", "today", "better", "great", "free", "full", "easy",
    "simple", "fast", "secure", "trusted", "global", "across", "around",
    "together", "built", "build", "deliver", "delivers", "delivery",
    "offering", "offer", "offerings", "experience", "experiences",
    "expert", "experts", "industry", "industries", "enterprise",
    "enterprises", "organization", "organizations", "management",
    "resources", "support", "pricing", "demo", "request",
    "start", "started", "overview", "features", "case", "studies",
    "stories", "events", "careers", "jobs", "press", "blog", "news",
    "media", "social", "follow", "share", "community", "join",
    "thousand", "thousands", "million", "millions", "value", "values",
    "mission", "vision", "story", "brand", "brands", "clients", "client",
    "partner", "partners", "partnership", "growth", "success",
    "innovation", "innovative", "future", "power", "powerful", "smart",
    "every", "each", "other", "others", "them", "they", "their", "these",
    "those", "there", "where", "which", "while", "when", "what", "will",
    "your", "ours", "with", "from", "that", "this", "have", "been",
    "were", "than", "then", "into", "over", "after", "before", "through",
    "between", "because", "being", "does", "done", "just", "only",
    "very", "much", "many", "most", "some", "same", "well", "ways",
    "way", "thing", "things", "real", "true", "type", "types", "kind",
    "part", "parts", "item", "items", "list", "lists", "name",
    # --- ultra-generic b2b nouns (appear on every SaaS/agency site) ---------
    "tools", "system", "systems", "process", "processes", "quality",
    "performance", "results", "goals", "reports", "insights", "analytics",
    "users", "user", "member", "members", "staff", "office", "offices",
    "location", "locations", "region", "regions", "national", "international",
})


def strip_html(html: str) -> str:
    """HTML -> lowercase prose: drop script/style blocks and tags, collapse
    whitespace. Entity decoding is intentionally minimal (&amp; &quot; etc.
    become spaces via the token filter anyway)."""
    if not html:
        return ""
    text = _SCRIPT_STYLE_RE.sub(" ", html)
    text = _TAG_RE.sub(" ", text)
    text = text.replace("&amp;", " and ").replace("&", " ")
    text = _WS_RE.sub(" ", text).strip().lower()
    return text


async def fetch_homepage_text(client: httpx.AsyncClient, domain: str) -> str:
    """Best-effort homepage text for ``domain``. Tries ``https://{domain}/``
    then ``https://www.{domain}/``; returns cleaned lowercase prose capped at
    ``MAX_TEXT_CHARS``. NEVER raises — every failure path returns ``""``.
    """
    dom = (domain or "").strip().lower().removeprefix("www.").strip("/")
    if not dom or "." not in dom or " " in dom:
        return ""
    urls = [f"https://{dom}/"]
    if not dom.startswith("www."):
        urls.append(f"https://www.{dom}/")
    for url in urls:
        try:
            resp = await client.get(
                url,
                timeout=HOMEPAGE_TIMEOUT_S,
                follow_redirects=True,
                headers={"User-Agent": HOMEPAGE_UA, "Accept": "text/html,*/*"},
            )
            if resp.status_code == 200:
                text = strip_html(resp.text)
                if text:
                    return text[:MAX_TEXT_CHARS]
        except Exception as exc:  # timeout / DNS / TLS / reset — all benign
            logger.debug("Homepage fetch failed for %s (%s): %s", dom, url, exc)
    return ""


def tokenize(text: str) -> list[str]:
    """Lowercase word tokens: runs of [a-z0-9], length >= 4, not pure
    digits, not stopwords."""
    return [
        tok for tok in _TOKEN_RE.findall((text or "").lower())
        if len(tok) >= 4 and not tok.isdigit() and tok not in STOPWORDS
    ]


def extract_niche_keywords(
    seed_texts: list[str], max_k: int = 8
) -> list[str]:
    """Niche keyword tokens shared across seed homepages.

    Multi-seed: tokens appearing in >= 2 of the per-seed token SETS (a token
    counts once per homepage), ranked by (doc count, total frequency, token).
    Single seed: top raw-frequency tokens. Deterministic for equal inputs.
    """
    texts = [t for t in (seed_texts or []) if t]
    if not texts:
        return []
    per_doc_sets = [set(tokenize(t)) for t in texts]
    doc_counts: Counter = Counter()
    total_freq: Counter = Counter()
    for tokens_per_doc in per_doc_sets:
        doc_counts.update(tokens_per_doc)
    for text in texts:
        total_freq.update(tok for tok in tokenize(text) if tok in doc_counts)
    if len(texts) == 1:
        ranked = sorted(
            total_freq, key=lambda tok: (-total_freq[tok], tok)
        )
    else:
        min_docs = 2
        ranked = sorted(
            (tok for tok, docs in doc_counts.items() if docs >= min_docs),
            key=lambda tok: (-doc_counts[tok], -total_freq[tok], tok),
        )
    return ranked[:max(0, max_k)]


def normalize_for_trigram(text: str) -> str:
    """Lowercase, non-alphanumeric -> space, collapsed — the canonical form
    for trigram comparison."""
    return _WS_RE.sub(" ", _TAG_RE.sub(" ", re.sub(r"[^a-z0-9]+", " ", (text or "").lower()))).strip()


def _trigrams(text: str) -> set[str]:
    return {text[i:i + 3] for i in range(len(text) - 2)} if len(text) >= 3 else set()


def trigram_similarity(a: str, b: str) -> float:
    """Character-3-gram set Jaccard on normalized text, 0..1.

    Identical non-empty strings -> 1.0; disjoint (or either side empty) ->
    0.0. Two empty strings count as identical (1.0) so an all-empty seed
    set does not fabricate distance where there is no information.
    """
    na, nb = normalize_for_trigram(a), normalize_for_trigram(b)
    if na == nb:
        return 1.0 if na else 0.0
    ga, gb = _trigrams(na), _trigrams(nb)
    if not ga or not gb:
        return 0.0
    return len(ga & gb) / len(ga | gb)


def combined_sha(texts: Iterable[str]) -> str:
    """Stable 12-hex fingerprint of the extracted seed texts (provenance for
    the analyze response: same texts -> same sha, no storage of the texts)."""
    joined = "\n".join(t or "" for t in texts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()[:12]


def top_tokens(text: str, max_k: int = 8) -> list[str]:
    """Top-frequency tokens of ONE text (per-seed website keywords chip data)."""
    counts = Counter(tokenize(text))
    return [tok for tok, _ in counts.most_common(max_k)]


def is_stopword(token: str) -> bool:  # pragma: no cover - trivial helper
    return (token or "").lower() in STOPWORDS


def optional_concat(parts: Optional[Iterable[Optional[str]]]) -> str:
    """Join truthy string parts with spaces (small immutability-friendly
    helper used when assembling candidate text)."""
    return " ".join(str(p) for p in (parts or []) if p)
