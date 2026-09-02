"""Local title-ICP gate shared by list_builder and pipeline.

Providers (Blitz waterfall especially) run FUZZY server-side matching:
``include_headline_search: true`` + a title like "President" returns
"Vice President of Product Management", "Chief Revenue Officer", etc.
A 2026-08-25 production job (a75c4cae) showed 77% of enriched rows not
matching the user's requested titles — 100% of them discovered via the
Blitz waterfall, which never passed through any local check.

This module re-applies the user's include/exclude titles LOCALLY after
every discovery path (Contacts DB, Blitz waterfall, generic fallbacks)
so the CSV only ever contains people the user actually asked for.

Activation rule: the gate applies ONLY when the request carried titles
(a cascade_config with include_title/exclude_title). No titles → no
filtering (today's behavior). ``strict_titles=false`` on the request
disables the gate entirely (escape hatch for volume-over-precision).

Import safety: this module imports NOTHING from the enrichment package,
so both list_builder.py and pipeline.py can import it without cycles.
"""
from __future__ import annotations

import json
import os
from typing import Any, Optional

# Pool size: how many Contacts-DB people to fetch per domain when a title
# filter is active, so the title matches are present before filtering.
TITLE_SEARCH_POOL = int(os.getenv("TITLE_SEARCH_POOL", "50"))

_TITLE_SYNONYMS = {
    "ceo": ("chief executive officer", "chief exec"),
    "cto": ("chief technology officer", "chief tech"),
    "cfo": ("chief financial officer",),
    "coo": ("chief operating officer",),
    "cmo": ("chief marketing officer",),
    "cio": ("chief information officer",),
    "cpo": ("chief product officer",),
    "vp": ("vice president",),
    "hr": ("human resources",),
    "pr": ("public relations",),
    "it": ("information technology",),
    "founder": ("co-founder", "cofounder", "founding"),
    "owner": ("proprietor",),
    "director": ("dir",),
    "manager": ("mgr", "management"),
}

# Tokens that mark a junior/entry-level role; the cascade exclude list usually
# contains a subset of these (assistant/intern/junior/associate).
_JUNIOR_EXCLUDE_TOKENS = {
    "assistant", "intern", "junior", "associate", "entry", "trainee", "graduate",
}

# Senior / decision-maker signals. If any is present in the title, the junior
# excludes above are NOT applied — so "Associate General Counsel",
# "Assistant Director", "Senior Associate", "Associate Partner" are KEPT, while
# standalone "Sales Associate" / "Intern" / "Junior Developer" are still dropped.
# Phrases where "president" appears but is NOT the seniority the user means
# when they type "President": "Vice President ..." ≠ President. Masked before
# the bare-"president" substring check in ``_hay_has_word``.
_PRESIDENT_NEGATIONS = (
    "vice president", "vice-president", "vicePresident".lower(),
    "deputy president", "associate president", "assistant president",
    "past president", "former president", "president emeritus",
)

# Connector words inside multi-word titles ("VP of growth", "Director of
# Sales"). They carry no matching semantics and are skipped during the
# ALL-words include check so structured signals can satisfy the real words.
_CONNECTOR_WORDS = {"of", "the", "for", "in", "and", "&"}

_SENIOR_INDICATORS = (
    "chief", "ceo", "cto", "cfo", "coo", "cmo", "cio", "cpo", "president",
    "vp", "director", "partner", "principal", "professor", "dean", "counsel",
    "general", "head", "founder", "owner", "manager", "lead", "senior",
    "chairman", "chair",
)


def _title_word_variants(word: str) -> list[str]:
    """Lowercase word plus common synonym expansions for substring matching."""
    w = (word or "").lower().strip()
    if not w:
        return []
    return [w, *_TITLE_SYNONYMS.get(w, ())]


def _hay_has_word(hay: str, word: str) -> bool:
    # "president" is negated by a prefix — "Vice President of Product
    # Management" is not a President. Mask those phrases before the substring
    # check. (The VP side is unaffected: the "vp" token matches via its
    # "vice president" synonym against the unmasked hay.)
    if word == "president":
        masked = hay
        for neg in _PRESIDENT_NEGATIONS:
            masked = masked.replace(neg, " ")
        return "president" in masked
    return any(v and v in hay for v in _title_word_variants(word))


def person_matches_titles(
    title: str, headline: str, include_titles: list[str], exclude_titles: list[str],
    seniority: str = "", function: str = "",
) -> bool:
    """True if a contact matches the title filter. Matches against title +
    headline + seniority + function. ``seniority``/``function`` are structured
    signals from the Contacts DB (e.g. seniority 'vp', function 'sales'), so
    'VP' matches seniority 'vp' and 'Sales' matches function 'sales' even when
    the free-text headline omits the word.

    - exclude: if ANY exclude token has a matching word -> drop.
    - include: a token matches when ALL its words match (comma = OR between tokens).
    - No include list -> keep (subject to exclude).
    """
    hay = f"{title or ''} {headline or ''} {seniority or ''} {function or ''}".lower()
    if not hay.strip():
        return False
    # Junior-level excludes are overridden when the title carries a senior/
    # decision-maker signal (keep "Associate General Counsel", "Assistant
    # Director", "Senior Associate"; drop standalone "Sales Associate"/"Intern").
    has_senior = any(_hay_has_word(hay, s) for s in _SENIOR_INDICATORS)
    for ex in exclude_titles or []:
        ex_words = ex.lower().split()
        if ex_words and ex_words[0] in _JUNIOR_EXCLUDE_TOKENS and has_senior:
            continue
        if any(_hay_has_word(hay, w) for w in ex_words):
            return False
    if not include_titles:
        return True
    for inc in include_titles or []:
        # Connector words ("VP of growth") carry no matching semantics — skip
        # them so structured signals (seniority='vp' + function='growth')
        # satisfy a multi-word include without a literal "of" anywhere.
        words = [w for w in inc.lower().split() if w not in _CONNECTOR_WORDS]
        if words and all(_hay_has_word(hay, w) for w in words):
            return True
    return False


def _parse_cascade(cascade_config) -> list:
    """Parse cascade_config (JSON string or list) into a list; [] on junk."""
    if not cascade_config:
        return []
    try:
        cascade = json.loads(cascade_config) if isinstance(cascade_config, str) else cascade_config
    except Exception:
        return []
    return cascade if isinstance(cascade, list) else []


def parse_cascade_titles(cascade_config) -> tuple[list[str], list[str]]:
    """Extract (include_titles, exclude_titles) from a cascade_config JSON string
    (or pre-parsed list) produced by routes._titles_to_cascade. Returns ([], [])
    when absent -> no filtering."""
    cascade = _parse_cascade(cascade_config)
    if not cascade:
        return [], []
    include: list[str] = []
    exclude: list[str] = []
    for tier in cascade:
        if not isinstance(tier, dict):
            continue
        include += [str(t).strip() for t in (tier.get("include_title") or []) if str(t).strip()]
        exclude += [str(t).strip() for t in (tier.get("exclude_title") or []) if str(t).strip()]
    return include, list(dict.fromkeys(exclude))


def gate_title_filter(strict_titles: bool, cascade_config, default_cascade=None) -> tuple[list[str], list[str]]:
    """Resolve the effective (include, exclude) lists for the DISCOVERY gates
    (Blitz waterfall, generic fallbacks, pipeline persons).

    - ``strict_titles`` False (request escape hatch) → ([], []) → no gate.
    - cascade identical to ``default_cascade`` (the built-in Blitz tiers — the
      request carried NO user titles) → ([], []) → no gate. Filtering
      title-less traffic against the default tiers would silently drop
      "VP of Engineering"-style decision makers nobody asked to exclude.
    - user-provided cascade with titles → (include, exclude) → gate active.
    """
    if strict_titles is False:
        return [], []
    cascade = _parse_cascade(cascade_config)
    if not cascade:
        return [], []
    if default_cascade is not None and cascade == default_cascade:
        return [], []
    return parse_cascade_titles(cascade)


def blitz_person_passes_gate(
    person: dict[str, Any],
    include_titles: list[str],
    exclude_titles: list[str],
    _current_title_fn=None,
) -> bool:
    """Gate one Blitz-waterfall ``person`` dict (shape: ``{person: {...},
    icp: N}`` or the inner ``person`` dict itself) against the title filter.

    Uses the CURRENT title — resolved via ``experiences[0].job_title`` first,
    then the direct ``title`` field, mirroring the CSV's ``dm_title``
    derivation — plus the headline. Pass the caller's ``_current_title``
    as ``_current_title_fn`` when available (list_builder and pipeline each
    have their own copy); falls back to the same inline logic.
    """
    if not include_titles and not exclude_titles:
        return True
    p = person.get("person") if isinstance(person, dict) and isinstance(person.get("person"), dict) else person
    if not isinstance(p, dict):
        return True  # fail-open on unknown shape; providers already filtered
    if _current_title_fn is not None:
        title = _current_title_fn(p.get("experiences", []), p.get("title", ""))
    else:
        experiences = p.get("experiences") or []
        if experiences and isinstance(experiences[0], dict) and experiences[0].get("job_title"):
            title = experiences[0]["job_title"]
        else:
            title = p.get("title", "")
    return person_matches_titles(
        title, p.get("headline", ""), include_titles, exclude_titles,
    )


def cascade_config_allows_strict_off(cascade_config) -> bool:
    """True when the persisted cascade_config carries the strict_titles=false
    marker (set by routes when the request opted out). Used on resume/restart
    so the escape hatch survives across restarts."""
    if not cascade_config:
        return False
    try:
        cascade = json.loads(cascade_config) if isinstance(cascade_config, str) else cascade_config
    except Exception:
        return False
    if not isinstance(cascade, list):
        return False
    return any(isinstance(t, dict) and t.get("strict_titles") is False for t in cascade)


def mark_cascade_strict_off(cascade: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return a copy of ``cascade`` with ``strict_titles: False`` stamped on the
    first tier (immutable — new list, new first dict)."""
    if not cascade:
        return cascade
    return [{**cascade[0], "strict_titles": False}, *cascade[1:]]


def filter_blitz_persons(
    persons: list[dict[str, Any]],
    include_titles: list[str],
    exclude_titles: list[str],
    _current_title_fn=None,
) -> tuple[list[dict[str, Any]], int]:
    """Filter Blitz-waterfall ``results`` in place-safe (new list). Returns
    (kept_persons, dropped_count). No-op when no titles configured."""
    if not include_titles and not exclude_titles:
        return persons, 0
    kept: list[dict[str, Any]] = []
    dropped = 0
    for item in persons:
        if blitz_person_passes_gate(item, include_titles, exclude_titles, _current_title_fn):
            kept.append(item)
        else:
            dropped += 1
    return kept, dropped
