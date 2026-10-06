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
import re
from typing import Any, Optional, Sequence

# Candidate-pool size: how many Contacts-DB people to fetch per domain when a
# title filter is active, so matches survive the local gate (consumers widen
# the fetch via max(max_results, TITLE_SEARCH_POOL) in pipeline.py and
# list_builder.py). This is a FREE contacts_db knob — NOT a Blitz billing
# knob: the waterfall bills 1 record per result RETURNED and always receives
# the raw max_results, so this value never changes Blitz spend. 12 keeps gate
# precision with far less load on a free-but-slow 75 RPS API (50 over-fetched
# candidates with no quality gain). Env override still wins.
TITLE_SEARCH_POOL = int(os.getenv("TITLE_SEARCH_POOL", "12"))

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
# include check so structured signals can satisfy the real words.
_CONNECTOR_WORDS = {"of", "the", "for", "in", "and", "&"}

# Separator class allowed BETWEEN the words of a phrase / synonym variant
# ("professional  services", "Professional-Services", "Sales & Marketing").
_PHRASE_SEP = r"[\s\-/,&]+"

# Words of a user token that denote SENIORITY ("VP", "Director", "Head").
# They may match anywhere in the role segment (or the structured
# seniority/function signals). Everything else in the token is a FUNCTION
# word and must appear CONTIGUOUSLY as a phrase — this is what stops a
# company name from satisfying "VP Consulting" via "VP of Sales @Admiral
# Consulting Group".
_SENIORITY_WORDS = {
    "vp", "svp", "avp", "evp", "dvp", "ceo", "cto", "cfo", "coo", "cmo",
    "cio", "cpo", "cro", "chief", "officer", "president", "director",
    "head", "manager", "mgr", "lead", "leader", "senior", "sr", "snr",
    "founder", "cofounder", "co-founder", "owner", "partner", "principal",
    "chair", "chairman", "chairperson",
}

_SENIOR_INDICATORS = (
    "chief", "ceo", "cto", "cfo", "coo", "cmo", "cio", "cpo", "president",
    "vp", "director", "partner", "principal", "professor", "dean", "counsel",
    "general", "head", "founder", "owner", "manager", "lead", "senior",
    "chairman", "chair",
)


def _variants(word: str) -> list[str]:
    """Lowercase word plus common synonym expansions."""
    w = (word or "").lower().strip()
    if not w:
        return []
    return [w, *_TITLE_SYNONYMS.get(w, ())]


def _word_pattern(variant: str) -> str:
    """Word-boundary regex source for one word/phrase variant, plural
    tolerant ("director" also matches "Directors"; "services" also
    "service"). Multi-word variants join with the phrase separator."""
    words = variant.split()
    if not words:
        return r"(?!x)x"  # never matches
    escaped = [re.escape(w) for w in words]
    last = words[-1]
    if len(last) > 3 and last.endswith("s"):
        escaped[-1] = re.escape(last[:-1]) + "s?"
    else:
        escaped[-1] = re.escape(last) + "s?"
    return r"\b" + _PHRASE_SEP.join(escaped) + r"\b"


def _word_in_hay(hay: str, word: str) -> bool:
    """True if ``word`` (or a synonym variant) occurs in ``hay`` as a whole
    word. Word boundaries kill substring leaks ("head" ⊄ "Headquarters")."""
    return any(
        v and re.search(_word_pattern(v), hay) for v in _variants(word)
    )


def _phrase_in_hay(hay: str, phrase_words: Sequence[str]) -> bool:
    """True if the words occur CONTIGUOUSLY, in order. An empty phrase
    trivially matches (token had only seniority words)."""
    if not phrase_words:
        return True
    return re.search(_word_pattern(" ".join(phrase_words)), hay) is not None


def _mask_president_negations(hay: str) -> str:
    masked = hay
    for neg in _PRESIDENT_NEGATIONS:
        masked = masked.replace(neg, " ")
    return masked


def _collapse_vice_president(words: list[str]) -> list[str]:
    """['vice', 'president'] -> ['vp'] inside a token, so an explicit
    "Vice President" include matches VPs (the bare-"President" negation
    masking would otherwise hide them)."""
    out = list(words)
    i = 0
    while i < len(out) - 1:
        if out[i] == "vice" and out[i + 1] == "president":
            out[i:i + 2] = ["vp"]
        i += 1
    return out


def _role_segment(title: str, headline: str) -> str:
    """Title + the ROLE part of the headline only. LinkedIn headlines are
    conventionally "Role | @Company" or "Role at Company" — everything from
    the first "|" / "@" (else the last " at ") is the company, and company
    words must not satisfy title tokens ("VP Consulting" must not match
    "VP of Sales | @Admiral Consulting Group")."""
    h = (headline or "").strip()
    cuts = [i for i in (h.find("|"), h.find("@")) if i > 0]
    if cuts:
        h = h[: min(cuts)]
    else:
        i = h.lower().rfind(" at ")
        if i > 0:
            h = h[:i]
    return f"{title or ''} {h}".strip()


def person_matches_titles(
    title: str, headline: str, include_titles: list[str], exclude_titles: list[str],
    seniority: str = "", function: str = "",
) -> bool:
    """True if a contact matches the title filter. Matches against the ROLE
    segment of title + headline (the company tail of a headline is ignored)
    plus the structured ``seniority``/``function`` signals from the Contacts
    DB (e.g. seniority 'vp', function 'sales').

    Precision rules (2026-10-06, RCA jobs 144ee780/939136b6):
    - exclude: if ANY exclude word matches -> drop (junior excludes are
      overridden by a senior/decision-maker signal).
    - include: a token matches when every SENIORITY word of the token
      matches somewhere in the role segment AND the FUNCTION words occur
      contiguously as a phrase. Words match on word boundaries, so
      company-name words in a headline ("@Admiral Consulting Group") can
      no longer satisfy an include token.
    - comma-separated tokens are OR'ed; no include list -> keep.
    """
    role = _role_segment(title, headline).lower()
    hay = " ".join(
        part for part in (role, (seniority or "").lower(), (function or "").lower())
        if part
    )
    if not hay.strip():
        return False
    # Junior-level excludes are overridden when the title carries a senior/
    # decision-maker signal (keep "Associate General Counsel", "Assistant
    # Director", "Senior Associate"; drop standalone "Sales Associate"/"Intern").
    has_senior = any(_word_in_hay(hay, s) for s in _SENIOR_INDICATORS)
    for ex in exclude_titles or []:
        ex_words = ex.lower().split()
        if ex_words and ex_words[0] in _JUNIOR_EXCLUDE_TOKENS and has_senior:
            continue
        if any(_word_in_hay(hay, w) for w in ex_words):
            return False
    if not include_titles:
        return True
    masked_hay: Optional[str] = None  # computed lazily on bare-'president'
    for inc in include_titles or []:
        # Connector words ("VP of growth") carry no matching semantics — skip
        # them so structured signals (seniority='vp' + function='growth')
        # satisfy a multi-word include without a literal "of" anywhere.
        words = _collapse_vice_president(
            [w for w in inc.lower().split() if w not in _CONNECTOR_WORDS]
        )
        if not words:
            continue
        seniority_words = [w for w in words if w in _SENIORITY_WORDS]
        function_words = [w for w in words if w not in _SENIORITY_WORDS]
        matched = True
        for w in seniority_words:
            if w == "president":
                # Bare "President" must NOT match "Vice President ..." —
                # mask the negation phrases before the boundary check.
                if masked_hay is None:
                    masked_hay = _mask_president_negations(hay)
                if not re.search(_word_pattern("president"), masked_hay):
                    matched = False
                    break
            elif not _word_in_hay(hay, w):
                matched = False
                break
        if matched and _phrase_in_hay(hay, function_words):
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
