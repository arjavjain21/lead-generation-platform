"""
Per-provider response normalization for the enrichment cascade.

Different providers (Contacts DB, Blitz, BetterEnrich, WizLeads,
SmartProspect) return contacts in different shapes with different
placeholder values for missing data. This module normalizes those
into a single canonical shape and filters out "junk" contacts that
have no meaningful identifier (email, name, or LinkedIn URL).

Design contract:
  * Pure functions only — no I/O, no logging, no globals.
  * Total functions — never raise for any input. Always return either
    a string or None.
  * All public functions are type-annotated.
  * Output dict shape is fixed (the canonical keys are email,
    first_name, last_name, full_name, title, headline, linkedin_url,
    domain, source) so downstream code can rely on every key being
    present.

The normalized dict is intended to be consumed by:
  * The upcoming ``RawContactCollector`` (per-row capture of every
    provider contact for audit/storage).
  * The existing ``contacts_writer`` paths (write-back to Contacts DB).

Junk filter rule (CRITICAL):
  A contact is "junk" (returns None) if ALL of the following are empty
  after normalization: email, full_name, linkedin_url. A contact with
  only an email OR only a name OR only a LinkedIn URL is kept.
"""

from __future__ import annotations

import re
from typing import Any, Callable, Optional

# ---------------------------------------------------------------------------
# Placeholder sets
# ---------------------------------------------------------------------------
#
# These are frozensets (immutable) so callers cannot accidentally mutate
# them. Each set covers the placeholders observed in real provider
# responses plus the common "missing data" sentinels found in CSV/JSON
# integrations ("n/a", "none", "null", "unknown", etc.).
#
# When a normalizer sees one of these (after strip+lowercase), it treats
# the value as missing and returns "".

EMAIL_PLACEHOLDERS: frozenset[str] = frozenset({
    "", "no_email", "no-email", "noemail",
    "n/a", "na", "n.a.",
    "none", "null", "nil", "unknown",
    "-", "--", "...",
    "not_found", "notfound",
    "false", "undefined",
    "no email", "no e-mail",
    "missing", "tbd", "n/k",
})

NAME_PLACEHOLDERS: frozenset[str] = frozenset({
    "", "unknown", "n/a", "na", "none", "null",
    "-", "--", "...",
    "test", "test user", "test test",
    "not found", "not_found",
    "tbd", "missing", "n/k", "nil",
    "first last",  # common template placeholder
    "john doe", "jane doe",  # common fake-example names
})

LINKEDIN_PLACEHOLDERS: frozenset[str] = frozenset({
    "", "n/a", "none", "null", "unknown",
    "-", "--", "...",
    "not found", "not_found", "no_linkedin", "no-linkedin",
    "no url", "no_profile", "false", "undefined", "missing",
})

DOMAIN_PLACEHOLDERS: frozenset[str] = frozenset({
    "", "n/a", "none", "null", "unknown",
    "-", "--", "...",
    "no_domain", "no-domain", "not found",
    "no website", "no_website", "false", "undefined", "missing",
})


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_WHITESPACE_RE = re.compile(r"\s+")
_DISPLAY_EMAIL_RE = re.compile(r"<?([^<>\s]+@[^<>\s]+)>?")


def _coerce_str(value: Any) -> str:
    """Coerce any value to a stripped string. Never raises.

    None, missing, or non-string values become "". Objects that can't be
    stringified also become "".
    """
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    try:
        s = str(value)
    except Exception:
        return ""
    # If coercion gives us non-finite floats or other oddities, strip
    # whitespace and let downstream checks handle it.
    return s.strip()


# ---------------------------------------------------------------------------
# Field normalizers
# ---------------------------------------------------------------------------

def normalize_email(value: Optional[str]) -> str:
    """Return a cleaned, lowercased email or "".

    Steps:
      1. None / empty / non-string -> "".
      2. Strip whitespace, lowercase.
      3. If the result is a known placeholder -> "".
      4. If the value contains a "Name <email@x>" display-name format,
         extract just the email@x portion.
      5. If the result has no "@" -> "".
      6. Otherwise return the cleaned email.
    """
    raw = _coerce_str(value)
    if not raw:
        return ""

    cleaned = raw.strip().lower()
    if cleaned in EMAIL_PLACEHOLDERS:
        return ""

    # Display name format: "John Doe <john@x.com>" or just "<john@x.com>"
    # The regex pulls the bare email out.
    if "<" in cleaned or ">" in cleaned:
        match = _DISPLAY_EMAIL_RE.search(cleaned)
        if match:
            cleaned = match.group(1).lower()

    if "@" not in cleaned:
        return ""

    # Disallow obviously broken shapes (no domain, spaces in local part
    # after strip, etc.). We keep this conservative — full RFC validation
    # is the responsibility of downstream mailtester / verifier.
    local, _, host = cleaned.partition("@")
    if not local or not host:
        return ""
    if "." not in host:
        # A naked TLD-less host (e.g. "localhost", bare hostname) is
        # almost certainly noise — a real email always has a dotted
        # domain like "acme.com".
        return ""
    if " " in cleaned:
        return ""

    return cleaned


def normalize_person_name(value: Optional[str]) -> str:
    """Return a cleaned person name or "".

    Steps:
      1. None / empty / non-string -> "".
      2. Strip whitespace.
      3. If lowercased value is a known placeholder -> "".
      4. Collapse internal whitespace (spaces, tabs, newlines) to single
         spaces.
      5. Reject values that have no alphabetic characters (e.g. "42",
         "(123) 456-7890") — a name must contain at least one letter.
      6. Return the cleaned name with original case preserved.
    """
    raw = _coerce_str(value)
    if not raw:
        return ""
    # Non-string inputs that coerce to digits-only (e.g. 42 -> "42") are
    # not meaningful names.
    if not isinstance(value, str):
        return ""
    stripped = raw.strip()
    if not stripped:
        return ""
    if stripped.lower() in NAME_PLACEHOLDERS:
        return ""
    # Collapse internal whitespace (handles "John   Doe", "John\nDoe", etc.)
    collapsed = _WHITESPACE_RE.sub(" ", stripped).strip()
    if not collapsed or collapsed.lower() in NAME_PLACEHOLDERS:
        return ""
    # A name must contain at least one alphabetic character (Unicode-aware).
    if not any(ch.isalpha() for ch in collapsed):
        return ""
    return collapsed


def normalize_linkedin_url(value: Optional[str]) -> str:
    """Return a canonical LinkedIn URL or "".

    Canonical form: ``linkedin.com/in/<slug>`` (no protocol, no www,
    no trailing slash, lowercased).

    Returns "" if:
      * Input is empty/placeholder.
      * "linkedin.com" does not appear in the value.
      * The path is empty after stripping the host.
    """
    raw = _coerce_str(value)
    if not raw:
        return ""
    cleaned = raw.strip().lower()
    if cleaned in LINKEDIN_PLACEHOLDERS:
        return ""
    if "linkedin.com" not in cleaned:
        return ""

    # Strip protocol.
    for prefix in ("https://", "http://"):
        if cleaned.startswith(prefix):
            cleaned = cleaned[len(prefix):]
            break

    # Strip leading "www." (and ww1., ww2., ...).
    while True:
        m = re.match(r"^w{2,3}\d?\.", cleaned)
        if m:
            cleaned = cleaned[m.end():]
        else:
            break

    # Strip query and fragment.
    cleaned = cleaned.split("?", 1)[0]
    cleaned = cleaned.split("#", 1)[0]

    # Strip trailing slash.
    cleaned = cleaned.rstrip("/")

    # Require a non-empty path after the host.
    if not cleaned.startswith("linkedin.com/"):
        return ""
    path = cleaned[len("linkedin.com/"):]
    if not path:
        return ""

    return cleaned


def normalize_domain(value: Optional[str]) -> str:
    """Return a canonical bare domain or "".

    Canonical form: ``acme.com`` (no protocol, no www, no path, no
    query, lowercased).

    Returns "" if:
      * Input is empty/placeholder.
      * The value contains "@" (an email, not a domain).
      * The value has no dot after stripping.
    """
    raw = _coerce_str(value)
    if not raw:
        return ""
    cleaned = raw.strip().lower()
    if cleaned in DOMAIN_PLACEHOLDERS:
        return ""

    # Strip protocol.
    for prefix in ("https://", "http://", "ftp://", "//"):
        if cleaned.startswith(prefix):
            cleaned = cleaned[len(prefix):]
            break

    # Strip credentials user:pass@host BEFORE the @-check so that
    # "user:pass@acme.com" is treated as a domain, not an email.
    # We require a ":" in the part before "@" — bare "user@host.com"
    # is an email, not credentials.
    head = cleaned.split("/", 1)[0]
    if "@" in head and ":" in head.split("@", 1)[0]:
        before, _, after = head.rpartition("@")
        if after and "." in after:
            tail = cleaned.split("/", 1)[1] if "/" in cleaned else ""
            cleaned = after + (("/" + tail) if tail else "")

    # An email-style value is NOT a domain.
    if "@" in cleaned:
        return ""

    # Strip path / query / fragment / port — keep just the host.
    for sep in ("/", "?", "#", ":"):
        if sep in cleaned:
            cleaned = cleaned.split(sep, 1)[0]

    # Strip leading "www." (and ww1., ww2., ...).
    while True:
        m = re.match(r"^w{2,3}\d?\.", cleaned)
        if m:
            cleaned = cleaned[m.end():]
        else:
            break

    cleaned = cleaned.strip(".").strip()
    if not cleaned:
        return ""
    # Require a dot — bare "localhost" etc. is not a domain.
    if "." not in cleaned:
        return ""
    if " " in cleaned:
        return ""
    return cleaned


# ---------------------------------------------------------------------------
# Predicates
# ---------------------------------------------------------------------------

def is_meaningful_email(value: Optional[str]) -> bool:
    """True if value normalizes to a non-empty email."""
    return bool(normalize_email(value))


def is_meaningful_person(value: Optional[str]) -> bool:
    """True if value normalizes to a non-empty person name."""
    return bool(normalize_person_name(value))


# ---------------------------------------------------------------------------
# Per-provider normalizers
# ---------------------------------------------------------------------------


def _build_record(
    *,
    source: str,
    email: Any = "",
    first_name: Any = "",
    last_name: Any = "",
    full_name: Any = "",
    title: Any = "",
    headline: Any = "",
    linkedin_url: Any = "",
    domain: Any = "",
) -> Optional[dict[str, str]]:
    """Build a normalized record dict with all keys populated.

    If email/full_name/linkedin_url are all empty after normalization,
    returns None (junk filter).
    """
    n_email = normalize_email(email)
    n_first = normalize_person_name(first_name)
    n_last = normalize_person_name(last_name)
    n_full = normalize_person_name(full_name)
    if not n_full and (n_first or n_last):
        n_full = " ".join(p for p in (n_first, n_last) if p).strip()
    n_title = normalize_person_name(title)
    n_headline = normalize_person_name(headline)
    n_linkedin = normalize_linkedin_url(linkedin_url)
    n_domain = normalize_domain(domain)

    # Junk filter: must have at least ONE of email, full_name, linkedin.
    if not (n_email or n_full or n_linkedin):
        # Per spec, return None for junk. Caller checks for None.
        return None

    return {
        "email": n_email,
        "first_name": n_first,
        "last_name": n_last,
        "full_name": n_full,
        "title": n_title,
        "headline": n_headline,
        "linkedin_url": n_linkedin,
        "domain": n_domain,
        "source": source,
    }


def _extract_blitz_title(person: dict) -> str:
    """Pull the current job title from a Blitz person dict.

    Blitz returns experiences as a list of dicts; the first
    ``job_is_current=True`` wins, otherwise the first entry.
    Falls back to a direct "title" field if present.
    """
    if not isinstance(person, dict):
        return ""
    direct = person.get("title") or ""
    if direct and isinstance(direct, str):
        return direct
    experiences = person.get("experiences")
    if isinstance(experiences, list):
        for exp in experiences:
            if isinstance(exp, dict) and exp.get("job_is_current"):
                jt = exp.get("job_title") or ""
                if isinstance(jt, str) and jt.strip():
                    return jt
        if experiences:
            first = experiences[0]
            if isinstance(first, dict):
                jt = first.get("job_title") or ""
                if isinstance(jt, str):
                    return jt
    return ""


def _extract_blitz_email(person: dict) -> str:
    """Pull the best email from a Blitz person dict.

    Priority: verified_email > emails[0] (each email is a dict with
    key "email" or a bare string) > direct "email" field.
    """
    if not isinstance(person, dict):
        return ""
    verified = person.get("verified_email")
    if isinstance(verified, str) and verified.strip():
        return verified
    emails = person.get("emails")
    if isinstance(emails, list) and emails:
        first = emails[0]
        if isinstance(first, dict):
            cand = first.get("email") or first.get("address") or ""
            if isinstance(cand, str):
                return cand
        elif isinstance(first, str):
            return first
    # Final fallback: a direct "email" key (used in bare person dicts
    # where Blitz returned a single email with no list wrapper).
    direct = person.get("email")
    if isinstance(direct, str):
        return direct
    return ""


def normalize_contacts_db_contact(raw: dict) -> Optional[dict[str, str]]:
    """Normalize a single Contacts DB contact dict.

    Contacts DB returns (per ``contacts_client.company_contacts_enriched``
    docstring at line 306) a list of dicts with shape::

        {
            "person_id": "uuid",
            "full_name": "John Doe",
            "email": "john@example.com",
            "title": "Software Engineer",
            "linkedin_url": "...",
            ...
        }

    Some responses also include ``first_name`` / ``last_name`` /
    ``headline`` / ``domain``.
    """
    if not isinstance(raw, dict):
        return None
    return _build_record(
        source="contacts_db",
        email=raw.get("email"),
        first_name=raw.get("first_name"),
        last_name=raw.get("last_name"),
        full_name=raw.get("full_name"),
        title=raw.get("title"),
        headline=raw.get("headline"),
        linkedin_url=raw.get("linkedin_url"),
        domain=raw.get("domain"),
    )


def normalize_blitz_contact(raw: dict) -> Optional[dict[str, str]]:
    """Normalize a single Blitz contact dict.

    Per ``blitz_client.waterfall_icp_search`` (line 175) and
    ``blitz_client.person_enrich`` (line 419), Blitz returns either:

    1. A waterfall result: ``{"icp": N, "ranking": N, "person": {...}}``
    2. A bare person dict: ``{"first_name", "last_name", ...,
       "verified_email", "emails", "experiences"}``

    This normalizer accepts BOTH shapes — if "person" key exists, we
    unwrap it. Otherwise we treat ``raw`` as the person dict directly.
    """
    if not isinstance(raw, dict):
        return None
    person = raw.get("person") if "person" in raw else raw
    if not isinstance(person, dict):
        return None

    title = _extract_blitz_title(person)
    email = _extract_blitz_email(person)

    return _build_record(
        source="blitz",
        email=email,
        first_name=person.get("first_name"),
        last_name=person.get("last_name"),
        full_name=person.get("full_name"),
        title=title,
        headline=person.get("headline"),
        linkedin_url=person.get("linkedin_url"),
        domain="",  # Blitz person responses don't carry a domain.
    )


def normalize_better_enrich_contact(raw: dict) -> Optional[dict[str, str]]:
    """Normalize a BetterEnrich contact dict.

    Per ``better_enrich_client.find_work_email_v3`` (line 392) and
    ``find_work_email`` (line 124), BetterEnrich's response shape is
    minimal — just an email plus verifier/esp metadata::

        {
            "email": "john@example.com",
            "email_status": "verified",
            "verifier": "...",
            "esp": "Google",
        }

    Some variants include ``full_name``, ``first_name``, ``last_name``,
    ``linkedin_url``, ``domain`` when the API surfaces them. We pull
    every key defensively.
    """
    if not isinstance(raw, dict):
        return None
    email = (
        raw.get("email")
        or raw.get("work_email")
        or raw.get("person_email")
    )
    # Some BetterEnrich responses nest the data one level deep.
    if not email and isinstance(raw.get("data"), dict):
        nested = raw["data"]
        email = nested.get("email") or nested.get("work_email")
        return _build_record(
            source="better_enrich",
            email=email,
            first_name=nested.get("first_name"),
            last_name=nested.get("last_name"),
            full_name=nested.get("full_name"),
            title=nested.get("title"),
            headline=nested.get("headline"),
            linkedin_url=nested.get("linkedin_url"),
            domain=nested.get("domain") or nested.get("company_domain"),
        )
    return _build_record(
        source="better_enrich",
        email=email,
        first_name=raw.get("first_name"),
        last_name=raw.get("last_name"),
        full_name=raw.get("full_name"),
        title=raw.get("title"),
        headline=raw.get("headline"),
        linkedin_url=raw.get("linkedin_url"),
        domain=raw.get("domain") or raw.get("company_domain"),
    )


def normalize_wizleads_contact(raw: dict) -> Optional[dict[str, str]]:
    """Normalize a WizLeads contact dict.

    Per ``wizleads_client.find_email`` (line 118), WizLeads returns::

        {
            "email": "john@example.com",
            "catchall": "YES",
            "provider": "Google",
            "normalized_fname": "John",  # sometimes present
            "normalized_lname": "Doe",
        }

    The caller often augments this dict with the input ``first_name`` /
    ``last_name`` / ``website`` for context; we read those defensively.
    """
    if not isinstance(raw, dict):
        return None
    first = raw.get("normalized_fname") or raw.get("first_name")
    last = raw.get("normalized_lname") or raw.get("last_name")
    return _build_record(
        source="wizleads",
        email=raw.get("email"),
        first_name=first,
        last_name=last,
        full_name=raw.get("full_name"),
        title=raw.get("title"),
        headline=raw.get("headline"),
        linkedin_url=raw.get("linkedin_url"),
        domain=raw.get("website") or raw.get("domain"),
    )


def normalize_smartprospect_contact(raw: dict) -> Optional[dict[str, str]]:
    """Normalize a SmartProspect contact dict.

    SmartProspect returns a flat shape::

        {
            "firstName": "John",
            "lastName": "Doe",
            "companyDomain": "example.com",
            "email_id": "john.doe@example.com",
            "status": "Found",
            "verification_status": "Valid"
        }

    Edge cases:
      * ``status: "Not Found"`` → ``email_id`` is empty,
        ``verification_status`` is null.
      * ``status: "Found"`` + ``verification_status: null`` → email
        found but unverified.

    The ``status`` / ``verification_status`` keys are NOT consumed by
    ``_build_record`` (which only carries canonical identity fields).
    They are preserved in the original ``raw`` dict that feeds the
    downstream ``RawContactCollector``; the collector's
    ``_extract_verified`` reads them directly from ``raw``.

    SmartProspect responses never carry ``title``, ``headline``, or
    ``linkedin_url`` — empty strings are passed for those fields unless
    the caller's synthetic dict supplies them (both shapes tolerated,
    see key aliasing below).

    Junk filter behavior: a "Not Found" contact has an empty
    ``email_id`` but still has ``firstName`` + ``lastName``. The
    ``_build_record`` helper will KEEP it (because ``full_name`` is
    non-empty), which is intentional — the collector records the name
    with ``dm_email=""``.

    **Key aliasing:** the raw SmartLead API returns camelCase keys
    (``firstName``, ``lastName``, ``companyDomain``, ``email_id``).
    Call sites in ``pipeline.py`` that construct synthetic dicts for
    capture use snake_case (``first_name``, ``last_name``, ``domain``,
    ``email``). We accept BOTH so captures normalize correctly.
    camelCase wins when both are present (matches the raw API contract).
    """
    if not isinstance(raw, dict):
        return None
    return _build_record(
        source="smartprospect",
        email=raw.get("email_id") or raw.get("email"),
        first_name=raw.get("firstName") or raw.get("first_name"),
        last_name=raw.get("lastName") or raw.get("last_name"),
        full_name=raw.get("full_name", ""),
        title=raw.get("title", ""),
        headline=raw.get("headline", ""),
        linkedin_url=raw.get("linkedin_url", ""),
        domain=raw.get("companyDomain") or raw.get("domain"),
    )


# ---------------------------------------------------------------------------
# Generic dispatcher
# ---------------------------------------------------------------------------

# Registry of known providers. Lookups are case-insensitive.
_PROVIDER_DISPATCH: dict[str, Callable[[dict], Optional[dict[str, str]]]] = {
    "contacts_db": normalize_contacts_db_contact,
    "contactsdb": normalize_contacts_db_contact,
    "contacts-db": normalize_contacts_db_contact,
    "blitz": normalize_blitz_contact,
    "better_enrich": normalize_better_enrich_contact,
    "betterenrich": normalize_better_enrich_contact,
    "better-enrich": normalize_better_enrich_contact,
    "wizleads": normalize_wizleads_contact,
    "wiz_leads": normalize_wizleads_contact,
    "wiz-leads": normalize_wizleads_contact,
    "smartprospect": normalize_smartprospect_contact,
    "smart_prospect": normalize_smartprospect_contact,
    "smart-prospect": normalize_smartprospect_contact,
}


def normalize_provider_contact(
    source: str, raw: dict
) -> Optional[dict[str, str]]:
    """Dispatch to the per-provider normalizer by source name.

    Args:
        source: Provider name (case-insensitive). Unknown providers
            return None.
        raw: The raw provider response dict.

    Returns:
        Normalized dict, or None for junk contacts / unknown providers.
    """
    if not isinstance(source, str) or not isinstance(raw, dict):
        return None
    key = source.strip().lower()
    fn = _PROVIDER_DISPATCH.get(key)
    if fn is None:
        return None
    return fn(raw)


__all__ = [
    "EMAIL_PLACEHOLDERS",
    "NAME_PLACEHOLDERS",
    "LINKEDIN_PLACEHOLDERS",
    "DOMAIN_PLACEHOLDERS",
    "normalize_email",
    "normalize_person_name",
    "normalize_linkedin_url",
    "normalize_domain",
    "is_meaningful_email",
    "is_meaningful_person",
    "normalize_contacts_db_contact",
    "normalize_smartprospect_contact",
    "normalize_blitz_contact",
    "normalize_better_enrich_contact",
    "normalize_wizleads_contact",
    "normalize_provider_contact",
]
