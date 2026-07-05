"""
Helpers for row-level identifier propagation.

Normalization rules:
  * Treat "", " ", "nan", "none", "null", "n/a", "-" as missing (returns "").
  * Strip whitespace.
  * Lowercase the LinkedIn URL host so trailing slashes/path noise doesn't break matching.

The functions are pure (no I/O) so they can be unit tested cheaply.
"""

from __future__ import annotations

import re
from typing import Any, Optional
from urllib.parse import urlparse


_MISSING_TOKENS = {"", "nan", "none", "null", "n/a", "na", "-", "—", "n/a"}


def normalize_value(value) -> str:
    """Return a stripped string, or '' if value is missing/empty/noise.

    Treats None, non-strings, whitespace, and common CSV noise as missing.
    """
    if value is None:
        return ""
    if not isinstance(value, str):
        try:
            value = str(value)
        except Exception:
            return ""
    stripped = value.strip()
    if not stripped:
        return ""
    if stripped.lower() in _MISSING_TOKENS:
        return ""
    return stripped


def normalize_domain(value) -> str:
    """Return a bare, lowercased domain, or '' if value is missing/empty/noise.

    Strips:
      * protocol prefix (http://, https://)
      * leading "www." (and "ww1.", "ww2." etc. — any leading "w" repeated)
      * path, query string, and fragment
      * trailing slashes / whitespace
      * surrounding whitespace

    Returns a bare host form like 'mesterh-service.de'. Email-style
    addresses (containing '@') and non-domain tokens return '' so we
    don't accidentally pass user data to provider APIs.

    Examples:
      'https://Mesterh-Service.de/?utm_source=x'  -> 'mesterh-service.de'
      'http://www.acme.com/path?q=1'              -> 'acme.com'
      'acme.com/'                                 -> 'acme.com'
      'Acme.com'                                  -> 'acme.com'
      'mesterh-service.de'                        -> 'mesterh-service.de'
      ''                                         -> ''
      'not a domain'                              -> ''
      'user@example.com'                          -> ''  (an email, not a domain)
    """
    raw = normalize_value(value)
    if not raw:
        return ""
    # If the value is an email (contains '@'), reject.
    if "@" in raw:
        return ""
    # Strip protocol.
    raw = re.sub(r"^https?://", "", raw, flags=re.IGNORECASE)
    # Now `raw` is the part after the protocol (or the original if none).
    # Use urlparse to handle the rest robustly.
    if "://" not in raw and "/" not in raw and "?" not in raw and "#" not in raw:
        # Fast path: already a bare token, just lowercase and trim.
        candidate = raw.strip().rstrip(".").lower()
    else:
        # Re-prepend https so urlparse behaves consistently.
        parsed = urlparse("https://" + raw)
        host = (parsed.netloc or "").strip().lower()
        # Strip credentials if present (user:pass@host).
        if "@" in host:
            host = host.rsplit("@", 1)[-1]
        candidate = host
    if not candidate:
        return ""
    # Strip leading "www." and any "ww<n>." prefixes from the host.
    # We loop to handle "ww1.", "ww2.", etc. — the first "w" group.
    while True:
        if candidate.startswith("www."):
            candidate = candidate[4:]
        else:
            break
    # Drop a trailing dot (FQDN notation).
    candidate = candidate.rstrip(".")
    # Must contain a dot — a single token like "localhost" is not a domain
    # we want to send to providers, and bare words like "acme" are noise.
    if "." not in candidate:
        return ""
    # Disallow obvious non-domain patterns.
    if " " in candidate or "/" in candidate:
        return ""
    return candidate


def dedupe_rows_by_domain(
    rows: list[dict[str, Any]],
    domain_col: str,
    normalize: bool,
) -> tuple[list[dict[str, Any]], int, list[str]]:
    """Dedupe input rows by the domain column. First occurrence wins.

    Args:
        rows: list of input row dicts (one per CSV row).
        domain_col: name of the column containing the domain.
        normalize: if True, use ``normalize_domain()`` for the dedupe key
            (so ``acme.com`` and ``https://acme.com/?utm=x`` collapse
            together). If False, use the raw stripped-lowercased value
            (so franchise locations with different sub-paths are
            preserved).

    Returns:
        A 3-tuple ``(kept_rows, deduped_count, skipped_domains)``:
          * ``kept_rows`` — the input rows in their original order, with
            all but the first occurrence of each duplicate domain removed.
          * ``deduped_count`` — how many rows were dropped.
          * ``skipped_domains`` — the raw values of the dropped rows
            (for auditability and persistence).

    Notes:
        * Empty-domain rows are NOT considered duplicates of each other
          (they pass through). The downstream pipeline's existing
          empty-domain skip path handles them.
        * When ``normalize=True``, sub-paths are stripped before
          comparison — so ``mcdonalds.com/location/001`` and
          ``mcdonalds.com/location/002`` WILL collapse to
          ``mcdonalds.com``. For multi-location franchises, use
          ``normalize=False`` to keep each location distinct.
        * Subdomains are NEVER collapsed — ``blog.acme.com`` and
          ``shop.acme.com`` are distinct keys.
    """
    seen: set[str] = set()
    kept_rows: list[dict[str, Any]] = []
    skipped_domains: list[str] = []
    deduped_count = 0

    for row in rows:
        raw = str(row.get(domain_col, "") or "").strip()
        if not raw:
            # Empty domain rows pass through untouched.
            kept_rows.append(row)
            continue

        if normalize:
            key = normalize_domain(raw)
        else:
            key = raw.lower()

        if key in seen:
            deduped_count += 1
            skipped_domains.append(raw)
        else:
            seen.add(key)
            kept_rows.append(row)

    return kept_rows, deduped_count, skipped_domains


_LINKEDIN_HOST_RE = re.compile(r"^(?:https?://)?(?:www\.)?linkedin\.com/", re.IGNORECASE)

# Accept facebook.com, fb.com, m.facebook.com, web.facebook.com,
# www.facebook.com. The capture group is the page slug (everything
# after the host and any optional /pg/, /pages/, /people/ prefix).
_FACEBOOK_HOST_RE = re.compile(
    r"^(?:https?://)?(?:www\.|m\.|web\.)?(?:facebook\.com|fb\.com)/",
    re.IGNORECASE,
)
# Strip the leading /pg/, /pages/<slug>/, /people/<slug>/ prefix and
# keep just the page slug.
_FACEBOOK_SLUG_STRIP_RE = re.compile(
    r"^/(?:pg/|pages/[^/]+/|people/[^/]+/|profile\.php\?id=)?",
    re.IGNORECASE,
)


def normalize_linkedin_url(value) -> str:
    """Return a canonical LinkedIn URL ('' if value is not a LinkedIn URL).

    Output is lowercased host and path, no trailing slash, no query/fragment.
    Examples:
      'HTTP://Linkedin.com/in/JohnDoe/'  -> 'https://linkedin.com/in/johndoe'
      'linkedin.com/company/Acme'        -> 'https://linkedin.com/company/acme'
      'https://example.com/in/john'       -> '' (not linkedin)
    """
    raw = normalize_value(value)
    if not raw:
        return ""
    if not _LINKEDIN_HOST_RE.match(raw):
        return ""
    if "://" not in raw:
        raw = "https://" + raw
    parsed = urlparse(raw)
    host = (parsed.netloc or "").lower()
    path = (parsed.path or "").rstrip("/").lower()
    if not path:
        return ""
    return f"https://{host}{path}"


def linkedin_username_from_url(value) -> str:
    """Return the LinkedIn username/slug for /in/<slug> URLs; '' otherwise.

    Examples:
      'https://linkedin.com/in/johndoe'           -> 'johndoe'
      'https://linkedin.com/in/john-doe-123/'     -> 'john-doe-123'
      'https://linkedin.com/company/acme'         -> ''  (companies don't have usernames)
    """
    url = normalize_linkedin_url(value)
    if not url:
        return ""
    parsed = urlparse(url)
    parts = [p for p in parsed.path.split("/") if p]
    if len(parts) >= 2 and parts[0].lower() == "in":
        return parts[1].lower()
    return ""


def normalize_facebook_url(value) -> str:
    """Return a canonical Facebook page URL ('' if value is not a Facebook URL).

    Output uses https://facebook.com/<slug> with no trailing slash, no query,
    no fragment. m.facebook.com and web.facebook.com are normalized to
    facebook.com. Non-Facebook hosts return ''.

    Examples:
      'https://www.facebook.com/SomePage'   -> 'https://facebook.com/somepage'
      'm.facebook.com/SomePage/'            -> 'https://facebook.com/somepage'
      'fb.com/somepage'                     -> 'https://facebook.com/somepage'
      'https://facebook.com/pages/X/Y'      -> 'https://facebook.com/y'
      'https://example.com/SomePage'        -> ''
    """
    raw = normalize_value(value)
    if not raw:
        return ""
    if not _FACEBOOK_HOST_RE.match(raw):
        return ""
    if "://" not in raw:
        raw = "https://" + raw
    parsed = urlparse(raw)
    host = (parsed.netloc or "").lower()
    # Strip www./m./web. prefix and fb.com alias.
    for prefix in ("www.", "m.", "web."):
        if host.startswith(prefix):
            host = host[len(prefix):]
    if host == "fb.com":
        host = "facebook.com"
    if host not in ("facebook.com",):
        return ""
    path = (parsed.path or "").rstrip("/")
    # Strip the legacy /pg/, /pages/<slug>/, /people/<slug>/, /profile.php?id=
    # prefix to keep just the page slug.
    path = _FACEBOOK_SLUG_STRIP_RE.sub("/", path, count=1)
    path = path.rstrip("/").lower()
    if not path or path == "/":
        return ""
    return f"https://{host}{path}"


_LINKEDIN_KEYS = {
    "linkedin",
    "linkedin_url",
    "linkedin url",
    "linkedinprofile",
    "linkedin_profile",
    "linkedin profile",
    "profile_url",
    "profile url",
    "profile",
    "li_url",
    "li url",
}
_PHONE_KEYS = {
    "phone",
    "phone_number",
    "phone number",
    "mobile",
    "mobile phone",
    "mobile_number",
    "mobile number",
    "cell",
    "cell_phone",
    "cell phone",
    "direct_phone",
    "direct phone",
    "work_phone",
    "work phone",
    "telephone",
    "contact_phone",
    "contact phone",
}
_COMPANY_KEYS = {
    "company",
    "company_name",
    "company name",
    "business",
    "business_name",
    "business name",
    "organization",
    "org",
    "employer",
    "account",
    "account_name",
    "account name",
}
_EMAIL_KEYS = {
    "email",
    "work_email",
    "work email",
    "existing_email",
    "existing email",
    "email_address",
    "email address",
    "contact_email",
    "contact email",
    "person_email",
    "person email",
}
_FACEBOOK_KEYS = {
    "facebook",
    "facebook_url",
    "facebook url",
    "facebook_page",
    "facebook page",
    "facebook_page_url",
    "facebook page url",
    "page_url",
    "page url",
    "fb",
    "fb_url",
    "fb url",
    "fb_page",
    "fb page",
}


def _normalize_key(name: str) -> str:
    return re.sub(r"[\s_\-]+", " ", (name or "").strip().lower())


def suggest_linkedin_column(columns: list[str]) -> Optional[str]:
    """Pick the first column from `columns` that looks like a LinkedIn URL column."""
    norm_to_orig = {_normalize_key(c): c for c in columns}
    for key in _LINKEDIN_KEYS:
        if key in norm_to_orig:
            return norm_to_orig[key]
    for col in columns:
        if "linkedin" in _normalize_key(col):
            return col
    return None


def suggest_phone_column(columns: list[str]) -> Optional[str]:
    norm_to_orig = {_normalize_key(c): c for c in columns}
    for key in _PHONE_KEYS:
        if key in norm_to_orig:
            return norm_to_orig[key]
    for col in columns:
        norm = _normalize_key(col)
        if "phone" in norm or "mobile" in norm or "cell" in norm:
            return col
    return None


def suggest_company_column(columns: list[str]) -> Optional[str]:
    norm_to_orig = {_normalize_key(c): c for c in columns}
    for key in _COMPANY_KEYS:
        if key in norm_to_orig:
            return norm_to_orig[key]
    for col in columns:
        if _normalize_key(col) in {"company", "company name", "business", "business name"}:
            return col
    return None


def suggest_email_column(columns: list[str]) -> Optional[str]:
    norm_to_orig = {_normalize_key(c): c for c in columns}
    for key in _EMAIL_KEYS:
        if key in norm_to_orig:
            return norm_to_orig[key]
    for col in columns:
        if "email" in _normalize_key(col):
            return col
    return None


def suggest_facebook_column(columns: list[str]) -> Optional[str]:
    """Pick the first column from `columns` that looks like a Facebook URL column."""
    norm_to_orig = {_normalize_key(c): c for c in columns}
    for key in _FACEBOOK_KEYS:
        if key in norm_to_orig:
            return norm_to_orig[key]
    for col in columns:
        if "facebook" in _normalize_key(col) or " fb " in f" {_normalize_key(col)} ":
            return col
    return None


def build_row_identifier_payload(
    row: dict,
    *,
    domain_col: Optional[str] = None,
    name_col: Optional[str] = None,
    first_name_col: Optional[str] = None,
    last_name_col: Optional[str] = None,
    linkedin_url_col: Optional[str] = None,
    phone_col: Optional[str] = None,
    company_name_col: Optional[str] = None,
    existing_email_col: Optional[str] = None,
    facebook_url_col: Optional[str] = None,
) -> dict:
    """Extract and normalize every identifier column from a single CSV row.

    Returns a dict with keys: domain, full_name, first_name, last_name,
    linkedin_url, phone, company_name, existing_email, facebook_url,
    normalized_linkedin_url, normalized_facebook_url, linkedin_username,
    input_fields_used.
    """
    domain = normalize_domain(row.get(domain_col)) if domain_col else ""
    full_name = normalize_value(row.get(name_col)) if name_col else ""
    first_name = normalize_value(row.get(first_name_col)) if first_name_col else ""
    last_name = normalize_value(row.get(last_name_col)) if last_name_col else ""

    # Backward compat: if no full_name column but we have first/last, build it.
    if not full_name and (first_name or last_name):
        full_name = f"{first_name} {last_name}".strip()

    linkedin_raw = normalize_value(row.get(linkedin_url_col)) if linkedin_url_col else ""
    normalized_li = normalize_linkedin_url(linkedin_raw) if linkedin_raw else ""
    li_username = linkedin_username_from_url(normalized_li) if normalized_li else ""

    facebook_raw = normalize_value(row.get(facebook_url_col)) if facebook_url_col else ""
    normalized_fb = normalize_facebook_url(facebook_raw) if facebook_raw else ""

    phone = normalize_value(row.get(phone_col)) if phone_col else ""
    company_name = normalize_value(row.get(company_name_col)) if company_name_col else ""
    existing_email = normalize_value(row.get(existing_email_col)) if existing_email_col else ""
    existing_email = existing_email.lower() if existing_email else ""

    used: list[str] = []
    if domain:
        used.append("domain")
    if full_name:
        used.append("name")
    if linkedin_raw:
        used.append("linkedin_url")
    if phone:
        used.append("phone")
    if company_name:
        used.append("company_name")
    if existing_email:
        used.append("existing_email")
    if facebook_raw:
        used.append("facebook_url")

    return {
        "domain": domain,
        "full_name": full_name,
        "first_name": first_name,
        "last_name": last_name,
        "linkedin_url": linkedin_raw,
        "phone": phone,
        "company_name": company_name,
        "existing_email": existing_email,
        "facebook_url": facebook_raw,
        "normalized_linkedin_url": normalized_li,
        "normalized_facebook_url": normalized_fb,
        "linkedin_username": li_username,
        "input_fields_used": ",".join(used),
        # Output-CSV column names (so attach_input_columns can drop them in directly).
        "input_domain": domain,
        "input_full_name": full_name,
        "input_linkedin_url": linkedin_raw,
        "input_phone": phone,
        "input_company_name": company_name,
        "input_existing_email": existing_email,
        "input_facebook_url": facebook_raw,
    }


def input_payload_columns() -> list[str]:
    """Columns exposed in the enriched output CSV for visibility."""
    return [
        "input_domain",
        "input_full_name",
        "input_linkedin_url",
        "input_phone",
        "input_company_name",
        "input_existing_email",
        "input_facebook_url",
        "normalized_linkedin_url",
        "linkedin_username",
        "input_fields_used",
    ]


def attach_input_columns(output_row: dict, payload: dict) -> dict:
    """Copy input_* fields from `payload` onto an output row, in place.

    Returns the same row dict for convenience. Existing keys on the output row
    are not overwritten; input_* keys are explicitly added.
    """
    for col in (
        "input_domain",
        "input_full_name",
        "input_linkedin_url",
        "input_phone",
        "input_company_name",
        "input_existing_email",
        "input_facebook_url",
        "normalized_linkedin_url",
        "linkedin_username",
        "input_fields_used",
    ):
        if col in output_row and output_row[col]:
            # Preserve non-empty existing values.
            continue
        output_row[col] = payload.get(col, "")
    return output_row
