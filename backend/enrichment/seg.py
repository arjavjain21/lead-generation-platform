"""SEG (Secure Email Gateway) MX classification for enrichment domains.

Classifies a domain by its MX records into the same five-value scheme the
contacts DB stores on ``core.domain_seg_map`` (``external_seg``,
``direct_google``, ``direct_microsoft``, ``other_or_unknown``, ``no_email``)
and emits the exact provider label literals the DB uses ("SEG: Mimecast",
"Microsoft", "Other / Unknown", "No Email (no MX)", "Invalid Domain") so
platform-computed rows match DB rows byte-for-byte.

Signature table and free-webmail sets are ported 1:1 from
``/opt/contacts_api/scripts/seg_common.py`` (the DB's authority — where it
disagrees with the older macOS skill scanner, seg_common wins).

Domain normalization: two stages, deliberately.
  1. Platform input hygiene — ``identifier_utils.normalize_domain`` runs first
     on the raw caller value. It rejects emails, CSV noise tokens ("nan",
     "none", "-"), and bare words, so junk never reaches DNS.
  2. Map key — ``seg_common.norm_domain`` equivalents run second to produce
     the key used against the contacts DB and the local cache. The two differ
     for port-bearing inputs ("acme.com:8080" -> platform drops it as a
     non-domain via urlparse host parsing vs seg_common cutting at ':') and
     for "www.www." chains, so stage 2 re-applies its own rules on the
     already-clean stage-1 output to guarantee cache-key parity with the DB.

Negative caching: domains that could not be classified from ANY layer are
written with ``seg_classification=''`` (empty-string sentinel — the column is
NOT NULL but '' is a legal value) so a repeated miss in the next batch does
not re-hit the contacts API and DNS. '' rows are re-checked after
``_MISS_TTL_DAYS`` (7 days) — long enough to ride out a transient DoH/HTTP
outage, short enough to pick up newly-covered domains as the DB backfill
progresses. Real hits have no TTL: MX SEG posture changes on the scale of
years, not minutes.

All functionality gates on ``is_seg_enabled()``; when the flag is off every
public entry point returns ``{}`` / no-ops with zero HTTP and zero DNS calls.

Everything here is best-effort: classification NEVER raises into a caller —
provider/network failures degrade to "domain absent from the result map".
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any, Iterable, Optional

import httpx

from shared import db

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Feature flag
# ---------------------------------------------------------------------------

# Strong refs for fire-and-forget contribute tasks so the loop can't GC them
# mid-flight (and tests can await them deterministically).
_CONTRIBUTE_TASKS: set[asyncio.Task] = set()


def is_seg_enabled() -> bool:
    """True if SEG classification should run.

    Indirection (function vs constant) so tests can monkeypatch without
    mutating the import-time env value — mirrors
    ``contacts_writer.is_v2_enabled``. Read per-call so a flag flip lands on
    the next request without a worker restart.
    """
    return os.getenv("ENABLE_SEG_CLASSIFICATION", "false").lower() == "true"


def normalize_seg_key(domain: str) -> str:
    """Raw caller domain -> the normalized key ``classify_domains`` returns.

    Public one-liner so call sites (routes, list_builder, contacts_writer)
    translate their raw domain into the same key the classifier result dict
    is keyed by, without each importing a private helper.
    """
    return _normalize_for_seg(domain)


# ---------------------------------------------------------------------------
# Signature table — 1:1 port of seg_common.SEG_SIGNATURES.
# INSERTION ORDER IS PRECEDENCE (dict preserves it): all external_seg
# signatures match before Google, which matches before Microsoft.
# Segments are lowercase substrings matched against lowercased MX hostnames.
# ---------------------------------------------------------------------------

SEG_SIGNATURES: dict[str, tuple[str, str]] = {
    # signature substring          (provider label, classification)
    "pphosted.com": ("SEG: Proofpoint", "external_seg"),
    "proofpoint.com": ("SEG: Proofpoint", "external_seg"),
    "ppe-hosted.com": ("SEG: Proofpoint Essentials", "external_seg"),
    "mimecast.com": ("SEG: Mimecast", "external_seg"),
    "mimecast-mx.com": ("SEG: Mimecast", "external_seg"),
    "barracudanetworks.com": ("SEG: Barracuda", "external_seg"),
    "barracuda.com": ("SEG: Barracuda", "external_seg"),
    "ess.barracuda": ("SEG: Barracuda", "external_seg"),
    "iphmx.com": ("SEG: Cisco IronPort", "external_seg"),
    "cisco": ("SEG: Cisco IronPort", "external_seg"),
    "trendmicro.com": ("SEG: Trend Micro", "external_seg"),
    "sophos.com": ("SEG: Sophos", "external_seg"),
    "hydra.sophos": ("SEG: Sophos", "external_seg"),
    "messagelabs.com": ("SEG: Symantec BES (MessageLabs)", "external_seg"),
    "symantec": ("SEG: Symantec BES (MessageLabs)", "external_seg"),
    "fortinet.com": ("SEG: Fortinet FortiMail", "external_seg"),
    "fortimail.com": ("SEG: Fortinet FortiMail", "external_seg"),
    "zix.com": ("SEG: Zix", "external_seg"),
    "zixcorp.com": ("SEG: Zix", "external_seg"),
    "securemx": ("SEG: SecureMX", "external_seg"),
    "mxthunder": ("SEG: MXThunder", "external_seg"),
    "mtaroutes": ("SEG: MTAroutes", "external_seg"),
    "mailchannels.com": ("SEG: MailChannels", "external_seg"),
    "appriver.com": ("SEG: AppRiver", "external_seg"),
    "mailanywhere": ("SEG: MailAnywhere", "external_seg"),
    "mailspamprotection": ("SEG: SiteGround AntiSpam", "external_seg"),
    "spamfilter": ("SEG: SpamFilter", "external_seg"),
    # cloud direct (after all SEG signatures — a fronted domain is a SEG)
    "google.com": ("Google", "direct_google"),
    "googlemail.com": ("Google", "direct_google"),
    "outlook.com": ("Microsoft", "direct_microsoft"),
    "outlook.jp": ("Microsoft", "direct_microsoft"),
}

FREE_WEBMAIL_GOOGLE = frozenset({"gmail.com", "googlemail.com"})
FREE_WEBMAIL_MICROSOFT = frozenset(
    {"hotmail.com", "outlook.com", "live.com", "msn.com", "msn.co.uk"}
)
FREE_WEBMAIL_OTHER = frozenset(
    {
        "yahoo.com", "yahoo.co.uk", "yahoo.fr", "yahoo.de", "yahoo.es", "yahoo.it",
        "ymail.com", "rocketmail.com",
        "aol.com", "icloud.com", "me.com", "mac.com",
        "protonmail.com", "proton.me", "pm.me",
        "zoho.com", "zohomail.com",
        "yandex.com", "yandex.ru",
        "mail.com", "gmx.com", "gmx.de", "gmx.net",
        "inbox.com", "hushmail.com", "tutanota.com", "fastmail.com",
    }
)

# Canonical label literals (seg_common parity — pinned by tests).
PROVIDER_GOOGLE = "Google"
PROVIDER_MICROSOFT = "Microsoft"
PROVIDER_OTHER = "Other / Unknown"
PROVIDER_NO_MX = "No Email (no MX)"
PROVIDER_INVALID = "Invalid Domain"

CLASSIFICATION_NO_EMAIL = "no_email"

# Negative-cache TTL for '' rows (days).
_MISS_TTL_DAYS = 7

# Noise tokens identifier_utils.normalize_domain treats as missing. Kept in
# sync with identifier_utils._MISSING_TOKENS by importing it lazily; this
# literal set is only the fallback for the stage-1 rejection path above.
_STAGE1_NOISE = {"", "nan", "none", "null", "n/a", "na", "-", "—"}


# ---------------------------------------------------------------------------
# Pure helpers — no I/O, unit-testable without network
# ---------------------------------------------------------------------------

def _normalize_for_seg(domain: str) -> str:
    """Raw caller value -> bare lowercase registrable-domain cache key.

    Stage 1 (input hygiene): ``identifier_utils.normalize_domain`` rejects
    emails, CSV noise tokens, and bare words (no dot). Stage 2 (map key): the
    seg_common rules (strip scheme / path / query / fragment / port, leading
    www., trailing dot) so the key is byte-identical to the DB's
    ``norm_domain`` output. Returns '' for anything stage 1 rejects; a
    dot-less word that survives stage 1 is returned as-is so the caller can
    stamp it 'Invalid Domain' (the DB's own semantic) without network.
    """
    from .identifier_utils import normalize_domain as _platform_normalize

    cleaned = _platform_normalize(domain)
    if not cleaned:
        # Stage 1 rejects noise AND dot-less words. Distinguish the two:
        # noise is dropped; a dot-less word is an invalid DOMAIN, which the
        # caller records as no_email/'Invalid Domain' (seg_common semantics).
        fallback = str(domain or "").strip().lower()
        if not fallback or fallback in _STAGE1_NOISE:
            return ""
        fallback = fallback.split("/", 1)[0].split("?", 1)[0].split("#", 1)[0]
        fallback = fallback.split(":", 1)[0].rstrip(".")
        while fallback.startswith("www."):
            fallback = fallback[4:]
        if "@" in fallback or " " in fallback or "." in fallback:
            return ""  # email, spaced, or actually a domain — not this path
        return fallback
    d = cleaned.strip().lower()
    if "://" in d:  # defensive: platform stage already removed schemes
        d = d.split("://", 1)[1]
    d = d.split("/", 1)[0]
    d = d.split("?", 1)[0].split("#", 1)[0]
    if ":" in d:
        d = d.split(":", 1)[0]
    while d.startswith("www."):
        d = d[4:]
    d = d.rstrip(".")
    return d


def _is_malformed(domain: str) -> bool:
    """Post-normalization malformed check (seg_common.is_malformed parity).

    These are classified 'Invalid Domain' with NO network call.
    """
    return (
        not domain
        or domain.startswith("-")
        or domain.startswith(".")
        or ":" in domain
        or "|" in domain
        or "." not in domain
        or any(ch.isspace() or ch == "," for ch in domain)
    )


def _classify_mx_hosts(mx_hosts: list[str]) -> tuple[str, str]:
    """MX hostnames -> (classification, provider_label). Pure.

    Precedence = SEG_SIGNATURES insertion order, then generic cloud patterns
    (google before microsoft, mirroring the original scanner's ordering).
    """
    joined = " ".join(mx_hosts).lower()
    for host in mx_hosts:
        host_lower = host.lower()
        for signature, (provider, classification) in SEG_SIGNATURES.items():
            if signature in host_lower:
                return classification, provider
    if "google" in joined or "googlemail" in joined:
        return "direct_google", PROVIDER_GOOGLE
    if "outlook" in joined or "microsoft" in joined:
        return "direct_microsoft", PROVIDER_MICROSOFT
    return "other_or_unknown", PROVIDER_OTHER


def _free_webmail_lookup(domain: str) -> Optional[tuple[str, str, str]]:
    """Free-webmail fast path. Returns (classification, provider, mx_hosts)
    or None when the domain is not a known free-webmail host. No DNS."""
    if domain in FREE_WEBMAIL_GOOGLE:
        return "direct_google", PROVIDER_GOOGLE, "google.com (free webmail)"
    if domain in FREE_WEBMAIL_MICROSOFT:
        return "direct_microsoft", PROVIDER_MICROSOFT, "microsoft (free webmail)"
    if domain in FREE_WEBMAIL_OTHER:
        return "other_or_unknown", f"Free Webmail ({domain})", "other free webmail"
    return None


def _classify_offline(domain: str) -> Optional[tuple[str, str, str]]:
    """All no-DNS classifications (malformed + free webmail). None = needs MX."""
    if _is_malformed(domain):
        return CLASSIFICATION_NO_EMAIL, PROVIDER_INVALID, ""
    return _free_webmail_lookup(domain)


# ---------------------------------------------------------------------------
# Cache layer (SQLite via shared/db.py thread-local connections)
# ---------------------------------------------------------------------------

_CACHE_CHUNK = 500


def _read_cache(domains: list[str]) -> dict[str, dict]:
    """Read domain_seg_cache rows. '' rows older than the miss TTL are
    ignored (caller re-checks); fresh ones count as unclassifiable.
    Never raises."""
    out: dict[str, dict] = {}
    try:
        conn = db.get_db()
        for i in range(0, len(domains), _CACHE_CHUNK):
            chunk = domains[i : i + _CACHE_CHUNK]
            placeholders = ",".join("?" for _ in chunk)
            rows = conn.execute(
                "SELECT domain, seg_classification, seg_provider, source, mx_hosts, "
                "fetched_at, "
                f"(fetched_at < datetime('now', '-{_MISS_TTL_DAYS} days')) AS expired "
                f"FROM domain_seg_cache WHERE domain IN ({placeholders})",
                chunk,
            ).fetchall()
            for row in rows:
                classification = row["seg_classification"] or ""
                if classification == "" and not row["expired"]:
                    # Fresh negative cache — unclassifiable for now, but the
                    # caller must NOT re-hit layer 2/3. Represent it as a
                    # "known miss" so it can be excluded from downstream work
                    # while still being absent from the result map.
                    out[row["domain"]] = {"_negative": True}
                    continue
                if classification == "" and row["expired"]:
                    continue  # stale miss — let the layers re-run
                out[row["domain"]] = {
                    "seg_classification": classification,
                    "seg_provider": row["seg_provider"] or "",
                    "source": row["source"] or "",
                }
    except Exception:
        logger.warning("seg cache read failed", exc_info=True)
    return out


def _write_cache(rows: list[tuple[str, str, str, str, Optional[str]]]) -> None:
    """Best-effort batch upsert into domain_seg_cache. Never raises.

    Rows are (domain, seg_classification, seg_provider, source, mx_hosts_json).
    """
    if not rows:
        return
    try:
        conn = db.get_db()
        conn.executemany(
            "INSERT OR REPLACE INTO domain_seg_cache "
            "(domain, seg_classification, seg_provider, source, mx_hosts, fetched_at) "
            "VALUES (?, ?, ?, ?, ?, datetime('now'))",
            rows,
        )
        conn.commit()
    except Exception:
        logger.warning("seg cache write failed (%d rows)", len(rows), exc_info=True)


def _extract_mx_hosts(data: Any) -> list[str]:
    """Pull MX hostnames out of a DoH JSON body. Pure."""
    answers = (data or {}).get("Answer") or []
    hosts: list[str] = []
    for answer in answers:
        if not isinstance(answer, dict) or answer.get("type") != 15:
            continue  # DNS type 15 = MX
        raw = str(answer.get("data", ""))
        parts = raw.split()
        host = parts[-1] if len(parts) >= 2 else raw
        hosts.append(host.rstrip(".").lower())
    return hosts


def get_seg_for_domains_sync(domains: Iterable[str]) -> dict[str, dict]:
    """Cache-only synchronous lookup (no network). For future sync call sites.

    Returns the same shape as classify_domains. Misses are absent.
    """
    normalized = _dedupe_normalized(domains)
    if not normalized:
        return {}
    cached = _read_cache(normalized)
    return {
        d: v for d, v in cached.items() if not v.get("_negative")
    }


def _dedupe_normalized(domains: Iterable[str]) -> list[str]:
    """Normalize + dedupe, preserving first-seen order.

    Values stage 1 rejects as noise (empty / 'nan' / 'none' / '-' / emails)
    are dropped entirely — they are not domains, not even invalid ones. A
    token that survives stage 1 hygiene but is still not a well-formed
    domain (e.g. a dot-less word like 'no-dot-here') is KEPT so
    ``_classify_offline`` can stamp it no_email/'Invalid Domain' — that is
    the DB's own semantic for such inputs, and it happens with zero network.
    """
    seen: set[str] = set()
    ordered: list[str] = []
    for raw in domains:
        norm = _normalize_for_seg(raw)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        ordered.append(norm)
    return ordered


# ---------------------------------------------------------------------------
# HTTP helpers — conventions mirror enrichment/contacts_client.py
# ---------------------------------------------------------------------------

CONTACTS_SEG_CHUNK = 100          # endpoint contract: max 100 domains/batch
DOH_CONCURRENCY = 10             # bounded DNS fan-out
DOH_TIMEOUT = httpx.Timeout(5.0, connect=3.0, pool=10.0)  # (3, 5) skill parity
DOH_CLOUDFLARE_URL = "https://cloudflare-dns.com/dns-query"
DOH_HEADERS = {"accept": "application/dns-json"}

_shared_http: Optional[httpx.AsyncClient] = None


def _get_http() -> httpx.AsyncClient:
    """Lazy shared client (one per worker). Do NOT close per-call — mirrors
    routes._get_contacts_http. Sized for the DoH semaphore (10 in flight)."""
    global _shared_http
    if _shared_http is None:
        _shared_http = httpx.AsyncClient(
            limits=httpx.Limits(
                max_keepalive_connections=10,
                max_connections=20,
                keepalive_expiry=30.0,
            ),
        )
    return _shared_http


def _base_url() -> str:
    return os.getenv("CONTACTS_API_BASE_URL", "https://leadsdatabase.cc").rstrip("/")


def _headers() -> dict[str, str]:
    token = os.getenv("CONTACTS_API_TOKEN", "")
    if not token:
        raise RuntimeError("CONTACTS_API_TOKEN environment variable is not set")
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


async def _fetch_seg_from_contacts_db(
    domains: list[str],
) -> tuple[dict[str, dict], set[str]]:
    """GET /v1/domain/seg for the batch. Returns (found_map, missing_set).

    The endpoint may not exist yet on the deployed contacts API — a 404 or
    transport failure is treated as "all missing" (never an error state) so
    classification still flows through to the DoH layer.
    """
    found: dict[str, dict] = {}
    missing: set[str] = set()
    try:
        headers = _headers()
    except RuntimeError as exc:
        logger.warning("seg layer 2 skipped: %s", exc)
        return found, set(domains)

    client = _get_http()
    for i in range(0, len(domains), CONTACTS_SEG_CHUNK):
        chunk = domains[i : i + CONTACTS_SEG_CHUNK]
        try:
            resp = await client.get(
                f"{_base_url()}/v1/domain/seg",
                headers=headers,
                params={"domains": ",".join(chunk)},
                timeout=15.0,
            )
            if resp.status_code != 200:
                # 404 = endpoint not deployed yet; 4xx/5xx = treat as miss.
                logger.info(
                    "seg contacts-db lookup HTTP %s (endpoint may be absent) — treating as miss",
                    resp.status_code,
                )
                missing.update(chunk)
                continue
            payload = resp.json() or {}
            for row in payload.get("found") or []:
                domain = str(row.get("domain") or "")
                if not domain:
                    continue
                found[domain] = {
                    "seg_classification": str(row.get("seg_classification") or ""),
                    "seg_provider": str(row.get("seg_provider") or ""),
                    "source": "contacts_db",
                }
            missing.update(d for d in chunk if d not in found)
        except Exception as exc:
            logger.warning(
                "seg contacts-db lookup failed (%s) — treating chunk as miss",
                type(exc).__name__,
            )
            missing.update(chunk)
    return found, missing


async def _doh_mx_for_domain(domain: str, sem: asyncio.Semaphore) -> list[str]:
    """One DoH MX query. Returns [] on no-answer / any failure."""
    async with sem:
        try:
            resp = await _get_http().get(
                DOH_CLOUDFLARE_URL,
                headers=DOH_HEADERS,
                params={"name": domain, "type": "MX"},
                timeout=DOH_TIMEOUT,
            )
            if resp.status_code != 200:
                return []
            return _extract_mx_hosts(resp.json())
        except Exception as exc:
            logger.debug("seg DoH lookup failed for %s: %s", domain, exc)
            return []


async def _doh_scan(domains: list[str]) -> dict[str, dict]:
    """DoH MX classification for a batch of domains, bounded at
    DOH_CONCURRENCY. Malformed + free-webmail domains are answered offline
    (no DNS). DNS failures negative-cache '' (retryable next TTL window)."""
    results: dict[str, dict] = {}
    cache_rows: list[tuple[str, str, str, str, Optional[str]]] = []
    sem = asyncio.Semaphore(DOH_CONCURRENCY)

    needs_dns: list[str] = []
    for domain in domains:
        offline = _classify_offline(domain)
        if offline is not None:
            classification, provider, mx_hosts = offline
            results[domain] = {
                "seg_classification": classification,
                "seg_provider": provider,
                "source": "doh",
            }
            cache_rows.append((domain, classification, provider, "doh", mx_hosts))
        else:
            needs_dns.append(domain)

    if needs_dns:
        mx_lists = await asyncio.gather(
            *[_doh_mx_for_domain(d, sem) for d in needs_dns]
        )
        for domain, mx_hosts in zip(needs_dns, mx_lists):
            if not mx_hosts:
                # Genuine "no MX" vs transport failure are indistinguishable
                # here by design: both negative-cache as a miss (''), so a
                # transient DoH outage never poisons the cache with a fake
                # no_email verdict. Domains that truly have no MX get picked
                # up on the next TTL-window retry.
                cache_rows.append((domain, "", "", "doh", None))
                continue
            classification, provider = _classify_mx_hosts(mx_hosts)
            results[domain] = {
                "seg_classification": classification,
                "seg_provider": provider,
                "source": "doh",
            }
            cache_rows.append(
                (domain, classification, provider, "doh", json.dumps(mx_hosts[:10]))
            )

    try:
        _write_cache(cache_rows)
    except Exception:  # defensive: classification must never fail on cache
        logger.warning("seg DoH cache write failed", exc_info=True)
    return results


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------

async def classify_domains(domains: list[str]) -> dict[str, dict]:
    """Classify domains via cache -> contacts DB -> DoH MX scan.

    Returns ``{normalized_domain: {"seg_classification", "seg_provider",
    "source"}}`` for every domain it could classify; unclassifiable domains
    are simply absent (callers treat missing as blank). NEVER raises.
    Returns ``{}`` immediately when the feature flag is off.
    """
    if not is_seg_enabled():
        return {}

    normalized = _dedupe_normalized(domains)
    if not normalized:
        return {}

    results: dict[str, dict] = {}
    cache_rows: list[tuple[str, str, str, str, Optional[str]]] = []

    # Offline verdicts first — no cache, no network.
    remaining: list[str] = []
    for domain in normalized:
        offline = _classify_offline(domain)
        if offline is not None:
            classification, provider, mx_hosts = offline
            results[domain] = {
                "seg_classification": classification,
                "seg_provider": provider,
                "source": "doh",
            }
            cache_rows.append((domain, classification, provider, "doh", mx_hosts))
        else:
            remaining.append(domain)

    # Layer 1: local cache.
    if remaining:
        cached = _read_cache(remaining)
        still_missing: list[str] = []
        for domain in remaining:
            entry = cached.get(domain)
            if entry is None:
                still_missing.append(domain)  # not cached (or stale miss)
                continue
            if entry.get("_negative"):
                continue  # fresh negative cache — skip network, absent from map
            results[domain] = entry
        remaining = still_missing

    # Layer 2: contacts DB batch lookup.
    if remaining:
        found, missing = await _fetch_seg_from_contacts_db(remaining)
        answered: set[str] = set()
        for domain, entry in found.items():
            if not entry["seg_classification"]:
                # Found-but-blank row: not an answer, and the contract's
                # `missing` list won't include it — route it to DoH explicitly.
                continue
            results[domain] = entry
            answered.add(domain)
            cache_rows.append(
                (
                    domain,
                    entry["seg_classification"],
                    entry["seg_provider"],
                    "contacts_db",
                    None,
                )
            )
        remaining = [d for d in remaining if d not in answered]

    # Layer 3: DoH MX scan.
    if remaining:
        doh_results = await _doh_scan(remaining)
        results.update(doh_results)

    try:
        _write_cache(cache_rows)
    except Exception:  # defensive: classification must never fail on cache
        logger.warning("seg cache write failed", exc_info=True)

    # Push platform-computed verdicts back into the contacts-DB canonical map
    # (fill-gaps-only server-side). Only rows with real DNS evidence (a JSON
    # mx_hosts list) are contributed — free-webmail/invalid offline verdicts
    # are derivable DB-side and would be noise. Fire-and-forget: the POST
    # must never add latency to enrichment.
    contrib = [
        {
            "domain": domain,
            "classification": classification,
            "provider": provider,
            "mx_hosts": json.loads(mx_hosts),
        }
        for domain, classification, provider, source, mx_hosts in cache_rows
        if source == "doh" and mx_hosts and mx_hosts.startswith("[")
    ]
    if contrib:
        task = asyncio.create_task(contribute_to_map(contrib))
        _CONTRIBUTE_TASKS.add(task)
        task.add_done_callback(_CONTRIBUTE_TASKS.discard)
    return results


async def contribute_to_map(entries: list[dict]) -> None:
    """POST platform-computed DoH results to /v1/domain/seg (best-effort).

    ``entries`` are ``{"domain", "classification", "provider", "mx_hosts"}``
    dicts — only rows the platform computed itself (source='doh') should be
    contributed, never contacts-db echoes. Chunks of 100. Never raises.
    No-ops when the flag is off.
    """
    if not is_seg_enabled() or not entries:
        return
    try:
        headers = _headers()
    except RuntimeError as exc:
        logger.warning("seg contribute skipped: %s", exc)
        return

    client = _get_http()
    for i in range(0, len(entries), CONTACTS_SEG_CHUNK):
        chunk = entries[i : i + CONTACTS_SEG_CHUNK]
        try:
            resp = await client.post(
                f"{_base_url()}/v1/domain/seg",
                headers=headers,
                json={"entries": chunk},
                timeout=15.0,
            )
            if resp.status_code not in (200, 201, 204):
                logger.info(
                    "seg contribute HTTP %s (endpoint may be absent) — swallowed",
                    resp.status_code,
                )
        except Exception as exc:
            logger.warning(
                "seg contribute failed (%s) — swallowed", type(exc).__name__
            )
