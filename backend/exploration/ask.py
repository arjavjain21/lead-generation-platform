"""Natural-language → validated Explorer filters (Gate 2B, Arjav-approved).

Safety model (the load-bearing part):
  * The LLM NEVER generates SQL and never touches the database. It only
    fills a strict, allowlisted filter schema — the same one the UI sends.
  * Output is validated against the live facet vocabulary (from
    aud.facet_summary via the CA facets endpoint) with fuzzy matching;
    anything unresolvable comes back in ``unknown_terms`` — never a guess.
  * Downstream, the filters go through the exact same parameterized
    ``build_where`` path as the UI. Zero provider calls, zero canonical
    writes — invariants unchanged.

LLM: Z.ai GLM chat-completions (OpenAI-compatible shape), configured via
backend/.env: ZAI_API_KEY / ZAI_BASE_URL / EXPLORER_ASK_MODEL.
The API key is never logged and never returned.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Optional

import httpx
from fastapi import HTTPException

logger = logging.getLogger(__name__)

_ASK_TIMEOUT = httpx.Timeout(25.0, connect=8.0)

# The ONLY fields the model may emit. Everything else is dropped server-side.
_ALLOWED_KEYS = {
    "q": str,
    "title_keywords": str,
    "company_domain": str,
    "company_name_contains": str,
    "seniority": list,
    "country": list,
    "industry": list,
    "employee_band": list,
    "lead_universe": list,
    "seg_classification": list,
    "email_verification_result": list,
    "has_email": bool,
    "is_verified": bool,
    "exclude_generic": bool,
    "has_linkedin": bool,
}

# Vocabulary-backed dims: values fuzzy-matched against live facet summaries.
_VOCAB_DIMS = ("country", "seniority", "industry", "employee_band",
               "lead_universe", "seg_classification", "email_verification_result")

_MAX_VALUES_PER_DIM = 20

_SYSTEM_PROMPT = """You translate a recruiter's natural-language request into filters for a contacts database.

Return ONLY a JSON object with these optional keys (omit any you cannot determine):
- "q": string — person name substring
- "title_keywords": string — comma-separated role keywords (e.g. "CMO, Head of Marketing")
- "company_domain": string — bare domain like "stripe.com"
- "company_name_contains": string — company name substring
- "seniority": array of strings — values like c_suite, vp, director, head, manager, founder, owner
- "country": array of strings — full country names from the vocabulary
- "industry": array of strings — normalized industries from the vocabulary
- "employee_band": array of strings — one or more of: 1-10, 11-50, 51-200, 201-500, 501-1000, 1001-5000, 5001-10000, 10001+
- "lead_universe": array of strings — e.g. local_business, b2b_agency, saas, ecom, data, quick_enrich
- "seg_classification": array of strings — one or more of: direct_google, direct_microsoft, external_seg, other_or_unknown, no_email
- "email_verification_result": array of strings — e.g. valid, invalid, catch_all
- "has_email": boolean, "is_verified": boolean, "exclude_generic": boolean, "has_linkedin": boolean
- "unknown_terms": array of strings — words from the request you could NOT map to any field above

Rules:
- Use ONLY vocabulary values provided in the user message for the vocabulary-backed lists.
- "verified email" → is_verified=true AND has_email=true. "Google/Microsoft email" → seg_classification.
- Employee ranges like "200 to 1000" → include EVERY overlapping band.
- Never invent values. Ambiguous → put in unknown_terms.
- Also return "interpretation": one short sentence describing the targeting you built."""


def _zai_config() -> tuple[str, str, str]:
    key = os.getenv("ZAI_API_KEY", "").strip()
    base = os.getenv("ZAI_BASE_URL", "https://api.z.ai/api").rstrip("/")
    model = os.getenv("EXPLORER_ASK_MODEL", "glm-4.7-flash").strip()
    if not key:
        raise HTTPException(status_code=503, detail="Explorer ask is not configured")
    return key, base, model


def _fuzzy_match(value: str, vocab: list[str]) -> Optional[str]:
    """Resolve a model-provided value to a vocabulary entry; None if no match."""
    v = value.strip().lower()
    if not v:
        return None
    for entry in vocab:
        if v == entry.lower():
            return entry
    for entry in vocab:  # substring both ways as fallback ("software" → "Software Development")
        if v in entry.lower() or entry.lower() in v:
            return entry
    return None


def parse_and_validate(raw: str, vocab: dict[str, list[str]]) -> dict[str, Any]:
    """Parse the LLM JSON and reduce it to a safe filter dict."""
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        # tolerate code-fenced JSON
        s, e = raw.find("{"), raw.rfind("}")
        if s < 0 or e <= s:
            raise HTTPException(status_code=502, detail="ask model returned unparseable output")
        data = json.loads(raw[s:e + 1])

    unknown_terms = [str(t) for t in data.get("unknown_terms", [])][:20]
    filters: dict[str, Any] = {}

    for key, kind in _ALLOWED_KEYS.items():
        if key not in data or data[key] in (None, "", []):
            continue
        val = data[key]
        if kind is bool:
            filters[key] = bool(val)
        elif kind is str:
            if isinstance(val, str) and val.strip():
                filters[key] = val.strip()[:200]
        elif kind is list:
            vals = val if isinstance(val, list) else [val]
            out: list[str] = []
            if key in _VOCAB_DIMS:
                for item in vals:
                    m = _fuzzy_match(str(item), vocab.get(key, []))
                    if m:
                        if m not in out:
                            out.append(m)
                    elif str(item).strip():
                        unknown_terms.append(f"{key}: {item}")
            else:
                for item in vals:
                    s = str(item).strip()
                    if s and s not in out:
                        out.append(s)
            if out:
                filters[key] = out[:_MAX_VALUES_PER_DIM]

    return {"filters": filters, "unknown_terms": unknown_terms,
            "interpretation": str(data.get("interpretation", ""))[:400]}


async def ask(question: str, fetch_facets) -> dict[str, Any]:
    """One Z.ai call → validated filters. ``fetch_facets`` is the BFF's own
    (canary-gated) unfiltered-facets fetch, giving the live vocabulary."""
    if not question or not question.strip() or len(question) > 500:
        raise HTTPException(status_code=422, detail="provide a question (≤500 chars)")

    vocab: dict[str, list[str]] = {}
    try:
        facets_resp = await fetch_facets()
        facets = json.loads(getattr(facets_resp, "body", b"{}") or b"{}")
        dim_map = {"country": "country", "seniority": "seniority", "industry": "industry",
                   "employee_band": "employee_band", "lead_universe": "lead_universe",
                   "seg_classification": "seg_classification",
                   "email_verification_result": "email_verification_result"}
        for api_dim, our_dim in dim_map.items():
            vals = facets.get("facets", {}).get(api_dim)
            if isinstance(vals, list):
                vocab[our_dim] = [str(v.get("value")) for v in vals if v.get("value")]
    except Exception:
        logger.warning("ask: vocabulary fetch failed; proceeding with schema-only validation")

    vocab_text = "\n".join(
        f"{dim} (use exactly these values): " + ", ".join(vals[:60])
        for dim, vals in sorted(vocab.items()) if vals)

    key, base, model = _zai_config()
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": f"VOCABULARY (live from the database):\n{vocab_text}\n\nREQUEST: {question.strip()}"},
        ],
        "temperature": 0.1,
        "max_tokens": 900,
        "response_format": {"type": "json_object"},
        "stream": False,
    }
    try:
        # test seam: exploration.explorer_routes._transport (MockTransport) if set
        import exploration.explorer_routes as _er
        _transport = getattr(_er, "_transport", None)
        async with httpx.AsyncClient(timeout=_ASK_TIMEOUT, transport=_transport) as client:
            resp = await client.post(
                f"{base}/paas/v4/chat/completions",
                json=payload,
                headers={"Authorization": f"Bearer {key}", "Accept-Language": "en-US,en"})
    except httpx.HTTPError as exc:
        logger.warning("ask: Z.ai transport error: %s", exc)
        raise HTTPException(status_code=502, detail="ask model unreachable")
    if resp.status_code != 200:
        # never include the key or the raw body in logs/responses
        logger.warning("ask: Z.ai HTTP %s", resp.status_code)
        raise HTTPException(status_code=502, detail="ask model error")
    try:
        content = resp.json()["choices"][0]["message"]["content"]
        usage = resp.json().get("usage", {})
    except Exception:
        raise HTTPException(status_code=502, detail="ask model returned unexpected shape")
    logger.info("ask: model=%s prompt_tokens=%s completion_tokens=%s request_id=%s",
                model, usage.get("prompt_tokens"), usage.get("completion_tokens"),
                resp.json().get("request_id", "-"))

    result = parse_and_validate(content, vocab)
    result["question"] = question.strip()
    return result
