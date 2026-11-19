---
name: listbuilding
description: "MOVED — do not use. This consolidated copy was retired 2026-11-19 to stop duplicate-copy drift (it had sat 5 weeks stale). The canonical, kept-fresh skills live in arjavjain21/hyperke-ai-toolkit → skills/listbuilding-enrichment and skills/listbuilding-scraper (team distribution: atishayjain708/cold-email-skills-hyperke → lead-list-enricher + listbuilding-scraper). Copy-paste request scripts remain in requests/ here."
---

# ListBuilding (consolidated platform copy) — RETIRED 2026-11-19

This was a consolidated enrichment+scraper doc inside the platform repo. It duplicated
two skills that are maintained and synced elsewhere, so it kept going stale silently
(found 5 weeks stale in the 2026-11-19 audit). **There is no skill content here anymore.**

| You need | Use |
|---|---|
| Canonical, always-fresh skills | `arjavjain21/hyperke-ai-toolkit` → `skills/listbuilding-enrichment` (emails/decision-makers/CSV enrichment/TAM) and `skills/listbuilding-scraper` (Google Maps scraping + ⚡ Instant US Database) |
| Team-distribution copies | `atishayjain708/cold-email-skills-hyperke` → `lead-list-enricher` + `listbuilding-scraper` (pushed by toolkit `scripts/sync-to-team-repo.sh`) |
| Ready-to-run curl scripts per UI page + MCP setup | [`requests/`](requests/README.md) — still maintained |

Freshness is enforced by toolkit `scripts/check-drift.sh` (local) and the team repo's
weekly `skills-freshness` GitHub Action (public `/api/meta` comparison) — a skill whose
`Verified live:` stamp ages past 21 days raises an alarm automatically.
