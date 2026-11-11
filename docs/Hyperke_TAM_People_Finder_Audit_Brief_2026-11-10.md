# Hyperke TAM, People Finder, and Email Verification: Audit Brief

**Prepared:** 10 November 2026  
**Intended for:** Claude Code or another repository-aware AI agent running in VS Code, with read-only access to the relevant Hyperke codebases and documentation.  
**Assignment type:** Investigation, architecture assessment, user journey design, and implementation feasibility. **Do not implement or deploy changes during this assignment.**

## 1. The request, simplified

The proposed default process for roughly 90% of Hyperke list-building cases is:

1. **Build the account TAM.** Starting from the client's ICP, discover and qualify relevant companies using the VPS and available sources such as QuickEnrich (QE), GetLeads (GL), LeadOcean (LO), and Blitz. Preserve all useful source/provenance details.
2. **Find the right people and their emails.** Take those accounts into the existing [ListBuilding People page](https://listbuilding.eagleinfoservice.com/#/people). Find contacts across all usable internal/provider resources using target job titles, optional person locations, and at least one company identifier. Enrich work emails through the appropriate waterfall.
3. **Verify a selected batch of emails.** Give operators a convenient page or flow in ListBuilding to verify a chosen portion of the list, including normal and catch-all addresses, then export results with clear statuses.

**Required company identifiers:** at least one of `company_linkedin`, `domain`, `company_name`. When more than one is present, the stated matching priority is **company LinkedIn URL > company domain > company name**. Determine whether the implementation should use other provided identifiers for corroboration or fallback without overriding the priority.

**Agreed operational aim:** a largely self-serve, repeatable three-stage process on Hyperke's own infrastructure, without Clay as a required processing step or Clay row/table limits. Most cases should not require bespoke sourcing. The process must still support exceptions.

**Atishay's qualification:** comprehensive account discovery still matters. Directory scraping, Google searches, public/industry data, lookalikes (such as DiscoLike), Prospeo, Apollo, Clay, and the discovery/signal playbook may be necessary for specific ICPs or unusually hard TAMs. Do not erase those routes or misrepresent the default provider set as exhaustive. Audit which are currently accessible, integrated, or only manual/optional.

**Three separate measures, not one claim of 'real TAM':**

- **Qualified account TAM:** unique qualifying companies, with discovery sources, ICP qualification rules, and coverage caveats.
- **Contact TAM:** unique in-ICP people found within those accounts, before and after email enrichment.
- **Usable / verified email TAM:** deduplicated addresses broken down by verification outcome (deliverable, catch-all/accept-all, invalid, unknown, etc.), with agreed sending eligibility.

The statement "find ALL people" is an objective to maximize recall, not a guarantee. The audit must determine measurable completeness, provider caps, missing coverage, and cases where an exhaustive result is impossible to prove.

## 2. Your mission as the auditing agent

Perform a **deep, evidence-backed, read-only audit** of the existing Hyperke ListBuilding website, its backend and VPS services, current TAM/account discovery capabilities, company-to-people search, email enrichment, verification, and relevant documentation. Find out what actually works today, what is partially built, what is merely documented, and what is absent.

You must:

1. Locate the relevant repositories, services, frontend routes, APIs, jobs, database/schema, scripts, provider integrations, configuration, tests, runbooks, product specs, SOPs, and operational documentation.
2. Trace each of the proposed three stages end to end, from what an operator enters through backend execution, provider calls, persistence, result counts, export, and error handling.
3. Compare the current implementation with the exact desired workflow and produce a **prioritized gap assessment**.
4. Identify the **most feasible product and architecture design** for an AM/list builder operating the ListBuilding website, favoring reuse and minimal changes where the evidence supports it.
5. Surface high-impact unanswered questions, propose defaults, and explain how answers change scope or design.
6. Return a single well-structured Markdown audit report suitable for an independent reviewer to validate and use as the basis for a later implementation plan.

**Do not rely on old architecture documents as proof of current production behavior.** Provider order and capabilities have changed over time. Establish the current truth from code, configuration, deployments, and safe checks, and date every finding.

## 3. Non-negotiable audit boundaries

- **Read-only:** no source edits, PRs, migrations, jobs, production writes, deployments, restarting services, modifying credentials, or changing verification/provider settings.
- **No unapproved billable activity:** do not launch bulk provider searches, paid enrichments, verification runs, scraping jobs, or production load tests. If a useful test would cost credits or mutate stored data, specify it as a proposed test with estimated cost and request authorization later.
- You may inspect code, current configuration names (not secret values), logs with sensitive information masked, docs, existing job metadata, schemas, static UI and API definitions, and pre-existing tests/fixtures. Only perform genuinely safe, non-mutating live probes if authorized in the environment and their side effects are understood.
- Do not print, copy into the report, or expose API keys, passwords, customer PII, raw prospect lists, or unrelated business data. Use redacted representative examples.
- Never claim a feature works just because a function, endpoint, or screen exists. Distinguish **implemented**, **exposed in UI**, **wired to backend**, **deployed**, **observed working**, and **documented only**.
- If a codebase, server, or private document is inaccessible, mark the corresponding finding **not verified** and explain the smallest access request needed. Do not guess.
- Treat this as an audit and design task. **Stop before implementation**, even if fixes look simple.

## 4. Investigation workstreams

### A. Map the existing system and its truth sources

- Identify repositories and their branches/commits; deployment version; production versus staging; website frontend framework and routing; API/backend framework and endpoint groups; worker queues/services; storage and cache layers; relevant cron/systemd/container jobs.
- Find the owner of each operational component and any stale, duplicated, or superseded services, including older people-finding interfaces such as FindX if present.
- Inspect README, architecture diagrams, API docs/OpenAPI, provider integration notes, TAM/list-building SOPs, signal discovery playbook, previous audits, issues, tests, and internal operator instructions.
- Inventory all related UI routes, especially `/#/people`, current TAM screens, file import/export, job progress/results, email enrichment, and verification.
- Resolve discrepancies among docs, code, deployed configuration, and observed behavior. Cite exact files/symbols/line ranges, API route definitions, and timestamps for every significant claim.

### B. Stage 1: Account TAM generation and qualification

Determine how operators currently go from a client's ICP to a maximally complete, qualified account list.

Inspect:

- Company discovery via QE, GL, LO, Blitz, internal company DB, existing TAM mechanisms, CSV/API jobs, saved searches, filters, and provider-specific routes. **Do not assume all four providers can perform equivalent company discovery.** Separate company search, domain resolution, company enrichment, contact search, and email lookup capabilities.
- ICP filters: geography, industry, employee count, revenue, ownership/company type, exclusions, technology, founded date, LinkedIn presence, domain, keywords, recent hires, and any signal-specific conditions.
- Public directory scraping, Google/Maps searches, lookalikes/DiscoLike, Prospeo, Apollo, Clay, Outscraper, industry directories, job boards, and custom scripts as either default, optional, or unsupported routes. Locate any current signals playbook and map its discovery methods to actual tools.
- Multi-source union, company identity resolution, website/LinkedIn canonicalization, duplicates/subsidiaries/branches, missing domains, same-name companies, invalid companies, hard exclusions, and ICP qualification/QA.
- Provider/result caps, pagination, cursor behavior, query partitioning/sharding, completeness heuristics, free-count endpoints, rate limits, fair-use limits, retry logic, caching, and incremental discovery until diminishing returns.
- Exact input/output contract for taking the qualified account list into Stage 2. Record whether `company_linkedin`, `domain`, `company_name` are actually populated, trusted, normalized, and exportable.
- How TAM counts are calculated, where they can be inflated/undercounted, and what can honestly be represented to a client.

**Deliver:** a current Stage 1 workflow, source capability matrix, coverage/cost/cap risks, and a proposed standard/default route with clear exception triggers.

### C. Stage 2: Company-scoped People search and email enrichment

Thoroughly inspect the current ListBuilding `/#/people` page and the complete backend path it invokes. Establish which features already exist, are broken, or are missing.

At a minimum, audit:

1. **Inputs:** single-company versus bulk list/CSV/URL; per-row or global title filters; optional person location filters; support for `company_linkedin`, `domain`, and `company_name` in any combination; company identifier precedence and fallback; normalization; validation; ambiguous matches; and precise error reporting for bad rows.
2. **Person search:** all internal and provider people-finding resources available today; routing/order; whether providers run as fallbacks or are unioned; full result retrieval with pagination/cursors; provider max result sizes; automatic partitioning; dedupe by LinkedIn/person identity; and logic preventing missed contacts.
3. **Targeting:** title families versus strict/exact titles, aliases/seniority/negative titles, matching behavior, location semantics (person vs HQ vs office), country/state/city, optional filters, company/people filtering order, and off-ICP counts. Verify whether strict title matching is presently enabled by default and whether old LinkedIn-company search bypass defects are still relevant.
4. **Email enrichment:** currently deployed waterfall order by input mode; internal DB lookup; QE, GL, LO, Blitz and any other active providers; work versus personal fallback; verified-versus-unverified sources; per-provider provenance; MailTester or other validity checks; treatment of catch-alls; misses/negative caching; quota skips and kill switches; first-hit versus best-email strategy; and persistence back into the shared DB.
5. **Bulk execution:** job submission, queue/worker concurrency, batches, resumes, idempotency, retries, status/progress, cancellation, expected runtime, rate limits, credits, and handling of 10k+ people or many thousands of companies without UI timeouts or truncated results.
6. **Output:** company and person identifiers, matched title, location, LinkedIn, email(s), email source, verification status, date/freshness, source/search provenance, costs and counts, unmatched companies, off-ICP rows, and export schema. Verify both in-UI review and downloadable CSV/API consumption.
7. **Usability:** Is a list builder truly able to start from the Stage 1 export, enter titles and optional locations, click a manageable number of actions, track progress, review missing/partial results, and obtain a clean export? Describe current friction exactly.

Important edge cases to trace with fixtures or code evidence:

- Only company LinkedIn URL; only domain; only company name; multiple identifiers conflicting; invalid LinkedIn; redirects/rebrands; two companies with the same name; missing identifier.
- One company versus thousands; zero-match companies; one company with more people than a provider's page/depth limit; duplicate people returned by multiple providers; missing person LinkedIn.
- Titles with variations, job-family matching versus strict titles, excluded titles, optional location left blank, multiple locations, location missing from a source.
- Provider partial failure, quota depletion, all-provider miss, stale cache, a known work email and a personal email, invalid email, accept-all domain, and repeated reruns of the same input.

**Deliver:** an input/provider/response contract, full route-to-code trace, a current-versus-required matrix, a reliability and completeness assessment, and a recommended Stage 2 UX.

### D. Stage 3: Normal and catch-all email verification

Find the present email-verification mechanisms already used in enrichment and in any standalone tool. Inspect the relevant APIs, libraries, verifiers (e.g. MailTester or other current integrations), databases, jobs, pricing/quota rules, and persistence.

Answer explicitly:

- What exactly is meant in the existing system by "normal" verification and "catch-all" verification? Are these different modes, second-pass routes, different vendors, or simply result categories? **Do not assume the terms are interchangeable.**
- Which email validity statuses exist in code and the UI? What evidence underlies each status? How are catch-all/accept-all, valid, invalid, risky, unknown, disposable, and role addresses handled?
- Can an operator upload, paste, or select **only a chunk** from Stage 2 rather than verify an entire list? Can it be batched, resumed, deduplicated, billed/limited, rechecked later, and exported with status and verifier provenance?
- Does verification run automatically on all enrichment or only on selected inputs? Can the website re-use recent valid cached results and avoid redundant paid verification?
- Are catch-all addresses actually verifiable with the available tools, or only risk-classifiable? Distinguish what is technically provable from what the UI should promise. Define recommended operator actions for each status rather than silently marking catch-all as safe.
- How would a new standalone ListBuilding verification page fit the existing auth/roles, job system, APIs, and results downloads? Could a tab or extension of the current page be more feasible?

**Deliver:** current verifier architecture and classifications; missing capabilities; least-complex credible UX; validation, cost, and quality safeguards.

### E. Shared infrastructure, economics, and risk

Inspect cross-stage issues:

- Existing data store, schema and write paths, company/person/email canonical IDs, dedupe and upsert, provenance lineage, lookup freshness, ownership/client isolation, and auditability.
- Existing infrastructure throughput, worker stability, retry/queue behavior, API latency/timeouts, 5xx rates, service health, job observability, and operator support burden.
- Rate/cost/quota boundaries, fair-use constraints, paid fallbacks, per-client budgets, API limits, provider coverage overlap, source switching, and whether the advertised "unlimited" sources are truly unmetered or only fair-use-based.
- Permission model, personal-data handling, export access, logging/redaction, role restrictions, abuse prevention, and retention policies.
- The impact of doing this entirely inside ListBuilding without depending on Clay. Identify any actual Clay dependencies and separate optional enrichment/discovery from critical-path requirements.
- Test coverage, fixtures, migrations, rollback feasibility, and production-safe observability for a later rollout.

## 5. Proposed product experience to evaluate

Do not simply recommend the most elaborate design. Compare at least three plausible options grounded in existing code:

1. **Minimal extension:** keep current TAM tooling, improve `/#/people` with bulk company inputs and unified people finding, plus verification as a small dedicated page or tab.
2. **Guided three-step ListBuilding workflow:** account import/discovery, people finding + email enrichment, selected verification, all connected under one project/job with stage-wise counts and exports.
3. **Backend-first unification:** reusable APIs/jobs with a thin operator-facing UI, preserving script/VPS flexibility for complex TAM sourcing.

If there is a better option supported by the audit, include it. For each, score relative development effort, code reuse, operational complexity, performance at scale, risk, maintenance, and time-to-value. Pick **one recommended route** and explain why.

Design the actual operator journey: initial ICP/account CSV or saved TAM, identifier mapping, global/per-row target titles, optional people locations, match settings, previewed account count, run confirmation, progress/completeness indicators, missing accounts, contact and email counts, selection of verification subset, normal/catch-all result treatment, QA, dedupe, and final exports. Be specific enough for a product owner to validate without writing implementation code.

## 6. Acceptance scenarios the eventual design must support

Assess whether each is **supported today, partially supported, absent, or unverified**. Describe tests required, but do not run costly or mutating tests during the audit.

1. Operator supplies 1,000 qualified companies with a mix of LinkedIn URLs, domains, and names; chooses several title families; leaves person location blank; obtains deduplicated contacts with work-email enrichment and a progress/result export.
2. Operator enters all three company identifiers on some rows; resolution consistently favors LinkedIn, uses other fields only under a documented corroboration/fallback rule, and flags conflicts rather than joining the wrong company.
3. Same input rerun does not produce avoidable duplicate contacts, double charges, or corrupted shared data.
4. Search discovers more than the first page/depth cap where providers allow it, and the interface identifies when coverage is capped or unknown.
5. Operator can see counts for initial companies, qualified accounts, matched companies, matched people, in-ICP people, found work emails, and verification outcomes. Numbers reconcile with the exported rows.
6. Operator selects 2,000 of 20,000 enriched contacts for verification, can distinguish deliverable and catch-all results, and receives a usable status-bearing export without processing the other 18,000.
7. Failures in one people/enrichment provider do not silently drop companies or make the entire job disappear. The UI indicates partial coverage and safe retry possibilities.
8. Complex ICP needs a directory or lookalike source in Stage 1. The process supports that exception without forcing Clay into the default people/enrichment pipeline.

## 7. Required evidence standards

For every important finding, include:

- **Claim** and **status** (confirmed deployed / confirmed in code / documented only / inferred / not verified).
- **Evidence pointer:** repo and branch/commit, file path + symbol + line numbers, route/API definition, test name, document link/section, safe command output summary, and/or masked UI evidence.
- **Practical impact:** what a list builder can or cannot accomplish today.
- **Confidence:** high/medium/low and reason.

Use a compact evidence register with IDs such as `E-001`, then cite those IDs in the audit tables. Separately record contradictory evidence and stale references. Do not invent performance, coverage, prices, provider quotas, or success rates; label them unknown if not verifiable.

When useful, create small **non-sensitive illustrative input and output schemas** and diagrams to make the contract understandable. Do not include real client/prospect records.

## 8. Questions to ask after reconnaissance

First investigate as far as access allows. Then ask only questions whose answers materially change architecture, cost, or acceptance. For each, state **why it matters, the options, and your recommended default**. Do not block the entire audit for issues that can be listed as assumptions.

Potential questions to validate rather than assume:

1. Is "account TAM" the full company universe for a client's ICP, a provider-reachable subset, or a qualified universe bounded by specific signals and freshness?
2. Is "ALL people" intended to include all employees, or only all matching titles/locations? Is company/location filtering strict or recall-maximizing?
3. Should the default workflow accept a bulk Stage 1 export, a manual company entry, or both? Which CSV fields must be compulsory?
4. Are titles and person locations global job filters, per-company fields, or both? What is the desired strict-match default?
5. Is the LinkedIn > domain > name priority a simple first-present preference or a verified identity resolution hierarchy with fallback and conflict alerts?
6. Are personal emails allowed, kept as flagged fallback, or excluded from the default export?
7. What does "normal+catchall" mean operationally, which verifier/result statuses are acceptable, and what is the sendability policy for catch-all and unknown addresses?
8. Who uses the interface and at what scale per job/day? What throughput, runtime, price/credit ceiling, concurrency, and client separation are required?
9. Is a consolidated project-based UI preferred, or should existing separate tools be minimally connected to ship faster?
10. What level of human QA/ICP approval is mandatory before a job moves from account discovery to people finding and finally to export?

If the actual code and docs resolve any question, answer it with evidence and do **not** ask it again.

## 9. Final deliverable: exact file and structure

Write **one Markdown file**, named:

`HYPERKE_TAM_PEOPLE_VERIFICATION_AUDIT_2026-11-10.md`

Save it in the active repository/workspace's designated audit/docs location or, if none exists, the current working directory. In your final response, give the **exact absolute file path**, concise major findings, and a list of blockers/questions. Do not only provide a chat summary.

The report must contain these sections, in this order:

1. **Executive verdict:** what exists, what is missing, feasibility of the three-step process, top recommendation, and confidence.
2. **Scope, access, and evidence:** repositories/branches, documents, services, safe commands/checks, inaccessible areas, evidence register.
3. **Current architecture:** component inventory and end-to-end flow diagrams for account TAM, people search/enrichment, and verification.
4. **Live capability and provider matrix:** QE, GL, LO, Blitz, internal DB, and all other relevant sources, separated by company discovery, person search, email finding, verification, caps, cost, and actual integration status.
5. **Stage 1 audit:** ICP/account discovery, qualification, counts, dedupe, exceptions, gaps.
6. **Stage 2 audit:** `/#/people` UI, API contracts, identifier precedence, filtering, waterfall, caps, bulk jobs, reliability, export, gaps.
7. **Stage 3 audit:** current verification, normal/catch-all semantics, batch selection, costs/quality, proposed screen, gaps.
8. **Requirement-to-evidence gap matrix:** each explicit requirement mapped to status, proof, missing work, severity, and owner/component.
9. **Proposed operator experience:** concrete screens/fields/actions, stage-to-stage data flow, counts, failure states, sample schemas.
10. **Architecture options compared:** at least three options with estimated effort, reuse, trade-offs, and **one selected recommendation**.
11. **Prioritized implementation backlog:** P0/P1/P2; each item with scope, dependencies, risk, acceptance test, effort (S/M/L or person-days with assumptions), and suggested rollout phase. Clearly distinguish bug fixes from new features.
12. **Verification/test plan:** fixtures, contract tests, non-billable dry runs, optional authorized provider probes, performance/QA checks, and go/no-go criteria.
13. **Open questions and recommended defaults:** only real unresolved decisions, ranked by impact.
14. **Bottom line:** what Hyperke can deploy quickly, what remains manual/exceptional, and the conditions under which this realistically covers ~90% of cases.

Use tables for feature/provider mappings. Provide line-level evidence for technical assertions. A vague feature checklist or speculative implementation proposal is **not an acceptable audit**.

## 10. Notes from prior discussions to verify, not treat as current facts

These are leads for where to look. They may be outdated or contradicted by today's code/deployment.

- Earlier versions had a ListBuilding React/FastAPI-style stack with `/#/people` and enrichment/list-building APIs. Confirm the actual current architecture and hosts.
- Internal contacts DB, SmartProspect, QE, GL, LO, Blitz, MoltSets, and other sources have appeared in historical cascades. **The ordering changed over time.** Inspect the current mode-specific waterfall, rather than assuming the short provider list is complete.
- LeadOcean was recently reported live after QE for email enrichment, with some people-discovery fallback. Verify company search versus person search versus email enrichment separately.
- Previous TAM/list-building features included strict title matching, recent-hire/MX filters, company LinkedIn routing, and source-specific filters. Check actual UI exposure, correctness, and default values.
- Historical work referenced MailTester and email validity/catch-all persistence. Check whether a standalone verification flow already exists or could reuse backend modules.
- Historical constraints included provider pagination/depth caps, fair-use limits, cached misses, and special routing/lookup modes. Confirm present limits and how the UI informs operators.
- Prior work proposed account-first TAM, use of directory/signal sourcing, and a separation between qualifying companies and enriching contacts. Look for the current signals playbook and supporting SOPs.

**End state:** an evidence-backed report that Arjav can give back for independent requirements validation and then use to commission a scoped implementation. Do not start that implementation as part of this assignment.
