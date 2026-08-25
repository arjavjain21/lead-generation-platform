# Design: Dedicated Job-Runner Process

Status: DESIGN (no code changed). Author context: 2026-08-24 abandoned-job churn.
Verified against `feat/enrichment-partial-download-resume` @ 4969ada.

---

## 1. Goal + Non-Goals

**Goal.** Move all long-running job execution (scraper, enrichment CSV jobs, LinkedIn
flows, phone enrichment) out of the 4 gunicorn request workers into a dedicated,
long-lived consumer process, so `--max-requests 1500` worker recycling (the Jul-27 OOM
tourniquet, still live in `worker-notify.conf`) can never murder an in-flight job again.
Request workers become enqueue + status/SSE/download servers only.

**Non-goals.**
- No change to the provider cascade, provider clients, or `VALID_PROVIDERS` sets.
- No destructive change to `jobs.db` — additive columns only, via the existing
  idempotent `ALTER TABLE` block in `shared/db.py:204-300` (`init_db()`).
- No change to `job_type` / `status` literal values (reapers, SSE, UI hard-code them).
- No big-bang migration. Every phase ships alone and rolls back with an env flag.
- No frontend work (source is external; `queued` status already renders — verified
  strings in `frontend/index.html`).

---

## 2. Current State Map (verified)

### 2.1 Where runners spawn today

| # | Site | Endpoint → runner | Mechanism |
|---|------|-------------------|-----------|
| 1 | `main.py:689` | `POST /api/jobs/{id}/chain` → `enrichment_routes._run_domain_enrich_job` | `BackgroundTasks` |
| 2 | `enrichment/routes.py:3667` | `POST /jobs` (legacy) → `_run_job` | `BackgroundTasks` |
| 3 | `enrichment/routes.py:4791` | `_restart_job_core` → `_run_domain_enrich_job` | `BackgroundTasks` **or bare `asyncio.create_task`** (`:4795`, auto-resume path) |
| 4 | `enrichment/routes.py:5041` | `POST /by-domains` → `_run_job` | `BackgroundTasks` |
| 5 | `enrichment/routes.py:5241` | `POST /flows/domain-enrich` → `_run_domain_enrich_job` | `BackgroundTasks` |
| 6 | `enrichment/routes.py:5670` | `POST /search/companies/enrich` → `_run_domain_enrich_job` | `BackgroundTasks` |
| 7 | `enrichment/routes.py:5755` | `POST /by-linkedin` → `_run_linkedin_job` | `BackgroundTasks` |
| 8 | `enrichment/routes.py:6029` | `POST /by-linkedin-v2` → `_run_linkedin_v2_job` | `BackgroundTasks` |
| 9 | `phone_enrichment/routes.py:183` | `POST /jobs` → `_run_job_background` | `asyncio.create_task`, retained in `_active_phone_jobs` for cancel-revoke |
| 10 | `scraper/dispatch.py:170` | dispatcher claim → `scraper/routes.py:1590 _launch_claimed_job` → `_run_job` / `_run_job_with_tasks` | `asyncio.create_task` |
| 11 | `enrichment/routes.py:4298,5444,5891,5928,6188,6225` | post-job contacts drain `_run_background_sync` | bare `create_task` |

Per-worker lifespan loops (`main.py:147-207`): call-tracker purge/health, job_events
prune, enrichment auto-resume, **scraper dispatcher + runtime guard**, outbox retry.
These run in all 4 workers; cross-worker races are settled by DB claims.

### 2.2 Lifecycle + cross-worker state (why churn happens)

- **In-memory, per-process:** `_job_signals: dict[str, asyncio.Event]`,
  `_active_jobs: set`, `_cancelled_jobs: set` (enrichment `routes.py:418-422`; scraper
  `routes.py:47-49`; phone `_active_phone_jobs` / `_cancelled_phone_jobs`). Restored at
  boot from the `job_state` table (`main.py:161-168`, `job_store_base.py:507-534`).
- **DB, shared:** `jobs` row (status, counters, `last_heartbeat`, `partial_output_path`,
  `resume_claimed_at`), `job_events` (SSE feed), `job_checkpoints` / `task_checkpoints`
  (resume), `job_state` (cancel/active persistence).
- **Heartbeat:** every runner spawns a 30s `heartbeat_loop`; reapers abandon any job
  whose heartbeat is >2 min stale and >3 min old. Today reapers are job_type-scoped but
  **enrichment (`enrichment/job_store.py:105-121`) and phone
  (`phone_enrichment/job_store.py:210-232`) still reap `'queued'` rows with a NULL
  heartbeat**; only the scraper store was fixed to running-only
  (`scraper/job_store.py:67-90`, 2026-08-24).
- **Worker recycle:** `--max-requests 1500 --max-requests-jitter 500` (drop-in
  `worker-notify.conf`, live on the running arbiter — verified via `/proc`) recycles each
  worker every few minutes under load, killing every runner it hosts → reaper marks
  `abandoned` → auto-resume (`shared/auto_resume.py`) creates a child. Tonight's 22-card
  fan-out came from the pre-4969ada claim race; the cap/claim fixes stopped the
  *multiplication*, but each recycle still loses up to one batch of work, burns
  `restart_count`, and mints a new UI card.
- **Claim precedents.** `scraper/dispatch.py:73-107` claims queued→running with a
  SELECT-then-CAS-UPDATE on the thread-local conn. `shared/auto_resume.py:300-435
  claim_scraper_resume_job` documents why that pattern lost races across processes
  (stale snapshots, child INSERT outside the txn) and does it correctly: **one
  `BEGIN IMMEDIATE` on a dedicated connection covering re-check + root-count bump +
  child INSERT.**

### 2.3 Signals: SSE, cancel, backpressure

- **SSE already survives a remote runner.** `GET /api/enrichment/jobs/{id}/stream`
  (`routes.py:3723-3783`) replays `job_events` from `seq`, then waits on the in-worker
  `_job_signals` Event **with a 2 s timeout, falling back to `await asyncio.sleep(2.0)`**
  when no signal exists (`:3774`). The scraper stream is identical (`routes.py:835-841`).
  So when the runner lives in another process, SSE silently degrades to 2 s DB polling —
  correct behavior, zero code change. The in-memory Event is only a latency optimization.
- **Cancel already crosses processes.** Cancel endpoints write `_cancelled_jobs` (fast
  path) *and* `store.set_cancelled(job_id)`; runners gate on
  `is_job_cancelled_or_abandoned()` — a DB read (`job_store_base.py:582-598`) checked
  per row / per batch of 50 / per task. Phone also revokes the retained task, which only
  works in-worker; the DB status is the real mechanism.
- **Backpressure today:** scrapers only — `MAX_CONCURRENT_SCRAPER_JOBS=6` platform-wide,
  `SCRAPER_JOBS_PER_WORKER=2` (`scraper/dispatch.py:51-61`). Enrichment/phone CSV jobs
  have **no job-level cap**; only `enforce_job_limit` (history pruning) and the
  per-job-internal `DOMAIN_CONCURRENCY=25` / `LINKEDIN_CONCURRENCY=15`
  (`list_builder.py:229-230`). `_ENRICH_SEMAPHORE` (`routes.py:433-443`) throttles the
  *synchronous* `/enrich` endpoint only, not CSV jobs.
- **Process-local rate limiters / breakers:** Blitz's limiter is module state
  (`blitz_client.py:35-56`), so today 4 workers can each drive up to 25 RPS
  (theoretical 100 RPS). Circuit breakers (`shared/circuit_breaker.py`) are likewise
  per-process.

---

## 3. Options

### Option A — dedicated job-runner systemd service (RECOMMENDED)

A second systemd unit (`lead-generation-job-runner.service`), same venv, same
`WorkingDirectory=backend`, same `.env`, **no network port** — it only opens SQLite and
reads/writes the shared FS (`data/` + `/mnt/disk/outputs` symlink). It runs one asyncio
loop that claims queued jobs and executes the *existing* runner functions
(`_run_domain_enrich_job`, `_run_job`, `_run_linkedin_job`, `_run_linkedin_v2_job`,
`_launch_claimed_job`, phone `_run_job_background`) unchanged. Web workers keep: HTTP,
SSE, downloads, cancel, resume/restart endpoints — all of which already work off the DB.

*Pros:* removes the root cause (runners live in a process with no `--max-requests`);
recycling web workers becomes harmless; job-table writers drop 4→1 (less WAL
contention); provider limiters + circuit breakers become real for job traffic
(one process = one 25 RPS Blitz limiter); job memory gets its own cgroup instead of
competing with the `/enrich` flood under the web tier's `MemoryMax=3G`.
*Cons:* new service to operate/monitor; runner-down = queued backlog; requires persisting
runner kwargs that today only live in memory; one more process writing SQLite.

### Option B — extend the scraper dispatcher pattern to all job types

Keep runners in web workers; add `queued` + claim + caps for enrichment/phone
(generalize `scraper/dispatch.py`). Cheap (~1/3 the work) and immediately fixes the
missing enrichment backpressure. **But it does not fix the stated root cause:** runners
still die on every recycle, auto-resume still mints children, `restart_count` still
burns, and one batch of provider work per recycle is still lost. It is the right
*stepping stone* and the wrong *end state*.

**Recommendation: Option A, reached through Option B's queue semantics.** Phases 0-1
below build the queue/claim/payload machinery exactly as B would; the runner service is
then a small consumer swap. Nothing is thrown away.

---

## 4. Proposed Architecture (Option A)

### 4.1 Queue semantics
- Jobs are created `status='queued'` (already the `create_job` default) with a new
  additive `runner_params TEXT` column holding the JSON kwargs the endpoint would have
  passed to the runner (`rows` excluded — re-read from `uploads/` or parent output, as
  `_restart_job_core` already proves works). Scraper needs nothing (its
  `_launch_claimed_job` already reconstructs from `regions`/`query`).
- **Claim = one `BEGIN IMMEDIATE` on a dedicated connection** (port
  `claim_scraper_resume_job`'s shape): `BEGIN IMMEDIATE` → re-check
  `status='queued' AND job_type IN (<migrated types>)` → platform cap re-check →
  `UPDATE ... SET status='running', runner_id=?, claimed_at=? WHERE job_id=? AND
  status='queued'` → `COMMIT`. rowcount==1 ⇒ exactly-once even if web and runner both
  poll during a flag flip. New module `shared/job_queue.py` (~150 LOC); the weaker CAS in
  `scraper/dispatch.py:73-107` is retired with Phase 1.
- Ordering: `created_at ASC`, per-type fairness (one claim per type per tick).

### 4.2 SSE
No web-side change. The runner keeps calling `append_event` (never write `job_events`
directly); SSE endpoints keep the 2 s poll fallback (`routes.py:3774`) which is already
the cross-process path. Latency degrades from ~instant to ≤2 s — acceptable for progress
bars; terminal events are already read from the `jobs` row, not the Event.

### 4.3 Cancel propagation
Unchanged protocol, minus the in-memory shortcut: cancel endpoint keeps doing
`_cancelled_jobs.add` (harmless no-op for remote jobs) + `save_job_state('cancelled')` +
`set_cancelled`. The runner's existing per-row/batch DB checks stop it. The phone
endpoint's task-revoke becomes a no-op for runner-hosted jobs; keep it for the
transition. `job_state` remains the durable record.

### 4.4 Heartbeat + reaping
Runners keep the 30s `heartbeat()`. The heartbeat-aware reaper stays, but **scoped to
`status='running'` only for all three job types** (Phase 0) so a queued backlog can wait
indefinitely without being falsely abandoned — this is already proven safe for scrapers
(`scraper/job_store.py:67-90` docstring) and is a prerequisite for any long queue.
Abandonment recovery (auto-resume) moves into the runner process's own guard loop, and
its enrichment arm stops calling `_restart_job_core` inline — it creates the queued
child and lets the runner claim it (no bare `create_task` outside the consumer).

### 4.5 Graceful shutdown (SIGTERM drain)
Runner traps SIGTERM: set a drain flag, stop claiming, wait up to `RUNNER_DRAIN_SECONDS`
(default 120) for in-flight jobs to reach a batch boundary — enrichment checkpoints per
50-row batch (`list_builder.py:1543`), scrapers per (center, zoom) task — then exit
leaving the job `running` with a fresh heartbeat and a flushed partial CSV. On reboot the
runner's own guard reaps + auto-resumes it (checkpoints preserved, no provider rework
beyond one batch). Never mark `done` during drain. `Restart=always`, `TimeoutStopSec=150`.
Memory hygiene without murdering work: the runner exits cleanly (systemd restarts it)
after `RUNNER_MAX_JOBS_PER_LIFETIME` (default 25) completed jobs, at a job boundary.

### 4.6 Backpressure
- `MAX_CONCURRENT_JOBS` per type in the claim txn (scraper keeps 6; enrichment starts at
  3 domain + 2 linkedin; phone 2), plus `MAX_JOBS_PER_USER_RUNNING` (default 2) to stop
  one user flooding the queue.
- Queued jobs hold zero resources (the dispatcher's key property — no Event, no runner,
  no memory), so a deep backlog costs nothing.
- Web-side `/enrich` sync path is untouched (`_ENRICH_SEMAPHORE` stays per-worker).

### 4.7 Runner DOWN
Jobs stay `queued`; UI shows "queued" (already rendered). Safe *because* of the Phase 0
running-only reapers. `monitor.sh` gains an `is-active lead-generation-job-runner.service`
check beside the existing one at line 304, plus a "queued > N for > 30 min" warning.
Health signal = the runner writes its own heartbeat row (reuse `jobs`-adjacent
`job_state`-style marker or a `runner_liveness` key-value) every 30 s.

---

## 5. Migration Plan (each phase independently shippable + rollbackable)

| Phase | Scope | Gate | Rollback |
|-------|-------|------|----------|
| **0** | Reaper scoping: enrichment + phone stale queries drop `'queued'` (mirror `scraper/job_store.py:67`). Pure bug fix, needed regardless. | none (always on) | git revert |
| **1** | Runner service runs **scrapers only**. Add `shared/job_queue.py` (BEGIN IMMEDIATE claim), `runner/__main__.py` entrypoint (calls `db.init_db()`, `call_tracker.init()`, then `_launch_claimed_job`), systemd unit, drain/lifetime logic. Web scraper dispatcher disabled when `scraper ∈ JOB_RUNNER_TYPES`. | `JOB_RUNNER_ENABLED=true`, `JOB_RUNNER_TYPES=scraper` | remove types / set `JOB_RUNNER_ENABLED=false` + restart — web dispatcher resumes claiming |
| **2** | Enrichment domain jobs (`_run_domain_enrich_job` + legacy `_run_job`). Add `runner_params` column; the 6 spawn sites (`main.py:689`, `routes.py:3667/5041/5241/5670` + `_restart_job_core:4791`) stop `add_task` and persist kwargs instead. | `JOB_RUNNER_TYPES=scraper,enrichment` | flip flag back; old `BackgroundTasks` paths kept until Phase 4 |
| **3** | LinkedIn (`:5755`, `:6029`) + phone (`phone_enrichment/routes.py:183`) move. Phone job create becomes queue-native. | `JOB_RUNNER_TYPES=…,linkedin,phone` | flip flag |
| **4** | Delete in-worker runner spawn paths + per-worker dispatcher/guard/auto-resume loops (keep prune/outbox in web). Scraper `dispatch.py` retired. | `JOB_RUNNER_TYPES=all` + dead-code removal | git revert (flag default keeps old paths until this phase) |

Both services read the **same** `backend/.env`, so one `JOB_RUNNER_TYPES` list drives
both sides coherently: web workers start in-worker loops only for types absent from the
list; the runner claims only types present. During a restart window both may poll the
same type — safe by construction (exactly-once claim, DB-counted caps). `conftest.py`
gains `JOB_RUNNER_ENABLED=false` alongside the existing `ENABLE_STARTUP_REAPERS=false`.

---

## 6. Risks + Mitigations

| Risk | Mitigation |
|------|------------|
| **SQLite write contention** runner vs 4 web workers | Net improvement: job-table writers go 4→1. Claims are ms-long `BEGIN IMMEDIATE` txns, one claim per tick; `busy_timeout=30s`, 200 MB WAL cap, and the 503 `Retry-After` handler (`main.py:61-78`) already absorb bursts. Never hold a txn across provider calls. |
| **Provider rate limits** | Job-side traffic centralizes into ONE Blitz limiter (25 RPS true) instead of 4 independent ones; breakers see real job-side state. Sync `/enrich` keeps its 4-worker share — no regression vs today, and per-provider runner caps (`RUNNER_BLITZ_RPS`…) make the split tunable. Watch `provider_call_log` 429s for a week per phase. |
| **Runner memory profile** | One process hosting N concurrent jobs, each with pandas frames + 25-domain semaphores. Own cgroup (`MemoryMax=2G` initial), per-type caps, clean exit after 25 jobs. Scrapers stay capped at 6 platform-wide as today. |
| **job_events volume** | Unchanged — same `append_event` calls, same 7-day prune (stays web-side, file-gated). 670 K rows today is bounded and indexed (`idx_job_events_job_seq`). |
| **Old + new runners simultaneously** | Exactly-once via the claim txn (rowcount gate). Flags scope types; worst case both poll, one wins, loser re-reads and idles. Caps are DB counts, so limits hold regardless of how many claimants exist. |
| **Runner down / wedged** | Jobs queue (Phase 0 makes that safe), UI shows queued, monitor alerts. Liveness row + `Restart=always`. |
| **`runner_params` loss / drift** | Golden parity test: for each endpoint, given the persisted row, reconstructed runner kwargs must equal what the endpoint would have passed. `rows` always re-read from the CSV on disk (already the resume path), never stored in the DB (4.4 GB DB — do not bloat). |
| **SSE latency 2 s** | Acceptable; terminal state comes from the `jobs` row. Optional later: runner `POST`s a wake to a tiny web endpoint — explicitly out of scope now. |
| **`_restart_job_core` coupling** | Runner imports `enrichment.routes` and calls it directly (the same contract `shared/auto_resume.py:282-297` already relies on). No refactor of the 6,355-line file beyond replacing the spawn tail. |

---

## 7. Estimate

| Phase | LOC (net) | Files |
|-------|-----------|-------|
| 0 | ~30 + 40 tests | `enrichment/job_store.py`, `phone_enrichment/job_store.py` |
| 1 | ~400 (150 runner entry + 150 queue + 60 wiring + 25 unit + tests) | new `shared/job_queue.py`, new `runner/__main__.py`, new `lead-generation-job-runner.service` (+ backup of any edited unit), `main.py`, `monitor.sh`, `backend/conftest.py` |
| 2 | ~350 (120 spawn-site edits + 120 launcher + tests) | `shared/db.py` (additive column), `enrichment/job_store.py`, `enrichment/routes.py` (6 sites), `main.py`, new `runner/enrichment_launch.py` |
| 3 | ~300 | `enrichment/routes.py` (2 sites), `phone_enrichment/{routes,job_store}.py`, new `runner/phone_launch.py` |
| 4 | ~−150 net | delete spawn tails + `scraper/dispatch.py` consumer side, prune `main.py` lifespan |

Total ≈ 900-1,000 LOC across ~12 files over 4 independently shippable commits
(`feat(runner): …` per phase). Longest single review is Phase 1; Phases 2-3 are
mechanical once the launcher pattern exists.

**Do-not-do list for implementers:** no `jobs.db` truncation or destructive migration;
no change to `job_type`/`status` literals or cascade order; no new port; no contacts
endpoint outside `contacts_writer`; cancel must keep writing DB state (not just memory).
