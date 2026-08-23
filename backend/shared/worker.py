"""
Gunicorn worker with decoupled arbiter-notify cadence (2026-08-20 incident).

The 12x/day "CRITICAL WORKER TIMEOUT" kills on Aug 19-20 murdered HEALTHY
workers: they were logging provider calls at the moment of death. Root cause
is a race baked into the stock UvicornWorker:

- gunicorn's arbiter murders a worker whose tmp file wasn't touched for
  ``--timeout`` seconds (murder_workers).
- uvicorn's Server touches the file via ``callback_notify`` -> worker.notify(),
  but the notify interval is hardwired to ``timeout_notify`` which
  UvicornWorker sets to the SAME ``--timeout`` value (actually timeout/2 as
  passed by the arbiter, still the same order of magnitude).

So the worker's "I'm alive" heartbeat lands at (murder deadline - epsilon).
Under event-loop starvation (the /enrich flood: 6-9K requests/3h through 4
workers, sync SQLite writes on the hot path), a single scheduling delay past
the deadline is fatal — the arbiter SIGABRTs a worker that was making
progress the whole time. Every kill destroyed any in-flight enrichment job
runner hosted by that worker.

Fix: keep the generous 600s murder timeout (a truly-hung worker SHOULD die)
but notify every 30s, leaving ~20x headroom. A worker now only gets murdered
if its loop is stalled for 20 consecutive missed notifies — i.e. genuinely
hung, not merely busy.

Loaded via gunicorn ``--worker-class shared.worker.LeadGenUvicornWorker``
(resolvable because the unit's WorkingDirectory is backend/, same as main:app).
"""

from __future__ import annotations

import os

from uvicorn.workers import UvicornWorker

# How often the worker tells the arbiter it's alive. Default 30s = ~20x
# headroom under the 600s murder timeout. Env-tunable without redeploy.
NOTIFY_INTERVAL_SECONDS = float(os.getenv("WORKER_NOTIFY_INTERVAL", "30"))


class LeadGenUvicornWorker(UvicornWorker):
    """UvicornWorker that pings the arbiter every NOTIFY_INTERVAL_SECONDS.

    The stock class sets ``timeout_notify = self.timeout`` (the murder
    deadline itself). We keep every other behavior identical and only shrink
    the notify interval, converting the notify/murder race into 20x slack.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        interval = min(NOTIFY_INTERVAL_SECONDS, self.timeout) if self.timeout else NOTIFY_INTERVAL_SECONDS
        # Config is constructed in super().__init__ with timeout_notify=self.timeout;
        # mutate it after the fact (it is read live by Server.on_tick).
        self.config.timeout_notify = interval
