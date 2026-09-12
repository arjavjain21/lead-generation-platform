"""
Gunicorn worker with decoupled arbiter-notify (2026-08-20 incident,
hardened 2026-09-11).

The 12x/day "CRITICAL WORKER TIMEOUT" kills on Aug 19-20 murdered HEALTHY
workers: they were logging provider calls at the moment of death. Root cause
is a race baked into the stock UvicornWorker:

- gunicorn's arbiter murders a worker whose tmp file wasn't touched for
  ``--timeout`` seconds (murder_workers).
- uvicorn's Server touches the file via ``callback_notify`` -> worker.notify(),
  but the notify interval is hardwired to ``timeout_notify`` which
  UvicornWorker sets to the SAME ``--timeout`` value.

So the worker's "I'm alive" heartbeat landed at (murder deadline - epsilon).
The first fix (2026-08-20) shrank ``timeout_notify`` to 30s — ~20x headroom.

2026-09-11 proved that insufficient: ``callback_notify`` still fires from
``Server.on_tick``, i.e. ON THE EVENT LOOP. Two enrichment jobs + a contacts-DB
500-retry storm starved the loop's timer callbacks for >900s while provider
coroutines kept logging — the arbiter SIGABRT'd workers that were alive and
working the whole time (07:42:24 and 07:49:02 murders; job 8f10dfe7 frozen
'running' for 16h because it missed the startup reaper by 13s).

Hardening: a daemon THREAD now notifies the arbiter every 30s independent of
the event loop. A pure-Python thread gets GIL slices even under total loop
saturation, so a worker is only murdered when the PROCESS is genuinely hung
(a C extension holding the GIL for 15+ minutes). ``WorkerTmp.notify()`` is a
single atomic ``os.utime(fd)`` — safe to call from two threads, so the
loop-based notify is kept as harmless redundancy.

Loaded via gunicorn ``--worker-class shared.worker.LeadGenUvicornWorker``
(resolvable because the unit's WorkingDirectory is backend/, same as main:app).
"""

from __future__ import annotations

import logging
import os
import threading
import time

from uvicorn.workers import UvicornWorker

logger = logging.getLogger(__name__)

# How often the worker tells the arbiter it's alive. Default 30s = ~20x
# headroom under the 600s murder timeout. Env-tunable without redeploy.
NOTIFY_INTERVAL_SECONDS = float(os.getenv("WORKER_NOTIFY_INTERVAL", "30"))


def _notify_interval(timeout: float | None) -> float:
    """Notify at most as often as the murder deadline allows (never slower
    than the configured interval; never longer than ``timeout``)."""
    return min(NOTIFY_INTERVAL_SECONDS, timeout) if timeout else NOTIFY_INTERVAL_SECONDS


def _notify_forever(worker: "LeadGenUvicornWorker") -> None:
    """Thread body: notify the arbiter forever, surviving notify failures.

    Must never exit on its own: a dead notifier is a murdered worker. Only
    sleep-interrupting exceptions (test harness / interpreter shutdown)
    propagate.
    """
    while True:
        time.sleep(_notify_interval(worker.timeout))
        try:
            worker.notify()
        except Exception:  # noqa: BLE001 - see docstring: never die
            logger.warning(
                "Arbiter notify failed for worker %s; retrying next interval",
                getattr(worker, "pid", "?"),
                exc_info=True,
            )


class LeadGenUvicornWorker(UvicornWorker):
    """UvicornWorker that pings the arbiter from a dedicated thread.

    The stock class relies on ``Server.on_tick`` (the event loop) for
    ``callback_notify``; under event-loop starvation the loop's timers can
    starve past the murder deadline. The notify thread removes that
    dependency. The loop-based notify is left enabled (double utime of the
    same fd is harmless).
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        interval = _notify_interval(self.timeout)
        # Config is constructed in super().__init__ with timeout_notify=self.timeout;
        # mutate it after the fact (it is read live by Server.on_tick).
        self.config.timeout_notify = interval

    def run(self):
        # __init__ runs in the arbiter pre-fork; threads do not survive fork,
        # so the notifier MUST start here — run() executes in the worker child
        # (Worker.init_process -> self.run()).
        thread = threading.Thread(
            target=_notify_forever,
            args=(self,),
            name="leadgen-arbiter-notify",
            daemon=True,
        )
        thread.start()
        super().run()
