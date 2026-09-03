from threading import BoundedSemaphore, Lock
from typing import NamedTuple, Optional

__all__ = ['VideoViewTrimmedBatchConfig', 'VideoViewTrimmedBatchController']


class VideoViewTrimmedBatchConfig(NamedTuple):
    """
    Knobs of the trimmed video_view BATCH path (default off -- a job built
    without a controller runs the single-aid path untouched). All three are
    the canary staircase's dials and INITIAL HYPOTHESES pending load-test
    calibration, like the batch timeouts in service/Service.py -- none of them
    is an approved final value.
    """
    # aids per batch request (conf caps this at the worker's MAX_AIDS)
    batch_size: int
    # 0-1 share of aids routed through the batch path; the rest (and every
    # aid after the breaker fires) take the single-aid path
    batch_fraction: float
    # pool-wide cap on SIMULTANEOUS batch invocations. Without it, every one
    # of the pool's fetch threads (300 in 51_) could hold a batch in flight at
    # batch_fraction=1, putting batch_size x 300 requests on the upstream at
    # once. The cap bounds upstream in-flight from the batch path at
    # batch_size x max_concurrent_batches, INDEPENDENT of the fetch thread
    # count; the single-aid path (including breaker fallback) is deliberately
    # NOT throttled by it. Default 30 keeps batch-path upstream in-flight at
    # or below today's 300 for batch_size <= 10 -- an initial safe assumption
    # for the calibration experiments, NOT a chosen W.
    max_concurrent_batches: int = 30


class VideoViewTrimmedBatchController:
    """
    Pool-shared runtime state of the batch path. ONE instance per run is
    shared by every FetchVideoRecordJob in the fetch pool, and owns the two
    cross-job concerns:

    1. The CONCURRENCY GATE (a semaphore of max_concurrent_batches slots):
       jobs hold a slot around the batch HTTP call, bounding simultaneous
       batch invocations pool-wide. Only batch invocations are gated -- the
       single-aid path never touches the controller.

    2. The CIRCUIT BREAKER: a one-way, run-scoped switch. Any job may trip it
       (trip(reason)); once tripped, every remaining aid takes the single-aid
       path, and a batch response still in flight at trip time is discarded
       by its job on return (jobs re-check is_enabled() before interpreting a
       response). Trip causes, decided by the job: a misalignment (the
       path's DATA cannot be trusted) or ANY whole-batch failure (the path is
       unavailable; a few extra single-aid calls beat any retry bookkeeping).
       There is deliberately no failure counting, no retry state and no
       backoff in here: reliability and auditability first.
    """

    def __init__(self, config: VideoViewTrimmedBatchConfig):
        self.config = config
        self._lock = Lock()
        self._enabled = True
        self.trip_reason: Optional[str] = None
        self._slots = BoundedSemaphore(config.max_concurrent_batches)

    # --- circuit breaker ---------------------------------------------------

    def is_enabled(self) -> bool:
        with self._lock:
            return self._enabled

    def trip(self, reason: str) -> bool:
        """Disable the batch path for the rest of the run. True iff THIS call
        flipped the switch (so exactly one job logs the critical line)."""
        with self._lock:
            if not self._enabled:
                return False
            self._enabled = False
            self.trip_reason = reason
            return True

    # --- concurrency gate --------------------------------------------------

    def try_acquire_slot(self, timeout_s: float) -> bool:
        """Wait up to timeout_s for a batch-invocation slot (timeout_s <= 0 is
        an instant probe). Callers loop on this (re-checking their deadline
        and the breaker between waits) rather than blocking indefinitely."""
        if timeout_s <= 0:
            return self._slots.acquire(blocking=False)
        return self._slots.acquire(timeout=timeout_s)

    def release_slot(self):
        self._slots.release()
