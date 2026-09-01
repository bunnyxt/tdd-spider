from threading import BoundedSemaphore, Lock
from typing import NamedTuple, Optional

__all__ = ['VideoViewTrimmedBatchConfig', 'VideoViewTrimmedBatchController',
           'BatchEntry', 'BatchDispatchResult']


class VideoViewTrimmedBatchConfig(NamedTuple):
    """
    Knobs of the trimmed video_view BATCH path (default off -- a job built
    without a controller runs the single-aid path untouched). batch_size,
    batch_fraction and max_concurrent_batches are the canary staircase's dials
    and are INITIAL HYPOTHESES pending load-test calibration, like the batch
    timeouts in service/Service.py -- none of them is an approved final value.
    """
    # aids per batch request (conf caps this at the worker's MAX_AIDS)
    batch_size: int
    # 0-1 share of aids routed through the batch path; the rest (and every
    # aid after a kill-switch trip) take the single-aid path
    batch_fraction: float
    # pool-wide cap on SIMULTANEOUS batch invocations. Without it, every one
    # of the pool's fetch threads (300 in 51_) could hold a batch in flight at
    # batch_fraction=1, putting batch_size x 300 requests on the upstream at
    # once. The cap bounds upstream in-flight from the batch path at
    # batch_size x max_concurrent_batches, INDEPENDENT of the fetch thread
    # count; the single-aid path (including kill-switch fallback) is
    # deliberately NOT throttled by it. Default 30 keeps batch-path upstream
    # in-flight at or below today's 300 for batch_size <= 10 -- an initial
    # safe assumption for the calibration experiments, NOT a chosen W.
    max_concurrent_batches: int = 30
    # total tries per aid before giving up, aligned with the single path's
    # Service retry=3 semantics
    max_attempts: int = 3


class BatchEntry(NamedTuple):
    """One aid waiting in (or dispatched from) a job's batch buffer."""
    aid: int
    attempts_spent: int  # failed tries so far; max_attempts total tries per aid


class BatchDispatchResult(NamedTuple):
    """Outcome of dispatching one batch, returned to the job's batch loop."""
    # failed entries to put back in the buffer (attempts_spent already
    # incremented); they mix with new aids into later batches
    retries: list
    # True -> the job must exit NOW (record writer stalled or dead), the same
    # contract as the single-aid path's drop-and-exit
    should_stop: bool


class VideoViewTrimmedBatchController:
    """
    Pool-shared runtime state of the batch path. ONE instance per run is
    shared by every FetchVideoRecordJob in the fetch pool, and owns the two
    cross-job concerns:

    1. The CONCURRENCY GATE (a semaphore of max_concurrent_batches slots):
       jobs must hold a slot around the batch HTTP call, bounding simultaneous
       batch invocations pool-wide. Only batch invocations are gated -- the
       single-aid path never touches the controller.

    2. The CIRCUIT BREAKER (kill-switch). Precise definition:

       - Outcomes are recorded in COMPLETION order -- the order in which jobs
         report them after their batch call returns -- which under concurrency
         is not the dispatch order.
       - record_whole_batch_failure() counts one whole-batch failure;
         record_batch_success() (called only for a response that passed FULL
         envelope + per-item identity validation) resets that count to zero.
       - The breaker trips when CONSECUTIVE_WHOLE_BATCH_FAILURE_LIMIT failures
         are recorded with no validated success recorded in between. In-flight
         batches do not delay or prevent the trip; conversely, their successes
         (once recorded) reset the count, so a healthy worker with many
         batches in flight is not tripped by a sprinkle of transient failures
         unless three of them complete back to back with no success landing
         between them -- and a spurious trip only costs falling back to the
         always-correct single-aid path for the rest of the run.
       - trip(reason) fires the breaker immediately regardless of the count
         (misalignment: the path's DATA cannot be trusted).
       - Tripping is one-way for the run. Jobs must re-check is_enabled()
         when a batch response comes back, BEFORE interpreting it: a response
         already in flight when the breaker tripped must be discarded and its
         aids refetched over the single-aid path.
    """

    CONSECUTIVE_WHOLE_BATCH_FAILURE_LIMIT = 3

    def __init__(self, config: VideoViewTrimmedBatchConfig):
        self.config = config
        self._lock = Lock()
        self._enabled = True
        self._consecutive_whole_batch_failures = 0
        self.trip_reason: Optional[str] = None
        self._slots = BoundedSemaphore(config.max_concurrent_batches)

    # --- circuit breaker ---------------------------------------------------

    def is_enabled(self) -> bool:
        with self._lock:
            return self._enabled

    def record_batch_success(self):
        """A batch response passed full validation: reset the failure streak."""
        with self._lock:
            self._consecutive_whole_batch_failures = 0

    def record_whole_batch_failure(self) -> bool:
        """Count one whole-batch failure (in completion order). True iff THIS
        call tripped the breaker (so exactly one job logs the critical line)."""
        with self._lock:
            self._consecutive_whole_batch_failures += 1
            if (self._enabled and self._consecutive_whole_batch_failures
                    >= self.CONSECUTIVE_WHOLE_BATCH_FAILURE_LIMIT):
                self._enabled = False
                self.trip_reason = 'consecutive whole-batch failures'
                return True
            return False

    def trip(self, reason: str) -> bool:
        """Fire the breaker outright (misalignment). True iff THIS call
        flipped it (so exactly one job logs the critical line)."""
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
