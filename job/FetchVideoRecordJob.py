from .Job import Job
from .VideoViewTrimmedBatchController import VideoViewTrimmedBatchController
from service import Service, CodeError, MisalignmentError, RateLimitError
from timer import Timer
from queue import Queue, Empty, Full
from core import RecordNew
from util import format_ts_ms, get_ts_s, ts_s_to_str
from task import fetch_video_record_via_video_view, build_video_record_via_video_view
from typing import Optional
import random
import time

__all__ = ['FetchVideoRecordJob']


class FetchVideoRecordJob(Job):
    """
    Fetch-only worker: HTTP fetch -> RecordNew -> record_queue. Persisting is
    left to a BatchInsertVideoRecordJob draining the queue, so fetch workers
    hold no DB connection and per-record commit contention disappears. This is
    the bulk counterpart of AddVideoRecordJob (which stays the right choice for
    small aid batches).

    Fetchers touch NO DB, at all -- they do not even import Session. An aid whose
    view call returns a CodeError (deleted / hidden / -403) is pushed to
    code_error_aid_queue for a bounded UpdateVideoJob pool to refresh, instead of
    the fetcher running update_video itself. Doing it inline meant any of the 250
    fetchers could grab a pooled connection (and, before that, pin one for its
    whole life) -- unbounded DB concurrency from the fetch tier, which is what
    deadlocked the 2026-07-15 04:00 full scan. It also cost a fetch slot and
    pulled a FULL (untrimmed, up to 2.8MB) view payload per code error.

    Two mutually exclusive process loops, chosen once at start:

    - batch_controller is None -> _run_single_path_loop(): the pre-batch
      behavior, verbatim. This is the production default.
    - batch_controller set -> _run_batch_path_loop(): a batch_fraction share
      of aids is buffered into batches of batch_size and fetched through
      Service.get_video_view_trimmed_batch; everything else takes the single
      path. Reliability-first policy -- the batch path is an optimisation
      layered on the single path, never a second retry machine:
        * item ok          -> record, exactly as the single path would
        * item CodeError   -> code_error_aid_queue, exactly as the single path
        * item transient   -> THAT aid is refetched over the single path
                              right away (Service's own retry=3 handles it)
        * whole-batch fail -> every aid of the batch is refetched over the
                              single path, and the breaker is tripped: the
                              path is unavailable, stop using it this run
        * misalignment     -> same fallback, breaker tripped: the path's DATA
                              cannot be trusted
      The pool-shared controller bounds simultaneous batch invocations and
      holds the breaker; once tripped, every remaining aid -- including
      responses still in flight, which are discarded on return -- takes the
      untouched single-aid path. No aid is ever dropped because of the batch
      path; the worst case is a few extra single-aid calls.
    """

    # give up on a record after this long stuck on a full queue, so a dead
    # writer can never hang the pool indefinitely (belt-and-braces: the
    # duration limit normally ends the wait first)
    MAX_PUT_WAIT_S = 300.0

    # wait for a batch-invocation slot in slices this long, re-checking the
    # run deadline and the breaker between waits
    SLOT_WAIT_SLICE_S = 1.0

    def __init__(self, name: str, aid_queue: Queue[int], record_queue: 'Queue[Optional[RecordNew]]',
                 service: Service,
                 code_error_aid_queue: 'Optional[Queue[Optional[int]]]' = None,
                 duration_limit_s: Optional[int] = None,
                 put_timeout_s: float = 30.0,
                 batch_controller: Optional[VideoViewTrimmedBatchController] = None):
        super().__init__(name)
        self.aid_queue = aid_queue
        self.record_queue = record_queue
        self.service = service
        # where CodeError aids go for a bounded UpdateVideoJob pool to refresh.
        # None -> code errors are only counted (no metadata refresh).
        self.code_error_aid_queue = code_error_aid_queue
        self.duration_limit_s = duration_limit_s
        self.duration_limit_due_ts_s = None
        # set once the job has counted its duration-limit hit, so the
        # several places that can observe the deadline never double-count
        self._duration_limit_counted = False
        self.put_timeout_s = put_timeout_s
        # ONE controller instance is shared by the whole pool: it carries the
        # batch knobs plus the two cross-job concerns (concurrency gate,
        # circuit breaker). None -> single-aid path only.
        self.batch_controller = batch_controller

    # --- pieces shared by both loops ----------------------------------------

    def _deadline_reached(self) -> bool:
        return (self.duration_limit_due_ts_s is not None
                and get_ts_s() >= self.duration_limit_due_ts_s)

    def _count_duration_limit_reached(self):
        """Mark this job as having hit its duration limit -- exactly once,
        whichever code path saw the deadline first. The pool summary's
        duration_limit_reached is THE number that says whether the 04:00 full
        scan finished inside its window, so every deadline exit must land in
        it, and never twice for one job."""
        if not self._duration_limit_counted:
            self._duration_limit_counted = True
            self.stat.condition['duration_limit_reached'] += 1

    def _put_record(self, record) -> bool:
        """Bounded put with a timeout. False -> queue still full, caller decides."""
        try:
            self.record_queue.put(record, timeout=self.put_timeout_s)
            return True
        except Full:
            return False

    def _put_record_with_backpressure(self, aid: int, record: RecordNew) -> bool:
        """
        Put one record, waiting out writer backpressure but never forever.
        False -> the writer is stalled or dead (or the deadline hit while
        waiting) and the job must exit, same as the single path always did.
        """
        # record_queue is BOUNDED, so put() can block when the writer
        # falls behind -- that backpressure is intentional. What is not
        # acceptable is blocking FOREVER: if every writer dies (or
        # cannot get a DB connection), an unbounded wait here hangs the
        # whole pool past the hour, and the duration-limit check at the
        # top of the loop is never reached again. Time out, re-check the
        # deadline, and give up on the record rather than the run.
        put_wait_s = 0.0
        while not self._put_record(record):
            put_wait_s += self.put_timeout_s
            if self._deadline_reached() or put_wait_s >= self.MAX_PUT_WAIT_S:
                self.logger.error(
                    f'Record queue still full after {put_wait_s:.0f}s -- writer stalled '
                    f'or dead. Dropping record and exiting. aid: {aid}')
                self.stat.condition['record_dropped_queue_full'] += 1
                return False
            self.logger.warning(
                f'Record queue full for {put_wait_s:.0f}s -- writer is stalled or dead. '
                f'Still waiting. aid: {aid}')
        self.stat.condition['success'] += 1
        return True

    def _fetch_single(self, aid: int) -> bool:
        """One aid through the single-aid path (fetch, route, count). This IS
        the pre-batch loop body, verbatim. False -> the job must exit."""
        self.logger.debug(f'Now start fetch video record. aid: {aid}')
        timer = Timer()
        timer.start()

        stage_stat = {}  # per-stage durations, filled by the task (http_ms)
        try:
            record = fetch_video_record_via_video_view(
                aid, self.service, out_stat=stage_stat)
        except RateLimitError as e:
            self.logger.warning(
                f'Video API rate limited; skipping current aid. '
                f'aid: {aid}, target: {e.target}, reason: {e.reason}')
            self.stat.condition['rate_limited'] += 1
        except CodeError as e:
            # hand off to the UpdateVideoJob pool: refreshing tdd_video.code
            # is what drops this aid out of future need_insert lists, but it
            # is DB work and must not happen on a fetch worker
            if self.code_error_aid_queue is not None:
                self.code_error_aid_queue.put(aid)
                self.logger.info(
                    f'Code error, queued for video update. aid: {aid}, error: {e}')
            else:
                self.logger.error(f'Fail to fetch video record. aid: {aid}, error: {e}')
            self.stat.condition['code_error'] += 1
        except Exception as e:
            self.logger.error(f'Fail to fetch video record. aid: {aid}, error: {e}')
            self.stat.condition['other_exception'] += 1
        else:
            if not self._put_record_with_backpressure(aid, record):
                return False

        timer.stop()
        # accumulate per-stage durations into the pool stats (JobPool's
        # heartbeat turns *_ms keys into live per-aid stage averages) and
        # emit a greppable per-aid line for offline analysis (--debug file)
        for stage_key, stage_ms in stage_stat.items():
            self.stat.condition[stage_key] += stage_ms
        self.logger.debug(
            f'TIMING aid={aid} '
            + ' '.join(f'{k[:-3]}={v}ms' for k, v in stage_stat.items())
            + f' total={timer.get_duration_ms()}ms')
        self.stat.total_count += 1
        self.stat.total_duration_ms += timer.get_duration_ms()
        return True

    # --- the two process loops ----------------------------------------------

    def process(self):
        if self.duration_limit_s is not None:
            self.duration_limit_due_ts_s = get_ts_s() + self.duration_limit_s
            self.logger.info(f'Duration limit due at {ts_s_to_str(self.duration_limit_due_ts_s)}.')

        if self.batch_controller is None:
            self._run_single_path_loop()
        else:
            self._run_batch_path_loop()

    def _run_single_path_loop(self):
        """The pre-batch process loop, unchanged -- what production runs while
        the batch path is off. Kept free of any batch bookkeeping so it stays
        trivially diffable against the original implementation."""
        while True:
            if self._deadline_reached():
                self.logger.info(f'Duration limit reached. Now exit. '
                                 f'{self.aid_queue.qsize()} aid(s) left unfetched.')
                # surface the cut in the pool summary, not just in a log line:
                # on the 04:00 full scan this is THE number that says whether we
                # finished inside the 40-minute window
                self.stat.condition['duration_limit_reached'] += 1
                break

            # get_nowait instead of empty()+get(): the latter races when several
            # workers see the same last item -- the losers block in get() forever
            # and the pool join never returns
            try:
                aid = self.aid_queue.get_nowait()
            except Empty:
                break

            if not self._fetch_single(aid):
                return

    def _run_batch_path_loop(self):
        """Pull aids, route each to the batch buffer or the single path, and
        dispatch full batches (plus the partial one left when the queue
        drains). Nothing comes back from a dispatch: every aid handed to
        _dispatch_batch is resolved there, one way or another."""
        config = self.batch_controller.config
        buffer: list[int] = []  # aids waiting for dispatch

        while True:
            if self._deadline_reached():
                buffered_note = (f' ({len(buffer)} aid(s) buffered for batch dropped)'
                                 if buffer else '')
                self.logger.info(f'Duration limit reached. Now exit. '
                                 f'{self.aid_queue.qsize()} aid(s) left unfetched.{buffered_note}')
                self._count_duration_limit_reached()
                break

            try:
                aid = self.aid_queue.get_nowait()
            except Empty:
                if buffer and not self._dispatch_batch(buffer):
                    return
                break

            # observe the breaker ONCE per aid. If it fired while this buffer
            # was assembling, resolve the buffer over the single path right
            # now -- before touching the aid just dequeued -- instead of
            # holding it until the queue drains (where a deadline could drop
            # it). No job keeps a batch buffer past the moment it sees the
            # breaker open.
            batch_enabled = self.batch_controller.is_enabled()
            if buffer and not batch_enabled:
                if not self._fallback_to_single(buffer):
                    return
                buffer = []

            if batch_enabled and random.random() < config.batch_fraction:
                if aid in buffer:
                    # duplicate of an aid already buffered: the worker contract
                    # leaves de-duplication to the caller, so never send the
                    # same token twice in one batch -- this occurrence takes
                    # the single path instead
                    self.stat.condition['batch_duplicate_single_path'] += 1
                    if not self._fetch_single(aid):
                        return
                else:
                    buffer.append(aid)
                    if len(buffer) >= config.batch_size:
                        if not self._dispatch_batch(buffer):
                            return
                        buffer = []
            else:
                if not self._fetch_single(aid):
                    return

    # --- batch dispatch -------------------------------------------------------

    def _dispatch_batch(self, aids: list) -> bool:
        """
        Resolve one assembled batch: gate -> HTTP -> per-item routing, with
        every failure mode ending in the single-aid path (see the class
        docstring). Returns False only when the job must exit (record writer
        stalled or dead), the same contract as _fetch_single.
        """
        # breaker check before spending a slot; a batch assembled just before
        # the trip goes straight to the single path
        if not self.batch_controller.is_enabled():
            return self._fallback_to_single(aids)

        # concurrency gate: bounds simultaneous batch invocations pool-wide
        if not self.batch_controller.try_acquire_slot(0):
            # gate is biting: count it once per dispatch (observability for
            # calibration), then wait in slices so a held-up slot can never
            # blind this job to the run deadline or to the breaker firing
            self.stat.condition['batch_concurrency_throttled'] += 1
            while not self.batch_controller.try_acquire_slot(self.SLOT_WAIT_SLICE_S):
                if self._deadline_reached():
                    return self._drop_at_deadline(aids)
                if not self.batch_controller.is_enabled():
                    return self._fallback_to_single(aids)

        # SLOT HELD from here to the finally below -- exactly one release on
        # every path. The breaker may have fired, or the deadline passed,
        # between the last check and the successful acquire (typically while
        # blocked in the wait above), so re-check BEFORE sending; when
        # abandoning, the slot is given back first and the fallback/drop work
        # runs unthrottled afterwards.
        abandon = None
        batch_start = time.perf_counter()
        try:
            try:
                if not self.batch_controller.is_enabled():
                    abandon = 'tripped'
                elif self._deadline_reached():
                    abandon = 'deadline'
                else:
                    self.stat.condition['batch_request'] += 1
                    items = self.service.get_video_view_trimmed_batch(aids)
            finally:
                # release before any fallback work -- holding a slot through
                # single-path refetches would starve the batch path
                self.batch_controller.release_slot()
        except MisalignmentError as e:
            # the path's data cannot be trusted (wrong endpoint, protocol
            # break, misrouted payloads): discard the whole result, stop using
            # the path this run, refetch these aids over the single path
            self.stat.condition['batch_misalignment'] += 1
            if self.batch_controller.trip(f'misalignment: {e}'):
                self.logger.critical(
                    f'Batch path misalignment -- batch path disabled for the rest of '
                    f'this run, remaining aids take the single-aid path. error: {e}')
            else:
                self.logger.error(f'Batch path misalignment (path already disabled). '
                                  f'aids: {aids}, error: {e}')
            return self._fallback_to_single(aids)
        except Exception as e:
            # nothing per-item arrived (transport failure, worker error
            # envelope, endpoint unconfigured, or an unexpected bug -- broad
            # on purpose, like the single path's except Exception): the path
            # is unavailable, stop using it this run rather than retrying it,
            # and refetch these aids over the single path
            self.stat.condition['batch_whole_failure'] += 1
            if self.batch_controller.trip(f'whole-batch failure: {e}'):
                self.logger.critical(
                    f'Whole-batch failure -- batch path disabled for the rest of this '
                    f'run, remaining aids take the single-aid path. aids: {aids}, error: {e}')
            else:
                self.logger.error(f'Whole-batch failure (path already disabled). '
                                  f'aids: {aids}, error: {e}')
            return self._fallback_to_single(aids)
        if abandon == 'tripped':
            return self._fallback_to_single(aids)
        if abandon == 'deadline':
            return self._drop_at_deadline(aids)
        batch_ms = int((time.perf_counter() - batch_start) * 1000)

        # RE-CHECK the breaker before interpreting anything: this response may
        # have been in flight when another job tripped it (misalignment means
        # the path's data cannot be trusted, this response included). Discard
        # it wholesale and refetch over the single path. A trip landing after
        # this check, while items are being written, is a residual window we
        # accept: every item below has already passed its own full identity
        # validation, so the recheck is defense in depth, not the last line.
        if not self.batch_controller.is_enabled():
            self.stat.condition['batch_discarded_after_trip'] += 1
            self.logger.warning(
                f'Discarding an in-flight batch response returned after the batch path '
                f'was disabled; refetching over the single-aid path. aids: {aids}')
            return self._fallback_to_single(aids)

        self.logger.debug(f'BATCH TIMING aids={aids} http={batch_ms}ms')

        # per-item routing, mirroring the single path outcome for outcome
        for aid, item in zip(aids, items):
            if item.view is not None:
                record = build_video_record_via_video_view(aid, item.view)
                if not self._put_record_with_backpressure(aid, record):
                    return False
            elif isinstance(item.error, CodeError):
                if self.code_error_aid_queue is not None:
                    self.code_error_aid_queue.put(aid)
                    self.logger.info(f'Code error, queued for video update. '
                                     f'aid: {aid}, error: {item.error}')
                else:
                    self.logger.error(
                        f'Fail to fetch video record. aid: {aid}, error: {item.error}')
                self.stat.condition['code_error'] += 1
            else:
                # transient per-item failure (item timeout, fetch error,
                # non-JSON or non-200 upstream): THIS aid goes over the
                # single path now, whose own retry=3 is the retry policy.
                # _fetch_single does its own per-aid bookkeeping.
                self.logger.debug(f'Batch item failed, refetching over the single-aid '
                                  f'path. aid: {aid}, error: {item.error}')
                self.stat.condition['batch_item_fallback_single'] += 1
                if not self._fetch_single(aid):
                    return False
                continue
            # per-aid closing bookkeeping for an aid resolved by the batch.
            # batch_ms is the whole batch round-trip: it is the wall-clock
            # latency this aid experienced, which keeps http_ms / STAGE AVG
            # comparable with the single path's per-aid timing
            self.stat.condition['http_ms'] += batch_ms
            self.stat.total_count += 1
            self.stat.total_duration_ms += batch_ms
        return True

    def _drop_at_deadline(self, aids: list) -> bool:
        """The run deadline passed while this batch was still waiting to be
        sent: same treatment as aids left in the queue at the deadline, but
        counted so it is visible. Also records the duration-limit hit itself:
        when this is the final flush after the queue drained, the caller
        breaks straight out and the loop-top check never runs again."""
        self.logger.info(f'Duration limit reached before the batch could be sent. '
                         f'{len(aids)} buffered aid(s) dropped.')
        self.stat.condition['batch_dropped_at_deadline'] += len(aids)
        self._count_duration_limit_reached()
        return True

    def _fallback_to_single(self, aids: list) -> bool:
        """Run every aid of a discarded batch through the single-aid path
        (which brings its own internal retries). False -> job must exit."""
        for aid in aids:
            self.stat.condition['batch_fallback_single'] += 1
            if not self._fetch_single(aid):
                return False
        return True
