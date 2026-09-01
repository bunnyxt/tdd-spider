from .Job import Job
from .VideoViewTrimmedBatchController import \
    VideoViewTrimmedBatchController, BatchEntry, BatchDispatchResult
from service import Service, CodeError, MisalignmentError
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
      path. Per-item outcomes route exactly like the single path; only failed
      ITEMS are retried (max_attempts per aid, mixed into later batches). The
      pool-shared controller bounds simultaneous batch invocations and owns
      the circuit breaker (see its docstring for the precise breaker
      definition); once the breaker fires, every remaining aid -- including
      results still in flight, which are discarded on return -- takes the
      untouched single-aid path.
    """

    # give up on a record after this long stuck on a full queue, so a dead
    # writer can never hang the pool indefinitely (belt-and-braces: the
    # duration limit normally ends the wait first)
    MAX_PUT_WAIT_S = 300.0

    # wait for a batch-invocation slot in slices this long, re-checking the
    # run deadline and the circuit breaker between waits
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
        self.put_timeout_s = put_timeout_s
        # ONE controller instance is shared by the whole pool: it carries the
        # batch knobs plus the two cross-job concerns (concurrency gate,
        # circuit breaker). None -> single-aid path only.
        self.batch_controller = batch_controller

    # --- pieces shared by both loops ----------------------------------------

    def _deadline_reached(self) -> bool:
        return (self.duration_limit_due_ts_s is not None
                and get_ts_s() >= self.duration_limit_due_ts_s)

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
        dispatch batches. Failure policy, retry accounting and the breaker all
        live in _dispatch_batch / the controller; this loop only moves aids."""
        config = self.batch_controller.config
        # entries waiting for dispatch; retried items re-enter here and mix
        # with new aids
        buffer: list[BatchEntry] = []

        while True:
            if self._deadline_reached():
                buffered_note = (f' ({len(buffer)} aid(s) buffered for batch dropped)'
                                 if buffer else '')
                self.logger.info(f'Duration limit reached. Now exit. '
                                 f'{self.aid_queue.qsize()} aid(s) left unfetched.{buffered_note}')
                self.stat.condition['duration_limit_reached'] += 1
                break

            try:
                aid = self.aid_queue.get_nowait()
            except Empty:
                if buffer:
                    # flush the partial batch (and keep looping: its retries
                    # re-enter the buffer until resolved or exhausted). With no
                    # new aids left a retry-only buffer would redispatch back to
                    # back; colddown first, mirroring the single path's
                    # between-trial sleep (Service._get)
                    if all(entry.attempts_spent > 0 for entry in buffer):
                        time.sleep(random.random() * 0.5 + 0.75)
                    result = self._dispatch_batch(buffer)
                    if result.should_stop:
                        return
                    buffer = result.retries
                    continue
                break

            if (self.batch_controller.is_enabled()
                    and random.random() < config.batch_fraction):
                if any(entry.aid == aid for entry in buffer):
                    # duplicate of an aid already buffered: the worker contract
                    # leaves de-duplication to the caller, so never send the
                    # same token twice in one batch -- this occurrence takes
                    # the single path instead
                    self.stat.condition['batch_duplicate_single_path'] += 1
                    if not self._fetch_single(aid):
                        return
                else:
                    buffer.append(BatchEntry(aid=aid, attempts_spent=0))
                    if len(buffer) >= config.batch_size:
                        result = self._dispatch_batch(buffer)
                        if result.should_stop:
                            return
                        buffer = result.retries
            else:
                if not self._fetch_single(aid):
                    return

    # --- batch dispatch -------------------------------------------------------

    def _dispatch_batch(self, entries: list) -> BatchDispatchResult:
        """Run one assembled batch through gate -> HTTP -> interpretation."""
        # breaker check before spending a slot; a batch assembled just before
        # the trip goes straight to the single path
        if not self.batch_controller.is_enabled():
            return self._fallback_result(entries)

        # concurrency gate: bounds simultaneous batch invocations pool-wide
        if not self.batch_controller.try_acquire_slot(0):
            # gate is biting: count it once per dispatch (observability for
            # calibration), then wait in slices so a held-up slot can never
            # blind this job to the run deadline or to the breaker firing
            self.stat.condition['batch_concurrency_throttled'] += 1
            while not self.batch_controller.try_acquire_slot(self.SLOT_WAIT_SLICE_S):
                if self._deadline_reached():
                    # hand the entries back untouched; the loop-top deadline
                    # check is the single place that logs and counts the drop
                    return BatchDispatchResult(retries=entries, should_stop=False)
                if not self.batch_controller.is_enabled():
                    return self._fallback_result(entries)

        aids = [entry.aid for entry in entries]
        self.stat.condition['batch_request'] += 1
        batch_start = time.perf_counter()
        try:
            try:
                items = self.service.get_video_view_trimmed_batch(aids)
            finally:
                # release before any fallback/retry work -- holding a slot
                # through single-path refetches would starve the batch path
                self.batch_controller.release_slot()
        except MisalignmentError as e:
            return self._on_misalignment(e, aids, entries)
        except Exception as e:
            # broad on purpose, like the single path's except Exception: a
            # bug-shaped error must degrade like a transport failure, never
            # kill a worker thread mid-run
            batch_ms = int((time.perf_counter() - batch_start) * 1000)
            return self._on_whole_batch_failure(e, aids, entries, batch_ms)
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
            return self._fallback_result(entries)

        self.batch_controller.record_batch_success()
        self.logger.debug(f'BATCH TIMING aids={aids} http={batch_ms}ms')

        # per-item routing, mirroring the single path outcome for outcome
        retries = []
        for entry, item in zip(entries, items):
            if item.view is not None:
                record = build_video_record_via_video_view(entry.aid, item.view)
                if not self._put_record_with_backpressure(entry.aid, record):
                    return BatchDispatchResult(retries=[], should_stop=True)
            elif isinstance(item.error, CodeError):
                if self.code_error_aid_queue is not None:
                    self.code_error_aid_queue.put(entry.aid)
                    self.logger.info(f'Code error, queued for video update. '
                                     f'aid: {entry.aid}, error: {item.error}')
                else:
                    self.logger.error(
                        f'Fail to fetch video record. aid: {entry.aid}, error: {item.error}')
                self.stat.condition['code_error'] += 1
            else:
                # transient per-item failure: retry THIS item only
                attempts_spent = entry.attempts_spent + 1
                if attempts_spent >= self.batch_controller.config.max_attempts:
                    self.logger.error(f'Fail to fetch video record (batch attempts '
                                      f'exhausted). aid: {entry.aid}, error: {item.error}')
                    self.stat.condition['other_exception'] += 1
                else:
                    self.stat.condition['batch_item_retry'] += 1
                    retries.append(BatchEntry(entry.aid, attempts_spent))
                    continue  # not final: no per-aid bookkeeping yet
            self._finalize_batch_aid(batch_ms)
        return BatchDispatchResult(retries=retries, should_stop=False)

    def _on_misalignment(self, error, aids, entries) -> BatchDispatchResult:
        """The batch path returned data that cannot be trusted (wrong endpoint,
        protocol break, misrouted payloads). Not a per-item problem: discard
        the WHOLE batch result, fire the breaker for the rest of the run,
        refetch these aids over the single path."""
        self.stat.condition['batch_misalignment'] += 1
        if self.batch_controller.trip(f'misalignment: {error}'):
            self.logger.critical(
                f'Batch path misalignment -- batch path disabled for the rest of '
                f'this run, remaining aids take the single-aid path. error: {error}')
        else:
            self.logger.error(f'Batch path misalignment (path already disabled). '
                              f'aids: {aids}, error: {error}')
        return self._fallback_result(entries)

    def _on_whole_batch_failure(self, error, aids, entries, batch_ms) -> BatchDispatchResult:
        """Nothing per-item arrived (transport failure, worker error envelope,
        endpoint unconfigured, or an unexpected bug): every aid in the batch
        pays one attempt; the breaker counts one completion-order failure."""
        self.stat.condition['batch_whole_failure'] += 1
        self.logger.error(f'Whole-batch failure. aids: {aids}, error: {error}')
        if self.batch_controller.record_whole_batch_failure():
            self.logger.critical(
                f'{VideoViewTrimmedBatchController.CONSECUTIVE_WHOLE_BATCH_FAILURE_LIMIT} '
                f'consecutive whole-batch failures -- batch path disabled for the '
                f'rest of this run, remaining aids take the single-aid path.')
            return self._fallback_result(entries)
        if not self.batch_controller.is_enabled():
            # another job fired the breaker while this batch was in flight;
            # retrying over the dead path would only burn attempts
            return self._fallback_result(entries)
        # colddown before the retry wave, mirroring the single path's
        # between-trial sleep (Service._get)
        time.sleep(random.random() * 0.5 + 0.75)
        retries = []
        for entry in entries:
            attempts_spent = entry.attempts_spent + 1
            if attempts_spent >= self.batch_controller.config.max_attempts:
                self.logger.error(f'Fail to fetch video record (batch attempts '
                                  f'exhausted). aid: {entry.aid}, error: {error}')
                self.stat.condition['other_exception'] += 1
                self._finalize_batch_aid(batch_ms)
            else:
                retries.append(BatchEntry(entry.aid, attempts_spent))
        return BatchDispatchResult(retries=retries, should_stop=False)

    def _fallback_result(self, entries: list) -> BatchDispatchResult:
        """Send a discarded batch down the single path, as a dispatch result."""
        return BatchDispatchResult(
            retries=[], should_stop=not self._fallback_batch_to_single(entries))

    def _fallback_batch_to_single(self, entries: list) -> bool:
        """Run every entry of a discarded batch through the single-aid path
        (which brings its own internal retries). False -> job must exit."""
        for entry in entries:
            self.stat.condition['batch_fallback_single'] += 1
            if not self._fetch_single(entry.aid):
                return False
        return True

    def _finalize_batch_aid(self, batch_ms: int):
        """Per-aid closing bookkeeping for an aid whose FINAL outcome came from
        a batch (success / code_error / attempts exhausted -- not a retry).
        batch_ms is the whole batch round-trip: it is the wall-clock latency
        this aid experienced, which keeps http_ms / STAGE AVG comparable with
        the single path's per-aid timing."""
        self.stat.condition['http_ms'] += batch_ms
        self.stat.total_count += 1
        self.stat.total_duration_ms += batch_ms
