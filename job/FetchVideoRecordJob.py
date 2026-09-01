from .Job import Job
from service import Service, CodeError, MisalignmentError
from timer import Timer
from queue import Queue, Empty, Full
from core import RecordNew
from threading import Lock
from util import format_ts_ms, get_ts_s, ts_s_to_str
from task import fetch_video_record_via_video_view, build_video_record_via_video_view
from typing import NamedTuple, Optional
import random
import time

__all__ = ['FetchVideoRecordJob',
           'VideoViewTrimmedBatchConfig', 'VideoViewTrimmedBatchState']


class VideoViewTrimmedBatchConfig(NamedTuple):
    """
    Knobs of the trimmed video_view BATCH path (default off -- a job built
    without a config runs the single-aid path untouched). batch_size /
    batch_fraction values are the canary staircase's dials and are INITIAL
    HYPOTHESES pending load-test calibration, like the batch timeouts in
    service/Service.py.
    """
    # aids per batch request (conf caps this at the worker's MAX_AIDS)
    batch_size: int
    # 0-1 share of aids routed through the batch path; the rest (and every
    # aid after a kill-switch trip) take the single-aid path
    batch_fraction: float
    # total tries per aid before giving up, aligned with the single path's
    # Service retry=3 semantics
    max_attempts: int = 3


class VideoViewTrimmedBatchState:
    """
    Run-scope state of the batch path, SHARED by every FetchVideoRecordJob in
    a pool (thread-safe): the kill-switch plus the consecutive whole-batch
    failure counter behind it. Tripping is one-way for the run -- once
    disabled, every remaining aid takes the single-aid path.

    Trip conditions (per the migration design):
    - any misalignment (trip(), called by the job that saw it), or
    - CONSECUTIVE_WHOLE_BATCH_FAILURE_LIMIT whole-batch failures in a row
      pool-wide with no success in between -- the self-healing path when the
      batch worker is dead, deleted, or unreachable.
    """

    CONSECUTIVE_WHOLE_BATCH_FAILURE_LIMIT = 3

    def __init__(self):
        self._lock = Lock()
        self._enabled = True
        self._consecutive_whole_batch_failures = 0
        self.trip_reason: Optional[str] = None

    def is_enabled(self) -> bool:
        with self._lock:
            return self._enabled

    def record_batch_success(self):
        with self._lock:
            self._consecutive_whole_batch_failures = 0

    def record_whole_batch_failure(self) -> bool:
        """Count one whole-batch failure. True iff THIS call tripped the
        kill-switch (so exactly one job logs the critical line)."""
        with self._lock:
            self._consecutive_whole_batch_failures += 1
            if (self._enabled and self._consecutive_whole_batch_failures
                    >= self.CONSECUTIVE_WHOLE_BATCH_FAILURE_LIMIT):
                self._enabled = False
                self.trip_reason = 'consecutive whole-batch failures'
                return True
            return False

    def trip(self, reason: str) -> bool:
        """Disable the batch path outright (misalignment). True iff THIS call
        flipped the switch (so exactly one job logs the critical line)."""
        with self._lock:
            if not self._enabled:
                return False
            self._enabled = False
            self.trip_reason = reason
            return True


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

    Optional BATCH mode (batch_config + batch_state, both or neither): a
    batch_fraction share of aids is buffered into batches of batch_size and
    fetched through Service.get_video_view_trimmed_batch, one Lambda invocation
    per batch instead of one per aid. Per-item outcomes route exactly like the
    single path (success -> record_queue, CodeError -> code_error_aid_queue);
    failed ITEMS are retried alone (max_attempts per aid, mixed into later
    batches), never the whole batch. A whole-batch transport failure charges
    every aid in it one attempt. The shared batch_state is the kill-switch:
    any misalignment, or 3 consecutive whole-batch failures pool-wide, sends
    every remaining aid down the single-aid path for the rest of the run.
    Without a config the process loop is byte-identical to the pre-batch one.
    """

    # give up on a record after this long stuck on a full queue, so a dead
    # writer can never hang the pool indefinitely (belt-and-braces: the
    # duration limit normally ends the wait first)
    MAX_PUT_WAIT_S = 300.0

    def __init__(self, name: str, aid_queue: Queue[int], record_queue: 'Queue[Optional[RecordNew]]',
                 service: Service,
                 code_error_aid_queue: 'Optional[Queue[Optional[int]]]' = None,
                 duration_limit_s: Optional[int] = None,
                 put_timeout_s: float = 30.0,
                 batch_config: Optional[VideoViewTrimmedBatchConfig] = None,
                 batch_state: Optional[VideoViewTrimmedBatchState] = None):
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
        # batch path: needs BOTH the config (knobs) and the pool-shared state
        # (kill-switch); anything less runs the single-aid path only
        self.batch_config = batch_config
        self.batch_state = batch_state

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
            hit_deadline = (self.duration_limit_due_ts_s is not None
                            and get_ts_s() >= self.duration_limit_due_ts_s)
            if hit_deadline or put_wait_s >= self.MAX_PUT_WAIT_S:
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
        the pre-batch loop body. False -> the job must exit."""
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

    def _fallback_batch_to_single(self, batch: list) -> bool:
        """Run every (aid, attempt) of a discarded batch through the single-aid
        path (which brings its own internal retries). False -> job must exit."""
        for batch_aid, _ in batch:
            self.stat.condition['batch_fallback_single'] += 1
            if not self._fetch_single(batch_aid):
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

    def _process_batch(self, batch: list) -> Optional[list]:
        """
        Dispatch one assembled batch of (aid, attempts_spent) entries. Returns
        the entries to re-buffer for retry (only the failed items, to be mixed
        with new aids), or None if the job must exit (writer stalled/dead,
        same contract as the single path).
        """
        # the kill-switch may have tripped between assembly and dispatch
        if not self.batch_state.is_enabled():
            return None if not self._fallback_batch_to_single(batch) else []

        aids = [batch_aid for batch_aid, _ in batch]
        self.stat.condition['batch_request'] += 1
        batch_start = time.perf_counter()
        try:
            items = self.service.get_video_view_trimmed_batch(aids)
        except MisalignmentError as e:
            # the batch path is returning data that cannot be trusted (wrong
            # endpoint, protocol break, misrouted payloads). Not a per-item
            # problem: discard the WHOLE batch result, kill the path for the
            # rest of the run, refetch these aids over the single path.
            self.stat.condition['batch_misalignment'] += 1
            if self.batch_state.trip(f'misalignment: {e}'):
                self.logger.critical(
                    f'Batch path misalignment -- batch path disabled for the rest of '
                    f'this run, remaining aids take the single-aid path. error: {e}')
            else:
                self.logger.error(f'Batch path misalignment (path already disabled). '
                                  f'aids: {aids}, error: {e}')
            return None if not self._fallback_batch_to_single(batch) else []
        except Exception as e:
            # whole-batch failure: transport (HTTP != 200, unparsable top
            # level, endpoint unconfigured -> ServiceError) or an unexpected
            # bug. Broad on purpose, like the single path's except Exception:
            # nothing per-item arrived, every aid in the batch pays one
            # attempt, and a worker thread must never die mid-run over it
            # (the kill-switch caps how long a broken path can spin).
            batch_ms = int((time.perf_counter() - batch_start) * 1000)
            self.stat.condition['batch_whole_failure'] += 1
            self.logger.error(f'Whole-batch failure. aids: {aids}, error: {e}')
            if self.batch_state.record_whole_batch_failure():
                self.logger.critical(
                    f'{VideoViewTrimmedBatchState.CONSECUTIVE_WHOLE_BATCH_FAILURE_LIMIT} '
                    f'consecutive whole-batch failures -- batch path disabled for the '
                    f'rest of this run, remaining aids take the single-aid path.')
                return None if not self._fallback_batch_to_single(batch) else []
            # colddown before the retry wave, mirroring the single path's
            # between-trial sleep (Service._get)
            time.sleep(random.random() * 0.5 + 0.75)
            retry = []
            for batch_aid, attempts_spent in batch:
                attempts_spent += 1
                if attempts_spent >= self.batch_config.max_attempts:
                    self.logger.error(f'Fail to fetch video record (batch attempts '
                                      f'exhausted). aid: {batch_aid}, error: {e}')
                    self.stat.condition['other_exception'] += 1
                    self._finalize_batch_aid(batch_ms)
                else:
                    retry.append((batch_aid, attempts_spent))
            return retry

        batch_ms = int((time.perf_counter() - batch_start) * 1000)
        self.batch_state.record_batch_success()
        self.logger.debug(f'BATCH TIMING aids={aids} http={batch_ms}ms')

        # per-item routing, mirroring the single path outcome for outcome
        retry = []
        for (batch_aid, attempts_spent), item in zip(batch, items):
            if item.view is not None:
                record = build_video_record_via_video_view(batch_aid, item.view)
                if not self._put_record_with_backpressure(batch_aid, record):
                    return None
            elif isinstance(item.error, CodeError):
                if self.code_error_aid_queue is not None:
                    self.code_error_aid_queue.put(batch_aid)
                    self.logger.info(f'Code error, queued for video update. '
                                     f'aid: {batch_aid}, error: {item.error}')
                else:
                    self.logger.error(
                        f'Fail to fetch video record. aid: {batch_aid}, error: {item.error}')
                self.stat.condition['code_error'] += 1
            else:
                # transient per-item failure: retry THIS item only
                attempts_spent += 1
                if attempts_spent >= self.batch_config.max_attempts:
                    self.logger.error(f'Fail to fetch video record (batch attempts '
                                      f'exhausted). aid: {batch_aid}, error: {item.error}')
                    self.stat.condition['other_exception'] += 1
                else:
                    self.stat.condition['batch_item_retry'] += 1
                    retry.append((batch_aid, attempts_spent))
                    continue  # not final: no per-aid bookkeeping yet
            self._finalize_batch_aid(batch_ms)
        return retry

    def process(self):
        if self.duration_limit_s is not None:
            self.duration_limit_due_ts_s = get_ts_s() + self.duration_limit_s
            self.logger.info(f'Duration limit due at {ts_s_to_str(self.duration_limit_due_ts_s)}.')

        batch_on = self.batch_config is not None and self.batch_state is not None
        # (aid, attempts_spent) waiting for dispatch; retried items re-enter
        # here and mix with new aids
        batch_buffer: list = []

        while True:
            if self.duration_limit_due_ts_s is not None and get_ts_s() >= self.duration_limit_due_ts_s:
                buffered_note = (f' ({len(batch_buffer)} aid(s) buffered for batch dropped)'
                                 if batch_buffer else '')
                self.logger.info(f'Duration limit reached. Now exit. '
                                 f'{self.aid_queue.qsize()} aid(s) left unfetched.{buffered_note}')
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
                if batch_buffer:
                    # flush the partial batch (and keep looping: its retries
                    # re-enter the buffer until resolved or exhausted). With no
                    # new aids left a retry-only buffer would redispatch back to
                    # back; colddown first, mirroring the single path's
                    # between-trial sleep (Service._get)
                    if all(attempts_spent > 0 for _, attempts_spent in batch_buffer):
                        time.sleep(random.random() * 0.5 + 0.75)
                    batch_buffer = self._process_batch(batch_buffer)
                    if batch_buffer is None:
                        return
                    continue
                break

            if (batch_on and self.batch_state.is_enabled()
                    and random.random() < self.batch_config.batch_fraction):
                if any(batch_aid == aid for batch_aid, _ in batch_buffer):
                    # duplicate of an aid already buffered: the worker contract
                    # leaves de-duplication to the caller, so never send the
                    # same token twice in one batch -- this occurrence takes
                    # the single path instead
                    self.stat.condition['batch_duplicate_single_path'] += 1
                    if not self._fetch_single(aid):
                        return
                else:
                    batch_buffer.append((aid, 0))
                    if len(batch_buffer) >= self.batch_config.batch_size:
                        batch_buffer = self._process_batch(batch_buffer)
                        if batch_buffer is None:
                            return
            else:
                if not self._fetch_single(aid):
                    return
