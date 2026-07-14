from .Job import Job
from service import Service, CodeError
from timer import Timer
from queue import Queue, Empty, Full
from core import RecordNew
from util import format_ts_ms, get_ts_s, ts_s_to_str
from task import fetch_video_record_via_video_view
from typing import Optional

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
    """

    # give up on a record after this long stuck on a full queue, so a dead
    # writer can never hang the pool indefinitely (belt-and-braces: the
    # duration limit normally ends the wait first)
    MAX_PUT_WAIT_S = 300.0

    def __init__(self, name: str, aid_queue: Queue[int], record_queue: 'Queue[Optional[RecordNew]]',
                 service: Service,
                 code_error_aid_queue: 'Optional[Queue[Optional[int]]]' = None,
                 duration_limit_s: Optional[int] = None,
                 put_timeout_s: float = 30.0):
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

    def _put_record(self, record) -> bool:
        """Bounded put with a timeout. False -> queue still full, caller decides."""
        try:
            self.record_queue.put(record, timeout=self.put_timeout_s)
            return True
        except Full:
            return False

    def process(self):
        if self.duration_limit_s is not None:
            self.duration_limit_due_ts_s = get_ts_s() + self.duration_limit_s
            self.logger.info(f'Duration limit due at {ts_s_to_str(self.duration_limit_due_ts_s)}.')

        while True:
            if self.duration_limit_due_ts_s is not None and get_ts_s() >= self.duration_limit_due_ts_s:
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
                        return
                    self.logger.warning(
                        f'Record queue full for {put_wait_s:.0f}s -- writer is stalled or dead. '
                        f'Still waiting. aid: {aid}')
                self.stat.condition['success'] += 1

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

