from .Job import Job
from service import Service, CodeError
from timer import Timer
from queue import Queue, Empty, Full
from db import Session
from core import RecordNew
from util import format_ts_ms, get_ts_s, ts_s_to_str
from task import fetch_video_record_via_video_view, update_video
from typing import Optional

__all__ = ['FetchVideoRecordJob']


class FetchVideoRecordJob(Job):
    """
    Fetch-only worker: HTTP fetch -> RecordNew -> record_queue. Persisting is
    left to a BatchInsertVideoRecordJob draining the queue, so fetch workers
    hold no DB connection and per-record commit contention disappears. This is
    the bulk counterpart of AddVideoRecordJob (which stays the right choice for
    small aid batches).

    Fetchers hold NO pooled DB connection: the rare CodeError -> update_video
    fallback takes a short-lived session and returns it immediately. Holding one
    per worker deadlocked the 2026-07-15 04:00 full scan -- see
    _update_video_with_own_session.
    """

    # give up on a record after this long stuck on a full queue, so a dead
    # writer can never hang the pool indefinitely (belt-and-braces: the
    # duration limit normally ends the wait first)
    MAX_PUT_WAIT_S = 300.0

    def __init__(self, name: str, aid_queue: Queue[int], record_queue: 'Queue[Optional[RecordNew]]',
                 service: Service,
                 update_if_code_error: bool = True, duration_limit_s: Optional[int] = None,
                 put_timeout_s: float = 30.0):
        super().__init__(name)
        self.aid_queue = aid_queue
        self.record_queue = record_queue
        self.service = service
        self.update_if_code_error = update_if_code_error
        self.duration_limit_s = duration_limit_s
        self.duration_limit_due_ts_s = None
        self.put_timeout_s = put_timeout_s

    def _update_video_with_own_session(self, aid: int):
        """
        Run the CodeError -> update_video fallback on a SHORT-LIVED session.

        This must NOT hold a pooled connection for the worker's lifetime. It
        used to: a lazy self.session was opened on the first CodeError and kept
        until cleanup(). With 250 workers against a 200-connection pool
        (pool_size=50 + max_overflow=150), a full scan spreads enough CodeErrors
        across workers that every worker eventually pins a connection -- the
        pool is then exhausted, the batch writer cannot get a connection to
        drain record_queue, record_queue fills to its cap, and every fetcher
        blocks forever in put() while still holding the connections the writer
        needs. That is a deadlock, and it hung the 2026-07-15 04:00 full scan.

        A fetcher needs the DB for well under 1% of aids, so it takes a
        connection only for that call and gives it straight back.
        """
        session = Session()
        try:
            return update_video(aid, self.service, session)
        except Exception:
            try:
                session.rollback()
            except Exception:
                pass
            raise
        finally:
            session.close()  # back to the pool immediately

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
                self.logger.info(f'Duration limit reached. Now exit.')
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
                if self.update_if_code_error:
                    self.logger.info(f'Code error occurred. Now start update video. aid: {aid}')
                    try:
                        # short-lived session: rollback + close are handled
                        # inside, so no pooled connection outlives this call
                        tdd_video_logs = self._update_video_with_own_session(aid)
                    except Exception as e2:
                        self.logger.error(f'Fail to update video info. aid: {aid}, error: {e2}')
                        self.stat.condition['update_exception'] += 1
                    else:
                        for log in tdd_video_logs:
                            self.logger.info(
                                f'Update video info. aid: {aid}, attr: {log.attr}, {log.oldval} -> {log.newval}')
                        self.logger.debug(f'{len(tdd_video_logs)} log(s) found. aid: {aid}')
                        self.stat.condition[f'{len(tdd_video_logs)}_update'] += 1
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

