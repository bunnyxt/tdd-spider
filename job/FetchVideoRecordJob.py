from .Job import Job
from service import Service, CodeError
from timer import Timer
from queue import Queue, Empty
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

    A DB session is opened lazily, only if the CodeError -> update_video
    fallback fires (a small fraction of aids).
    """

    def __init__(self, name: str, aid_queue: Queue[int], record_queue: 'Queue[Optional[RecordNew]]',
                 service: Service,
                 update_if_code_error: bool = True, duration_limit_s: Optional[int] = None):
        super().__init__(name)
        self.aid_queue = aid_queue
        self.record_queue = record_queue
        self.service = service
        self.session = None  # lazy, most workers never need one
        self.update_if_code_error = update_if_code_error
        self.duration_limit_s = duration_limit_s
        self.duration_limit_due_ts_s = None

    def _get_session(self):
        if self.session is None:
            self.session = Session()
        return self.session

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
                        tdd_video_logs = update_video(aid, self.service, self._get_session())
                    except Exception as e2:
                        # Recover the session so a failed DB op does not leave
                        # it in an invalid-transaction state that would cascade
                        # to every subsequent aid in this worker.
                        try:
                            self.session.rollback()
                        except Exception:
                            pass
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
                self.record_queue.put(record)
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

    def cleanup(self):
        if self.session is not None:
            self.session.close()
