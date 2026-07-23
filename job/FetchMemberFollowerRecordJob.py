from .Job import Job
from service import Service, ServiceError
from timer import Timer
from queue import Queue, Empty, Full
from core import MemberFollowerRecordNew
from util import get_ts_s, ts_s_to_str
from task import fetch_member_follower_record
from typing import Optional

__all__ = ['FetchMemberFollowerRecordJob']


class FetchMemberFollowerRecordJob(Job):
    """
    Fetch-only worker: HTTP fetch -> MemberFollowerRecordNew -> record_queue.
    Holds NO DB connection (member-follower is append-only, so there is no
    read/compare, just the relation fetch). A BatchInsertMemberFollowerRecordJob
    drains the queue and batch-inserts, removing the per-record commit
    contention that pinned 17_'s old per-worker-insert path.
    """

    def __init__(self, name: str, mid_queue: Queue[int],
                 record_queue: 'Queue[Optional[MemberFollowerRecordNew]]',
                 service: Service,
                 duration_limit_s: Optional[int] = None, put_timeout_s: float = 30.0):
        super().__init__(name)
        self.mid_queue = mid_queue
        self.record_queue = record_queue
        self.service = service
        self.duration_limit_s = duration_limit_s
        self.duration_limit_due_ts_s = None
        self.put_timeout_s = put_timeout_s

    def _put_record(self, record) -> bool:
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
            if (self.duration_limit_due_ts_s is not None
                    and get_ts_s() >= self.duration_limit_due_ts_s):
                self.logger.info(f'Duration limit reached. Now exit. '
                                 f'{self.mid_queue.qsize()} mid(s) left unfetched.')
                self.stat.condition['duration_limit_reached'] += 1
                break

            # get_nowait, not empty()+get(): the latter races -- workers seeing
            # the same last item all call get(), the losers block forever.
            try:
                mid = self.mid_queue.get_nowait()
            except Empty:
                break
            self.logger.debug(f'Now start fetch member follower record. mid: {mid}')
            timer = Timer()
            timer.start()

            stage_stat = {}
            try:
                record = fetch_member_follower_record(mid, self.service, out_stat=stage_stat)
            except ServiceError as e:
                self.logger.error(f'Fail to fetch member follower record. mid: {mid}, error: {e}')
                self.stat.condition['exception'] += 1
            except Exception as e:
                self.logger.error(f'Fail to fetch member follower record. mid: {mid}, error: {e}')
                self.stat.condition['other_exception'] += 1
            else:
                # bounded queue: block on a slow writer, but never forever --
                # give up on the record if a dead writer stalls the pool
                while not self._put_record(record):
                    if (self.duration_limit_due_ts_s is not None
                            and get_ts_s() >= self.duration_limit_due_ts_s):
                        self.logger.error(
                            f'Record queue full and duration limit reached -- writer stalled. '
                            f'Dropping record and exiting. mid: {mid}')
                        self.stat.condition['record_dropped_queue_full'] += 1
                        return
                    self.logger.warning(
                        f'Record queue full for {self.put_timeout_s}s -- writer slow/dead. '
                        f'Still waiting. mid: {mid}')
                self.stat.condition['success'] += 1

            timer.stop()
            for stage_key, stage_ms in stage_stat.items():
                self.stat.condition[stage_key] += stage_ms
            self.stat.total_count += 1
            self.stat.total_duration_ms += timer.get_duration_ms()
