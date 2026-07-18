from .Job import Job
from db import Session
from service import Service
from queue import Queue, Empty
from task import update_member
from timer import Timer
from util import format_ts_ms
from typing import Optional

__all__ = ['UpdateMemberJob']


class UpdateMemberJob(Job):
    """
    Refresh tdd_member info for mids drained from mid_queue. Terminates on a None
    sentinel: the producer must put one None per worker after filling the queue.

    Sentinel termination (not `while not queue.empty(): queue.get()`) fixes a
    race -- two workers both see a non-empty queue, both call get(), and the
    loser blocks forever on the last item. A blocking get with timeout also lets
    the worker exit cleanly.
    """

    def __init__(self, name: str, mid_queue: 'Queue[Optional[int]]', service: Service,
                 poll_timeout_s: float = 1.0):
        super().__init__(name)
        self.mid_queue = mid_queue
        self.service = service
        self.poll_timeout_s = poll_timeout_s
        self.session = Session()

    def process(self):
        while True:
            try:
                mid = self.mid_queue.get(timeout=self.poll_timeout_s)
            except Empty:
                continue  # producer may still be filling; sentinel ends us
            if mid is None:  # sentinel: producer finished
                break

            self.logger.debug(f'Now start update member info. mid: {mid}')
            timer = Timer()
            timer.start()

            try:
                tdd_member_logs = update_member(mid, self.service, self.session)
            except Exception as e:
                # roll back, else the failed transaction poisons this session
                # and every subsequent mid on this worker fails too
                try:
                    self.session.rollback()
                except Exception:
                    pass
                self.logger.error(f'Fail to update member info. mid: {mid}, error: {e}')
                self.stat.condition['update_exception'] += 1
            else:
                for log in tdd_member_logs:
                    self.logger.info(f'Update member info. mid: {mid}, '
                                     f'attr: {log.attr}, {log.oldval} -> {log.newval}')
                self.logger.debug(f'{len(tdd_member_logs)} log(s) found. mid: {mid}')
                self.stat.condition[f'{len(tdd_member_logs)}_update'] += 1

            timer.stop()
            self.logger.debug(f'Finish update member info. '
                              f'mid: {mid}, duration: {format_ts_ms(timer.get_duration_ms())}')
            self.stat.total_count += 1
            self.stat.total_duration_ms += timer.get_duration_ms()

    def cleanup(self):
        self.session.close()
