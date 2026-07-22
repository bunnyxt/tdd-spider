from .Job import Job
from db import Session
from service import Service
from queue import Queue, Empty
from task import add_member_follower_record
from timer import Timer
from util import format_ts_ms
from typing import Optional

__all__ = ['AddMemberFollowerRecordJob']


class AddMemberFollowerRecordJob(Job):
    """
    Fetch a member's follower/following counts and append a follower record.
    Terminates on a None sentinel: the producer must put one None per worker
    after filling the queue.

    Sentinel termination (not `while not queue.empty(): queue.get()`) fixes a
    race -- two workers both see a non-empty queue, both call get(), and the
    loser blocks forever on the last item.
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

            self.logger.debug(f'Now start add member follower record. mid: {mid}')
            timer = Timer()
            timer.start()

            try:
                new_follower_record = add_member_follower_record(mid, self.service, self.session)
            except Exception as e:
                # roll back, else the failed transaction poisons this session
                # and every subsequent mid on this worker fails too
                try:
                    self.session.rollback()
                except Exception:
                    pass
                self.logger.error(f'Fail to add member follower record. mid: {mid}, error: {e}')
                self.stat.condition['exception'] += 1
            else:
                self.logger.debug(f'New member follower record {new_follower_record} added. mid: {mid}')
                self.stat.condition['success'] += 1

            timer.stop()
            self.logger.debug(f'Finish add member follower record. '
                              f'mid: {mid}, duration: {format_ts_ms(timer.get_duration_ms())}')
            self.stat.total_count += 1
            self.stat.total_duration_ms += timer.get_duration_ms()

    def cleanup(self):
        self.session.close()
