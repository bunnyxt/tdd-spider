import time
from .Job import Job
from service import Service
from queue import Queue, Empty
from task import add_member_follower_record
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

    def process(self):
        while True:
            try:
                mid = self.mid_queue.get(timeout=self.poll_timeout_s)
            except Empty:
                continue  # producer may still be filling; sentinel ends us
            if mid is None:  # sentinel: producer finished
                break

            self.logger.debug(f'Now start add member follower record. mid: {mid}')
            item_start = time.perf_counter()

            try:
                record_key = add_member_follower_record(mid, self.service)
            except Exception as e:
                self.logger.error(f'Fail to add member follower record. mid: {mid}, error: {e}')
                self.stat.condition['exception'] += 1
            else:
                self.logger.debug(f'New follower record committed. key (mid, added, id): {record_key}')
                self.stat.condition['success'] += 1

            duration_ms = int((time.perf_counter() - item_start) * 1000)
            self.logger.debug(f'Finish add member follower record. '
                              f'mid: {mid}, duration: {format_ts_ms(duration_ms)}')
            self.stat.total_count += 1
            self.stat.total_duration_ms += duration_ms
