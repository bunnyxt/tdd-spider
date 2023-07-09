from .Job import Job
from db import Session
from service import Service
from queue import Queue
from task import add_member_follower_record
from timer import Timer
from util import format_ts_ms

__all__ = ['AddMemberFollowerRecordJob']


class AddMemberFollowerRecordJob(Job):
    def __init__(self, name: str, mid_queue: Queue[int], service: Service):
        super().__init__(name)
        self.mid_queue = mid_queue
        self.service = service
        self.session = Session()

    def process(self):
        while not self.mid_queue.empty():
            mid = self.mid_queue.get()
            self.logger.debug(f'Now start add member follower record. mid: {mid}')
            timer = Timer()
            timer.start()

            try:
                new_follower_record = add_member_follower_record(mid, self.service, self.session)
            except Exception as e:
                self.logger.error(f'Fail to update member info. mid: {mid}, error: {e}')
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
