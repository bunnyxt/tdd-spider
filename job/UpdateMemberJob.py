from .Job import Job
from db import Session
from service import Service
from queue import Queue
from task import update_member
from timer import Timer
from util import format_ts_ms

__all__ = ['UpdateMemberJob']


class UpdateMemberJob(Job):
    def __init__(self, name: str, mid_queue: Queue[int], service: Service):
        super().__init__(name)
        self.mid_queue = mid_queue
        self.service = service
        self.session = Session()

    def process(self):
        while not self.mid_queue.empty():
            mid = self.mid_queue.get()
            self.logger.debug(f'Now start update member info. mid: {mid}')
            timer = Timer()
            timer.start()

            try:
                tdd_member_logs = update_member(mid, self.service, self.session)
            except Exception as e:
                self.logger.error(f'Fail to update member info. mid: {mid}, error: {e}')
                self.stat.condition['exception'] += 1
            else:
                for log in tdd_member_logs:
                    self.logger.info(f'Update member info. bvid: {mid}, attr: {log.attr}, {log.oldval} -> {log.newval}')
                self.logger.debug(f'{len(tdd_member_logs)} log(s) found. mid: {mid}')
                self.stat.condition[f'{len(tdd_member_logs)}_update'] += 1

            timer.stop()
            self.logger.debug(f'Finish update member info. '
                              f'mid: {mid}, duration: {format_ts_ms(timer.get_duration_ms())}')
            self.stat.total_count += 1
            self.stat.total_duration_ms += timer.get_duration_ms()

    def cleanup(self):
        self.session.close()
