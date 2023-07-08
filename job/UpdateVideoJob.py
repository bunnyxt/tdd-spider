from .Job import Job
from db import Session
from service import Service
from queue import Queue
from collections import defaultdict
from task import update_video
from util import b2a, get_ts_ms, format_ts_ms

__all__ = ['UpdateVideoJob']


class UpdateVideoJob(Job):
    def __init__(self, name: str, bvid_queue: Queue[str], statistics: defaultdict[str, int], service: Service):
        super().__init__(name)
        self.bvid_queue = bvid_queue
        self.statistics = statistics
        self.service = service
        self.session = Session()

    def run(self):
        self.logger.info(f'Job start.')
        while not self.bvid_queue.empty():
            bvid = self.bvid_queue.get()
            start_ts_ms = get_ts_ms()
            try:
                tdd_video_logs = update_video(b2a(bvid), self.service, self.session)
            except Exception as e:
                self.logger.error(f'Fail to update video info. bvid: {bvid}, error: {e}')
                self.statistics['other_exception_count'] += 1
            else:
                if len(tdd_video_logs) == 0:
                    self.statistics['no_update_count'] += 1
                else:
                    self.statistics['change_count'] += 1
                for log in tdd_video_logs:
                    self.logger.info(f'Update video info. bvid: {bvid}, attr: {log.attr}, {log.oldval} -> {log.newval}')
                    self.statistics['change_log_count'] += 1
                self.logger.debug(f'{tdd_video_logs} log(s) found. bvid: {bvid}')
            end_ts_ms = get_ts_ms()
            cost_ms = end_ts_ms - start_ts_ms
            self.logger.debug(f'Finish update video info. bvid: {bvid}, cost: {format_ts_ms(cost_ms)}')
            self.statistics['total_count'] += 1
            self.statistics['total_cost_ms'] += cost_ms
        self.session.close()
        self.logger.info(f'Job end.')
