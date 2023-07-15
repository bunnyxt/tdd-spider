from .Job import Job
from db import Session
from service import Service
from queue import Queue
from task import update_video
from timer import Timer
from util import b2a, format_ts_ms

__all__ = ['UpdateVideoJob']


class UpdateVideoJob(Job):
    def __init__(self, name: str, bvid_queue: Queue[str], service: Service):
        super().__init__(name)
        self.bvid_queue = bvid_queue
        self.service = service
        self.session = Session()

    def process(self):
        while not self.bvid_queue.empty():
            bvid = self.bvid_queue.get()
            self.logger.debug(f'Now start update video info. bvid: {bvid}')
            timer = Timer()
            timer.start()

            try:
                tdd_video_logs = update_video(b2a(bvid), self.service, self.session)
            except Exception as e:
                self.logger.error(f'Fail to update video info. bvid: {bvid}, error: {e}')
                self.stat.condition['exception'] += 1
            else:
                for log in tdd_video_logs:
                    self.logger.info(f'Update video info. bvid: {bvid}, attr: {log.attr}, {log.oldval} -> {log.newval}')
                self.logger.debug(f'{len(tdd_video_logs)} log(s) found. bvid: {bvid}')
                self.stat.condition[f'{len(tdd_video_logs)}_update'] += 1

            timer.stop()
            self.logger.debug(f'Finish update video info. '
                              f'bvid: {bvid}, duration: {format_ts_ms(timer.get_duration_ms())}')
            self.stat.total_count += 1
            self.stat.total_duration_ms += timer.get_duration_ms()

    def cleanup(self):
        self.session.close()
