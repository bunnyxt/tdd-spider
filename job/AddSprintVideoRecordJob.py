from .Job import Job
from db import Session
from service import Service
from queue import Queue
from task import add_sprint_video_record_via_video_view
from timer import Timer
from util import format_ts_ms

__all__ = ['AddSprintVideoRecordJob']


class AddSprintVideoRecordJob(Job):
    def __init__(self, name: str, aid_queue: Queue[int], service: Service):
        super().__init__(name)
        self.aid_queue = aid_queue
        self.service = service
        self.session = Session()

    def process(self):
        while not self.aid_queue.empty():
            aid = self.aid_queue.get()
            self.logger.debug(f'Now start add sprint video record. aid: {aid}')
            timer = Timer()
            timer.start()

            try:
                new_sprint_video_record = add_sprint_video_record_via_video_view(aid, self.service, self.session)
            except Exception as e:
                self.logger.error(f'Fail to add sprint video record. aid: {aid}, error: {e}')
                self.stat.condition['exception'] += 1
            else:
                self.logger.debug(f'New sprint video record {new_sprint_video_record} added. aid: {aid}')
                self.stat.condition['success'] += 1

                # check million
                if new_sprint_video_record.view >= 1000000:
                    self.logger.info(f'New sprint video record {new_sprint_video_record} has million views. '
                                     f'Now start change sprint video status to finished. aid: {aid}')
                    try:
                        sql = f'update tdd_sprint_video set status = "finished" where aid = {aid}'
                        self.session.execute(sql)
                        self.session.commit()
                    except Exception as e:
                        self.logger.error(f'Fail to change sprint video status to finished. aid: {aid}, error: {e}')
                        self.stat.condition['million_exception'] += 1
                    else:
                        self.logger.info(f'Sprint video status changed to finished. aid: {aid}')
                        self.stat.condition['million_success'] += 1

            timer.stop()
            self.logger.debug(f'Finish add sprint video record. '
                              f'aid: {aid}, duration: {format_ts_ms(timer.get_duration_ms())}')
            self.stat.total_count += 1
            self.stat.total_duration_ms += timer.get_duration_ms()

    def cleanup(self):
        self.session.close()
