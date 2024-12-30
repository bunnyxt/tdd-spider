from .Job import Job
from service import Service
from timer import Timer
from queue import Queue
from db import Session
from util import format_ts_ms
from task import add_video, AlreadyExistError

__all__ = ['AddVideoJob']


class AddVideoJob(Job):
    def __init__(self, name: str, aid_queue: Queue[int], service: Service, test_exist: bool = True):
        super().__init__(name)
        self.aid_queue = aid_queue
        self.service = service
        self.session = Session()
        self.test_exist = test_exist

    def process(self):
        while not self.aid_queue.empty():
            aid = self.aid_queue.get()
            self.logger.debug(f'Now start add video. aid: {aid}')
            timer = Timer()
            timer.start()

            try:
                new_video = add_video(
                    aid, self.service, self.session, self.test_exist)
            except AlreadyExistError:
                self.logger.info(
                    f'Video already exist! aid: {aid}')
                self.stat.condition['already_exist_video'] += 1
            except Exception as e:
                self.logger.error(
                    f'Fail to add video! aid: {aid}, error: {e}')
                self.stat.condition['other_exception'] += 1
            else:
                self.logger.info(
                    f'New video added! video: {new_video}')
                self.stat.condition['success'] += 1

            timer.stop()
            self.logger.debug(f'Finish add video record. '
                              f'aid: {aid}, duration: {format_ts_ms(timer.get_duration_ms())}')
            self.stat.total_count += 1
            self.stat.total_duration_ms += timer.get_duration_ms()

    def cleanup(self):
        self.session.close()
