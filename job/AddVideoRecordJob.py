from .Job import Job
from service import Service, CodeError
from timer import Timer
from queue import Queue
from db import Session, TddVideoRecord
from util import format_ts_ms, get_ts_s, ts_s_to_str
from task import add_video_record_via_video_view, update_video
from typing import Optional

__all__ = ['AddVideoRecordJob']


class AddVideoRecordJob(Job):
    def __init__(self, name: str, aid_queue: Queue[int], video_record_queue: Queue[TddVideoRecord], service: Service,
                 update_if_code_error: bool = True, duration_limit_s: Optional[int] = None):
        super().__init__(name)
        self.aid_queue = aid_queue
        self.video_record_queue = video_record_queue
        self.service = service
        self.session = Session()
        self.update_if_code_error = update_if_code_error
        self.duration_limit_s = duration_limit_s
        self.duration_limit_due_ts_s = None

    def process(self):
        if self.duration_limit_s is not None:
            self.duration_limit_due_ts_s = get_ts_s() + self.duration_limit_s
            self.logger.info(f'Duration limit due at {ts_s_to_str(self.duration_limit_due_ts_s)}.')

        while not self.aid_queue.empty():
            if self.duration_limit_due_ts_s is not None and get_ts_s() >= self.duration_limit_due_ts_s:
                self.logger.info(f'Duration limit reached. Now exit.')
                break

            aid = self.aid_queue.get()
            self.logger.debug(f'Now start add video record. aid: {aid}')
            timer = Timer()
            timer.start()

            try:
                new_video_record = add_video_record_via_video_view(aid, self.service, self.session)
            except CodeError as e:
                if self.update_if_code_error:
                    self.logger.info(f'Code error occurred. Now start update video. aid: {aid}')
                    try:
                        tdd_video_logs = update_video(aid, self.service, self.session)
                    except Exception as e2:
                        self.logger.error(f'Fail to update video info. aid: {aid}, error: {e2}')
                        self.stat.condition['update_exception'] += 1
                    else:
                        for log in tdd_video_logs:
                            self.logger.info(
                                f'Update video info. aid: {aid}, attr: {log.attr}, {log.oldval} -> {log.newval}')
                        self.logger.debug(f'{len(tdd_video_logs)} log(s) found. aid: {aid}')
                        self.stat.condition[f'{len(tdd_video_logs)}_update'] += 1
                else:
                    self.logger.error(f'Fail to add video record. aid: {aid}, error: {e}')
                self.stat.condition['code_error'] += 1
            except Exception as e:
                self.logger.error(f'Fail to add video record. aid: {aid}, error: {e}')
                self.stat.condition['other_exception'] += 1
            else:
                self.video_record_queue.put(new_video_record)
                self.logger.debug(f'New video record {new_video_record} added. aid: {aid}')
                self.stat.condition['success'] += 1

            timer.stop()
            self.logger.debug(f'Finish add video record. '
                              f'aid: {aid}, duration: {format_ts_ms(timer.get_duration_ms())}')
            self.stat.total_count += 1
            self.stat.total_duration_ms += timer.get_duration_ms()

    def cleanup(self):
        self.session.close()
