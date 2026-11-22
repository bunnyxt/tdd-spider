import time
import os
from .Job import Job
from service import Service
from queue import Queue
from task import fetch_video_record_via_video_view, commit_sprint_video_record
from util import format_ts_ms

__all__ = ['AddSprintVideoRecordJob']


class AddSprintVideoRecordJob(Job):
    def __init__(self, name: str, aid_queue: Queue[int], service: Service):
        super().__init__(name)
        self.aid_queue = aid_queue
        self.service = service
        self.error = None
        for name in ('fetch_success', 'fetch_failed', 'db_write_failed',
                     'success', 'persisted', 'exception', 'million_success'):
            self.stat.condition[name] = 0

    def run(self):
        try:
            super().run()
        except Exception as error:
            self.error = error
            self.logger.exception('Sprint record job failed')

    def process(self):
        while not self.aid_queue.empty():
            aid = self.aid_queue.get()
            self.logger.debug(f'Now start add sprint video record. aid: {aid}')
            item_start = time.perf_counter()

            try:
                record = fetch_video_record_via_video_view(aid, self.service)
            except Exception as e:
                self.logger.error('Sprint record fetch failed. aid: %s, error: %s. '
                                  'No failed-write file exists for this request; re-fetch this aid.', aid, e)
                self.stat.condition['fetch_failed'] += 1
                self.stat.condition['exception'] += 1
            else:
                self.stat.condition['fetch_success'] += 1
                try:
                    new_sprint_video_record = commit_sprint_video_record(record)
                except Exception as e:
                    self.logger.error('Sprint database write unconfirmed. aid: %s, error: %s. '
                                      'Failure input is saved best-effort under %s; check the write '
                                      'log for the actual file or a save failure. Reconcile the '
                                      'database before manually resubmitting saved input.', aid, e,
                                      os.environ.get('TDD_FAILURE_DIR', 'data/failed-writes'))
                    self.stat.condition['db_write_failed'] += 1
                    self.stat.condition['exception'] += 1
                else:
                    self.logger.info(f'New sprint video record {new_sprint_video_record} added. aid: {aid}')
                    self.stat.condition['success'] += 1
                    self.stat.condition['persisted'] += 1

                    # The task confirms the record and status in one transaction.
                    if new_sprint_video_record.view is not None and new_sprint_video_record.view >= 1000000:
                        self.logger.info(f'Sprint video status confirmed finished. aid: {aid}')
                        self.stat.condition['million_success'] += 1

            duration_ms = int((time.perf_counter() - item_start) * 1000)
            self.logger.debug(f'Finish add sprint video record. '
                              f'aid: {aid}, duration: {format_ts_ms(duration_ms)}')
            self.stat.total_count += 1
            self.stat.total_duration_ms += duration_ms
