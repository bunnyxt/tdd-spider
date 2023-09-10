from .Job import Job
from db import Session
from service import Service, ArchiveRankByPartionArchive
from queue import Queue
from timer import Timer
from typing import Tuple
from util import format_ts_ms, get_ts_s

__all__ = ['GetPartionArchiveJob']


class GetPartionArchiveJob(Job):
    def __init__(self, name: str, tid: int, page_num_queue: Queue[int],
                 archive_video_queue: Queue[Tuple[int, ArchiveRankByPartionArchive]], service: Service):
        super().__init__(name)
        self.tid = tid
        self.page_num_queue = page_num_queue
        self.archive_video_queue = archive_video_queue
        self.service = service
        self.session = Session()

    def process(self):
        while not self.page_num_queue.empty():
            page_num = self.page_num_queue.get()
            self.logger.debug(f'Now start get partion video from archive page. page_num: {page_num}')
            timer = Timer()
            timer.start()

            # get archive rank by partion
            try:
                # override retry for get_archive_rank_by_partion to at least 10
                if self.service.get_default_retry() < 10:
                    archive_rank_by_partion = self.service.get_archive_rank_by_partion(
                        {'tid': self.tid, 'pn': page_num, 'ps': 50}, retry=10)
                else:
                    archive_rank_by_partion = self.service.get_archive_rank_by_partion(
                        {'tid': self.tid, 'pn': page_num, 'ps': 50})
            except Exception as e:
                self.logger.error(f'Fail to get archive rank by partion. '
                                  f'tid: {self.tid}, pn: {page_num}, ps: 50, error: {e}')
                self.stat.condition['get_archive_exception'] += 1
            else:
                added = get_ts_s()
                for archive in archive_rank_by_partion.archives:
                    # put archive video
                    self.archive_video_queue.put((added, archive))
                page_archive_len = len(archive_rank_by_partion.archives)
                if page_archive_len < 50:
                    self.logger.warning(f'Not fully loaded page found. '
                                        f'page_num: {page_num}, archive_len: {page_archive_len}')
                    self.stat.condition['not_fully_loaded_page'] += 1

            timer.stop()
            self.logger.debug(f'Finish put archive video from archive page. '
                              f'page_num: {page_num}, duration: {format_ts_ms(timer.get_duration_ms())}')
            self.stat.total_count += 1
            self.stat.total_duration_ms += timer.get_duration_ms()

    def cleanup(self):
        self.session.close()
