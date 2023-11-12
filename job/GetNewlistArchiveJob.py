from .Job import Job
from db import Session
from service import Service, NewlistArchive
from queue import Queue
from timer import Timer
from util import format_ts_ms, get_ts_s

__all__ = ['GetNewlistArchiveJob']


class GetNewlistArchiveJob(Job):
    def __init__(self, name: str, tid: int, page_num_queue: Queue[int],
                 archive_video_queue: Queue[tuple[int, NewlistArchive]], service: Service):
        super().__init__(name)
        self.tid = tid
        self.page_num_queue = page_num_queue
        self.archive_video_queue = archive_video_queue
        self.service = service
        self.session = Session()

    def process(self):
        while not self.page_num_queue.empty():
            page_num = self.page_num_queue.get()
            self.logger.debug(f'Now start get archive video from archive page. page_num: {page_num}')
            timer = Timer()
            timer.start()

            # get newlist
            try:
                newlist = self.service.get_newlist({'rid': self.tid, 'pn': page_num, 'ps': 50})
            except Exception as e:
                self.logger.error(f'Fail to get newlist. '
                                  f'rid: {self.tid}, pn: {page_num}, ps: 50, error: {e}')
                self.stat.condition['get_newlist_exception'] += 1
            else:
                added = get_ts_s()
                for archive in newlist.archives:
                    # put archive video
                    self.archive_video_queue.put((added, archive))
                page_archive_len = len(newlist.archives)
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
