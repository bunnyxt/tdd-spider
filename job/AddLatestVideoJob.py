from .Job import Job
from db import Session
from service import Service
from task import add_video, commit_video_record_via_archive_stat, AlreadyExistError
from timer import Timer
from util import format_ts_ms

__all__ = ['AddLatestVideoJob']


class AddLatestVideoJob(Job):
    def __init__(self, name: str, tid: int, service: Service, latest_page_num: int = 3):
        super().__init__(name)
        self.tid = tid
        self.service = service
        self.latest_page_num = latest_page_num
        self.session = Session()

    def process(self):
        for page_num in range(1, self.latest_page_num + 1):
            self.logger.debug(f'Now start add latest video from archive page. page_num: {page_num}')
            timer = Timer()
            timer.start()

            # get archive rank by partion
            try:
                archive_rank_by_partion = self.service.get_archive_rank_by_partion(
                    {'tid': self.tid, 'pn': page_num, 'ps': 50})
            except Exception as e:
                self.logger.error(f'Fail to get archive rank by partion. '
                                  f'tid: {self.tid}, pn: {page_num}, ps: 50, error: {e}')
                self.stat.condition['get_archive_exception'] += 1
                continue

            for archive in archive_rank_by_partion.archives:
                # add video
                try:
                    new_video = add_video(archive.aid, self.service, self.session)
                except AlreadyExistError:
                    self.logger.debug(f'Video parsed from archive already exist! archive: {archive}')
                except Exception as e:
                    self.logger.error(f'Fail to add video parsed from archive! archive: {archive}, error: {e}')
                    self.stat.condition['add_video_exception'] += 1
                else:
                    self.logger.info(f'New video parsed from archive added! video: {new_video}')
                    self.stat.condition['new_video'] += 1

                    # commit video record via archive stat
                    try:
                        new_video_record = commit_video_record_via_archive_stat(archive.stat, self.session)
                    except Exception as e:
                        self.logger.error(f'Fail to add video record parsed from archive stat! '
                                          f'archive: {archive}, error: {e}')
                        self.stat.condition['commit_video_record_exception'] += 1
                    else:
                        self.logger.info(f'New video record parsed from archive stat committed! '
                                         f'video record: {new_video_record}')
                        self.stat.condition['new_video_record'] += 1

            timer.stop()
            self.logger.debug(f'Finish add latest video from archive page. '
                              f'page_num: {page_num}, duration: {format_ts_ms(timer.get_duration_ms())}')
            self.stat.total_count += 1
            self.stat.total_duration_ms += timer.get_duration_ms()

    def cleanup(self):
        self.session.close()
