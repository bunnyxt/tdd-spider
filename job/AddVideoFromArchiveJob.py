from .Job import Job
from db import Session
from service import Service, NewlistArchive
from task import add_video, commit_video_record_via_newlist_archive_stat, AlreadyExistError
from queue import Queue

__all__ = ['AddVideoFromArchiveJob']


class AddVideoFromArchiveJob(Job):
    def __init__(self, name: str,
                 archive_video_queue: Queue[tuple[int, NewlistArchive]], service: Service):
        super().__init__(name)
        self.archive_video_queue = archive_video_queue
        self.service = service
        self.session = Session()

    def process(self):
        while not self.archive_video_queue.empty():
            added, archive = self.archive_video_queue.get()
            try:
                new_video = add_video(archive.aid, self.service, self.session)
            except AlreadyExistError:
                self.logger.debug(f'Video presented in archives already exist! archive: {archive}')
            except Exception as e:
                self.logger.error(f'Fail to add video parsed from archive! archive: {archive}, error: {e}')
                self.stat.condition['add_video_exception'] += 1
            else:
                self.logger.info(f'New video detected in archives added! video: {new_video}')
                self.stat.condition['new_video'] += 1

                # commit video record via archive stat
                try:
                    new_video_record = commit_video_record_via_newlist_archive_stat(archive.stat, self.session)
                except Exception as e:
                    self.logger.error(f'Fail to add video record parsed from archive stat! '
                                      f'archive: {archive}, error: {e}')
                    self.stat.condition['commit_video_record_exception'] += 1
                else:
                    self.logger.info(f'New video record parsed from archive stat committed! '
                                     f'video record: {new_video_record}')
                self.stat.condition['new_video_record'] += 1

    def cleanup(self):
        self.session.close()
