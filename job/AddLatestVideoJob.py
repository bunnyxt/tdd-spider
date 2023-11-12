from .Job import Job
from .JobStat import JobStat
from db import Session
from service import Service, NewlistArchive
from timer import Timer
from GetNewlistArchiveJob import GetNewlistArchiveJob
from AddVideoFromArchiveJob import AddVideoFromArchiveJob
from queue import Queue

__all__ = ['AddLatestVideoJob']


class AddLatestVideoJob(Job):
    def __init__(self, name: str, tid: int, service: Service, latest_page_num: int = 3):
        super().__init__(name)
        self.tid = tid
        self.service = service
        self.latest_page_num = latest_page_num
        self.session = Session()

    def process(self):
        self.logger.debug(f'Now start adding latest video from archive page...')
        timer = Timer()
        timer.start()

        # prepare page num queue
        page_num_queue = Queue[int]()

        # put page num
        for page_num in range(1, self.latest_page_num + 1):
            page_num_queue.put(page_num)
        self.logger.debug(f'Page num queue prepared. page_num_queue: {page_num_queue}')

        # prepare archive video queue
        archive_video_queue = Queue[tuple[int, NewlistArchive]]()

        # create get newlist archive job
        get_newlist_archive_job = GetNewlistArchiveJob(
            'job_tid_30', self.tid, page_num_queue, archive_video_queue, self.service)

        # create add video from archive jobs
        add_video_from_archive_job_num = 10
        add_video_from_archive_job_list = []
        for i in range(add_video_from_archive_job_num):
            add_video_from_archive_job_list.append(AddVideoFromArchiveJob(
                f'job_{i}', archive_video_queue, self.service))

        # start get newlist archive job
        get_newlist_archive_job.start()
        self.logger.debug(f'1 get newlist archive job started.')

        # start add video from archive jobs
        for job in add_video_from_archive_job_list:
            job.start()
        self.logger.debug(f'{add_video_from_archive_job_num} add video from archive jobs started.')

        # wait for get newlist archive job
        get_newlist_archive_job.join()

        # wait for add video from archive jobs
        for job in add_video_from_archive_job_list:
            job.join()

        # collect statistic
        get_newlist_archive_job_stat = get_newlist_archive_job.stat
        add_video_from_archive_job_stat_list: list[JobStat] = []
        for job in add_video_from_archive_job_list:
            add_video_from_archive_job_stat_list.append(job.stat)

        # merge statistic
        add_video_from_archive_job_stat_merged = sum(add_video_from_archive_job_stat_list, JobStat())

        timer.stop()

        # generate new job stat
        self.stat.start_ts_ms = timer.start_ts_ms
        self.stat.end_ts_ms = timer.end_ts_ms
        self.stat.total_count = add_video_from_archive_job_stat_merged.total_count
        self.stat.total_duration_ms = add_video_from_archive_job_stat_merged.total_duration_ms
        self.stat.condition = get_newlist_archive_job_stat.condition + add_video_from_archive_job_stat_merged.condition

    def cleanup(self):
        self.session.close()
