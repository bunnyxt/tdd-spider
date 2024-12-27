from service import Service, NewlistArchive
from serverchan import sc_send_summary
from timer import Timer
from queue import Queue
from job import JobStat, GetNewlistArchiveJob, AddVideoFromArchiveJob
from util import logging_init, fullname
import logging

script_id = '12'
script_name = 'add-latest-video-with-tid-30'
script_fullname = fullname(script_id, script_name)
logger = logging.getLogger(script_id)


def add_latest_video_with_tid_30():
    logger.info(f'Now start {script_fullname}...')
    timer = Timer()
    timer.start()

    tid = 30
    latest_page_num = 3
    service = Service(mode='worker')

    # prepare page num queue
    page_num_queue = Queue[int]()

    # put page num
    for page_num in range(1, latest_page_num + 1):
        page_num_queue.put(page_num)
    logger.info(f'Page num queue from 1 to {latest_page_num} prepared.')

    # prepare archive video queue
    archive_video_queue = Queue[tuple[int, NewlistArchive]]()

    # create get newlist archive job
    get_newlist_archive_job = GetNewlistArchiveJob(
        'job_tid_30', tid, page_num_queue, archive_video_queue, service)

    # start get newlist archive job
    get_newlist_archive_job.start()
    logger.info(f'1 get newlist archive job started.')

    # wait for get newlist archive job
    get_newlist_archive_job.join()

    # collect statistic
    get_newlist_archive_job_stat = get_newlist_archive_job.stat

    logger.info(
        f'{archive_video_queue.qsize()} archive(s) from newlist archive pages fetched.')

    # create add video from archive jobs
    add_video_from_archive_job_num = 10
    add_video_from_archive_job_list = []
    for i in range(add_video_from_archive_job_num):
        add_video_from_archive_job_list.append(
            AddVideoFromArchiveJob(f'job_{i}', archive_video_queue, service))

    # start add video from archive jobs
    for job in add_video_from_archive_job_list:
        job.start()
    logger.info(
        f'{add_video_from_archive_job_num} add video from archive jobs started.')

    # wait for add video from archive jobs
    for job in add_video_from_archive_job_list:
        job.join()

    # collect statistic
    add_video_from_archive_job_stat_list: list[JobStat] = []
    for job in add_video_from_archive_job_list:
        add_video_from_archive_job_stat_list.append(job.stat)

    # merge statistic
    add_video_from_archive_job_stat_merged = sum(
        add_video_from_archive_job_stat_list, JobStat())

    # generate concatenated job stat for summary
    concatenated_stat = JobStat()
    concatenated_stat.total_count = add_video_from_archive_job_stat_merged.total_count
    concatenated_stat.total_duration_ms = add_video_from_archive_job_stat_merged.total_duration_ms
    concatenated_stat.condition = get_newlist_archive_job_stat.condition + \
        add_video_from_archive_job_stat_merged.condition

    timer.stop()
    concatenated_stat.start_ts_ms = timer.start_ts_ms
    concatenated_stat.end_ts_ms = timer.end_ts_ms

    # summary
    logger.info(f'Finish {script_fullname}!')
    logger.info(timer.get_summary())
    logger.info(concatenated_stat.get_summary())
    if concatenated_stat.condition['get_newlist_exception'] > 0 \
            or concatenated_stat.condition['add_video_exception'] > 0 \
            or concatenated_stat.condition['commit_video_record_exception'] > 0:
        sc_send_summary(script_fullname, timer, concatenated_stat)


def main():
    add_latest_video_with_tid_30()


if __name__ == '__main__':
    logging_init(file_prefix=script_id)
    main()
