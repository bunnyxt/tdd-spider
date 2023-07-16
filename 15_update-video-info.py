from db import DBOperation, Session
from service import Service
from serverchan import sc_send_summary
from util import logging_init, get_week_day, fullname
from timer import Timer
from queue import Queue
from typing import List
from job import UpdateVideoJob, JobStat
import logging

script_id = '15'
script_name = 'update-video-info'
script_fullname = fullname(script_id, script_name)
logger = logging.getLogger(script_id)


def update_video_info():
    logger.info(f'Now start {script_fullname}...')
    timer = Timer()
    timer.start()

    session = Session()
    service = Service(mode='worker')

    # get all bvids
    all_bvids: List[str] = DBOperation.query_all_video_bvids(session)
    logger.info(f'Total {len(all_bvids)} videos got.')

    # add latest 5000 bvids first
    bvids = all_bvids[-5000:]

    # TODO: add top 1000 view bvids

    # for the rest, add 1 / 7 of them, according to the week day (0-6)
    week_day = get_week_day()
    for idx, bvid in enumerate(all_bvids[:-5000]):
        if idx % 7 == week_day:
            bvids.append(bvid)

    logger.info(f'Will update {len(bvids)} videos info.')

    # put bvid into queue
    bvid_queue: Queue[str] = Queue()
    for bvid in bvids:
        bvid_queue.put(bvid)
    logger.info(f'{bvid_queue.qsize()} bvids put into queue.')

    # create jobs
    job_num = 20
    job_list = []
    for i in range(job_num):
        job_list.append(UpdateVideoJob(f'job_{i}', bvid_queue, service))

    # start jobs
    for job in job_list:
        job.start()
    logger.info(f'{job_num} job(s) started.')

    # wait for jobs
    for job in job_list:
        job.join()

    # collect statistics
    job_stat_list: List[JobStat] = []
    for job in job_list:
        job_stat_list.append(job.stat)

    # merge statistics counters
    job_stat_merged = sum(job_stat_list, JobStat())

    session.close()

    timer.stop()

    # summary
    logger.info(f'Finish {script_fullname}!')
    logger.info(timer.get_summary())
    logger.info(job_stat_merged.get_summary())
    sc_send_summary(script_fullname, timer, job_stat_merged)


def main():
    update_video_info()


if __name__ == '__main__':
    logging_init(file_prefix=script_id)
    main()
