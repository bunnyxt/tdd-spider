from db import DBOperation, Session
from service import Service
from util import logging_init, fullname
from serverchan import sc_send_summary
from queue import Queue
from typing import List
from timer import Timer
from job import AddMemberFollowerRecordJob, JobStat
import logging

script_id = '17'
script_name = 'add-member-follower-record'
script_fullname = fullname(script_id, script_name)
logger = logging.getLogger(script_id)


def add_member_follower_record():
    logger.info(f'Now start {script_fullname}...')
    timer = Timer()
    timer.start()

    session = Session()
    service = Service(mode='worker')

    # load all mids
    mids: List[int] = DBOperation.query_all_member_mids(session=session)
    logger.info(f'Total {len(mids)} members got.')

    # put mid into queue
    mid_queue: Queue[int] = Queue()
    for mid in mids:
        mid_queue.put(mid)
    logger.info(f'{mid_queue.qsize()} mids put into queue.')

    # create jobs
    job_num = 20
    job_list = []
    for i in range(job_num):
        job_list.append(AddMemberFollowerRecordJob(f'job_{i}', mid_queue, service))

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
    add_member_follower_record()


if __name__ == '__main__':
    logging_init(file_prefix=script_id)
    main()
