from db import DBOperation, Session
from service import Service
from util import get_ts_s, ts_s_to_str
from serverchan import sc_send
from queue import Queue
from typing import List
from util import format_ts_s
from job import AddMemberFollowerRecordJob, JobStat
from logutils import logging_init
import logging

logger = logging.getLogger('17')


def add_member_follower_record():
    logger.info('Now start add member follower record...')
    start_ts = get_ts_s()  # get start ts

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

    # get end ts
    end_ts = get_ts_s()

    # make summary
    summary = \
        '# add member follower record done!\n\n' \
        f'start: {ts_s_to_str(start_ts)}, ' \
        f'end: {ts_s_to_str(end_ts)}, ' \
        f'duration: {format_ts_s(end_ts - start_ts)}\n\n' \
        f'{job_stat_merged.get_summary()}\n\n' \
        f'by bunnyxt, {ts_s_to_str(get_ts_s())}'

    logger.info('Finish add member follower record!')
    logger.warning(summary)

    # send sc
    sc_send('Finish add member follower record!', summary)

    session.close()


def main():
    add_member_follower_record()


if __name__ == '__main__':
    logging_init(file_prefix='17')
    main()
