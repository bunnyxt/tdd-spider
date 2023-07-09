from db import DBOperation, Session
from util import get_ts_s, ts_s_to_str, get_week_day, format_ts_s
from queue import Queue
from service import Service
from serverchan import sc_send
from typing import List
from job import UpdateMemberJob, JobStat
from logutils import logging_init
import logging

logger = logging.getLogger('16')


def update_member_info():
    logger.info('Now start update member info...')
    start_ts = get_ts_s()  # get start ts

    session = Session()
    service = Service(mode='worker', retry=20)

    # get all mids
    all_mids: List[int] = DBOperation.query_all_member_mids(session)
    logger.info(f'Total {len(all_mids)} members got.')

    # add latest 1000 mids first
    mids = all_mids[-1000:]

    # TODO: add top 200 follower mids

    # for the rest, add 1 / 7 of them, according to the week day (0-6)
    week_day = get_week_day()
    for idx, mid in enumerate(all_mids[:-1000]):
        if idx % 7 == week_day:
            mids.append(mid)

    logger.info(f'Will update {len(mids)} videos info.')

    mids = mids[-1000:]

    # put mid into queue
    mid_queue: Queue[int] = Queue()
    for mid in mids:
        mid_queue.put(mid)
    logger.info(f'{mid_queue.qsize()} mids put into queue.')

    # create jobs
    job_num = 20
    job_list = []
    for i in range(job_num):
        job_list.append(UpdateMemberJob(f'job_{i}', mid_queue, service))

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
        '# update video info done!\n\n' \
        f'start: {ts_s_to_str(start_ts)}, ' \
        f'end: {ts_s_to_str(end_ts)}, ' \
        f'duration: {format_ts_s(end_ts - start_ts)}\n\n' \
        f'{job_stat_merged.get_summary()}\n\n' \
        f'by bunnyxt, {ts_s_to_str(get_ts_s())}'

    logger.info('Finish update member info!')
    logger.warning(summary)

    # send sc
    sc_send('Finish update member info!', summary)

    session.close()


def main():
    update_member_info()


if __name__ == '__main__':
    logging_init(file_prefix='16')
    main()
