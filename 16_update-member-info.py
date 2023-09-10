from db import DBOperation, Session
from util import logging_init, get_week_day, fullname
from queue import Queue
from service import Service
from serverchan import sc_send_summary
from timer import Timer
from job import UpdateMemberJob, JobStat
import logging

script_id = '16'
script_name = 'update-member-info'
script_fullname = fullname(script_id, script_name)
logger = logging.getLogger(script_id)


def update_member_info():
    logger.info(f'Now start {script_fullname}...')
    timer = Timer()
    timer.start()

    session = Session()
    service = Service(mode='worker', retry=20)

    # get all mids
    all_mids: list[int] = DBOperation.query_all_member_mids(session)
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
    job_stat_list: list[JobStat] = []
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
    update_member_info()


if __name__ == '__main__':
    logging_init(file_prefix=script_id)
    main()
