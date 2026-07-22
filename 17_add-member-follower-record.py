from db import DBOperation, Session
from service import Service
from util import logging_init, fullname
from serverchan import sc_send_summary
from queue import Queue
from timer import Timer
from job import AddMemberFollowerRecordJob, JobPool
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
    mids: list[int] = DBOperation.query_all_member_mids(session=session)
    logger.info(f'Total {len(mids)} members got.')

    # put mid into queue
    mid_queue: Queue[int] = Queue()
    for mid in mids:
        mid_queue.put(mid)
    # one sentinel per worker (AddMemberFollowerRecordJob is sentinel-terminated)
    job_num = 20
    for _ in range(job_num):
        mid_queue.put(None)
    logger.info(f'{len(mids)} mids put into queue.')

    # JobPool gives a per-30s PROGRESS heartbeat over the ~1h run (previously
    # blind) and merges the workers' stats.
    pool = JobPool(
        [AddMemberFollowerRecordJob(f'job_{i}', mid_queue, service) for i in range(job_num)],
        progress_total=len(mids),
        progress_label='follower-record',
        progress_interval_s=30.0,
        logger_name=script_id)
    pool.start()
    logger.info(f'{job_num} job(s) started.')
    job_stat_merged = pool.join()

    session.close()

    timer.stop()

    # summary
    logger.info(f'Finish {script_fullname}!')
    logger.info(timer.get_summary())
    logger.info(job_stat_merged.get_summary('follower-record'))
    sc_send_summary(script_fullname, timer, job_stat_merged)


def main():
    add_member_follower_record()


if __name__ == '__main__':
    logging_init(file_prefix=script_id)
    main()
