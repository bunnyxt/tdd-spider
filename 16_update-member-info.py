from db import DBOperation, Session
from util import logging_init, get_week_day, fullname
from queue import Queue
from service import Service
from serverchan import sc_send_summary
from timer import Timer
from job import UpdateMemberJob, JobPool
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

    logger.info(f'Will update {len(mids)} members info.')

    # put mid into queue
    mid_queue: Queue[int] = Queue()
    for mid in mids:
        mid_queue.put(mid)
    # one sentinel per worker (UpdateMemberJob is sentinel-terminated)
    # 50 workers (was 20): each member spends most of its ~25s parked in the
    # member-card anti-crawler 60s sleep, so more workers just means more
    # members sleeping in parallel -- worth trying to cut the multi-hour runtime.
    # (The proper fix for the anti-crawler is a separate, thornier change.)
    job_num = 50
    for _ in range(job_num):
        mid_queue.put(None)
    logger.info(f'{len(mids)} mids put into queue.')

    # JobPool gives a per-30s PROGRESS heartbeat over the multi-hour run
    # (previously blind) and merges the workers' stats.
    pool = JobPool(
        [UpdateMemberJob(f'job_{i}', mid_queue, service) for i in range(job_num)],
        progress_total=len(mids),
        progress_label='member-update',
        progress_interval_s=30.0,  # very slow job (~25s/member) -- 30s is plenty
        logger_name=script_id)
    pool.start()
    logger.info(f'{job_num} job(s) started.')
    job_stat_merged = pool.join()

    session.close()

    timer.stop()

    # summary
    logger.info(f'Finish {script_fullname}!')
    logger.info(timer.get_summary())
    logger.info(job_stat_merged.get_summary('member-update'))
    sc_send_summary(script_fullname, timer, job_stat_merged)


def main():
    update_member_info()


if __name__ == '__main__':
    logging_init(file_prefix=script_id)
    main()
