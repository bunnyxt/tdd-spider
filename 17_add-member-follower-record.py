from db import DBOperation, Session
from service import Service
from util import logging_init, fullname
from serverchan import sc_send_summary
from queue import Queue
from timer import Timer
from job import FetchMemberFollowerRecordJob, BatchInsertMemberFollowerRecordJob, JobPool
import logging

script_id = '17'
script_name = 'add-member-follower-record'
script_fullname = fullname(script_id, script_name)
logger = logging.getLogger(script_id)


def add_member_follower_record():
    logger.info(f'Now start {script_fullname}...')
    timer = Timer()
    timer.start()

    # fetch/write split (mirrors 51_): many fetch-only workers (HTTP, no DB) ->
    # bounded queue -> ONE batch writer. Replaces the old 20 per-worker-insert
    # jobs, whose per-record commits contended on fsync and pinned the run at
    # ~1h10m. 50 fetchers + batched inserts should finish it in well under the
    # ~40 min before 51_'s 12:00 run -- it runs to completion (no duration cap).
    job_num = 50

    session = Session()
    service = Service(mode='worker', pool_maxsize=job_num + 32)

    # load all mids
    mids: list[int] = DBOperation.query_all_member_mids(session=session)
    session.close()
    logger.info(f'Total {len(mids)} members got.')

    # mid queue for the fetch pool
    mid_queue: Queue[int] = Queue()
    for mid in mids:
        mid_queue.put(mid)

    # fetched records -> single batch writer. BOUNDED so fetchers apply
    # backpressure at writer speed instead of growing RSS.
    record_queue: Queue = Queue(maxsize=20000)

    fetch_pool = JobPool(
        [FetchMemberFollowerRecordJob(f'job_{i}', mid_queue, record_queue, service)
         for i in range(job_num)],
        progress_total=len(mids),
        progress_label='follower-fetch',
        ensure_conditions=['success', 'exception', 'other_exception',
                           'record_dropped_queue_full'],
        logger_name=script_id)
    writer_pool = JobPool(
        [BatchInsertMemberFollowerRecordJob('writer_0', record_queue)],
        progress_total=len(mids),
        progress_label='follower-db-writer',
        progress_interval_s=5.0,
        ensure_conditions=['batch_insert', 'batch_insert_split',
                           'batch_insert_split_ok', 'batch_insert_fail'],
        logger_name=script_id)

    fetch_pool.start()
    writer_pool.start()

    fetch_stat = fetch_pool.join()
    # sentinel AFTER all fetchers finished (FIFO -> arrives behind every record)
    record_queue.put(None)
    writer_stat = writer_pool.join()

    timer.stop()

    # summary
    logger.info(f'Finish {script_fullname}!')
    logger.info(timer.get_summary())
    logger.info(fetch_stat.get_summary('follower-fetch'))
    logger.info(writer_stat.get_summary('follower-db-writer'))
    logger.info(f'{writer_stat.total_count} follower record(s) fetched and inserted.')
    sc_send_summary(script_fullname, timer, fetch_stat)


def main():
    add_member_follower_record()


if __name__ == '__main__':
    logging_init(file_prefix=script_id)
    main()
