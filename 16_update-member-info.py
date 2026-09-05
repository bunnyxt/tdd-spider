from db import DBOperation, Session
from util import logging_init, get_week_day, fullname
from queue import Queue
from service import Service
from serverchan import sc_send_summary
from timer import Timer
from job import UpdateMemberJob, JobPool
from threading import Event, Thread
import logging

script_id = '16'
script_name = 'update-member-info'
script_fullname = fullname(script_id, script_name)
logger = logging.getLogger(script_id)

# How often the shared Service request counters are dumped while the pool runs.
# The counters are what answer "which API did we hit, how many times, and how
# did those attempts end" without waiting for the run to finish -- this job has
# historically had to be stopped by hand, so a final-only summary is not enough.
REQSTAT_INTERVAL_S = 60.0


def _report_request_stats(service: Service, stop_event: Event) -> None:
    while not stop_event.wait(REQSTAT_INTERVAL_S):
        service.request_stats.log_summary('in-flight')


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
    # Shared Service state keeps rate-limited member-card workers out of the
    # candidate pool. If every worker is limited, each job records that condition
    # and briefly slows down before moving to the next member.
    # 20, not 50: member-card is rate limited well below what 50 concurrent
    # jobs ask for, so the extra concurrency only bought a shorter burst before
    # the whole pool cooled down. See the 2026-09-05 fleet test.
    job_num = 20
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

    stats_stop = Event()
    stats_thread = Thread(target=_report_request_stats,
                          args=(service, stats_stop), daemon=True)
    stats_thread.start()

    job_stat_merged = pool.join()
    stats_stop.set()

    session.close()

    timer.stop()

    # summary
    logger.info(f'Finish {script_fullname}!')
    logger.info(timer.get_summary())
    logger.info(job_stat_merged.get_summary('member-update'))
    service.request_stats.log_summary('final')
    sc_send_summary(script_fullname, timer, job_stat_merged)


def main():
    update_member_info()


if __name__ == '__main__':
    logging_init(file_prefix=script_id)
    main()
