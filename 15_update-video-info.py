from db import DBOperation, Session
from service import Service
from serverchan import sc_send_summary
from util import logging_init, get_week_day, fullname, b2a
from timer import Timer
from queue import Queue
from job import UpdateVideoJob, JobPool
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
    all_bvids: list[str] = DBOperation.query_all_video_bvids(session)
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

    # put aid into queue. UpdateVideoJob takes aids (it used to take bvids and
    # b2a them itself); converting here keeps the job free of that concern and
    # lets 51_ feed it aids straight from the fetch pipeline.
    aid_queue: Queue[int] = Queue()
    for bvid in bvids:
        aid_queue.put(b2a(bvid))
    # one sentinel per worker: UpdateVideoJob is now sentinel-terminated (an
    # empty queue means "wait", so it can also run alongside a live producer in
    # 51_). This also retires the old `while not queue.empty(): queue.get()`
    # loop, which races -- two workers both see a non-empty queue, both call
    # get(), and the loser blocks forever on the last item.
    # 50 workers (was 20): this is worker-bound with inbound headroom (the
    # full-view responses are large but the downlink is fat), so more workers
    # cut the multi-hour runtime roughly proportionally. It hits the full-view
    # Lambda -- a different endpoint than 51_'s trimmed one -- and sends little
    # outbound per request, so it does not meaningfully slow 51_'s hourly runs
    # even when it overruns. No duration cap: 15_ runs to completion.
    job_num = 50
    for _ in range(job_num):
        aid_queue.put(None)  # one sentinel per worker (sentinel-terminated)
    logger.info(f'{len(bvids)} aids put into queue.')

    # JobPool gives a per-interval PROGRESS heartbeat over the multi-hour run
    # (previously blind) and merges the workers' stats.
    pool = JobPool(
        [UpdateVideoJob(f'job_{i}', aid_queue, service) for i in range(job_num)],
        progress_total=len(bvids),
        progress_label='video-update',
        progress_interval_s=10.0,  # slow long job -- 10s keeps the log readable
        ensure_conditions=['0_update', '1_update', 'update_exception'],
        logger_name=script_id)
    pool.start()
    logger.info(f'{job_num} job(s) started.')
    job_stat_merged = pool.join()

    session.close()

    timer.stop()

    # summary
    logger.info(f'Finish {script_fullname}!')
    logger.info(timer.get_summary())
    logger.info(job_stat_merged.get_summary('video-update'))
    sc_send_summary(script_fullname, timer, job_stat_merged)


def main():
    update_video_info()


if __name__ == '__main__':
    logging_init(file_prefix=script_id)
    main()
