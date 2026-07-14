from db import DBOperation, Session
from service import Service
from serverchan import sc_send_summary
from util import logging_init, get_week_day, fullname, b2a
from timer import Timer
from queue import Queue
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
    job_num = 20
    for _ in range(job_num):
        aid_queue.put(None)
    logger.info(f'{len(bvids)} aids put into queue.')

    # create jobs
    job_list = []
    for i in range(job_num):
        job_list.append(UpdateVideoJob(f'job_{i}', aid_queue, service))

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
    update_video_info()


if __name__ == '__main__':
    logging_init(file_prefix=script_id)
    main()
