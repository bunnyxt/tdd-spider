from db import DBOperation, Session
from service import Service
from serverchan import sc_send
from util import get_ts_s, ts_s_to_str, get_week_day, format_ts_s, format_ts_ms
from queue import Queue
from collections import defaultdict
from typing import List
from job import UpdateVideoJob
from logutils import logging_init
import logging

logger = logging.getLogger('15')


def update_video_info():
    logger.info('Now start update video info...')
    start_ts = get_ts_s()  # get start ts

    session = Session()
    service = Service(mode='worker')

    # get all bvids
    all_bvids: List[str] = DBOperation.query_all_video_bvids(session)
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

    # put bvid into queue
    bvid_queue: Queue[str] = Queue()
    for bvid in bvids:
        bvid_queue.put(bvid)
    logger.info(f'{bvid_queue.qsize()} bvids put into queue.')

    # prepare statistics
    statistics: defaultdict[str, int] = defaultdict(int)

    # create jobs
    job_num = 20
    job_list = []
    for i in range(job_num):
        job_list.append(UpdateVideoJob(f'job_{i}', bvid_queue, statistics, service))

    # start jobs
    for job in job_list:
        job.start()
    logger.info(f'{job_num} job(s) started.')

    # wait for jobs
    for job in job_list:
        job.join()

    # get end ts
    end_ts = get_ts_s()

    # make summary
    summary = \
        'update video info done!\n\n' \
        f'start: {ts_s_to_str(start_ts)}, ' \
        f'end: {ts_s_to_str(end_ts)}, ' \
        f'cost: {format_ts_s(end_ts - start_ts)}\n\n' \
        f'total count: {statistics["total_count"]}, ' \
        f'average cost per service: {format_ts_ms(statistics["total_cost_ms"] // statistics["total_count"])}\n\n' \
        f'other exception count: {statistics["other_exception_count"]}\n\n' \
        f'no update count: {statistics["no_update_count"]}\n\n' \
        f'change count: {statistics["change_count"]}\n\n' \
        f'change log count: {statistics["change_log_count"]}\n\n' \
        f'by.bunnyxt, {ts_s_to_str(get_ts_s())}'

    logger.info('Finish update video info!')
    logger.warning(summary)

    # send sc
    sc_send('Finish update video info!', summary)

    session.close()


def main():
    update_video_info()


if __name__ == '__main__':
    logging_init(file_prefix='15')
    main()
