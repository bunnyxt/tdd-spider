from db import Session
from timer import Timer
from job import AddSprintVideoRecordJob
from service import Service
from serverchan import sc_send
from util import get_ts_s, ts_s_to_str
from queue import Queue
from logutils import logging_init
import logging

logger = logging.getLogger('71')


def add_sprint_video_record():
    logger.info('Now start add sprint video record...')
    timer = Timer()
    timer.start()  # start timer

    session = Session()
    service = Service(mode='worker')

    # load processing video aids from db
    result = session.execute('select aid from tdd_sprint_video where status = "processing"')
    aids = [r['aid'] for r in result]
    logger.info(f'Total {len(aids)} videos got.')

    # put aid into queue
    aid_queue: Queue[int] = Queue()
    for aid in aids:
        aid_queue.put(aid)
    logger.info(f'{aid_queue.qsize()} aids put into queue.')

    # create job
    job = AddSprintVideoRecordJob('job', aid_queue, service)

    # start job
    job.start()

    # wait for job
    job.join()

    # collect statistic
    job_stat = job.stat

    timer.stop()  # stop timer

    # make summary
    summary = \
        '# add sprint video record done!\n\n' \
        f'{timer.get_summary()}\n\n' \
        f'{job_stat.get_summary()}\n\n' \
        f'by bunnyxt, {ts_s_to_str(get_ts_s())}'

    logger.info('Finish add sprint video record!')
    logger.warning(summary)

    # send sc
    if job_stat.condition['exception'] > 0 \
            or job_stat.condition['million_exception'] > 0 \
            or job_stat.condition['million_success'] > 0:
        sc_send('Finish add sprint video record!', summary)

    session.close()


def main():
    add_sprint_video_record()


if __name__ == '__main__':
    logging_init(file_prefix='71')
    main()
