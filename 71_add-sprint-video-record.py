from db import Session
from timer import Timer
from job import AddSprintVideoRecordJob
from service import Service
from serverchan import sc_send_summary
from util import logging_init
from queue import Queue
import logging

script_id = '71'
script_name = 'add-sprint-video-record'
logger = logging.getLogger(script_id)


def add_sprint_video_record():
    logger.info(f'Now start {script_id} - {script_name}...')
    timer = Timer()
    timer.start()

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

    session.close()

    timer.stop()

    # summary
    logger.info(f'Finish {script_id} - {script_name}!')
    logger.info(timer.get_summary())
    logger.info(job_stat.get_summary())
    if job_stat.condition['exception'] > 0 \
            or job_stat.condition['million_exception'] > 0 \
            or job_stat.condition['million_success'] > 0:
        sc_send_summary(script_id, script_name, timer, job_stat)


def main():
    add_sprint_video_record()


if __name__ == '__main__':
    logging_init(file_prefix=script_id)
    main()
