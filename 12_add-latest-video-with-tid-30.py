from service import Service
from serverchan import sc_send_summary
from timer import Timer
from job import AddLatestVideoJob
from util import logging_init, fullname
import logging

script_id = '12'
script_name = 'add-latest-video-with-tid-30'
script_fullname = fullname(script_id, script_name)
logger = logging.getLogger(script_id)


def add_latest_video_with_tid_30():
    logger.info(f'Now start {script_fullname}...')
    timer = Timer()
    timer.start()

    service = Service(mode='worker')

    # create job
    job = AddLatestVideoJob('job_tid_30', 30, service)

    # start job
    job.start()

    # wait for job
    job.join()

    # collect statistic
    job_stat = job.stat

    timer.stop()

    # summary
    logger.info(f'Finish {script_fullname}!')
    logger.info(timer.get_summary())
    logger.info(job_stat.get_summary())
    if job_stat.condition['get_archive_exception'] > 0 \
            or job_stat.condition['add_video_exception'] > 0 \
            or job_stat.condition['commit_video_record_exception'] > 0:
        sc_send_summary(script_fullname, timer, job_stat)


def main():
    add_latest_video_with_tid_30()


if __name__ == '__main__':
    logging_init(file_prefix=script_id)
    main()
