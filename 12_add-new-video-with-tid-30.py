from service import Service
from serverchan import sc_send
from timer import Timer
from job import AddLatestVideoJob
from util import get_ts_s, ts_s_to_str, format_ts_s
from logutils import logging_init
import logging

logger = logging.getLogger('12')


def add_new_video_with_tid_30():
    logger.info('Now start add new video with tid 30...')
    timer = Timer()
    timer.start()  # start timer

    service = Service(mode='worker')

    # create job
    job = AddLatestVideoJob('job_tid_30', 30, service)

    # start job
    job.start()

    # wait for job
    job.join()

    # collect statistic
    job_stat = job.stat

    timer.stop()  # stop timer

    # make summary
    summary = \
        'add new video with tid 30 done!\n\n' \
        f'{timer.get_summary()}\n\n' \
        f'{job_stat.get_summary()}\n\n' \
        f'by bunnyxt, {ts_s_to_str(get_ts_s())}'

    logger.info('Finish add new video with tid 30!')
    logger.warning(summary)

    # send sc
    if job_stat.condition['get_archive_exception'] > 0 \
            or job_stat.condition['add_video_exception'] > 0 \
            or job_stat.condition['commit_video_record_exception'] > 0:
        sc_send('Finish add new video with tid 30!', summary)


def main():
    add_new_video_with_tid_30()


if __name__ == '__main__':
    logging_init(file_prefix='12')
    main()
