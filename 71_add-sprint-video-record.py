from db.queries import query_processing_sprint_aids
from job import AddSprintVideoRecordJob
from service import Service
from runrecord import track
from util import logging_init, fullname, get_ts_ms, format_duration_summary
from queue import Queue
import logging

script_id = '71'
script_name = 'add-sprint-video-record'
script_fullname = fullname(script_id, script_name)
logger = logging.getLogger(script_id)


def add_sprint_video_record():
    logger.info(f'Now start {script_fullname}...')
    start_ts_ms = get_ts_ms()

    with track(script_fullname) as recorder:
        service = Service(mode='worker')

        # load processing video aids from db
        aids = query_processing_sprint_aids()
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
        with aid_queue.mutex:
            unfetched_aids = list(aid_queue.queue)
        job_stat.condition['unfetched'] = len(unfetched_aids)

        end_ts_ms = get_ts_ms()

        # run-record metrics (best-effort; a disabled recorder is a no-op)
        recorder.add_job_stat_metrics('sprint-video-record', job_stat)
        recorder.add_api_stat_metrics(service.stats)

        # summary
        logger.info(f'Finish {script_fullname}!')
        logger.info(format_duration_summary(start_ts_ms, end_ts_ms))
        logger.info(job_stat.get_summary())
        service.stats.log_summary(logger)
        logger.info('Sprint results: persisted=%d, db_write_failed=%d, fetch_failed=%d, unfetched=%d.',
                    job_stat.condition['persisted'], job_stat.condition['db_write_failed'],
                    job_stat.condition['fetch_failed'], len(unfetched_aids))
        if unfetched_aids:
            logger.warning('Sprint aids left unfetched: %s', unfetched_aids)
        if (getattr(job, 'error', None) is not None or job_stat.condition['exception']
                or job_stat.condition['persisted'] != job_stat.total_count or unfetched_aids):
            raise RuntimeError('Sprint records were not all confirmed; inspect the separate '
                               'database-write and fetch counts above')


def main():
    add_sprint_video_record()


if __name__ == '__main__':
    logging_init(file_prefix=script_id)
    main()
