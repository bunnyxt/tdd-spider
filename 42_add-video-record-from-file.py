import argparse
from serverchan import sc_send_summary
from job import AddVideoRecordJob
from util import logging_init, fullname
from timer import Timer
from service import Service
from db import TddVideoRecord
from job import JobStat
from queue import Queue
import logging

script_id = '42'
script_name = 'add-video-record-from-file'
script_fullname = fullname(script_id, script_name)
logger = logging.getLogger(script_id)


def add_video_record_from_file(file_path: str):
    logger.info(f'Now start {script_fullname}...')

    timer = Timer()
    timer.start()

    service = Service(mode='worker')

    # read file
    aid_list: list[int] = []
    with open(file_path, 'r') as f:
        for line in f:
            try:
                aid = line.strip()
                if aid.isdigit():
                    aid_list.append(int(aid))
                else:
                    logger.error(
                        f'Failed to parse aid from line: {line}, not a number.')
            except Exception as e:
                logger.error(
                    f'Failed to parse aid from line: {line}, error: {e}')
    logger.info(f'{len(aid_list)} aid(s) found in {file_path}.')

    # put aid into queue
    aid_queue: Queue[int] = Queue()
    for aid in aid_list:
        aid_queue.put(aid)

    # create video record queue
    video_record_queue: Queue[TddVideoRecord] = Queue()

    # create jobs
    job_num = 50
    job_list: list[AddVideoRecordJob] = []
    for i in range(job_num):
        job_list.append(AddVideoRecordJob(
            f'job_{i}', aid_queue, video_record_queue, service, duration_limit_s=60*60*12))

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

    timer.stop()

    logger.info('Finish add video record from file!')
    logger.info(job_stat_merged.get_summary())

    # send sc
    sc_send_summary(
        f'{script_fullname}.add_video_record_from_file', timer, job_stat_merged)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--file', '-f', type=str, required=True,
                        help='Path to the file containing aid(s), one per line.')
    args = parser.parse_args()

    add_video_record_from_file(args.file)


if __name__ == '__main__':
    logging_init(file_prefix=script_id)
    main()
