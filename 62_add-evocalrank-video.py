import argparse
import datetime
import logging
import re
from db import Session
from service import Service
from util import logging_init, fullname
import requests
from timer import Timer
from queue import Queue
from job import AddVideoJob, JobStat
from serverchan import sc_send_summary

script_id = '62'
script_name = 'add-evocalrank-video'
script_fullname = fullname(script_id, script_name)
logger = logging.getLogger(script_id)


def calculate_ranknum_from_date() -> int:
    """
    Calculate ranknum number based on current date.
    Reference: week of 2025-12-17 (Monday to Sunday) -> 697
    """
    # Reference date: 2025-12-17 (Monday of week 697)
    reference_date = datetime.date(2025, 12, 17)

    # Get Monday of the reference week
    reference_monday = reference_date - \
        datetime.timedelta(days=reference_date.weekday())

    # Get current date and find Monday of current week
    current_date = datetime.date.today()
    current_monday = current_date - \
        datetime.timedelta(days=current_date.weekday())

    # Calculate weeks difference
    weeks_diff = (current_monday - reference_monday).days // 7

    # Ranknum = 697 + weeks difference
    ranknum = 697 + weeks_diff

    return ranknum


def add_evocalrank_video(ranknum: int):
    logger.info(f'Now start {script_fullname}...')
    timer = Timer()
    timer.start()

    service = Service(mode="worker", retry=20)

    logger.info(f'Will add evocalrank video with ranknum: {ranknum}')

    # load evocalrank json, min ranknum is 520
    logger.info(f'Loading evocalrank data...')
    url = f'https://www.evocalrank.com/data/rank_data/{ranknum}.json'
    try:
        response = requests.get(url)
        response.raise_for_status()
        evocalrank_data = response.json()
    except Exception as e:
        logger.critical(
            f'Failed to get evocalrank data from {url}, error: {e}')
        exit(1)
    logger.info(f'Evocalrank data loaded successfully!')

    # load avid from evocalrank_data
    logger.info(f'Now extract avid from evocalrank data...')
    aid_list: list[int] = []
    rank_name_list = [
        'main_rank',
        'second_rank',
        'super_hit',
        'pick_up',
        'oth_pickup', 'Vocaloid_pick_up',
        'history-1-year',
        'history-10-year',
        'ed',
        'op'
    ]
    for rank_name in rank_name_list:
        logger.info(f'Now extract aid from {rank_name}...')
        if rank_name not in evocalrank_data:
            logger.warning(
                f'Rank name {rank_name} not found in evocalrank data')
            continue
        rank_data = evocalrank_data[rank_name]
        aid_count = 0
        for item in rank_data:
            if 'avid' not in item:
                logger.warning(
                    f'Item missing "avid" property in {rank_name}: {item}')
                continue
            avid_str = item['avid']
            # Validate format: av + digits (at least one)
            match = re.match(r'^av(\d+)$', avid_str, re.IGNORECASE)
            if match:
                try:
                    aid = int(match.group(1))
                    # Validate aid > 0 and fits in signed bigint (64-bit: -2^63 to 2^63-1)
                    MAX_SIGNED_BIGINT = 9223372036854775807  # 2^63 - 1
                    if aid <= 0:
                        logger.warning(
                            f'Invalid aid: {aid} (must be > 0), from avid: {avid_str}')
                    elif aid > MAX_SIGNED_BIGINT:
                        logger.warning(
                            f'Invalid aid: {aid} (exceeds signed bigint max: {MAX_SIGNED_BIGINT}), from avid: {avid_str}')
                    else:
                        aid_list.append(aid)
                        aid_count += 1
                except ValueError as e:
                    logger.warning(
                        f'Failed to convert avid digits to int: {avid_str}, error: {e}')
            else:
                logger.warning(
                    f'Failed to extract aid from avid: {avid_str}, format invalid (expected: av+digits)')
        logger.info(
            f'Extract {aid_count} aids from {rank_name}, rank data length: {len(rank_data)}')
    logger.info(f'Total {len(aid_list)} aids extracted from evocalrank data!')
    aid_list = list(set(aid_list))
    logger.info(f'Total {len(aid_list)} aids after deduplication: {aid_list}')

    # put aid into queue
    aid_queue: Queue[int] = Queue()
    for aid in aid_list:
        aid_queue.put(aid)

    # create jobs
    job_num = 50
    job_list: list[AddVideoJob] = []
    for i in range(job_num):
        job_list.append(AddVideoJob(
            f'job_{i}', aid_queue, service))

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

    logger.info('Finish add evocalrank video!')
    logger.info(job_stat_merged.get_summary())

    # send sc
    sc_send_summary(
        f'{script_fullname}.add_evocalrank_video', timer, job_stat_merged)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--ranknum', '-r', type=int, required=False,
                        help='Ranknum number of evocalrank video. If not provided, will be calculated based on current date.')
    args = parser.parse_args()

    if args.ranknum is not None:
        ranknum = args.ranknum
    else:
        ranknum = calculate_ranknum_from_date()
        logger.info(f'Ranknum not provided, calculated from date: {ranknum}')

    add_evocalrank_video(ranknum)


if __name__ == '__main__':
    logging_init(file_prefix=script_id)
    main()
