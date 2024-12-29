from serverchan import sc_send
from task import add_video, commit_video_record_via_newlist_archive_stat
import math
from util import get_ts_s, ts_s_to_str, format_ts_s, logging_init, fullname
from service import Service
from collections import defaultdict
from db import Session
import logging

script_id = '01'
script_name = 'add-all-videos-with-tid-30'
script_fullname = fullname(script_id, script_name)
logger = logging.getLogger(script_id)


def add_all_video_with_tid_30():
    # NOTE: NOT TESTED
    # due to the bug of newlist api, will miss about 20% video
    logger.info(f'Now start {script_fullname}...')
    start_ts = get_ts_s()  # get start ts

    session = Session()
    service = Service(mode='worker')

    # prepare statistics
    statistics: defaultdict[str, int] = defaultdict(int)

    # get page total
    try:
        newlist = service.get_newlist(
            {'rid': 30, 'pn': 1, 'ps': 50})
    except Exception as e:
        logger.critical(
            f'Fail to get newlist. rid: 30, pn: 1, ps: 50, error: {e}')
        exit(1)
    page_total = math.ceil(newlist.page.count / 50)
    logging.info(f'Found {page_total} page(s) in total.')

    # add all video
    page_num = 1
    while page_num <= page_total:
        # get newlist
        try:
            newlist = service.get_newlist(
                {'rid': 30, 'pn': page_num, 'ps': 50})
        except Exception as e:
            logger.error(
                f'Fail to get newlist. rid: 30, pn: {page_num}, ps: 50, error: {e}')
            statistics['other_exception_count'] += 1
            page_num += 1
            continue

        for archive in newlist.archives:
            # add video
            try:
                new_video = add_video(
                    archive.aid, service, session, commit_video_record=False)
            except Exception as e:
                logger.error(
                    f'Fail to add video parsed from archive! archive: {archive}, error: {e}')
                statistics['other_exception_count'] += 1
            else:
                logger.info(
                    f'New video parsed from archive added! video: {new_video}')
                statistics['total_count'] += 1
                # commit video record via archive stat
                try:
                    new_video_record = commit_video_record_via_newlist_archive_stat(
                        archive.stat, session)
                except Exception as e:
                    logger.error(f'Fail to add video record parsed from archive stat! '
                                 f'archive: {archive}, error: {e}')
                    statistics['other_exception_count'] += 1
                else:
                    logger.info(f'New video record parsed from archive stat committed! '
                                f'video record: {new_video_record}')

        logger.debug(f'Archive page {page_num} done.')
        page_num += 1

    # get end ts
    end_ts = get_ts_s()

    # make summary
    summary = \
        'add all video with tid 30 done!\n\n' \
        f'start: {ts_s_to_str(start_ts)}, ' \
        f'end: {ts_s_to_str(end_ts)}, ' \
        f'cost: {format_ts_s(end_ts - start_ts)}\n\n' \
        f'total count: {statistics["total_count"]}\n\n' + \
        f'other exception count: {statistics["other_exception_count"]}\n\n' \
        f'by.bunnyxt, {ts_s_to_str(get_ts_s())}'

    logger.info('Finish add all video with tid 30!')
    logger.warning(summary)

    # send sc
    sc_send('Finish add all video with tid 30!', summary)

    session.close()


def main():
    add_all_video_with_tid_30()


if __name__ == '__main__':
    logging_init(file_prefix=script_id)
    main()
