from service import Service
from serverchan import sc_send
from task import add_video, commit_video_record_via_archive_stat, AlreadyExistError
from collections import defaultdict
from db import Session
from util import get_ts_s, ts_s_to_str, format_ts_s
from logutils import logging_init
import logging

logger = logging.getLogger('12')


def add_new_video_with_tid_30():
    logger.info('Now start add new video with tid 30...')
    start_ts = get_ts_s()  # get start ts

    session = Session()
    service = Service(mode='worker')

    # prepare statistics
    statistics: defaultdict[str, int] = defaultdict(int)

    # add new video
    page_num = 1
    while page_num <= 3:  # check latest 3 page
        # get archive rank by partion
        try:
            archive_rank_by_partion = service.get_archive_rank_by_partion({'tid': 30, 'pn': page_num, 'ps': 50})
        except Exception as e:
            logger.error(f'Fail to get archive rank by partion. tid: 30, pn: {page_num}, ps: 50, error: {e}')
            statistics['other_exception_count'] += 1
            page_num += 1
            continue

        for archive in archive_rank_by_partion.archives:
            # add video
            try:
                new_video = add_video(archive.aid, service, session)
            except AlreadyExistError:
                logger.debug(f'Video parsed from archive already exist! archive: {archive}')
            except Exception as e:
                logger.warning(f'Fail to add video parsed from archive! archive: {archive}, error: {e}')
                statistics['other_exception_count'] += 1
            else:
                logger.info(f'New video parsed from archive added! video: {new_video}')
                statistics['total_count'] += 1
                # commit video record via archive stat
                try:
                    new_video_record = commit_video_record_via_archive_stat(archive.stat, new_video.aid)
                except Exception as e:
                    logger.warning(f'Fail to add video record parsed from archive stat! '
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
        'add new video with tid 30 done!\n\n' \
        f'start: {ts_s_to_str(start_ts)}, ' \
        f'end: {ts_s_to_str(end_ts)}, ' \
        f'cost: {format_ts_s(end_ts - start_ts)}\n\n' \
        f'total count: {statistics["total_count"]}\n\n' + \
        f'other exception count: {statistics["other_exception_count"]}\n\n' \
        f'by.bunnyxt, {ts_s_to_str(get_ts_s())}'

    logger.info('Finish add new video with tid 30!')
    logger.warning(summary)

    # send sc
    if statistics["other_exception_count"] > 0:
        sc_send('Finish add new video with tid 30!', summary)

    session.close()


def main():
    add_new_video_with_tid_30()


if __name__ == '__main__':
    logging_init(file_prefix='12')
    main()
