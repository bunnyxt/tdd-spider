from db import DBOperation, Session
from service import Service
from common.error import TddError
from task import update_video
from serverchan import sc_send
from util import get_ts_s, ts_s_to_str, get_week_day, b2a, format_ts_s
from logutils import logging_init
import logging

logger = logging.getLogger('15')


def update_video_info():
    logger.info('Now start update video info...')
    start_ts = get_ts_s()  # get start ts

    session = Session()
    service = Service(mode='worker')

    all_bvids = DBOperation.query_all_video_bvids(session)
    logger.info(f'Total {len(all_bvids)} videos got.')

    # add latest 5000 bvids first
    bvids = all_bvids[-5000:]

    # for the rest, add 1 / 7 of them, according to the week day (0-6)
    week_day = get_week_day()
    for idx, bvid in enumerate(all_bvids[:-5000]):
        if idx % 7 == week_day:
            bvids.append(bvid)

    logger.info(f'Will update {len(bvids)} videos info.')

    total_count = len(bvids)
    tdd_error_count = 0
    other_exception_count = 0
    no_update_count = 0
    change_count = 0
    change_log_count = 0

    for idx, bvid in enumerate(bvids, 1):
        try:
            tdd_video_logs = update_video(b2a(bvid), service, session)
        except TddError as e:
            logger.warning(f'Fail to update video info. bvid: {bvid}, error: {e}')
            tdd_error_count += 1
        except Exception as e:
            logger.warning(f'Fail to update video info. bvid: {bvid}, error: {e}')
            other_exception_count += 1
        else:
            if len(tdd_video_logs) == 0:
                no_update_count += 1
            else:
                change_count += 1
            for log in tdd_video_logs:
                logger.info(f'Update video info. bvid: {bvid}, attr: {log.attr}, {log.oldval} -> {log.newval}')
                change_log_count += 1
            logger.debug(f'Finish update video info. bvid: {bvid}')
        finally:
            if idx % 1000 == 0:
                logger.info(f'{idx} / {total_count} done')
    logger.info(f'{total_count} / {total_count} done')

    # get end ts
    end_ts = get_ts_s()

    # make summary
    summary = \
        'update video info done!\n\n' \
        f'start: {ts_s_to_str(start_ts)}, ' \
        f'end: {ts_s_to_str(end_ts)}, ' \
        f'cost: {format_ts_s(end_ts - start_ts)}\n\n' \
        f'total count: {total_count}\n\n' + \
        f'tdd error count: {tdd_error_count}\n\n' + \
        f'other exception count: {other_exception_count}\n\n' + \
        f'no update count: {no_update_count}\n\n' + \
        f'change count: {change_count}\n\n' + \
        f'change log count: {change_log_count}\n\n' + \
        f'by.bunnyxt, {ts_s_to_str(get_ts_s())}'

    logger.info('Finish update video info!')
    logger.warning(summary)

    # send sc
    sc_result = sc_send('Finish update video info!', summary)
    if sc_result['errno'] == 0:
        logger.info('Sc summary sent: succeed!')
    else:
        logger.warning(f'Sc summary sent: failed! sc_result = {sc_result}.')

    session.close()


def main():
    update_video_info()


if __name__ == '__main__':
    logging_init(file_prefix='15')
    main()
