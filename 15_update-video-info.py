from db import DBOperation, Session
from pybiliapi import BiliApi
from common import update_video_via_bvid, TddCommonError
from serverchan import sc_send
from util import get_ts_s, ts_s_to_str, get_week_day
from conf import get_proxy_pool_url
from logutils import logging_init
import logging
logger = logging.getLogger('15')


def update_video_info():
    logger.info('Now start update video info...')
    start_ts = get_ts_s()  # get start ts

    session = Session()
    bapi_with_proxy = BiliApi(get_proxy_pool_url())

    all_bvids = DBOperation.query_all_video_bvids(session)
    logger.info('%d all bvids got' % len(all_bvids))

    # add latest 5000 bvids first
    bvids = all_bvids[-5000:]

    # for the rest, add 1 / 7 of them, according to the week day (0-6)
    week_day = get_week_day()
    for i, bvid in enumerate(all_bvids[:-5000]):
        if i % 7 == week_day:
            bvids.append(bvid)

    logger.info('will update %d videos info' % len(bvids))

    total_count = len(bvids)
    tdd_common_error_count = 0
    other_exception_count = 0
    no_update_count = 0
    change_count = 0
    change_log_count = 0

    for i, bvid in enumerate(bvids, 1):
        try:
            tdd_video_logs = update_video_via_bvid(bvid, bapi_with_proxy, session)
        except TddCommonError as e:
            logger.error('Fail to update video info bvid %s, TddCommonError Detail: %s' % (bvid, e))
            tdd_common_error_count += 1
        except Exception as e:
            logger.error('Fail to update video info bvid %s, Exception Detail: %s' % (bvid, e))
            other_exception_count += 1
        else:
            if len(tdd_video_logs) == 0:
                no_update_count += 1
            else:
                change_count += 1
            for log in tdd_video_logs:
                logger.info('%s, %s, %s, %s' % (log.bvid, log.attr, log.oldval, log.newval))
                change_log_count += 1
            logger.debug('Finish update video info bvid %s' % bvid)
        finally:
            if i % 1000 == 0:
                logger.info('%d / %d done' % (i, total_count))
    logger.info('%d / %d done' % (total_count, total_count))

    # get finish ts
    finish_ts = get_ts_s()

    # make summary
    summary = \
        'update video info done!\n\n' + \
        'start: %s, finish: %s, timespan: %ss\n\n' \
        % (ts_s_to_str(start_ts), ts_s_to_str(finish_ts), (finish_ts - start_ts)) + \
        'total count: %d\n\n' % total_count + \
        'tdd common error count: %d\n\n' % tdd_common_error_count + \
        'other exception count: %d\n\n' % other_exception_count + \
        'no update count: %d\n\n' % no_update_count + \
        'change count: %d\n\n' % change_count + \
        'change log count: %d\n\n' % change_log_count + \
        'by.bunnyxt, %s' % ts_s_to_str(get_ts_s())

    logger.info('Finish update video info!')
    logger.warning(summary)

    # send sc
    sc_result = sc_send('Finish update video info!', summary)
    if sc_result['errno'] == 0:
        logger.info('Sc summary sent: succeed!')
    else:
        logger.warning('Sc summary sent: failed! sc_result = %s.' % sc_result)

    session.close()


def main():
    update_video_info()


if __name__ == '__main__':
    logging_init(file_prefix='15')
    main()
