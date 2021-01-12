import schedule
import threading
import time
from logutils import logging_init
from db import DBOperation, Session
from pybiliapi import BiliApi
from common import update_member, TddCommonError
from serverchan import sc_send
from util import get_ts_s, ts_s_to_str
from conf import get_proxy_pool_url
import logging
logger = logging.getLogger('16')


def regularly_update_member_info():
    logger.info('Now start update member info...')
    start_ts = get_ts_s()  # get start ts

    session = Session()
    bapi_with_proxy = BiliApi(get_proxy_pool_url())

    # get all mids
    mids = DBOperation.query_all_member_mids(session)
    logger.info('%d mids got' % len(mids))

    total_count = len(mids)
    tdd_common_error_count = 0
    other_exception_count = 0
    no_update_count = 0
    change_count = 0
    change_log_count = 0

    for i, mid in enumerate(mids, 1):
        try:
            tdd_member_logs = update_member(mid, bapi_with_proxy, session)
        except TddCommonError as e:
            logger.error('Fail to update member info mid %d, TddCommonError Detail: %s' % (mid, e))
            tdd_common_error_count += 1
        except Exception as e:
            logger.error('Fail to update member info mid %d, Exception Detail: %s' % (mid, e))
            other_exception_count += 1
        else:
            if len(tdd_member_logs) == 0:
                no_update_count += 1
            else:
                change_count += 1
            for log in tdd_member_logs:
                logger.info('%d, %s, %s, %s' % (log.mid, log.attr, log.oldval, log.newval))
                change_log_count += 1
            logger.debug('Finish update member info mid %d' % mid)
        finally:
            if i % 1000 == 0:
                logger.info('%d / %d done' % (i, total_count))
    logger.info('%d / %d done' % (total_count, total_count))

    # get finish ts
    finish_ts = get_ts_s()

    # make summary
    summary = \
        'update member info done!\n\n' + \
        'start: %s, finish: %s, timespan: %ss\n\n' \
        % (ts_s_to_str(start_ts), ts_s_to_str(finish_ts), (finish_ts - start_ts)) + \
        'total count: %d\n\n' % total_count + \
        'tdd common error count: %d\n\n' % tdd_common_error_count + \
        'other exception count: %d\n\n' % other_exception_count + \
        'no update count: %d\n\n' % no_update_count + \
        'change count: %d\n\n' % change_count + \
        'change log count: %d\n\n' % change_log_count + \
        'by.bunnyxt, %s' % ts_s_to_str(get_ts_s())

    logger.info('Finish update member info!')

    # send sc
    sc_result = sc_send('Finish update member info!', summary)
    if sc_result['errno'] == 0:
        logger.info('Sc summary sent: succeed!')
    else:
        logger.warning('Sc summary sent: failed! sc_result = %s.' % sc_result)

    session.close()


def regularly_update_member_info_task():
    threading.Thread(target=regularly_update_member_info).start()


def main():
    logger.info('16: regularly update member info')
    logger.info('will execute everyday at 00:00')
    schedule.every().day.at('00:00').do(regularly_update_member_info_task)

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    logging_init(file_prefix='16')
    main()
