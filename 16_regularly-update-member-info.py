import schedule
import threading
import time
from logger import logger_16
from db import DBOperation, Session
from pybiliapi import BiliApi
from common import update_member, TddCommonError
from serverchan import sc_send
from util import get_ts_s, ts_s_to_str


def regularly_update_member_info():
    logger_16.info('Now start update member info...')
    start_ts = get_ts_s()  # get start ts

    session = Session()
    bapi = BiliApi()

    # week_num = int(time.strftime('%w', time.localtime(time.time())))
    # size = 5000
    # offset = week_num * size
    # logger_16.info('Week %d, go fetch aids, %d ~ %s' % (week_num, offset + 1, offset + size))

    # get all mids
    size = 40000
    offset = 0

    mids = DBOperation.query_member_mids(offset, size, session)
    logger_16.info('%d mids got' % len(mids))

    total_count = len(mids)
    tdd_common_error_count = 0
    other_exception_count = 0
    no_update_count = 0
    change_count = 0
    change_log_count = 0

    for mid in mids:
        try:
            tdd_member_logs = update_member(mid, bapi, session)
        except TddCommonError as e:
            logger_16.error(e)
            tdd_common_error_count += 1
        except Exception as e:
            logger_16.error(e)
            other_exception_count += 1
        else:
            if len(tdd_member_logs) == 0:
                no_update_count += 1
            else:
                change_count += 1
            for log in tdd_member_logs:
                logger_16.info('%d, %s, %s, %s' % (log.mid, log.attr, log.oldval, log.newval))
                change_log_count += 1
            logger_16.debug('Finish update member info mid %d' % mid)
        time.sleep(0.2)  # avoid ban ip

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

    logger_16.info('Finish update member info!')
    logger_16.warning(summary)

    # send sc
    sc_result = sc_send('Finish update member info!', summary)
    if sc_result['errno'] == 0:
        logger_16.info('Sc summary sent: succeed!')
    else:
        logger_16.warning('Sc summary sent: failed! sc_result = %s.' % sc_result)

    session.close()


def regularly_update_member_info_task():
    threading.Thread(target=regularly_update_member_info).start()


def main():
    logger_16.info('16: regularly update member info')
    schedule.every().day.at('00:00').do(regularly_update_member_info_task)

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    main()
