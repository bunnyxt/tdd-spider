import schedule
import threading
import time
from logger import logger_15
from db import DBOperation, Session
from pybiliapi import BiliApi
from common import update_video, TddCommonError
from serverchan import sc_send
from util import get_ts_s, ts_s_to_str


def regularly_update_video_info():
    logger_15.info('Now start update video info...')
    start_ts = get_ts_s()  # get start ts

    session = Session()
    bapi = BiliApi()

    week_num = int(time.strftime('%w', time.localtime(time.time())))
    size = 40000
    offset = week_num * size
    logger_15.info('Week %d, go fetch aids, %d ~ %s' % (week_num, offset + 1, offset + size))

    aids = DBOperation.query_video_aids(offset, size, session)
    logger_15.info('% aids got' % len(aids))

    total_count = len(aids)
    tdd_common_error_count = 0
    other_exception_count = 0
    no_update_count = 0
    change_count = 0
    change_log_count = 0

    for aid in aids:
        try:
            tdd_video_logs = update_video(aid, bapi, session)
        except TddCommonError as e:
            logger_15.error(e)
            tdd_common_error_count += 1
        except Exception as e:
            logger_15.error(e)
            other_exception_count += 1
        else:
            if len(tdd_video_logs) == 0:
                no_update_count += 1
            else:
                change_count += 1
            for log in tdd_video_logs:
                logger_15.info('%d, %s, %s, %s' % (log.aid, log.attr, log.oldval, log.newval))
                change_log_count += 1
            logger_15.debug('Finish update video info aid %d' % aid)
        time.sleep(0.2)  # avoid ban ip

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

    logger_15.info('Finish update video info!')
    logger_15.warning(summary)

    # send sc
    sc_result = sc_send('Finish update video info!', summary)
    if sc_result['errno'] == 0:
        logger_15.info('Sc summary sent: succeed!')
    else:
        logger_15.warning('Sc summary sent: failed! sc_result = %s.' % sc_result)

    session.close()


def regularly_update_video_info_task():
    threading.Thread(target=regularly_update_video_info).start()


def main():
    logger_15.info('15: regularly update video info')
    schedule.every().day.at('00:00').do(regularly_update_video_info_task)

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    main()
