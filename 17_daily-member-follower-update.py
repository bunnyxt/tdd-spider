import schedule
from logger import logger_17
import time
import threading
from db import Session, DBOperation
from pybiliapi import BiliApi
from common import add_member_follower_record_via_relation_api, TddCommonError
from serverchan import sc_send
from util import get_ts_s, ts_s_to_str


def daily_member_follower_update():
    logger_17.info('Now start daily member follower update...')

    session = Session()
    bapi = BiliApi()

    # get member mids
    mids = DBOperation.query_all_member_mids(session)
    logger_17.info('Get %d mids.' % (len(mids)))

    total_mids_count = len(mids)
    added_mids_count = 0
    fail_mids = []

    start_ts = get_ts_s()  # get start ts

    for mid in mids:
        # add member follower record
        try:
            new_member_follower_record = add_member_follower_record_via_relation_api(mid, bapi, session)
        except TddCommonError as e:
            logger_17.warning(e)
            fail_mids.append(mid)
        else:
            logger_17.info('Add new record %s' % new_member_follower_record)
            # logger_17.debug('Add new record %s' % new_member_follower_record)
            added_mids_count += 1

        time.sleep(0.2)  # api duration banned

    # fail aids, need further check
    logger_17.warning('Fail mids: %s' % fail_mids)

    # get finish ts
    finish_ts = get_ts_s()

    # make summary
    summary = \
        '17 daily member follower update done\n\n' + \
        'start: %s, finish: %s, timespan: %ss\n\n' \
        % (ts_s_to_str(start_ts), ts_s_to_str(finish_ts), (finish_ts - start_ts)) + \
        'total mids count: %d\n\n' % total_mids_count + \
        'added mids count: %d\n\n' % added_mids_count + \
        'fail mids: %s, count: %d\n\n' % (fail_mids, len(fail_mids)) + \
        'by.bunnyxt, %s' % ts_s_to_str(get_ts_s())

    logger_17.info('Finish daily member follower update!')

    logger_17.warning(summary)

    # send sc
    sc_result = sc_send('Finish daily member follower update!', summary)
    if sc_result['errno'] == 0:
        logger_17.info('Sc summary sent successfully.')
    else:
        logger_17.warning('Sc summary sent wrong. sc_result = %s.' % sc_result)

    session.close()


def daily_member_follower_update_task():
    threading.Thread(target=daily_member_follower_update).start()


def main():
    logger_17.info('17: daily member follower update')
    # daily_member_follower_update()  # debug
    schedule.every().day.at("04:00").do(daily_member_follower_update_task)

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    main()
