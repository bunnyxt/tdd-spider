import schedule
from logger import logger_72
import time
import threading
from db import Session
import datetime
from util import get_ts_s
import math


def add_sprint_daily():
    session = Session()
    logger_72.info('now start adding sprint daily...')

    # get now time to format as date
    date = datetime.datetime.now().strftime('%Y%m%d')

    # calc start ts and end ts
    end_ts = int(time.mktime(time.strptime(date, "%Y%m%d"))) + 60 * 60 * 6
    start_ts = end_ts - 60 * 60 * 24

    # get start records
    result = session.execute('select aid, `view` from tdd_sprint_video_record where added >= %d && added < %d' % (
        start_ts, start_ts + 30 * 60
    ))
    start_records = [(r['aid'], r['view']) for r in result]
    start_records_view = {}
    for (aid, view) in start_records:
        if aid not in start_records_view.keys():
            start_records_view[aid] = view
    logger_72.info('get %d start video' % len(start_records_view.keys()))

    # get end records
    result = session.execute('select aid, `view` from tdd_sprint_video_record where added >= %d && added < %d' % (
        end_ts, end_ts + 30 * 60
    ))
    end_records = [(r['aid'], r['view']) for r in result]
    end_records_view = {}
    for (aid, view) in end_records:
        if aid not in end_records_view.keys():
            end_records_view[aid] = view
    logger_72.info('get %d end video' % len(end_records_view.keys()))

    # assemble records
    new_video_aids = []
    million_video_aids = []
    view_incr_total = 0
    video_total = 0
    for aid in set(list(start_records_view.keys())+list(end_records_view.keys())):
        if aid not in start_records_view.keys():
            logger_72.info('new video aid: %d detected' % aid)
            new_video_aids.append(aid)
            result = session.execute('select `view` from tdd_sprint_video_record where aid = %d order by added limit 1' % aid)
            start_view = [r['view'] for r in result][0]
        else:
            start_view = start_records_view[aid]

        if aid not in end_records_view.keys():
            logger_72.info('million video aid: %d detected' % aid)
            million_video_aids.append(aid)
            end_view = 1000000
        else:
            end_view = end_records_view[aid]

        view_incr = end_view - start_view
        view_incr_total += view_incr

        if end_view == 1000000:
            continue

        # calc pday
        added = get_ts_s()
        result = session.execute('select created from tdd_sprint_video where aid = %d' % aid)
        created = [r['created'] for r in result][0]
        pday = math.floor((added - created) / (24 * 60 * 60))

        # calc lday
        lday = math.floor((1000000 - end_view) / view_incr)

        session.execute('insert into tdd_sprint_daily_record (added, `date`, aid, `view`, viewincr, pday, lday) '
                        'values (%d, "%s", %d, %d, %d, %d, %d)' % (added, date, aid, end_view, view_incr, pday, lday))
        session.commit()
        logger_72.info('%d, "%s", %d, %d, %d, %d, %d' % (added, date, aid, end_view, view_incr, pday, lday))

        video_total += 1

    # calc view incr incr
    result = session.execute('select `viewincr` from tdd_sprint_daily order by id desc limit 1')
    last_view_incr = [r['viewincr'] for r in result][0]
    view_incr_incr = view_incr_total - last_view_incr

    # make str
    newvids_str = ''
    for aid in new_video_aids:
        newvids_str += '%d;' % aid
    millvids_str = ''
    for aid in million_video_aids:
        millvids_str += '%d;' % aid

    session.execute('insert into tdd_sprint_daily (added, `date`, correct, vidnum, newvids, millvids, viewincr, viewincrincr, comment) '
                    'values (%d, "%s", 1, %d, "%s", "%s", %d, %d, "")' % (get_ts_s(), date, video_total, newvids_str, millvids_str, view_incr_total, view_incr_incr))
    session.commit()
    logger_72.info('%d, "%s", %d, "%s", "%s", %d, %d' % (get_ts_s(), date, video_total, newvids_str, millvids_str, view_incr_total, view_incr_incr))

    session.close()


def add_sprint_daily_task():
    threading.Thread(target=add_sprint_daily).start()


def main():
    logger_72.info('sprint add sprint daily registered')
    schedule.every().day.at('06:30').do(add_sprint_daily_task)

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    main()
