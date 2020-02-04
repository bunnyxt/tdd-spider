import schedule
import threading
import time
import math
from logger import logger_14
from db import TddVideoRecord, DBOperation, Session
from pybiliapi import BiliApi
from util import get_ts_s, ts_s_to_str
from common import get_valid, test_archive_rank_by_partion, test_video_view, \
    add_video_record_via_stat_api, add_video_record_via_awesome_stat, InvalidObjCodeError, TddCommonError
from serverchan import sc_send


def add_new_video_record_via_stat_api(aids, time_label):
    logger_14.info('%s Now start add new video record via stat api...' % time_label)
    bapi = BiliApi()
    session = Session()

    aids_len = len(aids)  # aids len
    start_ts = get_ts_s()  # get start ts

    fail_aids = []  # aids fail to get valid stat obj
    code_changed_count = 0  # video code changed count
    added_count = 0  # aids added count

    for aid in aids:
        # add video record
        try:
            new_video_record = add_video_record_via_stat_api(aid, bapi, session)
        except InvalidObjCodeError as e:
            DBOperation.update_video_code(aid, e.code, session)
            logger_14.warning('%s Update video aid = %d code from 0 to %d.' % (time_label, aid, e.code))
            code_changed_count += 1
        except TddCommonError as e:
            logger_14.warning('%s %r' % (time_label, e))
            fail_aids.append(aid)
        else:
            logger_14.debug('%s Add new record %s' % (time_label, new_video_record))
            added_count += 1
        time.sleep(0.2)  # api duration banned

    # fail aids, need manual check
    logger_14.warning('%s Fail aids: %s' % (time_label, fail_aids))

    # get finish ts
    finish_ts = get_ts_s()

    # make summary
    summary = \
        '%s c0 aids done\n\n' % time_label + \
        'start: %s, finish: %s, timespan: %ss\n\n' \
        % (ts_s_to_str(start_ts), ts_s_to_str(finish_ts), (finish_ts - start_ts)) + \
        'total aids count: %d\n\n' % aids_len + \
        'added aids count: %d\n\n' % added_count + \
        'code changed aids count: %d\n\n' % code_changed_count + \
        'fail aids count: %d, detail: %r\n\n' % (len(fail_aids), fail_aids) + \
        'by.bunnyxt, %s' % ts_s_to_str(get_ts_s())

    logger_14.info('%s Finish updating c0 aids!' % time_label)
    logger_14.warning(summary)

    # send sc
    sc_result = sc_send('%s Finish updating c0 aids!' % time_label, summary)
    if sc_result['errno'] == 0:
        logger_14.info('%s Sc summary sent: succeed!' % time_label)
    else:
        logger_14.warning('%s Sc summary sent: failed! sc_result = %s.' % (time_label, sc_result))

    session.close()


def add_new_video_record_via_awesome_api(aids, time_label):
    logger_14.info('%s Now start add new video record via awesome api...' % time_label)
    bapi = BiliApi()
    session = Session()

    aids_len = len(aids)  # aids len
    start_ts = get_ts_s()  # get start ts

    fail_aids = []
    code_changed_count = 0  # video code changed count
    added_count = 0  # aids added count

    last_page_aids = []  # aids added in last page
    this_page_aids = []  # aids added in this page

    # calculate page total num
    obj = get_valid(bapi.get_archive_rank_by_partion, (30, 1, 50), test_archive_rank_by_partion)
    page_total = math.ceil(obj['data']['page']['count'] / 50)

    page_num = 1
    goon = True
    while page_num <= page_total and goon:
        # get obj via awesome api
        obj = get_valid(bapi.get_archive_rank_by_partion, (30, page_num, 50), test_archive_rank_by_partion)
        if obj is None:
            logger_14.warning('%s Page num %d fail! Cannot get valid obj.' % (time_label, page_num))
            page_num += 1
            continue

        if len(aids) == 0:
            logger_14.info('%s No aids left, now break.' % time_label)
            goon = False
            break

        # record obj request ts
        added = get_ts_s()

        min_aid = 999999999  # big number
        this_page_added = 0

        try:
            # process each video in archives
            for arch in obj['data']['archives']:
                # get video aid
                aid = arch['aid']

                if aid < min_aid:
                    min_aid = aid

                if aid in last_page_aids:
                    # aid added in last page, continue
                    logger_14.warning(
                        '%s Aid %d already added in last page (page_num = %d).' % (time_label, aid, page_num - 1))
                    continue

                if aid in aids:
                    # aid in aids, go add video record, get stat first
                    stat = arch['stat']
                    # add stat record, which comes from awesome api
                    try:
                        new_video_record = add_video_record_via_awesome_stat(added, stat, session)
                    except TddCommonError as e:
                        logger_14.warning('%s %r' % (time_label, e))
                        fail_aids.append(aid)
                    else:
                        logger_14.debug('%s Add new video record %s' % (time_label, new_video_record))
                        added_count += 1
                        this_page_added += 1
                    aids.remove(aid)
                    this_page_aids.append(aid)  # add aid to this page aids
                else:
                    # aid not in db c30 aids, maybe video not added in db
                    # logger_11_c30.warning('Aid %d not in update aids.' % aid)
                    # not_added_aids.append(aid)  # add to not added aids
                    pass
        except Exception as e:
            logger_14.error(
                '%s Exception caught when process each video in archives. page_num = %d. Detail: %s' % (
                    time_label, page_num, e))

        # assign this page aids to last page aids and reset it
        last_page_aids = this_page_aids
        this_page_aids = []

        logger_14.info('%s Page %d / %d done, %d added.' % (time_label, page_num, page_total, this_page_added))

        # update page num
        page_total = math.ceil(obj['data']['page']['count'] / 50)
        page_num += 1

        if min_aid < min(aids) * 0.9 or len(aids) < 25:
            logger_14.info('%s Find min_aid < min(aids) * 0.9 or len(aids) < 25, now break' % time_label)
            goon = False
            break

    # fail aids, need manual check
    logger_14.warning('%s Fail aids: %s' % (time_label, fail_aids))

    # not added aids
    not_added_aids = aids
    logger_14.warning('%s Not added aids: %s' % (time_label, not_added_aids))

    not_added_unsolved_aids = []
    tid_changed_aids = []

    # check code
    for aid in not_added_aids:
        time.sleep(0.2)  # api duration banned
        # get view obj
        view_obj = get_valid(bapi.get_video_view, (aid,), test_video_view)
        if view_obj is None:
            logger_14.warning('%s Aid %d fail! Cannot get valid view obj.' % (time_label, aid))
            not_added_unsolved_aids.append(aid)
            continue

        # record view obj request ts
        view_obj_added = get_ts_s()

        try:
            # get video code
            code = view_obj['code']

            if code == 0:
                # code==0, check tid next
                if 'tid' in view_obj['data'].keys():
                    # get video tid
                    tid = view_obj['data']['tid']
                    if tid != 30:
                        # video tid!=30 now, change tid
                        # TODO change update function here
                        DBOperation.update_video_tid(aid, tid, session)
                        DBOperation.update_video_isvc(aid, 5, session)
                        logger_14.warning(
                            '%s Update video aid = %d tid from 30 to %d then update isvc = %d.' % (
                                time_label, aid, tid, 5))
                        tid_changed_aids.append(aid)
                    else:
                        # video tid==30, add video record
                        # logger_14.warning(
                        #     '%s Found aid = %d code == 0 and tid == 30! Now try add video record...' % (
                        #         time_label, aid))

                        # get stat first
                        stat = view_obj['data']['stat']

                        # make new tdd video record obj and assign stat info from api
                        new_video_record = TddVideoRecord()
                        new_video_record.aid = aid
                        new_video_record.added = view_obj_added
                        new_video_record.view = -1 if stat['view'] == '--' else stat['view']
                        new_video_record.danmaku = stat['danmaku']
                        new_video_record.reply = stat['reply']
                        new_video_record.favorite = stat['favorite']
                        new_video_record.coin = stat['coin']
                        new_video_record.share = stat['share']
                        new_video_record.like = stat['like']

                        # add to db
                        DBOperation.add(new_video_record, session)
                        logger_14.info('%s Add record %s.' % (time_label, new_video_record))
                        added_count += 1
                else:
                    logger_14.error('%s View obj %s got code == 0 but no tid field! Need further check!' % (
                        time_label, view_obj))
                    not_added_unsolved_aids.append(aid)
            else:
                # code!=0, change code
                DBOperation.update_video_code(aid, code, session)
                logger_14.warning('%s Update video aid = %d code from 0 to %d.' % (time_label, aid, code))
                code_changed_count += 1
        except Exception as e:
            logger_14.error(
                '%s Exception caught when process view obj of left aid %d. Detail: %s' % (time_label, aid, e))
            not_added_unsolved_aids.append(aid)

    # get finish ts
    finish_ts = get_ts_s()

    # make summary
    summary = \
        '%s c30 aids done\n\n' % time_label + \
        'start: %s, finish: %s, timespan: %ss\n\n' \
        % (ts_s_to_str(start_ts), ts_s_to_str(finish_ts), (finish_ts - start_ts)) + \
        'total aids count: %d\n\n' % aids_len + \
        'added aids count: %d\n\n' % added_count + \
        'code changed aids count: %d\n\n' % code_changed_count + \
        'tid changed aids count: %d, detail: %r\n\n' % (len(tid_changed_aids), tid_changed_aids) + \
        'fail aids count: %d, detail: %r\n\n' % (len(fail_aids), fail_aids) + \
        'not added unsolved aids count: %d, detail: %r\n\n' % (len(not_added_unsolved_aids), not_added_unsolved_aids) +\
        'by.bunnyxt, %s' % ts_s_to_str(get_ts_s())

    logger_14.info('%s Finish updating c30 aids!' % time_label)
    logger_14.warning(summary)

    # send sc
    sc_result = sc_send('%s Finish updating c30 aids!' % time_label, summary)
    if sc_result['errno'] == 0:
        logger_14.info('%s Sc summary sent: succeed!' % time_label)
    else:
        logger_14.warning('%s Sc summary sent: failed! sc_result = %s.' % (time_label, sc_result))

    session.close()


def add_15m_aids(time_label, task_label, c0_aids, c30_aids, session):
    # daily new video
    c0_daily_new_video_aids = DBOperation.query_daily_new_video_aids(is_tid_30=False, session=session)
    logger_14.info('%s %s: get %d c0 daily new video aids' % (time_label, task_label, len(c0_daily_new_video_aids)))
    c0_aids.extend(c0_daily_new_video_aids)
    c30_daily_new_video_aids = DBOperation.query_daily_new_video_aids(is_tid_30=True, session=session)
    logger_14.info('%s %s: get %d c30 daily new video aids' % (time_label, task_label, len(c30_daily_new_video_aids)))
    c30_aids.extend(c30_daily_new_video_aids)


def add_1h_aids(time_label, task_label, c0_aids, c30_aids, session):
    # hot video
    c0_hot_video_aids = DBOperation.query_hot_video_aids(is_tid_30=False, session=session)
    logger_14.info('%s %s: get %d c0 hot video aids' % (time_label, task_label, len(c0_hot_video_aids)))
    c0_aids.extend(c0_hot_video_aids)
    c30_hot_video_aids = DBOperation.query_hot_video_aids(is_tid_30=True, session=session)
    logger_14.info('%s %s: get %d c30 hot video aids' % (time_label, task_label, len(c30_hot_video_aids)))
    c30_aids.extend(c30_hot_video_aids)
    # weekly new video
    c0_weekly_new_video_aids = DBOperation.query_weekly_new_video_aids(is_tid_30=False, session=session)
    logger_14.info('%s %s: get %d c0 weekly new video aids' % (time_label, task_label, len(c0_weekly_new_video_aids)))
    c0_aids.extend(c0_weekly_new_video_aids)
    c30_weekly_new_video_aids = DBOperation.query_weekly_new_video_aids(is_tid_30=True, session=session)
    logger_14.info('%s %s: get %d c30 weekly new video aids' % (time_label, task_label, len(c30_weekly_new_video_aids)))
    c30_aids.extend(c30_weekly_new_video_aids)


def add_6h_aids(time_label, task_label, c0_aids, c30_aids, session):
    # active video
    c0_active_video_aids = DBOperation.query_active_video_aids(is_tid_30=False, session=session)
    logger_14.info('%s %s: get %d c0 active video aids' % (time_label, task_label, len(c0_active_video_aids)))
    c0_aids.extend(c0_active_video_aids)
    c30_active_video_aids = DBOperation.query_active_video_aids(is_tid_30=True, session=session)
    logger_14.info('%s %s: get %d c30 active video aids' % (time_label, task_label, len(c30_active_video_aids)))
    c30_aids.extend(c30_active_video_aids)


def _24h(time_label):
    # TODO
    logger_14('_24h: here!')


def _6h(time_label):
    session = Session()

    c0_aids = []
    c30_aids = []

    task_label = '_6h'
    add_15m_aids(time_label, task_label, c0_aids, c30_aids, session)
    add_1h_aids(time_label, task_label, c0_aids, c30_aids, session)

    update_thread_list = [threading.Thread(target=add_new_video_record_via_stat_api, args=(c0_aids, time_label)),
                          threading.Thread(target=add_new_video_record_via_awesome_api, args=(c30_aids, time_label))]

    for t in update_thread_list:
        t.start()

    for t in update_thread_list:
        t.join()

    # TODO maybe do something...

    session.close()


def _1h(time_label):
    session = Session()

    c0_aids = []
    c30_aids = []

    task_label = '_1h'
    add_15m_aids(time_label, task_label, c0_aids, c30_aids, session)
    add_1h_aids(time_label, task_label, c0_aids, c30_aids, session)

    update_thread_list = [threading.Thread(target=add_new_video_record_via_stat_api, args=(c0_aids, time_label)),
                          threading.Thread(target=add_new_video_record_via_awesome_api, args=(c30_aids, time_label))]

    for t in update_thread_list:
        t.start()

    for t in update_thread_list:
        t.join()

    # TODO maybe do something...

    session.close()


def _15m(time_label):
    session = Session()

    c0_aids = []
    c30_aids = []

    task_label = '_15m'
    add_15m_aids(time_label, task_label, c0_aids, c30_aids, session)

    update_thread_list = [threading.Thread(target=add_new_video_record_via_stat_api, args=(c0_aids, time_label)),
                          threading.Thread(target=add_new_video_record_via_awesome_api, args=(c30_aids, time_label))]

    for t in update_thread_list:
        t.start()

    for t in update_thread_list:
        t.join()

    # TODO maybe do something...

    session.close()


def _24h_task(time_label):
    threading.Thread(target=_24h, args=(time_label,)).start()


def _6h_task(time_label):
    threading.Thread(target=_6h, args=(time_label,)).start()


def _1h_task(time_label):
    threading.Thread(target=_1h, args=(time_label,)).start()


def _15m_task(time_label):
    threading.Thread(target=_15m, args=(time_label,)).start()


def main():
    logger_14.info('14: regularly add new video record')

    _24h_list = [
        '04:00'
    ]

    _6h_list = [
        '10:00',
        '16:00',
        '22:00',
    ]

    _1h_list = [
        '00:00', '01:00', '02:00', '03:00',
        '05:00', '06:00', '07:00', '08:00', '09:00',
        '11:00', '12:00', '13:00', '14:00', '15:00',
        '17:00', '18:00', '19:00', '20:00', '21:00',
        '23:00',
    ]

    _15m_list = [
        '00:15', '00:30', '00:45',
        '01:15', '01:30', '01:45',
        '02:15', '02:30', '02:45',
        '03:15', '03:30', '03:45',
        '04:30', '04:45',
        '05:15', '05:30', '05:45',
        '06:15', '06:30', '06:45',
        '07:15', '07:30', '07:45',
        '08:15', '08:30', '08:45',
        '09:15', '09:30', '09:45',
        '10:15', '10:30', '10:45',
        '11:15', '11:30', '11:45',
        '12:15', '12:30', '12:45',
        '13:15', '13:30', '13:45',
        '14:15', '14:30', '14:45',
        '15:15', '15:30', '15:45',
        '16:15', '16:30', '16:45',
        '17:15', '17:30', '17:45',
        '18:15', '18:30', '18:45',
        '19:15', '19:30', '19:45',
        '20:15', '20:30', '20:45',
        '21:15', '21:30', '21:45',
        '22:15', '22:30', '22:45',
        '23:15', '23:30', '23:45',
    ]

    for time_label in _24h_list:
        schedule.every().day.at(time_label).do(_24h_task, time_label)
        logger_14.info('Will do 24h task at %s' % time_label)

    for time_label in _6h_list:
        schedule.every().day.at(time_label).do(_6h_task, time_label)
        logger_14.info('Will do 6h task at %s' % time_label)

    for time_label in _1h_list:
        schedule.every().day.at(time_label).do(_1h_task, time_label)
        logger_14.info('Will do 1h task at %s' % time_label)

    for time_label in _15m_list:
        schedule.every().day.at(time_label).do(_15m_task, time_label)
        logger_14.info('Will do 15m task at %s' % time_label)

    logger_14.info('All task registered.')

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    main()
