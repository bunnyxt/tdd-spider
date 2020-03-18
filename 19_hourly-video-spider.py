import schedule
import threading
from logger import logger_19
from db import DBOperation, Session, TddVideoRecord
import time
from util import get_ts_s, ts_s_to_str
from pybiliapi import BiliApi
import math
from common import get_valid, test_archive_rank_by_partion, add_video_record_via_stat_api, InvalidObjCodeError, \
    update_video, TddCommonError


def get_need_insert_aid_list(time_label, is_tid_30, session):
    if time_label == '04:00':
        # return total
        return DBOperation.query_freq_update_video_aids(0, is_tid_30, session)

    # add 1 hour aids
    aid_list = DBOperation.query_freq_update_video_aids(2, is_tid_30, session)

    if time_label in ['00:00', '04:00', '08:00', '12:00', '16:00', '20:00']:
        # add 4 hour aids
        aid_list += DBOperation.query_freq_update_video_aids(1, is_tid_30, session)

    return aid_list


def hour(time_label):
    task_label = ts_s_to_str(get_ts_s())[:11] + time_label
    logger_19.info('Now start hourly video task %s..' % task_label)

    bapi = BiliApi()
    session = Session()

    c30_new_video_record_list = []

    # get page total
    obj = bapi.get_archive_rank_by_partion(30, 1, 50)
    page_total = math.ceil(obj['data']['page']['count'] / 50)
    logger_19.info('%d page(s) found' % page_total)

    page_num = 1
    while page_num <= page_total:
        # get obj via awesome api
        obj = get_valid(bapi.get_archive_rank_by_partion, (30, page_num, 50), test_archive_rank_by_partion)
        if obj is None:
            logger_19.warning('Page num %d fail! Cannot get valid obj.' % page_num)
            page_num += 1

        try:
            added = get_ts_s()
            for arch in obj['data']['archives']:
                # make new video record
                new_video_record = TddVideoRecord()
                new_video_record.aid = arch['aid']
                new_video_record.added = added
                new_video_record.view = -1 if arch['stat']['view'] == '--' else arch['stat']['view']
                new_video_record.danmaku = arch['stat']['danmaku']
                new_video_record.reply = arch['stat']['reply']
                new_video_record.favorite = arch['stat']['favorite']
                new_video_record.coin = arch['stat']['coin']
                new_video_record.share = arch['stat']['share']
                new_video_record.like = arch['stat']['like']

                c30_new_video_record_list.append(new_video_record)
            if page_num % 10 == 0:
                logger_19.info('Awesome api fetch %d / %d done' % (page_num, page_total))
        except Exception as e:
            logger_19.error('Awesome api fetch %d / %d error, Exception caught. Detail: %s' % (page_num, page_total, e))
        page_num += 1

    # get need insert c0 aids
    need_insert_c0_aid_list = get_need_insert_aid_list(time_label, is_tid_30=False, session=session)

    c0_fail_aids = []
    c0_success_aids = []
    c0_visited = 0
    c0_new_video_record_list = []

    for aid in need_insert_c0_aid_list:
        # add video record
        try:
            new_video_record = add_video_record_via_stat_api(aid, bapi, session)
            c0_new_video_record_list.append(new_video_record)
        except InvalidObjCodeError as e:
            # update video code
            try:
                tdd_video_logs = update_video(aid, bapi, session)
            except TddCommonError as e:
                logger_19.warning('Fail to update video aid %d, Exception caught. Detail: %s' % (aid, e))
            except Exception as e:
                logger_19.error('Fail to update video aid %d, Exception caught. Detail: %s' % (aid, e))
            else:
                for log in tdd_video_logs:
                    logger_19.info('Update video aid %d, attr: %s, oldval: %s, newval: %s'
                                   % (log.aid, log.attr, log.oldval, log.newval))
            c0_fail_aids.append(aid)
        except TddCommonError as e:
            logger_19.warning('Fail to update video aid %d, Exception caught. Detail: %s', (aid, e))
            c0_fail_aids.append(aid)
        else:
            c0_success_aids.append(aid)
            logger_19.debug('Add new record %s' % new_video_record)

        c0_visited += 1
        if c0_visited % 10 == 0:
            logger_19.info('c0 aid add %d / %d done' % (c0_visited, len(need_insert_c0_aid_list)))
        time.sleep(0.2)  # api duration banned

    # go insert c30 record
    need_insert_c30_aid_list = get_need_insert_aid_list(time_label, is_tid_30=True, session=session)

    c30_success_aids = []
    c30_visited = 0

    for record in c30_new_video_record_list:
        if record.aid in need_insert_c30_aid_list:
            if record.aid in c30_success_aids:
                logger_19.info('c30 aid %d already added' % record.aid)
                continue
            need_insert_c30_aid_list.remove(record.aid)
            session.add(record)  # TODO may cause error?
            c30_visited += 1
            if c30_visited % 100 == 0:
                try:
                    session.commit()
                except Exception as e:
                    logger_19.error('Fail to add c30 aid add %d / %d, Exception caught. Detail: %s'
                                    % (c30_visited, len(need_insert_c30_aid_list), e))
                    session.rollback()
                else:
                    logger_19.info('c30 aid add %d / %d done' % (c30_visited, len(need_insert_c30_aid_list)))

    # there aids should got record from awesome api, but no seems they dont
    # might because they are now tid != 30 or code != 0
    c30_not_added_aids = need_insert_c30_aid_list

    for aid in c30_not_added_aids:
        # TODO check tid and code
        pass

    # save to file
    new_video_record_list = c30_new_video_record_list + c0_new_video_record_list
    with open('%s.csv' % task_label, 'w') as f:
        f.write('aid,added,view,danmaku,reply,favorite,coin,share,like\n')
        for record in new_video_record_list:
            f.write('%d,%d,%d,%d,%d,%d,%d,%d,%d\n'
                    % (record.aid, record.added, record.view, record.danmaku,
                       record.reply, record.favorite, record.coin, record.share, record.like))
    logger_19.info('%d record(s) stored.' % len(new_video_record_list))

    # check record
    for record in new_video_record_list:
        # TODO check record
        pass


def hour_task(time_label):
    threading.Thread(target=hour, args=(time_label,)).start()


def main():
    logger_19.info('14: hourly video spider')

    hour_list = [
        '00:00', '01:00', '02:00', '03:00', '04:00', '05:00',
        '06:00', '07:00', '08:00', '09:00', '10:00', '11:00',
        '12:00', '13:00', '14:00', '15:00', '16:00', '17:00',
        '18:00', '19:00', '20:00', '21:00', '22:00', '23:00'
    ]
    hour_list = [
        '00:00', '01:00', '02:00', '03:00', '05:00',
        '06:00', '07:00', '08:00', '09:00', '10:00', '11:00',
        '12:00', '13:00', '14:00', '15:00', '16:00', '17:00',
        '18:00', '19:00', '20:00', '21:00', '22:00', '23:00'
    ]  # TODO debug, 04:00 not use it

    # for time_label in hour_list:
    #     schedule.every().day.at(time_label).do(hour_task, time_label)
    hour_task('20:37')  # for debug

    logger_19.info('All hourly task registered.')

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    main()
