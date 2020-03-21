import schedule
import threading
from logger import logger_19
from db import DBOperation, Session, TddVideoRecord, TddVideoLog, TddVideoRecordAbnormalChange
import time
from util import get_ts_s, ts_s_to_str
from pybiliapi import BiliApi
import math
from common import get_valid, test_archive_rank_by_partion, add_video_record_via_stat_api, InvalidObjCodeError, \
    update_video, TddCommonError, test_video_view, add_video, AlreadyExistError
from collections import defaultdict, namedtuple


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

    logger_19.info('01: make c30 new video records from awesome api')

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
            continue

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
            if page_num % 100 == 0:
                logger_19.info('Awesome api fetch %d / %d done' % (page_num, page_total))
        except Exception as e:
            logger_19.error('Awesome api fetch %d / %d error, Exception caught. Detail: %s' % (page_num, page_total, e))
        finally:
            page_num += 1
    logger_19.info('Awesome api fetch %d / %d done' % (page_num - 1, page_total))

    logger_19.info('01 done! c30_new_video_record_list count: %d' % len(c30_new_video_record_list))

    logger_19.info('02: make c0 new video records and insert them')

    # get need insert c0 aids
    need_insert_c0_aid_list = get_need_insert_aid_list(time_label, is_tid_30=False, session=session)
    logger_19.info('got %d need insert c0 aids' % len(need_insert_c0_aid_list))

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

    logger_19.info('02 done! c0_total_aids count: %d, c0_success_aids count: %d, c0_fail_aids count: %d' % (
        len(need_insert_c0_aid_list), len(c0_success_aids), len(c0_fail_aids)))
    logger_19.info('c0_fail_aids: %r' % c0_fail_aids)

    logger_19.info('03: insert c30 video records')

    # go insert c30 record
    need_insert_c30_aid_list = get_need_insert_aid_list(time_label, is_tid_30=True, session=session)
    logger_19.info('got %d need insert c30 aids' % len(need_insert_c30_aid_list))

    c30_success_aids = []
    c30_visited = 0
    c30_not_added_record_list = []
    need_insert_c30_aid_list_count = len(need_insert_c30_aid_list)

    for record in c30_new_video_record_list:
        if record.aid in need_insert_c30_aid_list:
            if record.aid in c30_success_aids:
                logger_19.warning('c30 aid %d already added' % record.aid)
                continue
            need_insert_c30_aid_list.remove(record.aid)
            session.add(record)  # TODO may cause error?
            c30_success_aids.append(record.aid)
            c30_visited += 1
            if c30_visited % 100 == 0:
                try:
                    session.commit()
                except Exception as e:
                    logger_19.error('Fail to add c30 aid add %d / %d, Exception caught. Detail: %s'
                                    % (c30_visited, need_insert_c30_aid_list_count, e))
                    session.rollback()
                else:
                    logger_19.info('c30 aid add %d / %d done' % (c30_visited, need_insert_c30_aid_list_count))
        else:
            c30_not_added_record_list.append(record)
    session.commit()
    logger_19.info('c30 aid add %d / %d done' % (c30_visited, need_insert_c30_aid_list_count))

    c30_left_aids = need_insert_c30_aid_list
    logger_19.info('03 done! c30_total_aids count: %d, c30_success_aids count: %d, '
                   % (need_insert_c30_aid_list_count, len(c30_success_aids)) +
                   'c30_left_aids count: %d, c30_not_added_record_list count: %d'
                   % (len(c30_left_aids), len(c30_not_added_record_list)))

    logger_19.info('04: check left aids, change tid or code')

    # there aids should got record from awesome api, but now seems they dont
    # might because they are now tid != 30 or code != 0
    c30_left_unsolved_aids = []  # fail to handle
    c30_left_tid_changed_aids = []  # tid changed 30 -> new tid
    c30_left_code_changed_aids = []  # code changed 0 -> new code
    c30_left_added_aids = []  # tid = 30 and code = 0, add new video record

    logger_19.info('got %d c30 left aids' % len(c30_left_aids))
    for aid in c30_left_aids:
        time.sleep(0.2)  # api duration banned
        # get view obj
        view_obj = get_valid(bapi.get_video_view, (aid,), test_video_view)
        if view_obj is None:
            logger_19.warning('Aid %d fail! Cannot get valid view obj.' % aid)
            c30_left_unsolved_aids.append(aid)
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
                        try:
                            old_video = DBOperation.query_video_via_aid(aid, session)
                            session.add(TddVideoLog(view_obj_added, aid, 'tid', old_video.tid, tid))
                            old_video.tid = tid
                            session.add(TddVideoLog(view_obj_added, aid, 'isvc', old_video.isvc, 5))
                            old_video.isvc = 5
                            session.commit()
                            logger_19.info(
                                'Update video aid = %d tid from 30 to %d then update isvc = %d.'
                                % (aid, tid, 5))
                            c30_left_tid_changed_aids.append(aid)
                        except Exception as e:
                            session.rollback()
                            logger_19.warning(
                                'Fail to update video aid = %d tid from 30 to %d then update isvc = %d. ' % (
                                    aid, tid, 5) + 'Exception caught. Detail: %s' % e)
                            c30_left_unsolved_aids.append(aid)
                    else:
                        # video tid==30, add video record
                        # logger_19.warning(
                        #     'Found aid = %d code == 0 and tid == 30! Now try add video record...' % aid)

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
                        logger_19.info('Add record %s.' % new_video_record)
                        c30_left_added_aids.append(aid)
                else:
                    logger_19.error('View obj %s got code == 0 but no tid field! Need further check!' % view_obj)
                    c30_left_unsolved_aids.append(aid)
            else:
                # code!=0, change code
                try:
                    old_video = DBOperation.query_video_via_aid(aid, session)
                    session.add(TddVideoLog(view_obj_added, aid, 'code', old_video.code, code))
                    old_video.code = code
                    session.commit()
                    logger_19.info('Update video aid = %d code from 0 to %d.' % (aid, code))
                    c30_left_code_changed_aids.append(aid)
                except Exception as e:
                    session.rollback()
                    logger_19.warning('Fail to update video aid = %d code from 0 to %d.' % (aid, code) +
                                      'Exception caught. Detail: %s' % e)
                    c30_left_unsolved_aids.append(aid)
        except Exception as e:
            logger_19.error(
                'Exception caught when process view obj of left aid %d. Detail: %s' % (aid, e))
            c30_left_unsolved_aids.append(aid)

    logger_19.info('04 done. c30_left_aids count: %d, c30_left_unsolved_aids count: %d, '
                   % (len(c30_left_aids), len(c30_left_unsolved_aids)) +
                   'c30_left_tid_changed_aids count: %d, c30_left_code_changed_aids count: %d, '
                   % (len(c30_left_tid_changed_aids), len(c30_left_code_changed_aids)) +
                   'c30_left_added_aids: %d' % len(c30_left_added_aids))

    logger_19.info('05: check c30 not added aid record list and add them')

    c30_not_added_add_video_aids = []
    if time_label == '04:00':
        logger_19.info('got %d c30 not added records' % len(c30_not_added_record_list))

        # check not added record list
        for record in c30_not_added_record_list:
            time.sleep(0.2)  # api duration banned
            aid = record.aid
            # add video
            try:
                new_video = add_video(aid, bapi, session)
            except AlreadyExistError:
                # video already exist, which is absolutely common
                pass
            except TddCommonError as e:
                logger_19.warning('Fail to add video aid %d. Exception caught. Detail: %s' % (aid, e))
                continue
            else:
                logger_19.info('Add new video %s' % new_video)
                c30_not_added_add_video_aids.append(aid)

            # add video record
            DBOperation.add(record, session)

        logger_19.info('05 done! c30_not_added_add_video_aids count: %d' % len(c30_not_added_add_video_aids))
    else:
        logger_19.info('05 done! time label is not 04:00, no need to process them')

    logger_19.info('06: save new video records to file')

    # save to file
    new_video_record_list = c30_new_video_record_list + c0_new_video_record_list
    filename = 'data/%s.csv' % task_label
    with open(filename, 'w') as f:
        f.write('aid,added,view,danmaku,reply,favorite,coin,share,like\n')
        for record in new_video_record_list:
            f.write('%d,%d,%d,%d,%d,%d,%d,%d,%d\n'
                    % (record.aid, record.added, record.view, record.danmaku,
                       record.reply, record.favorite, record.coin, record.share, record.like))
    index_filename = 'data/index.txt'
    with open(index_filename, 'a') as f:
        f.write('%s\n' % filename)

    logger_19.info('06 done! %d record(s) stored in file %s' % (len(new_video_record_list), filename))

    logger_19.info('07: load records from history files')

    # load records from history files
    history_filename_list = []
    with open(index_filename, 'r') as f:
        lines = f.readlines()
        for line in lines[-25:]:
            history_filename_list.append(line.rstrip('\n'))
    logger_19.info('Will load records from file list %r' % history_filename_list)

    VideoRecord = namedtuple("VideoRecord",
                             ['aid', 'added', 'view', 'danmaku', 'reply', 'favorite', 'coin', 'share', 'like'])
    history_record_dict = defaultdict(list)
    history_record_count = 0
    for filename in history_filename_list:
        try:
            with open(filename, 'r') as f:
                lines = f.readlines()
                for line in lines[1:]:
                    try:
                        line_list = line.rstrip('\n').split(',')
                        video_record = VideoRecord(
                            int(line_list[0]),
                            int(line_list[1]),
                            int(line_list[2]),
                            int(line_list[3]),
                            int(line_list[4]),
                            int(line_list[5]),
                            int(line_list[6]),
                            int(line_list[7]),
                            int(line_list[8]),
                        )
                        video_record_list = history_record_dict[video_record.aid]
                        video_record_list.append(video_record)
                    except Exception as e:
                        logger_19.warning('Fail to make video record from line: %s. Exception caught. Detail: %s'
                                          % (line, e))
                    finally:
                        history_record_count += 1
            logger_19.info('Finish load records from file %s' % filename)
        except Exception as e:
            logger_19.warning('Fail to read load records from file %s. Exception caught. Detail: %s' % (filename, e))

    logger_19.info('07 done! loaded %d history records from %d files'
                   % (history_record_count, len(history_filename_list)))

    logger_19.info('08: check params of history video records')

    # get video pubdate
    video_pubdate_list = DBOperation.query_video_pubdate_all(session)
    video_pubdate_dict = dict()
    for (aid, pubdate) in video_pubdate_list:
        video_pubdate_dict[aid] = pubdate
    logger_19.info('Finish make video pubdate dict with %d aids.' % len(video_pubdate_dict))

    # check record
    check_total_count = len(new_video_record_list)
    check_visited_count = 0
    for record in new_video_record_list:
        video_record_list = history_record_dict[record.aid]
        if len(video_record_list) <= 2:  # at least require 3 record
            continue

        video_record_list.sort(key=lambda r: r.added)

        # remove all zero situation
        if record.view == 0 and record.danmaku == 0 and record.reply == 0 and record.favorite == 0 and \
                record.coin == 0 and record.share == 0 and record.like == 0:
            logger_19.warning('%d got all params of record = 0, maybe API bug, continue', record.aid)
            continue

        timespan_now = video_record_list[-1].added - video_record_list[-2].added
        if timespan_now == 0:
            logger_19.warning('%d got timespan_now = 0, continue', record.aid)
            continue
        speed_now_dict = dict()
        speed_now_dict['view'] = (video_record_list[-1].view - video_record_list[-2].view) / timespan_now * 3600
        speed_now_dict['danmaku'] = (video_record_list[-1].danmaku - video_record_list[-2].danmaku) / timespan_now * 3600
        speed_now_dict['reply'] = (video_record_list[-1].reply - video_record_list[-2].reply) / timespan_now * 3600
        speed_now_dict['favorite'] = (video_record_list[-1].favorite - video_record_list[-2].favorite) / timespan_now * 3600
        speed_now_dict['coin'] = (video_record_list[-1].coin - video_record_list[-2].coin) / timespan_now * 3600
        speed_now_dict['share'] = (video_record_list[-1].share - video_record_list[-2].share) / timespan_now * 3600
        speed_now_dict['like'] = (video_record_list[-1].like - video_record_list[-2].like) / timespan_now * 3600

        timespan_last = video_record_list[-2].added - video_record_list[-3].added
        if timespan_last == 0:
            logger_19.warning('%d got timespan_last = 0, continue', record.aid)
            continue
        speed_last_dict = dict()
        speed_last_dict['view'] = (video_record_list[-2].view - video_record_list[-3].view) / timespan_last * 3600
        speed_last_dict['danmaku'] = (video_record_list[-2].danmaku - video_record_list[-3].danmaku) / timespan_last * 3600
        speed_last_dict['reply'] = (video_record_list[-2].reply - video_record_list[-3].reply) / timespan_last * 3600
        speed_last_dict['favorite'] = (video_record_list[-2].favorite - video_record_list[-3].favorite) / timespan_last * 3600
        speed_last_dict['coin'] = (video_record_list[-2].coin - video_record_list[-3].coin) / timespan_last * 3600
        speed_last_dict['share'] = (video_record_list[-2].share - video_record_list[-3].share) / timespan_last * 3600
        speed_last_dict['like'] = (video_record_list[-2].like - video_record_list[-3].like) / timespan_last * 3600

        speed_now_incr_rate_dict = dict()
        speed_now_incr_rate_dict['view'] = (speed_now_dict['view'] - speed_last_dict['view']) / (speed_last_dict['view'] + 0.01)
        speed_now_incr_rate_dict['danmaku'] = (speed_now_dict['danmaku'] - speed_last_dict['danmaku']) / (speed_last_dict['danmaku'] + 0.01)
        speed_now_incr_rate_dict['reply'] = (speed_now_dict['reply'] - speed_last_dict['reply']) / (speed_last_dict['reply'] + 0.01)
        speed_now_incr_rate_dict['favorite'] = (speed_now_dict['favorite'] - speed_last_dict['favorite']) / (speed_last_dict['favorite'] + 0.01)
        speed_now_incr_rate_dict['coin'] = (speed_now_dict['coin'] - speed_last_dict['coin']) / (speed_last_dict['coin'] + 0.01)
        speed_now_incr_rate_dict['share'] = (speed_now_dict['share'] - speed_last_dict['share']) / (speed_last_dict['share'] + 0.01)
        speed_now_incr_rate_dict['like'] = (speed_now_dict['like'] - speed_last_dict['like']) / (speed_last_dict['like'] + 0.01)

        period_range = video_record_list[-1].added - video_record_list[0].added
        if period_range == 0:
            logger_19.warning('%d got period_range = 0, continue', record.aid)
            continue

        speed_period_dict = dict()
        speed_period_dict['view'] = (video_record_list[-1].view - video_record_list[0].view) / period_range * 3600
        speed_period_dict['danmaku'] = (video_record_list[-1].danmaku - video_record_list[0].danmaku) / period_range * 3600
        speed_period_dict['reply'] = (video_record_list[-1].reply - video_record_list[0].reply) / period_range * 3600
        speed_period_dict['favorite'] = (video_record_list[-1].favorite - video_record_list[0].favorite) / period_range * 3600
        speed_period_dict['coin'] = (video_record_list[-1].coin - video_record_list[0].coin) / period_range * 3600
        speed_period_dict['share'] = (video_record_list[-1].share - video_record_list[0].share) / period_range * 3600
        speed_period_dict['like'] = (video_record_list[-1].like - video_record_list[0].like) / period_range * 3600

        overall_range = video_record_list[-1].added
        if record.aid in video_pubdate_dict.keys() and video_pubdate_dict[record.aid]:
            overall_range -= video_pubdate_dict[record.aid]
        if overall_range == 0:
            logger_19.warning('%d got overall_range = 0, continue', record.aid)
            continue

        speed_overall_dict = dict()
        speed_overall_dict['view'] = video_record_list[-1].view / overall_range * 3600
        speed_overall_dict['danmaku'] = video_record_list[-1].danmaku / overall_range * 3600
        speed_overall_dict['reply'] = video_record_list[-1].reply / overall_range * 3600
        speed_overall_dict['favorite'] = video_record_list[-1].favorite / overall_range * 3600
        speed_overall_dict['coin'] = video_record_list[-1].coin / overall_range * 3600
        speed_overall_dict['share'] = video_record_list[-1].share / overall_range * 3600
        speed_overall_dict['like'] = video_record_list[-1].like / overall_range * 3600

        has_abnormal_change = False
        new_change_list = []

        # check unexpected drop
        for (key, value) in speed_now_dict.items():
            if value < -10:
                new_change = TddVideoRecordAbnormalChange()
                new_change.added = video_record_list[-1].added
                new_change.aid = record.aid
                new_change.attr = key
                new_change.speed_now = speed_now_dict[key]
                new_change.speed_last = speed_last_dict[key]
                new_change.speed_now_incr_rate = speed_now_incr_rate_dict[key]
                new_change.period_range = period_range
                new_change.speed_period = speed_period_dict[key]
                new_change.speed_overall = speed_overall_dict[key]
                new_change.description = 'unexpected drop detected, speed now of %s is %f, < -10' % (key, value)
                logger_19.info('%d change: %s' % (record.aid, new_change.description))
                has_abnormal_change = True
                new_change_list.append(new_change)

        # check unexpected increase speed
        for (key, value) in speed_now_incr_rate_dict.items():
            if value > 2 and speed_now_dict[key] > 50:
                new_change = TddVideoRecordAbnormalChange()
                new_change.added = video_record_list[-1].added
                new_change.aid = record.aid
                new_change.attr = key
                new_change.speed_now = speed_now_dict[key]
                new_change.speed_last = speed_last_dict[key]
                new_change.speed_now_incr_rate = speed_now_incr_rate_dict[key]
                new_change.period_range = period_range
                new_change.speed_period = speed_period_dict[key]
                new_change.speed_overall = speed_overall_dict[key]
                new_change.description = 'unexpected increase speed detected, speed now of {0} is {1}%, > 200%'.format(
                    key, value * 100)
                logger_19.info('%d change: %s' % (record.aid, new_change.description))
                has_abnormal_change = True
                new_change_list.append(new_change)

        if has_abnormal_change and record.id is None:
            DBOperation.add(record, session)
            logger_19.info('Add video record %s' % record)

        # TODO change freq

        try:
            for new_change in new_change_list:
                new_change.record_id = record.id
                session.add(new_change)
            session.commit()
        except Exception as e:
            logger_19.error('Fail to add new change list with aid %d. Exception caught. Detail: %s' % (record.aid, e))

        check_visited_count += 1
        if check_visited_count % 10000 == 0:
            logger_19.info('check %d / %d done' % (check_visited_count, check_total_count))

    logger_19.info('check %d / %d done' % (check_visited_count, check_total_count))

    logger_19.info('08 done! Finish check params of history video records')

    # TODO change activity and recent icon

    session.close()


def hour_task(time_label):
    threading.Thread(target=hour, args=(time_label,)).start()


def main():
    logger_19.info('19: hourly video spider')

    hour_list = [
        '00:00', '01:00', '02:00', '03:00', '04:00', '05:00',
        '06:00', '07:00', '08:00', '09:00', '10:00', '11:00',
        '12:00', '13:00', '14:00', '15:00', '16:00', '17:00',
        '18:00', '19:00', '20:00', '21:00', '22:00', '23:00'
    ]
    hour_list = [
        '00:00', '01:00', '02:00', '03:00',          '05:00',
        '06:00', '07:00', '08:00', '09:00', '10:00', '11:00',
        '12:00', '13:00', '14:00', '15:00', '16:00', '17:00',
        '18:00', '19:00', '20:00', '21:00', '22:00', '23:00'
    ]  # debug no 04:00

    for time_label in hour_list:
        schedule.every().day.at(time_label).do(hour_task, time_label)

    logger_19.info('All hourly task registered.')

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    main()
