import schedule
import threading
import time
from logger import logger_11, logger_11_c0, logger_11_c30
from pybiliapi import BiliApi
import math
from db import update_engine, Session, DBOperation, TddVideoRecord
from util import get_ts_s, ts_s_to_str
from common import get_valid, test_video_view, test_video_stat, test_archive_rank_by_partion
from serverchan import sc_send


def update_aids_c0(aids):
    logger_11.info('Now update c0 aids...')
    logger_11_c0.info('Now update c0 aids...')
    bapi = BiliApi()
    session = Session()

    aids_len = len(aids)  # aids len
    start_ts = get_ts_s()  # get start ts

    fail_aids = []  # aids fail to get valid stat obj
    main_loop_visit_count = aids_len  # aids visited count
    main_loop_add_count = 0  # aids added count

    for aid in aids:
        # get obj via stat api
        obj = get_valid(bapi.get_video_stat, (aid,), test_video_stat)
        if obj is None:
            logger_11_c0.warning('Aid %d fail! Cannot get valid stat obj.' % aid)
            fail_aids.append(aid)
            continue

        # record obj request ts
        added = get_ts_s()

        # check code
        code = obj['code']
        if code == 0:
            # code==0, go add video record, get stat first
            stat = obj['data']

            # make new tdd video record obj and assign stat info from api
            record = TddVideoRecord()
            record.aid = aid
            record.added = added
            record.view = -1 if stat['view'] == '--' else stat['view']
            record.danmaku = stat['danmaku']
            record.reply = stat['reply']
            record.favorite = stat['favorite']
            record.coin = stat['coin']
            record.share = stat['share']
            record.like = stat['like']

            # add to db
            DBOperation.add(record, session)
            logger_11_c0.info('Add record %s.' % record)
        else:
            # code!=0, change code
            DBOperation.update_video_code(aid, code, session)
            logger_11_c0.warning('Update video aid = %d code from 0 to %d.' % (aid, code))

        main_loop_add_count += 1  # add main loop add count
        time.sleep(0.2)  # api duration banned

    # fail aids, need further check
    logger_11_c0.warning('Fail aids: %s' % fail_aids)

    # get finish ts
    finish_ts = get_ts_s()

    # make summary
    summary = \
        '11 updating c0 aids done\n\n' + \
        'start: %s, finish: %s, timespan: %ss\n\n' \
            % (ts_s_to_str(start_ts), ts_s_to_str(finish_ts), (finish_ts - start_ts)) + \
        'target aids count: %d\n\n' % aids_len + \
        'main loop: visited: %d, added: %s, others: %d\n\n' \
            % (main_loop_visit_count, main_loop_add_count, (main_loop_visit_count - main_loop_add_count)) + \
        'fail aids: %s, count: %d\n\n' % (fail_aids, len(fail_aids)) + \
        'by.bunnyxt, %s' % ts_s_to_str(get_ts_s())

    logger_11.info('Finish updating c0 aids!')
    logger_11_c0.info('Finish updating c0 aids!')

    logger_11.warning(summary)
    logger_11_c0.warning(summary)

    # send sc
    sc_result = sc_send('Finish updating c0 aids!', summary)
    if sc_result['errno'] == 0:
        logger_11_c0.info('Sc summary sent successfully.')
    else:
        logger_11_c0.warning('Sc summary sent wrong. sc_result = %s.' % sc_result)

    session.close()


def update_aids_c30(aids):
    logger_11.info('Now update c30 aids...')
    logger_11_c30.info('Now update c30 aids...')
    bapi = BiliApi()
    session = Session()

    aids_len = len(aids)  # aids len
    start_ts = get_ts_s()  # get start ts

    # calculate page total num
    obj = get_valid(bapi.get_archive_rank_by_partion, (30, 1, 50), test_archive_rank_by_partion)
    page_total = math.ceil(obj['data']['page']['count'] / 50)

    not_added_aids = []  # aids in api page but not in db c30 aids
    last_page_aids = []  # aids added in last page
    this_page_aids = []  # aids added in this page
    main_loop_visit_count = 0  # aids visited count
    main_loop_add_count = 0  # aids added count

    page_num = 1
    while page_num <= page_total:
        # get obj via awesome api
        obj = get_valid(bapi.get_archive_rank_by_partion, (30, page_num, 50), test_archive_rank_by_partion)
        if obj is None:
            logger_11_c30.warning('Page num %d fail! Cannot get valid obj.' % page_num)
            page_num += 1
            continue

        # record obj request ts
        added = get_ts_s()

        try:
            # process each video in archives
            for arch in obj['data']['archives']:
                # get video aid
                aid = arch['aid']
                main_loop_visit_count += 1  # add main loop visit count

                if aid in last_page_aids:
                    # aid added in last page, continue
                    logger_11_c30.warning('Aid %d already added in last page (page_num = %d).' % (aid, page_num - 1))
                    continue

                if aid in aids:
                    # aid in db c30 aids, go add video record, get stat first
                    stat = arch['stat']

                    # make new tdd video record obj and assign stat info from api
                    new_video_record = TddVideoRecord()
                    new_video_record.aid = aid
                    new_video_record.added = added
                    new_video_record.view = -1 if stat['view'] == '--' else stat['view']
                    new_video_record.danmaku = stat['danmaku']
                    new_video_record.reply = stat['reply']
                    new_video_record.favorite = stat['favorite']
                    new_video_record.coin = stat['coin']
                    new_video_record.share = stat['share']
                    new_video_record.like = stat['like']

                    # add to db
                    DBOperation.add(new_video_record, session)
                    logger_11_c30.info('Add record %s.' % new_video_record)

                    this_page_aids.append(aid)  # add aid to this page aids
                    aids.remove(aid)  # remove aid from aids
                    main_loop_add_count += 1  # add main loop add count
                else:
                    # aid not in db c30 aids, maybe video not added in db
                    # logger_11_c30.warning('Aid %d not in update aids.' % aid)
                    not_added_aids.append(aid)  # add to not added aids
        except Exception as e:
            logger_11_c30.error(
                'Exception caught when process each video in archives. page_num = %d. Detail: %s' % (page_num, e))

        # assign this page aids to last page aids and reset it
        last_page_aids = this_page_aids
        this_page_aids = []

        logger_11_c30.info('Page %d / %d done.' % (page_num, page_total))

        # update page num
        page_total = math.ceil(obj['data']['page']['count'] / 50)
        page_num += 1

    # finish main loop add record from awesome api
    logger_11_c30.warning(
        'Finish main loop, %d aid(s) visited, %d aids(s) added.' % (main_loop_visit_count, main_loop_add_count))

    # check aids left in aids
    # they are not found in the whole tid==30 videos via awesome api, maybe video code!=0
    logger_11_c30.warning('%d aid(s) left in c30 aids, now check them.' % len(aids))
    logger_11_c30.warning(aids)

    left_aids_visit_count = len(aids)  # aids visited count
    left_aids_solve_count = 0  # aids solved count

    for aid in aids:
        # get view obj
        view_obj = get_valid(bapi.get_video_view, (aid,), test_video_view)
        if view_obj is None:
            logger_11_c30.warning('Aid %d fail! Cannot get valid view obj.' % aid)
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
                        logger_11_c30.warning(
                            'Update video aid = %d tid from 30 to %d then update isvc = %d.' % (aid, tid, 5))
                    else:
                        # video tid==30, add video record
                        logger_11_c30.warning(
                            'Found aid = %d code == 0 and tid == 30! Now try add video record...' % aid)

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
                        logger_11_c30.info('Add record %s.' % new_video_record)

                        left_aids_solve_count += 1  # add left aids solve count
                else:
                    logger_11_c30.error('View obj %s got code == 0 but no tid field! Need further check!' % view_obj)
            else:
                # code!=0, change code
                DBOperation.update_video_code(aid, code, session)
                logger_11_c30.warning('Update video aid = %d code from 0 to %d.' % (aid, code))
                left_aids_solve_count += 1  # add left aids solve count
        except Exception as e:
            logger_11_c30.error('Exception caught when process view obj of left aid %d. Detail: %s' % (aid, e))

    # finish check aids left in aids
    logger_11_c30.warning(
        'Finish check aids left in aids, %d aid(s) visited, %d aids(s) solved.' % (
            left_aids_visit_count, left_aids_solve_count))

    # TODO check not added aids, maybe some video not added
    logger_11_c30.warning('%d aid(s) left in c30 not added aids, now check them.' % len(not_added_aids))
    logger_11_c30.warning(not_added_aids)

    not_added_aids_visit_count = len(not_added_aids)  # aids visited count
    not_added_aids_solve_count = 0  # aids solved count

    for aid in not_added_aids:
        # TODO change logic here, need to use stat api, some video doesn't have quanxian to add
        # get view obj
        view_obj = get_valid(bapi.get_video_view, (aid,), test_video_view)
        if view_obj is None:
            logger_11_c30.warning('Aid %d fail! Cannot get valid view obj.' % aid)
            continue

        # record view obj request ts
        view_obj_added = get_ts_s()

        try:
            # query video in db
            video = DBOperation.query_video_via_aid(aid, session)
            if video is None:
                # video not added in db, add video
                # TODO add video, including members, staff, category test
                logger_11_c30.warning(
                    'Aid %d not added in db, TODO add video, including members, staff, category test' % aid)

            # add video record, get stat first
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
            logger_11_c30.info('Add record %s.' % new_video_record)
            not_added_aids_solve_count += 1  # add not added aids solve count
        except Exception as e:
            logger_11_c30.error('Exception caught when process view obj of not added aid %d. Detail: %s' % (aid, e))

    # finish check not added aids
    logger_11_c30.warning(
        'Finish check not added aids, %d aid(s) visited, %d aids(s) solved.' % (
            not_added_aids_visit_count, not_added_aids_solve_count))

    # get finish ts
    finish_ts = get_ts_s()

    # make summary
    summary = \
        '11 updating c30 aids done\n\n' + \
        'start: %s, finish: %s, timespan: %ss\n\n' \
            % (ts_s_to_str(start_ts), ts_s_to_str(finish_ts), (finish_ts - start_ts)) + \
        'target aids count: %d\n\n' % aids_len + \
        'main loop: visited: %d, added: %s, others: %d\n\n' \
            % (main_loop_visit_count, main_loop_add_count, (main_loop_visit_count - main_loop_add_count)) + \
        'aids left in aids: visited: %d, solved: %d, others: %d\n\n' \
            % (left_aids_visit_count, left_aids_solve_count, (left_aids_visit_count - left_aids_solve_count)) + \
        'not added aids: visited: %d, solved: %d, others: %d\n\n' \
            % (not_added_aids_visit_count, not_added_aids_solve_count,
            (not_added_aids_visit_count - not_added_aids_solve_count)) + \
        'by.bunnyxt, %s' % ts_s_to_str(get_ts_s())

    logger_11.info('Finish updating c30 aids!')
    logger_11_c30.info('Finish updating c30 aids!')

    logger_11.warning(summary)
    logger_11_c30.warning(summary)

    # send sc
    sc_result = sc_send('Finish updating c30 aids!', summary)
    if sc_result['errno'] == 0:
        logger_11_c30.info('Sc summary sent successfully.')
    else:
        logger_11_c30.warning('Sc summary sent wrong. sc_result = %s.' % sc_result)

    session.close()


def daily_video_update():
    logger_11.info('Now start daily video update...')

    # update engine
    # update_engine()  # since update per 6 hours, no need to update engine

    # get videos aids
    session = Session()
    aids_c30 = DBOperation.query_update_c30_aids(0, session)
    logger_11.info('Get %d c30 aids.' % (len(aids_c30)))
    aids_c0 = DBOperation.query_update_c0_aids(0, session)
    logger_11.info('Get %d c0 aids.' % (len(aids_c0)))
    session.close()

    # start a thread to update aids_c0
    threading.Thread(target=update_aids_c30, args=(aids_c30,)).start()

    # start a thread to update aids_c30
    threading.Thread(target=update_aids_c0, args=(aids_c0,)).start()

    logger_11.info('Two thread started!')


def daily_video_update_task():
    threading.Thread(target=daily_video_update).start()


def main():
    logger_11.info('Daily video update registered.')
    daily_video_update_task()
    schedule.every().day.at("04:00").do(daily_video_update_task)
    # schedule.every().day.at("10:00").do(daily_video_update_task)
    # schedule.every().day.at("16:00").do(daily_video_update_task)
    # schedule.every().day.at("22:00").do(daily_video_update_task)

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    main()
