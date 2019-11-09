import schedule
import threading
import time
from logger import logger_11, logger_11_c0, logger_11_c30
from pybiliapi import BiliApi
import math
from db import update_engine, Session, DBOperation, TddVideoRecord
from util import get_ts_s


def update_aids_c0(aids):
    logger_11.info('Now update c0 aids...')
    logger_11_c0.info('Now update c0 aids...')
    bapi = BiliApi()
    session = Session()

    fail_aids = []
    for aid in aids:
        obj = bapi.get_video_stat(aid)
        is_valid = False
        re_count = 1
        while True:
            # ensure obj is valid
            try:
                _ = obj['code']
                is_valid = True
                break
            except Exception as e:
                # logger_11_c0.warning('Exception %s, re-call aid = %d, re_count  = %d' % (e, aid, re_count))
                re_count += 1
                if re_count == 5:
                    # logger_11_c0.warning('Fail to get valid stat obj with aid %d, continue to next aid' % aid)
                    break
                time.sleep(1)
                obj = bapi.get_video_stat(aid)
        if not is_valid:
            logger_11_c0.warning('Aid %d fail! Cannot get valid view_obj.' % aid)
            fail_aids.append(aid)
            continue

        added = get_ts_s()

        # check code
        code = obj['code']
        if code == 0:
            # add record
            stat = obj['data']

            view = -1 if stat['view'] == '--' else stat['view']
            danmaku = stat['danmaku']
            reply = stat['reply']
            favorite = stat['favorite']
            coin = stat['coin']
            share = stat['share']
            like = stat['like']

            record = TddVideoRecord()
            record.aid = aid
            record.added = added
            record.view = view
            record.danmaku = danmaku
            record.reply = reply
            record.favorite = favorite
            record.coin = coin
            record.share = share
            record.like = like

            DBOperation.add(record, session)
            logger_11_c0.info('Add record %s.' % record)
        else:
            # change code
            DBOperation.update_video_code(aid, code, session)
            logger_11_c0.warning('Update video aid = %d code from 0 to %d.' % (aid, code))

    session.close()
    logger_11_c0.warning('Fail aids: %s' % fail_aids)
    logger_11.info('Finish updating c0 aids!')
    logger_11_c0.info('Finish updating c0 aids!')


def update_aids_c30(aids):
    logger_11.info('Now update c30 aids...')
    logger_11_c30.info('Now update c30 aids...')
    bapi = BiliApi()
    session = Session()

    obj = bapi.get_archive_rank_by_partion(30, 1, 50)
    page_total = math.ceil(obj['data']['page']['count'] / 50)

    not_added_aids = []
    last_page_aids = []
    this_page_aids = []

    page_num = 1
    while page_num <= page_total:
        # get obj via awesome api
        obj = bapi.get_archive_rank_by_partion(30, page_num, 50)
        is_valid = False
        re_count = 1
        while True:
            # ensure obj is valid
            try:
                for _ in obj['data']['archives']:
                    pass
                is_valid = True
                break
            except TypeError:
                # logger_11_c30.warning('TypeError caught, re-call page_num = %d, re_count = %d' % (page_num, re_count))
                re_count += 1
                if re_count == 5:
                    # logger_11_c30.warning('Fail to get valid obj with page_num %d, continue to next page' % page_num)
                    break
                time.sleep(1)
                obj = bapi.get_archive_rank_by_partion(30, page_num, 50)
        if not is_valid:
            logger_11_c30.warning('Page num %d fail! Cannot get valid obj.' % page_num)
            page_num += 1
            continue

        added = get_ts_s()

        # process each video
        try:
            for arch in obj['data']['archives']:
                aid = arch['aid']
                if aid in last_page_aids:
                    logger_11_c30.warning('Aid %d already added in last page (page_num = %d).' % (aid, page_num - 1))
                    continue

                if aid in aids:
                    stat = arch['stat']

                    view = -1 if stat['view'] == '--' else stat['view']
                    danmaku = stat['danmaku']
                    reply = stat['reply']
                    favorite = stat['favorite']
                    coin = stat['coin']
                    share = stat['share']
                    like = stat['like']

                    record = TddVideoRecord()
                    record.aid = aid
                    record.added = added
                    record.view = view
                    record.danmaku = danmaku
                    record.reply = reply
                    record.favorite = favorite
                    record.coin = coin
                    record.share = share
                    record.like = like

                    DBOperation.add(record, session)
                    logger_11_c30.info('Add record %s.' % record)
                    this_page_aids.append(aid)
                    aids.remove(aid)
                else:
                    # record, video not added, or just updated
                    # logger_11_c30.warning('Aid %d not in update aids.' % aid)
                    not_added_aids.append(aid)
        except Exception as e:
            logger_11_c30.error('Exception caught. Detail: %s' % e)

        # reset aids records
        last_page_aids = this_page_aids
        this_page_aids = []

        # update page num
        logger_11_c30.info('Page %d / %d done.' % (page_num, page_total))
        page_total = math.ceil(obj['data']['page']['count'] / 50)
        page_num += 1

    # check other aid in aids, maybe video not exist, or not in tid=30, change code of them
    logger_11_c30.warning('%d aid left in c30 aids, now check them' % len(aids))
    logger_11_c30.warning(aids)
    for aid in aids:
        try:
            # get view
            view_obj = bapi.get_video_view(aid)
            is_valid = False
            re_count = 1
            while True:
                # ensure view obj is valid
                try:
                    _ = view_obj['code']
                    is_valid = True
                    break
                except Exception as e:
                    # logger_11_c30.warning('Exception %s, re-call view api aid = %d, re_count = %d' % (e, aid, re_count))
                    re_count += 1
                    if re_count == 5:
                        # logger_11_c30.warning('Fail to get valid view with aid %d, continue to next aid' % aid)
                        break
                    time.sleep(1)
                    view_obj = bapi.get_video_view(aid)
            if not is_valid:
                logger_11_c30.warning('Aid %d fail! Cannot get valid view_obj.' % aid)
                continue
            view_obj_added = get_ts_s()

            code = view_obj['code']
            if code == 0:
                # check tid
                if 'tid' in view_obj['data'].keys():
                    tid = view_obj['data']['tid']
                    if tid != 30:
                        DBOperation.update_video_tid(aid, tid, session)
                        DBOperation.update_video_isvc(aid, 5, session)
                        logger_11_c30.warning('Update video aid = %d tid from 30 to %d and set isvc = %d' % (aid, tid, 5))
                    else:
                        logger_11_c30.warning(
                            'Found aid = %d not been updated but code == 0 and tid == 30! Now try add...' % aid)
                        # add record
                        stat = view_obj['data']

                        view = -1 if stat['view'] == '--' else stat['view']
                        danmaku = stat['danmaku']
                        reply = stat['reply']
                        favorite = stat['favorite']
                        coin = stat['coin']
                        share = stat['share']
                        like = stat['like']

                        record = TddVideoRecord()
                        record.aid = aid
                        record.added = view_obj_added
                        record.view = view
                        record.danmaku = danmaku
                        record.reply = reply
                        record.favorite = favorite
                        record.coin = coin
                        record.share = share
                        record.like = like

                        DBOperation.add(record, session)
                        logger_11_c30.info('Add record %s.' % record)
                else:
                    logger_11_c30.error('View obj %s got code == 0 but no tid field! Need further check!' % view_obj)
            else:
                # change code
                DBOperation.update_video_code(aid, code, session)
                logger_11_c30.warning('Update video aid = %d code from 0 to %d.' % (aid, code))
        except Exception as e:
            logger_11_c30.error('Exception caught. Detail: %s' % e)

    # TODO check not added aids, maybe some video not added
    logger_11_c30.warning('%d aid left in c30 not added aids, now check them' % len(not_added_aids))
    logger_11_c30.warning(not_added_aids)
    # for aid in not_added_aids:
    #     # get view obj
    #     try:
    #         # get view
    #         view_obj = bapi.get_video_view(aid)
    #         is_valid = False
    #         re_count = 1
    #         while True:
    #             # ensure view obj is valid
    #             try:
    #                 _ = view_obj['code']
    #                 is_valid = True
    #                 break
    #             except Exception as e:
    #                 # logger_11_c30.warning('Exception %s, re-call view api aid = %d, re_count = %d' % (e, aid, re_count))
    #                 re_count += 1
    #                 if re_count == 5:
    #                     # logger_11_c30.warning('Fail to get valid view with aid %d, continue to next aid' % aid)
    #                     break
    #                 time.sleep(1)
    #                 view_obj = bapi.get_video_view(aid)
    #         if not is_valid:
    #             logger_11_c30.warning('Aid %d fail! Cannot get valid view_obj.' % aid)
    #             continue
    #
    #         video = DBOperation.query_video_via_aid(aid, session)
    #         if video is None:
    #             # add video
    #             pass
    #
    #         # add record
    #     except Exception as e:
    #         logger_11_c30.error('Exception caught. Detail: %s' % e)

    logger_11.info('Finish updating c30 aids!')
    session.close()


def daily_video_update():
    logger_11.info('Now start daily video update...')

    # update engine
    update_engine()

    # get videos aids
    session = Session()
    aids_c30 = DBOperation.query_update_c30_aids(0, session)
    logger_11.info('Get %d c30 aids.' % (len(aids_c30)))
    aids_c0 = DBOperation.query_update_c0_aids(0, session)
    logger_11.info('Get %d c0 aids.' % (len(aids_c0)))
    session.close()

    # start a thread to update aids_c0
    threading.Thread(target=update_aids_c30, args=(aids_c30, )).start()

    # start a thread to update aids_c30
    threading.Thread(target=update_aids_c0, args=(aids_c0, )).start()

    logger_11.info('Two thread started!')


def daily_video_update_task():
    threading.Thread(target=daily_video_update).start()


def main():
    logger_11.info('Daily video update registered.')
    daily_video_update_task()
    schedule.every().day.at("04:00").do(daily_video_update_task)

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    main()
