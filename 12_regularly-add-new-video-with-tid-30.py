from pybiliapi import BiliApi
from logger import logger_12
from db import Session, DBOperation, TddVideoRecord
import math
import time
import schedule
import threading
from common import get_valid, test_archive_rank_by_partion, add_video, add_video_via_bvid, \
    add_video_record_via_awesome_stat
from util import get_ts_s
from common.error import *


def regularly_add_new_video():
    logger_12.info('Now start add new video with tid 30...')

    bapi = BiliApi()
    session = Session()

    # get last added aids
    last_15_videos = DBOperation.query_last_x_video(15, session)
    # last_15_aids = list(map(lambda x: x.aid, last_15_videos))
    # logger_12.debug('Last 15 aids: %s' % last_15_aids)
    last_15_bvids = list(map(lambda x: x.bvid, last_15_videos))
    logger_12.debug('Last 15 bvids: %s' % last_15_bvids)

    # get page total
    obj = bapi.get_archive_rank_by_partion(30, 1, 50)
    page_total = math.ceil(obj['data']['page']['count'] / 50)
    logger_12.debug('%d page(s) found.' % page_total)

    # add add video
    page_num = 1
    goon = True
    # last_aid_count = 0
    # added_aid_count = 0
    last_bvid_count = 0
    added_bvid_count = 0
    while page_num <= page_total and goon:
        # get obj via awesome api
        obj = get_valid(bapi.get_archive_rank_by_partion, (30, page_num, 50), test_archive_rank_by_partion)
        if obj is None:
            logger_12.warning('Fail to get valid obj with page_num %d, continue to next page' % page_num)
            page_num += 1

        # process each video
        try:
            for arch in obj['data']['archives']:
                # aid = arch['aid']
                bvid = arch['bvid'][2:]  # remove BV prefix

                # check in last bvids or not
                if last_bvid_count >= 5:
                    logger_12.debug('5 last bvids meet, now break')
                    goon = False
                    break

                if bvid in last_15_bvids:
                    last_bvid_count += 1
                    logger_12.debug('Bvid %s in last bvids, count %d / 5, now continue' % (bvid, last_bvid_count))
                    continue

                # add video
                try:
                    # new_video = add_video(aid, bapi, session)
                    new_video = add_video_via_bvid(bvid, bapi, session)
                except TddCommonError as e:
                    logger_12.warning(e)
                else:
                    added_bvid_count += 1
                    logger_12.info('Add new video %s' % new_video)
                    # add stat record, which comes from awesome api
                    if 'stat' in arch.keys():
                        stat = arch['stat']
                        try:
                            new_video_record = add_video_record_via_awesome_stat(get_ts_s(), stat, session)
                        except TddCommonError as e:
                            logger_12.warning(e)
                        else:
                            logger_12.info('Add new video record %s' % new_video_record)
                    else:
                        logger_12.warning('Fail to get stat info of video with bvid %s from awesome api!' % bvid)

        except Exception as e:
            logger_12.error('Exception caught. Detail: %s' % e)

        # update page num
        logger_12.debug('Page %d / %d done.' % (page_num, page_total))
        page_total = math.ceil(obj['data']['page']['count'] / 50)
        page_num += 1

    session.close()
    logger_12.info('%d new video(s) added.' % added_bvid_count)
    logger_12.info('Finish add new video with tid 30!')


def regularly_add_new_video_task():
    threading.Thread(target=regularly_add_new_video).start()


def main():
    logger_12.info('Regularly add new video with tid 30 registered.')
    regularly_add_new_video_task()
    schedule.every(10).minutes.do(regularly_add_new_video_task)

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    main()
