from pybiliapi import BiliApi
from logger import logger_12
from db import Session, DBOperation
import math
import time
import schedule
import threading
from common import get_valid, test_archive_rank_by_partion, add_video


def regularly_add_new_video():
    logger_12.info('Now start add new video with tid 30...')

    bapi = BiliApi()
    session = Session()

    # get last added aids
    last_15_videos = DBOperation.query_last_x_video(15, session)
    last_15_aids = list(map(lambda x: x.aid, last_15_videos))
    logger_12.info('Last 15 aids: %s' % last_15_aids)

    # get page total
    obj = bapi.get_archive_rank_by_partion(30, 1, 50)
    page_total = math.ceil(obj['data']['page']['count'] / 50)
    logger_12.info('%d page(s) found.' % page_total)

    # add add video
    page_num = 1
    goon = True
    last_aid_count = 0
    added_aid_count = 0
    while page_num <= page_total and goon:
        # get obj via awesome api
        obj = get_valid(bapi.get_archive_rank_by_partion, (30, page_num, 50), test_archive_rank_by_partion)
        if obj is None:
            logger_12.warning('Fail to get valid obj with page_num %d, continue to next page' % page_num)
            page_num += 1

        # process each video
        try:
            for arch in obj['data']['archives']:
                aid = arch['aid']

                # check in last aids or not
                if last_aid_count >= 5:
                    logger_12.info('5 last aids meet, now break')
                    goon = False
                    break

                if aid in last_15_aids:
                    last_aid_count += 1
                    logger_12.info('Aid %d in last aids, count %d / 5, now continue' % (aid, last_aid_count))
                    continue

                # add video
                add_video_result = add_video(aid, bapi, session)

                if add_video_result == 0:
                    logger_12.info('Add new video with aid %d!' % aid)
                elif add_video_result == 1:
                    logger_12.warning('Aid %d video already exist!' % aid)
                elif add_video_result == 2:
                    logger_12.warning('Fail to get valid view_obj with aid %d!' % aid)
                elif add_video_result == 3:
                    logger_12.warning('Video with aid %d code != 0!' % aid)
                else:
                    logger_12.warning('Unexpected result code %d got while add video with aid %d!'
                                      % (add_video_result, aid))

        except Exception as e:
            logger_12.error('Exception caught. Detail: %s' % e)

        # update page num
        logger_12.info('Page %d / %d done.' % (page_num, page_total))
        page_total = math.ceil(obj['data']['page']['count'] / 50)
        page_num += 1

    session.close()
    logger_12.info('%d new video(s) added.' % added_aid_count)
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
