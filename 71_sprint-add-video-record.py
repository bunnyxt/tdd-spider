import schedule
import time
from logger import logger_71
import threading
from db import Session
from pybiliapi import BiliApi
from util import get_ts_s


def add_video_record():
    bapi = BiliApi()
    session = Session()
    logger_71.info('now start adding videos...')

    # load processing video aids from db
    result = session.execute('select aid from tdd_sprint_video where status = "processing"')
    aids = [r['aid'] for r in result]
    logger_71.info('get %d videos' % len(aids))

    for aid in aids:
        try:
            stat = bapi.get_video_stat(aid)
            added = get_ts_s()
            view = stat['data']['view'] if type(stat['data']['view']) == int else -1
            danmaku = stat['data']['danmaku']
            reply = stat['data']['reply']
            favorite = stat['data']['favorite']
            coin = stat['data']['coin']
            share = stat['data']['share']
            like = stat['data']['like']
            sql = 'insert into tdd_sprint_video_record ' \
                  '(added, aid, `view`, danmaku, reply, favorite, coin, `share`, `like`) ' \
                  'values ' \
                  '(%d, %d, %d, %d, %d, %d, %d, %d, %d)' % \
                  (added, aid, view, danmaku, reply, favorite, coin, share, like)
            session.execute(sql)
            session.commit()
            logger_71.info('%d, %d, %d, %d, %d, %d, %d, %d, %d' %
                           (added, aid, view, danmaku, reply, favorite, coin, share, like))
        except Exception as e:
            logger_71.warning(e)
        time.sleep(0.2)

    logger_71.info('finish add %d video records' % len(aids))


def add_video_record_task():
    threading.Thread(target=add_video_record).start()


def main():
    logger_71.info('sprint add video record registered')
    add_video_record()
    schedule.every(10).minutes.do(add_video_record_task)

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    main()
