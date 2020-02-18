import schedule
import sys
import time
from common import add_video_record_via_stat_api
from pybiliapi import BiliApi
from db import Session
import threading


def update(aid):
    bapi = BiliApi()
    session = Session()

    try:
        new_video_record = add_video_record_via_stat_api(aid, bapi, session)
        print(new_video_record)
    except Exception as e:
        print(e)

    session.close()


def update_task(aid):
    threading.Thread(target=update, args=(aid,)).start()


def main(aid):
    if aid is None:
        print('No aid assigned!')
        return

    update_task(aid)
    schedule.every().minutes.do(update_task, aid)

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    _aid = None
    if len(sys.argv) == 2:
        _aid = sys.argv[1]
    main(_aid)
