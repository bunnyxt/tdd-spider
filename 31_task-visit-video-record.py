import time
from db import DBOperation, Session
from pybiliapi import BiliApi
from common import add_video_record_via_stat_api, InvalidObjCodeError, TddCommonError
from logger import logger_31
from util import get_ts_s


def main():
    while True:
        logger_31.info('Now start process task visit video record...')
        session = Session()

        # get tasks
        tasks = DBOperation.query_task_video_record(session)

        task_total = len(tasks)
        if task_total == 0:
            logger_31.info('No tasks left now.')
            session.close()
            time.sleep(30)
            continue

        task_success = 0
        task_in_cd = 0
        task_fail = 0

        # process tasks
        bapi = BiliApi()
        for task in tasks:
            time.sleep(0.2)
            aid = task.aid

            # get last added
            last_added = DBOperation.query_last_added_via_aid(aid, session)
            if not last_added:
                logger_31.warning('Cannot find last added of aid %d' % aid)
                task.status = 3
                session.commit()
                task_fail += 1
                continue

            # check last added
            added = get_ts_s()
            if added - last_added <= 5 * 60:
                task.status = 2
                session.commit()
                logger_31.info('Video aid %d record added in last 5 min' % aid)
                task_in_cd += 1
                continue

            # add video record
            try:
                new_video_record = add_video_record_via_stat_api(aid, bapi, session)
            except InvalidObjCodeError as e:
                DBOperation.update_video_code(aid, e.code, session)
                logger_31.warning('Update video aid = %d code from 0 to %d.' % (aid, e.code))
                task.status = 3
                session.commit()
                task_fail += 1
            except TddCommonError as e:
                logger_31.warning(e)
                task.status = 3
                session.commit()
                task_fail += 1
            else:
                logger_31.info('Add new record %s' % new_video_record)
                task.status = 1
                session.commit()
                task_success += 1

        logger_31.info('Done! total: %d, success: %d, in cd: %d, fail: %d'
                       % (task_total, task_success, task_in_cd, task_fail))

        session.close()


if __name__ == '__main__':
    main()
