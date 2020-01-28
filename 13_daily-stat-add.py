import schedule
import time
from logger import logger_13
from util import get_ts_s
from db import Session, DBOperation, TddStatDaily


def daily_stat_add_task():
    added = get_ts_s()
    session = Session()

    try:
        video_count = DBOperation.count_table_until_ts('tdd_video', added, session)
        member_count = DBOperation.count_table_until_ts('tdd_member', added, session)
        video_record_count = DBOperation.count_table_until_ts('tdd_video_record', added, session)

        new_stat_daily = TddStatDaily()
        new_stat_daily.added = added
        new_stat_daily.video_count = video_count
        new_stat_daily.member_count = member_count
        new_stat_daily.video_record_count = video_record_count
        DBOperation.add(new_stat_daily, session)

        logger_13.info('Add %r' % new_stat_daily)
    except Exception as e:
        logger_13.warning(e)

    session.close()


def main():
    logger_13.info('Daily stat add registered.')
    # daily_stat_add_task()
    schedule.every().day.at("06:00").do(daily_stat_add_task)

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    main()
