import logging
from util import logging_init, get_ts_s, get_current_line_no
from db import Session, DBOperation, TddStatDaily
from timer import Timer
from serverchan import sc_send_critical

script_id = '13'
script_name = 'add-stat-daily'
logger = logging.getLogger(script_id)


def add_stat_daily():
    logger.info(f'Now start {script_id} - {script_name}...')
    timer = Timer()
    timer.start()

    session = Session()

    try:
        added = get_ts_s()
        video_count = DBOperation.count_table_until_ts('tdd_video', added, session)
        member_count = DBOperation.count_table_until_ts('tdd_member', added, session)
        video_record_count = DBOperation.count_table_until_ts('tdd_video_record', added, session)

        new_stat_daily = TddStatDaily(
            added=added,
            video_count=video_count,
            member_count=member_count,
            video_record_count=video_record_count
        )
        DBOperation.add(new_stat_daily, session)

        logger.info(f'Add stat daily done! new_stat_daily: {new_stat_daily}')
    except Exception as e:
        critical_title = 'Exception occurred when adding stat daily!'
        critical_message = f'error: {e}'
        logger.critical(f'{critical_title} {critical_message}')
        sc_send_critical(critical_title, critical_message, __file__, get_current_line_no())
        session.rollback()
        session.close()
        exit(1)

    session.close()

    timer.stop()

    # summary
    logger.info(f'Finish {script_id} - {script_name}!')
    logger.info(timer.get_summary())


def main():
    add_stat_daily()


if __name__ == '__main__':
    logging_init(file_prefix=script_id)
    main()
