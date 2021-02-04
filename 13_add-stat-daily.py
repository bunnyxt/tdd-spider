import logging
from logutils import logging_init
from util import get_ts_s
from db import Session, DBOperation, TddStatDaily
logger = logging.getLogger('13')


def add_stat_daily():
    logger.info('Now start add stat daily...')
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

        logger.info('Finish add stat daily %r!' % new_stat_daily)
    except Exception as e:
        logger.warning('Fail to add stat daily! Exception caught. Detail: %s' % e)

    session.close()


def main():
    add_stat_daily()


if __name__ == '__main__':
    logging_init(file_prefix='13')
    main()
