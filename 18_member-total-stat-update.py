from db import Session, TddMemberTotalStatRecord
from util import logging_init, get_ts_s, get_current_line_no, fullname
from timer import Timer
from serverchan import sc_send_critical
import logging

script_id = '18'
script_name = 'member-total-stat-update'
script_fullname = fullname(script_id, script_name)
logger = logging.getLogger(script_id)


def member_total_stat_update():
    logger.info(f'Now start {script_fullname}...')
    timer = Timer()
    timer.start()

    session = Session()

    try:
        added = get_ts_s()

        sql = 'select ' \
              'v.aid, v.mid as v_mid, ' \
              'r.view, r.danmaku, r.reply, r.favorite, r.coin, r.share, r.like, ' \
              's.mid as s_mid ' \
              'from ' \
              'tdd_video v ' \
              'left join tdd_video_record r on v.laststat = r.id ' \
              'left join tdd_video_staff s on v.aid = s.aid ' \
              'where ' \
              'v.mid is not null && v.laststat is not null;'
        result = session.execute(sql)
        result = [[r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9]] for r in result]
        result_len = len(result)
        logger.info(f'Total {result_len} result got.')

        mid_dict = {}
        for r in result:
            mid = r[1] if r[9] is None else r[9]  # if has staff, use staff mid instead
            if mid not in mid_dict.keys():
                mid_dict[mid] = TddMemberTotalStatRecord(added, mid)  # init mid in dict
            mid_dict[mid].video_count += 1
            mid_dict[mid].view += r[2]
            mid_dict[mid].danmaku += r[3]
            mid_dict[mid].reply += r[4]
            mid_dict[mid].favorite += r[5]
            mid_dict[mid].coin += r[6]
            mid_dict[mid].share += r[7]
            mid_dict[mid].like += r[8]
        mid_dict_len = len(mid_dict)
        logger.info(f'Total {mid_dict_len} items in mid_dict created.')

        cnt = 0
        for v in mid_dict.values():
            session.add(v)
            cnt += 1
            if cnt % 100 == 0:
                session.commit()
                logger.info(f'{cnt} / {mid_dict_len} added')
        if cnt % 100 != 0:
            session.commit()
            logger.info(f'{cnt} / {mid_dict_len} added')
    except Exception as e:
        critical_title = 'Exception occurred when updating member total stat!'
        critical_message = f'error: {e}'
        logger.critical(f'{critical_title} {critical_message}')
        sc_send_critical(critical_title, critical_message, __file__, get_current_line_no())
        session.rollback()
        session.close()
        exit(1)

    session.close()

    timer.stop()

    # summary
    logger.info(f'Finish {script_fullname}!')
    logger.info(timer.get_summary())


def main():
    member_total_stat_update()


if __name__ == '__main__':
    logging_init(file_prefix=script_id)
    main()
