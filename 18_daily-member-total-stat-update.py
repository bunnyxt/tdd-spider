import schedule
import logging
from logutils import logging_init
import time
import threading
from db import Session, TddMemberTotalStatRecord
from util import get_ts_s, ts_s_to_str
from serverchan import sc_send


def daily_member_total_stat_update():
    logging.info('Now start daily member total stat update...')

    session = Session()

    try:
        start_ts = get_ts_s()  # get start ts
        added = start_ts

        sql = 'select v.aid, v.mid as v_mid, r.view, r.danmaku, r.reply, r.favorite, r.coin, r.share, r.like, s.mid as s_mid ' + \
              'from tdd_video v left join tdd_video_record r on v.laststat = r.id left join tdd_video_staff s on v.aid = s.aid ' + \
              'where v.mid is not null && v.laststat is not null;'
        result = session.execute(sql)
        result = [[r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9]] for r in result]
        result_len = len(result)
        logging.info('%d result got' % result_len)

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

        count = 0
        mid_dict_len = len(mid_dict)
        for v in mid_dict.values():
            session.add(v)
            count += 1
            if count % 100 == 0:
                session.commit()
                logging.info('%d / %d added' % (count, mid_dict_len))

        finish_ts = get_ts_s()

        # make summary
        summary = \
            '18 daily member total stat update done\n\n' + \
            'start: %s, finish: %s, timespan: %ss\n\n' \
            % (ts_s_to_str(start_ts), ts_s_to_str(finish_ts), (finish_ts - start_ts)) + \
            'result len: %d\n\n' % result_len + \
            'mid dict len: %d\n\n' % mid_dict_len + \
            'by.bunnyxt, %s' % ts_s_to_str(get_ts_s())

        logging.info('Finish daily member total stat update!')

        logging.info(summary)

        # send sc
        sc_result = sc_send('Finish daily member total stat update!', summary)
        if sc_result['errno'] == 0:
            logging.info('Sc summary sent successfully.')
        else:
            logging.warning('Sc summary sent wrong. sc_result = %s.' % sc_result)
    except Exception as e:
        logging.exception(e)

    session.close()


def daily_member_total_stat_update_task():
    threading.Thread(target=daily_member_total_stat_update).start()


def main():
    logging.info('18: daily member total stat update')
    logging.info('will execute everyday at 04:45')
    schedule.every().day.at("04:45").do(daily_member_total_stat_update_task)  # ensure do after daily 0400 update

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    logging_init(file_prefix='18')
    main()
