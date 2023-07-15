import time
from db import Session
import datetime
from timer import Timer
from serverchan import sc_send, sc_send_critical
from util import logging_init, get_ts_s, ts_s_to_str, get_current_line_no
import math
import logging

logger = logging.getLogger('72')


def add_sprint_daily():
    logger.info('Now start add sprint daily...')
    timer = Timer()
    timer.start()  # start timer

    session = Session()

    try:
        # get now time to format as date
        date = datetime.datetime.now().strftime('%Y%m%d')

        # calc start ts and end ts
        end_ts = int(time.mktime(time.strptime(date, "%Y%m%d"))) + 60 * 60 * 6
        start_ts = end_ts - 60 * 60 * 24

        # get start records
        result = session.execute(f'select aid, `view` from tdd_sprint_video_record '
                                 f'where added >= {start_ts} && added < {start_ts + 30 * 60}')
        start_records = [(r['aid'], r['view']) for r in result]
        start_records_view = {}
        for (aid, view) in start_records:
            if aid not in start_records_view.keys():
                start_records_view[aid] = view
        logger.info(f'Total {len(start_records_view)} start video got.')

        # get end records
        result = session.execute(f'select aid, `view` from tdd_sprint_video_record '
                                 f'where added >= {end_ts} && added < {end_ts + 30 * 60}')
        end_records = [(r['aid'], r['view']) for r in result]
        end_records_view = {}
        for (aid, view) in end_records:
            if aid not in end_records_view.keys():
                end_records_view[aid] = view
        logger.info(f'Total {len(end_records_view)} end video got.')

        # assemble records
        new_video_aids = []
        million_video_aids = []
        view_incr_total = 0
        video_total = 0
        for aid in set(list(start_records_view.keys()) + list(end_records_view.keys())):
            if aid not in start_records_view.keys():
                logger.info(f'New video detected. aid: {aid}')
                new_video_aids.append(aid)
                result = session.execute(f'select `view` from tdd_sprint_video_record '
                                         f'where aid = {aid} order by added limit 1')
                start_view = [r['view'] for r in result][0]
            else:
                start_view = start_records_view[aid]

            if aid not in end_records_view.keys():
                logger.info(f'New million video detected. aid: {aid}')
                million_video_aids.append(aid)
                end_view = 1000000
            else:
                end_view = end_records_view[aid]

            view_incr = end_view - start_view
            view_incr_total += view_incr

            if end_view == 1000000:
                continue

            # calc pday
            added = get_ts_s()
            result = session.execute(f'select created from tdd_sprint_video where aid = {aid}')
            created = [r['created'] for r in result][0]
            pday = math.floor((added - created) / (24 * 60 * 60))

            # calc lday
            # if no view incr, lday is 9999999
            lday = math.floor((1000000 - end_view) / view_incr) if view_incr != 0 else 9999999

            session.execute(f'insert into tdd_sprint_daily_record (added, `date`, aid, `view`, viewincr, pday, lday) '
                            f'values ({added}, "{date}", {aid}, {end_view}, {view_incr}, {pday}, {lday})')
            session.commit()
            logger.info(f'Add new daily record. '
                        f'added: {added}, date: {date}, aid: {aid}, end_view: {end_view}, view_incr: {view_incr}, '
                        f'pday: {pday}, lday: {lday}')

            video_total += 1

        # calc view incr incr
        result = session.execute('select `viewincr` from tdd_sprint_daily '
                                 'where viewincr is not NULL order by id desc limit 1')
        last_view_incr = [r['viewincr'] for r in result][0]
        view_incr_incr = view_incr_total - last_view_incr

        # make str
        newvids_str = ''
        for aid in new_video_aids:
            newvids_str += f'{aid};'
        millvids_str = ''
        for aid in million_video_aids:
            millvids_str += f'{aid};'

        session.execute(f'insert into tdd_sprint_daily '
                        f'(added, `date`, correct, vidnum, newvids, millvids, viewincr, viewincrincr, comment) '
                        f'values ({get_ts_s()}, "{date}", 1, {video_total}, "{newvids_str}", "{millvids_str}", '
                        f'{view_incr_total}, {view_incr_incr}, "")')
        session.commit()
        logger.info(f'Add new daily summary. '
                    f'added: {get_ts_s()}, date: {date}, video_total: {video_total}, '
                    f'newvids_str: {newvids_str}, millvids_str: {millvids_str}, '
                    f'view_incr_total: {view_incr_total}, view_incr_incr: {view_incr_incr}')
    except Exception as e:
        critical_title = f'Exception occurred when updating member total stat!'
        critical_message = f'error: {e}'
        logger.critical(f'{critical_title} {critical_message}')
        sc_send_critical(critical_title, critical_message, __file__, get_current_line_no())
        session.rollback()
        session.close()
        exit(1)

    timer.stop()  # stop timer

    # make summary
    summary = \
        '# add sprint daily done!\n\n' \
        f'{timer.get_summary()}\n\n' \
        f'date: {date}, video_total: {video_total}\n\n' \
        f'newvids_str: {newvids_str}, millvids_str: {millvids_str}\n\n' \
        f'view_incr_total: {view_incr_total}, view_incr_incr: {view_incr_incr}\n\n' \
        f'by bunnyxt, {ts_s_to_str(get_ts_s())}'

    logger.info('Finish add sprint daily!')
    logger.warning(summary)

    # send sc
    sc_send('Finish add sprint daily!', summary)

    session.close()


def main():
    add_sprint_daily()


if __name__ == '__main__':
    logging_init(file_prefix='72')
    main()
