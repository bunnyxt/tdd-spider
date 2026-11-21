from db.queries import (query_sprint_records_in_window, query_first_sprint_record,
                        query_sprint_video_created, query_last_sprint_daily_view_incr)
from db.sprint import insert_sprint_daily_record, insert_sprint_daily_summary
import datetime
from runrecord import track
from util import logging_init, get_ts_s, ts_s_to_str, \
    fullname, get_ts_ms, format_duration_summary
import math
import logging

script_id = '72'
script_name = 'add-sprint-daily'
script_fullname = fullname(script_id, script_name)
logger = logging.getLogger(script_id)


def add_sprint_daily():
    logger.info(f'Now start {script_fullname}...')
    start_ts_ms = get_ts_ms()

    with track(script_fullname) as recorder:
        try:
            # get now time to format as date
            cst = datetime.timezone(datetime.timedelta(hours=8))
            date = datetime.datetime.fromtimestamp(get_ts_s(), cst).strftime('%Y%m%d')

            # calc start ts and end ts
            end_ts = int(datetime.datetime.strptime(date, '%Y%m%d').replace(tzinfo=cst).timestamp()) + 60 * 60 * 6
            start_ts = end_ts - 60 * 60 * 24

            # get start records
            start_records = query_sprint_records_in_window(start_ts, start_ts + 30 * 60)
            start_records_view = {}
            for (aid, view) in start_records:
                if aid not in start_records_view.keys():
                    start_records_view[aid] = view
            logger.info(f'Total {len(start_records_view)} start video got.')

            # get end records
            end_records = query_sprint_records_in_window(end_ts, end_ts + 30 * 60)
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
                    first_record = query_first_sprint_record(aid)
                    if first_record is None:
                        raise ValueError(f'Sprint record missing for aid {aid}')
                    start_view = first_record[1]
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
                created = query_sprint_video_created(aid)
                if created is None:
                    raise ValueError(f'Sprint video missing for aid {aid}')
                pday = math.floor((added - created) / (24 * 60 * 60))

                # calc lday
                # if no view incr, lday is 9999999
                lday = math.floor((1000000 - end_view) / view_incr) if view_incr != 0 else 9999999

                insert_sprint_daily_record(added=added, date=date, aid=aid, view=end_view,
                                                  viewincr=view_incr, pday=pday, lday=lday)
                logger.info(f'Add new daily record. '
                            f'added: {added}, date: {date}, aid: {aid}, end_view: {end_view}, view_incr: {view_incr}, '
                            f'pday: {pday}, lday: {lday}')

                video_total += 1

            # calc view incr incr
            last_view_incr = query_last_sprint_daily_view_incr()
            if last_view_incr is None:
                raise ValueError('Previous sprint daily total is missing')
            view_incr_incr = view_incr_total - last_view_incr

            # make str
            newvids_str = ''
            for aid in new_video_aids:
                newvids_str += f'{aid};'
            millvids_str = ''
            for aid in million_video_aids:
                millvids_str += f'{aid};'

            insert_sprint_daily_summary(added=get_ts_s(), date=date, correct=1, vidnum=video_total,
                                               newvids=newvids_str, millvids=millvids_str, viewincr=view_incr_total,
                                               viewincrincr=view_incr_incr, comment='')
            logger.info(f'Add new daily summary. '
                        f'added: {get_ts_s()}, date: {date}, video_total: {video_total}, '
                        f'newvids_str: {newvids_str}, millvids_str: {millvids_str}, '
                        f'view_incr_total: {view_incr_total}, view_incr_incr: {view_incr_incr}')
        except Exception as e:
            message = f'Exception occurred when adding sprint daily! error: {e}'
            logger.critical(message)
            raise SystemExit(1)

        end_ts_ms = get_ts_ms()

        # run-record metrics (best-effort; a disabled recorder is a no-op).
        # Only the numbers this script already computes -- no fabricated JobStat,
        # and none of the message/aid-list text goes into the database.
        recorder.add_metric('sprint-daily', 'start_videos', len(start_records_view), 'count')
        recorder.add_metric('sprint-daily', 'end_videos', len(end_records_view), 'count')
        recorder.add_metric('sprint-daily', 'daily_records_added', video_total, 'count')
        recorder.add_metric('sprint-daily', 'new_videos', len(new_video_aids), 'count')
        recorder.add_metric('sprint-daily', 'million_videos', len(million_video_aids), 'count')
        recorder.add_metric('sprint-daily', 'view_incr_total', view_incr_total, 'views')
        recorder.add_metric('sprint-daily', 'view_incr_incr', view_incr_incr, 'views')

        # make summary
        summary = \
            '# add sprint daily done!\n\n' \
            f'{format_duration_summary(start_ts_ms, end_ts_ms)}\n\n' \
            f'date: {date}, video_total: {video_total}\n\n' \
            f'newvids_str: {newvids_str}, millvids_str: {millvids_str}\n\n' \
            f'view_incr_total: {view_incr_total}, view_incr_incr: {view_incr_incr}\n\n' \
            f'by bunnyxt, {ts_s_to_str(get_ts_s())}'

        logger.info('Finish add sprint daily!')
        logger.warning(summary)


def main():
    add_sprint_daily()


if __name__ == '__main__':
    logging_init(file_prefix=script_id)
    main()
