from db import Session
from util import logging_init, get_ts_s, fullname, \
    get_ts_ms, format_duration_summary
from runrecord import track
import logging
import time
from util.member_total_stat import aggregate_member_stats
from util.member_total_stat_writer import write_member_stats

script_id = '18'
script_name = 'member-total-stat-update'
script_fullname = fullname(script_id, script_name)
logger = logging.getLogger(script_id)


def member_total_stat_update():
    logger.info(f'Now start {script_fullname}...')
    start_ts_ms = get_ts_ms()

    with track(script_fullname) as recorder:
        connection = Session()
        cnt = 0
        batch_size = 1000

        try:
            added = get_ts_s()
            read_start = time.perf_counter()
            sql = 'select ' \
                  'v.aid, v.mid as v_mid, ' \
                  'r.view, r.danmaku, r.reply, r.favorite, r.coin, r.share, r.like, ' \
                  's.mid as s_mid, v.code, v.state, r.id ' \
                  'from ' \
                  'tdd_video v ' \
                  'left join tdd_video_record_latest r on v.aid = r.aid ' \
                  'left join tdd_video_staff s on v.aid = s.aid ' \
                  'where ' \
                  'v.mid is not null AND r.id is not null;'
            members = {r[0]:r[1] for r in connection.execute(
                'SELECT m.mid, r.id FROM tdd_member m '
                'LEFT JOIN tdd_member_total_stat_record_latest r ON m.mid = r.mid WHERE m.mid > 0')}
            # Include previous aggregate entities whose profile has never arrived.
            members.update({r[0]:r[1] for r in connection.execute(
                'SELECT mid, id FROM tdd_member_total_stat_record_latest WHERE mid > 0')})
            result = [tuple(row.values()) for row in connection.execute(sql)]
            result_len = len(result)
            logger.info(f'Total {result_len} result got.')
            logger.info('Aggregate read duration: %.3fs', time.perf_counter() - read_start)

            aggregate_start = time.perf_counter()
            mid_dict = aggregate_member_stats(result, members, added, logger)
            logger.info('Aggregate calculation duration: %.3fs',
                        time.perf_counter() - aggregate_start)
            del result, members
            mid_dict_len = len(mid_dict)
            logger.info(f'Total {mid_dict_len} items in mid_dict created.')

            write_start = time.perf_counter()
            cnt = write_member_stats(connection, mid_dict.values(), logger, batch_size)
            if cnt:
                logger.info(f'{cnt} / {mid_dict_len} added')
            logger.info('Aggregate write duration: %.3fs, committed_rows=%d, batch_size=%d',
                        time.perf_counter() - write_start, cnt, batch_size)
        except Exception as e:
            message = f'Exception occurred when updating member total stat! error: {e}'
            logger.critical(message)
            logger.critical('Incomplete aggregate snapshot: added=%s', locals().get('added'))
            connection.rollback()
            raise SystemExit(1)
        finally:
            connection.close()

        end_ts_ms = get_ts_ms()

        # run-record metrics (best-effort; a disabled recorder is a no-op).
        # Only the counts this script already computes -- no fabricated JobStat.
        recorder.add_metric('member-total-stat', 'result_rows', result_len, 'count')
        recorder.add_metric('member-total-stat', 'member_count', mid_dict_len, 'count')
        recorder.add_metric('member-total-stat', 'records_added', cnt, 'count')

        # summary
        logger.info(f'Finish {script_fullname}!')
        logger.info(format_duration_summary(start_ts_ms, end_ts_ms))


def main():
    member_total_stat_update()


if __name__ == '__main__':
    logging_init(file_prefix=script_id)
    main()
