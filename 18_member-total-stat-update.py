from db import Session, TddMemberTotalStatRecord
from util import logging_init, get_ts_s, fullname, \
    get_ts_ms, format_duration_summary
from runrecord import track
import logging
import time
from util.member_total_stat import aggregate_member_stats

script_id = '18'
script_name = 'member-total-stat-update'
script_fullname = fullname(script_id, script_name)
logger = logging.getLogger(script_id)


def member_total_stat_update():
    logger.info(f'Now start {script_fullname}...')
    start_ts_ms = get_ts_ms()

    with track(script_fullname) as recorder:
        session = Session()
        cnt = 0
        batch_size = 100
        batch_start = 1

        try:
            added = get_ts_s()
            read_start = time.perf_counter()
            members = dict(session.execute(
                'select mid, last_total_stat from tdd_member where mid > 0'))

            sql = 'select ' \
                  'v.aid, v.mid as v_mid, ' \
                  'r.view, r.danmaku, r.reply, r.favorite, r.coin, r.share, r.like, ' \
                  's.mid as s_mid, v.code, v.state, r.id ' \
                  'from ' \
                  'tdd_video v ' \
                  'left join tdd_video_record r on v.laststat = r.id ' \
                  'left join tdd_video_staff s on v.aid = s.aid ' \
                  'where ' \
                  'v.mid is not null && v.laststat is not null;'
            result = session.execute(sql)
            result = list(result)
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
            for index, values in enumerate(mid_dict.values(), 1):
                session.add(TddMemberTotalStatRecord(**values))
                if index % batch_size == 0:
                    session.commit()
                    cnt = index
                    logger.info(f'{cnt} / {mid_dict_len} added')
                    batch_start = cnt + 1
            if cnt < mid_dict_len:
                session.commit()
                cnt = mid_dict_len
                logger.info(f'{cnt} / {mid_dict_len} added')
            logger.info('Aggregate write duration: %.3fs, committed_rows=%d, batch_size=%d',
                        time.perf_counter() - write_start, cnt, batch_size)
        except Exception as e:
            message = f'Exception occurred when updating member total stat! error: {e}'
            logger.critical(message)
            logger.critical('Incomplete aggregate: added=%s, confirmed_committed_rows=%d, '
                            'next_batch_start=%d. Earlier batches remain; a failed commit '
                            'may have an unknown outcome. Do not rerun blindly: '
                            'there is no (mid, added) uniqueness constraint.',
                            locals().get('added'), cnt, batch_start)
            session.rollback()
            session.close()
            raise SystemExit(1)

        session.close()

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
