import logging
from db import Session
from service import Service
from task import update_member
from common.error import TddError
from collections import defaultdict
from util import logging_init, get_ts_s, ts_s_to_str, format_ts_s, format_ts_ms, get_ts_ms, fullname
from serverchan import sc_send

script_id = '61'
script_name = 'update-member-info-of-name-duplicated-member'
script_fullname = fullname(script_id, script_name)
logger = logging.getLogger(script_id)


def update_member_info_of_name_duplicated_member():
    logger.info(f'Now start {script_fullname}...')
    start_ts = get_ts_s()  # get start ts

    session = Session()
    service = Service(mode="worker", retry=20)
    statistics = defaultdict(int)

    logger.info('Now get duplicated names...')
    results = session.execute(
        'select `name`, count(`name`) as count from tdd_member where `name` != "账号已注销" '
        'group by `name` having count > 1 order by count desc;'
    )
    duplicated_name_list = []
    for r in results:
        logger.info(f'{r[1]} members have duplicated name {r[0]}')
        duplicated_name_list.append(r[0])
    if len(duplicated_name_list) == 0:
        logger.info('No duplicated name detected!')
    else:
        logger.info(f'{len(duplicated_name_list)} duplicated names detected!')

        logger.info('Now get member mids with duplicated name...')
        results = session.execute('select mid from tdd_member where `name` in (%s);'
                                  % ', '.join(map(lambda name: '"%s"' % name, duplicated_name_list)))
        name_duplicated_mids = []
        for r in results:
            name_duplicated_mids.append(r[0])
        logger.info(f'{len(name_duplicated_mids)} name duplicated mids got!')

        for idx, mid in enumerate(name_duplicated_mids, 1):
            start_ts_ms = get_ts_ms()
            try:
                tdd_member_logs = update_member(mid, service, session)
            except TddError as e:
                logger.warning(f'Fail to update member info. mid: {mid}, error: {e}')
                statistics['tdd_error_count'] += 1
            except Exception as e:
                logger.warning(f'Fail to update member info. mid: {mid}, error: {e}')
                statistics['other_exception_count'] += 1
            else:
                if len(tdd_member_logs) == 0:
                    statistics['no_update_count'] += 1
                else:
                    statistics['change_count'] += 1
                for log in tdd_member_logs:
                    logger.info(f'Update member info. mid: {mid}, attr: {log.attr}, {log.oldval} -> {log.newval}')
                    statistics['change_log_count'] += 1
                logger.debug(f'{tdd_member_logs} log(s) found. mid: {mid}')
            end_ts_ms = get_ts_ms()
            cost_ms = end_ts_ms - start_ts_ms
            logger.debug(f'Finish update member info. mid: {mid}, cost: {format_ts_ms(cost_ms)}')
            statistics['total_count'] += 1
            statistics['total_cost_ms'] += cost_ms

    # get end ts
    end_ts = get_ts_s()

    # make summary
    summary = \
        'update member info of name duplicated name done!\n\n' \
        f'start: {ts_s_to_str(start_ts)}, ' \
        f'end: {ts_s_to_str(end_ts)}, ' \
        f'cost: {format_ts_s(end_ts - start_ts)}\n\n' \
        f'total count: {statistics["total_count"]}, ' + \
        f'average cost per service: {format_ts_ms(statistics["total_cost_ms"] // statistics["total_count"])}\n\n' \
        f'tdd error count: {statistics["tdd_error_count"]}\n\n' \
        f'other exception count: {statistics["other_exception_count"]}\n\n' \
        f'no update count: {statistics["no_update_count"]}\n\n' \
        f'change count: {statistics["change_count"]}\n\n' \
        f'change log count: {statistics["change_log_count"]}\n\n' \
        f'by.bunnyxt, {ts_s_to_str(get_ts_s())}'

    logger.info('Finish update member info of name duplicated member!')
    logger.warning(summary)

    # send sc
    sc_send('Finish update member info of name duplicated name!', summary)

    session.close()


def main():
    update_member_info_of_name_duplicated_member()


if __name__ == '__main__':
    logging_init(file_prefix=script_id)
    main()
