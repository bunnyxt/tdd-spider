import logging
from logutils import logging_init
from db import Session
from pybiliapi import BiliApi
from common import update_member, TddCommonError

logger = logging.getLogger('61')


def update_member_info_of_name_duplicated_member():
    logger.info('Now start update member info of name duplicated member...')

    session = Session()
    bapi = BiliApi()

    logger.info('Now get duplicated names...')
    results = session.execute(
        'select `name`, count(`name`) as count from tdd_member where `name` != "账号已注销" '
        'group by `name` having count > 1 order by count desc;'
    )
    duplicated_name_list = []
    for r in results:
        logger.info('%d members have duplicated name %s' % (r[1], r[0]))
        duplicated_name_list.append(r[0])
    if len(duplicated_name_list) == 0:
        logger.info('No duplicated name detected!')
    else:
        logger.info('%d duplicated names detected!' % len(duplicated_name_list))

        logger.info('Now get member mids with duplicated name...')
        results = session.execute('select mid from tdd_member where `name` in (%s);'
                                  % ', '.join(map(lambda name: '"%s"' % name, duplicated_name_list)))
        name_duplicated_mids = []
        for r in results:
            name_duplicated_mids.append(r[0])
        logger.info('%d name duplicated mids got!' % len(name_duplicated_mids))

        for i, mid in enumerate(name_duplicated_mids, 1):
            try:
                tdd_member_logs = update_member(mid, bapi, session)
            except TddCommonError as e:
                logger.error('Fail to update member info mid %d, TddCommonError Detail: %s' % (mid, e))
            except Exception as e:
                logger.error('Fail to update member info mid %d, Exception Detail: %s' % (mid, e))
            else:
                if len(tdd_member_logs) == 0:
                    logger.info('Member mid %d info not changed' % mid)
                else:
                    for log in tdd_member_logs:
                        logger.info('Member mid %d attribute %s changed from %s to %s' % (
                            log.mid, log.attr, log.oldval, log.newval))
                logger.debug('Finish update member info mid %d' % mid)

    logger.info('Finish update member info of name duplicated member!')

    session.close()


def main():
    update_member_info_of_name_duplicated_member()


if __name__ == '__main__':
    logging_init(file_prefix='61')
    main()
