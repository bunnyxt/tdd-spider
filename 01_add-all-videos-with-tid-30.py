from common import add_video_via_bvid, add_video_record_via_awesome_stat
from common.error import TddCommonError, AlreadyExistError
from common.iteration import iter_get_archive_rank_by_partion
from pybiliapi import BiliApi
from db import Session
from logutils import logging_init
import logging
logger = logging.getLogger('01')


def iter_func(iter_item, context, **iter_context):
    bvid = iter_item['bvid'][2:]
    bapi = context['bapi']
    session = context['session']

    # add video
    try:
        new_video = add_video_via_bvid(bvid, bapi, session)
    except AlreadyExistError as e:
        logger.debug(e)
    except TddCommonError as e:
        logger.warning(e)
    else:
        logger.info('Add new video %s' % new_video)

        # add stat record, which comes from awesome api
        if 'stat' in iter_item.keys():
            stat = iter_item['stat']
            try:
                new_video_record = add_video_record_via_awesome_stat(iter_context['added'], stat, session)
            except TddCommonError as e:
                logger.warning(e)
            else:
                logger.info('Add new video record %s' % new_video_record)
        else:
            logger.warning('Fail to get stat info of video with bvid %s from awesome api' % bvid)


def main():
    # build context
    bapi = BiliApi()
    session = Session()
    context = {
        'bapi': bapi,
        'session': session,
    }

    iter_get_archive_rank_by_partion(30, iter_func, context, start_page_num=1)

    session.close()


if __name__ == '__main__':
    logging_init(file_prefix='01')
    main()
