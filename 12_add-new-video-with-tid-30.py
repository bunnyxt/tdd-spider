from pybiliapi import BiliApi
from db import Session
from common import get_valid, test_archive_rank_by_partion, add_video_via_bvid, add_video_record_via_awesome_stat, \
    AlreadyExistError, TddCommonError
from util import get_ts_s
from conf import get_proxy_pool_url
from logutils import logging_init
import logging
logger = logging.getLogger('12')


def add_new_video_with_tid_30():
    logger.info('Now start add new video with tid 30...')
    start_ts = get_ts_s()  # get start ts

    session = Session()
    bapi_with_proxy = BiliApi(get_proxy_pool_url())

    # add add video
    page_num = 1
    added_bvid_count = 0
    while page_num <= 3:  # check latest 3 page
        # get page_obj via awesome api
        page_obj = get_valid(bapi_with_proxy.get_archive_rank_by_partion, (30, page_num, 50), test_archive_rank_by_partion)
        if page_obj is None:
            logger.warning('Fail to get valid obj with page_num %d, continue to next page' % page_num)
            page_num += 1
        # process each video
        try:
            for arch in page_obj['data']['archives']:
                bvid = arch['bvid'][2:]  # remove BV prefix
                # add video
                try:
                    new_video = add_video_via_bvid(bvid, bapi_with_proxy, session)
                except AlreadyExistError as e:
                    # video already added, completely common
                    # logger.debug('AlreadyExistError detected when add video bvid %s! Detail: %s' % (bvid, e))
                    pass
                except TddCommonError as e:
                    logger.warning('TddCommonError detected when add video bvid %s! Detail: %s' % (bvid, e))
                except Exception as e:
                    logger.warning('Exception caught when add video bvid %s! Detail: %s' % (bvid, e))
                else:
                    added_bvid_count += 1
                    logger.info('Add new video %s' % new_video)
                    # add stat record, which comes from awesome api
                    if 'stat' in arch.keys():
                        stat = arch['stat']
                        try:
                            new_video_record = add_video_record_via_awesome_stat(get_ts_s(), stat, session)
                        except TddCommonError as e:
                            logger.warning('TddCommonError detected when add new video record of video bvid %s! Detail: %s' % (bvid, e))
                        else:
                            logger.info('Add new video record %s' % new_video_record)
                    else:
                        logger.warning('Fail to get stat info of video with bvid %s from awesome api!' % bvid)
        except Exception as e:
            logger.error('Exception caught when traverse page_obj[\'data\'][\'archives\']. Detail: %s' % e)
        # update page num
        # logger.debug('Page %d done.' % page_num)
        page_num += 1

    session.close()
    end_ts = get_ts_s()
    logger.info('%d new video(s) added.' % added_bvid_count)
    logger.info('Finish add new video with tid 30! Time usage: %ds' % (end_ts - start_ts))


def main():
    add_new_video_with_tid_30()


if __name__ == '__main__':
    logging_init(file_prefix='12')
    main()
