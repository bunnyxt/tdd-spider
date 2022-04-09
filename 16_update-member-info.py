from logutils import logging_init
from db import DBOperation, Session
from util import get_ts_s, ts_s_to_str
from conf import get_kdl_order_id, get_kdl_apikey
from spider import WebSpider
from spider.custom import ApiFetcher, TddMemberParserForUpdate, DbListSaver, KdlProxieser
import logging
logger = logging.getLogger('16')


def update_member_info():
    logger.info('Now start update member info...')
    start_ts = get_ts_s()  # get start ts

    session = Session()

    # get all mids
    mids = DBOperation.query_all_member_mids(session)
    logger.info('Total %d mids got' % len(mids))

    # init urls
    left_url_list = []
    for mid in mids:
        left_url_list.append('http://api.bilibili.com/x/space/acc/info?mid=%d' % mid)

    # create web spider
    web_spider = WebSpider(fetcher=ApiFetcher(),
                           parser=TddMemberParserForUpdate(get_session=Session),
                           saver=DbListSaver(get_session=Session),
                           proxieser=KdlProxieser(
                               order_id=get_kdl_order_id(),
                               apikey=get_kdl_apikey(),
                               sleep_time=5,
                               proxy_num=2,
                           ),
                           queue_parse_size=100, queue_proxies_size=10)

    # init add urls
    for url in left_url_list:
        web_spider.put_item_to_queue_fetch(1, url, {}, 0, 0)

    # launch web spider
    logger.info('spyder start, urls total len %d' % len(left_url_list))
    web_spider.start_working(fetcher_num=10)
    web_spider.wait_for_finished()

    # get fail urls
    fail_url_list = web_spider.get_fail_url_list()
    logger.info('spyder finish, fail urls total len %d' % len(fail_url_list))
    logger.info('%s' % fail_url_list)

    end_ts = get_ts_s()
    logger.info('start time %s' % ts_s_to_str(start_ts))
    logger.info('end time %s' % ts_s_to_str(end_ts))
    logger.info('timespan %d min' % ((end_ts - start_ts) // 60))


def main():
    update_member_info()


if __name__ == '__main__':
    logging_init(file_prefix='16')
    main()
