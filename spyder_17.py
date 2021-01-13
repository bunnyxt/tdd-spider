from spider import WebSpider
from spider.custom import ApiFetcher, TddMemberFollowerRecordParser, DbSaver, LocalProxieser
import logging
from db import DBOperation, Session
from util import get_ts_s, ts_s_to_str
# from spider.custom import FileSaver


def create_web_spider(sequential=False):
    if sequential:
        # for residual fetch
        # no proxieser, longer fetcher sleep time, fewer fetch max repeat
        web_spider = WebSpider(fetcher=ApiFetcher(sleep_time=3, max_repeat=3),
                               parser=TddMemberFollowerRecordParser(),
                               saver=DbSaver(get_session=Session),
                               # saver=FileSaver(filename='17_save.log'),
                               queue_parse_size=200)
    else:
        # for main fetch
        web_spider = WebSpider(fetcher=ApiFetcher(sleep_time=0, max_repeat=10),
                               parser=TddMemberFollowerRecordParser(),
                               saver=DbSaver(get_session=Session),
                               # saver=FileSaver(filename='17_save.log'),
                               proxieser=LocalProxieser(sleep_time=5, proxy_num=100),
                               queue_parse_size=200, queue_proxies_size=500)
    return web_spider


def spider_17():
    start_ts = get_ts_s()

    # load all mids
    session = Session()
    mids = DBOperation.query_all_member_mids(session=session)
    session.close()

    # init urls
    left_url_list = []
    for mid in mids:
        left_url_list.append('http://api.bilibili.com/x/relation/stat?vmid={0}'.format(mid))

    # main fetch, multithreading spider
    spyder_round = 1
    left_url_num = len(left_url_list)
    max_spyder_round = 5  # at most 5 round
    least_left_url_num = 100  # at least 100 urls left to fetch
    while spyder_round <= max_spyder_round and left_url_num >= least_left_url_num:
        # create web spider
        web_spider = create_web_spider()

        # init add urls
        for url in left_url_list:
            web_spider.put_item_to_queue_fetch(1, url, {}, 0, 0)

        # launch web spider
        logging.warning('spyder round %d start, url total %d' % (spyder_round, left_url_num))
        web_spider.start_working(fetcher_num=50)
        web_spider.wait_for_finished()

        # update left urls
        left_url_list = web_spider.get_fail_url_list()
        left_url_num = len(left_url_list)
        logging.warning('spyder round %d done, url left %d' % (spyder_round, left_url_num))
        spyder_round += 1

    # residual fetch, sequential spider
    if left_url_num > 0:
        # create web spider
        web_spider = create_web_spider(sequential=True)

        # init add urls
        for url in left_url_list:
            web_spider.put_item_to_queue_fetch(1, url, {}, 0, 0)

        # launch web spider
        logging.warning('residual spyder start, url total %d' % left_url_num)
        web_spider.start_working(fetcher_num=1)  # one fetcher only
        web_spider.wait_for_finished()

        # update left urls
        left_url_list = web_spider.get_fail_url_list()
        left_url_num = len(left_url_list)
        logging.warning('residual spyder done, url left %d' % left_url_num)
        if left_url_num > 0:
            logging.warning('left url list: %s' % left_url_list)

    end_ts = get_ts_s()
    logging.warning('start time %s' % ts_s_to_str(start_ts))
    logging.warning('end time %s' % ts_s_to_str(end_ts))
    logging.warning('timespan %d min' % ((end_ts - start_ts) // 60))


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s\t%(levelname)s\t%(message)s")
    spider_17()
