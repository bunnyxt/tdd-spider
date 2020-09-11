from spider import WebSpider
from spider.custom import ApiFetcher, TddMemberFollowerRecordParser, DbSaver, LocalProxieser
import logging
from db import Session
from util import get_ts_s, ts_s_to_str


def spider_17():
    start_ts = get_ts_s()
    web_spider = WebSpider(fetcher=ApiFetcher(sleep_time=0, max_repeat=10),
                           parser=TddMemberFollowerRecordParser(),
                           saver=DbSaver(get_session=Session),
                           proxieser=LocalProxieser(sleep_time=5, proxy_num=100),
                           queue_parse_size=200, queue_proxies_size=300)

    # load all mids
    session = Session()
    result = session.execute('select m.mid from tdd_member m join tdd_member_follower_record r '
                             'on m.last_follower = r.id order by r.follower desc')
    mids = [r[0] for r in result]
    session.close()
    # add urls to fetch queue
    for mid in mids:
        url = 'http://api.bilibili.com/x/relation/stat?vmid={0}'.format(mid)
        web_spider.put_item_to_queue_fetch(1, url, {}, 0, 0)

    spyder_round = 1
    while True:
        web_spider.start_working(fetcher_num=50)
        web_spider.wait_for_finished()

        fetcher_fail_url_list = web_spider.get_fetcher_fail_url_list()
        if len(fetcher_fail_url_list) > 0:
            # go continue
            logging.warning('round %d done, url left %d' % (spyder_round, len(fetcher_fail_url_list)))
            spyder_round += 1

            web_spider = WebSpider(fetcher=ApiFetcher(sleep_time=0, max_repeat=10),
                                   parser=TddMemberFollowerRecordParser(),
                                   saver=DbSaver(get_session=Session),
                                   proxieser=LocalProxieser(sleep_time=5, proxy_num=100),
                                   queue_parse_size=200, queue_proxies_size=300)

            for url in fetcher_fail_url_list:
                web_spider.put_item_to_queue_fetch(1, url, {}, 0, 0)
        else:
            break
    logging.warning('round %d done, all finished' % spyder_round)

    end_ts = get_ts_s()
    logging.warning('start time %s' % ts_s_to_str(start_ts))
    logging.warning('end time %s' % ts_s_to_str(end_ts))
    logging.warning('timespan %d min' % ((end_ts - start_ts) // 60))


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s\t%(levelname)s\t%(message)s")
    spider_17()
