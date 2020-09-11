import spider
import logging
import requests
import json
import random
from db import TddMemberFollowerRecord, DBOperation, Session
from util import get_ts_s, ts_s_to_str


class ApiFetcher(spider.Fetcher):
    def random_ua(self):
        ua_list = [
            'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/14.0.835.163 Safari/535.1',
            'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:6.0) Gecko/20100101 Firefox/6.0',
            'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50',
            'Opera/9.80 (Windows NT 6.1; U; zh-cn) Presto/2.9.168 Version/11.50',
            'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 2.0.50727; SLCC2; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.3; .NET4.0C; Tablet PC 2.0; .NET4.0E)',
            'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; InfoPath.3)',
            'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.3; .NET4.0C; .NET4.0E) QQBrowser/6.9.11079.201',
            'User-Agent,Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 UBrowser/6.2.4094.1 Safari/537.36',
            'User-Agent,Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.2 Safari/605.1.15',
            'User-Agent, MQQBrowser/26 Mozilla/5.0 (Linux; U; Android 2.3.7; zh-cn; MB200 Build/GRJ22; CyanogenMod-7) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1'
        ]
        return random.choice(ua_list)

    def url_fetch(self, priority: int, url: str, keys: dict, deep: int, repeat: int, proxies=None):
        response = requests.get(url, proxies=proxies, verify=False, allow_redirects=False,
                                headers={'User-Agent': self.random_ua()}, timeout=(3.05, 10))
        response.raise_for_status()
        return 1, (response.status_code, response.url, response.text), 1  # fetch_state (1: success), content, proxies_state (1: success)


class TddMemberFollowerRecordParser(spider.Parser):
    def htm_parse(self, priority: int, url: str, keys: dict, deep: int, content: object):
        status_code, url_now, html_text = content
        obj = json.loads(html_text)
        item = TddMemberFollowerRecord()
        item.mid = obj['data']['mid']
        item.added = get_ts_s()
        if obj['code'] == 0:
            item.follower = obj['data']['follower']
        else:
            raise RuntimeError('api return code != 0')  # TODO
        return 1, [], item  # parse_state (1: success), url_list (do not add new urls), item (obj to be saved)


class DbSaver(spider.Saver):
    def __init__(self, get_session):
        spider.Saver.__init__(self)
        self._session = get_session()

    def item_save(self, priority: int, url: str, keys: dict, deep: int, item: dict):
        DBOperation.add(item, self._session)
        return 1, None


class LocalProxies(spider.Proxieser):
    # ref: https://github.com/Python3WebSpider/ProxyPool
    def __init__(self, sleep_time=10, random_url='http://localhost:5555/random', proxy_num=50):
        spider.Proxieser.__init__(self, sleep_time=sleep_time)
        self._random_url = random_url
        self._proxy_num = proxy_num

    def proxies_get(self):
        proxies_list = []
        for _ in range(self._proxy_num):
            proxy_url = requests.get(self._random_url).text
            proxies_list.append({'http': proxy_url})
        return 1, proxies_list


def spider_17():
    start_ts = get_ts_s()
    web_spider = spider.WebSpider(fetcher=ApiFetcher(sleep_time=0, max_repeat=10),
                                  parser=TddMemberFollowerRecordParser(),
                                  saver=DbSaver(get_session=Session),
                                  proxieser=LocalProxies(sleep_time=5, proxy_num=75),
                                  queue_parse_size=200, queue_proxies_size=200)

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

            web_spider = spider.WebSpider(fetcher=ApiFetcher(sleep_time=0, max_repeat=10),
                                          parser=TddMemberFollowerRecordParser(),
                                          saver=DbSaver(get_session=Session),
                                          proxieser=LocalProxies(sleep_time=5, proxy_num=75),
                                          queue_parse_size=200, queue_proxies_size=300)

            for url in fetcher_fail_url_list:
                web_spider.put_item_to_queue_fetch(1, url, {}, 0, 0)
        else:
            break
    logging.warning('round %d done, all finished' % spyder_round)

    end_ts = get_ts_s()
    logging.warning('start time %s' % ts_s_to_str(start_ts))
    logging.warning('end time %s' % ts_s_to_str(end_ts))
    logging.warning('timespan %d min' % (end_ts - start_ts) // 60)


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s\t%(levelname)s\t%(message)s")
    spider_17()
