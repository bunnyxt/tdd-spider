from ..instances import Proxieser
import requests


class LocalProxieser(Proxieser):
    # ref: https://github.com/Python3WebSpider/ProxyPool
    def __init__(self, sleep_time=10, random_url='http://localhost:5555/random', proxy_num=100):
        Proxieser.__init__(self, sleep_time=sleep_time)
        self._random_url = random_url
        self._proxy_num = proxy_num

    def proxies_get(self):
        proxies_list = []
        for _ in range(self._proxy_num):
            proxy_url = requests.get(self._random_url).text
            proxies_list.append({'http': proxy_url})
        return 1, proxies_list


class KdlProxieser(Proxieser):
    def __init__(self, order_id, apikey, sleep_time=5, random_url='https://dps.kdlapi.com/api/getdps', proxy_num=2):
        Proxieser.__init__(self, sleep_time=sleep_time)
        self._order_id = order_id
        self._apikey = apikey
        self._random_url = random_url
        self._proxy_num = proxy_num

    def proxies_get(self):
        proxies_list = []
        proxy_response = requests.get(self._random_url, params={
            'orderid': self._order_id,
            'num': self._proxy_num,
            'signature': self._apikey,
            'format': 'json',
            'sep': 1,
        }).json()
        if proxy_response['code'] == 0:
            for proxy_url in proxy_response['data']['proxy_list']:
                proxies_list.append({'http': proxy_url})
        return 1, proxies_list
