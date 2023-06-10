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
