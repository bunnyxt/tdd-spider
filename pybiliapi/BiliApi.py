import urllib3
import json
import random
from conf import get_video_stat_cfw_url
import logging
logger = logging.getLogger('BiliApi')

__all__ = ['BiliApi']

# disable InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class BiliApi:
    """core class for api call"""

    def __init__(self, proxy_pool_url=None):
        self.http = urllib3.PoolManager()
        if proxy_pool_url:
            # using ProxyPool, https://github.com/Python3WebSpider/ProxyPool
            self.use_proxy = True
            self.proxy_pool_url = proxy_pool_url
            self.max_proxy_trial = 20
            self.timeout = 2  # may have risk of missing valid proxy, but reduce seeking proxy time!
            self.retries = 1
            # check proxy pool url
            proxy_url = self._get_proxy_url()
            if not proxy_url:
                logger.critical('Error! Cannot connect to proxy pool at %s.' % self.proxy_pool_url)
                raise ConnectionError('Cannot connect to proxy pool at %s.' % self.proxy_pool_url)
            self._last_valid_proxy_url = None
        else:
            self.use_proxy = False
            self.timeout = 5
            self.retries = 3
        self._ua_list = [
            # PC Browser
            # Google,win
            r'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36',
            # Google,mac
            r'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36',
            # Google,linux
            r'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36',
            # Opera,win
            r'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.87 Safari/537.36 OPR/37.0.2178.31',
            # Opera,mac
            r'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.87 Safari/537.36 OPR/37.0.2178.31',
            # Firefox,win
            r'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:46.0) Gecko/20100101 Firefox/46.0',
            # Firefox,mac
            r'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.11; rv:46.0) Gecko/20100101 Firefox/46.0',
            # Safari,mac
            r'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/7046A194A',
            # 360 browser
            r'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; 360SE)',
            # Sogou browser
            r'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SE 2.X MetaSr 1.0; SE 2.X MetaSr 1.0; .NET CLR 2.0.50727; SE 2.X MetaSr 1.0)',
            # UC browser
            r'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 UBrowser/6.2.4094.1 Safari/537.36',
            # Internet Explorer 8
            r'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0)',
            # Internet Explorer 9
            r'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)',

            # Mobile Browser
            # Android QQ browser For android
            r'MQQBrowser/26 Mozilla/5.0 (Linux; U; Android 2.3.7; zh-cn; MB200 Build/GRJ22; CyanogenMod-7) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1',
            # Android Opera Mobile
            r'Opera/9.80 (Android 2.3.4; Linux; Opera Mobi/build-1107180945; U; en-GB) Presto/2.8.149 Version/11.10',
            # BlackBerry
            r'Mozilla/5.0 (BlackBerry; U; BlackBerry 9800; en) AppleWebKit/534.1+ (KHTML, like Gecko) Version/6.0.0.337 Mobile Safari/534.1+',
            # Nokia N97
            r'Mozilla/5.0 (SymbianOS/9.4; Series60/5.0 NokiaN97-1/20.0.019; Profile/MIDP-2.1 Configuration/CLDC-1.1) AppleWebKit/525 (KHTML, like Gecko) BrowserNG/7.1.18124',
            # Android N1
            r'Mozilla/5.0 (Linux; U; Android 2.3.7; en-us; Nexus One Build/FRF91) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1'
        ]

    def _get_random_ua(self):
        return random.choice(self._ua_list)

    def _get_proxy_url(self, https=False):
        if not self.use_proxy:
            logger.warning('Error! Cannot get proxy url in direct mode.')
            return None

        try:
            r = self.http.request('GET', self.proxy_pool_url)
            if r.status != 200:
                raise Exception
            proxy_url = r.data.decode()
            return 'http%s://%s' % ('s' if https else '', proxy_url)
        except Exception:
            logger.warning('Fail to get proxy url from proxy pool %s.' % self.proxy_pool_url)
            return None

    def _url_request(self, method, url):
        if method not in ['GET']:
            logger.warning('Method %s not support.' % method)
            return None

        if self.use_proxy:
            for proxy_trial_index in range(1, self.max_proxy_trial+1):
                # get proxy url
                if self._last_valid_proxy_url:
                    proxy_url = self._last_valid_proxy_url
                    self._last_valid_proxy_url = None  # set to None to ensure only use once
                else:
                    proxy_url = self._get_proxy_url()
                if not proxy_url:
                    logger.critical('Cannot connect to proxy pool at %s.' % self.proxy_pool_url)
                    raise ConnectionError('Cannot connect to proxy pool at %s.' % self.proxy_pool_url)

                # create proxy http
                http = urllib3.ProxyManager(proxy_url)

                # go request
                try:
                    response = http.request(method, url, timeout=self.timeout, retries=self.retries, headers={'User-Agent': self._get_random_ua()})
                    if response.status != 200:
                        continue
                    html = response.data.decode()
                    obj = json.loads(html)
                except Exception:
                    logger.debug('Fail to get valid response at %d proxy trial(s).' % proxy_trial_index)
                    continue
                else:
                    logger.debug('Get valid response at %d proxy trial(s).' % proxy_trial_index)
                    self._last_valid_proxy_url = proxy_url
                    return obj

            logger.warning('Fail to get valid response after %d proxy trials.' % self.max_proxy_trial)
            return None
        else:
            try:
                response = self.http.request(method, url, timeout=self.timeout, retries=self.retries, headers={'User-Agent': self._get_random_ua()})
                if response.status != 200:
                    logger.warning('Fail to get response with status code %d.' % response.status)
                    return None
                html = response.data.decode()
                obj = json.loads(html)
                return obj
            except Exception as e:
                logger.warning('Exception occurred during request, decode and parse json! %s' % e)
                return None

    def get_video_view(self, aid):
        return self._url_request('GET', 'http://api.bilibili.com/x/web-interface/view?aid={0}'.format(aid))

    def get_video_view_via_bvid(self, bvid):
        # bvid with BV/bv prefix is also acceptable
        return self._url_request('GET', 'http://api.bilibili.com/x/web-interface/view?bvid={0}'.format(bvid))

    def get_video_tags(self, aid):
        return self._url_request('GET', 'http://api.bilibili.com/x/tag/archive/tags?aid={0}'.format(aid))

    def get_video_tags_via_bvid(self, bvid):
        # bvid with BV/bv prefix is also acceptable
        return self._url_request('GET', 'http://api.bilibili.com/x/tag/archive/tags?bvid={0}'.format(bvid))

    def get_video_pagelist(self, aid):
        return self._url_request('GET', 'http://api.bilibili.com/x/player/pagelist?aid={0}'.format(aid))

    def get_video_stat(self, aid):
        return self._url_request('GET', 'http://api.bilibili.com/archive_stat/stat?aid={0}'.format(aid))

    def get_video_stat_cfw(self, aid):
        return self._url_request('GET', get_video_stat_cfw_url() + '?aid={0}'.format(aid))

    def get_member(self, mid):
        return self._url_request('GET', 'http://api.bilibili.com/x/space/acc/info?mid={0}'.format(mid))

    def get_member_relation(self, mid):
        return self._url_request('GET', 'http://api.bilibili.com/x/relation/stat?vmid={0}'.format(mid))

    def get_archive_rank_by_partion(self, tid, pn, ps):
        return self._url_request('GET', 'http://api.bilibili.com/archive_rank/getarchiverankbypartion?jsonp=jsonp&tid={0}&pn={1}&ps={2}'.format(tid, pn, ps))
