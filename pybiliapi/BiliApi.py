import urllib3
import json
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
            self.max_proxy_trial = 50
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
                    response = http.request(method, url, timeout=self.timeout, retries=self.retries)
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
                response = self.http.request(method, url, timeout=self.timeout, retries=self.retries)
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
        return self._url_request('GET', 'http://api.bilibili.com/x/web-interface/archive/stat?aid={0}'.format(aid))

    def get_member(self, mid):
        return self._url_request('GET', 'http://api.bilibili.com/x/space/acc/info?mid={0}'.format(mid))

    def get_member_relation(self, mid):
        return self._url_request('GET', 'http://api.bilibili.com/x/relation/stat?vmid={0}'.format(mid))

    def get_archive_rank_by_partion(self, tid, pn, ps):
        return self._url_request('GET', 'http://api.bilibili.com/archive_rank/getarchiverankbypartion?jsonp=jsonp&tid={0}&pn={1}&ps={2}'.format(tid, pn, ps))
