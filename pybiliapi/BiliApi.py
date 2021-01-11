import urllib3
import json
import logging

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
            self.timeout = 3
            self.retries = 1
            # check proxy pool url
            proxy_url = self._get_proxy_url()
            if not proxy_url:
                logging.critical('[BiliApi] Error! Cannot connect to proxy pool at %s' % self.proxy_pool_url)
                exit(1)
            self._last_valid_proxy_url = None
        else:
            self.use_proxy = False
            self.timeout = 5
            self.retries = 3

    def _get_proxy_url(self, https=False):
        if not self.use_proxy:
            logging.warning('[BiliApi] Error! Cannot get proxy url in direct mode.')
            return None

        r = self.http.request('GET', self.proxy_pool_url)
        if r.status == 200:
            proxy_url = r.data.decode()
            return 'http%s://%s' % ('s' if https else '', proxy_url)
        else:
            logging.warning('[BiliApi] Error! Fail to get proxy url from proxy pool %s.' % self.proxy_pool_url)
            return None

    def _url_request(self, method, url):
        if method not in ['GET']:
            logging.warning('[BiliApi] Error! Method %s not support.' % method)
            return None

        if self.use_proxy:
            for proxy_trial_index in range(1, self.max_proxy_trial+1):
                # get proxy url
                if self._last_valid_proxy_url:
                    proxy_url = self._last_valid_proxy_url
                    self._last_valid_proxy_url = None
                else:
                    proxy_url = self._get_proxy_url()
                if not proxy_url:
                    logging.critical('[BiliApi] Error! Cannot connect to proxy pool at %s.' % self.proxy_pool_url)
                    exit(1)

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
                    continue
                else:
                    logging.debug('[BiliApi] Get valid response at %d proxy trial(s).' % proxy_trial_index)
                    self._last_valid_proxy_url = proxy_url
                    return obj

            logging.warning('[BiliApi] Fail to get valid response after %d proxy trials.' % self.max_proxy_trial)
            return None
        else:
            response = self.http.request(method, url, timeout=self.timeout, retries=self.retries)
            if response.status != 200:
                logging.warning('[BiliApi] Error! Fail to get response with status code %d.' % response.status)
                return None

            try:
                html = response.data.decode()
                obj = json.loads(html)
                return obj
            except Exception as e:
                logging.warning('[BiliApi] Exception occurred during decode and parse json! %s' % e)
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
