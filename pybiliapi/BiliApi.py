import urllib3
import json

__all__ = ['BiliApi']

# disable InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class BiliApi:
    """core class for api call"""

    def __init__(self):
        self.http = urllib3.PoolManager()

    def _url_request(self, method, url):
        if method in ['GET']:
            response = self.http.request(method, url)
            if response.status == 200:
                try:
                    html = response.data.decode()
                    obj = json.loads(html)
                    return obj
                except Exception as e:
                    print(e)
                    return None
            else:
                print('Error! Fail to get response with status code %d.' % response.status)
                return None
        else:
            print('Error! Method %s not support.' % method)
            return None

    def set_headers(self, headers):
        self.http.headers = headers

    def get_video_view(self, aid):
        return self._url_request('GET', 'http://api.bilibili.com/x/web-interface/view?aid={0}'.format(aid))

    def get_video_tags(self, aid):
        return self._url_request('GET', 'http://api.bilibili.com/x/tag/archive/tags?aid={0}'.format(aid))

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
