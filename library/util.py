import conf
import urllib3

__all__ = ['check_cookie_validity', 'library_request']


def check_cookie_validity():
    response = library_request('GET', 'http://tianyi.biliran.moe/yuezheng')
    url = response.geturl()
    if url == 'http://tianyi.biliran.moe/yuezheng':
        return True
    else:
        return False


def library_request(method, url, fields=None):  # TODO what about post?
    cookie = conf.get_library_cookie()
    http = urllib3.PoolManager()
    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64)\
            AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.108 Safari/537.36',
        'Cookie': cookie
    }
    return http.request(method, url, fields=fields if fields else None, headers=header)
