import requests
import json
import random
from collections import namedtuple
from pathlib import Path
from typing import Optional
import logging

logger = logging.getLogger('Service')

# TODO: use typing.Literal in python 3.8+
# RequestMode = Literal['direct', 'worker', 'proxy']
RequestMode = str
VideoStat = namedtuple('VideoStat',
                       ['aid', 'view', 'danmaku', 'reply', 'favorite', 'coin', 'share', 'now_rank', 'his_rank', 'like',
                        'dislike', 'vt', 'vv'])

__all__ = ['Service', 'RequestMode', 'VideoStat']


class Service:

    def __init__(self, headers: dict = None):
        # set default config
        self.headers = headers if headers is not None else {}

        # load endpoints
        try:
            with Path(__file__).with_name('endpoints.json').open('r') as f:
                self.endpoints = json.load(f)
        except FileNotFoundError:
            logger.critical("The file 'endpoints.json' was not found.")
            exit(1)
        except json.JSONDecodeError:
            logger.critical('Invalid JSON format in endpoints.json.')
            exit(1)
        except Exception as e:
            logger.critical(f'An unexpected error occurred when load and parse endpoints.json file. {e}')
            exit(1)

        # define User Agent list
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

    def _get(self, url: str, params: dict = None, headers: dict = None) -> Optional[dict]:
        # assemble headers
        if headers is None:
            headers = self.headers
        else:
            headers = {**self.headers, **headers}
        # add User-Agent if not exists
        if 'User-Agent' not in headers:
            headers['User-Agent'] = random.choice(self._ua_list)

        # go request
        # TODO: add retry
        r = requests.get(url, params=params, headers=headers)
        if r.status_code != 200:
            logger.debug(f'Fail to get response with status code {r.status_code}. url: {url}, params: {params}')
            return None

        # parse response
        try:
            return r.json()
        except json.JSONDecodeError:
            logger.debug(f'Fail to decode response to json. Response: {r.text}, url: {url}, params: {params}')
            return None

    def get_video_stat(
            self, params: dict = None, headers: dict = None, mode: RequestMode = 'direct'
    ) -> Optional[VideoStat]:
        """
        params: { aid: int }
        mode: 'direct' | 'worker'
        """
        # validate params
        if mode not in ['direct', 'worker']:
            logger.critical(f'Invalid request mode: {mode}.')
            exit(1)

        # get endpoint url
        try:
            url = self.endpoints['get_video_stat']['direct']
            if mode == 'worker':
                url = random.choice(self.endpoints['get_video_stat']['workers'])
        except KeyError:
            logger.critical('Endpoint "get_video_stat" not found.')
            exit(1)

        # get response
        response = self._get(url, params=params, headers=headers)
        if response is None:
            logger.warning(f'Fail to get video stat. Params: {params}')
            return None

        # validate format

        # response should contain keys
        for key in ['code', 'message', 'ttl']:
            if key not in response.keys():
                logger.warning(
                    f'Invalid video stat response format. '
                    f'Response should contain key {key}. Response: {response}')
                return None
        # response code should be 0 and response data should be a dict
        if response['code'] != 0 or type(response['data']) != dict:
            logger.warning(
                f'Invalid video stat response format. '
                f'Response code should be 0 and should contain dict data. Response: {response}')
            return None
        # data should contain keys
        for key in ['aid', 'view', 'danmaku', 'reply', 'favorite', 'coin', 'share', 'now_rank', 'his_rank', 'like',
                    'dislike', 'vt', 'vv']:
            if key not in response['data'].keys():
                logger.warning(
                    f'Invalid video stat response format. '
                    f'Response data should contain key {key}. Response: {response}')
                return None

        # assemble data
        return VideoStat(
            aid=response['data']['aid'],
            view=response['data']['view'],
            danmaku=response['data']['danmaku'],
            reply=response['data']['reply'],
            favorite=response['data']['favorite'],
            coin=response['data']['coin'],
            share=response['data']['share'],
            now_rank=response['data']['now_rank'],
            his_rank=response['data']['his_rank'],
            like=response['data']['like'],
            dislike=response['data']['dislike'],
            vt=response['data']['vt'],
            vv=response['data']['vv']
        )
