import requests
import json
import time
import random
from collections import namedtuple
from pathlib import Path
from typing import Optional, Callable
from common.error import TddError
import logging

logger = logging.getLogger('Service')

# TODO: use typing.Literal in python 3.8+
# RequestMode = Literal['direct', 'worker', 'proxy']
RequestMode = str
VideoStat = namedtuple('VideoStat',
                       ['aid', 'view', 'danmaku', 'reply', 'favorite', 'coin', 'share', 'now_rank', 'his_rank', 'like',
                        'dislike', 'vt', 'vv'])

__all__ = ['Service',
           'ServiceError', 'ResponseError', 'ValidationError', 'FormatError', 'CodeError',
           'RequestMode', 'VideoStat']


class ServiceError(TddError):
    def __init__(self):
        super().__init__()

    def __str__(self):
        return '<ServiceError>'


class ResponseError(ServiceError):
    def __init__(self, target: str, params: dict):
        super().__init__()
        self.target = target
        self.params = params

    def __str__(self):
        return f'<ResponseError(target={self.target},params={self.params})>'


class ValidationError(ServiceError):
    def __init__(self, target: str, params: dict, response: dict):
        super().__init__()
        self.target = target
        self.params = params
        self.response = response

    def __str__(self):
        return f'<ValidationError(target={self.target},params={self.params},response={self.response})>'


class FormatError(ValidationError):
    def __init__(self, target: str, params: dict, response: dict, message: str):
        super().__init__(target, params, response)
        self.message = message

    def __str__(self):
        return f'<FormatError(target={self.target},params={self.params},response={self.response},message={self.message})>'


class CodeError(ValidationError):
    def __init__(self, target: str, params: dict, response: dict, code: int):
        super().__init__(target, params, response)
        self.code = code

    def __str__(self):
        return f'<CodeError(target={self.target},params={self.params},response={self.response},code={self.code})>'


class Service:

    def __init__(
            self, headers: dict = None, retry: int = 3, timeout: float = 5.0, colddown_factor: float = 1.0,
            mode: RequestMode = 'direct', get_proxy_url: Callable = None
    ):
        # set default config
        self._headers = headers if headers is not None else {}
        self._retry = retry
        self._timeout = timeout
        self._colddown_factor = colddown_factor
        self._mode = mode
        self._get_proxy_url = get_proxy_url

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

    def _get(
            self, url: str, params: dict = None, headers: dict = None,
            retry: int = None, timeout: float = None, colddown_factor: float = None,
            get_proxy_url: Callable = None
    ) -> Optional[dict]:
        # assemble headers
        if headers is None:
            headers = self._headers
        else:
            headers = {**self._headers, **headers}
        # add User-Agent if not exists
        if 'User-Agent' not in headers:
            headers['User-Agent'] = random.choice(self._ua_list)

        # config
        retry = retry if retry is not None else self._retry
        timeout = timeout if timeout is not None else self._timeout
        colddown_factor = colddown_factor if colddown_factor is not None else self._colddown_factor

        # go request
        response = None
        for trial in range(1, retry + 1):
            # colddown for retry
            if trial > 1:
                # fluctuation range 0.75 ~ 1.25
                time.sleep((trial - 1) * (random.random() * 0.5 + 0.75) * colddown_factor)

            # get proxy
            proxies = None
            if get_proxy_url is not None:
                proxy_url = get_proxy_url()
                proxies = {
                    'http': proxy_url,
                }

            # try to get response
            try:
                r = requests.get(url, params=params, headers=headers, timeout=timeout, proxies=proxies)
            except requests.exceptions.RequestException as e:
                logger.debug(
                    f'Fail to get response. '
                    f'url: {url}, params: {params}, trial: {trial}, error: {e}'
                )
                continue

            # check status code
            if r.status_code != 200:
                logger.debug(
                    f'Fail to get response with status code {r.status_code}. '
                    f'url: {url}, params: {params}, trial: {trial}'
                )
                continue

            # parse response
            try:
                response = r.json()
                break
            except json.JSONDecodeError:
                logger.debug(
                    f'Fail to decode response to json. '
                    f'Response: {r.text}, url: {url}, params: {params}, trial: {trial}'
                )
                continue
        return response

    def get_video_stat(
            self, params: dict = None, headers: dict = None,
            retry: int = None, timeout: float = None, colddown_factor: float = None,
            mode: RequestMode = None, get_proxy_url: Callable = None
    ) -> VideoStat:
        """
        params: { aid: int }
        mode: 'direct' | 'worker' | 'proxy'
        """
        # config mode and get_proxy_url
        mode = mode if mode is not None else self._mode
        get_proxy_url = get_proxy_url if get_proxy_url is not None else self._get_proxy_url

        # validate params
        if mode not in ['direct', 'worker', 'proxy']:
            logger.critical(f'Invalid request mode: {mode}.')
            exit(1)
        if mode == 'proxy' and get_proxy_url is None:
            logger.critical('Proxy mode requires get_proxy_url function.')
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
        response = self._get(url, params=params, headers=headers,
                             retry=retry, timeout=timeout, colddown_factor=colddown_factor,
                             get_proxy_url=get_proxy_url if mode == 'proxy' else None)
        if response is None:
            raise ResponseError('video_stat', params)

        # validate format

        # response should contain keys
        for key in ['code', 'message']:
            if key not in response.keys():
                raise FormatError('video_stat', params, response, f'Response should contain key {key}.')
        # response code should be 0
        if response['code'] != 0:
            raise CodeError('video_stat', params, response, response['code'])
        # response data should be a dict
        if type(response['data']) != dict:
            raise FormatError('video_stat', params, response, 'Response data should be a dict.')
        # data should contain keys
        for key in ['aid', 'view', 'danmaku', 'reply', 'favorite', 'coin', 'share', 'now_rank', 'his_rank', 'like',
                    'dislike', 'vt', 'vv']:
            if key not in response['data'].keys():
                raise FormatError('video_stat', params, response, f'Response data should contain key {key}.')

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
