import requests
import json
import time
import random
from collections import namedtuple
from pathlib import Path
from typing import Optional, Callable
from .error import ResponseError, FormatError, CodeError
import logging

logger = logging.getLogger('Service')

# TODO: use typing.Literal in python 3.8+
# RequestMode = Literal['direct', 'worker', 'proxy']
RequestMode = str
VideoStat = namedtuple('VideoStat',
                       ['aid', 'view', 'danmaku', 'reply', 'favorite', 'coin', 'share', 'now_rank', 'his_rank', 'like',
                        'dislike', 'vt', 'vv'])
VideoView = namedtuple('VideoView',
                       ['bvid', 'aid', 'videos', 'tid', 'tname', 'copyright', 'pic', 'title', 'pubdate', 'ctime',
                        'desc', 'state', 'duration', 'owner', 'stat',
                        # optional
                        'attribute', 'forward', 'staff'])
VideoViewOwner = namedtuple('VideoViewOwner', ['mid', 'name', 'face'])
VideoViewStat = namedtuple('VideoViewStat',
                           ['aid', 'view', 'danmaku', 'reply', 'favorite', 'coin', 'share', 'now_rank', 'his_rank',
                            'like', 'dislike'])
VideoViewStaffItem = namedtuple('VideoViewStaffItem', ['mid', 'title', 'name', 'face'])
VideoTags = namedtuple('VideoTags', ['tags'])
VideoTag = namedtuple('VideoTag', ['tag_id', 'tag_name'])
MemberSpace = namedtuple('MemberSpace', ['mid', 'name', 'sex', 'face', 'sign'])
MemberRelation = namedtuple('MemberRelation', ['mid', 'following', 'follower'])

__all__ = ['Service',
           'RequestMode',
           'VideoStat',
           'VideoView', 'VideoViewOwner', 'VideoViewStat', 'VideoViewStaffItem',
           'VideoTags', 'VideoTag',
           'MemberSpace',
           'MemberRelation']


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
            get_proxy_url: Callable = None, parser: Callable[[str], Optional[dict]] = None
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
            if parser is None:
                try:
                    response = r.json()
                    break
                except json.JSONDecodeError:
                    logger.debug(
                        f'Fail to decode response to json. '
                        f'response: {r.text}, url: {url}, params: {params}, trial: {trial}'
                    )
                    continue
            else:
                response = parser(r.text)
                if response is not None:
                    break
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

    def get_video_view(
            self, params: dict = None, headers: dict = None,
            retry: int = None, timeout: float = None, colddown_factor: float = None,
            mode: RequestMode = None, get_proxy_url: Callable = None
    ) -> VideoView:
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
            url = self.endpoints['get_video_view']['direct']
            if mode == 'worker':
                url = random.choice(self.endpoints['get_video_view']['workers'])
        except KeyError:
            logger.critical('Endpoint "get_video_view" not found.')
            exit(1)

        # get response
        response = self._get(url, params=params, headers=headers,
                             retry=retry, timeout=timeout, colddown_factor=colddown_factor,
                             get_proxy_url=get_proxy_url if mode == 'proxy' else None)
        if response is None:
            raise ResponseError('video_view', params)

        # validate format

        # response should contain keys
        for key in ['code', 'message', 'ttl']:
            if key not in response.keys():
                raise FormatError('video_view', params, response, f'Response should contain key {key}.')
        # response code should be 0
        if response['code'] != 0:
            raise CodeError('video_view', params, response, response['code'])
        # response data should be a dict
        if type(response['data']) != dict:
            raise FormatError('video_view', params, response, 'Response data should be a dict.')
        # data should contain keys
        for key in ['bvid', 'aid', 'videos', 'tid', 'tname', 'copyright', 'pic', 'title', 'pubdate', 'ctime', 'desc',
                    'state', 'duration', 'owner', 'stat']:
            if key not in response['data'].keys():
                raise FormatError('video_view', params, response, f'Response data should contain key {key}.')
        # response data owner should be a dict
        if type(response['data']['owner']) != dict:
            raise FormatError('video_view', params, response, 'Response data owner should be a dict.')
        # data owner should contain keys
        for key in ['mid', 'name', 'face']:
            if key not in response['data']['owner'].keys():
                raise FormatError('video_view', params, response, f'Response data owner should contain key {key}.')
        # response data stat should be a dict
        if type(response['data']['stat']) != dict:
            raise FormatError('video_view', params, response, 'Response data stat should be a dict.')
        # data stat should contain keys
        for key in ['aid', 'view', 'danmaku', 'reply', 'favorite', 'coin', 'share', 'now_rank', 'his_rank', 'like',
                    'dislike']:
            if key not in response['data']['stat'].keys():
                raise FormatError('video_view', params, response, f'Response data owner should contain key {key}.')
        # response data staff should be a list if exists
        if 'staff' in response['data'].keys():
            if type(response['data']['staff']) != list:
                raise FormatError('video_view', params, response, 'Response data staff should be a list.')
            # staff item should be a dict
            for staff_item in response['data']['staff']:
                if type(staff_item) != dict:
                    raise FormatError('video_view', params, response, 'Response data staff item should be a dict.')
                # staff item should contain keys
                for key in ['mid', 'title', 'name', 'face']:
                    if key not in staff_item.keys():
                        raise FormatError('video_view', params, response,
                                          f'Response data staff item should contain key {key}.')

        # assemble data
        staff = None
        if 'staff' in response['data'].keys():
            staff = []
            for staff_item in response['data']['staff']:
                staff.append(VideoViewStaffItem(
                    mid=staff_item['mid'],
                    title=staff_item['title'],
                    name=staff_item['name'],
                    face=staff_item['face']
                ))
        return VideoView(
            bvid=response['data']['bvid'],
            aid=response['data']['aid'],
            videos=response['data']['videos'],
            tid=response['data']['tid'],
            tname=response['data']['tname'],
            copyright=response['data']['copyright'],
            pic=response['data']['pic'],
            title=response['data']['title'],
            pubdate=response['data']['pubdate'],
            ctime=response['data']['ctime'],
            desc=response['data']['desc'],
            state=response['data']['state'],
            duration=response['data']['duration'],
            owner=VideoViewOwner(
                mid=response['data']['owner']['mid'],
                name=response['data']['owner']['name'],
                face=response['data']['owner']['face']
            ),
            stat=VideoViewStat(
                aid=response['data']['stat']['aid'],
                view=response['data']['stat']['view'],
                danmaku=response['data']['stat']['danmaku'],
                reply=response['data']['stat']['reply'],
                favorite=response['data']['stat']['favorite'],
                coin=response['data']['stat']['coin'],
                share=response['data']['stat']['share'],
                now_rank=response['data']['stat']['now_rank'],
                his_rank=response['data']['stat']['his_rank'],
                like=response['data']['stat']['like'],
                dislike=response['data']['stat']['dislike']
            ),
            attribute=response['data'].get('attribute', None),
            forward=response['data'].get('forward', None),
            staff=staff
        )

    def get_video_tags(
            self, params: dict = None, headers: dict = None,
            retry: int = None, timeout: float = None, colddown_factor: float = None,
            mode: RequestMode = None, get_proxy_url: Callable = None
    ) -> VideoTags:
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
            url = self.endpoints['get_video_tags']['direct']
            if mode == 'worker':
                url = random.choice(self.endpoints['get_video_tags']['workers'])
        except KeyError:
            logger.critical('Endpoint "get_video_tags" not found.')
            exit(1)

        # get response
        response = self._get(url, params=params, headers=headers,
                             retry=retry, timeout=timeout, colddown_factor=colddown_factor,
                             get_proxy_url=get_proxy_url if mode == 'proxy' else None)
        if response is None:
            raise ResponseError('video_tags', params)

        # validate format

        # response should contain keys
        for key in ['code', 'message', 'ttl']:
            if key not in response.keys():
                raise FormatError('video_tags', params, response, f'Response should contain key {key}.')
        # response code should be 0
        if response['code'] != 0:
            raise CodeError('video_tags', params, response, response['code'])
        # response data should be a dict
        if type(response['data']) != list:
            raise FormatError('video_tags', params, response, 'Response data should be a list.')
        # for each data item
        for data_item in response['data']:
            # data item should be a dict
            if type(data_item) != dict:
                raise FormatError('video_tags', params, response, 'Response data item should be a dict.')
            # data item should contain keys
            for key in ['tag_id', 'tag_name']:
                if key not in data_item.keys():
                    raise FormatError('video_tags', params, response, f'Response data item should contain key {key}.')

        # assemble data
        videoTags = VideoTags(tags=[])
        for data_item in response['data']:
            videoTags.tags.append(VideoTag(
                tag_id=data_item['tag_id'],
                tag_name=data_item['tag_name']
            ))
        return videoTags

    def get_member_space(
            self, params: dict = None, headers: dict = None,
            retry: int = None, timeout: float = None, colddown_factor: float = None,
            mode: RequestMode = None, get_proxy_url: Callable = None
    ) -> MemberSpace:
        """
        params: { mid: int }
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
            url = self.endpoints['get_member_space']['direct']
            if mode == 'worker':
                url = random.choice(self.endpoints['get_member_space']['workers'])
        except KeyError:
            logger.critical('Endpoint "get_member_space" not found.')
            exit(1)

        # define parser
        def parser(text: str) -> Optional[dict]:
            logger.debug(f'Try to parse member space response text. text: {text}.')
            parsed_response = None
            try:
                parsed_response = json.loads(text)
            except json.JSONDecodeError:
                logger.debug(f'Fail to decode text to json. Try to parse two jsons.')
                split_index = text.find('}{')
                if split_index == -1:
                    logger.debug('Fail to parse two jsons. Return None.')
                else:
                    try:
                        status_response = json.loads(text[:split_index + 1])
                        info_response = json.loads(text[split_index + 1:])
                        logger.debug(
                            f'Successfully parse two jsons. Assign status_response to parsed_response.'
                            f'status_response: {status_response}, info_response: {info_response}.'
                        )
                        parsed_response = info_response
                    except json.JSONDecodeError:
                        logger.debug('Fail to parse two jsons. Return None.')
            if parsed_response is not None and parsed_response['code'] == -401:
                logger.debug('Status code -401 found. Anti-crawler triggered, return None for retry.')
                parsed_response = None
            return parsed_response

        # get response
        response = self._get(url, params=params, headers=headers,
                             retry=retry, timeout=timeout, colddown_factor=colddown_factor,
                             get_proxy_url=get_proxy_url if mode == 'proxy' else None, parser=parser)
        if response is None:
            raise ResponseError('member_space', params)

        # validate format

        # response should contain keys
        for key in ['code', 'message', 'ttl']:
            if key not in response.keys():
                raise FormatError('member_space', params, response, f'Response should contain key {key}.')
        # response code should be 0
        if response['code'] != 0:
            raise CodeError('member_space', params, response, response['code'])
        # response data should be a dict
        if type(response['data']) != dict:
            raise FormatError('member_space', params, response, 'Response data should be a dict.')
        # data should contain keys
        for key in ['mid', 'name', 'sex', 'face', 'sign']:
            if key not in response['data'].keys():
                raise FormatError('member_space', params, response, f'Response data should contain key {key}.')

        # assemble data
        return MemberSpace(
            mid=response['data']['mid'],
            name=response['data']['name'],
            sex=response['data']['sex'],
            face=response['data']['face'],
            sign=response['data']['sign']
        )

    def get_member_relation(
            self, params: dict = None, headers: dict = None,
            retry: int = None, timeout: float = None, colddown_factor: float = None,
            mode: RequestMode = None, get_proxy_url: Callable = None
    ) -> MemberRelation:
        """
        params: { mid: int }
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
            url = self.endpoints['get_member_relation']['direct']
            if mode == 'worker':
                url = random.choice(self.endpoints['get_member_relation']['workers'])
        except KeyError:
            logger.critical('Endpoint "get_member_relation" not found.')
            exit(1)

        # get response
        response = self._get(url, params=params, headers=headers,
                             retry=retry, timeout=timeout, colddown_factor=colddown_factor,
                             get_proxy_url=get_proxy_url if mode == 'proxy' else None)
        if response is None:
            raise ResponseError('member_relation', params)

        # validate format

        # response should contain keys
        for key in ['code', 'message', 'ttl']:
            if key not in response.keys():
                raise FormatError('member_relation', params, response, f'Response should contain key {key}.')
        # response code should be 0
        if response['code'] != 0:
            raise CodeError('member_relation', params, response, response['code'])
        # response data should be a dict
        if type(response['data']) != dict:
            raise FormatError('member_relation', params, response, 'Response data should be a dict.')
        # data should contain keys
        for key in ['mid', 'following', 'follower']:
            if key not in response['data'].keys():
                raise FormatError('member_relation', params, response, f'Response data should contain key {key}.')

        # assemble data
        return MemberRelation(
            mid=response['data']['mid'],
            following=response['data']['following'],
            follower=response['data']['follower']
        )
