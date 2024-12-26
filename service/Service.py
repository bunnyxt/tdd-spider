import requests
import json
import time
import random
from pathlib import Path
from typing import Optional, Callable, Literal
from .error import ResponseError, FormatError, CodeError
from .response import \
    VideoViewOwner, VideoViewStat, VideoViewStaffItem, VideoView, \
    VideoTag, VideoTags, \
    MemberCard, \
    MemberSpace, \
    MemberRelation, \
    ArchiveRankByPartionPage, ArchiveRankByPartionArchiveStat, ArchiveRankByPartionArchive, ArchiveRankByPartion, \
    NewlistPage, NewlistArchiveStat, NewlistArchiveOwner, NewlistArchive, Newlist
import logging

logger = logging.getLogger('Service')

RequestMode = Literal['direct', 'worker', 'proxy']

__all__ = ['Service', 'RequestMode']


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
            logger.critical(
                f'An unexpected error occurred when load and parse endpoints.json file. {e}')
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

    # since default configs are designed to be immutable, we should use following getters

    def get_default_headers(self) -> Optional[dict]:
        return self._headers

    def get_default_retry(self) -> int:
        return self._retry

    def get_default_timeout(self) -> float:
        return self._timeout

    def get_default_colddown_factor(self) -> float:
        return self._colddown_factor

    def get_default_mode(self) -> RequestMode:
        return self._mode

    def get_default_get_proxy_url(self) -> Optional[Callable]:
        return self._get_proxy_url

    # default config getters end

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
                time.sleep((trial - 1) * (random.random()
                           * 0.5 + 0.75) * colddown_factor)

            # get proxy
            proxies = None
            if get_proxy_url is not None:
                proxy_url = get_proxy_url()
                proxies = {
                    'http': proxy_url,
                }

            # try to get response
            try:
                r = requests.get(url, params=params, headers=headers,
                                 timeout=timeout, proxies=proxies)
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
                url = random.choice(
                    self.endpoints['get_video_view']['workers'])
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
                raise FormatError('video_view', params, response,
                                  f'Response should contain key {key}.')
        # response code should be 0
        if response['code'] != 0:
            raise CodeError('video_view', params, response, response['code'])
        # response data should be a dict
        if type(response['data']) != dict:
            raise FormatError('video_view', params, response,
                              'Response data should be a dict.')
        # data should contain keys
        for key in ['bvid', 'aid', 'videos', 'tid', 'tname', 'copyright', 'pic', 'title', 'pubdate', 'ctime', 'desc',
                    'state', 'duration', 'owner', 'stat']:
            if key not in response['data'].keys():
                raise FormatError('video_view', params, response,
                                  f'Response data should contain key {key}.')
        # response data owner should be a dict
        if type(response['data']['owner']) != dict:
            raise FormatError('video_view', params, response,
                              'Response data owner should be a dict.')
        # data owner should contain keys
        for key in ['mid', 'name', 'face']:
            if key not in response['data']['owner'].keys():
                raise FormatError('video_view', params, response,
                                  f'Response data owner should contain key {key}.')
        # response data stat should be a dict
        if type(response['data']['stat']) != dict:
            raise FormatError('video_view', params, response,
                              'Response data stat should be a dict.')
        # data stat should contain keys
        for key in ['aid', 'view', 'danmaku', 'reply', 'favorite', 'coin', 'share', 'now_rank', 'his_rank', 'like',
                    'dislike']:
            if key not in response['data']['stat'].keys():
                raise FormatError('video_view', params, response,
                                  f'Response data stat should contain key {key}.')
        # response data staff should be a list if exists
        if 'staff' in response['data'].keys():
            if type(response['data']['staff']) != list:
                raise FormatError('video_view', params, response,
                                  'Response data staff should be a list.')
            # staff item should be a dict
            for staff_item in response['data']['staff']:
                if type(staff_item) != dict:
                    raise FormatError(
                        'video_view', params, response, 'Response data staff item should be a dict.')
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
                dislike=response['data']['stat']['dislike'],
                vt=response['data']['stat'].get('vt', None),
                vv=response['data']['stat'].get('vv', None),
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
                url = random.choice(
                    self.endpoints['get_video_tags']['workers'])
        except KeyError:
            logger.critical('Endpoint "get_video_tags" not found.')
            exit(1)

        # define parser
        def parser(text: str) -> Optional[dict]:
            logger.debug(
                f'Try to parse video tags response text. text: {text}.')
            parsed_response = None
            try:
                parsed_response = json.loads(text)
            except json.JSONDecodeError:
                logger.debug(f'Fail to decode text to json. Return None.')
            if parsed_response is not None:
                code = parsed_response['code']
                if code in [-500, -504]:
                    logger.debug(
                        f'Status code {code} found. Server timeout occurred, return None for retry.')
                    parsed_response = None
            return parsed_response

        # get response
        response = self._get(url, params=params, headers=headers,
                             retry=retry, timeout=timeout, colddown_factor=colddown_factor,
                             get_proxy_url=get_proxy_url if mode == 'proxy' else None, parser=parser)
        if response is None:
            raise ResponseError('video_tags', params)

        # validate format

        # response should contain keys
        for key in ['code', 'message', 'ttl']:
            if key not in response.keys():
                raise FormatError('video_tags', params, response,
                                  f'Response should contain key {key}.')
        # response code should be 0
        if response['code'] != 0:
            raise CodeError('video_tags', params, response, response['code'])
        # response data should be a list
        if type(response['data']) != list:
            raise FormatError('video_tags', params, response,
                              'Response data should be a list.')
        # for each data item
        for data_item in response['data']:
            # data item should be a dict
            if type(data_item) != dict:
                raise FormatError('video_tags', params, response,
                                  'Response data item should be a dict.')
            # data item should contain keys
            for key in ['tag_id', 'tag_name']:
                if key not in data_item.keys():
                    raise FormatError('video_tags', params, response,
                                      f'Response data item should contain key {key}.')

        # assemble data
        videoTags = VideoTags(tags=[])
        for data_item in response['data']:
            videoTags.tags.append(VideoTag(
                tag_id=data_item['tag_id'],
                tag_name=data_item['tag_name']
            ))
        return videoTags

    def get_member_card(
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
            url = self.endpoints['get_member_card']['direct']
            if mode == 'worker':
                url = random.choice(
                    self.endpoints['get_member_card']['workers'])
        except KeyError:
            logger.critical('Endpoint "get_member_card" not found.')
            exit(1)

        # get response
        response = self._get(url, params=params, headers=headers,
                             retry=retry, timeout=timeout, colddown_factor=colddown_factor,
                             get_proxy_url=get_proxy_url if mode == 'proxy' else None)
        if response is None:
            raise ResponseError('member_card', params)

        # validate format

        # response should contain keys
        for key in ['code', 'message', 'ttl']:
            if key not in response.keys():
                raise FormatError('member_card', params, response,
                                  f'Response should contain key {key}.')
        # response code should be 0
        if response['code'] != 0:
            raise CodeError('member_card', params, response, response['code'])
        # response data should be a dict
        if type(response['data']) != dict:
            raise FormatError('member_card', params, response,
                              'Response data should be a dict.')
        # data should contain keys
        for key in ['card']:
            if key not in response['data'].keys():
                raise FormatError('member_card', params, response,
                                  f'Response data should contain key {key}.')
        # data card should be a dict
        if type(response['data']['card']) != dict:
            raise FormatError('member_card', params, response,
                              'Response data card should be a dict.')
        # data card should contain keys
        for key in ['mid', 'name', 'sex', 'face', 'sign']:
            if key not in response['data']['card'].keys():
                raise FormatError('member_card', params, response,
                                  f'Response data card should contain key {key}.')

        # assemble data
        return MemberCard(
            mid=response['data']['card']['mid'],
            name=response['data']['card']['name'],
            sex=response['data']['card']['sex'],
            face=response['data']['card']['face'],
            sign=response['data']['card']['sign']
        )

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
                url = random.choice(
                    self.endpoints['get_member_space']['workers'])
        except KeyError:
            logger.critical('Endpoint "get_member_space" not found.')
            exit(1)

        # define parser
        def parser(text: str) -> Optional[dict]:
            logger.debug(
                f'Try to parse member space response text. text: {text}.')
            parsed_response = None
            try:
                parsed_response = json.loads(text)
            except json.JSONDecodeError:
                logger.debug(
                    f'Fail to decode text to json. Try to parse two jsons.')
                split_index = text.find('}{')
                if split_index == -1:
                    logger.debug('Fail to parse two jsons. Return None.')
                else:
                    try:
                        status_response = json.loads(text[:split_index + 1])
                        info_response = json.loads(text[split_index + 1:])
                        logger.debug(
                            f'Successfully parse two jsons. Assign status_response to parsed_response. '
                            f'status_response: {status_response}, info_response: {info_response}.'
                        )
                        parsed_response = info_response
                    except json.JSONDecodeError:
                        logger.debug('Fail to parse two jsons. Return None.')
            if parsed_response is not None:
                code = parsed_response['code']
                if code in [-401, -799]:
                    logger.debug(
                        f'Status code {code} found. Anti-crawler triggered, return None for retry.')
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
                raise FormatError('member_space', params, response,
                                  f'Response should contain key {key}.')
        # response code should be 0
        if response['code'] != 0:
            raise CodeError('member_space', params, response, response['code'])
        # response data should be a dict
        if type(response['data']) != dict:
            raise FormatError('member_space', params, response,
                              'Response data should be a dict.')
        # data should contain keys
        for key in ['mid', 'name', 'sex', 'face', 'sign']:
            if key not in response['data'].keys():
                raise FormatError('member_space', params, response,
                                  f'Response data should contain key {key}.')

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
        params: { vmid: int }
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
                url = random.choice(
                    self.endpoints['get_member_relation']['workers'])
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
                raise FormatError('member_relation', params,
                                  response, f'Response should contain key {key}.')
        # response code should be 0
        if response['code'] != 0:
            raise CodeError('member_relation', params,
                            response, response['code'])
        # response data should be a dict
        if type(response['data']) != dict:
            raise FormatError('member_relation', params,
                              response, 'Response data should be a dict.')
        # data should contain keys
        for key in ['mid', 'following', 'follower']:
            if key not in response['data'].keys():
                raise FormatError('member_relation', params, response,
                                  f'Response data should contain key {key}.')

        # assemble data
        return MemberRelation(
            mid=response['data']['mid'],
            following=response['data']['following'],
            follower=response['data']['follower']
        )

    def get_archive_rank_by_partion(
            self, params: dict = None, headers: dict = None,
            retry: int = None, timeout: float = None, colddown_factor: float = None,
            mode: RequestMode = None, get_proxy_url: Callable = None
    ) -> ArchiveRankByPartion:
        """
        params: { tid: int, pn: int, ps: int }
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
            url = self.endpoints['get_archive_rank_by_partion']['direct']
            if mode == 'worker':
                url = random.choice(
                    self.endpoints['get_archive_rank_by_partion']['workers'])
        except KeyError:
            logger.critical(
                'Endpoint "get_archive_rank_by_partion" not found.')
            exit(1)

        # define parser
        def parser(text: str) -> Optional[dict]:
            logger.debug(
                f'Try to parse archive rank by partion response text. text: {text}.')
            parsed_response = None
            try:
                parsed_response = json.loads(text)
            except json.JSONDecodeError:
                logger.debug(f'Fail to decode text to json. Return None.')
            if parsed_response is not None:
                code = parsed_response['code']
                if code in [-40002]:
                    logger.debug(
                        f'Status code {code} found. Server timeout occurred, return None for retry.')
                    parsed_response = None
            return parsed_response

        # get response
        response = self._get(url, params=params, headers=headers,
                             retry=retry, timeout=timeout, colddown_factor=colddown_factor,
                             get_proxy_url=get_proxy_url if mode == 'proxy' else None, parser=parser)
        if response is None:
            raise ResponseError('archive_rank_by_partion', params)

        # validate format

        # response should contain keys
        for key in ['code', 'message']:
            if key not in response.keys():
                raise FormatError('archive_rank_by_partion', params,
                                  response, f'Response should contain key {key}.')
        # response code should be 0
        if response['code'] != 0:
            raise CodeError('archive_rank_by_partion', params,
                            response, response['code'])
        # response data should be a dict
        if type(response['data']) != dict:
            raise FormatError('archive_rank_by_partion', params,
                              response, 'Response data should be a dict.')
        # data should contain keys
        for key in ['archives', 'page']:
            if key not in response['data'].keys():
                raise FormatError('archive_rank_by_partion', params, response,
                                  f'Response data should contain key {key}.')
        # data archives should be a list
        if type(response['data']['archives']) != list:
            raise FormatError('archive_rank_by_partion', params,
                              response, 'Response data archives should be a list.')
        # for each data archives item
        for data_archives_item in response['data']['archives']:
            # data archives item should be a dict
            if type(data_archives_item) != dict:
                raise FormatError('archive_rank_by_partion', params, response,
                                  'Response data archives item should be a dict.')
            # data archives item should contain keys
            for key in ['aid', 'videos', 'tid', 'tname', 'copyright', 'pic', 'title', 'stat', 'bvid', 'description',
                        'mid']:
                if key not in data_archives_item.keys():
                    raise FormatError('archive_rank_by_partion', params, response,
                                      f'Response data archives item should contain key {key}.')
                # data archives item stat should be a dict
                if type(data_archives_item['stat']) != dict:
                    raise FormatError('archive_rank_by_partion', params, response,
                                      'Response data archives item stat should be a dict.')
                # data archives item stat should contain keys
                for key2 in ['aid', 'view', 'danmaku', 'reply', 'favorite', 'coin', 'share', 'now_rank', 'his_rank',
                             'like', 'dislike', 'vt', 'vv']:
                    if key2 not in data_archives_item['stat'].keys():
                        raise FormatError('archive_rank_by_partion', params, response,
                                          f'Response data archives item stat should contain key {key2}.')
        # data page should be a dict
        if type(response['data']['page']) != dict:
            raise FormatError('archive_rank_by_partion', params,
                              response, 'Response data page should be a dict.')
        # data page should contain keys
        for key in ['count', 'num', 'size']:
            if key not in response['data']['page'].keys():
                raise FormatError('archive_rank_by_partion', params, response,
                                  f'Response data page should contain key {key}.')

        # assemble data
        archiveRankByPartionPage = ArchiveRankByPartionPage(
            count=response['data']['page']['count'],
            num=response['data']['page']['num'],
            size=response['data']['page']['size']
        )
        archiveRankByPartionArchives = []
        for data_archives_item in response['data']['archives']:
            archiveRankByPartionArchives.append(ArchiveRankByPartionArchive(
                aid=data_archives_item['aid'],
                videos=data_archives_item['videos'],
                tid=data_archives_item['tid'],
                tname=data_archives_item['tname'],
                copyright=data_archives_item['copyright'],
                pic=data_archives_item['pic'],
                title=data_archives_item['title'],
                stat=ArchiveRankByPartionArchiveStat(
                    aid=data_archives_item['stat']['aid'],
                    view=data_archives_item['stat']['view'],
                    danmaku=data_archives_item['stat']['danmaku'],
                    reply=data_archives_item['stat']['reply'],
                    favorite=data_archives_item['stat']['favorite'],
                    coin=data_archives_item['stat']['coin'],
                    share=data_archives_item['stat']['share'],
                    now_rank=data_archives_item['stat']['now_rank'],
                    his_rank=data_archives_item['stat']['his_rank'],
                    like=data_archives_item['stat']['like'],
                    dislike=data_archives_item['stat']['dislike'],
                    vt=data_archives_item['stat']['vt'],
                    vv=data_archives_item['stat']['vv']
                ),
                bvid=data_archives_item['bvid'],
                description=data_archives_item['description'],
                mid=data_archives_item['mid'],
            ))
        return ArchiveRankByPartion(
            archives=archiveRankByPartionArchives,
            page=archiveRankByPartionPage
        )

    def get_newlist(
            self, params: dict = None, headers: dict = None,
            retry: int = None, timeout: float = None, colddown_factor: float = None,
            mode: RequestMode = None, get_proxy_url: Callable = None
    ) -> Newlist:
        """
        params: { rid: int, pn: int, ps: int }
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
            url = self.endpoints['get_newlist']['direct']
            if mode == 'worker':
                url = random.choice(self.endpoints['get_newlist']['workers'])
        except KeyError:
            logger.critical('Endpoint "get_newlist" not found.')
            exit(1)

        # define parser
        def parser(text: str) -> Optional[dict]:
            logger.debug(f'Try to parse newlist response text. text: {text}.')
            parsed_response = None
            try:
                parsed_response = json.loads(text)
            except json.JSONDecodeError:
                logger.debug(f'Fail to decode text to json. Return None.')
            if parsed_response is not None:
                code = parsed_response['code']
                if code in [-40002]:
                    logger.debug(
                        f'Status code {code} found. Server timeout occurred, return None for retry.')
                    parsed_response = None
            return parsed_response

        # get response
        response = self._get(url, params=params, headers=headers,
                             retry=retry, timeout=timeout, colddown_factor=colddown_factor,
                             get_proxy_url=get_proxy_url if mode == 'proxy' else None, parser=parser)
        if response is None:
            raise ResponseError('newlist', params)

        # validate format

        # response should contain keys
        for key in ['code', 'message']:
            if key not in response.keys():
                raise FormatError('newlist', params, response,
                                  f'Response should contain key {key}.')
        # response code should be 0
        if response['code'] != 0:
            raise CodeError('newlist', params, response, response['code'])
        # response data should be a dict
        if type(response['data']) != dict:
            raise FormatError('newlist', params, response,
                              'Response data should be a dict.')
        # data should contain keys
        for key in ['archives', 'page']:
            if key not in response['data'].keys():
                raise FormatError('newlist', params, response,
                                  f'Response data should contain key {key}.')
        # data archives should be a list
        if type(response['data']['archives']) != list:
            raise FormatError('newlist', params, response,
                              'Response data archives should be a list.')
        # for each data archives item
        for data_archives_item in response['data']['archives']:
            # data archives item should be a dict
            if type(data_archives_item) != dict:
                raise FormatError('newlist', params, response,
                                  'Response data archives item should be a dict.')
            # data archives item should contain keys
            for key in ['aid', 'videos', 'tid', 'tname', 'copyright', 'pic', 'title', 'stat', 'bvid', 'desc', 'owner']:
                if key not in data_archives_item.keys():
                    raise FormatError('newlist', params, response,
                                      f'Response data archives item should contain key {key}.')
                # data archives item stat should be a dict
                if type(data_archives_item['stat']) != dict:
                    raise FormatError('newlist', params, response,
                                      'Response data archives item stat should be a dict.')
                # data archives item stat should contain keys
                for key2 in ['aid', 'view', 'danmaku', 'reply', 'favorite', 'coin', 'share', 'now_rank', 'his_rank',
                             'like', 'dislike', 'vt', 'vv']:
                    if key2 not in data_archives_item['stat'].keys():
                        raise FormatError('newlist', params, response,
                                          f'Response data archives item stat should contain key {key2}.')
                # data archives item owner should be a dict
                if type(data_archives_item['owner']) != dict:
                    raise FormatError('newlist', params, response,
                                      'Response data archives item owner should be a dict.')
                # data archives item stat should contain keys
                for key2 in ['mid', 'name', 'face']:
                    if key2 not in data_archives_item['owner'].keys():
                        raise FormatError('newlist', params, response,
                                          f'Response data archives item owner should contain key {key2}.')
        # data page should be a dict
        if type(response['data']['page']) != dict:
            raise FormatError('newlist', params, response,
                              'Response data page should be a dict.')
        # data page should contain keys
        for key in ['count', 'num', 'size']:
            if key not in response['data']['page'].keys():
                raise FormatError('newlist', params, response,
                                  f'Response data page should contain key {key}.')

        # assemble data
        newlistPage = NewlistPage(
            count=response['data']['page']['count'],
            num=response['data']['page']['num'],
            size=response['data']['page']['size']
        )
        newlistArchives = []
        for data_archives_item in response['data']['archives']:
            newlistArchives.append(NewlistArchive(
                aid=data_archives_item['aid'],
                videos=data_archives_item['videos'],
                tid=data_archives_item['tid'],
                tname=data_archives_item['tname'],
                copyright=data_archives_item['copyright'],
                pic=data_archives_item['pic'],
                title=data_archives_item['title'],
                stat=NewlistArchiveStat(
                    aid=data_archives_item['stat']['aid'],
                    view=data_archives_item['stat']['view'],
                    danmaku=data_archives_item['stat']['danmaku'],
                    reply=data_archives_item['stat']['reply'],
                    favorite=data_archives_item['stat']['favorite'],
                    coin=data_archives_item['stat']['coin'],
                    share=data_archives_item['stat']['share'],
                    now_rank=data_archives_item['stat']['now_rank'],
                    his_rank=data_archives_item['stat']['his_rank'],
                    like=data_archives_item['stat']['like'],
                    dislike=data_archives_item['stat']['dislike'],
                    vt=data_archives_item['stat']['vt'],
                    vv=data_archives_item['stat']['vv']
                ),
                bvid=data_archives_item['bvid'],
                desc=data_archives_item['desc'],
                owner=NewlistArchiveOwner(
                    mid=data_archives_item['owner']['mid'],
                    name=data_archives_item['owner']['name'],
                    face=data_archives_item['owner']['face']
                ),
            ))
        return Newlist(
            archives=newlistArchives,
            page=newlistPage
        )
