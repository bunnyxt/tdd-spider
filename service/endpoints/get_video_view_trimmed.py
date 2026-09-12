from typing import Callable, Optional

from ..error import CodeError, FormatError
from ..response import VideoViewStat, VideoViewTrimmed

ENDPOINT = 'get_video_view_trimmed'
RESULT_TYPE = VideoViewTrimmed


def get(request: Callable[..., dict], params: Optional[dict] = None,
        headers: Optional[dict] = None, retry: Optional[int] = None,
        timeout: Optional[float] = None,
        colddown_factor: Optional[float] = None) -> VideoViewTrimmed:
    response = request(
        ENDPOINT, params=params, headers=headers, retry=retry, timeout=timeout,
        colddown_factor=colddown_factor)
    for key in ['code', 'message', 'ttl']:
        if key not in response:
            raise FormatError(ENDPOINT, RESULT_TYPE, params, response,
                              f'Response should contain key {key}.')
    if response['code'] != 0:
        raise CodeError(ENDPOINT, RESULT_TYPE, params, response, response['code'])
    if type(response['data']) != dict:
        raise FormatError(ENDPOINT, RESULT_TYPE, params, response,
                          'Response data should be a dict.')
    data = response['data']
    for key in ['bvid', 'aid', 'stat']:
        if key not in data:
            raise FormatError(ENDPOINT, RESULT_TYPE, params, response,
                              f'Response data should contain key {key}.')
    if type(data['stat']) != dict:
        raise FormatError(ENDPOINT, RESULT_TYPE, params, response,
                          'Response data stat should be a dict.')
    for key in ['aid', 'view', 'danmaku', 'reply', 'favorite', 'coin', 'share',
                'now_rank', 'his_rank', 'like', 'dislike']:
        if key not in data['stat']:
            raise FormatError(ENDPOINT, RESULT_TYPE, params, response,
                              f'Response data stat should contain key {key}.')
    return VideoViewTrimmed(
        bvid=data['bvid'], aid=data['aid'],
        stat=VideoViewStat(
            aid=data['stat']['aid'], view=data['stat']['view'],
            danmaku=data['stat']['danmaku'], reply=data['stat']['reply'],
            favorite=data['stat']['favorite'], coin=data['stat']['coin'],
            share=data['stat']['share'], now_rank=data['stat']['now_rank'],
            his_rank=data['stat']['his_rank'], like=data['stat']['like'],
            dislike=data['stat']['dislike'], vt=data['stat'].get('vt'),
            vv=data['stat'].get('vv')))
