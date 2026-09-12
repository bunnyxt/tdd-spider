from typing import Callable, Optional

from ..error import CodeError, FormatError
from ..response import VideoView, VideoViewOwner, VideoViewStaffItem, VideoViewStat

ENDPOINT = 'get_video_view'
RESULT_TYPE = VideoView


def get(request: Callable[..., dict], params: Optional[dict] = None,
        headers: Optional[dict] = None, retry: Optional[int] = None,
        timeout: Optional[float] = None,
        colddown_factor: Optional[float] = None) -> VideoView:
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
    for key in ['bvid', 'aid', 'videos', 'tid', 'tname', 'copyright', 'pic', 'title',
                'pubdate', 'ctime', 'desc', 'state', 'duration', 'owner', 'stat']:
        if key not in data:
            raise FormatError(ENDPOINT, RESULT_TYPE, params, response,
                              f'Response data should contain key {key}.')
    if type(data['owner']) != dict:
        raise FormatError(ENDPOINT, RESULT_TYPE, params, response,
                          'Response data owner should be a dict.')
    for key in ['mid', 'name', 'face']:
        if key not in data['owner']:
            raise FormatError(ENDPOINT, RESULT_TYPE, params, response,
                              f'Response data owner should contain key {key}.')
    if type(data['stat']) != dict:
        raise FormatError(ENDPOINT, RESULT_TYPE, params, response,
                          'Response data stat should be a dict.')
    for key in ['aid', 'view', 'danmaku', 'reply', 'favorite', 'coin', 'share',
                'now_rank', 'his_rank', 'like', 'dislike']:
        if key not in data['stat']:
            raise FormatError(ENDPOINT, RESULT_TYPE, params, response,
                              f'Response data stat should contain key {key}.')

    staff = None
    if 'staff' in data:
        if type(data['staff']) != list:
            raise FormatError(ENDPOINT, RESULT_TYPE, params, response,
                              'Response data staff should be a list.')
        staff = []
        for staff_item in data['staff']:
            if type(staff_item) != dict:
                raise FormatError(ENDPOINT, RESULT_TYPE, params, response,
                                  'Response data staff item should be a dict.')
            for key in ['mid', 'title', 'name', 'face']:
                if key not in staff_item:
                    raise FormatError(ENDPOINT, RESULT_TYPE, params, response,
                                      f'Response data staff item should contain key {key}.')
            staff.append(VideoViewStaffItem(
                mid=staff_item['mid'], title=staff_item['title'],
                name=staff_item['name'], face=staff_item['face']))

    return VideoView(
        bvid=data['bvid'], aid=data['aid'], videos=data['videos'], tid=data['tid'],
        tname=data['tname'], copyright=data['copyright'], pic=data['pic'],
        title=data['title'], pubdate=data['pubdate'], ctime=data['ctime'],
        desc=data['desc'], state=data['state'], duration=data['duration'],
        owner=VideoViewOwner(
            mid=data['owner']['mid'], name=data['owner']['name'], face=data['owner']['face']),
        stat=VideoViewStat(
            aid=data['stat']['aid'], view=data['stat']['view'],
            danmaku=data['stat']['danmaku'], reply=data['stat']['reply'],
            favorite=data['stat']['favorite'], coin=data['stat']['coin'],
            share=data['stat']['share'], now_rank=data['stat']['now_rank'],
            his_rank=data['stat']['his_rank'], like=data['stat']['like'],
            dislike=data['stat']['dislike'], vt=data['stat'].get('vt'),
            vv=data['stat'].get('vv')),
        attribute=data.get('attribute'), forward=data.get('forward'), staff=staff)
