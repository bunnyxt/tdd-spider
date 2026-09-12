import json
import logging
from typing import Callable, Optional

from ..error import CodeError, FormatError
from ..response import (Newlist, NewlistArchive, NewlistArchiveOwner,
                         NewlistArchiveStat, NewlistPage)

logger = logging.getLogger('Service')
ENDPOINT = 'get_newlist'
RESULT_TYPE = Newlist


def parse(text: str) -> Optional[dict]:
    logger.debug(f'Try to parse newlist response text. text: {text}.')
    try:
        response = json.loads(text)
    except json.JSONDecodeError:
        logger.debug('Fail to decode text to json. Return None.')
        return None
    if response['code'] == -40002:
        logger.debug(f'Status code {response["code"]} found. Return None for retry.')
        return None
    return response


def get(request: Callable[..., dict], params: Optional[dict] = None,
        headers: Optional[dict] = None, retry: Optional[int] = None,
        timeout: Optional[float] = None,
        colddown_factor: Optional[float] = None) -> Newlist:
    response = request(ENDPOINT, params=params, headers=headers, retry=retry,
                       timeout=timeout, colddown_factor=colddown_factor, parser=parse)
    for key in ['code', 'message']:
        if key not in response:
            raise FormatError(ENDPOINT, RESULT_TYPE, params, response,
                              f'Response should contain key {key}.')
    if response['code'] != 0:
        raise CodeError(ENDPOINT, RESULT_TYPE, params, response, response['code'])
    if type(response['data']) != dict:
        raise FormatError(ENDPOINT, RESULT_TYPE, params, response,
                          'Response data should be a dict.')
    data = response['data']
    for key in ['archives', 'page']:
        if key not in data:
            raise FormatError(ENDPOINT, RESULT_TYPE, params, response,
                              f'Response data should contain key {key}.')
    if type(data['archives']) != list:
        raise FormatError(ENDPOINT, RESULT_TYPE, params, response,
                          'Response data archives should be a list.')
    archives = []
    for item in data['archives']:
        if type(item) != dict:
            raise FormatError(ENDPOINT, RESULT_TYPE, params, response,
                              'Response data archives item should be a dict.')
        for key in ['aid', 'videos', 'tid', 'tname', 'copyright', 'pic', 'title', 'stat', 'bvid', 'desc', 'owner']:
            if key not in item:
                raise FormatError(ENDPOINT, RESULT_TYPE, params, response,
                                  f'Response data archives item should contain key {key}.')
        if type(item['stat']) != dict:
            raise FormatError(ENDPOINT, RESULT_TYPE, params, response,
                              'Response data archives item stat should be a dict.')
        for key in ['aid', 'view', 'danmaku', 'reply', 'favorite', 'coin', 'share', 'now_rank', 'his_rank', 'like', 'dislike', 'vt', 'vv']:
            if key not in item['stat']:
                raise FormatError(ENDPOINT, RESULT_TYPE, params, response,
                                  f'Response data archives item stat should contain key {key}.')
        if type(item['owner']) != dict:
            raise FormatError(ENDPOINT, RESULT_TYPE, params, response,
                              'Response data archives item owner should be a dict.')
        for key in ['mid', 'name', 'face']:
            if key not in item['owner']:
                raise FormatError(ENDPOINT, RESULT_TYPE, params, response,
                                  f'Response data archives item owner should contain key {key}.')
        stat = item['stat']
        archives.append(NewlistArchive(
            aid=item['aid'], videos=item['videos'], tid=item['tid'], tname=item['tname'],
            copyright=item['copyright'], pic=item['pic'], title=item['title'],
            stat=NewlistArchiveStat(stat['aid'], stat['view'], stat['danmaku'], stat['reply'],
                                    stat['favorite'], stat['coin'], stat['share'], stat['now_rank'],
                                    stat['his_rank'], stat['like'], stat['dislike'], stat['vt'], stat['vv']),
            bvid=item['bvid'], desc=item['desc'],
            owner=NewlistArchiveOwner(item['owner']['mid'], item['owner']['name'], item['owner']['face'])))
    if type(data['page']) != dict:
        raise FormatError(ENDPOINT, RESULT_TYPE, params, response,
                          'Response data page should be a dict.')
    for key in ['count', 'num', 'size']:
        if key not in data['page']:
            raise FormatError(ENDPOINT, RESULT_TYPE, params, response,
                              f'Response data page should contain key {key}.')
    return Newlist(archives, NewlistPage(data['page']['count'], data['page']['num'], data['page']['size']))
