import json
import logging
from typing import Callable, Optional

from ..error import CodeError, FormatError
from ..response import VideoTag, VideoTags

logger = logging.getLogger('Service')

ENDPOINT = 'get_video_tags'
RESULT_TYPE = VideoTags


def parse(text: str) -> Optional[dict]:
    logger.debug(f'Try to parse video tags response text. text: {text}.')
    try:
        response = json.loads(text)
    except json.JSONDecodeError:
        logger.debug('Fail to decode text to json. Return None.')
        return None
    if response['code'] in [-500, -504]:
        logger.debug(f'Status code {response["code"]} found. Return None for retry.')
        return None
    return response


def get(request: Callable[..., dict], params: Optional[dict] = None,
        headers: Optional[dict] = None, retry: Optional[int] = None,
        timeout: Optional[float] = None,
        colddown_factor: Optional[float] = None) -> VideoTags:
    response = request(
        ENDPOINT, params=params, headers=headers, retry=retry, timeout=timeout,
        colddown_factor=colddown_factor, parser=parse)
    for key in ['code', 'message', 'ttl']:
        if key not in response:
            raise FormatError(ENDPOINT, RESULT_TYPE, params, response,
                              f'Response should contain key {key}.')
    if response['code'] != 0:
        raise CodeError(ENDPOINT, RESULT_TYPE, params, response, response['code'])
    if type(response['data']) != list:
        raise FormatError(ENDPOINT, RESULT_TYPE, params, response,
                          'Response data should be a list.')
    for data_item in response['data']:
        if type(data_item) != dict:
            raise FormatError(ENDPOINT, RESULT_TYPE, params, response,
                              'Response data item should be a dict.')
        for key in ['tag_id', 'tag_name']:
            if key not in data_item:
                raise FormatError(ENDPOINT, RESULT_TYPE, params, response,
                                  f'Response data item should contain key {key}.')
    return VideoTags(tags=[
        VideoTag(tag_id=data_item['tag_id'], tag_name=data_item['tag_name'])
        for data_item in response['data']])
