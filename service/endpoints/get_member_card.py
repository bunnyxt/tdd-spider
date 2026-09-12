from typing import Callable, Optional

from ..error import CodeError, FormatError
from ..response import MemberCard

ENDPOINT = 'get_member_card'
RESULT_TYPE = MemberCard


def get(request: Callable[..., dict], params: Optional[dict] = None,
        headers: Optional[dict] = None, retry: Optional[int] = None,
        timeout: Optional[float] = None,
        colddown_factor: Optional[float] = None) -> MemberCard:
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
    if 'card' not in response['data']:
        raise FormatError(ENDPOINT, RESULT_TYPE, params, response,
                          'Response data should contain key card.')
    if type(response['data']['card']) != dict:
        raise FormatError(ENDPOINT, RESULT_TYPE, params, response,
                          'Response data card should be a dict.')
    for key in ['mid', 'name', 'sex', 'face', 'sign']:
        if key not in response['data']['card']:
            raise FormatError(ENDPOINT, RESULT_TYPE, params, response,
                              f'Response data card should contain key {key}.')
    return MemberCard(
        mid=response['data']['card']['mid'],
        name=response['data']['card']['name'],
        sex=response['data']['card']['sex'],
        face=response['data']['card']['face'],
        sign=response['data']['card']['sign'])
