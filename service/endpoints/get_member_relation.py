from typing import Callable, Optional

from ..error import CodeError, FormatError
from ..response import MemberRelation

ENDPOINT = 'get_member_relation'
RESULT_TYPE = MemberRelation


def get(request: Callable[..., dict], params: Optional[dict] = None,
        headers: Optional[dict] = None, retry: Optional[int] = None,
        timeout: Optional[float] = None,
        colddown_factor: Optional[float] = None) -> MemberRelation:
    response = request(
        ENDPOINT, params=params, headers=headers, retry=retry, timeout=timeout,
        colddown_factor=colddown_factor)
    for key in ['code', 'message', 'ttl']:
        if key not in response:
            raise FormatError(ENDPOINT, RESULT_TYPE, params, response,
                              f'Response should contain key {key}.')
    if response['code'] != 0:
        raise CodeError(ENDPOINT, RESULT_TYPE, params, response, response['code'])
    if not isinstance(response['data'], dict):
        raise FormatError(ENDPOINT, RESULT_TYPE, params, response,
                          'Response data should be a dict.')
    for key in ['mid', 'following', 'follower']:
        if key not in response['data']:
            raise FormatError(ENDPOINT, RESULT_TYPE, params, response,
                              f'Response data should contain key {key}.')
    return MemberRelation(
        mid=response['data']['mid'],
        following=response['data']['following'],
        follower=response['data']['follower'])
