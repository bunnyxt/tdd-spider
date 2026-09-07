from typing import Callable, Optional

from ..error import CodeError, FormatError
from ..response import MemberRelation

TARGET = 'get_member_relation'


def get(request: Callable[..., dict], params: Optional[dict] = None,
        headers: Optional[dict] = None, retry: Optional[int] = None,
        timeout: Optional[float] = None,
        colddown_factor: Optional[float] = None) -> MemberRelation:
    response = request(
        TARGET, params=params, headers=headers, retry=retry, timeout=timeout,
        colddown_factor=colddown_factor)
    for key in ['code', 'message', 'ttl']:
        if key not in response:
            raise FormatError('member_relation', params, response,
                              f'Response should contain key {key}.')
    if response['code'] != 0:
        raise CodeError('member_relation', params, response, response['code'])
    if not isinstance(response['data'], dict):
        raise FormatError('member_relation', params, response,
                          'Response data should be a dict.')
    for key in ['mid', 'following', 'follower']:
        if key not in response['data']:
            raise FormatError('member_relation', params, response,
                              f'Response data should contain key {key}.')
    return MemberRelation(
        mid=response['data']['mid'],
        following=response['data']['following'],
        follower=response['data']['follower'])
