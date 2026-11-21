from typing import NamedTuple

__all__ = ['Record', 'MemberFollowerRecordNew']


class Record(NamedTuple):
    added: int
    aid: int
    bvid: str
    view: int
    danmaku: int
    reply: int
    favorite: int
    coin: int
    share: int
    like: int


class MemberFollowerRecordNew(NamedTuple):
    added: int
    mid: int
    follower: int
