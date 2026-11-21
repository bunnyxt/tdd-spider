from typing import NamedTuple

__all__ = ['VideoRecord', 'MemberFollowerRecord']


class VideoRecord(NamedTuple):
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


class MemberFollowerRecord(NamedTuple):
    added: int
    mid: int
    follower: int
