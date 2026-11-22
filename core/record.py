from typing import NamedTuple

__all__ = ['VideoRecord', 'MemberFollowerRecord']


class VideoRecord(NamedTuple):
    added: int
    aid: int
    bvid: str
    view: int | None
    danmaku: int | None
    reply: int | None
    favorite: int | None
    coin: int | None
    share: int | None
    like: int | None


class MemberFollowerRecord(NamedTuple):
    added: int
    mid: int
    follower: int | None
