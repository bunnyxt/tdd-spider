from typing import NamedTuple

__all__ = ['RecordNew', 'MemberFollowerRecordNew']


class RecordNew(NamedTuple):
    """Session-independent observation; CSV carries exactly these ten fields."""
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
