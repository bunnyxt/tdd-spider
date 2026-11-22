"""Creation contracts follow API response types, not legacy column nullability.

These annotations do not validate raw responses. Generated ids and derived
references belong to database snapshots.
"""
from typing import NamedTuple

__all__ = ['NewVideo', 'NewMember']


class NewVideo(NamedTuple):
    added: int
    aid: int
    bvid: str
    mid: int
    title: str
    pubdate: int
    videos: int
    tid: int
    tname: str
    copyright: int
    pic: str
    desc: str
    tags: str
    state: int
    attribute: int | None = None
    forward: int | None = None
    code: int = 0
    hasstaff: int = 0
    isvc: int = -1
    recent: int = 2
    # solo/original/employed/engine/freq/activity use their database defaults.


class NewMember(NamedTuple):
    added: int
    mid: int
    name: str
    face: str
    sex: str
    sign: str
    code: int = 0
