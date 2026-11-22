"""Typed creation inputs; generated ids and derived references belong to DB snapshots."""
from typing import NamedTuple

__all__ = ['NewVideo', 'NewMember']


class NewVideo(NamedTuple):
    added: int
    aid: int
    bvid: str
    mid: int | None
    title: str
    pubdate: int | None
    videos: int | None = None
    tid: int | None = None
    tname: str | None = None
    copyright: int | None = None
    pic: str | None = None
    desc: str | None = None
    tags: str | None = None
    attribute: int | None = None
    state: int | None = 0
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
    sex: str | None = None
    sign: str | None = None
    code: int = 0
