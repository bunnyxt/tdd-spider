"""Write inputs for the database layer.

Stored TddXXX snapshots are built only by db from rows it reads back.
"""
from typing import NamedTuple

__all__ = ['NewVideo', 'NewMember', 'NewVideoLog', 'NewMemberLog']


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


class NewVideoLog(NamedTuple):
    added: int
    aid: int
    attr: str
    oldval: str | None = None
    newval: str | None = None


class NewMemberLog(NamedTuple):
    added: int
    mid: int
    attr: str
    oldval: str | None = None
    newval: str | None = None
