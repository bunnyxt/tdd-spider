from typing import Optional, NamedTuple

__all__ = [
    'VideoViewOwner', 'VideoViewStat', 'VideoViewStaffItem', 'VideoView',
    'VideoTag', 'VideoTags',
    'MemberCard',
    'MemberRelation',
    'NewlistPage', 'NewlistArchiveStat', 'NewlistArchiveOwner', 'NewlistArchive', 'Newlist',
]


class VideoViewOwner(NamedTuple):
    mid: int
    name: str
    face: str


class VideoViewStat(NamedTuple):
    aid: int
    view: int
    danmaku: int
    reply: int
    favorite: int
    coin: int
    share: int
    now_rank: int
    his_rank: int
    like: int
    dislike: int
    vt: Optional[int]
    vv: Optional[int]


class VideoViewStaffItem(NamedTuple):
    mid: int
    title: str
    name: str
    face: str


class VideoView(NamedTuple):
    bvid: str
    aid: int
    videos: int
    tid: int
    tname: str
    copyright: int
    pic: str
    title: str
    pubdate: int
    ctime: int
    desc: str
    state: int
    duration: int
    owner: VideoViewOwner
    stat: VideoViewStat
    attribute: Optional[int]
    forward: Optional[int]
    staff: Optional[list[VideoViewStaffItem]]


class VideoTag(NamedTuple):
    tag_id: int
    tag_name: str


class VideoTags(NamedTuple):
    tags: list[VideoTag]


class MemberCard(NamedTuple):
    mid: int
    name: str
    sex: str
    face: str
    sign: str


class MemberRelation(NamedTuple):
    mid: int
    following: int
    follower: int


class NewlistPage(NamedTuple):
    count: int
    num: int
    size: int


class NewlistArchiveStat(NamedTuple):
    aid: int
    view: int
    danmaku: int
    reply: int
    favorite: int
    coin: int
    share: int
    now_rank: int
    his_rank: int
    like: int
    dislike: int
    vt: int
    vv: int


class NewlistArchiveOwner(NamedTuple):
    mid: int
    name: str
    face: str


class NewlistArchive(NamedTuple):
    aid: int
    videos: int
    tid: int
    tname: str
    copyright: int
    pic: str
    title: str
    stat: NewlistArchiveStat
    bvid: str
    desc: str
    owner: NewlistArchiveOwner


class Newlist(NamedTuple):
    archives: list[NewlistArchive]
    page: NewlistPage
