from typing import Optional, NamedTuple

__all__ = [
    'VideoStat',
    'VideoViewOwner', 'VideoViewStat', 'VideoViewStaffItem', 'VideoView',
    'VideoTag', 'VideoTags',
    'MemberSpace',
    'MemberRelation',
    'ArchiveRankByPartionPage', 'ArchiveRankByPartionArchiveStat', 'ArchiveRankByPartionArchive',
    'ArchiveRankByPartion',
    'NewlistPage', 'NewlistArchiveStat', 'NewlistArchive', 'Newlist',
]


class VideoStat(NamedTuple):
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


class MemberSpace(NamedTuple):
    mid: int
    name: str
    sex: str
    face: str
    sign: str


class MemberRelation(NamedTuple):
    mid: int
    following: int
    follower: int


class ArchiveRankByPartionPage(NamedTuple):
    count: int
    num: int
    size: int


class ArchiveRankByPartionArchiveStat(NamedTuple):
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


class ArchiveRankByPartionArchive(NamedTuple):
    aid: int
    videos: int
    tid: int
    tname: str
    copyright: int
    pic: str
    title: str
    stat: ArchiveRankByPartionArchiveStat
    bvid: str
    description: str
    mid: int


class ArchiveRankByPartion(NamedTuple):
    archives: list[ArchiveRankByPartionArchive]
    page: ArchiveRankByPartionPage


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
    description: str
    mid: int


class Newlist(NamedTuple):
    archives: list[NewlistArchive]
    page: NewlistPage
