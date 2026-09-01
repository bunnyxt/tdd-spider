from typing import Optional, NamedTuple

__all__ = [
    'VideoViewOwner', 'VideoViewStat', 'VideoViewStaffItem', 'VideoView', 'VideoViewTrimmed',
    'VideoViewTrimmedBatchItem',
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


# minimal view payload for stat-record jobs: exactly what RecordNew needs.
# Served by the trimmed video_view worker (service/workers/video_view/), whose
# response is a strict subset of the full view -- so the same parser also
# accepts the full API response (direct mode / full worker).
class VideoViewTrimmed(NamedTuple):
    bvid: str
    aid: int
    stat: VideoViewStat


# per-aid outcome of Service.get_video_view_trimmed_batch. Exactly one of
# view/error is set:
# - view:  the item passed every check -- usable exactly like a single
#          get_video_view_trimmed result
# - error: CodeError     -> upstream said no (deleted/hidden/-403); route it
#                           the same way as a single-path CodeError
#          ResponseError -> transient per-item failure (item timeout, fetch
#                           error, non-JSON upstream, non-200 upstream status);
#                           the caller owns the retry, this item only
# Failures that invalidate the WHOLE batch (transport failure, contract or
# identity violation) never appear here -- get_video_view_trimmed_batch raises
# for those instead of returning items.
class VideoViewTrimmedBatchItem(NamedTuple):
    aid: int
    view: Optional[VideoViewTrimmed]
    error: Optional[Exception]


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
