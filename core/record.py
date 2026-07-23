from typing import NamedTuple, Optional

__all__ = ['RecordNew', 'MemberFollowerRecordNew']


# Lightweight, immutable, session-independent video record DTO. This is the type
# that flows through the app (fetch -> queue -> csv/db archive -> analysis); ORM
# objects (db.TddVideoRecord) are confined to the persistence call inside task
# functions and never leave them. Kept here in `core` as a global domain type
# (distinct from the API-response DTOs in service.response).
# TODO: fold the legacy `Record` namedtuple in 51 into this once migrated.
class RecordNew(NamedTuple):
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
    dislike: int
    now_rank: int
    his_rank: int
    vt: Optional[int]
    vv: Optional[int]


# Lightweight, session-independent member-follower record DTO. Same role as
# RecordNew for the follower-count time series (fetch -> queue -> batch INSERT
# into tdd_member_follower_record); ORM objects never cross the thread boundary.
class MemberFollowerRecordNew(NamedTuple):
    added: int
    mid: int
    follower: int
