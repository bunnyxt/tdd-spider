"""Fetch observations outside transactions, then persist each business change together."""
import logging
import time
from collections.abc import Iterable, Sequence

from service import Service, ServiceError, CodeError, MemberCard, VideoViewStat
from db import TddVideo, TddVideoLog, TddMemberLog, TddSprintVideoRecord
from db.ingest import HistoryKey
from db.queries import query_video_via_aid, query_member_via_mid
from db.write import insert_video_records, insert_member_follower_records
from db.profiles import create_video, update_video_profile, update_member_profile
from db.sprint import insert_sprint_video_record
from util import get_ts_s
from core import VideoRecord, MemberFollowerRecord, NewVideo, NewMember
from .error import AlreadyExistError, NotExistError

logger = logging.getLogger('task')
STAT_FIELDS = ('view','danmaku','reply','favorite','coin','share','like')
__all__ = ['add_video_record_via_video_view','fetch_video_record_via_video_view','commit_video_records_batch',
           'commit_sprint_video_record','add_video','update_video','update_member',
           'fetch_member_follower_record',
           'commit_member_follower_records_batch']


def _stats(stat: VideoViewStat) -> dict[str, int]:
    return {field: -1 if field == 'view' and getattr(stat, field) == '--' else getattr(stat, field)
            for field in STAT_FIELDS}



def fetch_video_record_via_video_view(aid: int, service: Service,
                                     out_stat: dict[str, int] | None = None) -> VideoRecord:
    start = time.perf_counter()
    video_view_trimmed = service.get_video_view_trimmed({'aid': aid})
    if out_stat is not None:
        out_stat['http_ms'] = int((time.perf_counter() - start) * 1000)
    return VideoRecord(added=get_ts_s(), aid=aid, bvid=video_view_trimmed.bvid.removeprefix('BV'),
                       **_stats(video_view_trimmed.stat))



def add_video_record_via_video_view(aid: int, service: Service,
                                   out_stat: dict[str, int] | None = None) -> VideoRecord:
    record = fetch_video_record_via_video_view(aid, service, out_stat)
    commit_video_records_batch([record], out_stat)
    return record



def commit_video_records_batch(records: Sequence[VideoRecord],
                               out_stat: dict[str, int] | None = None) -> list[HistoryKey]:
    """Persist collected records and return their complete (aid, added, id) keys."""
    start = time.perf_counter()
    keys = insert_video_records(records)
    if out_stat is not None:
        out_stat['db_ms'] = int((time.perf_counter() - start) * 1000)
    return keys



def commit_sprint_video_record(record: VideoRecord) -> TddSprintVideoRecord:
    return insert_sprint_video_record(record.aid, record.added,
                                      {field: getattr(record, field) for field in STAT_FIELDS})



def _new_member(mid: int, card: MemberCard, added: int) -> NewMember:
    return NewMember(mid=mid, added=added, name=card.name, face=card.face,
                     sex=card.sex, sign=card.sign, code=0)



def _prepare_members(aid: int, mids: Iterable[int], service: Service,
                     strict: bool = False) -> dict[int, NewMember]:
    prepared = {}
    for mid in sorted(set(mids), key=lambda value: str(value)):
        if type(mid) is not int or mid <= 0:
            logger.warning('Skip invalid video member: aid=%s, mid=%r', aid,mid)
            continue
        if query_member_via_mid(mid) is not None:
            continue
        try:
            prepared[mid] = _new_member(mid, service.get_member_card(
                {'mid':mid}, timeout=1.5, colddown_factor=0.1), get_ts_s())
        except ServiceError as error:
            if strict:
                raise
            logger.warning('Missing video member; retry on metadata refresh: aid=%s, mid=%s, error=%s',aid,mid,error)
    return prepared



def add_video(aid: int, service: Service, test_exist: bool = True) -> TddVideo:
    if test_exist and query_video_via_aid(aid) is not None:
        raise AlreadyExistError(table='tdd_video',params={'aid':aid})
    video_view = service.get_video_view({'aid':aid})
    added = get_ts_s()
    tags = _get_video_tags_str(aid,service)
    staff = video_view.staff or []
    members = _prepare_members(aid,[video_view.owner.mid] + [s.mid for s in staff],service,strict=True)
    isvc = 2 if any(tag.upper() == 'VOCALOID中文曲' for tag in tags.split(';')) else -1
    record = VideoRecord(added=added,aid=aid,bvid=video_view.bvid.removeprefix('BV'),
                         **_stats(video_view.stat))
    new_video = NewVideo(
        added=added, aid=aid, bvid=video_view.bvid.removeprefix('BV'), mid=video_view.owner.mid,
        videos=video_view.videos, tid=video_view.tid, tname=video_view.tname,
        copyright=video_view.copyright, pic=video_view.pic, title=video_view.title,
        pubdate=video_view.pubdate, desc=video_view.desc, tags=tags,
        attribute=video_view.attribute, state=video_view.state, forward=video_view.forward,
        code=0, hasstaff=1 if video_view.staff is not None else 0, isvc=isvc, recent=2)
    inserted = create_video(new_video, new_members=list(members.values()),
                            staff=video_view.staff, first_record=record)
    if inserted is None:
        raise AlreadyExistError(table='tdd_video',params={'aid':aid})
    return inserted



def update_video(aid: int, service: Service) -> list[TddVideoLog]:
    # Fetch can be slow. No database write lock is held while waiting for HTTP.
    code_error, video_view = None, None
    try:
        video_view = service.get_video_view({'aid':aid})
    except CodeError as error:
        if error.code == -403:
            # A login failure does not establish that the video is unavailable.
            return []
        code_error = error
    # Keep the view/status observation time for change logs, before enrichment.
    added = get_ts_s()
    tags = None
    members = {}
    if video_view is not None:
        mids = [video_view.owner.mid] + [s.mid for s in (video_view.staff or [])]
        # Retry missing member cards even when owner/staff relationships are unchanged.
        members = _prepare_members(aid,mids,service)
        try:
            tags = _get_video_tags_str(aid,service)
        except ServiceError as error:
            # Keep old tags for a later retry while persisting successful view/status changes.
            logger.warning('Skip tags update (view changes kept). aid: %s, error: %s',aid,error)
    logs = update_video_profile(aid,video_view,None if code_error is None else code_error.code,
                                tags,list(members.values()),added)
    if logs is None:
        raise NotExistError(table='tdd_video',params={'aid':aid})
    return logs



def update_member(mid: int, service: Service) -> list[TddMemberLog]:
    card, code = None, 0
    try:
        card = service.get_member_card({'mid':mid},timeout=1.5,colddown_factor=0.1)
    except CodeError as error:
        # Only -404 means a missing member: retain known profile fields and record status.
        # Anti-crawler/request errors must propagate without overwriting that status.
        if error.code != -404:
            raise
        code = error.code
    logs = update_member_profile(mid,card,code,get_ts_s())
    if logs is None:
        raise NotExistError(table='tdd_member',params={'mid':mid})
    return logs



def fetch_member_follower_record(mid: int, service: Service,
                                 out_stat: dict[str, int] | None = None) -> MemberFollowerRecord:
    start = time.perf_counter()
    relation = service.get_member_relation({'vmid':mid})
    if out_stat is not None:
        out_stat['http_ms'] = int((time.perf_counter()-start)*1000)
    return MemberFollowerRecord(added=get_ts_s(),mid=mid,follower=relation.follower)



def commit_member_follower_records_batch(records: Sequence[MemberFollowerRecord],
                                         out_stat: dict[str, int] | None = None) -> list[HistoryKey]:
    """Persist collected followers and return complete (mid, added, id) keys."""
    start = time.perf_counter()
    keys = insert_member_follower_records(records)
    if out_stat is not None:
        out_stat['db_ms'] = int((time.perf_counter() - start) * 1000)
    return keys


def _get_video_tags_str(aid: int, service: Service) -> str:
    return ''.join(tag.tag_name + ';' for tag in service.get_video_tags({'aid':aid}).tags)
