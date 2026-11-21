"""Fetch observations outside transactions, then persist each business change together."""
import logging
import time

from service import Service, ServiceError, CodeError
from db import TddMember, TddSprintVideoRecord
from db.queries import query_video_via_aid, query_member_via_mid
from db.write import insert_video_records, insert_member_follower_records
from db.profiles import insert_video, update_video_profile, insert_member, update_member_profile, ensure_members
from db.sprint import insert_sprint_video_record
from db.values import clean_text
from util import get_ts_s, a2b
from core import VideoRecord, MemberFollowerRecord
from .error import AlreadyExistError, NotExistError

logger = logging.getLogger('task')
STAT_FIELDS = ('view','danmaku','reply','favorite','coin','share','like')
__all__ = ['add_video_record_via_video_view','fetch_video_record_via_video_view','commit_video_records_batch',
           'commit_video_record_via_video_view','commit_video_record_via_newlist_archive_stat',
           'add_sprint_video_record_via_video_view','add_video','update_video','add_member','update_member',
           'add_member_follower_record','fetch_member_follower_record',
           'commit_member_follower_records_batch']


def _stats(stat):
    return {field: -1 if field == 'view' and getattr(stat, field) == '--' else getattr(stat, field)
            for field in STAT_FIELDS}



def fetch_video_record_via_video_view(aid, service, out_stat=None):
    start = time.perf_counter()
    view = service.get_video_view_trimmed({'aid': aid})
    if out_stat is not None:
        out_stat['http_ms'] = int((time.perf_counter() - start) * 1000)
    return VideoRecord(added=get_ts_s(), aid=aid, bvid=view.bvid.removeprefix('BV'), **_stats(view.stat))



def add_video_record_via_video_view(aid, service, out_stat=None):
    record = fetch_video_record_via_video_view(aid, service, out_stat)
    commit_video_records_batch([record], out_stat)
    return record



def commit_video_records_batch(records, out_stat=None):
    """Persist collected records and return their complete (aid, added, id) keys."""
    start = time.perf_counter()
    keys = insert_video_records(records)
    if out_stat is not None:
        out_stat['db_ms'] = int((time.perf_counter() - start) * 1000)
    return keys



def commit_video_record_via_video_view(video_view, added):
    record = VideoRecord(added=added, aid=video_view.aid, bvid=video_view.bvid.removeprefix('BV'),
                    **_stats(video_view.stat))
    return commit_video_records_batch([record])[0]



def commit_video_record_via_newlist_archive_stat(stat, added, aid):
    aid = stat.aid if stat.aid != 0 else aid
    record = VideoRecord(added=added, aid=aid, bvid=a2b(aid), **_stats(stat))
    return commit_video_records_batch([record])[0]



def add_sprint_video_record_via_video_view(aid, service):
    view = service.get_video_view_trimmed({'aid':aid})
    return insert_sprint_video_record(TddSprintVideoRecord(added=get_ts_s(), aid=aid, **_stats(view.stat)))



def _member_object(mid, card, added):
    return TddMember(mid=mid, added=added, code=0,
                     **{field:clean_text(getattr(card,field)) for field in ('name','face','sex','sign')})



def _prepare_members(aid, mids, service, strict=False):
    prepared = {}
    for mid in sorted(set(mids), key=lambda value: str(value)):
        if type(mid) is not int or mid <= 0:
            logger.warning('Skip invalid video member: aid=%s, mid=%r', aid,mid)
            continue
        if query_member_via_mid(mid) is not None:
            continue
        try:
            prepared[mid] = _member_object(mid, service.get_member_card(
                {'mid':mid}, timeout=1.5, colddown_factor=0.1), get_ts_s())
        except ServiceError as error:
            if strict:
                raise
            logger.warning('Missing video member; retry on metadata refresh: aid=%s, mid=%s, error=%s',aid,mid,error)
    return prepared



def add_video(aid, service, test_exist=True, commit_video_record=True):
    if test_exist and query_video_via_aid(aid) is not None:
        raise AlreadyExistError(table='tdd_video',params={'aid':aid})
    video_view = service.get_video_view({'aid':aid})
    if video_view.title is None:
        logger.warning('Reject new video with null title: aid=%s',aid)
        raise ValueError('New video title must not be null')
    added = get_ts_s()
    tags = _get_video_tags_str(aid,service)
    staff = video_view.staff or []
    members = _prepare_members(aid,[video_view.owner.mid] + [s.mid for s in staff],service,strict=True)
    isvc = 2 if any(tag.upper() == 'VOCALOID中文曲' for tag in tags.split(';')) else -1
    record = VideoRecord(added=added,aid=aid,bvid=video_view.bvid.removeprefix('BV'),
                         **_stats(video_view.stat)) if commit_video_record else None
    inserted = insert_video(
        added=added, aid=aid, bvid=video_view.bvid.removeprefix('BV'), mid=video_view.owner.mid,
        videos=video_view.videos, tid=video_view.tid, tname=video_view.tname,
        copyright=video_view.copyright, pic=video_view.pic, title=video_view.title,
        pubdate=video_view.pubdate, desc=video_view.desc, tags=tags,
        attribute=video_view.attribute, state=video_view.state, forward=video_view.forward,
        hasstaff=1 if video_view.staff is not None else 0, isvc=isvc,
        staff=video_view.staff, members=list(members.values()), record=record)
    if inserted is None:
        raise AlreadyExistError(table='tdd_video',params={'aid':aid})
    return inserted



def update_video(aid, service, out_context=None):
    # Fetch can be slow. No database write lock is held while waiting for HTTP.
    code_error, view = None, None
    try:
        view = service.get_video_view({'aid':aid})
        if out_context is not None:
            out_context['video_view'] = view
    except CodeError as error:
        code_error = error
    # Metadata enrichment may take much longer than the view request. Order
    # profile changes by when the view/status was observed, before enrichment.
    added = get_ts_s()
    tags = None
    members = {}
    if view is not None:
        mids = [view.owner.mid] + [s.mid for s in (view.staff or [])]
        members = _prepare_members(aid,mids,service)
        try:
            tags = _get_video_tags_str(aid,service)
        except ServiceError as error:
            logger.warning('Skip tags update (view changes kept). aid: %s, error: %s',aid,error)
    logs = update_video_profile(aid,view,None if code_error is None else code_error.code,
                                tags,list(members.values()),added)
    if logs is None:
        raise NotExistError(table='tdd_video',params={'aid':aid})
    return logs



def ensure_video_member(aid, mid, service):
    prepared = _prepare_members(aid,[mid],service)
    return ensure_members(list(prepared.values()))



def add_member(mid, service, test_exist=True):
    if type(mid) is not int or mid <= 0:
        raise ValueError(f'Member mid must be a positive integer: {mid!r}')
    if test_exist and query_member_via_mid(mid) is not None:
        raise AlreadyExistError(table='member',params={'mid':mid})
    member = _member_object(mid,service.get_member_card({'mid':mid},timeout=1.5,colddown_factor=0.1),get_ts_s())
    inserted = insert_member(member)
    if inserted is None:
        raise AlreadyExistError(table='member',params={'mid':mid})
    return inserted



def update_member(mid, service):
    card, code = None, 0
    try:
        card = service.get_member_card({'mid':mid},timeout=1.5,colddown_factor=0.1)
    except CodeError as error:
        if error.code != -404:
            raise
        code = error.code
    logs = update_member_profile(mid,card,code,get_ts_s())
    if logs is None:
        raise NotExistError(table='tdd_member',params={'mid':mid})
    return logs



def fetch_member_follower_record(mid, service, out_stat=None):
    start = time.perf_counter()
    relation = service.get_member_relation({'vmid':mid})
    if out_stat is not None:
        out_stat['http_ms'] = int((time.perf_counter()-start)*1000)
    return MemberFollowerRecord(added=get_ts_s(),mid=mid,follower=relation.follower)



def add_member_follower_record(mid, service):
    record = fetch_member_follower_record(mid,service)
    return commit_member_follower_records_batch([record])[0]



def commit_member_follower_records_batch(records, out_stat=None):
    """Persist collected followers and return complete (mid, added, id) keys."""
    start = time.perf_counter()
    keys = insert_member_follower_records(records)
    if out_stat is not None:
        out_stat['db_ms'] = int((time.perf_counter() - start) * 1000)
    return keys


def _get_video_tags_str(aid, service):
    return ''.join(clean_text(tag.tag_name) + ';' for tag in service.get_video_tags({'aid':aid}).tags)
