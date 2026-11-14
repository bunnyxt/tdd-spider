"""Fetch observations outside transactions, then persist each business change together."""
import logging
import time
from uuid import uuid4, uuid5

from service import Service, ServiceError, CodeError
from db import DBOperation, Session, TddVideo, TddVideoRecord, TddVideoLog, TddVideoStaff, TddMember, \
    TddMemberFollowerRecord, TddMemberLog, TddSprintVideoRecord
from db.write import append_objects
from db.values import clean_text
from db.replay import observe
from util import get_ts_s, a2b, same_pic_url
from core import RecordNew, MemberFollowerRecordNew
from .error import AlreadyExistError, NotExistError

logger = logging.getLogger('task')
STAT_FIELDS = ('view','danmaku','reply','favorite','coin','share','like')
__all__ = ['add_video_record_via_video_view','fetch_video_record_via_video_view','commit_video_records_batch',
           'commit_video_record_via_video_view','commit_video_record_via_newlist_archive_stat',
           'add_sprint_video_record_via_video_view','add_video','update_video','add_member','update_member',
           'commit_staff','add_member_follower_record','fetch_member_follower_record',
           'commit_member_follower_records_batch','get_video_tags_str']


def _stats(stat):
    return {field: -1 if field == 'view' and getattr(stat, field) == '--' else getattr(stat, field)
            for field in STAT_FIELDS}


def fetch_video_record_via_video_view(aid, service, out_stat=None):
    start = time.perf_counter()
    view = service.get_video_view_trimmed({'aid': aid})
    if out_stat is not None:
        out_stat['http_ms'] = int((time.perf_counter() - start) * 1000)
    return RecordNew(added=get_ts_s(), aid=aid, bvid=view.bvid.removeprefix('BV'), **_stats(view.stat))


def add_video_record_via_video_view(aid, service, session, out_stat=None):
    record = fetch_video_record_via_video_view(aid, service, out_stat)
    commit_video_records_batch([record], session, out_stat)
    return record


def _commit_objects(objects, session, out_stat=None, batch_id=None):
    start = time.perf_counter()
    persisted = observe('append', {'objects':objects}, batch_id=batch_id)
    if out_stat is not None:
        out_stat['db_ms'] = int((time.perf_counter() - start) * 1000)
    return persisted


def commit_video_records_batch(records, session, out_stat=None, batch_id=None):
    return _commit_objects([TddVideoRecord(added=r.added, aid=r.aid,
                           **{field:getattr(r,field) for field in STAT_FIELDS}) for r in records],
                           session, out_stat, batch_id)


def commit_video_record_via_video_view(video_view, added, session):
    obj = TddVideoRecord(added=added, aid=video_view.aid, **_stats(video_view.stat))
    return _commit_objects([obj], session)[0]


def commit_video_record_via_newlist_archive_stat(stat, added, aid, session):
    obj = TddVideoRecord(added=added, aid=stat.aid if stat.aid != 0 else aid, **_stats(stat))
    return _commit_objects([obj], session)[0]


def add_sprint_video_record_via_video_view(aid, service, session):
    view = service.get_video_view_trimmed({'aid':aid})
    return observe('sprint-observe',{'record':TddSprintVideoRecord(added=get_ts_s(), aid=aid, **_stats(view.stat))})


def _present(obj, field):
    fields = getattr(obj, 'present_fields', None)
    return fields is None or field in fields


def _member_object(mid, card, added):
    return TddMember(mid=mid, added=added, code=0,
                     **{field:clean_text(getattr(card,field)) for field in ('name','face','sex','sign')})


def _prepare_members(aid, mids, service, session, strict=False):
    prepared = {}
    for mid in sorted(set(mids), key=lambda value: str(value)):
        if type(mid) is not int or mid <= 0:
            logger.warning('Skip invalid video member: aid=%s, mid=%r', aid,mid)
            continue
        if DBOperation.query_member_via_mid(mid,session) is not None:
            continue
        try:
            prepared[mid] = _member_object(mid, service.get_member_card(
                {'mid':mid}, timeout=1.5, colddown_factor=0.1), get_ts_s())
        except ServiceError as error:
            if strict:
                raise
            logger.warning('Missing video member; retry on metadata refresh: aid=%s, mid=%s, error=%s',aid,mid,error)
    return prepared


def _insert_prepared_members(prepared, session):
    for mid, member in prepared.items():
        # The unique key handles another concurrent observation of the same profile.
        fields = tuple(field for field in member.fields if field not in ('id','last_video_aid'))
        from db.values import database_value
        session.execute('INSERT INTO tdd_member (' + ','.join('"'+f+'"' for f in fields) + ') VALUES (' +
                        ','.join(['%s']*len(fields)) + ') ON CONFLICT (mid) DO NOTHING',
                        tuple(database_value(field,getattr(member,field)) for field in fields))


def add_video(aid, service, session, test_exist=True, commit_video_record=True):
    if test_exist and DBOperation.query_video_via_aid(aid,session) is not None:
        raise AlreadyExistError(table='tdd_video',params={'aid':aid})
    view = service.get_video_view({'aid':aid})
    if view.title is None:
        logger.warning('Reject new video with null title: aid=%s',aid)
        raise ValueError('New video title must not be null')
    added = get_ts_s()
    tags = get_video_tags_str(aid,service)
    staff = view.staff or []
    members = _prepare_members(aid,[view.owner.mid] + [s.mid for s in staff],service,session,strict=True)
    video = TddVideo(added=added, aid=aid, bvid=view.bvid.removeprefix('BV'), mid=view.owner.mid,
                     code=0, tags=tags, recent=2, hasstaff=1 if view.staff is not None else 0,
                     **{field:clean_text(getattr(view,field)) for field in
                        ('videos','tid','tname','copyright','pic','title','pubdate','desc','attribute','state','forward')})
    if any(tag.upper() == 'VOCALOID中文曲' for tag in tags.split(';')):
        video.isvc = 2
    return observe('video-add',dict(aid=aid,video=video,view=view,members=list(members.values()),
                                    added=added,commit_video_record=commit_video_record))


def _apply_video_add(payload,session,batch_id):
    aid,video,view,added = (payload[field] for field in ('aid','video','view','added'))
    members = {member.mid:member for member in payload['members']}
    staff = view.staff or []
    commit_video_record = payload['commit_video_record']
    if DBOperation.query_video_via_aid(aid,session) is not None:
        raise AlreadyExistError(table='tdd_video',params={'aid':aid})
    _insert_prepared_members(members,session)
    DBOperation.add(video,session)
    for item in staff:
        try:
            commit_staff(added,aid,item.mid,item.title,session)
        except AlreadyExistError:
            logger.debug('Video staff relationship already exists: aid=%s, mid=%s',aid,item.mid)
    if commit_video_record:
        append_objects(session,[TddVideoRecord(added=added,aid=aid,**_stats(view.stat))],uuid5(batch_id,'tdd_video_record'))
    return video


def _change(current, field, new, logs, added, key, model):
    old, new = clean_text(getattr(current,field)), clean_text(new)
    if old == new:
        return False
    logs.append(model(added=added, **key, attr=field, oldval=None if old is None else str(old),
                      newval=None if new is None else str(new)))
    setattr(current,field,new)
    return True


def update_video(aid, service, session, out_context=None):
    # Fetch can be slow. No database write lock is held while waiting for HTTP.
    code_error, view = None, None
    try:
        view = service.get_video_view({'aid':aid})
        if out_context is not None:
            out_context['video_view'] = view
    except CodeError as error:
        code_error = error
    tags = None
    members = {}
    if view is not None:
        mids = [view.owner.mid] + [s.mid for s in (view.staff or [])]
        members = _prepare_members(aid,mids,service,session)
        try:
            tags = get_video_tags_str(aid,service)
        except ServiceError as error:
            logger.warning('Skip tags update (view changes kept). aid: %s, error: %s',aid,error)
    return observe('video-update',dict(aid=aid,view=view,code_error=None if code_error is None else code_error.code,
                                       tags=tags,members=list(members.values()),added=get_ts_s()))


def _apply_video_update(payload,session,batch_id):
    aid,view,tags,added,code_error = (payload[field] for field in ('aid','view','tags','added','code_error'))
    members = {member.mid:member for member in payload['members']}
    logs = []
    current = DBOperation.query_video_via_aid(aid,session,lock=True)
    if current is None:
        raise NotExistError(table='tdd_video',params={'aid':aid})
    changed = []
    def change(field,new):
        if _change(current,field,new,logs,added,{'aid':aid},TddVideoLog):
            changed.append(field)
    if code_error is not None:
        if code_error != -403:
            change('code',code_error)
    else:
        _insert_prepared_members(members,session)
        change('code',0)
        for field in ('videos','tid','tname','copyright','pic','title','pubdate','desc','state','attribute','forward'):
            if not _present(view,field):
                continue
            new = clean_text(getattr(view,field))
            if field == 'pic' and current.pic is not None and new is not None and same_pic_url(new,current.pic):
                continue
            change(field,new)
        change('mid',view.owner.mid)
        # A successful full view response without staff means no staff flag.
        change('hasstaff',1 if view.staff is not None else 0)
        if _present(view,'staff'):
            existing = {item.mid:item for item in DBOperation.query_video_staff_via_aid(aid,session)}
            remaining,seen = set(existing),set()
            for item in view.staff or []:
                if item.mid in seen:
                    logger.warning('Skip repeated video staff relationship: aid=%s, mid=%s',aid,item.mid)
                    continue
                seen.add(item.mid)
                remaining.discard(item.mid)
                old = existing.get(item.mid)
                title = clean_text(item.title)
                if old is None:
                    new_staff = commit_staff(added,aid,item.mid,title,session,test_exist=False)
                    logs.append(TddVideoLog(added,aid,attr='staff',newval=f'mid: {new_staff.mid}; title: {title}'))
                elif clean_text(old.title) != title:
                    logs.append(TddVideoLog(added,aid,attr='staff',oldval=f'mid: {old.mid}; title: {clean_text(old.title)}',newval=f'mid: {item.mid}; title: {title}'))
                    old.title = title
                    DBOperation.update(old,['title'],session)
            for mid,item in existing.items():
                if mid not in remaining:
                    continue
                DBOperation.delete_video_staff_via_id(item.id,session)
                logs.append(TddVideoLog(added,aid,attr='staff',oldval=f'mid: {item.mid}; title: {clean_text(item.title)}'))
        if tags is not None:
            if sorted(tags.split(';')) != sorted((clean_text(current.tags) or '').split(';')):
                change('tags',tags)
    DBOperation.update(current,changed,session)
    append_objects(session,logs,uuid5(batch_id,'tdd_video_log'))
    return logs


def ensure_video_member(aid, mid, service, session):
    prepared = _prepare_members(aid,[mid],service,session)
    return observe('member-ensure',dict(members=list(prepared.values())))


def add_member(mid, service, session, test_exist=True):
    if type(mid) is not int or mid <= 0:
        raise ValueError(f'Member mid must be a positive integer: {mid!r}')
    if test_exist and DBOperation.query_member_via_mid(mid,session) is not None:
        raise AlreadyExistError(table='member',params={'mid':mid})
    member = _member_object(mid,service.get_member_card({'mid':mid},timeout=1.5,colddown_factor=0.1),get_ts_s())
    return observe('member-add',dict(member=member))


def update_member(mid, service, session):
    card, code = None, 0
    try:
        card = service.get_member_card({'mid':mid},timeout=1.5,colddown_factor=0.1)
    except CodeError as error:
        if error.code != -404:
            raise
        code = error.code
    return observe('member-update',dict(mid=mid,card=card,code=code,added=get_ts_s()))


def _apply_member_update(payload,session,batch_id):
    mid,card,code,added = (payload[field] for field in ('mid','card','code','added'))
    logs = []
    current = DBOperation.query_member_via_mid(mid,session,lock=True)
    if current is None:
        raise NotExistError(table='tdd_member',params={'mid':mid})
    changed = []
    def change(field,new):
        if _change(current,field,new,logs,added,{'mid':mid},TddMemberLog):
            changed.append(field)
    change('code',code)
    if card is not None:
        for field in ('sex','name','face','sign'):
            if not _present(card,field):
                continue
            value = getattr(card,field)
            if field == 'face' and value is not None and current.face is not None and same_pic_url(value,current.face):
                continue
            change(field,value)
    DBOperation.update(current,changed,session)
    append_objects(session,logs,uuid5(batch_id,'tdd_member_log'))
    return logs


def commit_staff(added, aid, mid, title, session, test_exist=True):
    if test_exist and DBOperation.query_video_staff_via_aid_mid(aid,mid,session) is not None:
        raise AlreadyExistError(table='video_staff',params={'aid':aid,'mid':mid})
    obj = TddVideoStaff(added=added,aid=aid,bvid=a2b(aid),mid=mid,title=clean_text(title))
    DBOperation.add(obj,session)
    return obj


def fetch_member_follower_record(mid, service, out_stat=None):
    start = time.perf_counter()
    relation = service.get_member_relation({'vmid':mid})
    if out_stat is not None:
        out_stat['http_ms'] = int((time.perf_counter()-start)*1000)
    return MemberFollowerRecordNew(added=get_ts_s(),mid=mid,follower=relation.follower)


def add_member_follower_record(mid, service, session):
    record = fetch_member_follower_record(mid,service)
    return commit_member_follower_records_batch([record],session)[0]


def commit_member_follower_records_batch(records, session, out_stat=None, batch_id=None):
    return _commit_objects([TddMemberFollowerRecord(**record._asdict()) for record in records],session,out_stat,batch_id)


def get_video_tags_str(aid, service):
    return ''.join(clean_text(tag.tag_name) + ';' for tag in service.get_video_tags({'aid':aid}).tags)


def apply_observation(operation,payload,session,batch_id):
    if operation == 'video-add':
        return _apply_video_add(payload,session,batch_id)
    if operation == 'video-update':
        return _apply_video_update(payload,session,batch_id)
    if operation == 'member-update':
        return _apply_member_update(payload,session,batch_id)
    if operation == 'sprint-observe':
        record = payload['record']
        append_objects(session,[record],uuid5(batch_id,'tdd_sprint_video_record'))
        if record.view is not None and record.view >= 1000000:
            session.execute('UPDATE tdd_sprint_video SET status = %s WHERE aid = %s',('finished',record.aid))
        return record
    if operation in ('sprint-daily-record','sprint-daily-summary'):
        from db.values import database_value
        if operation == 'sprint-daily-record':
            table = 'tdd_sprint_daily_record'
            columns = ('added','date','aid','view','viewincr','pday','lday')
        else:
            table = 'tdd_sprint_daily'
            columns = ('added','date','correct','vidnum','newvids','millvids','viewincr','viewincrincr','comment')
        if set(payload) != set(columns):
            raise ValueError('Unexpected sprint daily columns')
        result = session.execute('INSERT INTO ' + table + ' (' + ','.join('"'+c+'"' for c in columns) +
                                 ') VALUES (' + ','.join(['%s']*len(columns)) + ') RETURNING id',
                                 tuple(database_value(c,payload[c]) for c in columns))
        return dict(result.first())
    if operation == 'member-add':
        return DBOperation.add(payload['member'],session)
    if operation == 'member-ensure':
        _insert_prepared_members({member.mid:member for member in payload['members']},session)
        return []
    raise ValueError('Unsupported replay operation')
