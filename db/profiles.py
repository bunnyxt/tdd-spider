"""Profile persistence: row snapshots, observation ordering, and atomic change logs."""
import logging
from .models import TddVideoLog, TddMemberLog, TddVideoStaff
from .operation import DBOperation
from .values import clean_text, database_value
from .write import append_objects, append_video_records
from .observation import execute_write
from .errors import AlreadyExistError, NotExistError
from util import a2b, same_pic_url

logger = logging.getLogger('db.profiles')

def insert_video(video, staff, members, record=None):
    return execute_write(_insert_video, video, staff, members, record)

def _insert_video(session, video, staff, members, record):
    aid, added = video.aid, video.added
    if DBOperation.query_video_via_aid(aid,session) is not None:
        raise AlreadyExistError(table='tdd_video',params={'aid':aid})
    _insert_prepared_members(members,session)
    DBOperation.add(video,session)
    _claim_profile_observation('video',aid,added,session)
    for item in staff or []:
        try:
            commit_staff(added,aid,item.mid,item.title,session)
        except AlreadyExistError:
            logger.debug('Video staff relationship already exists: aid=%s, mid=%s',aid,item.mid)
    if record is not None:
        append_video_records(session,[record])
    return video

def update_video_profile(aid, view, code_error, tags, members, added):
    return execute_write(_update_video_profile, aid, view, code_error, tags, members, added)

def insert_member(member):
    return execute_write(_insert_member, member)

def _insert_member(session, member):
    DBOperation.add(member,session)
    _claim_profile_observation('member',member.mid,member.added,session)
    return member

def ensure_members(members):
    return execute_write(_ensure_members, members)

def _ensure_members(session, members):
    _insert_prepared_members(members,session)
    return []

def update_member_profile(mid, card, code, added):
    return execute_write(_update_member_profile, mid, card, code, added)

def _insert_prepared_members(prepared, session):
    for member in prepared:
        mid = member.mid
        # The unique key handles another concurrent observation of the same profile.
        fields = tuple(field for field in member.fields if field not in ('id','last_video_aid'))
        inserted = session.execute('INSERT INTO tdd_member (' + ','.join('"'+f+'"' for f in fields) + ') VALUES (' +
                        ','.join(['%s']*len(fields)) + ') ON CONFLICT (mid) DO NOTHING RETURNING mid',
                        tuple(database_value(field,getattr(member,field)) for field in fields)).first()
        if inserted is not None:
            _claim_profile_observation('member',mid,member.added,session)


def _claim_profile_observation(kind, entity_id, observed_at, session):
    """Advance a profile watermark inside its row-locked business transaction.

    The UPSERT also serializes the first watermark for an existing migrated
    profile. Equal-second inputs retain the usual last-write-wins behavior.
    A failed profile/log transaction rolls its watermark back too.
    """
    if kind not in ('video','member') or type(observed_at) is not int:
        raise ValueError('A profile observation requires a kind and integer Unix seconds')
    accepted = session.execute(
        'INSERT INTO tdd_ingest.profile_observation AS previous '
        '(entity_kind,entity_id,observed_at) VALUES (%s,%s,%s) '
        'ON CONFLICT (entity_kind,entity_id) DO UPDATE SET observed_at = EXCLUDED.observed_at '
        'WHERE previous.observed_at <= EXCLUDED.observed_at RETURNING observed_at',
        (kind,entity_id,observed_at)).first()
    if accepted is None:
        logger.warning('Skip stale profile observation: kind=%s, entity_id=%s, observed_at=%s',
                       kind,entity_id,observed_at)
        return False
    return True


def _update_video_profile(session, aid, view, code_error, tags, members, added):
    logs = []
    current = DBOperation.query_video_via_aid(aid,session,lock=True)
    if current is None:
        raise NotExistError(table='tdd_video',params={'aid':aid})
    if code_error == -403:
        return []
    if not _claim_profile_observation('video',aid,added,session):
        return []
    changes = {}
    def change(field,new):
        old, new = clean_text(getattr(current,field)), clean_text(new)
        if old == new:
            return
        changes[field] = (old,new)
        logs.append(TddVideoLog(added,aid,attr=field,
                               oldval=None if old is None else str(old),
                               newval=None if new is None else str(new)))
    if code_error is not None:
        if code_error != -403:
            change('code',code_error)
    else:
        _insert_prepared_members(members,session)
        change('code',0)
        for field in ('videos','tid','tname','copyright','pic','title','pubdate','desc','state','attribute','forward'):
            if field in ('attribute','forward') and field not in view.present_fields:
                continue
            new = clean_text(getattr(view,field))
            if field == 'pic' and current.pic is not None and new is not None and same_pic_url(new,current.pic):
                continue
            change(field,new)
        change('mid',view.owner.mid)
        # A successful full view response without staff means no staff flag.
        change('hasstaff',1 if view.staff is not None else 0)
        if view.staff is not None:
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
                    session.execute('UPDATE tdd_video_staff SET title = %s WHERE id = %s',
                                    (title,old.id))
            for mid,item in existing.items():
                if mid not in remaining:
                    continue
                DBOperation.delete_video_staff_via_id(item.id,session)
                logs.append(TddVideoLog(added,aid,attr='staff',oldval=f'mid: {item.mid}; title: {clean_text(item.title)}'))
        if tags is not None:
            if sorted(tags.split(';')) != sorted((clean_text(current.tags) or '').split(';')):
                change('tags',tags)
    if changes:
        assignments = ', '.join('"' + field + '" = %s' for field in changes)
        session.execute('UPDATE tdd_video SET ' + assignments + ' WHERE id = %s',
                        tuple(database_value(field,new) for field,(_,new) in changes.items()) + (current.id,))
    append_objects(session,logs)
    return logs


def _update_member_profile(session, mid, card, code, added):
    logs = []
    current = DBOperation.query_member_via_mid(mid,session,lock=True)
    if current is None:
        raise NotExistError(table='tdd_member',params={'mid':mid})
    if not _claim_profile_observation('member',mid,added,session):
        return []
    changes = {}
    def change(field,new):
        old, new = clean_text(getattr(current,field)), clean_text(new)
        if old == new:
            return
        changes[field] = (old,new)
        logs.append(TddMemberLog(added,mid,attr=field,
                                oldval=None if old is None else str(old),
                                newval=None if new is None else str(new)))
    change('code',code)
    if card is not None:
        for field in ('sex','name','face','sign'):
            value = getattr(card,field)
            if field == 'face' and value is not None and current.face is not None and same_pic_url(value,current.face):
                continue
            change(field,value)
    if changes:
        assignments = ', '.join('"' + field + '" = %s' for field in changes)
        session.execute('UPDATE tdd_member SET ' + assignments + ' WHERE id = %s',
                        tuple(database_value(field,new) for field,(_,new) in changes.items()) + (current.id,))
    append_objects(session,logs)
    return logs


def commit_staff(added, aid, mid, title, session, test_exist=True):
    if test_exist and DBOperation.query_video_staff_via_aid_mid(aid,mid,session) is not None:
        raise AlreadyExistError(table='video_staff',params={'aid':aid,'mid':mid})
    obj = TddVideoStaff(added=added,aid=aid,bvid=a2b(aid),mid=mid,title=clean_text(title))
    DBOperation.add(obj,session)
    return obj
