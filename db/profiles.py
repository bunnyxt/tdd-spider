"""Profile persistence: row snapshots, observation ordering, and atomic change logs."""
import logging
from .models import TddVideo, TddMember, TddVideoLog, TddMemberLog, TddVideoStaff
from .values import clean_text, database_value, epoch
from .write import _append_objects, append_video_records
from .observation import execute_write
from util import a2b, same_pic_url

logger = logging.getLogger('db.profiles')

def insert_video(*, added, aid, bvid, mid, title, pubdate, staff, members,
                 videos=None, tid=None, tname=None, copyright=None, pic=None, desc=None,
                 tags=None, attribute=None, state=0, forward=None, hasstaff=0, isvc=-1,
                 record=None):
    """Create a video and return its stored snapshot, or None for an existing aid."""
    return execute_write(
        _insert_video, added=added, aid=aid, bvid=bvid, mid=mid, title=title, pubdate=pubdate,
        staff=staff, members=members, videos=videos, tid=tid, tname=tname, copyright=copyright,
        pic=pic, desc=desc, tags=tags, attribute=attribute, state=state, forward=forward,
        hasstaff=hasstaff, isvc=isvc, record=record)


def _insert_video(conn, *, added, aid, bvid, mid, title, pubdate, staff, members,
                  videos, tid, tname, copyright, pic, desc, tags, attribute, state,
                  forward, hasstaff, isvc, record, run_id=None):
    # New-video business values are explicit rather than inherited from a row object.
    row = conn.execute(
        'INSERT INTO tdd_video '
        '(added,aid,bvid,videos,tid,tname,copyright,pic,title,pubdate,"desc",tags,mid,'
        'code,attribute,state,forward,hasstaff,solo,original,employed,isvc,engine,freq,activity,recent) '
        'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'
        '0,%s,%s,%s,%s,-1,-1,-1,%s,-1,0,0,2) '
        'ON CONFLICT (aid) DO NOTHING '
        'RETURNING id,added,aid,bvid,videos,tid,tname,copyright,pic,title,pubdate,"desc",tags,mid,'
        'code,attribute,state,forward,hasstaff,solo,original,employed,isvc,engine,freq,activity,recent',
        (database_value('added',added),aid,clean_text(bvid),videos,tid,clean_text(tname),
         copyright,clean_text(pic),clean_text(title),pubdate,clean_text(desc),clean_text(tags),mid,
         attribute,state,forward,hasstaff,isvc)).fetchone()
    if row is None:
        return None
    video = TddVideo(**dict(zip(TddVideo.fields,map(epoch,row))))
    _insert_prepared_members(members,conn)
    _claim_profile_observation('video',aid,added,conn)
    for item in staff or []:
        if _insert_staff(conn,added,aid,item.mid,item.title) is None:
            logger.debug('Video staff relationship already exists: aid=%s, mid=%s',aid,item.mid)
    if record is not None:
        append_video_records(conn,[record],run_id=run_id)
    return video

def update_video_profile(aid, view, code_error, tags, members, added):
    """Return change logs (possibly empty), or None when the video is missing."""
    return execute_write(_update_video_profile, aid, view, code_error, tags, members, added)

def insert_member(member):
    """Return the inserted member, or None when its mid already exists."""
    return execute_write(_insert_member, member)

def _insert_member(conn, member, *, run_id=None):
    fields = tuple(field for field in member.fields if field not in ('id','last_video_aid'))
    row = conn.execute(
        'INSERT INTO tdd_member (' + ','.join('"'+f+'"' for f in fields) + ') VALUES (' +
        ','.join(['%s']*len(fields)) + ') ON CONFLICT (mid) DO NOTHING RETURNING id',
        tuple(database_value(field,getattr(member,field)) for field in fields)).fetchone()
    if row is None:
        return None
    member.id = row[0]
    _claim_profile_observation('member',member.mid,member.added,conn)
    return member

def ensure_members(members):
    return execute_write(_ensure_members, members)

def _ensure_members(conn, members, *, run_id=None):
    _insert_prepared_members(members,conn)
    return []

def update_member_profile(mid, card, code, added):
    """Return change logs (possibly empty), or None when the member is missing."""
    return execute_write(_update_member_profile, mid, card, code, added)

def _insert_prepared_members(prepared, conn):
    for member in prepared:
        _insert_member(conn,member)


def _claim_profile_observation(kind, entity_id, observed_at, conn):
    """Advance a profile watermark inside its row-locked business transaction.

    The UPSERT also serializes the first watermark for an existing migrated
    profile. Equal-second inputs retain the usual last-write-wins behavior.
    A failed profile/log transaction rolls its watermark back too.
    """
    if kind not in ('video','member') or type(observed_at) is not int:
        raise ValueError('A profile observation requires a kind and integer Unix seconds')
    accepted = conn.execute(
        'INSERT INTO tdd_ingest.profile_observation AS previous '
        '(entity_kind,entity_id,observed_at) VALUES (%s,%s,%s) '
        'ON CONFLICT (entity_kind,entity_id) DO UPDATE SET observed_at = EXCLUDED.observed_at '
        'WHERE previous.observed_at <= EXCLUDED.observed_at RETURNING observed_at',
        (kind,entity_id,observed_at)).fetchone()
    if accepted is None:
        logger.warning('Skip stale profile observation: kind=%s, entity_id=%s, observed_at=%s',
                       kind,entity_id,observed_at)
        return False
    return True


def _update_video_profile(conn, aid, view, code_error, tags, members, added, *, run_id=None):
    logs = []
    current = _query_video(conn,aid)
    if current is None:
        return None
    if code_error == -403:
        return []
    if not _claim_profile_observation('video',aid,added,conn):
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
        _insert_prepared_members(members,conn)
        change('code',0)
        for field in ('videos','tid','tname','copyright','pic','title','pubdate','desc','state','attribute','forward'):
            if field in ('attribute','forward') and not getattr(view,'response_has_'+field):
                continue
            new = clean_text(getattr(view,field))
            if field == 'pic' and current.pic is not None and new is not None and same_pic_url(new,current.pic):
                continue
            change(field,new)
        change('mid',view.owner.mid)
        # A successful full view response without staff means no staff flag.
        change('hasstaff',1 if view.staff is not None else 0)
        if view.staff is not None:
            existing = {item.mid:item for item in _query_staff(conn,aid)}
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
                    new_staff = _insert_staff(conn,added,aid,item.mid,title)
                    if new_staff is not None:
                        logs.append(TddVideoLog(added,aid,attr='staff',newval=f'mid: {new_staff.mid}; title: {title}'))
                elif clean_text(old.title) != title:
                    logs.append(TddVideoLog(added,aid,attr='staff',oldval=f'mid: {old.mid}; title: {clean_text(old.title)}',newval=f'mid: {item.mid}; title: {title}'))
                    conn.execute('UPDATE tdd_video_staff SET title = %s WHERE id = %s',
                                    (title,old.id))
            for mid,item in existing.items():
                if mid not in remaining:
                    continue
                conn.execute('DELETE FROM tdd_video_staff WHERE id = %s',(item.id,))
                logs.append(TddVideoLog(added,aid,attr='staff',oldval=f'mid: {item.mid}; title: {clean_text(item.title)}'))
        if tags is not None:
            if sorted(tags.split(';')) != sorted((clean_text(current.tags) or '').split(';')):
                change('tags',tags)
    if changes:
        assignments = ', '.join('"' + field + '" = %s' for field in changes)
        conn.execute('UPDATE tdd_video SET ' + assignments + ' WHERE id = %s',
                        tuple(database_value(field,new) for field,(_,new) in changes.items()) + (current.id,))
    _append_objects(conn,logs,run_id=run_id)
    return logs


def _update_member_profile(conn, mid, card, code, added, *, run_id=None):
    logs = []
    current = _query_member(conn,mid)
    if current is None:
        return None
    if not _claim_profile_observation('member',mid,added,conn):
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
        conn.execute('UPDATE tdd_member SET ' + assignments + ' WHERE id = %s',
                        tuple(database_value(field,new) for field,(_,new) in changes.items()) + (current.id,))
    _append_objects(conn,logs,run_id=run_id)
    return logs


def _insert_staff(conn, added, aid, mid, title):
    obj = TddVideoStaff(added=added,aid=aid,bvid=a2b(aid),mid=mid,title=clean_text(title))
    row = conn.execute(
        'INSERT INTO tdd_video_staff (added,aid,bvid,mid,title) VALUES (%s,%s,%s,%s,%s) '
        'ON CONFLICT (aid,mid) DO NOTHING RETURNING id',
        (database_value('added',added),aid,obj.bvid,mid,obj.title)).fetchone()
    if row is None:
        return None
    obj.id = row[0]
    return obj


def _query_video(conn, aid):
    columns = ','.join('"'+field+'"' for field in TddVideo.fields)
    row = conn.execute('SELECT '+columns+' FROM tdd_video WHERE aid = %s FOR UPDATE',(aid,)).fetchone()
    return TddVideo(**dict(zip(TddVideo.fields,map(epoch,row)))) if row is not None else None


def _query_member(conn, mid):
    columns = ','.join('"'+field+'"' for field in TddMember.fields)
    row = conn.execute('SELECT '+columns+' FROM tdd_member WHERE mid = %s FOR UPDATE',(mid,)).fetchone()
    return TddMember(**dict(zip(TddMember.fields,map(epoch,row)))) if row is not None else None


def _query_staff(conn, aid):
    columns = ','.join('"'+field+'"' for field in TddVideoStaff.fields)
    rows = conn.execute('SELECT '+columns+' FROM tdd_video_staff WHERE aid = %s',(aid,)).fetchall()
    return [TddVideoStaff(**dict(zip(TddVideoStaff.fields,map(epoch,row)))) for row in rows]
