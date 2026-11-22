"""Profile persistence: row snapshots, observation ordering, and atomic change logs."""
import logging
from typing import Literal
from uuid import UUID

from psycopg import Connection
from core import NewVideo, NewMember, VideoRecord
from service import MemberCard, VideoView, VideoViewStaffItem
from .models import TddVideo, TddMember, TddVideoLog, TddMemberLog, TddVideoStaff
from .values import clean_text, database_value, epoch
from .write import _append_objects, append_video_records
from .observation import execute_write
from util import a2b, same_pic_url

logger = logging.getLogger('db.profiles')

def create_video(video: NewVideo, *, new_members: list[NewMember],
                 staff: list[VideoViewStaffItem] | None,
                 first_record: VideoRecord | None = None) -> TddVideo | None:
    """Commit the complete creation and return its stored video snapshot.

    An existing aid returns None without creating related rows. Members found
    after the task's precheck are retained; their conflicts do not block creation.
    Other database failures propagate, including an uncertain commit outcome.
    """
    return execute_write(_create_video, video, new_members=new_members, staff=staff,
                         first_record=first_record)


def _create_video(conn: Connection, video: NewVideo, *, new_members: list[NewMember],
                  staff: list[VideoViewStaffItem] | None, first_record: VideoRecord | None,
                  run_id: UUID | None = None) -> TddVideo | None:
    row = conn.execute(
        'INSERT INTO tdd_video '
        '(added,aid,bvid,videos,tid,tname,copyright,pic,title,pubdate,"desc",tags,mid,'
        'code,attribute,state,forward,hasstaff,isvc,recent) '
        'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) '
        'ON CONFLICT (aid) DO NOTHING '
        'RETURNING id,added,aid,bvid,videos,tid,tname,copyright,pic,title,pubdate,"desc",tags,mid,'
        'code,attribute,state,forward,hasstaff,solo,original,employed,isvc,engine,freq,activity,recent',
        (database_value('added',video.added),video.aid,clean_text(video.bvid),video.videos,
         video.tid,clean_text(video.tname),video.copyright,clean_text(video.pic),
         clean_text(video.title),video.pubdate,clean_text(video.desc),clean_text(video.tags),
         video.mid,video.code,video.attribute,video.state,video.forward,video.hasstaff,
         video.isvc,video.recent)).fetchone()
    if row is None:
        return None
    stored_video = TddVideo(**dict(zip(TddVideo.fields,map(epoch,row))))
    _insert_prepared_members(new_members,conn)
    _claim_profile_observation('video',video.aid,video.added,conn)
    for item in staff or []:
        if _insert_staff(conn,video.added,video.aid,item.mid,item.title) is None:
            logger.debug('Video staff relationship already exists: aid=%s, mid=%s',video.aid,item.mid)
    if first_record is not None:
        append_video_records(conn,[first_record],run_id=run_id)
    return stored_video

def update_video_profile(aid: int, video_view: VideoView | None, code_error: int | None,
                         tags: str | None, members: list[NewMember], added: int) -> list[TddVideoLog] | None:
    """Return change logs (possibly empty), or None when the video is missing."""
    return execute_write(_update_video_profile, aid, video_view, code_error, tags, members, added)

def _insert_member(conn: Connection, member: NewMember, *, run_id: UUID | None = None) -> TddMember | None:
    row = conn.execute(
        'INSERT INTO tdd_member (added,mid,sex,name,face,sign,code) '
        'VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (mid) DO NOTHING '
        'RETURNING id,added,mid,sex,name,face,sign,code,last_video_aid',
        (database_value('added',member.added),member.mid,clean_text(member.sex),
         clean_text(member.name),clean_text(member.face),clean_text(member.sign),member.code)).fetchone()
    if row is None:
        return None
    stored_member = TddMember(**dict(zip(TddMember.fields,map(epoch,row))))
    _claim_profile_observation('member',member.mid,member.added,conn)
    return stored_member

def update_member_profile(mid: int, card: MemberCard | None, code: int, added: int) -> list[TddMemberLog] | None:
    """Return change logs (possibly empty), or None when the member is missing."""
    return execute_write(_update_member_profile, mid, card, code, added)

def _insert_prepared_members(prepared: list[NewMember], conn: Connection) -> None:
    for member in prepared:
        _insert_member(conn,member)


def _claim_profile_observation(kind: Literal["video", "member"], entity_id: int,
                               observed_at: int, conn: Connection) -> bool:
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


def _update_video_profile(conn: Connection, aid: int, video_view: VideoView | None, code_error: int | None,
                          tags: str | None, members: list[NewMember], added: int, *,
                          run_id: UUID | None = None) -> list[TddVideoLog] | None:
    logs = []
    current = _query_video(conn,aid)
    if current is None:
        return None
    if code_error == -403:
        # This may be a login failure rather than an unavailable video. Keep
        # both status and watermark unchanged so it cannot suppress valid observations.
        return []
    if not _claim_profile_observation('video',aid,added,conn):
        return []
    changes = {}
    def change(field: str, new: str | int | None) -> None:
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
            if field in ('attribute','forward') and not getattr(video_view,'response_has_'+field):
                continue
            new = clean_text(getattr(video_view,field))
            if field == 'pic' and current.pic is not None and new is not None and same_pic_url(new,current.pic):
                continue
            change(field,new)
        change('mid',video_view.owner.mid)
        # A successful full view response without staff means no staff flag.
        change('hasstaff',1 if video_view.staff is not None else 0)
        if video_view.staff is not None:
            existing = {item.mid:item for item in _query_staff(conn,aid)}
            remaining,seen = set(existing),set()
            for item in video_view.staff or []:
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


def _update_member_profile(conn: Connection, mid: int, card: MemberCard | None, code: int,
                           added: int, *, run_id: UUID | None = None) -> list[TddMemberLog] | None:
    logs = []
    current = _query_member(conn,mid)
    if current is None:
        return None
    if not _claim_profile_observation('member',mid,added,conn):
        return []
    changes = {}
    def change(field: str, new: str | int | None) -> None:
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


def _insert_staff(conn: Connection, added: int, aid: int, mid: int, title: str) -> TddVideoStaff | None:
    row = conn.execute(
        'INSERT INTO tdd_video_staff (added,aid,bvid,mid,title) VALUES (%s,%s,%s,%s,%s) '
        'ON CONFLICT (aid,mid) DO NOTHING RETURNING id,added,aid,bvid,mid,title',
        (database_value('added',added),aid,a2b(aid),mid,clean_text(title))).fetchone()
    if row is None:
        return None
    return TddVideoStaff(**dict(zip(TddVideoStaff.fields,map(epoch,row))))


def _query_video(conn: Connection, aid: int) -> TddVideo | None:
    columns = ','.join('"'+field+'"' for field in TddVideo.fields)
    row = conn.execute('SELECT '+columns+' FROM tdd_video WHERE aid = %s FOR UPDATE',(aid,)).fetchone()
    return TddVideo(**dict(zip(TddVideo.fields,map(epoch,row)))) if row is not None else None


def _query_member(conn: Connection, mid: int) -> TddMember | None:
    columns = ','.join('"'+field+'"' for field in TddMember.fields)
    row = conn.execute('SELECT '+columns+' FROM tdd_member WHERE mid = %s FOR UPDATE',(mid,)).fetchone()
    return TddMember(**dict(zip(TddMember.fields,map(epoch,row)))) if row is not None else None


def _query_staff(conn: Connection, aid: int) -> list[TddVideoStaff]:
    columns = ','.join('"'+field+'"' for field in TddVideoStaff.fields)
    rows = conn.execute('SELECT '+columns+' FROM tdd_video_staff WHERE aid = %s',(aid,)).fetchall()
    return [TddVideoStaff(**dict(zip(TddVideoStaff.fields,map(epoch,row)))) for row in rows]
