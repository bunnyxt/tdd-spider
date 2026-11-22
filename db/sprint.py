"""Named persistence operations for legacy sprint collection."""
from __future__ import annotations
from uuid import UUID

from psycopg import Connection
from service.response import VideoViewStat
from .ingest import record_trial_keys
from .models import TddSprintVideoRecord

from .observation import execute_write
from .values import database_value, epoch


def insert_sprint_video_record(aid: int, added: int, stat: VideoViewStat) -> TddSprintVideoRecord:
    return execute_write(_insert_sprint_video_record, aid, added, stat)


def _insert_sprint_video_record(conn: Connection, aid: int, added: int, stat: VideoViewStat, *,
                                run_id: UUID | None = None) -> TddSprintVideoRecord:
    row = conn.execute(
        'INSERT INTO tdd_sprint_video_record '
        '(added,aid,view,danmaku,reply,favorite,coin,share,"like") '
        'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) '
        'RETURNING id,added,aid,view,danmaku,reply,favorite,coin,share,"like"',
        (database_value('added', added), aid, -1 if stat.view == '--' else stat.view,
         stat.danmaku, stat.reply, stat.favorite, stat.coin, stat.share, stat.like)).fetchone()
    if row is None:
        raise RuntimeError('Sprint insert did not return its stored record')
    record = TddSprintVideoRecord(id=row[0], added=epoch(row[1]), aid=row[2],
                                  view=row[3], danmaku=row[4], reply=row[5], favorite=row[6],
                                  coin=row[7], share=row[8], like=row[9])
    record_trial_keys(conn, 'tdd_sprint_video_record', [(record.id,)], run_id=run_id)
    if record.view is not None and record.view >= 1000000:
        conn.execute('UPDATE tdd_sprint_video SET status = %s WHERE aid = %s',
                     ('finished', record.aid))
    return record


def insert_sprint_daily_record(added: int, date: str, aid: int, view: int | None,
                              viewincr: int, pday: int, lday: int) -> dict[str, int]:
    return execute_write(_insert_sprint_daily_record, added, date, aid, view, viewincr, pday, lday)


def _insert_sprint_daily_record(conn: Connection, added: int, date: str, aid: int, view: int | None,
                               viewincr: int, pday: int, lday: int, *,
                               run_id: UUID | None = None) -> dict[str, int]:
    row = conn.execute('INSERT INTO tdd_sprint_daily_record '
                          '(added,date,aid,view,viewincr,pday,lday) VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id',
                          (database_value('added', added), database_value('date', date), aid, view, viewincr, pday, lday)).fetchone()
    return {'id': row[0]}


def insert_sprint_daily_summary(added: int, date: str, correct: int, vidnum: int, newvids: str,
                               millvids: str, viewincr: int, viewincrincr: int,
                               comment: str | None) -> dict[str, int]:
    return execute_write(_insert_sprint_daily_summary, added, date, correct, vidnum, newvids,
                         millvids, viewincr, viewincrincr, comment)


def _insert_sprint_daily_summary(conn: Connection, added: int, date: str, correct: int, vidnum: int,
                                newvids: str, millvids: str, viewincr: int, viewincrincr: int,
                                comment: str | None, *, run_id: UUID | None = None) -> dict[str, int]:
    row = conn.execute('INSERT INTO tdd_sprint_daily '
                          '(added,date,correct,vidnum,newvids,millvids,viewincr,viewincrincr,comment) '
                          'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id',
                          (database_value('added', added), database_value('date', date), correct, vidnum,
                           database_value('newvids', newvids), database_value('millvids', millvids),
                           viewincr, viewincrincr, database_value('comment', comment))).fetchone()
    return {'id': row[0]}
