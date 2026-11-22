"""Named persistence operations for legacy sprint collection."""
from __future__ import annotations

from psycopg import Connection
from .ingest import append_rows
from .models import STAT_FIELDS, TddSprintVideoRecord

from .transaction import write
from .values import database_value, epoch


def insert_sprint_video_record(aid: int, added: int, stat: dict[str, int | None]) -> TddSprintVideoRecord:
    return write(_insert_sprint_video_record, aid, added, stat)


def _insert_sprint_video_record(conn: Connection, aid: int, added: int,
                                stat: dict[str, int | None]) -> TddSprintVideoRecord:
    rows = append_rows(
        conn, 'tdd_sprint_video_record',
        ('added', 'aid', 'view', 'danmaku', 'reply', 'favorite', 'coin', 'share', 'like'),
        [(database_value('added', added), aid, *(stat[field] for field in STAT_FIELDS))],
        returning=TddSprintVideoRecord.fields)
    if len(rows) != 1:
        raise RuntimeError('Sprint insert did not return its stored record')
    record = TddSprintVideoRecord(**dict(zip(TddSprintVideoRecord.fields, map(epoch, rows[0]))))
    if record.view is not None and record.view >= 1000000:
        conn.execute('UPDATE tdd_sprint_video SET status = %s WHERE aid = %s',
                     ('finished', record.aid))
    return record


def insert_sprint_daily_record(added: int, date: str, aid: int, view: int | None,
                              viewincr: int, pday: int, lday: int) -> dict[str, int]:
    return write(_insert_sprint_daily_record, added, date, aid, view, viewincr, pday, lday)


def _insert_sprint_daily_record(conn: Connection, added: int, date: str, aid: int, view: int | None,
                               viewincr: int, pday: int, lday: int) -> dict[str, int]:
    row = conn.execute('INSERT INTO tdd_sprint_daily_record '
                          '(added,date,aid,view,viewincr,pday,lday) VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id',
                          (database_value('added', added), database_value('date', date), aid, view, viewincr, pday, lday)).fetchone()
    return {'id': row[0]}


def insert_sprint_daily_summary(added: int, date: str, correct: int, vidnum: int, newvids: str,
                               millvids: str, viewincr: int, viewincrincr: int,
                               comment: str | None) -> dict[str, int]:
    return write(_insert_sprint_daily_summary, added, date, correct, vidnum, newvids,
                         millvids, viewincr, viewincrincr, comment)


def _insert_sprint_daily_summary(conn: Connection, added: int, date: str, correct: int, vidnum: int,
                                newvids: str, millvids: str, viewincr: int, viewincrincr: int,
                                comment: str | None) -> dict[str, int]:
    row = conn.execute('INSERT INTO tdd_sprint_daily '
                          '(added,date,correct,vidnum,newvids,millvids,viewincr,viewincrincr,comment) '
                          'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id',
                          (database_value('added', added), database_value('date', date), correct, vidnum,
                           database_value('newvids', newvids), database_value('millvids', millvids),
                           viewincr, viewincrincr, database_value('comment', comment))).fetchone()
    return {'id': row[0]}
