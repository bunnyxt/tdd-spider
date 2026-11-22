"""Named persistence operations for legacy sprint collection."""
from __future__ import annotations
from uuid import UUID

from psycopg import Connection
from .models import TddSprintVideoRecord

from .observation import execute_write
from .write import _append_objects
from .values import database_value


def insert_sprint_video_record(record: TddSprintVideoRecord) -> TddSprintVideoRecord:
    return execute_write(_insert_sprint_video_record, record)


def _insert_sprint_video_record(conn: Connection, record: TddSprintVideoRecord, *,
                                run_id: UUID | None = None) -> TddSprintVideoRecord:
    _append_objects(conn, [record], run_id=run_id)
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
