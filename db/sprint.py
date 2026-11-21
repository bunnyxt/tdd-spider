"""Named persistence operations for legacy sprint collection."""
from .observation import execute_write
from .write import _append_objects
from .values import database_value


def insert_sprint_video_record(record):
    return execute_write(_insert_sprint_video_record, record)


def _insert_sprint_video_record(conn, record, *, run_id=None):
    _append_objects(conn, [record], run_id=run_id)
    if record.view is not None and record.view >= 1000000:
        conn.execute('UPDATE tdd_sprint_video SET status = %s WHERE aid = %s',
                        ('finished', record.aid))
    return record


def insert_sprint_daily_record(added, date, aid, view, viewincr, pday, lday):
    return execute_write(_insert_sprint_daily_record, added, date, aid, view, viewincr, pday, lday)


def _insert_sprint_daily_record(conn, added, date, aid, view, viewincr, pday, lday, *, run_id=None):
    row = conn.execute('INSERT INTO tdd_sprint_daily_record '
                          '(added,date,aid,view,viewincr,pday,lday) VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id',
                          (database_value('added', added), database_value('date', date), aid, view, viewincr, pday, lday)).fetchone()
    return {'id': row[0]}


def insert_sprint_daily_summary(added, date, correct, vidnum, newvids, millvids, viewincr, viewincrincr, comment):
    return execute_write(_insert_sprint_daily_summary, added, date, correct, vidnum, newvids,
                         millvids, viewincr, viewincrincr, comment)


def _insert_sprint_daily_summary(conn, added, date, correct, vidnum, newvids, millvids,
                                viewincr, viewincrincr, comment, *, run_id=None):
    row = conn.execute('INSERT INTO tdd_sprint_daily '
                          '(added,date,correct,vidnum,newvids,millvids,viewincr,viewincrincr,comment) '
                          'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id',
                          (database_value('added', added), database_value('date', date), correct, vidnum,
                           database_value('newvids', newvids), database_value('millvids', millvids),
                           viewincr, viewincrincr, database_value('comment', comment))).fetchone()
    return {'id': row[0]}
