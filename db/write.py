"""Append records with the shared business transaction and trial complete-key manifest."""
import os
from uuid import UUID
from .values import database_value

VIDEO_RECORD_COLUMNS = ('added', 'aid', 'view', 'danmaku', 'reply', 'favorite', 'coin', 'share', 'like')


def trial_run_id():
    value = os.environ.get('TDD_TRIAL_RUN_ID')
    return UUID(value) if value else None


def append_video_records(session, records):
    """Bind collected records directly and return (aid, added, id) keys in order."""
    if not records:
        return []
    from .ingest import append_rows
    rows = [tuple(database_value(field, getattr(record, field)) for field in VIDEO_RECORD_COLUMNS)
            for record in records]
    return append_rows(session.connection, 'tdd_video_record', VIDEO_RECORD_COLUMNS, rows,
                       run_id=session.run_id if hasattr(session, 'run_id') else trial_run_id())


def append_objects(session, objects):
    if not objects:
        return []
    from .ingest import append_rows
    model = type(objects[0])
    if any(type(obj) is not model for obj in objects):
        raise ValueError('An append batch must contain one row type')
    columns = tuple(field for field in model.fields if field != 'id')
    rows = [tuple(database_value(field, getattr(obj, field)) for field in columns) for obj in objects]
    keys = append_rows(session.connection, model.table, columns, rows,
                       run_id=session.run_id if hasattr(session,'run_id') else trial_run_id())
    for obj, key in zip(objects, keys):
        obj.id = key[-1]
    return keys


def append_member_follower_records(session, records):
    """Bind collected followers directly; retain independent same-second keys."""
    if not records:
        return []
    from .ingest import append_rows
    columns = ('added', 'mid', 'follower')
    rows = [tuple(database_value(field, getattr(record, field)) for field in columns)
            for record in records]
    return append_rows(session.connection, 'tdd_member_follower_record', columns, rows,
                       run_id=session.run_id if hasattr(session, 'run_id') else trial_run_id())


def insert_video_records(records):
    from .observation import execute_write
    return execute_write(append_video_records, records)


def insert_member_follower_records(records):
    from .observation import execute_write
    return execute_write(append_member_follower_records, records)


def insert_objects(objects):
    from .observation import execute_write
    return execute_write(_insert_objects, objects)


def _insert_objects(session, objects):
    append_objects(session, objects)
    return objects
