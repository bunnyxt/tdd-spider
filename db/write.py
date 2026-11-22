"""Append records inside their owner's transaction and trial complete-key manifest."""
from __future__ import annotations
from collections.abc import Mapping, Sequence
from typing import cast

from psycopg import Connection
from core import VideoRecord, MemberFollowerRecord
from .ingest import AppendKey, HistoryKey
from .models import Row, TddVideoRecordAbnormalChange

import os
from uuid import UUID
from .values import database_value

VIDEO_RECORD_COLUMNS = ('added', 'aid', 'view', 'danmaku', 'reply', 'favorite', 'coin', 'share', 'like')


def trial_run_id() -> UUID | None:
    value = os.environ.get('TDD_TRIAL_RUN_ID')
    return UUID(value) if value else None


def append_video_records(conn: Connection, records: Sequence[VideoRecord], *,
                         run_id: UUID | None = None) -> list[HistoryKey]:
    """Bind collected records directly and return (aid, added, id) keys in order."""
    if not records:
        return []
    from .ingest import append_rows
    rows = [tuple(database_value(field, getattr(record, field)) for field in VIDEO_RECORD_COLUMNS)
            for record in records]
    return cast(list[HistoryKey], append_rows(conn, 'tdd_video_record', VIDEO_RECORD_COLUMNS, rows, run_id=run_id))


def _append_objects(conn: Connection, objects: Sequence[Row], *,
                    run_id: UUID | None = None) -> list[AppendKey]:
    """Internal homogeneous append used by profile logs and legacy record writes."""
    if not objects:
        return []
    from .ingest import append_rows
    model = type(objects[0])
    if any(type(obj) is not model for obj in objects):
        raise ValueError('An append batch must contain one row type')
    columns = tuple(field for field in model.fields if field != 'id')
    rows = [tuple(database_value(field, getattr(obj, field)) for field in columns) for obj in objects]
    keys = append_rows(conn, model.table, columns, rows, run_id=run_id)
    for obj, key in zip(objects, keys):
        obj.id = key[-1]
    return keys


def append_member_follower_records(conn: Connection, records: Sequence[MemberFollowerRecord], *,
                                  run_id: UUID | None = None) -> list[HistoryKey]:
    """Bind collected followers directly; retain independent same-second keys."""
    if not records:
        return []
    from .ingest import append_rows
    columns = ('added', 'mid', 'follower')
    rows = [tuple(database_value(field, getattr(record, field)) for field in columns)
            for record in records]
    return cast(list[HistoryKey], append_rows(conn, 'tdd_member_follower_record', columns, rows, run_id=run_id))


def insert_video_records(records: Sequence[VideoRecord]) -> list[HistoryKey]:
    from .observation import execute_write
    return execute_write(append_video_records, records)


def insert_member_follower_records(records: Sequence[MemberFollowerRecord]) -> list[HistoryKey]:
    from .observation import execute_write
    return execute_write(append_member_follower_records, records)


def insert_member_total_stat_records(records: Sequence[Mapping[str, int]]) -> list[HistoryKey]:
    from .observation import execute_write
    return execute_write(_insert_member_total_stat_records, records)


def _insert_member_total_stat_records(conn: Connection, records: Sequence[Mapping[str, int]], *,
                                      run_id: UUID | None = None) -> list[HistoryKey]:
    from .ingest import append_rows
    columns = ('added', 'mid', 'video_count') + VIDEO_RECORD_COLUMNS[2:]
    rows = [tuple(database_value(field, record[field]) for field in columns) for record in records]
    return cast(list[HistoryKey], append_rows(conn, 'tdd_member_total_stat_record', columns, rows, run_id=run_id))


def insert_video_record_abnormal_change(change: TddVideoRecordAbnormalChange) -> TddVideoRecordAbnormalChange:
    from .observation import execute_write
    return execute_write(_insert_video_record_abnormal_change, change)


def _insert_video_record_abnormal_change(conn: Connection, change: TddVideoRecordAbnormalChange, *,
                                       run_id: UUID | None = None) -> TddVideoRecordAbnormalChange:
    _append_objects(conn, [change], run_id=run_id)
    return change
