"""Append records inside their owner's transaction."""
from __future__ import annotations
from collections.abc import Mapping, Sequence
from typing import cast

from psycopg import Connection
from core import VideoRecord, MemberFollowerRecord, NewVideoLog, NewMemberLog
from .ingest import HistoryKey
from .models import TddVideoLog, TddMemberLog
from .transaction import write

from .values import database_value, epoch

VIDEO_RECORD_COLUMNS = ('added', 'aid', 'view', 'danmaku', 'reply', 'favorite', 'coin', 'share', 'like')


def append_video_records(conn: Connection, records: Sequence[VideoRecord]) -> list[HistoryKey]:
    """Bind collected records directly and return (aid, added, id) keys in order."""
    if not records:
        return []
    from .ingest import append_rows
    rows = [tuple(database_value(field, getattr(record, field)) for field in VIDEO_RECORD_COLUMNS)
            for record in records]
    return cast(list[HistoryKey], append_rows(conn, 'tdd_video_record', VIDEO_RECORD_COLUMNS, rows))


def append_video_logs(conn: Connection, logs: Sequence[NewVideoLog]) -> list[TddVideoLog]:
    """Append profile differences and return stored snapshots in the same transaction."""
    if not logs:
        return []
    from .ingest import append_rows
    columns = ('added', 'aid', 'attr', 'oldval', 'newval')
    rows = [tuple(database_value(field, getattr(log, field)) for field in columns) for log in logs]
    stored = append_rows(conn, 'tdd_video_log', columns, rows, returning=TddVideoLog.fields)
    return [TddVideoLog(**dict(zip(TddVideoLog.fields, map(epoch, row)))) for row in stored]


def append_member_logs(conn: Connection, logs: Sequence[NewMemberLog]) -> list[TddMemberLog]:
    """Append profile differences and return stored snapshots in the same transaction."""
    if not logs:
        return []
    from .ingest import append_rows
    columns = ('added', 'mid', 'attr', 'oldval', 'newval')
    rows = [tuple(database_value(field, getattr(log, field)) for field in columns) for log in logs]
    stored = append_rows(conn, 'tdd_member_log', columns, rows, returning=TddMemberLog.fields)
    return [TddMemberLog(**dict(zip(TddMemberLog.fields, map(epoch, row)))) for row in stored]


def append_member_follower_records(conn: Connection, records: Sequence[MemberFollowerRecord]) -> list[HistoryKey]:
    """Bind collected followers directly; retain independent same-second keys."""
    if not records:
        return []
    from .ingest import append_rows
    columns = ('added', 'mid', 'follower')
    rows = [tuple(database_value(field, getattr(record, field)) for field in columns)
            for record in records]
    return cast(list[HistoryKey], append_rows(conn, 'tdd_member_follower_record', columns, rows))


def insert_video_records(records: Sequence[VideoRecord]) -> list[HistoryKey]:
    return write(append_video_records, records)


def insert_member_follower_records(records: Sequence[MemberFollowerRecord]) -> list[HistoryKey]:
    return write(append_member_follower_records, records)


def insert_member_total_stat_records(records: Sequence[Mapping[str, int]]) -> list[HistoryKey]:
    return write(_insert_member_total_stat_records, records)


def _insert_member_total_stat_records(conn: Connection, records: Sequence[Mapping[str, int]]) -> list[HistoryKey]:
    from .ingest import append_rows
    columns = ('added', 'mid', 'video_count') + VIDEO_RECORD_COLUMNS[2:]
    rows = [tuple(database_value(field, record[field]) for field in columns) for record in records]
    return cast(list[HistoryKey], append_rows(conn, 'tdd_member_total_stat_record', columns, rows))


def insert_video_record_abnormal_change(*, added: int, aid: int, attr: str,
                                       speed_now: float, speed_last: float, speed_now_incr_rate: float,
                                       period_range: int, speed_period: float, speed_overall: float,
                                       this_record: VideoRecord, last_record: VideoRecord,
                                       description: str, comment: str | None = None) -> None:
    write(_insert_video_record_abnormal_change, added=added, aid=aid, attr=attr,
                  speed_now=speed_now, speed_last=speed_last, speed_now_incr_rate=speed_now_incr_rate,
                  period_range=period_range, speed_period=speed_period, speed_overall=speed_overall,
                  this_record=this_record, last_record=last_record, description=description, comment=comment)


def _insert_video_record_abnormal_change(conn: Connection, *, added: int, aid: int, attr: str,
                                        speed_now: float, speed_last: float, speed_now_incr_rate: float,
                                        period_range: int, speed_period: float, speed_overall: float,
                                        this_record: VideoRecord, last_record: VideoRecord,
                                        description: str, comment: str | None = None) -> None:
    from .ingest import append_rows
    columns = ('added', 'aid', 'attr', 'speed_now', 'speed_last', 'speed_now_incr_rate',
               'period_range', 'speed_period', 'speed_overall', 'this_record_id', 'this_added',
               'this_view', 'this_danmaku', 'this_reply', 'this_favorite', 'this_coin', 'this_share', 'this_like',
               'last_record_id', 'last_added', 'last_view', 'last_danmaku', 'last_reply', 'last_favorite',
               'last_coin', 'last_share', 'last_like', 'description', 'comment')
    values = (added, aid, attr, speed_now, speed_last, speed_now_incr_rate,
              period_range, speed_period, speed_overall, None, this_record.added,
              this_record.view, this_record.danmaku, this_record.reply, this_record.favorite,
              this_record.coin, this_record.share, this_record.like, None, last_record.added,
              last_record.view, last_record.danmaku, last_record.reply, last_record.favorite,
              last_record.coin, last_record.share, last_record.like, description, comment)
    row = tuple(database_value(field, value) for field, value in zip(columns, values))
    append_rows(conn, 'tdd_video_record_abnormal_change', columns, [row])
