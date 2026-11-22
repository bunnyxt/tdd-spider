"""Pipelined immutable inserts with explicit returned database values."""
from __future__ import annotations
from collections.abc import Iterable, Sequence
from datetime import datetime
from typing import Any

from psycopg import Connection, sql
from psycopg.pq import TransactionStatus

from . import migration

HistoryKey = tuple[int, datetime, int]
AppendKey = HistoryKey | tuple[int]

_STATS = ('view', 'danmaku', 'reply', 'favorite', 'coin', 'share', 'like')
_BASE = ('added', 'aid') + _STATS
_SPEC = {
    'tdd_video_record': (_BASE, ('aid', 'added', 'id')),
    'tdd_member_follower_record': (('added', 'mid', 'follower'), ('mid', 'added', 'id')),
    'tdd_member_total_stat_record': (('added', 'mid', 'video_count') + _STATS, ('mid', 'added', 'id')),
    'tdd_sprint_video_record': (_BASE, ('id',)),
    'tdd_video_log': (('added', 'aid', 'attr', 'oldval', 'newval'), ('id',)),
    'tdd_member_log': (('added', 'mid', 'attr', 'oldval', 'newval'), ('id',)),
    'tdd_video_record_abnormal_change': (
        ('added', 'aid', 'attr', 'speed_now', 'speed_last', 'speed_now_incr_rate',
         'period_range', 'speed_period', 'speed_overall', 'this_record_id', 'this_added')
        + tuple('this_' + s for s in _STATS) + ('last_record_id', 'last_added')
        + tuple('last_' + s for s in _STATS) + ('description', 'comment'), ('id',)),
}



def append_rows(conn: Connection, table: str, columns: Sequence[str],
                rows: Iterable[Sequence[Any]], *,
                returning: Sequence[str] | None = None) -> list[tuple[Any, ...]]:
    """Append a batch and return stored values in input order.

    The caller owns the transaction. By default return complete primary keys;
    callers needing a stored snapshot may select business fields as well.
    Pipelined prepared INSERTs use default identity values and one RETURNING
    result set per input row. Failed attempts can consume sequence values.
    """
    if table not in _SPEC:
        raise ValueError('Table is not an approved append target')
    columns, rows = tuple(columns), [tuple(row) for row in rows]
    allowed, pk = _SPEC[table]
    if len(set(columns)) != len(columns) or set(columns) != set(allowed):
        raise ValueError('Append columns must match the complete business row without id')
    if any(len(row) != len(columns) for row in rows):
        raise ValueError('Append row width does not match columns')
    returned_columns = pk if returning is None else tuple(returning)
    if (len(set(returned_columns)) != len(returned_columns)
            or not set(pk) <= set(returned_columns) <= set(allowed + pk)):
        raise ValueError('Returned columns must include the complete key and only stored fields')
    if conn.info.transaction_status != TransactionStatus.INTRANS:
        raise RuntimeError('An explicit transaction is required for append writes')
    results = []
    query = sql.SQL('INSERT INTO public.{} ({}) VALUES ({}) RETURNING {}').format(
        sql.Identifier(table), sql.SQL(',').join(map(sql.Identifier, columns)),
        sql.SQL(',').join(sql.Placeholder() for _ in columns),
        sql.SQL(',').join(map(sql.Identifier, returned_columns)))
    # Psycopg pipelines these commands; each result set belongs to one input.
    for start in range(0, len(rows), 1000):
        chunk = rows[start:start + 1000]
        with conn.cursor() as cursor:
            cursor.executemany(query, chunk, returning=True)
            for index in range(len(chunk)):
                row = cursor.fetchone()
                if row is None or len(row) != len(returned_columns):
                    raise RuntimeError('An append did not return its complete key and selected fields')
                results.append(tuple(row))
                if index + 1 < len(chunk) and not cursor.nextset():
                    raise RuntimeError('An append batch has missing result sets')
    migration.record_keys(conn, table,
        ({field: row[returned_columns.index(field)] for field in pk} for row in results))
    return results
