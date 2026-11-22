"""Committed writes for the hourly collector's derived video fields."""
from __future__ import annotations
from collections.abc import Mapping, Sequence

from psycopg import Connection

from .transaction import write
from .values import timestamp


def update_video_recent(now_ts: int) -> None:
    return write(_update_video_recent, now_ts)


def _update_video_recent(conn: Connection, now_ts: int) -> None:
    last_1d_ts = timestamp(now_ts - 24 * 60 * 60)
    last_7d_ts = timestamp(now_ts - 7 * 24 * 60 * 60)
    conn.execute('update tdd_video set recent = 0 where added < %s', (last_7d_ts,))
    conn.execute('update tdd_video set recent = 1 where added >= %s AND added < %s',
                 (last_7d_ts, last_1d_ts))
    conn.execute('update tdd_video set recent = 2 where added >= %s', (last_1d_ts,))


def update_video_activity(desired: Mapping[int, Sequence[int]]) -> int:
    # Even an empty input must enter the guarded transaction before any DB access.
    return write(_update_video_activity, desired)


def _update_video_activity(conn: Connection, desired: Mapping[int, Sequence[int]]) -> int:
    current = dict(conn.execute('select aid, activity from tdd_video where activity != 0').fetchall())
    changed = 0
    for activity, aids in sorted(desired.items()):
        changes = [aid for aid in aids if current.get(aid, 0) != activity]
        for i in range(0, len(changes), 1000):
            conn.execute('update tdd_video set activity = %s where aid = ANY(%s)',
                         (activity, changes[i:i + 1000]))
        changed += len(changes)
    return changed


def update_video_freq() -> None:
    return write(_update_video_freq)


def _update_video_freq(conn: Connection) -> None:
    conn.execute('update tdd_video set freq = 0')
    conn.execute('update tdd_video set freq = 1 where activity = 1')
    conn.execute('update tdd_video set freq = 2 where activity = 2 OR recent = 1')
