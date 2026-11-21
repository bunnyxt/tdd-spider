"""Guarded SQL transactions and trial-only complete-key manifests."""
import os
from datetime import datetime, timezone
from uuid import UUID, uuid4

from psycopg import sql
from psycopg.pq import TransactionStatus
from psycopg.types.json import Jsonb
from conf import get_db_timeouts

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


def _encode(value):
    """Keep trial key timestamps compatible with migration cleanup tools."""
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError('Manifest datetimes must have a timezone')
        return {'__tdd_type__': 'datetime', 'value': value.astimezone(timezone.utc).isoformat()}
    return value


def _require_transaction(conn):
    if conn.info.transaction_status != TransactionStatus.INTRANS:
        raise RuntimeError('An explicit transaction is required for ingest writes')
    if conn.execute('SHOW transaction_isolation').fetchone()[0] != 'read committed':
        raise RuntimeError('Ingest requires READ COMMITTED isolation')


def _check_run_control(conn, run_id):
    run_id = UUID(str(run_id)) if run_id is not None else None
    # Maintenance owns the exclusive session-level version of this lock. This
    # separate SELECT runs after it is released, giving READ COMMITTED a fresh
    # control snapshot before business data is read or changed.
    conn.execute('SELECT pg_advisory_xact_lock_shared(141376009,169)').fetchone()
    control = conn.execute(
        'SELECT mode,active_run_id FROM tdd_ingest.control WHERE singleton').fetchone()
    if control is None:
        if os.environ.get('TDD_ALLOW_UNMANAGED_INGEST') != '1':
            raise RuntimeError('Ingest control is not configured')
    elif not ((control[0] == 'trial' and run_id == control[1] and run_id is not None)
              or (control[0] == 'formal' and run_id is None)):
        raise RuntimeError('Ingest control does not permit this run')


def guard_write_transaction(conn, run_id=None):
    """Gate derived writes and hold off maintenance until the caller commits.

    Call first inside an explicit READ COMMITTED transaction, before reading
    business state used by the update. Recomputable derived updates need no
    manifest, but must obey the same maintenance lock and run control.
    """
    _require_transaction(conn)
    _set_transaction_limits(conn)
    _check_run_control(conn, run_id)


def _set_transaction_limits(conn):
    limits = get_db_timeouts()
    conn.execute("SELECT set_config('lock_timeout',%s,true),"
                 "set_config('statement_timeout',%s,true),"
                 "set_config('transaction_timeout',%s,true)",
                 [str(limits[name]) + 'ms' for name in
                  ('lock_timeout', 'statement_timeout', 'transaction_timeout')]).fetchone()


def append_rows(conn, table, columns, rows, *, run_id=None):
    """Append one immutable batch inside the caller's transaction.

    The caller must acquire guard_write_transaction before business work.
    Return complete PK tuples in input order. Pipelined prepared INSERTs use
    default identity values and one RETURNING result set per input row, so the
    mapping does not depend on multi-row RETURNING order. Failed attempts can
    consume sequence values; gaps are expected.
    """
    if table not in _SPEC:
        raise ValueError('Table is not an approved append target')
    columns, rows = tuple(columns), [tuple(row) for row in rows]
    allowed, pk = _SPEC[table]
    if len(set(columns)) != len(columns) or set(columns) != set(allowed):
        raise ValueError('Append columns must match the complete business row without id')
    if any(len(row) != len(columns) for row in rows):
        raise ValueError('Append row width does not match columns')
    _require_transaction(conn)
    keys = []
    query = sql.SQL('INSERT INTO public.{} ({}) VALUES ({}) RETURNING {}').format(
        sql.Identifier(table), sql.SQL(',').join(map(sql.Identifier, columns)),
        sql.SQL(',').join(sql.Placeholder() for _ in columns),
        sql.SQL(',').join(map(sql.Identifier, pk)))
    # Psycopg 3 executemany pipelines its commands rather than waiting for each
    # network round trip. Each result set still corresponds to one input row.
    for start in range(0, len(rows), 1000):
        chunk = rows[start:start + 1000]
        with conn.cursor() as cursor:
            cursor.executemany(query, chunk, returning=True)
            for index in range(len(chunk)):
                key = cursor.fetchone()
                if key is None:
                    raise RuntimeError('An append did not return its complete key')
                keys.append(tuple(key))
                if index + 1 < len(chunk) and not cursor.nextset():
                    raise RuntimeError('An append batch has missing result sets')
    if run_id is not None and keys:
        batch_id = uuid4()
        with conn.cursor() as cursor:
            cursor.executemany(
                'INSERT INTO tdd_ingest.manifest(batch_id,ordinal,run_id,table_name,key) '
                'VALUES(%s,%s,%s,%s,%s)',
                [(UUID(str(batch_id)), i, UUID(str(run_id)), table,
                  Jsonb({field: _encode(value) for field, value in zip(pk, key)}))
                 for i, key in enumerate(keys)])
    return keys


def run_transaction(connection_factory, callback, *, run_id=None):
    """Run database-only work in one guarded transaction, without automatic retry.

    The caller fetches HTTP input beforehand. An exception, including an
    uncertain commit acknowledgement, propagates for manual reconciliation.
    """
    with connection_factory() as conn:
        if conn.info.transaction_status != TransactionStatus.IDLE:
            raise RuntimeError('run_transaction requires an idle connection')
        with conn.transaction():
            guard_write_transaction(conn, run_id)
            result = callback(conn)
            _require_transaction(conn)
    return result
