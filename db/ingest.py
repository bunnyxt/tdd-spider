"""Transactional append receipts and trial complete-key manifests.

An observation receives a UUID *before* attempting a transaction. Reuse that
UUID, payload and run ID after an uncertain commit; independent observations
must receive new UUIDs, including observations collected in the same second.
Input persistence and operator recovery live in :mod:`db.replay`.
"""
import hashlib
import json
import math
import os
import time
from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID

import psycopg
from psycopg import sql
from psycopg.pq import TransactionStatus
from psycopg.types.json import Jsonb

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
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError('Receipt datetimes must have a timezone')
        return {'__tdd_type__': 'datetime', 'value': value.astimezone(timezone.utc).isoformat()}
    if isinstance(value, (UUID, Decimal)):
        return {'__tdd_type__': type(value).__name__, 'value': str(value)}
    if isinstance(value, dict):
        if '__tdd_type__' in value or any(not isinstance(k, str) for k in value):
            raise ValueError('Receipt maps require string keys without __tdd_type__')
        return {k: _encode(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)):
        return [_encode(v) for v in value]
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float) and math.isfinite(value):
        return value
    raise TypeError('Unsupported receipt value type')


def _decode(value):
    if isinstance(value, dict):
        if '__tdd_type__' in value:
            constructor = {'datetime': datetime.fromisoformat, 'UUID': UUID, 'Decimal': Decimal}
            return constructor[value['__tdd_type__']](value['value'])
        return {k: _decode(v) for k, v in value.items()}
    return [_decode(v) for v in value] if isinstance(value, list) else value


def _digest(payload):
    encoded = _encode(payload)
    raw = json.dumps(encoded, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)
    return hashlib.sha256(raw.encode()).hexdigest()


def _require_transaction(conn):
    if conn.info.transaction_status != TransactionStatus.INTRANS:
        raise RuntimeError('An explicit transaction is required for append receipts')
    if conn.execute('SHOW transaction_isolation').fetchone()[0] != 'read committed':
        raise RuntimeError('Ingest requires READ COMMITTED isolation')


def _claim(conn, batch_id, kind, payload, run_id):
    batch_id = UUID(str(batch_id))
    run_id = UUID(str(run_id)) if run_id is not None else None
    digest = _digest(payload)
    # A conflicting uncommitted insert blocks here. The following statement in
    # READ COMMITTED sees its committed result; a rolled-back insert is ours.
    inserted = conn.execute(
        'INSERT INTO tdd_ingest.batch(batch_id,kind,payload_sha256,run_id) '
        'VALUES(%s,%s,%s,%s) ON CONFLICT(batch_id) DO NOTHING RETURNING batch_id',
        (batch_id, kind, digest, run_id)).fetchone()
    if inserted:
        return True, None
    row = conn.execute(
        'SELECT kind,payload_sha256,run_id,result FROM tdd_ingest.batch '
        'WHERE batch_id=%s FOR UPDATE', (batch_id,)).fetchone()
    if row is None or row[:3] != (kind, digest, run_id):
        raise ValueError('Batch UUID was reused with different input or run ID')
    if row[3] is None:
        raise RuntimeError('An incomplete receipt cannot be replayed')
    return False, _decode(row[3])


def _finish(conn, batch_id, result):
    # Wrap null results so SQL NULL remains an unmistakable unfinished receipt.
    conn.execute('UPDATE tdd_ingest.batch SET result=%s,committed_at=clock_timestamp() '
                 'WHERE batch_id=%s', (Jsonb({'value': _encode(result)}), UUID(str(batch_id))))


def _set_transaction_limits(conn):
    limits = []
    for name, default in (('lock_timeout', 10000), ('statement_timeout', 60000),
                          ('transaction_timeout', 120000)):
        value = int(os.environ.get('TDD_DB_' + name.upper() + '_MS', default))
        if not 1 <= value <= 300000:
            raise ValueError('Database transaction timeouts must be 1..300000 milliseconds')
        limits.append(str(value) + 'ms')
    conn.execute("SELECT set_config('lock_timeout',%s,true),"
                 "set_config('statement_timeout',%s,true),"
                 "set_config('transaction_timeout',%s,true)", limits).fetchone()


def append_rows(conn, table, columns, rows, *, batch_id, run_id=None):
    """Append one immutable batch inside the caller's transaction.

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
    fresh, result = _claim(conn, batch_id, 'append:' + table,
                           {'columns': columns, 'rows': rows}, run_id)
    if not fresh:
        return [tuple(key) for key in result['value']]
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
        with conn.cursor() as cursor:
            cursor.executemany(
                'INSERT INTO tdd_ingest.manifest(batch_id,ordinal,run_id,table_name,key) '
                'VALUES(%s,%s,%s,%s,%s)',
                [(UUID(str(batch_id)), i, UUID(str(run_id)), table,
                  Jsonb({field: _encode(value) for field, value in zip(pk, key)}))
                 for i, key in enumerate(keys)])
    _finish(conn, batch_id, keys)
    return keys


def run_batch(connection_factory, callback, *, batch_id, payload, run_id=None, max_attempts=3):
    """Commit a whole observation once, checking its receipt after reconnecting.

    callback(conn) must do database work only, must not commit/rollback, and
    returns a receipt-serializable result. Fetch HTTP input before this call.
    Persist UUID and payload before calling; on failure retain them for replay.
    """
    if max_attempts < 1:
        raise ValueError('max_attempts must be positive')
    # Validate before acquiring a connection, without exposing payload in errors.
    _digest(payload)
    for attempt in range(max_attempts):
        try:
            with connection_factory() as conn:
                if conn.info.transaction_status != TransactionStatus.IDLE:
                    raise RuntimeError('run_batch requires an idle connection')
                with conn.transaction():
                    _require_transaction(conn)
                    _set_transaction_limits(conn)
                    fresh, previous = _claim(conn, batch_id, 'observation', payload, run_id)
                    if fresh:
                        result = callback(conn)
                        _require_transaction(conn)
                        _finish(conn, batch_id, result)
                    else:
                        result = previous['value']
            return result
        except (psycopg.OperationalError, psycopg.InterfaceError,
                psycopg.errors.TransactionTimeout) as error:
            # Includes serialization/deadlock and lost commit acknowledgement.
            # Reconnect with the SAME UUID; never split or blindly reinsert.
            if attempt + 1 == max_attempts:
                raise
            if (error.sqlstate and error.sqlstate != '25P04'
                    and error.sqlstate[:2] not in ('08', '40', '53', '55', '57', '58')):
                raise
            time.sleep(min(0.1 * 2 ** attempt, 1.0))
