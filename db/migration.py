"""Temporary write controls and cleanup manifests for the migration window.

Remove this module and its two write-path hooks after migration wrap-up.
Trial configuration is fixed for the process lifetime: stop collectors before
changing the active run or switching to formal mode, then restart them.
"""
from __future__ import annotations
from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
import os
from uuid import UUID, uuid4

from psycopg import Connection
from psycopg.pq import TransactionStatus
from psycopg.types.json import Jsonb


def _trial_run_id() -> UUID | None:
    value = os.environ.get('TDD_TRIAL_RUN_ID')
    return UUID(value) if value else None


def before_write(conn: Connection) -> dict[str, str | None]:
    """Gate a transaction before business reads; hold maintenance off to commit.

    The returned label is only for a failed-input diagnostic, never a business
    function argument. Isolation is checked once at this write boundary.
    """
    if conn.info.transaction_status != TransactionStatus.INTRANS:
        raise RuntimeError('An explicit transaction is required for migration writes')
    if conn.execute('SHOW transaction_isolation').fetchone()[0] != 'read committed':
        raise RuntimeError('Migration writes require READ COMMITTED isolation')
    run_id = _trial_run_id()
    # The separate control SELECT after the lock gets a fresh READ COMMITTED
    # snapshot after any preceding maintenance transaction has finished.
    conn.execute('SELECT pg_advisory_xact_lock_shared(141376009,169)').fetchone()
    control = conn.execute(
        'SELECT mode,active_run_id FROM tdd_ingest.control WHERE singleton').fetchone()
    if control is None:
        if os.environ.get('TDD_ALLOW_UNMANAGED_INGEST') != '1':
            raise RuntimeError('Ingest control is not configured')
    elif not ((control[0] == 'trial' and run_id == control[1] and run_id is not None)
              or (control[0] == 'formal' and run_id is None)):
        raise RuntimeError('Ingest control does not permit this run')
    return {'run_id': str(run_id) if run_id else None}


def _encode(value: int | datetime) -> int | dict[str, str]:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError('Manifest datetimes must have a timezone')
        return {'__tdd_type__': 'datetime', 'value': value.astimezone(timezone.utc).isoformat()}
    return value


def record_keys(conn: Connection, table: str,
                keys: Iterable[Mapping[str, int | datetime]]) -> None:
    """Store returned complete keys in the same transaction as their inserts.

    Formal mode returns without consuming keys or allocating a batch UUID.
    Only the append helper calls this hook; it supplies the table's PK fields.
    """
    run_id = _trial_run_id()
    if run_id is None:
        return
    keys = list(keys)
    if not keys:
        return
    batch_id = uuid4()
    with conn.cursor() as cursor:
        cursor.executemany(
            'INSERT INTO tdd_ingest.manifest(batch_id,ordinal,run_id,table_name,key) '
            'VALUES(%s,%s,%s,%s,%s)',
            [(batch_id, i, run_id, table,
              Jsonb({field: _encode(value) for field, value in key.items()}))
             for i, key in enumerate(keys)])
