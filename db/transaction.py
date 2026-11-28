"""Execute one write transaction; save input only after a failed attempt.

Failure files are diagnostics for manual reconciliation. They are not a durable
queue or proof that a transaction rolled back: a lost COMMIT acknowledgement may
leave the data committed. No input is automatically retried or replayed.
"""
from __future__ import annotations
from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from typing import Any, TypeVar

from psycopg import Connection
from psycopg.pq import TransactionStatus

_Result = TypeVar("_Result")

import inspect
import json
import logging
import os
from pathlib import Path
import tempfile

from .basic import engine
from .models import Row
from . import migration

logger = logging.getLogger('db.transaction')


def encode(value: Any) -> Any:
    """Make a failure input readable without serializing successful batches."""
    if isinstance(value, Row):
        return {'row_type': type(value).__name__,
                'values': {field: encode(getattr(value, field)) for field in value.fields}}
    if hasattr(value, '_asdict'):
        return {'response_type': type(value).__name__,
                'values': {key: encode(item) for key, item in value._asdict().items()}}
    if isinstance(value, (set, frozenset)):
        return {'set_values': sorted(value)}
    if isinstance(value, dict):
        return {str(key): encode(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [encode(item) for item in value]
    return value


def _save_failure(operation: str, payload: Mapping[str, Any], error: Exception,
                  metadata: Mapping[str, Any]) -> Path:
    directory = Path(os.environ.get('TDD_FAILURE_DIR', 'data/failed-writes'))
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    directory.chmod(0o700)
    # Only the failure path creates a file. mkstemp creates it with mode 0600;
    # no fsync/durable publishing or automatic replay is promised here.
    fd, filename = tempfile.mkstemp(prefix='failed-', suffix='.json', dir=directory)
    path = Path(filename)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as stream:
            json.dump({**metadata, 'version': 1, 'operation': operation,
                       'commit_status': 'unconfirmed', 'error_type': type(error).__name__,
                       'payload': encode(payload)}, stream, ensure_ascii=False,
                      allow_nan=False, separators=(',', ':'))
            stream.write('\n')
    except Exception:
        path.unlink(missing_ok=True)
        raise
    return path


def write(function: Callable[..., _Result], *args: Any,
          connection_factory: Callable[[], AbstractContextManager[Connection]] | None = None,
          **kwargs: Any) -> _Result:
    """Run named database work once; return its result only after commit.

    HTTP input is fetched beforehand. Any exception, including an uncertain
    COMMIT acknowledgement, is recorded best-effort and re-raised without retry.
    """
    if not callable(function):
        raise TypeError('Database work must be a callable')
    metadata: Mapping[str, Any] = {}
    try:
        with (connection_factory or engine.connection)() as connection:
            if connection.info.transaction_status != TransactionStatus.IDLE:
                raise RuntimeError('write requires an idle connection')
            with connection.transaction():
                metadata = migration.before_write(connection)
                result = function(connection, *args, **kwargs)
        return result
    except Exception as error:
        operation = getattr(function, '__name__', type(function).__name__)
        try:
            signature = inspect.signature(function)
            arguments = signature.bind(None, *args, **kwargs).arguments
            arguments.pop(next(iter(signature.parameters)), None)
            path = _save_failure(operation, arguments, error, metadata)
        except Exception:
            logger.error('Database write failed; failure input could not be saved. '
                         'operation=%s. Commit status is unconfirmed; reconcile manually '
                         'before retrying.', operation, exc_info=True)
        else:
            logger.error('Database write failed; input saved to %s. operation=%s. '
                         'Commit status is unconfirmed; reconcile manually before retrying.',
                         path, operation, exc_info=True)
        raise
