"""Persist one observation; save input only after a failed database attempt.

Failure files are diagnostics for manual reconciliation. They are not a durable
queue or proof that a transaction rolled back: a lost COMMIT acknowledgement may
leave the data committed. No input is automatically retried or replayed.
"""
import json
import logging
import os
from pathlib import Path
import tempfile

from .basic import engine, Session
from .models import Row
from .write import trial_run_id

logger = logging.getLogger('db.observation')


def encode(value):
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


def _save_failure(operation, payload, run_id, error):
    directory = Path(os.environ.get('TDD_FAILURE_DIR', 'data/failed-writes'))
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    directory.chmod(0o700)
    # Only the failure path creates a file. mkstemp creates it with mode 0600;
    # no fsync/durable publishing or automatic replay is promised here.
    fd, filename = tempfile.mkstemp(prefix='failed-', suffix='.json', dir=directory)
    path = Path(filename)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as stream:
            json.dump({'version': 1, 'operation': operation,
                       'run_id': str(run_id) if run_id else None,
                       'commit_status': 'unconfirmed', 'error_type': type(error).__name__,
                       'payload': encode(payload)}, stream, ensure_ascii=False,
                      allow_nan=False, separators=(',', ':'))
            stream.write('\n')
    except Exception:
        path.unlink(missing_ok=True)
        raise
    return path


def _dispatch(operation, payload, session):
    if operation == 'video-records':
        from .write import append_video_records
        return append_video_records(session, payload['records'])
    if operation == 'append':
        from .write import append_objects
        objects = payload['objects']
        append_objects(session, objects)
        return objects
    from task.task import apply_observation
    return apply_observation(operation, payload, session)


def observe(operation, payload, *, connection_factory=None):
    """Attempt one business transaction and propagate any unconfirmed failure."""
    from .ingest import run_transaction
    run_id = trial_run_id()

    def callback(connection):
        session = Session(connection)
        session._depth = 1
        session.run_id = run_id
        return _dispatch(operation, payload, session)

    try:
        return run_transaction(connection_factory or engine.connection, callback, run_id=run_id)
    except Exception as error:
        try:
            path = _save_failure(operation, payload, run_id, error)
        except Exception:
            logger.error('Database write failed; failure input could not be saved. '
                         'operation=%s. Commit status is unconfirmed; reconcile manually '
                         'before retrying.', operation, exc_info=True)
        else:
            logger.error('Database write failed; input saved to %s. operation=%s. '
                         'Commit status is unconfirmed; reconcile manually before retrying.',
                         path, operation, exc_info=True)
        raise
