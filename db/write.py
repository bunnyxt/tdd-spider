"""Append records with the shared transactional receipt and complete-key manifest."""
import os
from uuid import UUID, uuid4
from .values import database_value


def trial_run_id():
    value = os.environ.get('TDD_TRIAL_RUN_ID')
    return UUID(value) if value else None


def append_objects(session, objects, batch_id=None):
    if not objects:
        return []
    from .ingest import append_rows
    model = type(objects[0])
    if any(type(obj) is not model for obj in objects):
        raise ValueError('An append batch must contain one row type')
    columns = tuple(field for field in model.fields if field != 'id')
    rows = [tuple(database_value(field, getattr(obj, field)) for field in columns) for obj in objects]
    keys = append_rows(session.connection, model.table, columns, rows,
                       batch_id=batch_id or uuid4(), run_id=session.run_id if hasattr(session,'run_id') else trial_run_id())
    for obj, key in zip(objects, keys):
        obj.id = key[-1]
    return keys
