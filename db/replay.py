"""Durable observation input and receipt-aware replay without another HTTP fetch."""
import argparse
from contextlib import contextmanager
import json
import logging
import os
from pathlib import Path
import tempfile
from uuid import UUID, uuid4, uuid5

from .basic import engine, Session
from .models import Row
from .write import trial_run_id

logger = logging.getLogger('db.replay')


def encode(value):
    if isinstance(value, Row):
        return {'row_type':type(value).__name__,'values':{f:encode(getattr(value,f)) for f in value.fields}}
    if hasattr(value,'_asdict'):
        return {'response_type':type(value).__name__,'values':{k:encode(v) for k,v in value._asdict().items()}}
    if isinstance(value,(set,frozenset)):
        return {'set_values':sorted(value)}
    if isinstance(value,dict):
        return {str(k):encode(v) for k,v in value.items()}
    if isinstance(value,(list,tuple)):
        return [encode(v) for v in value]
    return value


def decode(value):
    if isinstance(value,list):
        return [decode(v) for v in value]
    if not isinstance(value,dict):
        return value
    if 'row_type' in value:
        from . import models
        model = getattr(models,value['row_type'],None)
        if not isinstance(model,type) or not issubclass(model,Row):
            raise ValueError('Unsupported replay row type')
        return model(**{k:decode(v) for k,v in value['values'].items()})
    if 'response_type' in value:
        from service import response
        model = getattr(response,value['response_type'],None)
        if model is None or not hasattr(model,'_fields'):
            raise ValueError('Unsupported replay response type')
        return model(**{k:decode(v) for k,v in value['values'].items()})
    if 'set_values' in value:
        return frozenset(value['set_values'])
    return {k:decode(v) for k,v in value.items()}


def _directory():
    return Path(os.environ.get('TDD_REPLAY_DIR','data/replay'))


def _fsync_directory(directory):
    fd = os.open(directory,os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _save(path, envelope):
    path.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
    content = json.dumps(envelope,ensure_ascii=False,sort_keys=True,separators=(',',':'))
    if path.exists():
        if path.read_text() != content:
            raise ValueError('Replay batch already exists with different input')
        return
    fd, temporary = tempfile.mkstemp(prefix='.pending-',dir=path.parent)
    try:
        with os.fdopen(fd,'w',encoding='utf-8') as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            # A hard link publishes the complete file without overwriting an
            # observation another process already published under this UUID.
            os.link(temporary,path)
        except FileExistsError:
            if path.read_text() != content:
                raise ValueError('Replay batch already exists with different input')
        _fsync_directory(path.parent)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _dispatch(operation, payload, session, batch_id):
    if operation == 'append':
        from .write import append_objects
        objects = payload['objects']
        append_objects(session,objects,uuid5(batch_id,objects[0].table)) if objects else None
        return objects
    from task.task import apply_observation
    return apply_observation(operation,payload,session,batch_id)


def _execute(envelope, connection_factory):
    from .ingest import run_batch
    batch_id = UUID(envelope['batch_id'])
    payload = envelope['payload']
    def callback(connection):
        session = Session(connection)
        session._depth = 1
        session.run_id = UUID(envelope['run_id']) if envelope['run_id'] else None
        result = _dispatch(envelope['operation'],decode(payload),session,batch_id)
        if envelope['operation'] == 'append':
            return {'append_ids':[obj.id for obj in result]}
        return encode(result)
    result = run_batch(connection_factory,callback,batch_id=batch_id,
                       payload={'operation':envelope['operation'],'payload':payload},
                       run_id=UUID(envelope['run_id']) if envelope['run_id'] else None)
    if envelope['operation'] == 'append' and isinstance(result,dict) and 'append_ids' in result:
        objects, identifiers = decode(payload)['objects'],result['append_ids']
        if len(objects) != len(identifiers) or any(type(ident) is not int for ident in identifiers):
            raise ValueError('Append receipt result does not match the pending observation')
        for obj,identifier in zip(objects,identifiers):
            obj.id = identifier
        return objects
    return decode(result)


def _clear_confirmed(path):
    try:
        path.unlink(missing_ok=True)
        _fsync_directory(path.parent)
    except OSError:
        # The receipt already confirms the database transaction. A retained or
        # restored file is safe to replay under the same observation UUID.
        logger.warning('Database commit confirmed; pending-input cleanup failed: %s',path.name,exc_info=True)


def observe(operation, payload, *, batch_id=None, connection_factory=None):
    batch_id = UUID(str(batch_id)) if batch_id is not None else uuid4()
    run_id = trial_run_id()
    envelope = {'version':1,'batch_id':str(batch_id),'run_id':str(run_id) if run_id else None,
                'operation':operation,'payload':encode(payload)}
    path = _directory() / (str(batch_id)+'.json')
    _save(path,envelope)
    result = _execute(envelope,connection_factory or engine.connection)
    _clear_confirmed(path)
    return result


def replay(path, connection_factory=None):
    path = Path(path)
    envelope = json.loads(path.read_text())
    if envelope.get('version') != 1 or path.stem != envelope['batch_id']:
        raise ValueError('Unsupported or mismatched replay input')
    result = _execute(envelope,connection_factory or engine.connection)
    _clear_confirmed(path)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('paths',nargs='+',help='Pending observation JSON files')
    args = parser.parse_args()
    for path in args.paths:
        replay(path)
        print('Confirmed replay:',path)


if __name__ == '__main__':
    main()
