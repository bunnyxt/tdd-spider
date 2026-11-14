"""Execute profile SQL against an isolated transactional store in unit tests."""
from contextlib import contextmanager
from datetime import datetime
import sqlite3
from unittest import mock
from uuid import uuid4

from db import TddVideo,TddMember,TddVideoStaff,TddVideoLog,TddMemberLog
from db.basic import Result,ResultRow
from db.values import epoch
from task.task import apply_observation


class ProfileSession:
    _depth = 1
    def __init__(self):
        self.db = sqlite3.connect(':memory:')
        self.statements = []
        for model in (TddVideo,TddMember,TddVideoStaff,TddVideoLog,TddMemberLog):
            fields = ','.join('"'+field+'"'+(' INTEGER PRIMARY KEY' if field == 'id' else '') for field in model.fields)
            unique = ', UNIQUE(aid,mid)' if model is TddVideoStaff else ', UNIQUE(mid)' if model is TddMember else ''
            self.db.execute('CREATE TABLE ' + model.table + '('+fields+unique+')')
        self.db.commit()

    def execute(self,statement,params=None):
        self.statements.append((statement,params))
        params = tuple(epoch(v) for v in params) if params is not None else ()
        cursor = self.db.execute(statement.replace(' FOR UPDATE','').replace('%s','?'),params)
        if not cursor.description:
            return Result()
        names = [col[0] for col in cursor.description]
        return Result(ResultRow(zip(names,row)) for row in cursor.fetchall())

    @contextmanager
    def transaction(self):
        with self.db:
            yield self

    def close(self):
        self.db.close()

    def seed(self,obj):
        from db import DBOperation
        DBOperation.add(obj,self)
        self.db.commit()
        return obj

    def append(self,session,objects,batch_id=None):
        for obj in objects:
            fields = [field for field in obj.fields if field != 'id']
            row = self.execute('INSERT INTO '+obj.table+' ('+','.join('"'+f+'"' for f in fields)+') VALUES ('+
                               ','.join(['%s']*len(fields))+') RETURNING id',tuple(getattr(obj,f) for f in fields)).first()
            obj.id = row[0]
        return [(obj.id,) for obj in objects]

    def observe(self,operation,payload,**kwargs):
        with self.transaction(), mock.patch('task.task.append_objects',side_effect=self.append):
            return apply_observation(operation,payload,self,uuid4())
