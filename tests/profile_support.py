"""Execute profile SQL against an isolated transactional store in unit tests."""
from contextlib import contextmanager
from datetime import datetime
import sqlite3
from unittest import mock

from db import TddVideo,TddMember,TddVideoStaff,TddVideoLog,TddMemberLog
from db.basic import Result,ResultRow
from db.values import epoch


class ProfileSession:
    _depth = 1
    def __init__(self):
        self.db = sqlite3.connect(':memory:')
        self.statements = []
        self.db.execute("ATTACH DATABASE ':memory:' AS tdd_ingest")
        self.db.execute('CREATE TABLE tdd_ingest.profile_observation '
                        '(entity_kind TEXT NOT NULL,entity_id INTEGER NOT NULL,observed_at INTEGER NOT NULL,'
                        'PRIMARY KEY(entity_kind,entity_id))')
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

    def append(self,session,objects):
        for obj in objects:
            fields = [field for field in obj.fields if field != 'id']
            row = self.execute('INSERT INTO '+obj.table+' ('+','.join('"'+f+'"' for f in fields)+') VALUES ('+
                               ','.join(['%s']*len(fields))+') RETURNING id',tuple(getattr(obj,f) for f in fields)).first()
            obj.id = row[0]
        return [(obj.id,) for obj in objects]

    def execute_write(self,function,*args,**kwargs):
        with self.transaction(), mock.patch('db.profiles.append_objects',side_effect=self.append):
            return function(self,*args,**kwargs)
