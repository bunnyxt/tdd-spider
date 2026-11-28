"""Exercise profile SQL with an isolated SQLite connection in unit tests."""
from contextlib import contextmanager
import sqlite3
from unittest import mock

from db import TddVideo,TddMember,TddVideoStaff,TddVideoLog,TddMemberLog
from db.values import database_value, epoch
from db.profiles import _query_video, _query_member
from db.write import VIDEO_RECORD_COLUMNS


class ProfileConnection:
    def __init__(self):
        self.db = sqlite3.connect(':memory:')
        self.statements = []
        self.db.execute('CREATE TABLE tdd_video_record (id INTEGER PRIMARY KEY, '
                        + ','.join('"'+field+'"' for field in VIDEO_RECORD_COLUMNS) + ')')
        for model in (TddVideo,TddMember,TddVideoStaff,TddVideoLog,TddMemberLog):
            defaults = {'solo':-1,'original':-1,'employed':-1,'engine':-1,'freq':0,'activity':0} if model is TddVideo else {}
            fields = ','.join('"'+field+'"'+(' INTEGER PRIMARY KEY' if field == 'id' else ' NOT NULL' if model is TddVideo and field == 'title' else '') + (' DEFAULT '+str(defaults[field]) if field in defaults else '') for field in model.fields)
            unique = ', UNIQUE(aid,mid)' if model is TddVideoStaff else ', UNIQUE(mid)' if model is TddMember else ', UNIQUE(aid)' if model is TddVideo else ''
            self.db.execute('CREATE TABLE ' + model.table + '('+fields+unique+')')
        self.db.commit()

    def execute(self,statement,params=None):
        self.statements.append((statement,params))
        params = tuple(epoch(v) for v in params) if params is not None else ()
        return self.db.execute(statement.replace(' FOR UPDATE','').replace('%s','?'),params)

    @contextmanager
    def transaction(self):
        with self.db:
            yield self

    def close(self):
        self.db.close()

    def seed(self,obj):
        self.append(self,[obj])
        self.db.commit()
        return obj

    def append(self,conn,objects):
        for obj in objects:
            fields = [field for field in obj.fields if field != 'id']
            row = self.execute('INSERT INTO '+obj.table+' ('+','.join('"'+f+'"' for f in fields)+') VALUES ('+
                               ','.join(['%s']*len(fields))+') RETURNING id',tuple(getattr(obj,f) for f in fields)).fetchone()
            obj.id = row[0]
        return [(obj.id,) for obj in objects]

    def write(self,function,*args,**kwargs):
        with self.transaction(), \
                mock.patch('db.profiles.append_video_logs',side_effect=self.append_video_logs), \
                mock.patch('db.profiles.append_member_logs',side_effect=self.append_member_logs), \
                mock.patch('db.profiles.append_video_records',side_effect=self.append_video_records):
            return function(self,*args,**kwargs)

    def _append_logs(self,model,logs):
        columns = model.fields[1:]
        statement = ('INSERT INTO ' + model.table + ' (' + ','.join(columns) + ') VALUES (' +
                     ','.join(['%s']*len(columns)) + ') RETURNING ' + ','.join(model.fields))
        return [model(**dict(zip(model.fields,map(epoch,self.execute(
            statement,tuple(database_value(field,getattr(log,field)) for field in columns)).fetchone()))))
            for log in logs]

    def append_video_logs(self,conn,logs):
        return self._append_logs(TddVideoLog,logs)

    def append_member_logs(self,conn,logs):
        return self._append_logs(TddMemberLog,logs)

    def append_video_records(self,conn,records):
        return [self.execute(
            'INSERT INTO tdd_video_record (' + ','.join('"'+f+'"' for f in VIDEO_RECORD_COLUMNS) +
            ') VALUES (' + ','.join(['%s']*len(VIDEO_RECORD_COLUMNS)) + ') RETURNING aid,added,id',
            tuple(getattr(record,field) for field in VIDEO_RECORD_COLUMNS)).fetchone()
            for record in records]

    def patch_task_reads(self,test):
        for name,query in (('query_video_via_aid',_query_video),('query_member_via_mid',_query_member)):
            patcher = mock.patch('task.task.'+name,side_effect=lambda key,query=query:query(self,key))
            patcher.start()
            test.addCleanup(patcher.stop)
