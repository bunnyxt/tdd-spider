"""Lazy, bounded PostgreSQL connections and explicit business transactions."""
from contextlib import contextmanager
from threading import Lock
from conf import get_db_args
from .values import epoch

__all__ = ['engine', 'Session', 'update_engine']


class ResultRow(dict):
    def __getitem__(self, key):
        return tuple(self.values())[key] if isinstance(key, int) else super().__getitem__(key)


class Result(list):
    def fetchall(self):
        return self
    def fetchone(self):
        return self[0] if self else None
    first = fetchone


class Engine:
    def __init__(self):
        self._pool = None
        self._lock = Lock()

    @property
    def pool(self):
        if self._pool is None:
            with self._lock:
                if self._pool is None:
                    from psycopg_pool import ConnectionPool
                    args = get_db_args()
                    args['dbname'] = args.pop('database', args.get('dbname'))
                    args.pop('max_connections', None)
                    self._pool = ConnectionPool('', kwargs={**args, 'autocommit': True},
                                                min_size=0, max_size=4, timeout=60)
        return self._pool

    def connection(self):
        return self.pool.connection()

    def raw_connection(self):
        return Session()

    def dispose(self):
        if self._pool is not None:
            self._pool.close()
            self._pool = None


engine = Engine()


def update_engine():
    engine.dispose()


class Session:
    def __init__(self, connection=None):
        self._connection = connection
        self._external = connection is not None
        self._depth = 0

    @property
    def connection(self):
        if self._connection is None:
            self._connection = engine.pool.getconn()
        return self._connection

    def _release(self):
        if self._connection is not None and not self._external:
            engine.pool.putconn(self._connection)
            self._connection = None

    def execute(self, statement, params=None):
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(statement, params)
                if cursor.description is None:
                    return Result()
                names = [col.name for col in cursor.description]
                return Result(ResultRow(zip(names, (epoch(v) for v in row))) for row in cursor.fetchall())
        finally:
            if not self._depth:
                self._release()

    def cursor(self):
        return self.connection.cursor()

    @contextmanager
    def transaction(self):
        self._depth += 1
        try:
            with self.connection.transaction():
                yield self
        finally:
            self._depth -= 1
            if self._depth == 0:
                self._release()

    def commit(self):
        if self._connection is not None:
            self._connection.commit()
        if not self._depth:
            self._release()

    def rollback(self):
        if self._connection is not None:
            self._connection.rollback()
            if not self._depth:
                self._release()

    def close(self):
        self.rollback()
        self._release()

    def add(self, obj):
        from .operation import DBOperation
        return DBOperation.add(obj, self)
