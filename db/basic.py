"""Lazy, bounded PostgreSQL connections and explicit business transactions."""
from __future__ import annotations
from contextlib import AbstractContextManager
from threading import Lock
from typing import TYPE_CHECKING

from conf import get_db_args

if TYPE_CHECKING:
    from psycopg import Connection
    from psycopg_pool import ConnectionPool

class Engine:
    def __init__(self) -> None:
        self._pool: ConnectionPool | None = None
        self._lock = Lock()

    @property
    def pool(self) -> ConnectionPool:
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

    def connection(self) -> AbstractContextManager[Connection]:
        return self.pool.connection()

    def dispose(self) -> None:
        if self._pool is not None:
            self._pool.close()
            self._pool = None


engine = Engine()
