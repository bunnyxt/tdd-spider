"""Lazy, bounded PostgreSQL connections and explicit business transactions."""
from threading import Lock
from conf import get_db_args

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

    def dispose(self):
        if self._pool is not None:
            self._pool.close()
            self._pool = None


engine = Engine()
