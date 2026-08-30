"""
SQLite driver shim.

The stdlib ``sqlite3`` module is used everywhere it is available (dev machines,
CI). The production ``venv-3.11`` (Ubuntu 16.04, Python 3.11.3) was built without
the ``_sqlite3`` extension, so ``import sqlite3`` raises there; on that host the
``pysqlite3-binary`` wheel (declared in requirements.txt for linux) provides a
drop-in DB-API module backed by its own statically-linked SQLite.

Only a local reference is rebound -- ``sys.modules['sqlite3']`` is left untouched
so nothing else in the process is affected.
"""

try:
    import sqlite3
except ImportError:  # pragma: no cover - exercised only on the prod venv
    try:
        import pysqlite3 as sqlite3  # type: ignore
    except ImportError:
        sqlite3 = None  # type: ignore
        # RunRecorder.start() turns a missing driver into a disabled (no-op)
        # recorder rather than a crash -- run recording must never break a
        # production script.

__all__ = ['sqlite3']
