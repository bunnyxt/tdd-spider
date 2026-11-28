"""
SQLite driver shim.

The supported Python 3.12 runtime uses the stdlib ``sqlite3`` module. For unusual
Python builds without ``_sqlite3``, an independently installed ``pysqlite3``
module remains an optional compatibility fallback; it is not a required
collector dependency.

Only a local reference is rebound -- ``sys.modules['sqlite3']`` is left untouched
so nothing else in the process is affected.
"""

try:
    import sqlite3
except ImportError:  # pragma: no cover - optional compatibility fallback
    try:
        import pysqlite3 as sqlite3  # type: ignore
    except ImportError:
        sqlite3 = None  # type: ignore
        # RunRecorder.start() turns a missing driver into a disabled (no-op)
        # recorder rather than a crash -- run recording must never break a
        # production script.

__all__ = ['sqlite3']
