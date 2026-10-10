"""
Maintenance commands for the run-record store: consistent snapshots and retention.

The query CLI (``python -m runrecord``) is read-only by contract; anything that
writes files or deletes rows lives here instead::

    python -m runrecord.maintain backup
    python -m runrecord.maintain prune --keep-days 365
    python -m runrecord.maintain prune --keep-days 365 --dry-run

``backup`` never copies the database file directly -- a copy taken while a
script is writing can be torn. It opens the database read-only and uses SQLite's
online backup API, which produces a consistent point-in-time copy. The copy is
checked with ``PRAGMA integrity_check`` before it is gzipped and published as
``run-records_<UTC stamp>.sqlite3.gz`` plus a ``.sha256`` file in ``sha256sum``
format. Both are written to temporary names first and renamed into place, so a
failed run never leaves a half-written snapshot behind. Snapshots accumulate in
the output directory until they are pulled off the host; this module does not
delete them.

``prune`` deletes runs whose ``started_at`` is older than ``--keep-days`` days,
together with their ``run_metric`` and ``run_log`` rows, in one transaction. It
never creates or migrates a database. ``--dry-run`` only reports what would go.

Exit codes follow the query CLI:

===  ==================================================================
  0  success
  2  invalid arguments (argparse)
  3  incompatible schema -- the database is newer than this code understands
  4  database error -- corrupt database, no SQLite driver, failed check
  5  database file not found
===  ==================================================================
"""

import argparse
import gzip
import hashlib
import os
import pathlib
import shutil
import sys
from datetime import datetime, timedelta, timezone

from ._sqlite import sqlite3
from .query import (
    QueryError,
    _check_schema,
    _connect_ro,
    EXIT_OK,
    EXIT_DB_ERROR,
    EXIT_NO_DB,
)
from .recorder import DEFAULT_DB_PATH, _resolve_db_path

__all__ = ['backup', 'prune', 'main', 'DEFAULT_SNAPSHOT_DIR', 'DEFAULT_KEEP_DAYS']

DEFAULT_SNAPSHOT_DIR = os.path.join(os.path.dirname(DEFAULT_DB_PATH), 'runrecord-snapshot')

# deliberately generous: the store only dates back to 2026-08-30 and grows by
# well under 1 MB a day, so the default should not delete anything for months.
DEFAULT_KEEP_DAYS = 365

_SNAPSHOT_PREFIX = 'run-records_'
_SNAPSHOT_SUFFIX = '.sqlite3.gz'


def _sha256(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _integrity_errors(conn):
    """``PRAGMA integrity_check`` findings; an empty list means the copy is sound."""
    rows = [row[0] for row in conn.execute('PRAGMA integrity_check').fetchall()]
    return [] if rows == ['ok'] else rows


def _remove_quietly(path):
    try:
        os.remove(path)
    except FileNotFoundError:
        pass


def backup(db_path=None, out_dir=None, now=None):
    """
    Write a consistent, gzipped snapshot of the run-record database.

    Returns a dict describing the published snapshot. Raises ``QueryError`` if
    the database is missing, unreadable, at an unknown schema, or the snapshot
    fails its integrity check; nothing is published in that case.
    """
    db_path = _resolve_db_path(db_path)
    out_dir = out_dir or DEFAULT_SNAPSHOT_DIR
    now = now or datetime.now(timezone.utc)

    name = f'{_SNAPSHOT_PREFIX}{now.strftime("%Y%m%dT%H%M%SZ")}{_SNAPSHOT_SUFFIX}'
    final_path = os.path.join(out_dir, name)
    sha_path = final_path + '.sha256'
    if os.path.exists(final_path) or os.path.exists(sha_path):
        raise QueryError(f'snapshot already exists: {final_path}', EXIT_DB_ERROR)

    src = _connect_ro(db_path)
    try:
        _check_schema(src)
        os.makedirs(out_dir, exist_ok=True)
        tmp_db = os.path.join(out_dir, f'.{name}.tmp.sqlite3')
        tmp_gz = os.path.join(out_dir, f'.{name}.tmp')
        tmp_sha = os.path.join(out_dir, f'.{name}.sha256.tmp')
        try:
            dst = sqlite3.connect(tmp_db)
            try:
                src.backup(dst)
                errors = _integrity_errors(dst)
                if errors:
                    raise QueryError(
                        f'snapshot failed integrity_check: {errors[:5]}', EXIT_DB_ERROR)
                version = dst.execute('PRAGMA user_version').fetchone()[0]
                runs = dst.execute('SELECT COUNT(*) FROM run').fetchone()[0]
                latest = dst.execute('SELECT MAX(started_at) FROM run').fetchone()[0]
            finally:
                dst.close()

            with open(tmp_db, 'rb') as raw, gzip.open(tmp_gz, 'wb') as packed:
                shutil.copyfileobj(raw, packed)
            digest = _sha256(tmp_gz)
            with open(tmp_sha, 'w') as f:
                f.write(f'{digest}  {name}\n')

            os.replace(tmp_gz, final_path)
            os.replace(tmp_sha, sha_path)
        except sqlite3.Error as e:
            raise QueryError(f'snapshot failed: {e}', EXIT_DB_ERROR)
        finally:
            for path in (tmp_db, tmp_gz, tmp_sha):
                _remove_quietly(path)
    finally:
        src.close()

    return {
        'path': final_path,
        'sha256': digest,
        'bytes': os.path.getsize(final_path),
        'schema_version': version,
        'runs': runs,
        'latest_started_at': latest,
    }


def _connect_rw(path):
    """Open an existing database for writing. Never creates one."""
    if sqlite3 is None:
        raise QueryError(
            'no SQLite driver available (stdlib sqlite3 missing, pysqlite3 not '
            'installed)', EXIT_DB_ERROR)
    if not os.path.exists(path):
        raise QueryError(f'run-records database not found: {path}', EXIT_NO_DB)
    uri = pathlib.Path(os.path.abspath(path)).as_uri() + '?mode=rw'
    try:
        return sqlite3.connect(uri, uri=True, timeout=30)
    except sqlite3.Error as e:
        raise QueryError(f'could not open database {path}: {e}', EXIT_DB_ERROR)


def prune(db_path=None, keep_days=DEFAULT_KEEP_DAYS, dry_run=False, now=None):
    """
    Delete runs that started more than ``keep_days`` days ago, with their
    metrics and log locations. Returns the cutoff and the per-table counts.
    """
    if keep_days < 1:
        raise ValueError('keep_days must be at least 1')
    db_path = _resolve_db_path(db_path)
    now = now or datetime.now(timezone.utc)
    # started_at is always ISO 8601 UTC with a +00:00 offset, so plain string
    # comparison against a cutoff in the same format orders chronologically
    cutoff = (now - timedelta(days=keep_days)).astimezone(timezone.utc).isoformat()
    old_runs = 'SELECT run_id FROM run WHERE started_at < ?'

    conn = _connect_rw(db_path)
    try:
        _check_schema(conn)
        counts = {
            'runs': conn.execute(
                f'SELECT COUNT(*) FROM ({old_runs})', (cutoff,)).fetchone()[0],
            'metrics': conn.execute(
                f'SELECT COUNT(*) FROM run_metric WHERE run_id IN ({old_runs})',
                (cutoff,)).fetchone()[0],
            'logs': conn.execute(
                f'SELECT COUNT(*) FROM run_log WHERE run_id IN ({old_runs})',
                (cutoff,)).fetchone()[0],
        }
        if not dry_run and counts['runs']:
            try:
                with conn:
                    conn.execute(
                        f'DELETE FROM run_metric WHERE run_id IN ({old_runs})', (cutoff,))
                    conn.execute(
                        f'DELETE FROM run_log WHERE run_id IN ({old_runs})', (cutoff,))
                    conn.execute('DELETE FROM run WHERE started_at < ?', (cutoff,))
            except sqlite3.Error as e:
                raise QueryError(f'prune failed and was rolled back: {e}', EXIT_DB_ERROR)
    finally:
        conn.close()

    return {'cutoff': cutoff, 'dry_run': dry_run, **counts}


def _positive_int(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError('must be at least 1')
    return number


def _parse_args(argv):
    parser = argparse.ArgumentParser(
        prog='python -m runrecord.maintain',
        description='Snapshot and retention commands for the run-record store.')
    parser.add_argument(
        '--db', metavar='PATH', default=None,
        help='path to the run-records SQLite file (default: $TDD_RUN_RECORD_DB '
             'or data/run-records.sqlite3)')
    sub = parser.add_subparsers(dest='command', required=True)

    p_backup = sub.add_parser('backup', help='write a consistent gzipped snapshot')
    p_backup.add_argument(
        '--out-dir', metavar='DIR', default=None,
        help='where to publish snapshots (default: data/runrecord-snapshot)')

    p_prune = sub.add_parser('prune', help='delete runs older than --keep-days')
    p_prune.add_argument(
        '--keep-days', type=_positive_int, default=DEFAULT_KEEP_DAYS,
        help=f'keep runs that started within this many days (default: {DEFAULT_KEEP_DAYS})')
    p_prune.add_argument(
        '--dry-run', action='store_true', help='report what would be deleted, delete nothing')
    return parser.parse_args(argv)


def main(argv=None):
    args = _parse_args(sys.argv[1:] if argv is None else list(argv))
    stamp = datetime.now(timezone.utc).isoformat(timespec='seconds')
    try:
        if args.command == 'backup':
            result = backup(args.db, args.out_dir)
            print(f'{stamp} backup ok: {result["path"]} bytes={result["bytes"]} '
                  f'runs={result["runs"]} latest={result["latest_started_at"]} '
                  f'schema=v{result["schema_version"]} sha256={result["sha256"]}')
        else:
            result = prune(args.db, args.keep_days, args.dry_run)
            verb = 'would delete' if result['dry_run'] else 'deleted'
            print(f'{stamp} prune ok: cutoff={result["cutoff"]} {verb} '
                  f'runs={result["runs"]} metrics={result["metrics"]} '
                  f'logs={result["logs"]}')
    except QueryError as e:
        print(f'{stamp} {args.command} error: {e}', file=sys.stderr)
        return e.exit_code
    return EXIT_OK


if __name__ == '__main__':
    sys.exit(main())
