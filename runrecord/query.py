"""
Read-only query CLI over the run-record store (BL-0005).

The write side (RunRecorder, schema, the driver shim, the UTC contract) is
BL-0001; this module never writes. It opens the SQLite database in read-only
mode and only issues ``SELECT`` / ``PRAGMA`` statements.

Run it as a module::

    python -m runrecord list
    python -m runrecord list --script 51_hourly-video-record-add --status failed --since 7d
    python -m runrecord show --latest --script 51_hourly-video-record-add
    python -m runrecord show a1b2c3d4          # full id or a unique prefix
    python -m runrecord list --json            # stable machine-readable output

Everything reused from the rest of the package rather than reimplemented:

* ``runrecord._sqlite.sqlite3``  -- the driver shim (stdlib sqlite3 / pysqlite3).
* ``recorder._resolve_db_path``  -- ``TDD_RUN_RECORD_DB`` / default ``data/`` path.
* ``recorder.display_status``    -- the ``running`` -> ``stale`` derivation.
* ``recorder._parse_iso`` / the ISO-8601-UTC timestamp contract.
* ``schema.SCHEMA_VERSION``      -- the schema-compatibility check.

Exit codes:

===  ==================================================================
  0  success -- the query ran and matched at least one run
  1  no match -- the query is valid but nothing matched
  2  invalid arguments (argparse)
  3  incompatible schema -- the database is newer than this code understands
  4  database error -- missing/corrupt database, no SQLite driver, bad table
  5  database file not found
===  ==================================================================
"""

import argparse
import json
import os
import pathlib
import re
import sys
from datetime import datetime, timedelta, timezone

from ._sqlite import sqlite3
from .recorder import (
    _parse_iso,
    _resolve_db_path,
    display_status,
    DEFAULT_STALE_AFTER_S,
    RUNNING,
    SUCCEEDED,
    FAILED,
)
from .schema import SCHEMA_VERSION

__all__ = ['main']

EXIT_OK = 0
EXIT_NO_MATCH = 1
EXIT_USAGE = 2
EXIT_SCHEMA = 3
EXIT_DB_ERROR = 4
EXIT_NO_DB = 5

# persisted statuses plus the read-time derivation
_STATUS_CHOICES = (RUNNING, SUCCEEDED, FAILED, 'stale')

# safety bound on the pre-derivation fetch; the store is a single-machine,
# low-volume personal workload (hourly-ish runs) so this is never reached in
# practice, it just stops a pathological query from reading an unbounded set.
_FETCH_CAP = 100_000

_CORE_COLUMNS = (
    'run_id', 'script_name', 'host', 'code_version',
    'started_at', 'finished_at', 'status',
)
_METRIC_COLUMNS = ('run_id', 'scope', 'name', 'value', 'unit')
_LOG_COLUMNS = ('run_id', 'level', 'path')

# every table + column set a v1 query may touch; all are probed up front so a
# partially-created database fails with a concise EXIT_DB_ERROR instead of
# leaking a sqlite3 error out of a detail query
_REQUIRED_V1_SURFACE = (
    ('run', _CORE_COLUMNS),
    ('run_metric', _METRIC_COLUMNS),
    ('run_log', _LOG_COLUMNS),
)


class QueryError(Exception):
    """A query could not be answered; carries the process exit code to use."""

    def __init__(self, message, exit_code=EXIT_DB_ERROR):
        super().__init__(message)
        self.exit_code = exit_code


# --------------------------------------------------------------------------- #
# time handling
# --------------------------------------------------------------------------- #

_REL_RE = re.compile(r'^(\d+)\s*([smhdw])$')
_REL_UNIT_SECONDS = {'s': 1, 'm': 60, 'h': 3600, 'd': 86400, 'w': 604800}


def _parse_time(value, now=None):
    """
    Parse a ``--since`` / ``--until`` bound into an aware UTC datetime.

    Accepts a relative span (``30m``, ``24h``, ``7d``, ``2w``) measured back from
    now, or an ISO-8601 date / datetime. A bare (offset-naive) ISO value is read
    as UTC, matching how the store persists timestamps.
    """
    now = now or datetime.now(timezone.utc)
    text = value.strip()

    m = _REL_RE.match(text)
    if m:
        seconds = int(m.group(1)) * _REL_UNIT_SECONDS[m.group(2)]
        return now - timedelta(seconds=seconds)

    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        raise QueryError(
            f'invalid time value {value!r}: use a span like 24h/7d or an '
            f'ISO-8601 date/datetime', EXIT_USAGE)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _duration_seconds(started_at, finished_at):
    start = _parse_iso(started_at) if started_at else None
    end = _parse_iso(finished_at) if finished_at else None
    if start is None or end is None:
        return None
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    return (end - start).total_seconds()


def _human_duration(seconds):
    if seconds is None:
        return '-'
    total = int(round(seconds))
    sign = '-' if total < 0 else ''
    total = abs(total)
    hours, rem = divmod(total, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f'{sign}{hours}h{minutes}m'
    if minutes:
        return f'{sign}{minutes}m{secs}s'
    return f'{sign}{secs}s'


def _local(iso_value):
    """ISO-8601 UTC string -> local wall-clock string, or '-' if unparseable."""
    dt = _parse_iso(iso_value) if iso_value else None
    if dt is None:
        return '-'
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone().strftime('%Y-%m-%d %H:%M:%S')


def _local_tz_label():
    tz = datetime.now().astimezone().tzinfo
    offset = datetime.now(timezone.utc).astimezone().strftime('%z')
    name = tz.tzname(datetime.now()) if tz else 'local'
    pretty = f'{offset[:3]}:{offset[3:]}' if len(offset) == 5 else offset
    return f'{name} (UTC{pretty})'


# --------------------------------------------------------------------------- #
# database access (read-only)
# --------------------------------------------------------------------------- #

def _connect_ro(path):
    """Open ``path`` read-only. Never creates or migrates the database."""
    if sqlite3 is None:
        raise QueryError(
            'no SQLite driver available (stdlib sqlite3 missing, pysqlite3 not '
            'installed)', EXIT_DB_ERROR)
    if not os.path.exists(path):
        raise QueryError(f'run-records database not found: {path}', EXIT_NO_DB)
    uri = pathlib.Path(os.path.abspath(path)).as_uri() + '?mode=ro'
    try:
        return sqlite3.connect(uri, uri=True, timeout=5)
    except sqlite3.Error as e:
        raise QueryError(f'could not open database {path}: {e}', EXIT_DB_ERROR)


def _check_schema(conn):
    """Return the database's ``user_version``; refuse a newer schema."""
    try:
        version = conn.execute('PRAGMA user_version').fetchone()[0]
    except sqlite3.DatabaseError as e:
        raise QueryError(f'not a readable SQLite database: {e}', EXIT_DB_ERROR)

    if version > SCHEMA_VERSION:
        raise QueryError(
            f'run-records database is at schema v{version}, but this CLI only '
            f'understands v{SCHEMA_VERSION}; upgrade the tooling', EXIT_SCHEMA)

    for table, columns in _REQUIRED_V1_SURFACE:
        try:
            conn.execute(f'SELECT {", ".join(columns)} FROM {table} LIMIT 0')
        except sqlite3.DatabaseError as e:
            raise QueryError(
                f'database is missing the v1 "{table}" query surface ({e}); '
                f'has it been initialised?', EXIT_DB_ERROR)
    return version


def _fetch_runs(conn, *, script=None, since=None, until=None, persisted_status=None,
                order='DESC', limit=_FETCH_CAP):
    where, params = [], []
    if script:
        where.append('script_name = ?')
        params.append(script)
    if since is not None:
        where.append('started_at >= ?')
        params.append(since.isoformat())
    if until is not None:
        where.append('started_at <= ?')
        params.append(until.isoformat())
    if persisted_status is not None:
        where.append('status = ?')
        params.append(persisted_status)

    sql = f'SELECT {", ".join(_CORE_COLUMNS)} FROM run'
    if where:
        sql += ' WHERE ' + ' AND '.join(where)
    sql += f' ORDER BY started_at {order}, run_id {order} LIMIT ?'
    params.append(limit)
    return _run_query(conn, sql, params)


def _run_query(conn, sql, params=()):
    """Execute a read query, translating any SQLite error into a QueryError."""
    try:
        return conn.execute(sql, params).fetchall()
    except sqlite3.DatabaseError as e:
        raise QueryError(f'query failed: {e}', EXIT_DB_ERROR)


def _grouped_metrics(conn, run_id):
    out = {}
    rows = _run_query(
        conn,
        'SELECT scope, name, value, unit FROM run_metric WHERE run_id = ? '
        'ORDER BY scope, name', (run_id,))
    for scope, name, value, unit in rows:
        out.setdefault(scope, []).append(
            {'name': name, 'value': value, 'unit': unit})
    return out


def _log_paths(conn, run_id):
    rows = _run_query(
        conn,
        'SELECT level, path FROM run_log WHERE run_id = ? ORDER BY level, path',
        (run_id,))
    return [{'level': level, 'path': path} for level, path in rows]


# --------------------------------------------------------------------------- #
# row -> structured record
# --------------------------------------------------------------------------- #

def _record(row, *, now, stale_after, conn=None, detail=False):
    run_id, script_name, host, code_version, started_at, finished_at, status = row
    shown = display_status(status, started_at, finished_at,
                           now=now, stale_after_s=stale_after)
    rec = {
        'run_id': run_id,
        'script_name': script_name,
        'host': host,
        'code_version': code_version,
        'started_at': started_at,
        'finished_at': finished_at,
        'status': status,
        'display_status': shown,
        'stale': shown == 'stale',
        'duration_s': _duration_seconds(started_at, finished_at),
    }
    if detail and conn is not None:
        rec['metrics'] = _grouped_metrics(conn, run_id)
        rec['logs'] = _log_paths(conn, run_id)
    return rec


def _select_by_status(records, status):
    """Filter derived records by a requested display status.

    ``running`` means genuinely running (not yet stale); ``stale`` means a
    ``running`` row past its budget. ``succeeded`` / ``failed`` were already
    filtered in SQL but re-checking is harmless.
    """
    if status is None:
        return records
    return [r for r in records if r['display_status'] == status]


# --------------------------------------------------------------------------- #
# commands
# --------------------------------------------------------------------------- #

def _persisted_prefilter(status):
    """Map a requested display status to the persisted status to filter in SQL."""
    if status in (RUNNING, 'stale'):
        return RUNNING
    return status  # succeeded / failed / None


def _query_dict(args, command):
    q = {'command': command, 'script': getattr(args, 'script', None),
         'status': getattr(args, 'status', None)}
    if command == 'list':
        q['since'] = args.since.isoformat() if args.since else None
        q['until'] = args.until.isoformat() if args.until else None
        q['limit'] = args.limit
    if command == 'show':
        q['run_id'] = args.run_id
        q['latest'] = args.latest
    q['stale_after_s'] = args.stale_after
    return q


def _emit(records, *, args, command, db_path, db_version, detail):
    payload = {
        'schema_version': db_version,
        'db_path': db_path,
        'generated_at': datetime.now(timezone.utc).isoformat(),
        'query': _query_dict(args, command),
        'count': len(records),
        'runs': records,
    }
    if args.json:
        print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))
    elif command == 'show':
        _print_detail(records[0], db_path=db_path, db_version=db_version)
    else:
        _print_table(records, db_path=db_path, db_version=db_version)
    return EXIT_OK if records else EXIT_NO_MATCH


def _cmd_list(args, conn, db_path, db_version):
    now = datetime.now(timezone.utc)
    rows = _fetch_runs(
        conn, script=args.script, since=args.since, until=args.until,
        persisted_status=_persisted_prefilter(args.status), order='DESC')
    records = [_record(r, now=now, stale_after=args.stale_after) for r in rows]
    records = _select_by_status(records, args.status)[:args.limit]
    return _emit(records, args=args, command='list', db_path=db_path,
                 db_version=db_version, detail=False)


def _cmd_show(args, conn, db_path, db_version):
    now = datetime.now(timezone.utc)
    if args.run_id and args.latest:
        raise QueryError('give a RUN_ID or --latest, not both', EXIT_USAGE)
    if not args.run_id and not args.latest:
        raise QueryError('give a RUN_ID or --latest', EXIT_USAGE)

    if args.run_id:
        row = _resolve_one(conn, args.run_id)
        records = [_record(row, now=now, stale_after=args.stale_after,
                           conn=conn, detail=True)]
        return _emit(records, args=args, command='show', db_path=db_path,
                     db_version=db_version, detail=True)

    rows = _fetch_runs(
        conn, script=args.script,
        persisted_status=_persisted_prefilter(args.status), order='DESC')
    records = [_record(r, now=now, stale_after=args.stale_after) for r in rows]
    records = _select_by_status(records, args.status)
    if not records:
        return EXIT_NO_MATCH
    chosen = records[0]
    full = _record(
        next(r for r in rows if r[0] == chosen['run_id']),
        now=now, stale_after=args.stale_after, conn=conn, detail=True)
    return _emit([full], args=args, command='show', db_path=db_path,
                 db_version=db_version, detail=True)


def _resolve_one(conn, run_id):
    exact = _run_query(
        conn, f'SELECT {", ".join(_CORE_COLUMNS)} FROM run WHERE run_id = ?',
        (run_id,))
    if len(exact) == 1:
        return exact[0]

    like = _run_query(
        conn,
        f'SELECT {", ".join(_CORE_COLUMNS)} FROM run WHERE run_id LIKE ? '
        f'ESCAPE \'\\\' ORDER BY run_id LIMIT 2',
        (run_id.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_') + '%',))
    if len(like) == 1:
        return like[0]
    if len(like) > 1:
        raise QueryError(f'run id prefix {run_id!r} is ambiguous', EXIT_USAGE)
    raise QueryError(f'no run with id {run_id!r}', EXIT_NO_MATCH)


# --------------------------------------------------------------------------- #
# human-readable rendering
# --------------------------------------------------------------------------- #

def _meta_line(db_path, db_version, count):
    return (f'{count} run(s)   db: {db_path}   schema: v{db_version}   '
            f'times: {_local_tz_label()}')


def _print_table(records, *, db_path, db_version):
    print(_meta_line(db_path, db_version, len(records)))
    if not records:
        print('(no matching runs)')
        return
    print()
    print(f'{"STARTED (local)":<19}  {"SCRIPT":<32}  {"STATUS":<9}  '
          f'{"DURATION":>8}  RUN ID')
    for r in records:
        script = r['script_name']
        if len(script) > 32:
            script = script[:31] + '…'
        print(f'{_local(r["started_at"]):<19}  {script:<32}  '
              f'{r["display_status"]:<9}  '
              f'{_human_duration(r["duration_s"]):>8}  {r["run_id"][:12]}')


def _print_detail(r, *, db_path, db_version):
    print(_meta_line(db_path, db_version, 1))
    print()
    rows = [
        ('run_id', r['run_id']),
        ('script', r['script_name']),
        ('host', r['host']),
        ('code_version', r['code_version'] or '-'),
        ('started', f'{_local(r["started_at"])}   ({r["started_at"]})'),
        ('finished', (f'{_local(r["finished_at"])}   ({r["finished_at"]})'
                      if r['finished_at'] else '-')),
        ('duration', _human_duration(r['duration_s'])),
        ('status', f'{r["status"]} (persisted)'),
        ('display', r['display_status']),
    ]
    width = max(len(k) for k, _ in rows)
    for key, value in rows:
        print(f'{key:<{width}}  {value}')

    metrics = r.get('metrics') or {}
    print()
    if metrics:
        print('metrics:')
        for scope in sorted(metrics):
            print(f'  {scope}')
            entries = metrics[scope]
            name_w = max((len(e['name']) for e in entries), default=0)
            rendered = []
            for e in entries:
                value = e['value']
                if isinstance(value, float) and value.is_integer():
                    value = int(value)
                rendered.append((e['name'], str(value), e['unit']))
            value_w = max((len(v) for _, v, _ in rendered), default=0)
            for name, value, unit in rendered:
                unit = f'  {unit}' if unit else ''
                print(f'    {name:<{name_w}}  {value:>{value_w}}{unit}')
    else:
        print('metrics: (none)')

    logs = r.get('logs') or []
    print()
    if logs:
        print('logs:')
        level_w = max(len(l['level']) for l in logs)
        for l in logs:
            print(f'  {l["level"]:<{level_w}}  {l["path"]}')
    else:
        print('logs: (none)')


# --------------------------------------------------------------------------- #
# argument parsing / entry point
# --------------------------------------------------------------------------- #

_EPILOG = (
    '`list` is the default command and may be omitted. --db, --stale-after and\n'
    '--json may appear before or after an explicit list / show.\n\n'
    'exit codes: 0 ok, 1 no match, 2 bad arguments, 3 incompatible schema, '
    '4 database error, 5 database file not found.\n\n'
    'examples:\n'
    '  python -m runrecord\n'
    '  python -m runrecord list --script 51_hourly-video-record-add --since 7d\n'
    '  python -m runrecord --json list --status stale\n'
    '  python -m runrecord show --latest --script 51_hourly-video-record-add\n'
    '  python -m runrecord --db ./run-records.sqlite3 show a1b2c3d4\n')


def _add_common(parser):
    parser.add_argument(
        '--db', metavar='PATH', default=None,
        help='path to the run-records SQLite file (default: $TDD_RUN_RECORD_DB '
             'or <repo>/data/run-records.sqlite3)')
    parser.add_argument(
        '--stale-after', type=int, default=DEFAULT_STALE_AFTER_S, metavar='SECONDS',
        help='a "running" run older than this is shown as "stale" '
             f'(default: {DEFAULT_STALE_AFTER_S})')
    parser.add_argument(
        '--json', action='store_true',
        help='emit a stable machine-readable JSON document instead of a table')


def _top_parser():
    parser = argparse.ArgumentParser(
        prog='python -m runrecord',
        description='Read-only query CLI over the run-record store. The default '
                    'command is "list"; --db, --stale-after and --json may '
                    'appear before or after an explicit list / show.',
        epilog=_EPILOG, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest='command', metavar='{list,show}')
    sub.add_parser('list', help='list recent runs (default)', add_help=False)
    sub.add_parser('show', help='show one run in full', add_help=False)
    _add_common(parser)
    return parser


def _list_parser():
    parser = argparse.ArgumentParser(
        prog='python -m runrecord list',
        description='List recent runs, most recent first.',
        epilog=_EPILOG, formatter_class=argparse.RawDescriptionHelpFormatter)
    _add_common(parser)
    parser.add_argument('--script', metavar='NAME', help='exact script_name')
    parser.add_argument('--status', choices=_STATUS_CHOICES,
                        help='filter by persisted or derived status')
    parser.add_argument('--since', metavar='WHEN',
                        help='only runs started at/after this (span 24h/7d or ISO)')
    parser.add_argument('--until', metavar='WHEN',
                        help='only runs started at/before this (span or ISO)')
    parser.add_argument('--limit', type=int, default=20, metavar='N',
                        help='max rows to show (default: 20)')
    return parser


def _show_parser():
    parser = argparse.ArgumentParser(
        prog='python -m runrecord show',
        description='Show one run: core fields, derived duration/stale status, '
                    'grouped metrics and associated log paths.',
        epilog=_EPILOG, formatter_class=argparse.RawDescriptionHelpFormatter)
    _add_common(parser)
    parser.add_argument('run_id', nargs='?', metavar='RUN_ID',
                        help='full run id or a unique prefix')
    parser.add_argument('--latest', action='store_true',
                        help='pick the most recent run (optionally filtered)')
    parser.add_argument('--script', metavar='NAME',
                        help='with --latest: restrict to this script_name')
    parser.add_argument('--status', choices=_STATUS_CHOICES,
                        help='with --latest: restrict to this status')
    return parser


def _dispatch(args):
    db_path = _resolve_db_path(args.db)
    conn = _connect_ro(db_path)
    try:
        db_version = _check_schema(conn)
        if args.command == 'show':
            return _cmd_show(args, conn, db_path, db_version)
        return _cmd_list(args, conn, db_path, db_version)
    finally:
        conn.close()


# options (on any command) that consume the following token as their value;
# used only to skip over `--opt value` while locating the command token
_VALUE_OPTS = frozenset((
    '--db', '--stale-after', '--script', '--status', '--since', '--until', '--limit',
))


def _split_command(argv):
    """
    Find an explicit ``list`` / ``show`` token anywhere before the first bare
    positional, and return ``(command, argv_without_that_token)``.

    This lets the common options (--db, --stale-after, --json) sit on either
    side of the command: everything is then handed to one flat per-command
    parser that accepts those options regardless of position.
    """
    i = 0
    while i < len(argv):
        tok = argv[i]
        if tok == '--':
            break
        if tok in ('list', 'show'):
            return tok, argv[:i] + argv[i + 1:]
        if tok in _VALUE_OPTS:
            i += 2                     # skip the option and its value
            continue
        if tok.startswith('-'):        # a flag / --opt=value / -h -- keep scanning
            i += 1
            continue
        break                          # a bare positional that isn't a command
    return None, argv


def _parse_args(argv):
    """Route to a flat per-command parser; `list` is the default command."""
    if argv and argv[0] in ('-h', '--help'):
        _top_parser().parse_args(argv)  # prints help, raises SystemExit(0)

    command, rest = _split_command(argv)
    if command is None:
        command = 'list'
    parser = _show_parser() if command == 'show' else _list_parser()

    args = parser.parse_args(rest)
    args.command = command
    if getattr(args, 'limit', 0) < 0:
        parser.error('--limit must be non-negative')
    if args.stale_after < 0:
        parser.error('--stale-after must be non-negative')

    # resolve time bounds here so a bad value is a clean usage error (exit 2)
    # rather than surfacing after the database is opened
    now = datetime.now(timezone.utc)
    for attr in ('since', 'until'):
        raw = getattr(args, attr, None)
        if raw is not None:
            try:
                setattr(args, attr, _parse_time(raw, now))
            except QueryError as e:
                parser.error(str(e))
    return args


def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]
    args = _parse_args(list(argv))
    try:
        return _dispatch(args)
    except QueryError as e:
        print(f'error: {e}', file=sys.stderr)
        return e.exit_code


if __name__ == '__main__':
    sys.exit(main())
