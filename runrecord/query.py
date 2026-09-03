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
    python -m runrecord overview               # one row per script, latest run + key metrics
    python -m runrecord trend --script 51_hourly-video-record-add --since 7d
    python -m runrecord trend --script 51_hourly-video-record-add \
        --metric record-fetch/total_count --metric record-fetch/other_exception --json

``overview`` and ``trend`` are the metric reading commands: ``overview`` compares
scripts at a glance, ``trend`` aligns one script's runs into a per-run time
series. Both organise already-recorded facts -- no health classification, rates,
thresholds, anomaly detection or forecasting.

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
import shutil
import sys
from datetime import datetime, timedelta, timezone

from ._sqlite import sqlite3
from .keymetric import is_key_metric
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


def _has_is_key_col(conn):
    """True if this database is schema v2+ (``run_metric.is_key`` present)."""
    try:
        return any(row[1] == 'is_key'
                   for row in conn.execute('PRAGMA table_info(run_metric)'))
    except sqlite3.DatabaseError:
        return False


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


def _explicit_key_flags(conn, run_id):
    """``{(scope, name): 0|1}`` for metrics of ``run_id`` with an explicit flag.

    Empty on a schema-v1 database (no ``is_key`` column) -- callers then fall
    back to the name-based key convention alone.
    """
    if not _has_is_key_col(conn):
        return {}
    rows = _run_query(
        conn,
        'SELECT scope, name, is_key FROM run_metric '
        'WHERE run_id = ? AND is_key IS NOT NULL', (run_id,))
    return {(scope, name): flag for scope, name, flag in rows}


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
    q = {'command': command, 'stale_after_s': args.stale_after}
    if command in ('list', 'show'):
        q['script'] = getattr(args, 'script', None)
        q['status'] = getattr(args, 'status', None)
    if command == 'list':
        q['since'] = args.since.isoformat() if args.since else None
        q['until'] = args.until.isoformat() if args.until else None
        q['limit'] = args.limit
    if command == 'show':
        q['run_id'] = args.run_id
        q['latest'] = args.latest
    if command == 'overview':
        q['since'] = args.since.isoformat() if args.since else None
        q['max_metrics'] = args.max_metrics
    if command == 'trend':
        q['script'] = args.script
        q['since'] = args.since.isoformat() if args.since else None
        q['until'] = args.until.isoformat() if args.until else None
        q['limit'] = args.limit
        q['metrics'] = ([list(m) for m in args.metrics] if args.metrics else None)
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
# overview / trend commands (metric overview and trend)
# --------------------------------------------------------------------------- #

def _parse_metric_arg(raw):
    """``"scope/name"`` -> ``(scope, name)``; ``"duration"`` -> the built-in series."""
    from .series import DURATION_SERIES

    text = raw.strip()
    if text in ('duration', 'duration_s', 'run/duration_s'):
        return DURATION_SERIES
    scope, sep, name = text.partition('/')
    scope, name = scope.strip(), name.strip()
    if not sep or not scope or not name:
        raise QueryError(
            f'--metric {raw!r} must be SCOPE/NAME (or "duration")', EXIT_USAGE)
    return (scope, name)


def _pick_key_metrics(grouped, flags, cap):
    """Flatten ``{scope: [entry]}`` to the key metrics, capped and ordered.

    ``flags`` is ``{(scope, name): 0|1}`` of explicit ``is_key`` values; the
    name-based convention decides the rest. Order is deterministic and
    independent of the run: ``total_count`` first, then everything else, each
    group sorted by ``(scope, name)``. A display choice only -- it carries no
    health, direction or threshold meaning.
    """
    flat = []
    for scope in grouped:
        for e in grouped[scope]:
            if is_key_metric(e['name'], flags.get((scope, e['name']))):
                flat.append({'scope': scope, 'name': e['name'],
                             'value': e['value'], 'unit': e['unit']})
    flat.sort(key=lambda m: (0 if m['name'].lower() == 'total_count' else 1,
                             m['scope'], m['name']))
    return flat if cap is None else flat[:cap]


def _cmd_overview(args, conn, db_path, db_version):
    now = datetime.now(timezone.utc)
    rows = _fetch_runs(conn, since=args.since, order='DESC')

    latest = {}
    for row in rows:
        latest.setdefault(row[1], row)          # row[1] == script_name; DESC -> newest kept

    records = []
    for script in sorted(latest):
        rec = _record(latest[script], now=now, stale_after=args.stale_after,
                      conn=conn, detail=True)
        flags = _explicit_key_flags(conn, rec['run_id'])
        rec['key_metrics'] = _pick_key_metrics(rec['metrics'], flags,
                                               args.max_metrics)
        records.append(rec)

    payload = {
        'schema_version': db_version,
        'db_path': db_path,
        'generated_at': now.isoformat(),
        'query': _query_dict(args, 'overview'),
        'count': len(records),
        'scripts': records,
    }
    if args.json:
        print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))
    else:
        _print_overview(records, db_path=db_path, db_version=db_version)
    return EXIT_OK if records else EXIT_NO_MATCH


def _cmd_trend(args, conn, db_path, db_version):
    from . import series as _series

    now = datetime.now(timezone.utc)
    # select the most recent --limit runs in the window, then present them
    # oldest -> newest so the table and the sparkline read left-to-right in time.
    result = _series.fetch_series(
        conn, args.script, metrics=args.metrics,
        since=args.since, until=args.until, order='DESC',
        limit=args.limit, now=now, stale_after_s=args.stale_after)
    result['points'].reverse()

    payload = {
        'schema_version': db_version,
        'db_path': db_path,
        'generated_at': now.isoformat(),
        'query': _query_dict(args, 'trend'),
        'script_name': result['script_name'],
        'series': result['series'],
        'count': len(result['points']),
        'points': result['points'],
    }
    if args.json:
        print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))
    else:
        _print_trend(payload, args=args, db_path=db_path, db_version=db_version)
    return EXIT_OK if result['points'] else EXIT_NO_MATCH


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
# overview / trend rendering
# --------------------------------------------------------------------------- #

_SPARK_TICKS = '▁▂▃▄▅▆▇█'


def _fmt_num(value):
    """Render a metric value compactly; ``None`` (a missing point) -> ``'-'``."""
    if value is None:
        return '-'
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, float):
        return str(int(value)) if value.is_integer() else repr(value)
    return str(value)


def _trend_value(point, scope, name):
    return point['values'].get(scope, {}).get(name)


def _sparkline(values):
    """A fixed-width magnitude sketch; a missing entry renders as ``·``.

    It encodes only the min..max range of the values actually present -- never a
    direction, rate or judgement. ``·`` marks a gap and is never a zero.
    """
    present = [v for v in values if v is not None]
    if not present:
        return '·' * len(values)
    lo, hi = min(present), max(present)
    span = hi - lo
    out = []
    for v in values:
        if v is None:
            out.append('·')
        elif span == 0:
            out.append(_SPARK_TICKS[len(_SPARK_TICKS) // 2])
        else:
            out.append(_SPARK_TICKS[
                int(round((v - lo) / span * (len(_SPARK_TICKS) - 1)))])
    return ''.join(out)


def _print_overview(records, *, db_path, db_version):
    print(f'{len(records)} script(s)   db: {db_path}   schema: v{db_version}   '
          f'times: {_local_tz_label()}')
    if not records:
        print('(no runs recorded)')
        return
    print()
    print(f'{"SCRIPT":<34}  {"LATEST (local)":<19}  {"STATUS":<9}  '
          f'{"DURATION":>8}  KEY METRICS')
    for r in records:
        script = r['script_name']
        if len(script) > 34:
            script = script[:33] + '…'
        km = r['key_metrics']
        rendered = ('  '.join(f'{m["scope"]}/{m["name"]}={_fmt_num(m["value"])}'
                              for m in km)
                    if km else '(no key metrics)')
        print(f'{script:<34}  {_local(r["started_at"]):<19}  '
              f'{r["display_status"]:<9}  {_human_duration(r["duration_s"]):>8}  '
              f'{rendered}')


def _print_trend(payload, *, args, db_path, db_version):
    points = payload['points']
    series_defs = payload['series']

    print(f'{len(points)} run(s)   script: {payload["script_name"]}   '
          f'db: {db_path}   schema: v{db_version}   times: {_local_tz_label()}')
    since = f'since {args.since.isoformat()}' if args.since else 'since -'
    until = f'until {args.until.isoformat()}' if args.until else 'until now'
    print(f'window: {since}   {until}   limit: {args.limit} newest, oldest first')
    print()

    if not points:
        print('(no runs in window)')
        return

    print('series:')
    for s in series_defs:
        unit = f' ({s["unit"]})' if s.get('unit') else ''
        tag = ' [key]' if s.get('key') else ''
        print(f'  {s["scope"]}/{s["name"]}{unit}{tag}')
    if not series_defs:
        print('  (no key metrics recorded by these runs; '
              'select some with --metric)')
    print()

    labels = [f'{s["scope"]}/{s["name"]}' for s in series_defs]
    cells = [[_fmt_num(_trend_value(p, s['scope'], s['name'])) for p in points]
             for s in series_defs]

    layout = args.layout
    if layout == 'auto':
        colw = [max([len(lab)] + [len(c) for c in col])
                for lab, col in zip(labels, cells)]
        table_w = 8 + 2 + 19 + 2 + 9 + 2 + 8 + sum(w + 2 for w in colw)
        term_w = shutil.get_terminal_size(fallback=(80, 24)).columns
        layout = 'table' if table_w <= term_w else 'blocks'

    if layout == 'table':
        _trend_as_table(points, labels, cells)
    else:
        _trend_as_blocks(points, labels, cells)

    if not series_defs:
        return
    print()
    print('trend (oldest -> newest, magnitude only -- not a health signal):')
    labw = max(len(l) for l in labels)
    for lab, s in zip(labels, series_defs):
        raw = [_trend_value(p, s['scope'], s['name']) for p in points]
        present = [v for v in raw if v is not None]
        ends = (f'{_fmt_num(present[0])} -> {_fmt_num(present[-1])}'
                if present else '(no values)')
        missing = sum(1 for v in raw if v is None)
        note = f'   {missing} missing' if missing else ''
        print(f'  {lab:<{labw}}  {_sparkline(raw)}  {ends}{note}')


def _trend_as_table(points, labels, cells):
    colw = [max([len(lab)] + [len(c) for c in col])
            for lab, col in zip(labels, cells)]
    header = (f'{"RUN":<8}  {"STARTED (local)":<19}  {"STATUS":<9}  '
              f'{"DURATION":>8}')
    for lab, w in zip(labels, colw):
        header += f'  {lab:>{w}}'
    print(header)
    for i, p in enumerate(points):
        line = (f'{p["run_id"][:8]:<8}  {_local(p["started_at"]):<19}  '
                f'{p["display_status"]:<9}  {_human_duration(p["duration_s"]):>8}')
        for col, w in zip(cells, colw):
            line += f'  {col[i]:>{w}}'
        print(line)


def _trend_as_blocks(points, labels, cells):
    labw = max([0] + [len(l) for l in labels])
    for i, p in enumerate(points):
        print(f'{p["run_id"][:8]}  {_local(p["started_at"])}  '
              f'{p["display_status"]}  {_human_duration(p["duration_s"])}')
        for lab, col in zip(labels, cells):
            print(f'  {lab:<{labw}}  {col[i]}')


# --------------------------------------------------------------------------- #
# argument parsing / entry point
# --------------------------------------------------------------------------- #

_EPILOG = (
    '`list` is the default command and may be omitted. --db, --stale-after and\n'
    '--json may appear before or after an explicit list / show / overview / trend.\n\n'
    'exit codes: 0 ok, 1 no match, 2 bad arguments, 3 incompatible schema, '
    '4 database error, 5 database file not found.\n\n'
    'examples:\n'
    '  python -m runrecord\n'
    '  python -m runrecord list --script 51_hourly-video-record-add --since 7d\n'
    '  python -m runrecord --json list --status stale\n'
    '  python -m runrecord show --latest --script 51_hourly-video-record-add\n'
    '  python -m runrecord --db ./run-records.sqlite3 show a1b2c3d4\n'
    '  python -m runrecord overview\n'
    '  python -m runrecord trend --script 51_hourly-video-record-add --since 7d\n'
    '  python -m runrecord --json trend --script 51_hourly-video-record-add '
    '--metric record-fetch/total_count --metric duration\n')


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
                    'appear before or after an explicit list / show / overview / '
                    'trend.',
        epilog=_EPILOG, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest='command',
                                metavar='{list,show,overview,trend}')
    sub.add_parser('list', help='list recent runs (default)', add_help=False)
    sub.add_parser('show', help='show one run in full', add_help=False)
    sub.add_parser('overview', help='one row per script: latest run + key metrics',
                   add_help=False)
    sub.add_parser('trend', help="one script's runs as an aligned metric series",
                   add_help=False)
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


def _overview_parser():
    parser = argparse.ArgumentParser(
        prog='python -m runrecord overview',
        description="One row per script_name: its most recent run and up to a "
                    "few of that run's key metrics. A scan aid -- it never "
                    "interprets a metric as good or bad.",
        epilog=_EPILOG, formatter_class=argparse.RawDescriptionHelpFormatter)
    _add_common(parser)
    parser.add_argument('--since', metavar='WHEN',
                        help='only consider runs started at/after this '
                             '(span 24h/7d or ISO); a script whose last run is '
                             'older then drops out of the overview')
    parser.add_argument('--max-metrics', type=int, default=4, metavar='N',
                        help='key metrics to show per script (default: 4)')
    return parser


def _trend_parser():
    parser = argparse.ArgumentParser(
        prog='python -m runrecord trend',
        description="One script's runs aligned into a per-run metric time "
                    "series: one run per row, chosen metric values in columns, "
                    "missing values shown as '-' and never zero-filled. It "
                    "organises recorded facts -- no rates, ratios, thresholds, "
                    "anomaly detection or forecasting.",
        epilog=_EPILOG, formatter_class=argparse.RawDescriptionHelpFormatter)
    _add_common(parser)
    parser.add_argument('--script', metavar='NAME', required=True,
                        help='exact script_name (required scope)')
    parser.add_argument('--metric', metavar='SCOPE/NAME', action='append',
                        help='metric to include; repeatable. "duration" selects '
                             'the built-in duration series. Default: the key '
                             'metrics of the runs in range.')
    parser.add_argument('--since', metavar='WHEN',
                        help='only runs started at/after this (span 24h/7d or ISO)')
    parser.add_argument('--until', metavar='WHEN',
                        help='only runs started at/before this (span or ISO)')
    parser.add_argument('--limit', type=int, default=20, metavar='N',
                        help='keep the most recent N runs in the window, then '
                             'render them oldest -> newest (default: 20)')
    parser.add_argument('--layout', choices=('auto', 'table', 'blocks'),
                        default='auto',
                        help='auto (default) picks the aligned table or per-run '
                             'blocks by terminal width')
    return parser


def _dispatch(args):
    db_path = _resolve_db_path(args.db)
    conn = _connect_ro(db_path)
    try:
        db_version = _check_schema(conn)
        if args.command == 'show':
            return _cmd_show(args, conn, db_path, db_version)
        if args.command == 'overview':
            return _cmd_overview(args, conn, db_path, db_version)
        if args.command == 'trend':
            return _cmd_trend(args, conn, db_path, db_version)
        return _cmd_list(args, conn, db_path, db_version)
    finally:
        conn.close()


# options (on any command) that consume the following token as their value;
# used only to skip over `--opt value` while locating the command token
_COMMANDS = ('list', 'show', 'overview', 'trend')
_VALUE_OPTS = frozenset((
    '--db', '--stale-after', '--script', '--status', '--since', '--until',
    '--limit', '--metric', '--max-metrics', '--layout',
))


def _split_command(argv):
    """
    Find an explicit ``list`` / ``show`` / ``overview`` / ``trend`` token
    anywhere before the first bare positional, and return
    ``(command, argv_without_that_token)``.

    This lets the common options (--db, --stale-after, --json) sit on either
    side of the command: everything is then handed to one flat per-command
    parser that accepts those options regardless of position.
    """
    i = 0
    while i < len(argv):
        tok = argv[i]
        if tok == '--':
            break
        if tok in _COMMANDS:
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
    parser = {
        'show': _show_parser,
        'overview': _overview_parser,
        'trend': _trend_parser,
    }.get(command, _list_parser)()

    args = parser.parse_args(rest)
    args.command = command
    if getattr(args, 'limit', 0) < 0:
        parser.error('--limit must be non-negative')
    if args.stale_after < 0:
        parser.error('--stale-after must be non-negative')
    if getattr(args, 'max_metrics', 0) < 0:
        parser.error('--max-metrics must be non-negative')

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

    if command == 'trend':
        parsed = []
        for raw in (args.metric or []):
            try:
                parsed.append(_parse_metric_arg(raw))
            except QueryError as e:
                parser.error(str(e))
        args.metrics = parsed or None

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
