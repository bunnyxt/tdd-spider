"""
Aligned per-run metric time series over the run-record store (Phase 1.5).

Given one ``script_name`` and a time window, return that script's runs as
*points* (one per run) carrying run context plus the values of a chosen set of
metrics, aligned so a single run is one row across every series.

Read-only: only ``SELECT`` / ``PRAGMA``, never migrates. Tolerates a schema-v1
database (no ``run_metric.is_key`` column) opened read-only -- ``is_key`` then
reads as NULL and the name convention alone decides ``key``.

Everything is scoped to the runs actually returned by the query (after
``since`` / ``until`` / ``order`` / ``limit``): metric discovery, ``key``, unit
and values all reflect those runs. Deliberately minimal -- it organises recorded
facts. No health classification, rates, ratios, anomaly detection, schedule
expectations or per-script configuration.

Unit stability is a producer contract: a metric may be written with a NULL unit
alongside one declared unit, but if a selected ``(scope, name)`` carries more
than one distinct non-null unit across the queried runs, ``fetch_series`` /
``available_metrics`` raise ``query.QueryError`` naming the metric and units
rather than trying to reconcile them.
"""

from datetime import datetime, timezone

from ._sqlite import sqlite3
from .keymetric import is_key_metric
from .query import QueryError, _fetch_runs, _record, _run_query, _FETCH_CAP
from .recorder import DEFAULT_STALE_AFTER_S

__all__ = ['DURATION_SERIES', 'available_metrics', 'default_key_metrics',
           'fetch_series']

# the built-in series derived from the run timestamps: selectable alongside the
# persisted metrics but never stored. Every point already carries ``duration_s``.
DURATION_SERIES = ('run', 'duration_s')


def _has_is_key_column(conn):
    try:
        return any(row[1] == 'is_key'
                   for row in conn.execute('PRAGMA table_info(run_metric)'))
    except sqlite3.DatabaseError:
        return False


def _metric_rows(conn, run_ids):
    """``(run_id, scope, name, value, unit, is_key)`` for the given runs."""
    if not run_ids:
        return []
    col = 'is_key' if _has_is_key_column(conn) else 'NULL'
    placeholders = ','.join('?' * len(run_ids))
    return _run_query(
        conn,
        f'SELECT run_id, scope, name, value, unit, {col} FROM run_metric '
        f'WHERE run_id IN ({placeholders}) ORDER BY run_id',
        list(run_ids))


def _identities(metric_rows):
    """``{(scope, name): {'units': set, 'explicit': int|None}}`` over the rows."""
    out = {}
    for _run_id, scope, name, _value, unit, is_key in metric_rows:
        info = out.setdefault((scope, name), {'units': set(), 'explicit': None})
        if unit is not None:
            info['units'].add(unit)
        if is_key is not None:
            info['explicit'] = int(is_key)   # rows are ordered by run_id
    return out


def _unit(scope, name, units):
    if len(units) > 1:
        raise QueryError(
            f'metric {scope}/{name} has inconsistent units '
            f'{sorted(units)} across the queried runs')
    return next(iter(units)) if units else None


def _identity_row(scope, name, info):
    return {'scope': scope, 'name': name,
            'unit': _unit(scope, name, info['units']),
            'key': is_key_metric(name, info['explicit'])}


def _fetch(conn, script_name, *, since, until, order, limit):
    return _fetch_runs(conn, script=script_name, since=since, until=until,
                       persisted_status=None, order=order,
                       limit=limit if limit is not None else _FETCH_CAP)


def available_metrics(conn, script_name, *, since=None, until=None,
                      order='DESC', limit=None):
    """
    The ``(scope, name, unit, key)`` identities recorded by the queried runs of
    ``script_name``, ordered by ``(scope, name)``. The same name under two
    scopes yields two identities. Raises ``QueryError`` for an inconsistent unit.
    """
    rows = _fetch(conn, script_name, since=since, until=until,
                  order=order, limit=limit)
    idents = _identities(_metric_rows(conn, [r[0] for r in rows]))
    return [_identity_row(scope, name, info)
            for (scope, name), info in sorted(idents.items())]


def default_key_metrics(conn, script_name, **kwargs):
    """``available_metrics`` filtered to the identities that resolve as key."""
    return [row for row in available_metrics(conn, script_name, **kwargs)
            if row['key']]


def _point(rec):
    return {
        'run_id': rec['run_id'],
        'started_at': rec['started_at'],
        'finished_at': rec['finished_at'],
        'duration_s': rec['duration_s'],
        'status': rec['status'],
        'display_status': rec['display_status'],
        'stale': rec['stale'],
        'host': rec['host'],
        'code_version': rec['code_version'],
        'values': {},
    }


def fetch_series(conn, script_name, *, metrics=None, since=None, until=None,
                 order='DESC', limit=None, now=None,
                 stale_after_s=DEFAULT_STALE_AFTER_S):
    """
    Aligned per-run time series for ``script_name``.

    ``metrics`` -- an iterable of ``(scope, name)`` pairs to select, order
    preserved; ``('run', 'duration_s')`` selects the built-in duration series;
    ``None`` selects the queried runs' key metrics. ``since`` / ``until`` bound
    ``started_at``; ``order`` is ``'DESC'`` (default) or ``'ASC'``; ``limit``
    caps the number of runs after ordering.

    Returns::

        {'script_name': str,
         'series': [{'scope', 'name', 'unit', 'key'}, ...],
         'points': [{'run_id', 'started_at', 'finished_at', 'duration_s',
                     'status', 'display_status', 'stale', 'host',
                     'code_version', 'values': {scope: {name: value}}}, ...]}

    Every point always carries ``duration_s`` (``None`` until the run finishes).
    ``values`` holds an entry only for metrics actually recorded on that run --
    an absent metric is distinct from a stored ``0`` and is never zero-filled.
    Raises ``QueryError`` if a selected metric has inconsistent units across the
    queried runs.
    """
    now = now or datetime.now(timezone.utc)
    order = 'ASC' if str(order).upper() == 'ASC' else 'DESC'

    rows = _fetch(conn, script_name, since=since, until=until,
                  order=order, limit=limit)
    points = [_point(_record(r, now=now, stale_after=stale_after_s))
              for r in rows]
    by_id = {p['run_id']: p for p in points}

    metric_rows = _metric_rows(conn, list(by_id))
    idents = _identities(metric_rows)

    if metrics is None:
        selected = [k for k in sorted(idents)
                    if is_key_metric(k[1], idents[k]['explicit'])]
    else:
        selected, seen = [], set()
        for pair in metrics:
            pair = tuple(pair)
            if pair not in seen:
                seen.add(pair)
                selected.append(pair)

    wanted = {p for p in selected if p != DURATION_SERIES}
    for run_id, scope, name, value, _unit, _is_key in metric_rows:
        if (scope, name) in wanted:
            by_id[run_id]['values'].setdefault(scope, {})[name] = value

    if DURATION_SERIES in selected:
        for point in points:
            if point['duration_s'] is not None:
                point['values'].setdefault('run', {})['duration_s'] = \
                    point['duration_s']

    series = []
    for scope, name in selected:
        if (scope, name) == DURATION_SERIES:
            series.append({'scope': 'run', 'name': 'duration_s',
                           'unit': 'seconds', 'key': False})
            continue
        info = idents.get((scope, name), {'units': set(), 'explicit': None})
        series.append(_identity_row(scope, name, info))

    return {'script_name': script_name, 'series': series, 'points': points}
