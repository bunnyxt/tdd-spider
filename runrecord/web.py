"""
Read-only web page over the run-record store.

A single-file, standard-library-only server for the daily "is everything
healthy?" glance and for scanning a metric across a script's runs. It never
writes: the database is opened read-only and only ``SELECT`` / ``PRAGMA`` run.
All the query logic (schema check, fetch, the ``running``->``stale``
derivation, id-prefix resolution, key-metric selection, the aligned per-run
series) is reused from ``runrecord.query`` / ``runrecord.series`` rather than
reimplemented.

Run it on demand, in the foreground, on the box that holds the database::

    python3 -m runrecord.web --host 127.0.0.1 --port 8765 --db data/run-records.sqlite3

and reach it from a workstation over an SSH tunnel::

    ssh -N -L 8765:127.0.0.1:8765 <server>
    # then open http://127.0.0.1:8765/

Ctrl-C stops it. ``--host`` defaults to 127.0.0.1; an empty ``--host`` is
rejected (it would bind every interface), and any non-loopback bind prints a
warning.

Routes:

===========================  =================================================
  ``GET /``                  script overview: health banner + one row per
                             script_name (latest run time, lifecycle, duration,
                             a few key metrics). Query params: ``since``
                             (``24h`` / ``7d`` / ISO; drops a script whose last
                             run is older), ``max_metrics`` (default 4),
                             ``format=json``.
  ``GET /script/<name>``     one script's runs aligned into a per-run metric
                             time series with simple SVG line charts and a
                             per-run table. Query params: ``metric``
                             (repeatable, ``scope/name`` or ``duration``;
                             default = the runs' key metrics + duration),
                             ``since`` / ``until``, ``limit`` (default 20, the
                             newest N in the window, shown oldest first),
                             ``format=json``.
  ``GET /runs``              the recent-runs stream. Query params: ``script``
                             (exact), ``status``
                             (running|succeeded|failed|stale), ``since``
                             (``24h`` / ``7d`` / ISO), ``limit`` (default 50),
                             ``refresh`` (seconds; adds a meta-refresh),
                             ``format=json``.
  ``GET /run/<id>``          one run in full: core fields, derived
                             duration/status, metrics grouped by scope, log
                             paths. ``<id>`` is a full run id or a unique
                             prefix. ``format=json`` supported.
  ``GET /healthz``           ``text/plain`` ``OK`` or ``DEGRADED`` + one line
                             per script whose latest run failed or is stale.
  anything else              404
  non-GET/HEAD               405
===========================  =================================================

A database problem (missing file, schema newer than this code, partially
created) renders a plain error page with a 503/500 status -- never a traceback.

The charts encode only the magnitude of the values that are present. A missing
run is a gap, never a zero; a non-succeeded run is drawn with a hollow marker.
Nothing on the page interprets a line going up or down as good or bad.
"""

import argparse
import html
import json
import socket
import sys
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, quote, unquote, urlparse

from . import series as _series
from .recorder import _resolve_db_path, DEFAULT_STALE_AFTER_S
from .query import (
    QueryError,
    EXIT_DB_ERROR,
    EXIT_NO_DB,
    EXIT_NO_MATCH,
    EXIT_SCHEMA,
    EXIT_USAGE,
    _STATUS_CHOICES,
    _check_schema,
    _connect_ro,
    _explicit_key_flags,
    _fetch_runs,
    _fmt_num,
    _human_duration,
    _local,
    _local_tz_label,
    _parse_metric_arg,
    _parse_time,
    _persisted_prefilter,
    _pick_key_metrics,
    _record,
    _resolve_one,
    _select_by_status,
)

__all__ = ['main', 'build_server', 'DEFAULT_HOST', 'DEFAULT_PORT']

DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = 8765
# An empty host is deliberately NOT here: the socket layer treats '' as "every
# interface", so it must never be classified as loopback (it would silently skip
# the exposure warning). main() rejects an empty --host outright.
_LOOPBACK_NAMES = {'127.0.0.1', '::1', 'localhost'}
_RUNS_LIMIT = 50
_OVERVIEW_MAX_METRICS = 4
_TREND_LIMIT = 20
_DURATION_TOKEN = 'duration'


# --------------------------------------------------------------------------- #
# data assembly (read-only; one fresh connection per request)
# --------------------------------------------------------------------------- #

def _open(db_path):
    """Open read-only and verify the schema. Returns (conn, user_version)."""
    conn = _connect_ro(db_path)
    version = _check_schema(conn)
    return conn, version


def _all_records(conn, stale_after, *, script=None, since=None,
                 persisted_status=None):
    now = datetime.now(timezone.utc)
    rows = _fetch_runs(conn, script=script, since=since,
                       persisted_status=persisted_status, order='DESC')
    return [_record(r, now=now, stale_after=stale_after) for r in rows]


def _latest_per_script(conn, stale_after, *, since=None):
    """The most recent run of every script, newest-first rows deduped by name."""
    latest = {}
    for rec in _all_records(conn, stale_after, since=since):
        latest.setdefault(rec['script_name'], rec)  # rows are DESC by started_at
    return latest


def _health(conn, stale_after):
    """Latest run per script, bucketed. Feeds the banner and /healthz."""
    latest = _latest_per_script(conn, stale_after)
    buckets = {'succeeded': [], 'running': [], 'failed': [], 'stale': []}
    for rec in latest.values():
        buckets.setdefault(rec['display_status'], []).append(rec)
    unhealthy = sorted(
        buckets['failed'] + buckets['stale'],
        key=lambda r: r['script_name'])
    return {
        'scripts': len(latest),
        'ok': len(buckets['succeeded']),
        'running': len(buckets['running']),
        'failed': len(buckets['failed']),
        'stale': len(buckets['stale']),
        'unhealthy': unhealthy,
        'degraded': bool(unhealthy),
    }


def _overview_records(conn, stale_after, *, since, max_metrics):
    """One record per script: its latest run + that run's key metrics.

    Mirrors the CLI ``overview`` command: newest run per script_name, each with
    up to ``max_metrics`` key metrics rendered inline (never dynamic columns).
    """
    now = datetime.now(timezone.utc)
    rows = _fetch_runs(conn, since=since, order='DESC')
    latest = {}
    for r in rows:
        latest.setdefault(r[1], r)  # r[1] == script_name; DESC keeps the newest
    out = []
    for name in sorted(latest):
        rec = _record(latest[name], now=now, stale_after=stale_after,
                      conn=conn, detail=True)
        flags = _explicit_key_flags(conn, rec['run_id'])
        rec['key_metrics'] = _pick_key_metrics(rec['metrics'], flags, max_metrics)
        out.append(rec)
    return out


def _script_seen(conn, name):
    """True if ``name`` has ever recorded a run (any window)."""
    return bool(_fetch_runs(conn, script=name, order='DESC', limit=1))


def _detail_record(conn, run_id, stale_after):
    now = datetime.now(timezone.utc)
    row = _resolve_one(conn, run_id)  # raises QueryError on miss / ambiguity
    return _record(row, now=now, stale_after=stale_after, conn=conn, detail=True)


def _trend_data(conn, name, stale_after, *, metric_tokens, since, until, limit):
    """
    Assemble the aligned per-run series for one script.

    ``metric_tokens`` is the raw list of ``?metric`` values (already stripped);
    empty means "the runs' key metrics plus the built-in duration series".
    Points come back oldest -> newest so the charts and table read
    left-to-right in time. Mirrors the CLI ``trend`` command.
    """
    now = datetime.now(timezone.utc)
    selected = [_parse_metric_arg(tok) for tok in metric_tokens]
    if not selected:
        selected = [(m['scope'], m['name']) for m in _series.default_key_metrics(
            conn, name, since=since, until=until, limit=limit)]
        selected.append(_series.DURATION_SERIES)

    result = _series.fetch_series(
        conn, name, metrics=selected, since=since, until=until,
        order='DESC', limit=limit, now=now, stale_after_s=stale_after)
    result['points'].reverse()
    return result


def _available_choices(conn, name, *, since, until, limit):
    """The metric identities offered by the picker: recorded metrics + duration."""
    rows = _series.available_metrics(
        conn, name, since=since, until=until, limit=limit)
    choices = [(f'{r["scope"]}/{r["name"]}', r['unit'], r['key']) for r in rows]
    choices.append((_DURATION_TOKEN, 'seconds', False))
    return choices


# --------------------------------------------------------------------------- #
# HTML rendering (server-side, no JavaScript)
# --------------------------------------------------------------------------- #

_STYLE = """
:root {
  color-scheme: light dark;
  --fg: #1b1b1b; --bg: #fafafa; --muted: #666; --line: #ddd;
  --card: #fff; --accent: #2563eb; --chart: #475569;
  --ok: #15803d; --ok-bg: #dcfce7;
  --warn: #b45309; --warn-bg: #fef3c7;
  --bad: #b91c1c; --bad-bg: #fee2e2;
  --run: #6d28d9; --run-bg: #ede9fe;
}
@media (prefers-color-scheme: dark) {
  :root {
    --fg: #e6e6e6; --bg: #16181c; --muted: #9aa0a6; --line: #33363b;
    --card: #1e2126; --accent: #60a5fa; --chart: #94a3b8;
    --ok: #4ade80; --ok-bg: #16351f;
    --warn: #fbbf24; --warn-bg: #3a2c0b;
    --bad: #f87171; --bad-bg: #3a1616;
    --run: #c4b5fd; --run-bg: #2a1f47;
  }
}
* { box-sizing: border-box; }
body { margin: 0; padding: 1.5rem; font: 14px/1.5 -apple-system, BlinkMacSystemFont,
  "Segoe UI", Roboto, Helvetica, Arial, sans-serif; color: var(--fg);
  background: var(--bg); }
main { max-width: 60rem; margin: 0 auto; }
h1 { font-size: 1.25rem; margin: 0 0 .25rem; }
h2 { font-size: 1rem; margin: 1.5rem 0 .5rem; }
a { color: var(--accent); text-decoration: none; }
a:hover { text-decoration: underline; }
.meta { color: var(--muted); font-size: .85rem; margin-bottom: 1rem; }
.banner { border: 1px solid var(--line); border-left: 4px solid var(--ok);
  background: var(--card); border-radius: 6px; padding: .75rem 1rem; }
.banner.degraded { border-left-color: var(--bad); }
.banner .headline { font-weight: 600; }
.banner ul { margin: .5rem 0 0; padding-left: 1.25rem; }
.counts { color: var(--muted); font-size: .9rem; }
form.filters { margin: 1rem 0; display: flex; flex-wrap: wrap; gap: .5rem; align-items: center; }
form.filters input, form.filters select { padding: .3rem .4rem; font: inherit;
  color: var(--fg); background: var(--card); border: 1px solid var(--line);
  border-radius: 4px; }
form.filters button { padding: .3rem .7rem; font: inherit; cursor: pointer;
  border: 1px solid var(--line); border-radius: 4px; background: var(--card);
  color: var(--fg); }
.tablewrap { overflow-x: auto; }
table { border-collapse: collapse; width: 100%; font-size: .9rem; }
th, td { text-align: left; padding: .4rem .5rem; border-bottom: 1px solid var(--line);
  white-space: nowrap; }
th { color: var(--muted); font-weight: 600; }
td.num { text-align: right; font-variant-numeric: tabular-nums; }
td.metrics { color: var(--muted); font-size: .85rem; }
td.metrics .tok { margin-right: .8rem; display: inline-block; }
tr:hover td { background: var(--card); }
.pill { display: inline-block; padding: .05rem .45rem; border-radius: 999px;
  font-size: .8rem; font-weight: 600; }
.pill.succeeded { color: var(--ok); background: var(--ok-bg); }
.pill.failed { color: var(--bad); background: var(--bad-bg); }
.pill.stale { color: var(--warn); background: var(--warn-bg); }
.pill.running { color: var(--run); background: var(--run-bg); }
dl.core { display: grid; grid-template-columns: max-content 1fr; gap: .25rem .75rem;
  margin: .5rem 0; }
dl.core dt { color: var(--muted); }
dl.core dd { margin: 0; font-variant-numeric: tabular-nums; }
.scope { margin: .5rem 0; }
.scope > .name { font-weight: 600; margin-bottom: .2rem; }
code { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: .85em; }
.empty { color: var(--muted); font-style: italic; }
.hint { color: var(--muted); font-size: .8rem; margin: .25rem 0 1rem; }
form.picker { margin: 1rem 0; }
form.picker .opts { display: flex; flex-wrap: wrap; gap: .35rem .9rem; margin: .4rem 0; }
form.picker label { font-size: .85rem; }
form.picker button { padding: .3rem .7rem; font: inherit; cursor: pointer;
  border: 1px solid var(--line); border-radius: 4px; background: var(--card);
  color: var(--fg); }
figure.series { margin: 1rem 0; border: 1px solid var(--line); border-radius: 6px;
  padding: .5rem .75rem; background: var(--card); }
figure.series figcaption { font-size: .85rem; }
figure.series .sub { color: var(--muted); font-size: .8rem; }
svg.chart { width: 100%; max-width: 32rem; height: auto; display: block;
  margin: .35rem 0; }
svg.chart .line { fill: none; stroke: var(--chart); stroke-width: 1.5; }
svg.chart .dot { fill: var(--chart); }
svg.chart .dot-open { fill: var(--bg); stroke: var(--chart); stroke-width: 1; }
svg.chart .frame { fill: none; stroke: var(--line); stroke-width: 1; }
.legend { color: var(--muted); font-size: .8rem; margin: .5rem 0 1rem; }
footer { margin-top: 2rem; color: var(--muted); font-size: .8rem; }
@media (max-width: 40rem) {
  body { padding: 1rem; }
  th, td { padding: .35rem .4rem; }
}
"""


def _page(title, body, *, refresh=None):
    head = [f'<title>{html.escape(title)}</title>',
            '<meta charset="utf-8">',
            '<meta name="viewport" content="width=device-width, initial-scale=1">',
            f'<style>{_STYLE}</style>']
    if refresh:
        head.append(f'<meta http-equiv="refresh" content="{int(refresh)}">')
    return (
        '<!doctype html>\n<html lang="en">\n<head>\n' + '\n'.join(head) +
        '\n</head>\n<body>\n<main>\n' + body +
        '\n<footer>tdd-spider run records &middot; read-only</footer>\n'
        '</main>\n</body>\n</html>\n')


def _pill(display_status):
    s = html.escape(display_status)
    return f'<span class="pill {s}">{s}</span>'


def _e(value):
    return html.escape('' if value is None else str(value))


def _script_href(name):
    return '/script/' + quote(name, safe='')


def _banner_html(health):
    cls = 'banner degraded' if health['degraded'] else 'banner'
    if health['degraded']:
        items = ''.join(
            f'<li><a href="/run/{_e(r["run_id"])}">{_e(r["script_name"])}</a> '
            f'&mdash; {_pill(r["display_status"])} '
            f'(started {_e(_local(r["started_at"]))})</li>'
            for r in health['unhealthy'])
        headline = (f'{len(health["unhealthy"])} of {health["scripts"]} '
                    f'script(s) need attention')
        body = f'<div class="headline">{headline}</div><ul>{items}</ul>'
    else:
        headline = (f'all {health["scripts"]} script(s) OK'
                    if health['scripts'] else 'no runs recorded yet')
        body = f'<div class="headline">{headline}</div>'
    counts = (f'<div class="counts">ok {health["ok"]} &middot; '
              f'running {health["running"]} &middot; failed {health["failed"]} '
              f'&middot; stale {health["stale"]}</div>')
    return f'<div class="{cls}">{body}{counts}</div>'


# -- overview (GET /) ------------------------------------------------------- #

def _overview_since_form(params):
    since = _e(params.get('since', ''))
    mm = _e(params.get('max_metrics', str(_OVERVIEW_MAX_METRICS)))
    return (
        '<form class="filters" method="get" action="/">'
        f'<input type="text" name="since" placeholder="since (24h / 7d / ISO)" value="{since}">'
        f'<label>key metrics <input type="number" name="max_metrics" min="0" '
        f'value="{mm}" style="width:4rem"></label>'
        '<button type="submit">apply</button>'
        '</form>')


def _key_metric_tokens(key_metrics):
    if not key_metrics:
        return '<span class="empty">no key metrics</span>'
    return ''.join(
        f'<span class="tok">{_e(m["scope"])}/{_e(m["name"])}='
        f'{_e(_fmt_num(m["value"]))}</span>'
        for m in key_metrics)


def _overview_table(records):
    if not records:
        return '<p class="empty">no runs recorded</p>'
    rows = []
    for r in records:
        rows.append(
            '<tr>'
            f'<td><a href="{_e(_script_href(r["script_name"]))}">'
            f'{_e(r["script_name"])}</a></td>'
            f'<td>{_e(_local(r["started_at"]))}</td>'
            f'<td>{_pill(r["display_status"])}</td>'
            f'<td class="num">{_e(_human_duration(r["duration_s"]))}</td>'
            f'<td class="metrics">{_key_metric_tokens(r["key_metrics"])}</td>'
            '</tr>')
    return (
        '<div class="tablewrap"><table><thead><tr>'
        '<th>script</th><th>latest run (local)</th><th>status</th>'
        '<th class="num">duration</th><th>key metrics</th>'
        '</tr></thead><tbody>' + ''.join(rows) + '</tbody></table></div>')


def _overview_html(health, records, params, db_version):
    meta = (f'schema v{db_version} &middot; {len(records)} script(s) &middot; '
            f'times {_local_tz_label()} &middot; '
            f'<a href="/runs">recent runs stream &rarr;</a>')
    body = (
        '<h1>run records</h1>'
        f'<div class="meta">{meta}</div>'
        + _banner_html(health)
        + _overview_since_form(params)
        + '<h2>scripts</h2>'
        + _overview_table(records))
    return _page('run records', body)


# -- single-script trend (GET /script/<name>) ----------------------------- #

def _metric_picker(name, choices, selected_tokens, params):
    """A no-JS checkbox form; submitting reloads /script/<name> with ?metric=."""
    hidden = []
    for k in ('since', 'until', 'limit'):
        if params.get(k):
            hidden.append(
                f'<input type="hidden" name="{k}" value="{_e(params[k])}">')
    opts = []
    for token, unit, key in choices:
        checked = ' checked' if token in selected_tokens else ''
        tag = ' [key]' if key else ''
        u = f' ({_e(unit)})' if unit else ''
        opts.append(
            f'<label><input type="checkbox" name="metric" value="{_e(token)}"{checked}> '
            f'{_e(token)}{u}{tag}</label>')
    if not opts:
        return '<p class="hint">this script has recorded no numeric metrics.</p>'
    return (
        f'<form class="picker" method="get" action="{_e(_script_href(name))}">'
        + ''.join(hidden)
        + '<div class="opts">' + ''.join(opts) + '</div>'
        + '<button type="submit">update charts</button>'
        + ' <span class="hint">no selection = the runs\' key metrics + duration</span>'
        + '</form>')


def _svg_chart(values, statuses, *, width=280, height=64, pad=8):
    """
    A fixed-viewBox line sketch of ``values`` over the runs, oldest -> newest.

    Encodes only the min..max range of the values that are present. A missing
    run breaks the line (never a zero); a non-succeeded run gets a hollow
    marker. No axis labels, no colour meaning.
    """
    n = len(values)
    present = [v for v in values if v is not None]
    frame = (f'<rect class="frame" x="0.5" y="0.5" '
             f'width="{width - 1}" height="{height - 1}" rx="3"/>')
    if not present or n == 0:
        return (f'<svg class="chart" viewBox="0 0 {width} {height}" '
                f'role="img" aria-label="no values">{frame}'
                f'<text x="{width / 2}" y="{height / 2 + 4}" '
                f'text-anchor="middle" fill="var(--muted)" '
                f'font-size="10">no values</text></svg>')

    lo, hi = min(present), max(present)
    span = (hi - lo) or 1.0
    inner_w, inner_h = width - 2 * pad, height - 2 * pad

    def px(i):
        return pad + (inner_w * i / (n - 1)) if n > 1 else width / 2

    def py(v):
        if hi == lo:
            return height / 2
        return pad + inner_h * (1 - (v - lo) / span)

    segments, cur = [], []
    for i, v in enumerate(values):
        if v is None:
            if len(cur) > 1:
                segments.append(cur)
            cur = []
        else:
            cur.append((px(i), py(v)))
    if len(cur) > 1:
        segments.append(cur)

    lines = ''.join(
        '<polyline class="line" points="'
        + ' '.join(f'{x:.1f},{y:.1f}' for x, y in seg) + '"/>'
        for seg in segments)

    dots = []
    for i, (v, st) in enumerate(zip(values, statuses)):
        if v is None:
            continue
        cx, cy = px(i), py(v)
        if st == 'succeeded':
            dots.append(f'<circle class="dot" cx="{cx:.1f}" cy="{cy:.1f}" r="2"/>')
        else:
            dots.append(f'<circle class="dot-open" cx="{cx:.1f}" cy="{cy:.1f}" '
                        f'r="2.5"/>')

    return (f'<svg class="chart" viewBox="0 0 {width} {height}" role="img" '
            f'aria-label="magnitude trend, {len(present)} of {n} runs have a value">'
            f'{frame}{lines}{"".join(dots)}</svg>')


def _series_label(s):
    unit = f' ({_e(s["unit"])})' if s.get('unit') else ''
    tag = ' [key]' if s.get('key') else ''
    return f'{_e(s["scope"])}/{_e(s["name"])}{unit}{tag}'


def _trend_value(point, scope, name):
    return point['values'].get(scope, {}).get(name)


def _trend_figures(series_defs, points):
    if not series_defs:
        return ('<p class="empty">no key metrics recorded by these runs &mdash; '
                'pick one or more metrics above.</p>')
    figs = []
    for s in series_defs:
        raw = [_trend_value(p, s['scope'], s['name']) for p in points]
        statuses = [p['display_status'] for p in points]
        present = [v for v in raw if v is not None]
        missing = sum(1 for v in raw if v is None)
        # the built-in duration series reads as wall-clock time, not raw seconds
        fmt = (_human_duration
               if (s['scope'], s['name']) == _series.DURATION_SERIES
               else _fmt_num)
        ends = (f'{fmt(present[0])} &rarr; {fmt(present[-1])}'
                if present else 'no values')
        sub = ends + (f' &middot; {missing} missing' if missing else '')
        figs.append(
            '<figure class="series">'
            f'<figcaption>{_series_label(s)}</figcaption>'
            + _svg_chart(raw, statuses)
            + f'<div class="sub">{sub}</div>'
            '</figure>')
    return ''.join(figs)


def _trend_table(series_defs, points):
    # the fixed "duration" column already shows the built-in duration series
    # (human-readable); don't add a second raw run/duration_s column for it
    cols = [s for s in series_defs
            if (s['scope'], s['name']) != _series.DURATION_SERIES]
    head = ['<th>run</th><th>started (local)</th><th>status</th>',
            '<th class="num">duration</th>']
    for s in cols:
        head.append(f'<th class="num">{_e(s["scope"])}/{_e(s["name"])}</th>')
    rows = []
    for p in points:
        cells = [
            f'<td><a href="/run/{_e(p["run_id"])}"><code>'
            f'{_e(p["run_id"][:12])}</code></a></td>',
            f'<td>{_e(_local(p["started_at"]))}</td>',
            f'<td>{_pill(p["display_status"])}</td>',
            f'<td class="num">{_e(_human_duration(p["duration_s"]))}</td>',
        ]
        for s in cols:
            cells.append(
                f'<td class="num">'
                f'{_e(_fmt_num(_trend_value(p, s["scope"], s["name"])))}</td>')
        rows.append('<tr>' + ''.join(cells) + '</tr>')
    return (
        '<div class="tablewrap"><table><thead><tr>' + ''.join(head)
        + '</tr></thead><tbody>' + ''.join(rows) + '</tbody></table></div>')


_LEGEND = (
    '<div class="legend">&#9679; finished run &middot; &#9711; '
    'running / failed / stale run &middot; a gap is a run with no value '
    '(never zero). The line shows magnitude only &mdash; not a health signal.'
    '</div>')


def _trend_html(name, result, choices, selected_tokens, params, db_version):
    points = result['points']
    series_defs = result['series']
    since = params.get('since') or '-'
    until = params.get('until') or 'now'
    limit = params.get('limit', str(_TREND_LIMIT))
    meta = (f'schema v{db_version} &middot; {len(points)} run(s) &middot; '
            f'times {_local_tz_label()} &middot; <a href="/">&larr; overview</a> '
            f'&middot; <a href="/runs?script={quote(name, safe="")}">runs</a>')
    window = (f'window: since {_e(since)} &middot; until {_e(until)} &middot; '
              f'limit {_e(limit)} newest, shown oldest first')

    body = [
        f'<h1>{_e(name)}</h1>',
        f'<div class="meta">{meta}</div>',
        f'<div class="hint">{window}</div>',
        _trend_window_form(name, params),
        _metric_picker(name, choices, selected_tokens, params),
    ]
    if not points:
        body.append('<p class="empty">no runs in the selected window.</p>')
        return _page(f'{name} trend', ''.join(body))

    body.append(_LEGEND)
    body.append(_trend_figures(series_defs, points))
    body.append('<h2>runs</h2>')
    body.append(_trend_table(series_defs, points))
    return _page(f'{name} trend', ''.join(body))


def _trend_window_form(name, params):
    since = _e(params.get('since', ''))
    until = _e(params.get('until', ''))
    limit = _e(params.get('limit', str(_TREND_LIMIT)))
    metrics = ''.join(
        f'<input type="hidden" name="metric" value="{_e(m)}">'
        for m in params.get('_metric_list', []))
    return (
        f'<form class="filters" method="get" action="{_e(_script_href(name))}">'
        + metrics
        + f'<input type="text" name="since" placeholder="since (24h / 7d / ISO)" value="{since}">'
        + f'<input type="text" name="until" placeholder="until" value="{until}">'
        + f'<input type="number" name="limit" min="1" value="{limit}" style="width:5rem">'
        + '<button type="submit">apply</button>'
        + '</form>')


# -- recent-runs stream (GET /runs) -------------------------------------- #

def _filter_form(params):
    script = _e(params.get('script', ''))
    since = _e(params.get('since', ''))
    limit = _e(params.get('limit', str(_RUNS_LIMIT)))
    cur_status = params.get('status', '')
    opts = ['<option value="">any status</option>']
    for s in _STATUS_CHOICES:
        sel = ' selected' if s == cur_status else ''
        opts.append(f'<option value="{s}"{sel}>{s}</option>')
    return (
        '<form class="filters" method="get" action="/runs">'
        f'<input type="text" name="script" placeholder="script name" value="{script}">'
        f'<select name="status">{"".join(opts)}</select>'
        f'<input type="text" name="since" placeholder="since (24h / 7d / ISO)" value="{since}">'
        f'<input type="number" name="limit" min="1" value="{limit}" style="width:6rem">'
        '<button type="submit">filter</button>'
        '</form>')


def _runs_table(records):
    if not records:
        return '<p class="empty">no matching runs</p>'
    rows = []
    for r in records:
        rows.append(
            '<tr>'
            f'<td>{_e(_local(r["started_at"]))}</td>'
            f'<td><a href="{_e(_script_href(r["script_name"]))}">'
            f'{_e(r["script_name"])}</a></td>'
            f'<td>{_pill(r["display_status"])}</td>'
            f'<td class="num">{_e(_human_duration(r["duration_s"]))}</td>'
            f'<td><a href="/run/{_e(r["run_id"])}"><code>{_e(r["run_id"][:12])}</code></a></td>'
            '</tr>')
    return (
        '<div class="tablewrap"><table><thead><tr>'
        '<th>started (local)</th><th>script</th><th>status</th>'
        '<th class="num">duration</th><th>run id</th>'
        '</tr></thead><tbody>' + ''.join(rows) + '</tbody></table></div>')


def _runs_html(records, params, db_version, refresh):
    meta = (f'schema v{db_version} &middot; {len(records)} run(s) shown &middot; '
            f'times {_local_tz_label()} &middot; <a href="/">&larr; overview</a>')
    body = (
        '<h1>recent runs</h1>'
        f'<div class="meta">{meta}</div>'
        + _filter_form(params)
        + _runs_table(records))
    return _page('recent runs', body, refresh=refresh)


# -- run detail (GET /run/<id>) ---------------------------------------- #

def _detail_html(rec, db_version):
    core = [
        ('run id', f'<code>{_e(rec["run_id"])}</code>'),
        ('script', f'<a href="{_e(_script_href(rec["script_name"]))}">'
                   f'{_e(rec["script_name"])}</a>'),
        ('host', _e(rec['host'])),
        ('code version', _e(rec['code_version'] or '-')),
        ('started', _e(f'{_local(rec["started_at"])}  ({rec["started_at"]})')),
        ('finished', _e(f'{_local(rec["finished_at"])}  ({rec["finished_at"]})')
         if rec['finished_at'] else '-'),
        ('duration', _e(_human_duration(rec['duration_s']))),
        ('status', f'{_e(rec["status"])} (persisted)'),
        ('display', _pill(rec['display_status'])),
    ]
    core_html = ''.join(f'<dt>{k}</dt><dd>{v}</dd>' for k, v in core)

    metrics = rec.get('metrics') or {}
    if metrics:
        blocks = []
        for scope in sorted(metrics):
            entries = ''.join(
                '<tr>'
                f'<td>{_e(m["name"])}</td>'
                f'<td class="num">{_e(_fmt_num(m["value"]))}</td>'
                f'<td>{_e(m["unit"] or "")}</td>'
                '</tr>'
                for m in metrics[scope])
            blocks.append(
                f'<div class="scope"><div class="name">{_e(scope)}</div>'
                f'<table><tbody>{entries}</tbody></table></div>')
        metrics_html = ''.join(blocks)
    else:
        metrics_html = '<p class="empty">no metrics</p>'

    logs = rec.get('logs') or []
    if logs:
        logs_html = '<table><tbody>' + ''.join(
            f'<tr><td>{_e(l["level"])}</td><td><code>{_e(l["path"])}</code></td></tr>'
            for l in logs) + '</tbody></table>'
    else:
        logs_html = '<p class="empty">no log locations recorded</p>'

    body = (
        f'<h1>{_e(rec["script_name"])}</h1>'
        f'<div class="meta">schema v{db_version} &middot; '
        f'<a href="/">&larr; overview</a> &middot; '
        f'<a href="{_e(_script_href(rec["script_name"]))}">trend</a> &middot; '
        f'<a href="/runs">runs</a></div>'
        f'<dl class="core">{core_html}</dl>'
        '<h2>metrics</h2>' + metrics_html +
        '<h2>logs</h2>' + logs_html)
    return _page(f'run {rec["run_id"][:12]}', body)


def _error_html(message):
    body = (
        '<h1>run records</h1>'
        '<div class="banner degraded"><div class="headline">unavailable</div>'
        f'<p>{_e(message)}</p></div>')
    return _page('run records - unavailable', body)


# --------------------------------------------------------------------------- #
# JSON rendering (mirrors runrecord.query / runrecord.series document shapes)
# --------------------------------------------------------------------------- #

def _json_bytes(payload):
    return json.dumps(payload, indent=2, ensure_ascii=False,
                      sort_keys=True).encode('utf-8')


# --------------------------------------------------------------------------- #
# request handling
# --------------------------------------------------------------------------- #

class _Handler(BaseHTTPRequestHandler):
    server_version = 'runrecord-web'
    protocol_version = 'HTTP/1.1'

    # set by build_server()
    db_path = None
    stale_after = DEFAULT_STALE_AFTER_S
    quiet = False

    def log_message(self, fmt, *args):
        if self.quiet:
            return
        sys.stderr.write('%s - %s\n' % (
            self.log_date_time_string(), fmt % args))

    # -- dispatch ---------------------------------------------------------- #

    def do_GET(self):
        self._route(body=True)

    def do_HEAD(self):
        self._route(body=False)

    def _method_not_allowed(self):
        self._send(405, b'method not allowed\n', 'text/plain; charset=utf-8',
                   body=True, extra={'Allow': 'GET, HEAD'})

    do_POST = do_PUT = do_DELETE = do_PATCH = do_OPTIONS = _method_not_allowed

    def _route(self, *, body):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip('/') or '/'
        multi = parse_qs(parsed.query)
        params = {k: v[0] for k, v in multi.items()}
        try:
            if path == '/':
                self._overview(params, body=body)
            elif path == '/runs':
                self._runs(params, body=body)
            elif path == '/healthz':
                self._healthz(body=body)
            elif path.startswith('/script/'):
                self._script(path[len('/script/'):], params, multi, body=body)
            elif path.startswith('/run/'):
                self._detail(path[len('/run/'):], params, body=body)
            else:
                self._send(404, b'not found\n', 'text/plain; charset=utf-8',
                           body=body)
        except QueryError as e:
            status = {
                EXIT_NO_DB: 503, EXIT_SCHEMA: 503,
                EXIT_NO_MATCH: 404, EXIT_USAGE: 400,
                EXIT_DB_ERROR: 503,
            }.get(e.exit_code, 500)
            if params.get('format') == 'json':
                self._send(status, _json_bytes({'error': str(e)}),
                           'application/json; charset=utf-8', body=body)
            else:
                self._send(status, _error_html(str(e)).encode('utf-8'),
                           'text/html; charset=utf-8', body=body)
        except BrokenPipeError:
            pass
        except Exception as e:  # never leak a traceback to the client
            self.log_message('unhandled error: %r', e)
            self._send(500, b'internal error\n', 'text/plain; charset=utf-8',
                       body=body)

    # -- routes ---------------------------------------------------------- #

    def _overview(self, params, *, body):
        conn, db_version = _open(self.db_path)
        try:
            health = _health(conn, self.stale_after)
            since = _parse_time(params['since']) if params.get('since') else None
            max_metrics = _parse_max_metrics(params.get('max_metrics'))
            records = _overview_records(
                conn, self.stale_after, since=since, max_metrics=max_metrics)
        finally:
            conn.close()

        if params.get('format') == 'json':
            payload = {
                'schema_version': db_version,
                'generated_at': datetime.now(timezone.utc).isoformat(),
                'health': {k: v for k, v in health.items() if k != 'unhealthy'},
                'unhealthy': [r['script_name'] for r in health['unhealthy']],
                'count': len(records),
                'scripts': [
                    {**{k: v for k, v in r.items() if k != 'logs'}}
                    for r in records
                ],
            }
            self._send(200, _json_bytes(payload),
                       'application/json; charset=utf-8', body=body)
            return

        page = _overview_html(health, records, params, db_version)
        self._send(200, page.encode('utf-8'), 'text/html; charset=utf-8',
                   body=body)

    def _script(self, name, params, multi, *, body):
        name = unquote(name.split('?')[0]).strip()
        if not name:
            raise QueryError('no script name given', EXIT_NO_MATCH)

        metric_tokens = [t.strip() for t in multi.get('metric', []) if t.strip()]
        params = dict(params)
        params['_metric_list'] = metric_tokens

        conn, db_version = _open(self.db_path)
        try:
            if not _script_seen(conn, name):
                raise QueryError(f'no runs recorded for script {name!r}',
                                 EXIT_NO_MATCH)
            since = _parse_time(params['since']) if params.get('since') else None
            until = _parse_time(params['until']) if params.get('until') else None
            limit = _parse_limit(params.get('limit'), _TREND_LIMIT)
            result = _trend_data(
                conn, name, self.stale_after, metric_tokens=metric_tokens,
                since=since, until=until, limit=limit)
            choices = _available_choices(
                conn, name, since=since, until=until, limit=limit)
        finally:
            conn.close()

        selected_tokens = set(metric_tokens) or {
            f'{s["scope"]}/{s["name"]}' if (s['scope'], s['name'])
            != _series.DURATION_SERIES else _DURATION_TOKEN
            for s in result['series']}

        if params.get('format') == 'json':
            payload = {
                'schema_version': db_version,
                'generated_at': datetime.now(timezone.utc).isoformat(),
                'query': {
                    'command': 'trend',
                    'script': name,
                    'since': since.isoformat() if since else None,
                    'until': until.isoformat() if until else None,
                    'limit': limit,
                    'metrics': ([list(_parse_metric_arg(t)) for t in metric_tokens]
                                or None),
                    'stale_after_s': self.stale_after,
                },
                'script_name': result['script_name'],
                'series': result['series'],
                'count': len(result['points']),
                'points': result['points'],
            }
            self._send(200, _json_bytes(payload),
                       'application/json; charset=utf-8', body=body)
            return

        page = _trend_html(name, result, choices, selected_tokens, params,
                           db_version)
        self._send(200, page.encode('utf-8'), 'text/html; charset=utf-8',
                   body=body)

    def _runs(self, params, *, body):
        conn, db_version = _open(self.db_path)
        try:
            since = _parse_time(params['since']) if params.get('since') else None
            status = params.get('status') or None
            if status is not None and status not in _STATUS_CHOICES:
                raise QueryError(f'unknown status {status!r}', EXIT_USAGE)
            records = _all_records(
                conn, self.stale_after,
                script=params.get('script') or None, since=since,
                persisted_status=_persisted_prefilter(status))
            records = _select_by_status(records, status)
            limit = _parse_limit(params.get('limit'), _RUNS_LIMIT)
            records = records[:limit]
        finally:
            conn.close()

        if params.get('format') == 'json':
            payload = {
                'schema_version': db_version,
                'generated_at': datetime.now(timezone.utc).isoformat(),
                'count': len(records),
                'runs': records,
            }
            self._send(200, _json_bytes(payload),
                       'application/json; charset=utf-8', body=body)
            return

        refresh = _parse_refresh(params.get('refresh'))
        page = _runs_html(records, params, db_version, refresh)
        self._send(200, page.encode('utf-8'), 'text/html; charset=utf-8',
                   body=body)

    def _detail(self, run_id, params, *, body):
        run_id = run_id.split('?')[0].strip()
        conn, db_version = _open(self.db_path)
        try:
            rec = _detail_record(conn, run_id, self.stale_after)
        finally:
            conn.close()

        if params.get('format') == 'json':
            self._send(200, _json_bytes(
                {'schema_version': db_version, 'run': rec}),
                'application/json; charset=utf-8', body=body)
            return
        self._send(200, _detail_html(rec, db_version).encode('utf-8'),
                   'text/html; charset=utf-8', body=body)

    def _healthz(self, *, body):
        conn, _ = _open(self.db_path)
        try:
            health = _health(conn, self.stale_after)
        finally:
            conn.close()
        if not health['degraded']:
            text = 'OK\n'
        else:
            lines = '\n'.join(
                f'{r["script_name"]} {r["display_status"]}'
                for r in health['unhealthy'])
            text = f'DEGRADED\n{lines}\n'
        self._send(200 if not health['degraded'] else 503,
                   text.encode('utf-8'), 'text/plain; charset=utf-8', body=body)

    # -- response helper ------------------------------------------------- #

    def _send(self, status, payload, content_type, *, body, extra=None):
        # one request per connection -- this is a low-traffic localhost tool and
        # closing avoids any keep-alive bookkeeping
        self.close_connection = True
        self.send_response(status)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(payload)))
        self.send_header('Cache-Control', 'no-store')
        self.send_header('Connection', 'close')
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        if body:
            self.wfile.write(payload)


def _parse_limit(raw, default):
    if raw is None or raw == '':
        return default
    try:
        n = int(raw)
    except ValueError:
        raise QueryError(f'invalid limit {raw!r}', EXIT_USAGE)
    if n < 1:
        raise QueryError('limit must be >= 1', EXIT_USAGE)
    return n


def _parse_max_metrics(raw):
    if raw is None or raw == '':
        return _OVERVIEW_MAX_METRICS
    try:
        n = int(raw)
    except ValueError:
        raise QueryError(f'invalid max_metrics {raw!r}', EXIT_USAGE)
    if n < 0:
        raise QueryError('max_metrics must be >= 0', EXIT_USAGE)
    return n


def _parse_refresh(raw):
    if raw is None or raw == '':
        return None
    try:
        n = int(raw)
    except ValueError:
        return None
    return n if n > 0 else None


# --------------------------------------------------------------------------- #
# server + CLI
# --------------------------------------------------------------------------- #

def build_server(host, port, db_path, stale_after, *, quiet=False):
    handler = type('_BoundHandler', (_Handler,), {
        'db_path': db_path,
        'stale_after': stale_after,
        'quiet': quiet,
    })
    httpd = ThreadingHTTPServer((host, port), handler)
    httpd.daemon_threads = True
    return httpd


def _is_loopback(host):
    if not host:
        # '' (and None) means "every interface" to the socket layer -- the one
        # case we must be sure NOT to treat as loopback
        return False
    if host in _LOOPBACK_NAMES:
        return True
    try:
        return all(
            info[4][0].startswith('127.') or info[4][0] in ('::1',)
            for info in socket.getaddrinfo(host, None))
    except socket.gaierror:
        return False


def _build_parser():
    p = argparse.ArgumentParser(
        prog='python -m runrecord.web',
        description='Read-only web page over the run-record store. Runs on the '
                    'host that holds the database; reach it over an SSH tunnel.',
        epilog='example:\n'
               '  # on the server\n'
               '  python3 -m runrecord.web --host 127.0.0.1 --port 8765 '
               '--db data/run-records.sqlite3\n'
               '\n  # on the workstation\n'
               '  ssh -N -L 8765:127.0.0.1:8765 <server>\n'
               '  # then open http://127.0.0.1:8765/\n',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--host', default=DEFAULT_HOST,
                   help=f'bind address (default: {DEFAULT_HOST})')
    p.add_argument('--port', type=int, default=DEFAULT_PORT,
                   help=f'bind port (default: {DEFAULT_PORT})')
    p.add_argument('--db', metavar='PATH', default=None,
                   help='run-records SQLite file (default: $TDD_RUN_RECORD_DB '
                        'or <repo>/data/run-records.sqlite3)')
    p.add_argument('--stale-after', type=int, default=DEFAULT_STALE_AFTER_S,
                   metavar='SECONDS',
                   help='a "running" run older than this shows as "stale" '
                        f'(default: {DEFAULT_STALE_AFTER_S})')
    return p


def main(argv=None):
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.stale_after < 0:
        parser.error('--stale-after must be non-negative')
    if args.host == '':
        # an empty host binds every interface; there is no safe silent reading
        # of it, and 0.0.0.0 is the explicit way to ask for that (with a warning)
        parser.error("--host must not be empty (an empty host binds every "
                     "interface); pass 127.0.0.1, or 0.0.0.0 to bind all")
    db_path = _resolve_db_path(args.db)

    if not _is_loopback(args.host):
        sys.stderr.write(
            f'WARNING: binding {args.host}:{args.port} -- this server is '
            'read-only but has no auth; prefer localhost + an SSH tunnel.\n')

    httpd = build_server(args.host, args.port, db_path, args.stale_after)
    sys.stderr.write(
        f'runrecord-web serving {db_path}\n'
        f'  http://{args.host}:{args.port}/   (Ctrl-C to stop)\n')
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        sys.stderr.write('\nstopping\n')
    finally:
        httpd.shutdown()
        httpd.server_close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
