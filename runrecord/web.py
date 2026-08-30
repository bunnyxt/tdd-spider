"""
Read-only web page over the run-record store (BL-0006).

A single-file, standard-library-only server for the daily "is everything
healthy?" glance. It never writes: the database is opened read-only and only
``SELECT`` / ``PRAGMA`` run. All the query logic (schema check, fetch, the
``running``->``stale`` derivation, id-prefix resolution) is reused from
``runrecord.query`` rather than reimplemented.

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
  ``GET /``                  health banner + recent runs. Query params:
                             ``script`` (exact), ``status``
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
"""

import argparse
import html
import json
import socket
import sys
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .recorder import _resolve_db_path, DEFAULT_STALE_AFTER_S
from .query import (
    QueryError,
    EXIT_NO_DB,
    EXIT_NO_MATCH,
    EXIT_SCHEMA,
    EXIT_USAGE,
    _STATUS_CHOICES,
    _check_schema,
    _connect_ro,
    _fetch_runs,
    _human_duration,
    _local,
    _local_tz_label,
    _parse_time,
    _persisted_prefilter,
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
_DEFAULT_LIMIT = 50


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


def _health(conn, stale_after):
    """Latest run per script, bucketed. Feeds both the banner and /healthz."""
    latest = {}
    for rec in _all_records(conn, stale_after):
        latest.setdefault(rec['script_name'], rec)  # rows are DESC by started_at
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


def _detail_record(conn, run_id, stale_after):
    now = datetime.now(timezone.utc)
    row = _resolve_one(conn, run_id)  # raises QueryError on miss / ambiguity
    return _record(row, now=now, stale_after=stale_after, conn=conn, detail=True)


# --------------------------------------------------------------------------- #
# HTML rendering (server-side, no JavaScript)
# --------------------------------------------------------------------------- #

_STYLE = """
:root {
  color-scheme: light dark;
  --fg: #1b1b1b; --bg: #fafafa; --muted: #666; --line: #ddd;
  --card: #fff; --accent: #2563eb;
  --ok: #15803d; --ok-bg: #dcfce7;
  --warn: #b45309; --warn-bg: #fef3c7;
  --bad: #b91c1c; --bad-bg: #fee2e2;
  --run: #6d28d9; --run-bg: #ede9fe;
}
@media (prefers-color-scheme: dark) {
  :root {
    --fg: #e6e6e6; --bg: #16181c; --muted: #9aa0a6; --line: #33363b;
    --card: #1e2126; --accent: #60a5fa;
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
form.filters { margin: 1rem 0; display: flex; flex-wrap: wrap; gap: .5rem; }
form.filters input, form.filters select { padding: .3rem .4rem; font: inherit;
  color: var(--fg); background: var(--card); border: 1px solid var(--line);
  border-radius: 4px; }
form.filters button { padding: .3rem .7rem; font: inherit; cursor: pointer;
  border: 1px solid var(--line); border-radius: 4px; background: var(--card);
  color: var(--fg); }
table { border-collapse: collapse; width: 100%; font-size: .9rem; }
th, td { text-align: left; padding: .4rem .5rem; border-bottom: 1px solid var(--line); }
th { color: var(--muted); font-weight: 600; }
td.num { text-align: right; font-variant-numeric: tabular-nums; }
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
footer { margin-top: 2rem; color: var(--muted); font-size: .8rem; }
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


def _filter_form(params):
    script = _e(params.get('script', ''))
    since = _e(params.get('since', ''))
    limit = _e(params.get('limit', str(_DEFAULT_LIMIT)))
    cur_status = params.get('status', '')
    opts = ['<option value="">any status</option>']
    for s in _STATUS_CHOICES:
        sel = ' selected' if s == cur_status else ''
        opts.append(f'<option value="{s}"{sel}>{s}</option>')
    return (
        '<form class="filters" method="get" action="/">'
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
            f'<td>{_e(r["script_name"])}</td>'
            f'<td>{_pill(r["display_status"])}</td>'
            f'<td class="num">{_e(_human_duration(r["duration_s"]))}</td>'
            f'<td><a href="/run/{_e(r["run_id"])}"><code>{_e(r["run_id"][:12])}</code></a></td>'
            '</tr>')
    return (
        '<table><thead><tr>'
        '<th>started (local)</th><th>script</th><th>status</th>'
        '<th class="num">duration</th><th>run id</th>'
        '</tr></thead><tbody>' + ''.join(rows) + '</tbody></table>')


def _index_html(health, records, params, db_version, refresh):
    meta = (f'schema v{db_version} &middot; {len(records)} run(s) shown &middot; '
            f'times {_local_tz_label()}')
    body = (
        '<h1>run records</h1>'
        f'<div class="meta">{meta}</div>'
        + _banner_html(health)
        + _filter_form(params)
        + '<h2>recent runs</h2>'
        + _runs_table(records))
    return _page('run records', body, refresh=refresh)


def _detail_html(rec, db_version):
    core = [
        ('run id', f'<code>{_e(rec["run_id"])}</code>'),
        ('script', _e(rec['script_name'])),
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
        f'<a href="/">&larr; all runs</a></div>'
        f'<dl class="core">{core_html}</dl>'
        '<h2>metrics</h2>' + metrics_html +
        '<h2>logs</h2>' + logs_html)
    return _page(f'run {rec["run_id"][:12]}', body)


def _fmt_num(value):
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def _error_html(message):
    body = (
        '<h1>run records</h1>'
        '<div class="banner degraded"><div class="headline">unavailable</div>'
        f'<p>{_e(message)}</p></div>')
    return _page('run records - unavailable', body)


# --------------------------------------------------------------------------- #
# JSON rendering (mirrors runrecord.query's document shape)
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
        params = {k: v[0] for k, v in parse_qs(parsed.query).items()}
        try:
            if path == '/':
                self._index(params, body=body)
            elif path == '/healthz':
                self._healthz(body=body)
            elif path.startswith('/run/'):
                self._detail(path[len('/run/'):], params, body=body)
            else:
                self._send(404, b'not found\n', 'text/plain; charset=utf-8',
                           body=body)
        except QueryError as e:
            status = {
                EXIT_NO_DB: 503, EXIT_SCHEMA: 503,
                EXIT_NO_MATCH: 404, EXIT_USAGE: 400,
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

    def _index(self, params, *, body):
        conn, db_version = _open(self.db_path)
        try:
            health = _health(conn, self.stale_after)
            since = _parse_time(params['since']) if params.get('since') else None
            status = params.get('status') or None
            if status is not None and status not in _STATUS_CHOICES:
                raise QueryError(f'unknown status {status!r}')
            records = _all_records(
                conn, self.stale_after,
                script=params.get('script') or None, since=since,
                persisted_status=_persisted_prefilter(status))
            records = _select_by_status(records, status)
            limit = _parse_limit(params.get('limit'))
            records = records[:limit]
        finally:
            conn.close()

        if params.get('format') == 'json':
            payload = {
                'schema_version': db_version,
                'generated_at': datetime.now(timezone.utc).isoformat(),
                'health': {k: v for k, v in health.items() if k != 'unhealthy'},
                'unhealthy': [r['script_name'] for r in health['unhealthy']],
                'count': len(records),
                'runs': records,
            }
            self._send(200, _json_bytes(payload),
                       'application/json; charset=utf-8', body=body)
            return

        refresh = _parse_refresh(params.get('refresh'))
        page = _index_html(health, records, params, db_version, refresh)
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


def _parse_limit(raw):
    if raw is None or raw == '':
        return _DEFAULT_LIMIT
    try:
        n = int(raw)
    except ValueError:
        raise QueryError(f'invalid limit {raw!r}')
    if n < 1:
        raise QueryError('limit must be >= 1')
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
