"""
Tests for the read-only run-record web page.

Run from the repo root:

    python tests/test_run_record_web.py
    # or
    python -m unittest discover -s tests

Stdlib only. A real ``ThreadingHTTPServer`` is bound on 127.0.0.1:0 in a
background thread and driven with ``http.client``; the server is exercised
end to end (status codes, headers, rendered HTML and JSON) against a temp
database.

Layout under test:

* ``GET /``               -- per-script overview (health banner + one row per
                             script_name, latest run + a few key metrics).
* ``GET /script/<name>``  -- one script's runs aligned into a per-run metric
                             time series with SVG line charts and a table.
* ``GET /runs``           -- the recent-runs stream with filters.
* ``GET /run/<id>``       -- one run in full.
* ``GET /healthz``        -- OK / DEGRADED.
"""

import http.client
import json
import os
import sys
import tempfile
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from runrecord import RunRecorder, schema, web  # noqa: E402
from runrecord._sqlite import sqlite3  # noqa: E402


class FakeStat:
    def __init__(self, total_count, condition):
        self.total_count = total_count
        self.condition = condition


def _insert_running(path, run_id, script_name, started_at):
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            'INSERT INTO run (run_id, script_name, host, code_version, '
            'started_at, finished_at, status) VALUES (?, ?, ?, ?, ?, NULL, ?)',
            (run_id, script_name, 'testhost', 'abc1234', started_at, 'running'))
        conn.commit()
    finally:
        conn.close()


def _backdate(path, run_id, started_at):
    conn = sqlite3.connect(path)
    try:
        conn.execute('UPDATE run SET started_at = ? WHERE run_id = ?',
                     (started_at, run_id))
        conn.commit()
    finally:
        conn.close()


def _set_span(path, run_id, started_at, finished_at):
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            'UPDATE run SET started_at = ?, finished_at = ? WHERE run_id = ?',
            (started_at, finished_at, run_id))
        conn.commit()
    finally:
        conn.close()


class _ServerFixture(unittest.TestCase):
    """Bring up the web server against a controlled database."""

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.db = os.path.join(self.dir, 'run-records.sqlite3')
        self.populate()

        self.httpd = web.build_server('127.0.0.1', 0, self.db,
                                      web.DEFAULT_STALE_AFTER_S, quiet=True)
        self.port = self.httpd.server_address[1]
        self.thread = threading.Thread(
            target=self.httpd.serve_forever, kwargs={'poll_interval': 0.05},
            daemon=True)
        self.thread.start()
        self.addCleanup(self._shutdown)

    def _shutdown(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=5)

    def populate(self):
        """Override to seed the database. Default: one succeeded run."""
        rec = RunRecorder.start('15_update-video-info', db_path=self.db)
        rec.add_job_stat_metrics('video-update', FakeStat(120, {'update_exception': 2}))
        rec.finish('succeeded')

    # -- helpers ------------------------------------------------------- #

    def get(self, path, method='GET'):
        conn = http.client.HTTPConnection('127.0.0.1', self.port, timeout=5)
        try:
            conn.request(method, path)
            resp = conn.getresponse()
            return resp.status, dict(resp.getheaders()), resp.read().decode('utf-8')
        finally:
            conn.close()

    def json_get(self, path):
        status, _, body = self.get(path)
        return status, json.loads(body)


# --------------------------------------------------------------------------- #
# GET / -- per-script overview
# --------------------------------------------------------------------------- #

class OverviewTest(_ServerFixture):
    def populate(self):
        # 15_ ran twice; the overview must show only the newer run
        old = RunRecorder.start('15_update-video-info', db_path=self.db)
        old.add_job_stat_metrics('video-update', FakeStat(90, {'update_exception': 9}))
        old.finish('succeeded')
        _backdate(self.db, old.run_id,
                  (datetime.now(timezone.utc) - timedelta(days=3)).isoformat())

        new = RunRecorder.start('15_update-video-info', db_path=self.db)
        new.add_job_stat_metrics(
            'video-update',
            FakeStat(120, {'0_update': 118, 'update_exception': 2, 'other_junk': 7}))
        new.finish('succeeded')
        self.new_15 = new.run_id

        RunRecorder.start('62_add-evocalrank-video', db_path=self.db).finish('failed')

    def test_one_row_per_script_latest_run(self):
        status, _, body = self.get('/')
        self.assertEqual(status, 200)
        self.assertIn('<h1>run records</h1>', body)
        # one row each, linking to the per-script page
        self.assertIn('<a href="/script/15_update-video-info">', body)
        self.assertIn('<a href="/script/62_add-evocalrank-video">', body)
        # the newer 15_ run's numbers, not the older one's
        self.assertIn('video-update/total_count=120', body)
        self.assertNotIn('update_exception=9', body)

    def test_key_metrics_are_inline_not_columns_and_capped(self):
        # non-key metric never shown; a small cap is honoured
        _, _, body = self.get('/?max_metrics=1')
        self.assertIn('video-update/total_count=120', body)
        self.assertNotIn('0_update', body)          # not key
        self.assertNotIn('other_junk', body)        # not key
        self.assertNotIn('update_exception=2', body)  # capped out at 1
        # there is no per-metric column header
        self.assertNotIn('<th>video-update/total_count</th>', body)

    def test_since_drops_scripts_whose_latest_run_is_old(self):
        old = RunRecorder.start('71_add-sprint-video-record', db_path=self.db)
        old.finish('succeeded')
        _backdate(self.db, old.run_id,
                  (datetime.now(timezone.utc) - timedelta(days=10)).isoformat())
        _, _, body = self.get('/?since=2d')
        self.assertNotIn('71_add-sprint-video-record', body)
        self.assertIn('15_update-video-info', body)

    def test_health_banner_still_present(self):
        _, _, body = self.get('/')
        self.assertIn('1 of 2 script(s) need attention', body)

    def test_json_shape(self):
        status, payload = self.json_get('/?format=json')
        self.assertEqual(status, 200)
        self.assertEqual(payload['schema_version'], schema.SCHEMA_VERSION)
        self.assertEqual(payload['count'], 2)
        self.assertEqual(payload['unhealthy'], ['62_add-evocalrank-video'])
        names = [s['script_name'] for s in payload['scripts']]
        self.assertEqual(names, ['15_update-video-info', '62_add-evocalrank-video'])
        s15 = payload['scripts'][0]
        self.assertEqual(s15['run_id'], self.new_15)
        km = {(m['scope'], m['name']): m['value'] for m in s15['key_metrics']}
        self.assertEqual(km[('video-update', 'total_count')], 120)
        self.assertNotIn(('video-update', '0_update'), km)
        # full run context is preserved (AC: aggregation must not lose it)
        self.assertIn('host', s15)
        self.assertIn('metrics', s15)          # grouped metrics still there
        self.assertNotIn('logs', s15)

    def test_bad_since_is_400(self):
        status, _, _ = self.get('/?since=not-a-time')
        self.assertEqual(status, 400)

    def test_bad_max_metrics_is_400(self):
        status, _, _ = self.get('/?max_metrics=-1')
        self.assertEqual(status, 400)


class EmptyOverviewTest(unittest.TestCase):
    def test_empty_database_renders_and_json_counts_zero(self):
        d = tempfile.mkdtemp()
        db = os.path.join(d, 'run-records.sqlite3')
        RunRecorder.start('seed', db_path=db).finish('succeeded')
        # wipe the row but keep the schema
        conn = sqlite3.connect(db)
        conn.execute('DELETE FROM run')
        conn.commit()
        conn.close()

        httpd = web.build_server('127.0.0.1', 0, db, web.DEFAULT_STALE_AFTER_S,
                                 quiet=True)
        port = httpd.server_address[1]
        t = threading.Thread(target=httpd.serve_forever,
                             kwargs={'poll_interval': 0.05}, daemon=True)
        t.start()
        try:
            c = http.client.HTTPConnection('127.0.0.1', port, timeout=5)
            c.request('GET', '/')
            r = c.getresponse()
            body = r.read().decode('utf-8')
            self.assertEqual(r.status, 200)
            self.assertIn('no runs recorded', body)
            c.close()

            c = http.client.HTTPConnection('127.0.0.1', port, timeout=5)
            c.request('GET', '/?format=json')
            r = c.getresponse()
            payload = json.loads(r.read().decode('utf-8'))
            self.assertEqual(payload['count'], 0)
            self.assertEqual(payload['scripts'], [])
            c.close()
        finally:
            httpd.shutdown()
            httpd.server_close()
            t.join(timeout=5)


# --------------------------------------------------------------------------- #
# GET /script/<name> -- single-script trend
# --------------------------------------------------------------------------- #

class ScriptTrendTest(_ServerFixture):
    SCRIPT = '51_hourly-video-record-add'

    def populate(self):
        base = datetime.now(timezone.utc) - timedelta(days=25)
        self.run_ids = []
        for i in range(25):
            rec = RunRecorder.start(self.SCRIPT, db_path=self.db)
            _backdate(self.db, rec.run_id, (base + timedelta(days=i)).isoformat())
            if i != 12:  # run 12 records no metrics -> a gap, not a zero
                rec.add_job_stat_metrics(
                    'record-fetch',
                    FakeStat(1000 + i * 10, {'other_exception': i}))
                rec.add_job_stat_metrics(
                    'record-video-update',
                    FakeStat(1000 + i * 10, {'update_exception': 0}))
            if i == 20:
                rec.finish('failed')
            elif i == 24:
                pass  # left running; far in the past -> stale
            else:
                rec.finish('succeeded')
            self.run_ids.append(rec.run_id)

    def test_default_page_has_charts_and_table(self):
        status, _, body = self.get(f'/script/{self.SCRIPT}')
        self.assertEqual(status, 200)
        self.assertIn(f'<h1>{self.SCRIPT}</h1>', body)
        self.assertIn('<svg class="chart"', body)
        self.assertIn('magnitude only', body)       # neutral framing
        self.assertNotIn('good', body.lower().split('footer')[0])
        # default = key metrics + built-in duration
        self.assertIn('record-fetch/total_count', body)
        self.assertIn('run/duration_s', body)

    def test_default_limit_keeps_the_newest_20_runs_oldest_first(self):
        _, payload = self.json_get(f'/script/{self.SCRIPT}?format=json')
        self.assertEqual(payload['count'], 20)
        starts = [p['started_at'] for p in payload['points']]
        self.assertEqual(starts, sorted(starts))          # oldest -> newest
        got = {p['run_id'] for p in payload['points']}
        self.assertEqual(got, set(self.run_ids[5:]))      # oldest 5 dropped
        self.assertNotIn('order', payload['query'])

    def test_missing_run_is_a_gap_not_a_zero(self):
        _, payload = self.json_get(
            f'/script/{self.SCRIPT}?metric=record-fetch/other_exception&format=json')
        by_id = {p['run_id']: p for p in payload['points']}
        gap = by_id[self.run_ids[12]]
        self.assertNotIn('record-fetch', gap['values'])   # absent, never 0
        # and the HTML table shows '-' for that run, never '0'
        _, _, body = self.get(
            f'/script/{self.SCRIPT}?metric=record-fetch/other_exception')
        tail = body.split(
            f'<code>{self.run_ids[12][:12]}</code>')[1].split('</tr>')[0]
        self.assertIn('<td class="num">-</td>', tail)
        self.assertNotIn('>0<', tail)

    def test_non_succeeded_points_are_distinguishable(self):
        _, _, body = self.get(f'/script/{self.SCRIPT}')
        self.assertIn('class="dot-open"', body)           # failed / stale marker
        self.assertIn('class="dot"', body)                # succeeded marker
        _, payload = self.json_get(f'/script/{self.SCRIPT}?format=json')
        kinds = {p['display_status'] for p in payload['points']}
        self.assertIn('failed', kinds)
        self.assertIn('stale', kinds)

    def test_metric_selection_and_duration_builtin(self):
        _, payload = self.json_get(
            f'/script/{self.SCRIPT}'
            f'?metric=record-fetch/other_exception&metric=duration&format=json')
        ids = [(s['scope'], s['name']) for s in payload['series']]
        self.assertEqual(ids, [('record-fetch', 'other_exception'),
                               ('run', 'duration_s')])
        for p in payload['points']:
            if p['duration_s'] is not None:
                self.assertEqual(p['values']['run']['duration_s'], p['duration_s'])

    def test_same_name_different_scope_not_merged(self):
        _, payload = self.json_get(
            f'/script/{self.SCRIPT}'
            f'?metric=record-fetch/total_count'
            f'&metric=record-video-update/total_count&format=json')
        ids = [(s['scope'], s['name']) for s in payload['series']]
        self.assertEqual(ids, [('record-fetch', 'total_count'),
                               ('record-video-update', 'total_count')])

    def test_bad_metric_is_400(self):
        status, _, _ = self.get(f'/script/{self.SCRIPT}?metric=nogscope')
        self.assertEqual(status, 400)

    def test_unknown_script_is_404(self):
        status, _, _ = self.get('/script/does-not-exist')
        self.assertEqual(status, 404)

    def test_empty_window_is_200_with_message(self):
        status, _, body = self.get(f'/script/{self.SCRIPT}?since=2100-01-01')
        self.assertEqual(status, 200)
        self.assertIn('no runs in the selected window', body)

    def test_limit_param(self):
        _, payload = self.json_get(f'/script/{self.SCRIPT}?limit=3&format=json')
        self.assertEqual(payload['count'], 3)

    def test_json_point_contract(self):
        _, payload = self.json_get(f'/script/{self.SCRIPT}?format=json')
        p = payload['points'][0]
        for key in ('run_id', 'started_at', 'finished_at', 'duration_s',
                    'status', 'display_status', 'stale', 'host', 'code_version',
                    'values'):
            self.assertIn(key, p)


class TrendDurationNotDoubledTest(_ServerFixture):
    """Duration appears once in the table (fixed column) and once as a chart;
    the HTML table never gets a second raw run/duration_s column, and the
    chart summary is human-readable -- but JSON keeps the series untouched."""

    SCRIPT = '51_hourly-video-record-add'

    def populate(self):
        base = datetime.now(timezone.utc) - timedelta(days=3)
        for i in range(3):
            rec = RunRecorder.start(self.SCRIPT, db_path=self.db)
            rec.add_job_stat_metrics(
                'record-fetch', FakeStat(1000 + i, {'other_exception': i}))
            rec.finish('succeeded')
            started = base + timedelta(days=i)
            # controlled, minute-scale durations: 10m, 13m30s, 17m
            finished = started + timedelta(seconds=600 + i * 210)
            _set_span(self.db, rec.run_id, started.isoformat(),
                      finished.isoformat())

    def test_default_page_still_has_the_duration_chart(self):
        _, _, body = self.get(f'/script/{self.SCRIPT}')
        self.assertIn('<figcaption>run/duration_s (seconds)</figcaption>', body)

    def test_table_has_exactly_one_duration_column(self):
        _, _, body = self.get(f'/script/{self.SCRIPT}')
        table = body.split('<h2>runs</h2>')[1]
        self.assertEqual(table.count('<th class="num">duration</th>'), 1)
        self.assertNotIn('<th class="num">run/duration_s</th>', table)
        # and no raw microsecond seconds leaked into a table cell
        self.assertNotIn('600.0', table)

    def test_duration_chart_summary_is_human_readable(self):
        _, _, body = self.get(f'/script/{self.SCRIPT}')
        fig = body.split(
            '<figcaption>run/duration_s (seconds)</figcaption>')[1].split(
            '</figure>')[0]
        self.assertIn('10m0s &rarr; 17m0s', fig)
        self.assertNotIn('600.0', fig)          # no microsecond float noise
        self.assertNotIn('1020.0', fig)

    def test_other_metric_series_formatting_unchanged(self):
        _, _, body = self.get(f'/script/{self.SCRIPT}')
        # the non-duration series still renders a plain numeric column + summary
        table = body.split('<h2>runs</h2>')[1]
        self.assertIn('<th class="num">record-fetch/other_exception</th>', table)

    def test_json_duration_series_and_values_unchanged(self):
        _, payload = self.json_get(f'/script/{self.SCRIPT}?format=json')
        self.assertIn({'scope': 'run', 'name': 'duration_s',
                       'unit': 'seconds', 'key': False}, payload['series'])
        for p in payload['points']:
            self.assertEqual(p['values']['run']['duration_s'], p['duration_s'])
        self.assertEqual([p['values']['run']['duration_s']
                          for p in payload['points']], [600.0, 810.0, 1020.0])


# --------------------------------------------------------------------------- #
# GET /runs -- the recent-runs stream (was GET /)
# --------------------------------------------------------------------------- #

class RunStreamTest(_ServerFixture):
    def populate(self):
        RunRecorder.start('15_update-video-info', db_path=self.db).finish('succeeded')
        RunRecorder.start('62_add-evocalrank-video', db_path=self.db).finish('failed')
        _insert_running(self.db, 'r' * 32, '51_hourly-video-record-add',
                        datetime.now(timezone.utc).isoformat())
        _insert_running(self.db, 's' * 32, '17_add-member-follower-record',
                        (datetime.now(timezone.utc) - timedelta(hours=9)).isoformat())

    def test_runs_lists_every_script(self):
        status, headers, body = self.get('/runs')
        self.assertEqual(status, 200)
        self.assertIn('text/html', headers['Content-Type'])
        for name in ('15_update-video-info', '62_add-evocalrank-video',
                     '51_hourly-video-record-add', '17_add-member-follower-record'):
            self.assertIn(name, body)

    def test_json_runs_shape(self):
        status, payload = self.json_get('/runs?format=json')
        self.assertEqual(status, 200)
        self.assertEqual(payload['schema_version'], schema.SCHEMA_VERSION)
        self.assertEqual(payload['count'], 4)

    def test_filter_by_script(self):
        _, payload = self.json_get('/runs?script=62_add-evocalrank-video&format=json')
        self.assertEqual([r['script_name'] for r in payload['runs']],
                         ['62_add-evocalrank-video'])

    def test_filter_by_persisted_status(self):
        _, payload = self.json_get('/runs?status=failed&format=json')
        self.assertEqual(payload['count'], 1)
        self.assertEqual(payload['runs'][0]['status'], 'failed')

    def test_filter_by_derived_stale_status(self):
        _, payload = self.json_get('/runs?status=stale&format=json')
        self.assertEqual([r['script_name'] for r in payload['runs']],
                         ['17_add-member-follower-record'])

    def test_filter_by_since_excludes_old_runs(self):
        _, payload = self.json_get('/runs?since=1h&format=json')
        names = {r['script_name'] for r in payload['runs']}
        self.assertNotIn('17_add-member-follower-record', names)  # 9h old
        self.assertIn('15_update-video-info', names)

    def test_bad_since_is_a_400(self):
        status, _, _ = self.get('/runs?since=not-a-time')
        self.assertEqual(status, 400)

    def test_refresh_param_adds_meta_refresh(self):
        _, _, body = self.get('/runs?refresh=15')
        self.assertIn('<meta http-equiv="refresh" content="15">', body)
        _, _, plain = self.get('/runs')
        self.assertNotIn('http-equiv="refresh"', plain)

    def test_healthz_degraded(self):
        status, headers, body = self.get('/healthz')
        self.assertEqual(status, 503)
        self.assertIn('text/plain', headers['Content-Type'])
        self.assertTrue(body.startswith('DEGRADED\n'))
        self.assertIn('62_add-evocalrank-video failed', body)
        self.assertIn('17_add-member-follower-record stale', body)


# --------------------------------------------------------------------------- #
# GET /run/<id> -- detail
# --------------------------------------------------------------------------- #

class DetailTest(_ServerFixture):
    def populate(self):
        rec = RunRecorder.start('15_update-video-info', db_path=self.db)
        rec.add_job_stat_metrics('video-update', FakeStat(120, {'0_update': 118, 'update_exception': 2}))
        rec.finish('succeeded')
        self.run_id = rec.run_id

    def test_detail_html_has_core_metrics_and_scope(self):
        status, headers, body = self.get(f'/run/{self.run_id}')
        self.assertEqual(status, 200)
        self.assertIn('15_update-video-info', body)
        self.assertIn('video-update', body)          # the scope
        self.assertIn('update_exception', body)      # a metric name
        self.assertIn('118', body)                   # a metric value
        # links back into the overview and the per-script trend page
        self.assertIn('href="/script/15_update-video-info"', body)

    def test_detail_by_unique_prefix(self):
        status, _, _ = self.get(f'/run/{self.run_id[:8]}')
        self.assertEqual(status, 200)

    def test_detail_json(self):
        status, payload = self.json_get(f'/run/{self.run_id}?format=json')
        self.assertEqual(status, 200)
        run = payload['run']
        self.assertEqual(run['run_id'], self.run_id)
        self.assertEqual(run['metrics']['video-update'][0]['unit'], 'count')

    def test_unknown_run_is_404(self):
        status, _, _ = self.get('/run/deadbeefdeadbeef')
        self.assertEqual(status, 404)


class RoutingTest(_ServerFixture):
    def test_unknown_path_404(self):
        status, _, _ = self.get('/nope')
        self.assertEqual(status, 404)

    def test_post_is_405(self):
        status, headers, _ = self.get('/', method='POST')
        self.assertEqual(status, 405)
        self.assertEqual(headers.get('Allow'), 'GET, HEAD')

    def test_head_has_no_body_but_correct_length(self):
        status, headers, body = self.get('/', method='HEAD')
        self.assertEqual(status, 200)
        self.assertEqual(body, '')
        self.assertGreater(int(headers['Content-Length']), 0)


class EscapingTest(_ServerFixture):
    def populate(self):
        RunRecorder.start('x<script>alert(1)</script>', db_path=self.db).finish('failed')

    def test_script_name_is_html_escaped_on_overview(self):
        _, _, body = self.get('/')
        self.assertNotIn('<script>alert(1)</script>', body)
        self.assertIn('&lt;script&gt;alert(1)&lt;/script&gt;', body)


class ReadOnlyTest(_ServerFixture):
    def populate(self):
        rec = RunRecorder.start('15_update-video-info', db_path=self.db)
        rec.add_job_stat_metrics('video-update', FakeStat(10, {'update_exception': 0}))
        rec.finish('succeeded')

    def test_requests_never_touch_the_database_file(self):
        before = os.stat(self.db)
        for path in ('/', '/?format=json', '/?since=1h',
                     '/runs', '/runs?status=stale', '/healthz', '/nope',
                     '/run/deadbeef', '/script/15_update-video-info',
                     '/script/15_update-video-info?format=json',
                     '/script/15_update-video-info?metric=duration'):
            self.get(path)
        time.sleep(0.05)
        after = os.stat(self.db)
        self.assertEqual((before.st_size, before.st_mtime_ns),
                         (after.st_size, after.st_mtime_ns))
        for sidecar in ('-wal', '-journal', '-shm'):
            self.assertFalse(os.path.exists(self.db + sidecar),
                             f'{sidecar} sidecar was created')


class MissingDatabaseTest(unittest.TestCase):
    def test_absent_db_renders_503_not_a_traceback(self):
        d = tempfile.mkdtemp()
        missing = os.path.join(d, 'nope.sqlite3')
        httpd = web.build_server('127.0.0.1', 0, missing,
                                 web.DEFAULT_STALE_AFTER_S, quiet=True)
        port = httpd.server_address[1]
        t = threading.Thread(target=httpd.serve_forever,
                             kwargs={'poll_interval': 0.05}, daemon=True)
        t.start()
        try:
            for path in ('/', '/script/x', '/runs'):
                conn = http.client.HTTPConnection('127.0.0.1', port, timeout=5)
                conn.request('GET', path)
                resp = conn.getresponse()
                body = resp.read().decode('utf-8')
                self.assertEqual(resp.status, 503)
                self.assertNotIn('Traceback', body)
                conn.close()
        finally:
            httpd.shutdown()
            httpd.server_close()
            t.join(timeout=5)


class DefaultsTest(unittest.TestCase):
    def test_default_bind_is_loopback(self):
        self.assertEqual(web.DEFAULT_HOST, '127.0.0.1')
        self.assertTrue(web._is_loopback(web.DEFAULT_HOST))
        self.assertFalse(web._is_loopback('0.0.0.0'))

    def test_empty_host_is_not_loopback(self):
        # '' binds every interface -- it must never be classified as loopback
        # (that would skip the exposure warning) and must not be in the set
        self.assertNotIn('', web._LOOPBACK_NAMES)
        self.assertFalse(web._is_loopback(''))
        self.assertFalse(web._is_loopback(None))

    def test_empty_host_argument_is_rejected(self):
        with self.assertRaises(SystemExit) as cm:
            web.main(['--host', ''])
        self.assertEqual(cm.exception.code, 2)  # argparse usage error

    def test_explicit_all_interfaces_is_still_allowed_with_a_warning(self):
        # 0.0.0.0 is the explicit, un-sneaky way to ask for every interface;
        # it is not rejected, only warned about, and it is not "loopback"
        self.assertFalse(web._is_loopback('0.0.0.0'))


if __name__ == '__main__':
    unittest.main(verbosity=2)
