"""
Tests for the read-only run-record web page (BL-0006).

Run from the repo root:

    python tests/test_run_record_web.py
    # or
    python -m unittest discover -s tests

Stdlib only. A real ``ThreadingHTTPServer`` is bound on 127.0.0.1:0 in a
background thread and driven with ``http.client``; the server is exercised
end to end (status codes, headers, rendered HTML and JSON) against a temp
database.
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


class BasicRenderTest(_ServerFixture):
    def populate(self):
        # a succeeded, a failed, a genuinely-running, and a stale run
        RunRecorder.start('15_update-video-info', db_path=self.db).finish('succeeded')
        RunRecorder.start('62_add-evocalrank-video', db_path=self.db).finish('failed')
        _insert_running(self.db, 'r' * 32, '51_hourly-video-record-add',
                        datetime.now(timezone.utc).isoformat())
        _insert_running(self.db, 's' * 32, '17_add-member-follower-record',
                        (datetime.now(timezone.utc) - timedelta(hours=9)).isoformat())

    def test_index_ok_and_lists_every_script(self):
        status, headers, body = self.get('/')
        self.assertEqual(status, 200)
        self.assertIn('text/html', headers['Content-Type'])
        for name in ('15_update-video-info', '62_add-evocalrank-video',
                     '51_hourly-video-record-add', '17_add-member-follower-record'):
            self.assertIn(name, body)

    def test_index_health_banner_reflects_mixed_state(self):
        _, _, body = self.get('/')
        self.assertIn('2 of 4 script(s) need attention', body)
        # the unhealthy ones are named, the healthy/running ones are not in the list
        self.assertRegex(body, r'62_add-evocalrank-video</a>\s*&mdash;\s*<span class="pill failed"')

    def test_json_index_shape(self):
        status, payload = self.json_get('/?format=json')
        self.assertEqual(status, 200)
        self.assertEqual(payload['schema_version'], schema.SCHEMA_VERSION)
        self.assertEqual(payload['count'], 4)
        self.assertEqual(payload['health']['ok'], 1)
        self.assertEqual(payload['health']['failed'], 1)
        self.assertEqual(payload['health']['stale'], 1)
        self.assertEqual(payload['health']['running'], 1)
        self.assertEqual(sorted(payload['unhealthy']),
                         ['17_add-member-follower-record', '62_add-evocalrank-video'])

    def test_filter_by_script(self):
        _, payload = self.json_get('/?script=62_add-evocalrank-video&format=json')
        self.assertEqual([r['script_name'] for r in payload['runs']],
                         ['62_add-evocalrank-video'])

    def test_filter_by_persisted_status(self):
        _, payload = self.json_get('/?status=failed&format=json')
        self.assertEqual(payload['count'], 1)
        self.assertEqual(payload['runs'][0]['status'], 'failed')

    def test_filter_by_derived_stale_status(self):
        _, payload = self.json_get('/?status=stale&format=json')
        self.assertEqual([r['script_name'] for r in payload['runs']],
                         ['17_add-member-follower-record'])

    def test_filter_by_since_excludes_old_runs(self):
        _, payload = self.json_get('/?since=1h&format=json')
        names = {r['script_name'] for r in payload['runs']}
        self.assertNotIn('17_add-member-follower-record', names)  # 9h old
        self.assertIn('15_update-video-info', names)

    def test_bad_since_is_a_400(self):
        status, _, _ = self.get('/?since=not-a-time')
        self.assertEqual(status, 400)

    def test_healthz_degraded(self):
        status, headers, body = self.get('/healthz')
        self.assertEqual(status, 503)
        self.assertIn('text/plain', headers['Content-Type'])
        self.assertTrue(body.startswith('DEGRADED\n'))
        self.assertIn('62_add-evocalrank-video failed', body)
        self.assertIn('17_add-member-follower-record stale', body)

    def test_refresh_param_adds_meta_refresh(self):
        _, _, body = self.get('/?refresh=15')
        self.assertIn('<meta http-equiv="refresh" content="15">', body)
        _, _, plain = self.get('/')
        self.assertNotIn('http-equiv="refresh"', plain)


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

    def test_script_name_is_html_escaped(self):
        _, _, body = self.get('/')
        self.assertNotIn('<script>alert(1)</script>', body)
        self.assertIn('&lt;script&gt;alert(1)&lt;/script&gt;', body)


class ReadOnlyTest(_ServerFixture):
    def populate(self):
        RunRecorder.start('15_update-video-info', db_path=self.db).finish('succeeded')

    def test_requests_never_touch_the_database_file(self):
        before = os.stat(self.db)
        for path in ('/', '/?format=json', '/healthz', '/nope',
                     '/run/deadbeef', '/?status=stale'):
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
            conn = http.client.HTTPConnection('127.0.0.1', port, timeout=5)
            conn.request('GET', '/')
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
