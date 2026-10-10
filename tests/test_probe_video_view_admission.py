"""Contract checks for the read-only worker admission probe."""

from contextlib import redirect_stdout
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread
import csv
import io
import json
import unittest
from argparse import Namespace

from tools.probe_video_view_admission import ProbeTracker, main, run_probe, sample_aids


class ViewHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        aid = int(self.path.split('aid=')[-1])
        body = json.dumps({
            'code': 0, 'message': '0', 'ttl': 1,
            'data': {'bvid': 'BV1xx', 'aid': aid,
                     'stat': {key: 1 for key in (
                         'aid', 'view', 'danmaku', 'reply', 'favorite', 'coin',
                         'share', 'now_rank', 'his_rank', 'like', 'dislike')}}
        }).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass


class AdmissionProbeTest(unittest.TestCase):
    def test_sample_is_deterministic_and_requires_enough_rows(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / 'aids.csv'
            with path.open('w', newline='') as stream:
                writer = csv.writer(stream)
                writer.writerow(['aid', 'view'])
                writer.writerows((aid, 1) for aid in range(100))
            self.assertEqual(sample_aids(path, 10, 7), sample_aids(path, 10, 7))
            self.assertEqual(len(set(sample_aids(path, 10, 7))), 10)
            with self.assertRaises(ValueError):
                sample_aids(path, 101, 7)

    def test_probe_uses_two_services_and_reports_aggregates_only(self):
        server = ThreadingHTTPServer(('127.0.0.1', 0), ViewHandler)
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with TemporaryDirectory() as directory:
                root = Path(directory)
                url = f'http://127.0.0.1:{server.server_port}/'
                config = root / 'endpoints.json'
                config.write_text(json.dumps({
                    'get_video_view_trimmed': {
                        'workers': [{'id': 'baseline', 'url': url,
                                     'platform': 'local', 'weight': 1,
                                     'enabled': True}],
                        'user_agents': ['probe-test-agent'],
                    }
                }))
                candidate = root / 'candidate.txt'
                candidate.write_text(url)
                aids = root / 'aids.csv'
                with aids.open('w', newline='') as stream:
                    writer = csv.writer(stream)
                    writer.writerow(['aid'])
                    writer.writerows((aid,) for aid in range(10, 20))
                output = io.StringIO()
                with redirect_stdout(output):
                    exit_code = main([
                        '--endpoints', str(config), '--baseline-id', 'baseline',
                        '--candidate-url-file', str(candidate),
                        '--aid-csv', str(aids), '--per-arm', '5',
                        '--rate', '100', '--concurrency', '4',
                    ])
                result = json.loads(output.getvalue())
                self.assertEqual(exit_code, 0)
                self.assertEqual(result['scheduled'], 10)
                for arm in ('baseline', 'candidate'):
                    self.assertEqual(result['arms'][arm]['n1'], 5)
                    self.assertEqual(result['arms'][arm]['p'], 0)
                    self.assertEqual(result['arms'][arm]['calls'], {'ok': 5})
                    self.assertEqual(result['trial1_windows'][0]['arms'][arm]['n1'], 5)
                self.assertNotIn(url, output.getvalue())
                self.assertNotIn('probe-test-agent', output.getvalue())
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

    def test_probe_stops_when_candidate_has_high_rejection(self):
        tracker = ProbeTracker({'baseline': 'aws', 'candidate': 'gcp'})

        class FakeService:
            def __init__(self, worker, fails):
                self.worker = worker
                self.fails = fails

            def get_video_view_trimmed(self, params):
                tracker.record('get_video_view_trimmed', self.worker, 1,
                               'http_412' if self.fails else 'ok')
                if self.fails:
                    raise RuntimeError('rejected')

        args = Namespace(rate=10000, concurrency=20, seed=1,
                         stop_after=10, stop_p=0.20, stop_gap=0.10)
        result = run_probe(
            args, {'baseline': FakeService('aws', False),
                   'candidate': FakeService('gcp', True)},
            list(range(1000)), {'baseline': 'aws', 'candidate': 'gcp'},
            tracker)
        self.assertEqual(result['stop_reason'], 'candidate_p_stop')
        self.assertLess(result['scheduled'], 1000)
        self.assertEqual(result['arms']['baseline']['p'], 0)
        self.assertEqual(result['arms']['candidate']['p'], 1)


if __name__ == '__main__':
    unittest.main()
