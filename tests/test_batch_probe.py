"""
Unit tests for tools/batch_probe.py -- the reusable batch-worker probe.

No network: URL resolution, the trimmed-envelope predicate, percentile math,
the perf-record parser (mocked requests), and the summary aggregator are all
exercised offline.

Run from the repo root:  python -m unittest discover -s tests
"""

import io
import json
import os
import sys
import unittest
from contextlib import redirect_stdout
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from tools import batch_probe as bp
except ImportError as e:  # pragma: no cover
    raise unittest.SkipTest(f"batch_probe deps unavailable: {e}")


class Args:
    def __init__(self, **kw):
        self.url = None
        self.endpoints = "/nonexistent/endpoints.json"
        self.__dict__.update(kw)


class TestUrlResolution(unittest.TestCase):
    def test_explicit_url_wins(self):
        a = Args(url="http://explicit.example/")
        with mock.patch.dict(os.environ, {"TDD_BATCH_URL": "http://env.example/"}):
            self.assertEqual(bp.resolve_batch_url(a), "http://explicit.example/")

    def test_env_beats_endpoints_file(self):
        a = Args()
        with mock.patch.dict(os.environ, {"TDD_BATCH_URL": "http://env.example/"}):
            self.assertEqual(bp.resolve_batch_url(a), "http://env.example/")

    def test_endpoints_file_last_resort(self):
        eps = {"get_video_view_trimmed_batch": {"workers": ["http://from-file.example/"]}}
        with mock.patch.object(bp, "_load_endpoints", return_value=eps):
            with mock.patch.dict(os.environ, {}, clear=True):
                a = Args()
                self.assertEqual(bp.resolve_batch_url(a), "http://from-file.example/")

    def test_no_source_raises(self):
        with mock.patch.object(bp, "_load_endpoints", return_value={}):
            with mock.patch.dict(os.environ, {}, clear=True):
                with self.assertRaises(SystemExit):
                    bp.resolve_batch_url(Args())

    def test_module_contains_no_lambda_url(self):
        # the probe must never embed a real worker URL
        from pathlib import Path
        src = Path(bp.__file__).read_text()
        self.assertNotIn("lambda-url", src)
        self.assertNotIn("on.aws", src)
        self.assertNotIn(".amazonaws.com", src)


class TestTrimmedEnvelope(unittest.TestCase):
    def test_accepts_trimmed_shape(self):
        body = {"code": 0, "message": "0", "ttl": 1,
                "data": {"bvid": "BV1", "aid": 1, "stat": {}}}
        self.assertTrue(bp._is_trimmed_envelope(body))

    def test_rejects_full_view_passthrough(self):
        body = {"code": 0, "message": "0", "ttl": 1,
                "data": {"bvid": "BV1", "aid": 1, "stat": {}, "title": "x", "owner": {}}}
        self.assertFalse(bp._is_trimmed_envelope(body))

    def test_rejects_error_passthrough(self):
        self.assertFalse(bp._is_trimmed_envelope({"code": -404, "message": "no", "ttl": 1}))

    def test_rejects_non_dict(self):
        self.assertFalse(bp._is_trimmed_envelope("nope"))


class TestPercentile(unittest.TestCase):
    def test_basic(self):
        xs = list(range(1, 101))
        self.assertEqual(bp._pctl(xs, 0.5), 50.5)
        self.assertEqual(bp._pctl(xs, 0.0), 1)
        self.assertEqual(bp._pctl(xs, 1.0), 100)

    def test_empty(self):
        self.assertIsNone(bp._pctl([], 0.5))


class TestOneBatchRaw(unittest.TestCase):
    def _mock_requests(self, status, payload):
        m = mock.Mock()
        m.status_code = status
        m.json.return_value = payload
        return mock.patch.object(bp.requests, "get", return_value=m)

    def test_parses_item_ms_and_max(self):
        payload = {"v": 1, "requested": 3, "results": [
            {"aid": "1", "ms": 120, "kind": "json", "status": 200},
            {"aid": "2", "ms": 900, "kind": "json", "status": 200},
            {"aid": "3", "ms": 300, "kind": "item_timeout"},
        ]}
        with self._mock_requests(200, payload):
            rec = bp._one_batch_raw("http://x/", [1, 2, 3])
        self.assertEqual(rec["outcome"], "ok")
        self.assertEqual(rec["max_item_ms"], 900)
        self.assertEqual(rec["item_ms"], [120, 900, 300])
        self.assertEqual(rec["kinds"], ["json", "json", "item_timeout"])

    def test_http_400_is_http_error(self):
        with self._mock_requests(400, {"v": 1, "error": "missing aids parameter"}):
            rec = bp._one_batch_raw("http://x/", [1])
        self.assertEqual(rec["outcome"], "http_error")
        self.assertEqual(rec["http_status"], 400)

    def test_exception_captured(self):
        with mock.patch.object(bp.requests, "get", side_effect=RuntimeError("boom")):
            rec = bp._one_batch_raw("http://x/", [1])
        self.assertEqual(rec["outcome"], "exception")
        self.assertIn("boom", rec["error"])


class TestSummarize(unittest.TestCase):
    def test_mixed_records_no_crash(self):
        recs = [
            {"n": 10, "wall_ms": 700, "outcome": "ok", "max_item_ms": 640,
             "item_ms": [100, 640, 200], "kinds": ["json", "json", "json"]},
            {"n": 10, "wall_ms": 5000, "outcome": "whole_batch_failure",
             "error": "ResponseError"},
            {"n": 20, "wall_ms": 900, "outcome": "ok", "max_item_ms": 880,
             "item_ms": [880], "kinds": ["json"]},
        ]
        buf = io.StringIO()
        with redirect_stdout(buf):
            bp._summarize(recs)
        out = buf.getvalue()
        self.assertIn("N=10", out)
        self.assertIn("N=20", out)
        self.assertIn("whole_batch_failure", out)


class TestRateLimiter(unittest.TestCase):
    def test_zero_rate_is_noop(self):
        rl = bp._RateLimiter(0)
        rl.wait()  # must not raise or block

    def test_positive_rate_spaces_calls(self):
        import time
        rl = bp._RateLimiter(50)  # 20ms gap
        t0 = time.perf_counter()
        for _ in range(5):
            rl.wait()
        self.assertGreaterEqual(time.perf_counter() - t0, 0.06)


if __name__ == "__main__":
    unittest.main()
