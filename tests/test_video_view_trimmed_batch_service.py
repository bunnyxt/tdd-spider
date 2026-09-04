"""
Tests for ``Service.get_video_view_trimmed_batch`` -- the client side of the
batch worker contract (service/workers/video_view/aws_lambda_batch.mjs, v1).

The HTTP layer (``Service._get``) is stubbed; every test feeds a canned
top-level response and asserts the three-tier error model:

- per-item outcomes (ok / CodeError / retryable ResponseError), including the
  rule that a top-level HTTP 200 proves nothing about the items and an item is
  only trusted when kind == json AND status == 200 AND body.code == 0 AND the
  identity checks pass -- notably, an upstream non-2xx carrying a code == 0
  JSON body must NOT produce a record;
- whole-batch failures (transport / unconfigured endpoint) raising
  ResponseError;
- contract/identity violations raising MisalignmentError (kill-switch food).

Run from the repo root:

    python -m unittest discover -s tests
"""

import logging
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from service import Service, ResponseError, CodeError, MisalignmentError  # noqa: E402
    from service.Service import \
        VIDEO_VIEW_TRIMMED_BATCH_READ_TIMEOUT_S, VIDEO_VIEW_TRIMMED_BATCH_DEADLINE_S  # noqa: E402
    from util import a2b  # noqa: E402
except ImportError as e:  # pragma: no cover -- e.g. requests not installed
    raise unittest.SkipTest(f'service dependencies unavailable: {e}')

BATCH_ENDPOINTS = {
    'get_video_view_trimmed_batch': {'workers': ['http://batch-worker.invalid/']},
}


def make_service(endpoints=None):
    return Service(mode='worker',
                   endpoints=BATCH_ENDPOINTS if endpoints is None else endpoints)


def stub_get(service, response):
    """Replace service._get with a stub returning `response`, recording calls."""
    calls = []

    def _get(target, mode, params=None, headers=None, **kwargs):
        calls.append({'target': target, 'mode': mode,
                      'params': params, 'headers': headers, **kwargs})
        return response

    service._get = _get
    return calls


def trimmed_body(aid, **overrides):
    """The exact envelope the batch worker builds for a healthy view."""
    stat = {
        'aid': aid, 'view': 100, 'danmaku': 1, 'reply': 2, 'favorite': 3,
        'coin': 4, 'share': 5, 'now_rank': 0, 'his_rank': 0, 'like': 6,
        'dislike': None, 'vt': None, 'vv': None,
    }
    stat.update(overrides.pop('stat', {}))
    body = {
        'code': 0, 'message': '0', 'ttl': 1,
        'data': {'bvid': 'BV' + a2b(aid), 'aid': aid, 'stat': stat},
    }
    body.update(overrides)
    return body


def json_item(aid, status=200, body=None, echo=None):
    return {'aid': str(aid) if echo is None else echo, 'ms': 5, 'kind': 'json',
            'status': status, 'body': trimmed_body(aid) if body is None else body}


def envelope(aids, results, **overrides):
    e = {'v': 1, 'requested': len(aids), 'results': results}
    e.update(overrides)
    return e


class TestBatchHappyPath(unittest.TestCase):

    def test_two_ok_items(self):
        service = make_service()
        aids = [170001, 170002]
        calls = stub_get(service, envelope(aids, [json_item(a) for a in aids]))

        items = service.get_video_view_trimmed_batch(aids)

        self.assertEqual([i.aid for i in items], aids)
        for aid, item in zip(aids, items):
            self.assertIsNone(item.error)
            self.assertEqual(item.view.aid, aid)
            self.assertEqual(item.view.bvid, 'BV' + a2b(aid))
            self.assertEqual(item.view.stat.view, 100)
            self.assertIsNone(item.view.stat.vt)
        self.assertEqual(len(calls), 1)

    def test_request_shape_and_batch_tunables(self):
        service = make_service()
        aids = [1, 2, 3]
        calls = stub_get(service, envelope(aids, [json_item(a) for a in aids]))

        service.get_video_view_trimmed_batch(aids)

        call = calls[0]
        self.assertEqual(call['target'], 'get_video_view_trimmed_batch')
        self.assertEqual(call['mode'], 'worker')
        self.assertEqual(call['params'], {'aids': '1,2,3'})
        # single whole-batch trial: retries are the CALLER's per-aid attempts
        self.assertEqual(call['retry'], 1)
        # batch-specific timeout chain, not the 5s/10s single-path defaults
        self.assertEqual(call['timeout'], VIDEO_VIEW_TRIMMED_BATCH_READ_TIMEOUT_S)
        self.assertEqual(call['deadline'], VIDEO_VIEW_TRIMMED_BATCH_DEADLINE_S)

    def test_vt_vv_missing_from_stat_default_to_none(self):
        # the worker backfills vt/vv with null, but the client must not rely
        # on it (same .get(None) leniency as the single path)
        aid = 170001
        body = trimmed_body(aid)
        del body['data']['stat']['vt']
        del body['data']['stat']['vv']
        service = make_service()
        stub_get(service, envelope([aid], [json_item(aid, body=body)]))

        item = service.get_video_view_trimmed_batch([aid])[0]

        self.assertIsNone(item.view.stat.vt)
        self.assertIsNone(item.view.stat.vv)

    def test_stat_aid_zero_is_accepted(self):
        # upstream precedent: stat.aid is sometimes 0 (see task.py's
        # commit_video_record_via_newlist_archive_stat)
        aid = 170001
        service = make_service()
        stub_get(service, envelope(
            [aid], [json_item(aid, body=trimmed_body(aid, stat={'aid': 0}))]))

        item = service.get_video_view_trimmed_batch([aid])[0]

        self.assertIsNone(item.error)
        self.assertEqual(item.view.stat.aid, 0)

    def test_hidden_view_count_passes_through_as_string(self):
        # '--' handling belongs to the RecordNew mapping, not here
        aid = 170001
        service = make_service()
        stub_get(service, envelope(
            [aid], [json_item(aid, body=trimmed_body(aid, stat={'view': '--'}))]))

        item = service.get_video_view_trimmed_batch([aid])[0]

        self.assertEqual(item.view.stat.view, '--')


class TestPerItemFailures(unittest.TestCase):

    def test_code_error_item(self):
        aids = [170001, 170002]
        service = make_service()
        stub_get(service, envelope(aids, [
            json_item(aids[0]),
            json_item(aids[1], body={'code': -404, 'message': 'not found', 'ttl': 1}),
        ]))

        items = service.get_video_view_trimmed_batch(aids)

        self.assertIsNone(items[0].error)
        error = items[1].error
        self.assertIsInstance(error, CodeError)
        self.assertEqual(error.code, -404)
        # same target as the single path so downstream logging matches
        self.assertEqual(error.target, 'video_view_trimmed')
        self.assertEqual(error.params, {'aid': aids[1]})
        self.assertIsNone(items[1].view)

    def test_transient_kinds_are_retryable(self):
        aids = [1, 2, 3]
        service = make_service()
        stub_get(service, envelope(aids, [
            {'aid': '1', 'ms': 4500, 'kind': 'item_timeout', 'timeout_ms': 4500},
            {'aid': '2', 'ms': 10, 'kind': 'fetch_error', 'detail': 'connect ECONNREFUSED'},
            {'aid': '3', 'ms': 20, 'kind': 'non_json', 'status': 404,
             'body_snippet': '<!DOCTYPE html>'},
        ]))

        items = service.get_video_view_trimmed_batch(aids)

        for item in items:
            self.assertIsNone(item.view)
            self.assertIsInstance(item.error, ResponseError)
            self.assertNotIsInstance(item.error, CodeError)

    def test_non_200_upstream_with_code_zero_body_is_not_trusted(self):
        # THE trap case: top-level 200, kind json, body.code == 0 -- but the
        # upstream status was 500. The single path never parses non-200
        # bodies, so this must be a retryable failure, never a record.
        aid = 170001
        service = make_service()
        stub_get(service, envelope(
            [aid], [json_item(aid, status=500)]))

        item = service.get_video_view_trimmed_batch([aid])[0]

        self.assertIsNone(item.view)
        self.assertIsInstance(item.error, ResponseError)

    def test_non_200_upstream_with_error_code_body_is_retryable_not_code_error(self):
        # e.g. a -412 rate-limit page served with HTTP 412: the single path
        # would retry it (non-200), not route it to the code-error queue
        aid = 170001
        service = make_service()
        stub_get(service, envelope(
            [aid],
            [json_item(aid, status=412,
                       body={'code': -412, 'message': 'rejected', 'ttl': 1})]))

        item = service.get_video_view_trimmed_batch([aid])[0]

        self.assertIsNone(item.view)
        self.assertIsInstance(item.error, ResponseError)
        self.assertNotIsInstance(item.error, CodeError)


class TestWholeBatchFailures(unittest.TestCase):

    def setUp(self):
        # these paths log loudly on purpose; keep the test run output clean
        logging.getLogger('Service').disabled = True
        self.addCleanup(
            lambda: setattr(logging.getLogger('Service'), 'disabled', False))

    def test_transport_failure_raises_response_error(self):
        # _get returns None on HTTP != 200 / unparsable body / network failure
        # (this includes the batch worker's own 400/500 error envelopes)
        service = make_service()
        stub_get(service, None)

        with self.assertRaises(ResponseError):
            service.get_video_view_trimmed_batch([1, 2])

    def test_missing_endpoint_exits_as_configuration_error(self):
        for endpoints in ({}, {'get_video_view_trimmed_batch': {'workers': []}}):
            with self.assertRaises(SystemExit):
                make_service(endpoints=endpoints)

    def test_direct_mode_is_a_programming_error(self):
        service = Service(mode='direct', endpoints=BATCH_ENDPOINTS)
        with self.assertRaises(SystemExit):
            service.get_video_view_trimmed_batch([1])


class TestMisalignment(unittest.TestCase):

    def _assert_misaligned(self, aids, response):
        service = make_service()
        stub_get(service, response)
        with self.assertRaises(MisalignmentError):
            service.get_video_view_trimmed_batch(aids)

    def test_old_single_worker_answer_is_misaligned(self):
        # a config error pointing the batch key at the single-aid worker:
        # upstream answers ?aids= with a -400 passthrough -- valid JSON, no v.
        # The v check is the double insurance that kills the path immediately
        # instead of poisoning anything.
        self._assert_misaligned([1, 2], {'code': -400, 'message': '请求错误', 'ttl': 1})

    def test_wrong_protocol_version(self):
        aids = [1]
        self._assert_misaligned(
            aids, envelope(aids, [json_item(1)], v=2))

    def test_boolean_protocol_version_is_misaligned(self):
        # True == 1 in Python; the protocol field must be a real int
        aids = [1]
        self._assert_misaligned(
            aids, envelope(aids, [json_item(1)], v=True))

    def test_requested_count_mismatch(self):
        aids = [1, 2]
        self._assert_misaligned(
            aids, envelope(aids, [json_item(1), json_item(2)], requested=3))

    def test_boolean_requested_is_misaligned(self):
        # True == 1 would otherwise slip through a one-aid batch
        aids = [1]
        self._assert_misaligned(
            aids, envelope(aids, [json_item(1)], requested=True))

    def test_json_item_with_missing_or_non_int_status_is_misaligned(self):
        # the worker always sets an integer status on json items: a missing,
        # string, or boolean one is a contract violation, not a retryable
        # upstream hiccup
        for bad_status in (None, '200', True):
            aid = 170001
            item = json_item(aid)
            if bad_status is None:
                del item['status']
            else:
                item['status'] = bad_status
            self._assert_misaligned([aid], envelope([aid], [item]))

    def test_results_length_mismatch(self):
        aids = [1, 2]
        self._assert_misaligned(aids, envelope(aids, [json_item(1)], requested=2))

    def test_aid_echo_mismatch(self):
        aids = [1, 2]
        self._assert_misaligned(
            aids, envelope(aids, [json_item(1), json_item(2, echo='999')]))

    def test_aid_echo_wrong_type(self):
        # the worker echoes the raw token, which we sent as a decimal string;
        # an int echo means we are NOT talking to the contract we think
        aids = [1]
        self._assert_misaligned(aids, envelope(aids, [json_item(1, echo=1)]))

    def test_unknown_kind(self):
        aids = [1]
        self._assert_misaligned(
            aids, envelope(aids, [{'aid': '1', 'ms': 1, 'kind': 'surprise'}]))

    def test_json_kind_without_dict_body(self):
        aids = [1]
        self._assert_misaligned(
            aids, envelope(aids, [{'aid': '1', 'ms': 1, 'kind': 'json',
                                   'status': 200, 'body': '{"code":0}'}]))

    def test_missing_top_level_body_key(self):
        aid = 170001
        body = trimmed_body(aid)
        del body['ttl']
        self._assert_misaligned([aid], envelope([aid], [json_item(aid, body=body)]))

    def test_missing_stat_key(self):
        aid = 170001
        body = trimmed_body(aid)
        del body['data']['stat']['dislike']
        self._assert_misaligned([aid], envelope([aid], [json_item(aid, body=body)]))

    def test_body_data_aid_mismatch(self):
        aid = 170001
        other = 170002
        # a payload wholesale from ANOTHER video: internally consistent
        # (aid+bvid+stat all match each other) but not the requested aid
        body = trimmed_body(other)
        self._assert_misaligned([aid], envelope([aid], [json_item(aid, body=body)]))

    def test_bvid_mismatch(self):
        aid = 170001
        body = trimmed_body(aid)
        body['data']['bvid'] = 'BV' + a2b(aid + 1)
        self._assert_misaligned([aid], envelope([aid], [json_item(aid, body=body)]))

    def test_stat_aid_mismatch(self):
        aid = 170001
        body = trimmed_body(aid, stat={'aid': 999})
        self._assert_misaligned([aid], envelope([aid], [json_item(aid, body=body)]))

    def test_misalignment_discards_healthy_siblings(self):
        # one bad item poisons the whole batch by design ("the path is
        # untrustworthy", not "skip the item"): no partial result comes back
        aids = [1, 2]
        service = make_service()
        stub_get(service, envelope(aids, [json_item(1), json_item(2, echo='42')]))
        with self.assertRaises(MisalignmentError):
            service.get_video_view_trimmed_batch(aids)


if __name__ == '__main__':
    unittest.main()
