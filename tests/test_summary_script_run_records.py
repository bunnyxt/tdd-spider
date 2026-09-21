"""
Per-entry-point verification that the six summary-reporting production
scripts (12_/15_/16_/17_/62_/71_) open, populate and close a run record
without changing their duration / JobStat summary behaviour.

Each script is imported by file path and its collaborators (Service, Session,
the Job classes / JobPool, ``requests``) are replaced with inert fakes, then
the top-level work function is driven once for a normal run and once for a
failing run. The assertions are:

* a ``run`` row is written, keyed by the canonical ``script_id_script_name``;
* it ends ``succeeded`` on a normal run and ``failed`` when the body raises;
* the JobStat counters land in ``run_metric`` under the expected scopes.

These scripts import ``db`` / ``service`` (SQLAlchemy, requests). Where those
are absent the whole module skips, matching ``test_run_record.py``'s handling of
a bare checkout.
"""

import importlib.util
import logging
import os
import sys
import tempfile
import types
import unittest
from unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from runrecord._sqlite import sqlite3  # noqa: E402


def _install_stub_conf():
    """
    ``conf/conf.ini`` is a git-ignored secret, so it is absent from a fresh
    checkout and ``import db`` (which builds a SQLAlchemy engine at import
    time) fails. Install an inert ``conf`` with dummy values: the engine
    URL never has to be valid here -- every DB call is faked -- and this keeps
    the test independent of real credentials.
    """
    if 'conf' in sys.modules and hasattr(sys.modules['conf'], 'get_db_args'):
        try:
            sys.modules['conf'].get_db_args()
            return  # a real, populated conf is already importable
        except Exception:
            pass
    stub = types.ModuleType('conf')
    stub.CONFIG_PATH = ''
    stub.CONFIG = None
    stub.get_db_args = lambda: {
        'user': 'stub', 'password': 'stub', 'host': '127.0.0.1',
        'port': '3306', 'dbname': 'stub'}
    stub.__all__ = ['CONFIG_PATH', 'CONFIG', 'get_db_args']
    sys.modules['conf'] = stub
    sys.modules['conf.conf'] = stub


try:
    _install_stub_conf()
    from job import JobStat
    import db  # noqa: F401
    import service  # noqa: F401
    _DEPS = True
except Exception as _e:  # pragma: no cover - depends on the environment
    _DEPS = False
    _DEPS_ERR = repr(_e)


def _load(filename):
    """Import a hyphenated top-level script by path under a safe module name."""
    path = os.path.join(ROOT, filename)
    name = 'scriptmod_' + filename.replace('-', '_').replace('.py', '')
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _stat(total_count=0, **conditions):
    s = JobStat()
    s.total_count = total_count
    for k, v in conditions.items():
        s.condition[k] = v
    return s


from service.apistat import NullApiStatTracker  # noqa: E402


class _FakeService:
    def __init__(self, *a, **kw):
        self.stats = NullApiStatTracker()


class _FakeApiStats:
    def __init__(self):
        self.log_calls = []

    def totals(self):
        return [
            {'scope': 'api:test_target:test_worker', 'name': 't1:ok',
             'value': 90.0},
            {'scope': 'api:test_target:test_worker', 'name': 't1:http_412',
             'value': 10.0},
        ]

    def log_summary(self, log=None):
        self.log_calls.append(log)


class _FakeServiceWithApiStats:
    def __init__(self, *a, **kw):
        self.stats = _FakeApiStats()


class _FakeSession:
    def execute(self, *a, **kw):
        return []

    def close(self):
        pass


class _FakeJob:
    """Stands in for a single Job: start()/join() no-op, carries a .stat."""

    def __init__(self, stat=None):
        self.stat = stat if stat is not None else _stat()

    def start(self):
        pass

    def join(self):
        pass


class _FakePool:
    """Stands in for a JobPool: start() no-op, join() returns a merged JobStat."""

    def __init__(self, merged):
        self._merged = merged

    def start(self):
        pass

    def join(self):
        return self._merged


def _db(path, query, params=()):
    conn = sqlite3.connect(path)
    try:
        return conn.execute(query, params).fetchall()
    finally:
        conn.close()


@unittest.skipUnless(_DEPS, 'db/service/job deps not importable in this environment')
class SummaryScriptRunRecordTest(unittest.TestCase):
    def setUp(self):
        self.db_path = os.path.join(tempfile.mkdtemp(), 'run-records.sqlite3')
        os.environ['TDD_RUN_RECORD_DB'] = self.db_path
        self.addCleanup(os.environ.pop, 'TDD_RUN_RECORD_DB', None)
        # keep the scripts' logging_init out of it -- attach_log_locations just
        # finds no FileHandlers on the root logger, which is fine here.
        logging.disable(logging.CRITICAL)
        self.addCleanup(logging.disable, logging.NOTSET)

    def _runs(self):
        return _db(self.db_path,
                   'SELECT run_id, script_name, status, finished_at FROM run '
                   'ORDER BY started_at')

    def _metrics(self, run_id):
        out = {}
        for scope, name, value in _db(
                self.db_path,
                'SELECT scope, name, value FROM run_metric WHERE run_id = ?',
                (run_id,)):
            out.setdefault(scope, {})[name] = value
        return out

    # ----------------------------------------------------------------- 12_ ---
    def test_12_add_latest_video_with_tid_30(self):
        m = _load('12_add-latest-video-with-tid-30.py')
        newlist_stat = _stat(3, not_fully_loaded_page=1, get_newlist_exception=0)
        add_stat = _stat(7, new_video=2, add_video_exception=0,
                         commit_video_record_exception=0)

        with mock.patch.object(m, 'Service', _FakeService), \
             mock.patch.object(m, 'GetNewlistArchiveJob',
                               lambda *a, **kw: _FakeJob(newlist_stat)), \
             mock.patch.object(m, 'AddVideoFromArchiveJob',
                               lambda *a, **kw: _FakeJob(add_stat)):
            m.add_latest_video_with_tid_30()

        (run_id, script_name, status, finished_at), = self._runs()
        self.assertEqual(script_name, '12_add-latest-video-with-tid-30')
        self.assertEqual(status, 'succeeded')
        self.assertTrue(finished_at)
        metrics = self._metrics(run_id)
        self.assertEqual(metrics['get-newlist-archive']['total_count'], 3.0)
        self.assertEqual(metrics['get-newlist-archive']['not_fully_loaded_page'], 1.0)
        # 10 AddVideoFromArchiveJob stats merged (fake returns the same stat)
        self.assertEqual(metrics['add-video-from-archive']['total_count'], 70.0)
        self.assertEqual(metrics['add-video-from-archive']['new_video'], 20.0)

    def test_12_failure_is_recorded(self):
        m = _load('12_add-latest-video-with-tid-30.py')
        with mock.patch.object(m, 'Service', _FakeService), \
             mock.patch.object(m, 'GetNewlistArchiveJob',
                               side_effect=RuntimeError('boom')):
            with self.assertRaises(RuntimeError):
                m.add_latest_video_with_tid_30()
        (_, _, status, _), = self._runs()
        self.assertEqual(status, 'failed')

    # ----------------------------------------------------------------- 15_ ---
    def test_15_update_video_info(self):
        m = _load('15_update-video-info.py')
        merged = _stat(120, **{'0_update': 118, 'update_exception': 2})
        service = _FakeServiceWithApiStats()

        with mock.patch.object(m, 'Service', return_value=service), \
             mock.patch.object(m, 'Session', _FakeSession), \
             mock.patch.object(m.DBOperation, 'query_all_video_bvids',
                               staticmethod(lambda s: [])), \
             mock.patch.object(m, 'JobPool', lambda *a, **kw: _FakePool(merged)), \
             mock.patch.object(m, 'b2a', lambda bv: 1):
            m.update_video_info()

        (run_id, script_name, status, _), = self._runs()
        self.assertEqual(script_name, '15_update-video-info')
        self.assertEqual(status, 'succeeded')
        metrics = self._metrics(run_id)
        self.assertEqual(metrics['video-update']['total_count'], 120.0)
        self.assertEqual(metrics['video-update']['update_exception'], 2.0)
        self.assertEqual(metrics['api:test_target:test_worker']['t1:ok'], 90.0)
        self.assertEqual(
            metrics['api:test_target:test_worker']['t1:http_412'], 10.0)
        self.assertEqual(service.stats.log_calls, [m.logger])

    # ----------------------------------------------------------------- 16_ ---
    def test_16_update_member_info(self):
        m = _load('16_update-member-info.py')
        merged = _stat(100, **{'0_update': 98, 'update_exception': 2})
        service = _FakeServiceWithApiStats()

        with mock.patch.object(m, 'Service', return_value=service), \
             mock.patch.object(m, 'Session', _FakeSession), \
             mock.patch.object(m.DBOperation, 'query_all_member_mids',
                               staticmethod(lambda s: [])), \
             mock.patch.object(m, 'JobPool', lambda *a, **kw: _FakePool(merged)):
            m.update_member_info()

        (run_id, script_name, status, finished_at), = self._runs()
        self.assertEqual(script_name, '16_update-member-info')
        self.assertEqual(status, 'succeeded')
        self.assertTrue(finished_at)
        metrics = self._metrics(run_id)
        self.assertEqual(metrics['member-update']['total_count'], 100.0)
        self.assertEqual(metrics['member-update']['update_exception'], 2.0)
        self.assertEqual(metrics['api:test_target:test_worker']['t1:ok'], 90.0)
        self.assertEqual(
            metrics['api:test_target:test_worker']['t1:http_412'], 10.0)
        self.assertEqual(service.stats.log_calls, [m.logger])

    def test_16_failure_is_recorded(self):
        m = _load('16_update-member-info.py')

        with mock.patch.object(m, 'Service', _FakeService), \
             mock.patch.object(m, 'Session', _FakeSession), \
             mock.patch.object(m.DBOperation, 'query_all_member_mids',
                               side_effect=RuntimeError('boom')):
            with self.assertRaises(RuntimeError):
                m.update_member_info()

        (_, script_name, status, finished_at), = self._runs()
        self.assertEqual(script_name, '16_update-member-info')
        self.assertEqual(status, 'failed')
        self.assertTrue(finished_at)

    # ----------------------------------------------------------------- 17_ ---
    def test_17_add_member_follower_record(self):
        m = _load('17_add-member-follower-record.py')
        fetch = _stat(100, success=100, exception=0)
        writer = _stat(100, batch_insert=1, batch_insert_fail=0)
        pools = iter([_FakePool(fetch), _FakePool(writer)])

        with mock.patch.object(m, 'Service', _FakeService), \
             mock.patch.object(m, 'Session', _FakeSession), \
             mock.patch.object(m.DBOperation, 'query_all_member_mids',
                               staticmethod(lambda session: [])), \
             mock.patch.object(m, 'JobPool', lambda *a, **kw: next(pools)):
            m.add_member_follower_record()

        (run_id, script_name, status, _), = self._runs()
        self.assertEqual(script_name, '17_add-member-follower-record')
        self.assertEqual(status, 'succeeded')
        metrics = self._metrics(run_id)
        self.assertEqual(metrics['follower-fetch']['success'], 100.0)
        self.assertEqual(metrics['follower-db-writer']['batch_insert'], 1.0)

    # ----------------------------------------------------------------- 62_ ---
    def test_62_add_evocalrank_video(self):
        m = _load('62_add-evocalrank-video.py')
        merged = _stat(5, success=5, other_exception=0)
        resp = mock.Mock()
        resp.json.return_value = {}  # no ranks -> no aids, still a clean run

        with mock.patch.object(m, 'Service', _FakeService), \
             mock.patch.object(m.requests, 'get', return_value=resp), \
             mock.patch.object(m, 'AddVideoJob',
                               lambda *a, **kw: _FakeJob(merged)):
            m.add_evocalrank_video(700)

        (run_id, script_name, status, _), = self._runs()
        self.assertEqual(script_name, '62_add-evocalrank-video')
        self.assertEqual(status, 'succeeded')
        # merged over 50 fake jobs: total_count 5 * 50, condition scaled too
        self.assertEqual(self._metrics(run_id)['add-evocalrank-video']['total_count'], 250.0)

    def test_62_fetch_failure_exits_and_records_failed(self):
        m = _load('62_add-evocalrank-video.py')
        resp = mock.Mock()
        resp.raise_for_status.side_effect = RuntimeError('503')

        with mock.patch.object(m, 'Service', _FakeService), \
             mock.patch.object(m.requests, 'get', return_value=resp):
            with self.assertRaises(SystemExit) as ctx:
                m.add_evocalrank_video(700)

        self.assertEqual(ctx.exception.code, 1)
        (_, _, status, finished_at), = self._runs()
        self.assertEqual(status, 'failed')
        self.assertTrue(finished_at)

    # ----------------------------------------------------------------- 71_ ---
    def test_71_add_sprint_video_record(self):
        m = _load('71_add-sprint-video-record.py')
        stat = _stat(13, success=12, exception=1)

        with mock.patch.object(m, 'Service', _FakeService), \
             mock.patch.object(m, 'Session', _FakeSession), \
             mock.patch.object(m, 'AddSprintVideoRecordJob',
                               lambda *a, **kw: _FakeJob(stat)):
            m.add_sprint_video_record()

        (run_id, script_name, status, _), = self._runs()
        self.assertEqual(script_name, '71_add-sprint-video-record')
        self.assertEqual(status, 'succeeded')
        self.assertEqual(self._metrics(run_id)['sprint-video-record']['exception'], 1.0)


if __name__ == '__main__':
    unittest.main(verbosity=2)
