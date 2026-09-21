"""
Per-entry-point verification that the two production scripts without a JobStat
summary -- ``18_member-total-stat-update.py`` and ``72_add-sprint-daily.py`` --
open, populate and close a run record without changing their duration or
logging behaviour.

Same approach as ``test_summary_script_run_records.py``: each script is imported
by file path, its DB session is replaced with an inert fake, and its top-level
work function is driven once for a normal run and once for a failing run. The
assertions are:

* a ``run`` row is written, keyed by the canonical ``script_id_script_name``;
* it ends ``succeeded`` on a normal run and ``failed`` when the body raises and
  the script's own ``except`` raises ``SystemExit(1)``;
* only the plain counts each script already computes land in ``run_metric``
  (no fabricated JobStat, no message text).

These scripts import ``db`` (SQLAlchemy). Where that is absent the whole module
skips, matching ``test_run_record.py``'s handling of a bare checkout.
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
    ``conf/conf.ini`` is a git-ignored secret, so on a fresh checkout
    ``import db`` (which builds a SQLAlchemy engine at import time) fails.
    Install an inert ``conf`` with dummy values -- every DB call is faked here,
    so the engine URL never has to be valid.
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
    import db  # noqa: F401
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


def _db(path, query, params=()):
    conn = sqlite3.connect(path)
    try:
        return conn.execute(query, params).fetchall()
    finally:
        conn.close()


class _RunRecordTestBase(unittest.TestCase):
    def setUp(self):
        self.db_path = os.path.join(tempfile.mkdtemp(), 'run-records.sqlite3')
        os.environ['TDD_RUN_RECORD_DB'] = self.db_path
        self.addCleanup(os.environ.pop, 'TDD_RUN_RECORD_DB', None)
        logging.disable(logging.CRITICAL)
        self.addCleanup(logging.disable, logging.NOTSET)

    def _runs(self):
        return _db(self.db_path,
                   'SELECT run_id, script_name, status, finished_at FROM run '
                   'ORDER BY started_at')

    def _metrics(self, run_id):
        out = {}
        for scope, name, value, unit in _db(
                self.db_path,
                'SELECT scope, name, value, unit FROM run_metric WHERE run_id = ?',
                (run_id,)):
            out.setdefault(scope, {})[name] = (value, unit)
        return out


# --------------------------------------------------------------------- 18_ ---

class _MemberStatSession:
    """Fake SQLAlchemy session for 18_: execute() returns preset rows."""

    def __init__(self, rows):
        self._rows = rows
        self.added = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def execute(self, *a, **kw):
        return list(self._rows)

    def add(self, obj):
        self.added.append(obj)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


@unittest.skipUnless(_DEPS, 'db deps not importable in this environment')
class MemberTotalStatUpdateRunRecordTest(_RunRecordTestBase):
    def test_normal_run_records_succeeded_with_counts(self):
        m = _load('18_member-total-stat-update.py')
        # [aid, v_mid, view, danmaku, reply, favorite, coin, share, like, s_mid]
        rows = [
            [1, 100, 10, 1, 1, 1, 1, 1, 1, None],
            [2, 100, 20, 2, 2, 2, 2, 2, 2, None],
            [3, 200, 30, 3, 3, 3, 3, 3, 3, None],
        ]
        session = _MemberStatSession(rows)

        with mock.patch.object(m, 'Session', lambda: session):
            m.member_total_stat_update()

        (run_id, script_name, status, finished_at), = self._runs()
        self.assertEqual(script_name, '18_member-total-stat-update')
        self.assertEqual(status, 'succeeded')
        self.assertTrue(finished_at)

        metrics = self._metrics(run_id)['member-total-stat']
        self.assertEqual(metrics['result_rows'], (3.0, 'count'))
        self.assertEqual(metrics['member_count'], (2.0, 'count'))
        self.assertEqual(metrics['records_added'], (2.0, 'count'))
        # every recorded name is a plain count -- nothing error-like fabricated
        self.assertEqual(set(metrics), {'result_rows', 'member_count', 'records_added'})
        # completeness signal: one ORM row per distinct member was staged
        self.assertEqual(len(session.added), 2)

    def test_no_data_still_records_succeeded(self):
        m = _load('18_member-total-stat-update.py')
        session = _MemberStatSession([])

        with mock.patch.object(m, 'Session', lambda: session):
            m.member_total_stat_update()

        (run_id, _, status, _), = self._runs()
        self.assertEqual(status, 'succeeded')
        metrics = self._metrics(run_id)['member-total-stat']
        self.assertEqual(metrics['result_rows'], (0.0, 'count'))
        self.assertEqual(metrics['records_added'], (0.0, 'count'))

    def test_failure_is_recorded(self):
        m = _load('18_member-total-stat-update.py')

        class _Boom(_MemberStatSession):
            def execute(self, *a, **kw):
                raise RuntimeError('db lost connection')

        session = _Boom([])
        with mock.patch.object(m, 'Session', lambda: session):
            with self.assertRaises(SystemExit) as ctx:
                m.member_total_stat_update()

        self.assertEqual(ctx.exception.code, 1)
        (_, _, status, finished_at), = self._runs()
        self.assertEqual(status, 'failed')
        self.assertTrue(finished_at)
        self.assertEqual(session.rollbacks, 1)


# --------------------------------------------------------------------- 72_ ---

class _SprintSession:
    """Fake SQLAlchemy session for 72_: execute() answers by SQL substring."""

    def __init__(self, responder):
        self._responder = responder
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def execute(self, sql, *a, **kw):
        return self._responder(str(sql).lower())

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


def _sprint_responder(now_s):
    def responder(sql):
        if 'order by added limit 1' in sql:            # per-new-video start view
            return [{'view': 100}]
        if 'from tdd_sprint_video_record' in sql:      # start + end range queries
            return [{'aid': 111, 'view': 900000}, {'aid': 222, 'view': 950000}]
        if 'from tdd_sprint_video where aid' in sql:   # created lookup
            return [{'created': now_s - 100 * 24 * 3600}]
        if 'insert into tdd_sprint_daily_record' in sql:
            return []
        if 'from tdd_sprint_daily' in sql:             # last viewincr
            return [{'viewincr': 1000}]
        if 'insert into tdd_sprint_daily' in sql:
            return []
        return []
    return responder


@unittest.skipUnless(_DEPS, 'db deps not importable in this environment')
class AddSprintDailyRunRecordTest(_RunRecordTestBase):
    def test_normal_run_records_succeeded_with_metrics(self):
        m = _load('72_add-sprint-daily.py')
        session = _SprintSession(_sprint_responder(m.get_ts_s()))

        with mock.patch.object(m, 'Session', lambda: session):
            m.add_sprint_daily()

        (run_id, script_name, status, finished_at), = self._runs()
        self.assertEqual(script_name, '72_add-sprint-daily')
        self.assertEqual(status, 'succeeded')
        self.assertTrue(finished_at)

        metrics = self._metrics(run_id)['sprint-daily']
        self.assertEqual(metrics['start_videos'], (2.0, 'count'))
        self.assertEqual(metrics['end_videos'], (2.0, 'count'))
        self.assertEqual(metrics['daily_records_added'], (2.0, 'count'))
        self.assertEqual(metrics['new_videos'], (0.0, 'count'))
        self.assertEqual(metrics['million_videos'], (0.0, 'count'))
        self.assertEqual(metrics['view_incr_total'], (0.0, 'views'))
        self.assertEqual(metrics['view_incr_incr'], (-1000.0, 'views'))

        # the summary body (which carries an aid list) never reaches the
        # database: only numeric metrics are stored
        for _run_id, _scope, name, value in _db(
                self.db_path, 'SELECT run_id, scope, name, value FROM run_metric'):
            self.assertNotIn(';', name)
            self.assertIsInstance(value, float)

    def test_failure_is_recorded(self):
        m = _load('72_add-sprint-daily.py')

        def _boom(_sql):
            raise RuntimeError('db lost connection')

        session = _SprintSession(_boom)
        with mock.patch.object(m, 'Session', lambda: session):
            with self.assertRaises(SystemExit) as ctx:
                m.add_sprint_daily()

        self.assertEqual(ctx.exception.code, 1)
        (_, _, status, finished_at), = self._runs()
        self.assertEqual(status, 'failed')
        self.assertTrue(finished_at)
        self.assertEqual(session.rollbacks, 1)


if __name__ == '__main__':
    unittest.main(verbosity=2)
