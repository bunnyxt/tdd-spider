"""
Tests for the metric reading commands ``runrecord overview`` and
``runrecord trend`` (built on the ``runrecord.series`` core).

Run from the repo root:

    python tests/test_run_record_overview_trend.py
    # or
    python -m unittest discover -s tests

Stdlib only (unittest + the driver shim). The CLI is exercised through
``query.main(argv)`` so exit codes and rendered output are both asserted.
``--layout`` is always passed explicitly so the assertions do not depend on the
runner's terminal width.
"""

import contextlib
import io
import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from runrecord import schema  # noqa: E402
from runrecord import query  # noqa: E402
from runrecord._sqlite import sqlite3  # noqa: E402


def _iso(dt):
    return dt.astimezone(timezone.utc).isoformat()


def run_cli(argv):
    out, err = io.StringIO(), io.StringIO()
    try:
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = query.main(argv)
    except SystemExit as e:  # argparse
        code = e.code if isinstance(e.code, int) else (0 if e.code is None else 1)
    return code, out.getvalue(), err.getvalue()


class _DbCase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.path = os.path.join(self.dir, 'run-records.sqlite3')
        conn = sqlite3.connect(self.path)
        schema.init(conn)
        conn.close()
        self.now = datetime.now(timezone.utc)

    def add_run(self, run_id, script, started, finished=None, status='succeeded',
                host='H', code_version='abc1234'):
        conn = sqlite3.connect(self.path)
        conn.execute(
            'INSERT INTO run (run_id, script_name, host, code_version, '
            'started_at, finished_at, status) VALUES (?, ?, ?, ?, ?, ?, ?)',
            (run_id, script, host, code_version, _iso(started),
             _iso(finished) if finished else None, status))
        conn.commit()
        conn.close()

    def add_metric(self, run_id, scope, name, value, unit='count', is_key=None):
        conn = sqlite3.connect(self.path)
        conn.execute(
            'INSERT OR REPLACE INTO run_metric '
            '(run_id, scope, name, value, unit, is_key) VALUES (?, ?, ?, ?, ?, ?)',
            (run_id, scope, name, float(value), unit, is_key))
        conn.commit()
        conn.close()

    def rid(self, prefix):
        return prefix + 'z' * (32 - len(prefix))

    def seed(self):
        """Two scripts. 51_ has three finished runs plus one still running;
        71_ has a single finished run."""
        h = lambda n: self.now - timedelta(hours=n)  # noqa: E731
        self.add_run(self.rid('a1'), '51_x', h(4), h(4) + timedelta(minutes=8))
        self.add_metric(self.rid('a1'), 'record-fetch', 'total_count', 170000)
        self.add_metric(self.rid('a1'), 'record-fetch', 'other_exception', 12)
        self.add_metric(self.rid('a1'), 'record-fetch', 'success', 169988)

        self.add_run(self.rid('a2'), '51_x', h(3), h(3) + timedelta(minutes=10))
        self.add_metric(self.rid('a2'), 'record-fetch', 'total_count', 171000)
        self.add_metric(self.rid('a2'), 'record-fetch', 'other_exception', 877)
        # NOTE: run a2 records no 'success' -> a genuinely missing value

        self.add_run(self.rid('a3'), '51_x', h(2), h(2) + timedelta(minutes=9),
                     status='failed')
        self.add_metric(self.rid('a3'), 'record-fetch', 'total_count', 5000)

        self.add_run(self.rid('a4'), '51_x', h(1), None, status='running')

        self.add_run(self.rid('b1'), '71_y', h(2), h(2) + timedelta(seconds=40))
        self.add_metric(self.rid('b1'), 'sprint', 'total_count', 13)
        self.add_metric(self.rid('b1'), 'sprint', 'exception', 1)


# --------------------------------------------------------------------------- #
# overview
# --------------------------------------------------------------------------- #

class OverviewText(_DbCase):
    def test_one_row_per_script_with_latest_run(self):
        self.seed()
        code, out, _ = run_cli(['overview', '--db', self.path])
        self.assertEqual(code, 0)
        self.assertEqual(out.count('51_x'), 1)
        self.assertEqual(out.count('71_y'), 1)
        # 51_x's latest run is the running one -> shown as 'running'
        line = next(l for l in out.splitlines() if l.startswith('51_x'))
        self.assertIn('running', line)
        # 71_y's key metrics are surfaced inline, not as columns
        line = next(l for l in out.splitlines() if l.startswith('71_y'))
        self.assertIn('sprint/total_count=13', line)
        self.assertIn('sprint/exception=1', line)

    def test_key_metrics_only_and_capped(self):
        self.seed()
        # a2 is 51_x's latest *finished* run; make it the latest overall
        self.add_run(self.rid('a5'), '51_x', self.now - timedelta(minutes=5),
                     self.now - timedelta(minutes=1))
        for i, name in enumerate(['total_count', 'other_exception', 'code_error',
                                  'batch_insert_fail', 'success', 'new_video']):
            self.add_metric(self.rid('a5'), 's', name, i + 1)
        code, out, _ = run_cli(['overview', '--db', self.path, '--max-metrics', '2'])
        self.assertEqual(code, 0)
        line = next(l for l in out.splitlines() if l.startswith('51_x'))
        # non-key metrics never appear
        self.assertNotIn('success', line)
        self.assertNotIn('new_video', line)
        # capped at 2, total_count floated first
        self.assertIn('s/total_count=', line)
        self.assertEqual(line.count('s/'), 2)

    def test_explicit_is_key_zero_suppresses_convention(self):
        self.add_run(self.rid('c1'), 'z_', self.now - timedelta(minutes=5),
                     self.now - timedelta(minutes=1))
        self.add_metric(self.rid('c1'), 's', 'total_count', 9, is_key=0)
        self.add_metric(self.rid('c1'), 's', 'widgets', 5, is_key=1)
        code, out, _ = run_cli(['overview', '--db', self.path])
        self.assertEqual(code, 0)
        line = next(l for l in out.splitlines() if l.startswith('z_'))
        self.assertNotIn('total_count', line)      # suppressed
        self.assertIn('s/widgets=5', line)          # promoted

    def test_since_drops_scripts_whose_last_run_is_older(self):
        self.seed()
        code, out, _ = run_cli(['overview', '--db', self.path, '--since', '90m'])
        self.assertEqual(code, 0)
        self.assertIn('51_x', out)      # has a run 1h ago
        self.assertNotIn('71_y', out)   # last run 2h ago

    def test_empty_db_exits_no_match(self):
        code, out, _ = run_cli(['overview', '--db', self.path])
        self.assertEqual(code, 1)
        self.assertIn('no runs recorded', out)


class OverviewJson(_DbCase):
    def test_json_structure(self):
        self.seed()
        code, out, _ = run_cli(['--json', 'overview', '--db', self.path])
        self.assertEqual(code, 0)
        doc = json.loads(out)
        self.assertEqual(doc['query']['command'], 'overview')
        self.assertEqual(doc['query']['max_metrics'], 4)
        self.assertEqual(doc['schema_version'], 2)
        self.assertEqual(doc['count'], 2)
        names = [s['script_name'] for s in doc['scripts']]
        self.assertEqual(names, sorted(names))
        y = next(s for s in doc['scripts'] if s['script_name'] == '71_y')
        self.assertEqual(
            {(m['scope'], m['name'], m['value']) for m in y['key_metrics']},
            {('sprint', 'total_count', 13.0), ('sprint', 'exception', 1.0)})
        # the full latest-run record is still there
        self.assertIn('metrics', y)
        self.assertIn('logs', y)


# --------------------------------------------------------------------------- #
# trend
# --------------------------------------------------------------------------- #

class TrendArgs(_DbCase):
    def test_script_is_required(self):
        code, _, err = run_cli(['trend', '--db', self.path])
        self.assertEqual(code, 2)
        self.assertIn('--script', err)

    def test_bad_metric_arg_is_usage_error(self):
        code, _, err = run_cli(['trend', '--db', self.path, '--script', '51_x',
                                '--metric', 'noslash'])
        self.assertEqual(code, 2)
        self.assertIn('SCOPE/NAME', err)

    def test_negative_limit_rejected(self):
        code, _, err = run_cli(['trend', '--db', self.path, '--script', '51_x',
                                '--limit', '-1'])
        self.assertEqual(code, 2)

    def test_order_option_is_gone(self):
        self.seed()
        code, _, err = run_cli(['trend', '--db', self.path, '--script', '51_x',
                                '--order', 'desc'])
        self.assertEqual(code, 2)
        self.assertIn('--order', err)


class TrendText(_DbCase):
    def test_default_is_key_metrics_only(self):
        self.seed()
        code, out, _ = run_cli(['trend', '--db', self.path, '--script', '51_x',
                                '--layout', 'table'])
        self.assertEqual(code, 0)
        self.assertIn('record-fetch/total_count', out)
        self.assertIn('record-fetch/other_exception', out)
        self.assertNotIn('record-fetch/success', out)   # not key, not selected

    def test_multi_metric_run_aligned_table_with_missing(self):
        self.seed()
        code, out, _ = run_cli(['trend', '--db', self.path, '--script', '51_x',
                                '--layout', 'table',
                                '--metric', 'record-fetch/total_count',
                                '--metric', 'record-fetch/success'])
        self.assertEqual(code, 0)
        rows = [l for l in out.splitlines() if l[:2] in ('a1', 'a2', 'a3', 'a4')]
        self.assertEqual(len(rows), 4)
        # one row per run, always oldest -> newest
        self.assertTrue(rows[0].startswith('a1'))
        self.assertTrue(rows[3].startswith('a4'))
        # 'success' is the last column; a2 recorded none -> trailing '-', not 0
        a2 = next(r for r in rows if r.startswith('a2'))
        self.assertTrue(a2.rstrip().endswith('-'), a2)
        self.assertNotIn(' 0 ', a2)

    def test_incomplete_lifecycle_points_are_shown_and_labelled(self):
        self.seed()
        code, out, _ = run_cli(['trend', '--db', self.path, '--script', '51_x',
                                '--layout', 'table',
                                '--metric', 'record-fetch/total_count'])
        self.assertEqual(code, 0)
        self.assertIn('failed', out)     # a3
        self.assertIn('running', out)    # a4 (within stale budget)
        a4 = next(l for l in out.splitlines() if l.startswith('a4'))
        # a4 has neither a finish time nor total_count -> both render '-'
        self.assertTrue(a4.rstrip().endswith('-'))

    def test_selected_metric_never_recorded_still_listed(self):
        self.seed()
        code, out, _ = run_cli(['trend', '--db', self.path, '--script', '51_x',
                                '--layout', 'table',
                                '--metric', 'record-fetch/ghost'])
        self.assertEqual(code, 0)
        self.assertIn('record-fetch/ghost', out)
        self.assertIn('4 missing', out)

    def test_duration_builtin_series(self):
        self.seed()
        code, out, _ = run_cli(['trend', '--db', self.path, '--script', '51_x',
                                '--layout', 'blocks', '--metric', 'duration'])
        self.assertEqual(code, 0)
        self.assertIn('run/duration_s', out)
        self.assertIn('(seconds)', out)

    def test_empty_window_exits_no_match(self):
        self.seed()
        code, out, _ = run_cli(['trend', '--db', self.path, '--script', '51_x',
                                '--since', '2000-01-01', '--until', '2000-02-01'])
        self.assertEqual(code, 1)
        self.assertIn('no runs in window', out)

    def test_unknown_script_exits_no_match(self):
        self.seed()
        code, out, _ = run_cli(['trend', '--db', self.path, '--script', 'nope'])
        self.assertEqual(code, 1)

    def test_limit_keeps_newest_then_renders_chronologically(self):
        self.seed()
        code, out, _ = run_cli(['trend', '--db', self.path, '--script', '51_x',
                                '--layout', 'table', '--limit', '2',
                                '--metric', 'record-fetch/total_count'])
        self.assertEqual(code, 0)
        rows = [l for l in out.splitlines() if l[:2] in ('a1', 'a2', 'a3', 'a4')]
        self.assertEqual(len(rows), 2)
        # newest two runs are a3 + a4, rendered oldest -> newest
        self.assertTrue(rows[0].startswith('a3'))
        self.assertTrue(rows[1].startswith('a4'))

    def test_default_limit_keeps_newest_twenty_chronological(self):
        # 25 finished runs, one metric that increases every run
        for i in range(25):
            rid = self.rid('r%02d' % i)
            st = self.now - timedelta(hours=25 - i)
            self.add_run(rid, 'big_', st, st + timedelta(minutes=5))
            self.add_metric(rid, 'sc', 'total_count', 1000 + i)
        code, out, _ = run_cli(['--json', 'trend', '--db', self.path,
                                '--script', 'big_',
                                '--metric', 'sc/total_count'])
        self.assertEqual(code, 0)
        doc = json.loads(out)
        self.assertEqual(doc['count'], 20)
        vals = [p['values']['sc']['total_count'] for p in doc['points']]
        # oldest five (1000..1004) dropped, newest twenty kept, chronological
        self.assertEqual(vals, [float(v) for v in range(1005, 1025)])
        starts = [p['started_at'] for p in doc['points']]
        self.assertEqual(starts, sorted(starts))
        # text output is chronological too
        _, txt, _ = run_cli(['trend', '--db', self.path, '--script', 'big_',
                             '--layout', 'table', '--metric', 'sc/total_count'])
        rows = [l for l in txt.splitlines() if l.startswith('r')]
        self.assertEqual(len(rows), 20)
        self.assertTrue(rows[0].startswith('r05'))
        self.assertTrue(rows[-1].startswith('r24'))

    def test_blocks_layout(self):
        self.seed()
        code, out, _ = run_cli(['trend', '--db', self.path, '--script', '51_x',
                                '--layout', 'blocks',
                                '--metric', 'record-fetch/total_count'])
        self.assertEqual(code, 0)
        self.assertIn('trend (', out)   # sparkline block still present

    def test_unit_conflict_is_reported(self):
        self.add_run(self.rid('u1'), 's_', self.now - timedelta(hours=2),
                     self.now - timedelta(hours=2) + timedelta(minutes=1))
        self.add_run(self.rid('u2'), 's_', self.now - timedelta(hours=1),
                     self.now - timedelta(hours=1) + timedelta(minutes=1))
        self.add_metric(self.rid('u1'), 'x', 'm', 1, unit='count')
        self.add_metric(self.rid('u2'), 'x', 'm', 2, unit='items')
        code, _, err = run_cli(['trend', '--db', self.path, '--script', 's_',
                                '--metric', 'x/m'])
        self.assertEqual(code, 4)
        self.assertIn('inconsistent units', err)


class TrendJson(_DbCase):
    def test_contract(self):
        self.seed()
        code, out, _ = run_cli(['--json', 'trend', '--db', self.path,
                                '--script', '51_x',
                                '--metric', 'record-fetch/total_count',
                                '--metric', 'record-fetch/success',
                                '--metric', 'duration'])
        self.assertEqual(code, 0)
        doc = json.loads(out)

        # query scope
        q = doc['query']
        self.assertEqual(q['command'], 'trend')
        self.assertEqual(q['script'], '51_x')
        self.assertNotIn('order', q)
        self.assertEqual(q['metrics'], [['record-fetch', 'total_count'],
                                        ['record-fetch', 'success'],
                                        ['run', 'duration_s']])

        # series identity, request order preserved
        self.assertEqual([(s['scope'], s['name']) for s in doc['series']],
                         [('record-fetch', 'total_count'),
                          ('record-fetch', 'success'),
                          ('run', 'duration_s')])
        dur = next(s for s in doc['series'] if s['name'] == 'duration_s')
        self.assertEqual(dur['unit'], 'seconds')

        # per-point run identity + context
        self.assertEqual(doc['count'], 4)
        # points are chronological, oldest -> newest
        self.assertTrue(doc['points'][0]['run_id'].startswith('a1'))
        self.assertTrue(doc['points'][-1]['run_id'].startswith('a4'))
        pt = doc['points'][0]
        for field in ('run_id', 'started_at', 'finished_at', 'duration_s',
                      'status', 'display_status', 'stale', 'host',
                      'code_version', 'values'):
            self.assertIn(field, pt)

        # missing stays missing, never zero-filled
        a2 = next(p for p in doc['points'] if p['run_id'].startswith('a2'))
        self.assertEqual(a2['values']['record-fetch']['total_count'], 171000.0)
        self.assertNotIn('success', a2['values'].get('record-fetch', {}))

        # a non-finished run: duration missing, not 0
        a4 = next(p for p in doc['points'] if p['run_id'].startswith('a4'))
        self.assertIsNone(a4['duration_s'])
        self.assertNotIn('run', a4['values'])
        self.assertEqual(a4['display_status'], 'running')


# --------------------------------------------------------------------------- #
# compatibility + read-only invariants
# --------------------------------------------------------------------------- #

class Compatibility(_DbCase):
    def test_list_and_show_unchanged(self):
        self.seed()
        code, out, _ = run_cli(['list', '--db', self.path])
        self.assertEqual(code, 0)
        self.assertIn('51_x', out)

        code, out, _ = run_cli(['--json', 'show', self.rid('a1'), '--db', self.path])
        self.assertEqual(code, 0)
        doc = json.loads(out)
        entry = doc['runs'][0]['metrics']['record-fetch'][0]
        # show's metric dicts stay exactly {name, value, unit} -- no is_key leak
        self.assertEqual(set(entry), {'name', 'value', 'unit'})

    def test_queries_do_not_touch_the_database_file(self):
        self.seed()
        before = os.stat(self.path)
        for argv in (['overview', '--db', self.path],
                     ['trend', '--db', self.path, '--script', '51_x'],
                     ['--json', 'trend', '--db', self.path, '--script', '51_x']):
            run_cli(argv)
        after = os.stat(self.path)
        self.assertEqual((before.st_size, before.st_mtime),
                         (after.st_size, after.st_mtime))
        for sidecar in ('-wal', '-journal', '-shm'):
            self.assertFalse(os.path.exists(self.path + sidecar))


if __name__ == '__main__':
    unittest.main()
