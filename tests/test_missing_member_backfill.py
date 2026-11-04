import importlib.util
import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from test_remaining_script_run_records import _DEPS

spec = importlib.util.spec_from_file_location('missing_member_backfill',
    Path(__file__).resolve().parents[1]/'43_add-missing-members.py')
backfill = importlib.util.module_from_spec(spec)
spec.loader.exec_module(backfill)

if _DEPS:
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from service import CodeError, ContentError, MemberCard, RateLimitError
    from task import AlreadyExistError


class AuditConnection:
    def __init__(self, mids, members, safe=True):
        self.mids, self.members, self.safe = mids, members, safe
        self.description = [('key',), ('Extra',)]
        self.rows = []
        self.pages_read = 0
        self.rolled_back = False

    def cursor(self):
        return self

    def execute(self, sql, params=()):
        if sql.startswith('SELECT MIN'):
            self.rows = [(min(self.mids), max(self.mids))] if self.mids else [(None, None)]
        elif sql == 'SELECT mid FROM tdd_member':
            self.rows = [(mid,) for mid in self.members]
        elif sql.startswith('EXPLAIN'):
            self.rows = [('mid_INDEX', 'Using index for group-by' if self.safe else 'Using index')]
        elif sql.startswith('SELECT mid FROM tdd_member_total_stat_record'):
            self.pages_read += 1
            self.rows = [(mid,) for mid in sorted(set(self.mids)) if mid > params[0]][:params[1]]

    def fetchone(self):
        return self.rows[0]

    def fetchall(self):
        return self.rows

    def rollback(self):
        self.rolled_back = True

    def close(self):
        pass


class MissingMemberAuditTest(unittest.TestCase):
    @unittest.skipUnless(_DEPS, 'database dependencies unavailable')
    def test_default_cli_is_read_only_and_preserves_reports(self):
        connection = AuditConnection([10], [])
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder)/'inventory.json'
            with mock.patch('db.engine.raw_connection', return_value=connection), \
                 contextlib.redirect_stdout(io.StringIO()), \
                 mock.patch('service.Service') as service:
                self.assertEqual(backfill.main(['--output', str(output)]), 0)
            service.assert_not_called()
            self.assertEqual(json.loads(output.read_text())['missing_positive_mids'], [10])
            self.assertEqual(output.stat().st_mode & 0o777, 0o600)
            original = output.read_bytes()
            with mock.patch('db.engine.raw_connection') as database, \
                 contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                backfill.main(['--output', str(output)])
            database.assert_not_called()
            self.assertEqual(output.read_bytes(), original)

    def test_distinct_pages_and_invalid_ids(self):
        connection = AuditConnection([-9, 0, 7, 7, 65536, 2147483648], [7])
        report = backfill.audit_missing_members(connection, page_size=2)
        self.assertTrue(report['complete'])
        self.assertEqual(report['aggregate_distinct_mids'], 5)
        self.assertEqual(report['missing_positive_mids'], [65536, 2147483648])
        self.assertEqual(report['missing_nonpositive_mids'], [-9, 0])
        self.assertTrue(connection.rolled_back)

    def test_unsafe_history_plan_is_never_executed(self):
        connection = AuditConnection([1, 2], [], safe=False)
        report = backfill.audit_missing_members(connection)
        self.assertFalse(report['complete'])
        self.assertEqual(report['error']['error_type'], 'RuntimeError')
        self.assertEqual(connection.pages_read, 0)

    def test_partial_inventory_cannot_be_applied(self):
        report = backfill.audit_missing_members(AuditConnection([1], []), max_seconds=0)
        self.assertFalse(report['complete'])
        with self.assertRaises(ValueError):
            backfill.inventory_mids(report)

    def test_apply_input_validates_ids_without_truncation(self):
        report = dict(complete=True, scope='all_aggregate_distinct_mids',
                      missing_positive_mids=[9000000000000, 32767, 2147483648, 32767])
        self.assertEqual(backfill.inventory_mids(report), [32767, 2147483648, 9000000000000])
        for mid in (0, -1, '123', True, 9223372036854775808):
            with self.subTest(mid=mid), self.assertRaises(ValueError):
                backfill.inventory_mids(dict(report, missing_positive_mids=[mid]))

    def test_failure_reason_excludes_arbitrary_exception_text(self):
        error = RuntimeError('private response')
        error.reason = 'request_exception'
        self.assertEqual(backfill.error_detail(error)['reason'], 'request_exception')
        error.reason = 'http_503'
        self.assertEqual(backfill.error_detail(error)['reason'], 'http_503')
        error.reason = 'https://private.invalid/secret'
        self.assertNotIn('reason', backfill.error_detail(error))


@unittest.skipUnless(_DEPS, 'database dependencies unavailable')
class MissingMemberApplyTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine('sqlite://')
        self.addCleanup(self.engine.dispose)
        self.engine.execute('CREATE TABLE tdd_member ('
            'id INTEGER PRIMARY KEY, added INTEGER, mid BIGINT UNIQUE, '
            'sex TEXT, name TEXT, face TEXT, sign TEXT, code INTEGER, '
            'video_count INTEGER, last_video BIGINT, '
            'last_total_stat BIGINT, last_follower BIGINT)')
        self.engine.execute('CREATE TABLE tdd_member_total_stat_record (id INTEGER PRIMARY KEY, mid BIGINT)')
        self.actual_session = sessionmaker(bind=self.engine)()
        self.addCleanup(self.actual_session.close)
        self.service = mock.Mock()
        self.service.get_member_card.side_effect = lambda params, **kwargs: MemberCard(
            params['mid'], 'real profile', '', '', '')
        actual_execute = self.actual_session.execute
        self.session = mock.Mock(wraps=self.actual_session)
        self.session.execute.side_effect = lambda sql, params=None: actual_execute(
            sql.replace(' FORCE INDEX(mid_INDEX)', ''), params)

    def aggregate(self, mid, ident):
        self.actual_session.execute('INSERT INTO tdd_member_total_stat_record VALUES (:id,:mid)',
                                    {'id': ident, 'mid': mid})
        self.actual_session.commit()

    def test_actual_add_member_latest_and_idempotent_rerun(self):
        mids = [32767, 2147483648, 9000000000000]
        for ident, mid in enumerate(mids, 1):
            self.aggregate(mid, ident)
        before = self.engine.execute('SELECT * FROM tdd_member_total_stat_record').fetchall()
        result = backfill.backfill_members(mids, self.session, self.service, delay=0)
        self.assertEqual(result['counts'], {'created': 3})
        self.assertTrue(result['all_resolved'])
        self.assertEqual(self.engine.execute('SELECT mid,last_total_stat FROM tdd_member ORDER BY mid').fetchall(),
                         list(zip(mids, range(1, 4))))
        again = backfill.backfill_members(mids, self.session, self.service, delay=0)
        self.assertEqual(again['counts'], {'already_exists': 3})
        self.assertEqual(self.service.get_member_card.call_count, 3)
        self.assertEqual(self.engine.execute('SELECT * FROM tdd_member_total_stat_record').fetchall(), before)

    def test_existing_profile_and_newer_pointer_are_preserved(self):
        self.aggregate(10, 1)
        self.actual_session.execute('INSERT INTO tdd_member (mid,name,last_total_stat) VALUES (10,"existing",99)')
        self.actual_session.commit()
        result = backfill.backfill_members([10], self.session, self.service, delay=0)
        self.assertEqual(result['counts'], {'already_exists': 1})
        self.assertEqual(self.engine.execute('SELECT name,last_total_stat FROM tdd_member').fetchone(), ('existing', 99))
        self.service.get_member_card.assert_not_called()

    def test_404_and_blank_card_never_create_fake_profiles(self):
        for mid in (10, 20):
            self.aggregate(mid, mid)
        self.service.get_member_card.side_effect = [
            CodeError('get_member_card', MemberCard, {'mid': 10}, {}, -404),
            ContentError('get_member_card', MemberCard, {'mid': 20}, {}, 'Blank name')]
        result = backfill.backfill_members([10, 20], self.session, self.service, delay=0)
        self.assertEqual(result['counts'], {'unavailable': 1, 'rejected_profile': 1})
        self.assertEqual(result['unresolved_mids'], [10, 20])
        self.assertEqual(self.engine.execute('SELECT COUNT(*) FROM tdd_member').scalar(), 0)

    def test_rate_limit_stops_before_the_next_request(self):
        self.aggregate(10, 1)
        self.aggregate(20, 2)
        self.service.get_member_card.side_effect = RateLimitError('get_member_card', 'http_412')
        result = backfill.backfill_members([10, 20], self.session, self.service, delay=0)
        self.assertEqual(result['stop_reason'], 'rate_limited')
        self.assertEqual(result['pending_mids'], [20])
        self.assertEqual(self.service.get_member_card.call_count, 1)
        self.assertEqual(result['unresolved_mids'], [10, 20])

    def test_blank_profiles_do_not_prevent_later_valid_backfills(self):
        for mid in (10, 20, 30, 40):
            self.aggregate(mid, mid)
        self.service.get_member_card.side_effect = [
            ContentError('get_member_card', MemberCard, {'mid': mid}, {}, 'Blank name')
            for mid in (10, 20, 30)] + [MemberCard(40, 'real profile', '', '', '')]
        result = backfill.backfill_members([10, 20, 30, 40], self.session, self.service, delay=0)
        self.assertEqual(result['counts'], {'rejected_profile': 3, 'created': 1})
        self.assertNotIn('stop_reason', result)
        self.assertEqual(result['unresolved_mids'], [10, 20, 30])

    def test_checkpoints_preserve_completed_ids_and_pending_work(self):
        self.aggregate(10, 1)
        self.aggregate(20, 2)
        snapshots = []
        result = backfill.backfill_members([10, 20], self.session, self.service,
            limit=1, delay=0, progress=lambda report: snapshots.append(json.loads(json.dumps(report))))
        self.assertEqual(snapshots[0]['counts'], {'created': 1})
        self.assertEqual(snapshots[0]['pending_mids'], [20])
        self.assertIsNone(snapshots[0]['finished_utc'])
        self.assertIsNotNone(snapshots[-1]['finished_utc'])
        self.assertEqual(result['next_offset'], 1)
        later = backfill.backfill_members([10, 20], self.session, self.service, offset=1, delay=0)
        self.assertEqual(later['unchecked_prefix_count'], 1)
        self.assertFalse(later['all_resolved'])

    def test_budget_limit_and_orphan_recheck(self):
        result = backfill.backfill_members([10], self.session, self.service, max_seconds=0, delay=0)
        self.assertEqual(result['pending_mids'], [10])
        self.service.get_member_card.assert_not_called()
        result = backfill.backfill_members([10], self.session, self.service, delay=0)
        self.assertEqual(result['counts'], {'not_referenced': 1})
        self.service.get_member_card.assert_not_called()


if __name__ == '__main__':
    unittest.main()
