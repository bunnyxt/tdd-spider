import logging
import sqlite3
import unittest

from util.member_total_stat_writer import FIELDS, write_member_stats
from test_remaining_script_run_records import _DEPS

if _DEPS:
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from db import TddMemberTotalStatRecord


LOG = logging.getLogger('test.aggregate.writer')


def records(count):
    return [dict(added=123, mid=2147483647+i,
                 video_count=i % 2, view=i*10, danmaku=i,
                 reply=0, favorite=0, coin=0, share=0, like=0)
            for i in range(count)]


def create_schema(connection):
    connection.executescript('''
        CREATE TABLE tdd_member (mid INTEGER PRIMARY KEY, last_total_stat INTEGER);
        CREATE TABLE tdd_member_total_stat_record (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            added INTEGER NOT NULL, mid BIGINT NOT NULL, video_count INTEGER NOT NULL,
            view INTEGER NOT NULL, danmaku INTEGER NOT NULL, reply INTEGER NOT NULL,
            favorite INTEGER NOT NULL, coin INTEGER NOT NULL, share INTEGER NOT NULL,
            "like" INTEGER NOT NULL);
        CREATE TRIGGER latest AFTER INSERT ON tdd_member_total_stat_record
        BEGIN
            UPDATE tdd_member SET last_total_stat=NEW.id WHERE mid=NEW.mid;
        END;
    ''')


class Connection:
    """Adapt only placeholder spelling; execute real statements and trigger."""
    def __init__(self, rows, fail_execute=None, fail_commit=None):
        self.db = sqlite3.connect(':memory:')
        create_schema(self.db)
        # Leave the final profile missing to exercise the trigger's zero match.
        self.db.executemany('INSERT INTO tdd_member VALUES (?, NULL)',
                            [(r['mid'],) for r in rows[:-1]])
        self.db.commit()
        self.calls = []
        self.commits = 0
        self.closed_cursors = 0
        self.fail_execute, self.fail_commit = fail_execute, fail_commit

    def cursor(self):
        owner = self

        class Cursor:
            def execute(self, sql, params):
                owner.calls.append((sql, params))
                if len(owner.calls) == owner.fail_execute:
                    raise RuntimeError('insert failed')
                owner.db.execute(sql.replace('%s', '?'), params)

            def close(self):
                owner.closed_cursors += 1

        return Cursor()

    def commit(self):
        self.commits += 1
        if self.commits == self.fail_commit:
            raise RuntimeError('commit outcome uncertain')
        self.db.commit()

    def rollback(self):
        self.db.rollback()


class MemberTotalStatWriterTest(unittest.TestCase):
    def connection(self, rows, **kwargs):
        connection = Connection(rows, **kwargs)
        self.addCleanup(connection.db.close)
        return connection

    def test_bound_multirow_statements_and_tail(self):
        for count in (0, 1, 999, 1000, 1001):
            with self.subTest(count=count):
                rows = records(count)
                connection = self.connection(rows)
                self.assertEqual(write_member_stats(connection, iter(rows), LOG), count)
                self.assertEqual(connection.commits, (count+999)//1000)
                self.assertEqual(len(connection.calls), connection.commits)
                for sql, params in connection.calls:
                    self.assertEqual(sql.count('%s'), len(params))
                    self.assertEqual(sql.count('), ('), len(params)//len(FIELDS)-1)
                    self.assertLessEqual(len(params)//len(FIELDS), 1000)
                    self.assertNotIn('2147483647', sql)
                actual = connection.db.execute('SELECT '+','.join('"'+f+'"' for f in FIELDS)+
                    ' FROM tdd_member_total_stat_record ORDER BY id').fetchall()
                self.assertEqual(actual, [tuple(row[f] for f in FIELDS) for row in rows])

    def test_prior_batch_remains_and_active_batch_rolls_back(self):
        for failure in ('execute', 'commit'):
            with self.subTest(failure=failure):
                rows = records(1001)
                connection = self.connection(rows, **{'fail_'+failure: 2})
                with self.assertLogs(LOG, level='CRITICAL') as logs:
                    with self.assertRaises(RuntimeError):
                        try:
                            write_member_stats(connection, rows, LOG)
                        except Exception:
                            connection.rollback()  # entry point owns rollback
                            raise
                self.assertIn('confirmed_committed_rows=1000', logs.output[0])
                self.assertIn('failed_batch_rows=1', logs.output[0])
                self.assertEqual(connection.db.execute(
                    'SELECT COUNT(*) FROM tdd_member_total_stat_record').fetchone()[0], 1000)
                self.assertEqual(connection.closed_cursors, 1)

    @unittest.skipUnless(_DEPS, 'database dependencies unavailable')
    def test_orm_and_sql_batches_have_identical_fields_and_latest(self):
        rows = records(1001)
        baseline = sqlite3.connect(':memory:')
        create_schema(baseline)
        baseline.executemany('INSERT INTO tdd_member VALUES (?, NULL)',
                             [(r['mid'],) for r in rows[:-1]])
        baseline.commit()
        engine = create_engine('sqlite://', creator=lambda: baseline)
        self.addCleanup(engine.dispose)
        session = sessionmaker(bind=engine)()
        self.addCleanup(session.close)
        for index, row in enumerate(rows, 1):
            session.add(TddMemberTotalStatRecord(**row))
            if index % 100 == 0:
                session.commit()
        session.commit()
        expected_rows = baseline.execute('SELECT * FROM tdd_member_total_stat_record ORDER BY id').fetchall()
        expected_latest = baseline.execute('SELECT * FROM tdd_member ORDER BY mid').fetchall()
        for batch_size in (100, 1000):
            connection = self.connection(rows)
            write_member_stats(connection, rows, LOG, batch_size)
            self.assertEqual(connection.db.execute('SELECT * FROM tdd_member_total_stat_record ORDER BY id').fetchall(), expected_rows)
            self.assertEqual(connection.db.execute('SELECT * FROM tdd_member ORDER BY mid').fetchall(), expected_latest)


if __name__ == '__main__':
    unittest.main()
