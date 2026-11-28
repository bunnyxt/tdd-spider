"""Opt-in classification regression; all fixtures and writes use TEMP tables."""
import os
import unittest

from db.basic import engine
from db.hourly import _update_video_freq, _update_video_recent
from db.values import timestamp


@unittest.skipUnless(os.environ.get('TDD_PG_INTEGRATION') == '1',
                     'requires an explicitly selected PostgreSQL test connection')
class HourlyClassificationTest(unittest.TestCase):
    def setUp(self):
        self.connection_context = engine.connection()
        self.conn = self.connection_context.__enter__()
        self.addCleanup(self.connection_context.__exit__, None, None, None)
        self.conn.execute('BEGIN')
        self.addCleanup(self.conn.execute, 'ROLLBACK')
        self.conn.execute('CREATE TEMP TABLE tdd_video '
                          '(aid bigint PRIMARY KEY, added timestamptz, '
                          'recent integer, activity integer, freq integer) ON COMMIT DROP')
        self.conn.execute('CREATE TEMP TABLE touched (aid bigint) ON COMMIT DROP')
        self.conn.execute('''CREATE FUNCTION pg_temp.track_updates() RETURNS trigger
            LANGUAGE plpgsql AS $$ BEGIN
                INSERT INTO touched VALUES (NEW.aid);
                RETURN NEW;
            END $$''')
        self.conn.execute('CREATE TRIGGER track_updates AFTER UPDATE ON pg_temp.tdd_video '
                          'FOR EACH ROW EXECUTE FUNCTION pg_temp.track_updates()')

    def test_recent_boundaries_and_repeated_run_do_not_touch_unchanged_rows(self):
        now = 1800000000
        seven_days, one_day = now - 7 * 86400, now - 86400
        expected, changes = [], 0
        for added in (seven_days - 1, seven_days, one_day - 1, one_day, now, now + 1):
            value = 0 if added < seven_days else 1 if added < one_day else 2
            for previous in (None, -1, 0, 1, 2):
                aid = len(expected) + 1
                expected.append((aid, value))
                changes += previous != value
                self.conn.execute('INSERT INTO tdd_video VALUES (%s,%s,%s,0,0)',
                                  (aid, timestamp(added), previous))
        _update_video_recent(self.conn, now)
        self.assertEqual(self.conn.execute('SELECT aid,recent FROM tdd_video ORDER BY aid').fetchall(), expected)
        self.assertEqual(self.conn.execute('SELECT count(*) FROM touched').fetchone()[0], changes)
        self.conn.execute('TRUNCATE touched')
        _update_video_recent(self.conn, now)
        self.assertEqual(self.conn.execute('SELECT count(*) FROM touched').fetchone()[0], 0)

    def test_freq_matches_legacy_precedence_and_updates_each_changed_row_once(self):
        expected, changes = [], 0
        for activity in (None, -1, 0, 1, 2, 3):
            for recent in (None, -1, 0, 1, 2):
                # Independently reproduce the three legacy assignments in order.
                value = 0
                if activity == 1:
                    value = 1
                if activity == 2 or recent == 1:
                    value = 2
                for previous in (None, -1, 0, 1, 2, 3):
                    aid = len(expected) + 1
                    expected.append((aid, value))
                    changes += previous != value
                    self.conn.execute('INSERT INTO tdd_video VALUES (%s,%s,%s,%s,%s)',
                                      (aid, timestamp(1800000000), recent, activity, previous))
        _update_video_freq(self.conn)
        self.assertEqual(self.conn.execute('SELECT aid,freq FROM tdd_video ORDER BY aid').fetchall(), expected)
        self.assertEqual(self.conn.execute('SELECT count(*) FROM touched').fetchone()[0], changes)
        self.assertEqual(self.conn.execute('SELECT count(DISTINCT aid) FROM touched').fetchone()[0], changes)
        self.conn.execute('TRUNCATE touched')
        _update_video_freq(self.conn)
        self.assertEqual(self.conn.execute('SELECT count(*) FROM touched').fetchone()[0], 0)
