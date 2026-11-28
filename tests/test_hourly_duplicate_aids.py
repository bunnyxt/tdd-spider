"""Hourly downstream de-duplication preserves the original bvid/earliest rule."""
import unittest
from unittest import mock

from core import VideoRecord
from test_remaining_script_run_records import _DEPS, _load


@unittest.skipUnless(_DEPS, 'db dependencies unavailable')
class HourlyDuplicateAidsTest(unittest.TestCase):
    def run_pipeline(self, records, expected):
        script = _load('51_hourly-video-record-add.py')
        time_task = '2026-11-11 05:00'

        def acquire(task, queue):
            self.assertEqual(task, time_task)
            for record in records:
                queue.put(record)
            return mock.Mock(error=None, failure_counts={})

        with mock.patch.object(script, 'VideoRecordAcquisitionJob', side_effect=acquire), \
                mock.patch.object(script, 'RecordsSaveToFileRunner',
                                  return_value=mock.Mock(error=None, metrics={}, failure_count=0)) as save, \
                mock.patch.object(script, 'RecentRecordsAnalystRunner',
                                  return_value=mock.Mock(error=None, metrics={}, failure_count=0)) as analyze, \
                mock.patch.object(script, 'RecentActivityFreqUpdateRunner',
                                  return_value=mock.Mock(error=None, metrics={}, failure_count=0)) as activity, \
                self.assertLogs(script.logger, level='INFO') as logs:
            script.run_hourly_video_record_add(time_task)

        save.assert_called_once_with(expected, time_task)
        analyze.assert_called_once_with(expected, time_task)
        activity.assert_called_once_with(time_task, expected)
        for runner in (save, analyze, activity):
            runner.return_value.start.assert_called_once_with()
            runner.return_value.join.assert_called_once_with()
        return [entry.getMessage() for entry in logs.records]

    def test_same_aid_with_different_bvids_preserves_both_as_before(self):
        records = [VideoRecord(1, 7, 'first', 100, 1, 2, 3, 4, 5, 6),
                   VideoRecord(2, 7, 'second', 101, 1, 2, 3, 4, 5, 6)]
        messages = self.run_pipeline(records, records)
        self.assertIn('No duplicate records detected!', messages)

    def test_duplicate_bvid_keeps_earliest_record_before_every_downstream_stage(self):
        later = VideoRecord(3, 7, 'same', 300, 1, 2, 3, 4, 5, 6)
        other = VideoRecord(2, 9, 'other', 200, 1, 2, 3, 4, 5, 6)
        earlier = VideoRecord(1, 8, 'same', 100, 1, 2, 3, 4, 5, 6)
        messages = self.run_pipeline([later, other, earlier], [other, earlier])
        self.assertIn('Duplicate records detected! 1 videos with 2 records in total.', messages)
        self.assertTrue(any('1 duplicate records removed' in message for message in messages))
        self.assertFalse(any('All confirmed observations will be retained' in message for message in messages))

    def test_same_second_duplicate_bvid_keeps_first_arrival(self):
        first = VideoRecord(1, 7, 'same', 100, 1, 2, 3, 4, 5, 6)
        second = first._replace(view=200)
        self.run_pipeline([first, second], [first])


if __name__ == '__main__':
    unittest.main()
