"""Hourly duplicate warnings use aid and leave confirmed observations intact."""
import unittest
from unittest import mock

from core import VideoRecord
from test_remaining_script_run_records import _DEPS, _load


@unittest.skipUnless(_DEPS, 'db dependencies unavailable')
class HourlyDuplicateAidsTest(unittest.TestCase):
    def run_pipeline(self, records):
        script = _load('51_hourly-video-record-add.py')
        time_task = '2026-11-11 05:00'

        def acquire(task, queue):
            self.assertEqual(task, time_task)
            for record in records:
                queue.put(record)
            return mock.Mock(error=None)

        with mock.patch.object(script, 'VideoRecordAcquisitionJob', side_effect=acquire), \
                mock.patch.object(script, 'RecordsSaveToFileRunner',
                                  return_value=mock.Mock(error=None)) as save, \
                mock.patch.object(script, 'RecentRecordsAnalystRunner',
                                  return_value=mock.Mock(error=None)) as analyze, \
                mock.patch.object(script, 'RecentActivityFreqUpdateRunner',
                                  return_value=mock.Mock(error=None)) as activity, \
                self.assertLogs(script.logger, level='INFO') as logs:
            script.run_hourly_video_record_add(time_task)

        save.assert_called_once_with(records, time_task)
        analyze.assert_called_once_with(records, time_task)
        activity.assert_called_once_with(time_task, records)
        for runner in (save, analyze, activity):
            runner.return_value.start.assert_called_once_with()
            runner.return_value.join.assert_called_once_with()
        return [entry.getMessage() for entry in logs.records]

    def test_same_aid_with_different_bvids_warns_and_keeps_every_observation(self):
        records = [VideoRecord(1, 7, 'first', 100, 1, 2, 3, 4, 5, 6),
                   VideoRecord(2, 7, 'second', 101, 1, 2, 3, 4, 5, 6),
                   VideoRecord(3, 8, 'third', 200, 1, 2, 3, 4, 5, 6)]
        messages = self.run_pipeline(records)
        self.assertIn('Duplicate records detected! 1 videos with 2 records in total.', messages)
        self.assertIn('All confirmed observations will be retained in the downstream files.', messages)

    def test_different_aids_with_same_bvid_are_not_duplicates(self):
        records = [VideoRecord(1, 7, 'same', 100, 1, 2, 3, 4, 5, 6),
                   VideoRecord(2, 8, 'same', 200, 1, 2, 3, 4, 5, 6)]
        messages = self.run_pipeline(records)
        self.assertIn('No duplicate records detected!', messages)
        self.assertFalse(any(message.startswith('Duplicate records detected!')
                             for message in messages))
