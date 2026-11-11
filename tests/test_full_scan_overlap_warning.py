"""Daily overlap warnings use the task's date and CST, including failed runs."""

import datetime
import logging
import os
import tempfile
import unittest
from unittest import mock

from test_remaining_script_run_records import _DEPS, _load

s51 = _load('51_hourly-video-record-add.py') if _DEPS else None
CST = datetime.timezone(datetime.timedelta(hours=8))


def stamp(value):
    return int(datetime.datetime.fromisoformat(value).timestamp() * 1000)


@unittest.skipUnless(_DEPS, 'db dependencies unavailable')
class FullScanOverlapWarningTest(unittest.TestCase):
    def test_before_and_exact_threshold_do_not_warn(self):
        for end in ['2026-11-11T04:44:59.999+08:00',
                    '2026-11-11T04:45:00+08:00']:
            with self.subTest(end=end), mock.patch.object(s51.logger, 'warning') as warning:
                s51.warn_full_scan_overlap('2026-11-11 04:00', stamp(end))
                warning.assert_not_called()

    def test_one_millisecond_late_warns_with_all_required_fields(self):
        with self.assertLogs('51', level='WARNING') as logs:
            s51.warn_full_scan_overlap('2026-11-11 04:00',
                                       stamp('2026-11-11T04:45:00.001+08:00'))
        self.assertEqual(len(logs.records), 1)
        message = logs.records[0].getMessage()
        for field in ['time_task=2026-11-11 04:00',
                      'end_time=2026-11-11T04:45:00.001+08:00',
                      'aggregate_start=2026-11-11T04:45:00+08:00',
                      'overlap_seconds=0.001', 'timezone=CST(UTC+08:00)']:
            self.assertIn(field, message)

    def test_cross_day_end_keeps_original_task_date(self):
        with self.assertLogs('51', level='WARNING') as logs:
            s51.warn_full_scan_overlap('2026-11-11 04:00',
                                       stamp('2026-11-12T00:00:00+08:00'))
        self.assertIn('aggregate_start=2026-11-11T04:45:00+08:00', logs.output[0])
        self.assertIn('overlap_seconds=69300.000', logs.output[0])

    def test_non_full_scan_hours_never_warn(self):
        for hour in range(24):
            if hour == 4:
                continue
            with self.subTest(hour=hour), mock.patch.object(s51.logger, 'warning') as warning:
                s51.warn_full_scan_overlap(f'2026-11-11 {hour:02}:00',
                                           stamp('2026-11-12T06:00:00+08:00'))
                warning.assert_not_called()

    def test_epoch_is_interpreted_in_cst_even_when_host_is_not(self):
        # 20:45 UTC on the previous day is 04:45 CST on the task's date.
        with self.assertLogs('51', level='WARNING') as logs:
            s51.warn_full_scan_overlap('2026-11-11 04:00',
                                       stamp('2026-11-10T20:46:00+00:00'))
        self.assertIn('end_time=2026-11-11T04:46:00.000+08:00', logs.output[0])
        self.assertIn('overlap_seconds=60.000', logs.output[0])

    def run_entry(self, end, failure=None, time_task='2026-11-11 04:00'):
        recorder = mock.Mock()
        with mock.patch.object(s51, 'get_ts_ms', side_effect=[
                stamp(time_task.replace(' ', 'T') + ':00+08:00'), stamp(end)]), \
                mock.patch.object(s51, 'get_ts_s_str', return_value=time_task + ':00'), \
                mock.patch.object(s51.RunRecorder, 'start', return_value=recorder), \
                mock.patch.object(s51, 'run_hourly_video_record_add', side_effect=failure):
            if failure:
                with self.assertRaises(SystemExit) as caught:
                    s51.hourly_video_record_add()
                self.assertEqual(caught.exception.code, 1)
            else:
                s51.hourly_video_record_add()
        recorder.finish.assert_called_once_with('failed' if failure else 'succeeded')

    def test_successful_slow_entry_reaches_warning_file(self):
        with tempfile.TemporaryDirectory() as folder:
            path = os.path.join(folder, 'warning.log')
            handler = logging.FileHandler(path, encoding='utf-8')
            handler.setLevel(logging.WARNING)
            s51.logger.addHandler(handler)
            try:
                self.run_entry('2026-11-11T04:46:00+08:00')
            finally:
                s51.logger.removeHandler(handler)
                handler.close()
            with open(path, encoding='utf-8') as stream:
                message = stream.read()
            self.assertEqual(message.count('Daily full scan ended'), 1)
            self.assertIn('overlap_seconds=60.000', message)

    def test_failed_slow_entry_warns_without_changing_failure_status(self):
        with self.assertLogs('51', level='WARNING') as logs:
            self.run_entry('2026-11-12T00:00:00+08:00', RuntimeError('analysis failed'))
        self.assertEqual([r.levelno for r in logs.records], [logging.CRITICAL, logging.WARNING])

    def test_fast_full_scan_and_other_hour_entries_do_not_warn(self):
        for task, end in [('2026-11-11 04:00', '2026-11-11T04:40:00+08:00'),
                          ('2026-11-11 05:00', '2026-11-11T05:30:00+08:00')]:
            with self.subTest(task=task), mock.patch.object(s51.logger, 'warning') as warning:
                self.run_entry(end, time_task=task)
                warning.assert_not_called()


if __name__ == '__main__':
    unittest.main()
