"""Hourly CSV failures do not stop packing, and cleanup requires an archive."""
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest
from unittest import mock

from core import VideoRecord
from test_remaining_script_run_records import _DEPS, _load

s51 = _load('51_hourly-video-record-add.py') if _DEPS else None
DAY = '2026-09-13'
OLD_DAY = '2026-09-10'
NOW = 1000000


@unittest.skipUnless(_DEPS, 'db dependencies unavailable')
class HourlyFilePackingTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        previous = os.getcwd()
        os.chdir(temporary.name)
        self.addCleanup(os.chdir, previous)
        self.folder = Path('data with spaces')
        self.folder.mkdir()
        self.record = VideoRecord(1, 7, 'BV7', 100, 1, 2, 3, 4, 5, 6)
        for patcher in (
                mock.patch.object(s51, 'get_ts_s', return_value=NOW),
                mock.patch.object(s51, 'ts_s_to_str', side_effect=lambda value:
                                  DAY + ' 23:00:00' if value == NOW else OLD_DAY + ' 23:00:00')):
            patcher.start()
            self.addCleanup(patcher.stop)

    def runner(self, hour='23:00'):
        return s51.RecordsSaveToFileRunner([self.record], f'{DAY} {hour}', str(self.folder))

    def test_csv_failure_still_packs_earlier_files_with_original_archive_layout(self):
        earlier = self.folder / f'{DAY} 22:00.csv'
        earlier.write_text('earlier records\n')
        runner = self.runner()
        with mock.patch.object(runner, '_save_records', side_effect=OSError('CSV disk error')), \
                self.assertLogs(runner.logger, level='ERROR'):
            runner.run()
        self.assertIsNone(runner.error)
        self.assertEqual(runner.failure_count, 1)
        self.assertEqual(runner.metrics, {'csv_save_failed': 1})
        with tarfile.open(self.folder / f'{DAY}.tar.gz') as archive:
            self.assertEqual(archive.extractfile(f'{DAY}/{earlier.name}').read(), b'earlier records\n')
            self.assertIn(DAY, archive.getnames())
        self.assertTrue(earlier.exists())
        self.assertFalse((self.folder / DAY).exists())

    def test_csv_failure_skips_0400_snapshot(self):
        runner = self.runner('04:00')
        with mock.patch.object(runner, '_save_records', side_effect=OSError('CSV disk error')), \
                mock.patch.object(runner, '_save_snapshot') as snapshot, \
                self.assertLogs(runner.logger, level='ERROR'):
            runner.run()
        snapshot.assert_not_called()
        self.assertEqual(runner.metrics, {'csv_save_failed': 1})

    def test_nonzero_packing_command_counts_failure_and_never_reports_success(self):
        runner = self.runner()
        def failed_tar(*args, **kwargs):
            (self.folder / f'{DAY}.tar.gz.tmp').write_bytes(b'partial archive')
            raise subprocess.CalledProcessError(2, args[0])
        with mock.patch.object(s51.subprocess, 'run', side_effect=failed_tar) as command, \
                self.assertLogs(runner.logger, level='INFO') as logs:
            runner.run()
        command.assert_called_once()
        self.assertTrue(command.call_args.kwargs['check'])
        self.assertIsNone(runner.error)
        self.assertEqual(runner.metrics, {'pack_failed': 1})
        self.assertEqual(runner.failure_count, 1)
        self.assertFalse((self.folder / f'{DAY}.tar.gz').exists())
        self.assertFalse(any('Finish packing' in entry for entry in logs.output))

    def test_missing_or_empty_archive_after_zero_exit_counts_failure(self):
        for empty in (False, True):
            with self.subTest(empty=empty):
                folder = self.folder / str(empty)
                folder.mkdir()
                previous_archive = folder / f'{DAY}.tar.gz'
                previous_archive.write_bytes(b'previous archive')
                runner = s51.RecordsSaveToFileRunner([self.record], f'{DAY} 23:00', str(folder))
                def no_archive(*args, **kwargs):
                    if empty:
                        (folder / f'{DAY}.tar.gz.tmp').touch()
                    return subprocess.CompletedProcess(args[0], 0, '', '')
                with mock.patch.object(s51.subprocess, 'run', side_effect=no_archive), \
                        self.assertLogs(runner.logger, level='ERROR'):
                    runner.run()
                self.assertEqual(runner.metrics, {'pack_failed': 1})
                self.assertEqual(runner.failure_count, 1)
                self.assertEqual(previous_archive.read_bytes(), b'previous archive')

    def test_existing_staging_directory_is_a_reported_packing_failure(self):
        (self.folder / DAY).mkdir()
        runner = self.runner()
        with mock.patch.object(s51.subprocess, 'run') as command, \
                self.assertLogs(runner.logger, level='ERROR'):
            runner.run()
        command.assert_not_called()
        self.assertEqual(runner.metrics, {'pack_failed': 1})

    def test_old_csv_is_kept_when_its_archive_is_missing_or_empty(self):
        for empty in (False, True):
            with self.subTest(empty=empty):
                folder = self.folder / str(empty)
                folder.mkdir()
                old_csv = folder / f'{OLD_DAY} 10:00.csv'
                old_csv.write_text('unarchived records\n')
                if empty:
                    (folder / f'{OLD_DAY}.tar.gz').touch()
                runner = s51.RecordsSaveToFileRunner([self.record], f'{DAY} 23:00', str(folder))
                with self.assertLogs(runner.logger, level='ERROR'):
                    runner.run()
                self.assertEqual(old_csv.read_text(), 'unarchived records\n')
                self.assertEqual(runner.metrics, {'cleanup_failed': 1})
                self.assertEqual(runner.failure_count, 1)
                self.assertTrue((folder / f'{DAY}.tar.gz').is_file())

    def test_only_three_day_old_csvs_with_a_nonempty_archive_are_deleted(self):
        old_csv = self.folder / f'{OLD_DAY} 10:00.csv'
        old_csv.write_text('archived records\n')
        archive_path = self.folder / f'{OLD_DAY}.tar.gz'
        with tarfile.open(archive_path, 'w:gz') as archive:
            archive.add(old_csv, arcname=f'{OLD_DAY}/{old_csv.name}')
        newer_csv = self.folder / '2026-09-11 10:00.csv'
        newer_csv.write_text('newer records\n')
        runner = self.runner()
        runner.run()
        self.assertFalse(old_csv.exists())
        self.assertTrue(archive_path.exists())
        self.assertTrue(newer_csv.exists())
        self.assertEqual(runner.failure_count, 0)
        self.assertEqual(runner.metrics, {})


if __name__ == '__main__':
    unittest.main()
