"""Cron entrypoints report failures and pass connection/run settings to psql.

The fake client checks process behavior only; SQL control and refresh semantics
still require validation against the installed PostgreSQL maintenance functions.
"""
import json
import os
from pathlib import Path
import shlex
import subprocess
import tempfile
import unittest
from uuid import uuid4


JOBS = Path(__file__).resolve().parents[1] / 'scripts' / 'postgresql'


class MaintenanceCommandTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.capture = self.root / 'arguments.json'
        client = self.root / 'psql'
        client.write_text(
            '#!/usr/bin/env python3\n'
            'import json, os, sys\n'
            'from pathlib import Path\n'
            'Path(os.environ["CAPTURE"]).write_text(json.dumps({\n'
            '    "args": sys.argv[1:], "connect_timeout": os.environ.get("PGCONNECT_TIMEOUT"),\n'
            '    "service": os.environ.get("PGSERVICE")}))\n'
            'print("simulated SQL result")\n'
            'raise SystemExit(int(os.environ.get("CLIENT_EXIT", "0")))\n',
            encoding='utf-8')
        client.chmod(0o700)
        self.environment = {**os.environ, 'PATH': str(self.root) + os.pathsep + os.environ['PATH'],
                            'CAPTURE': str(self.capture), 'PGSERVICE': 'maintenance-test'}
        self.environment.pop('TDD_TRIAL_RUN_ID', None)

    def run_job(self, job, **environment):
        return subprocess.run(['bash', str(JOBS / 'run-maintenance.sh'), job],
                              cwd=self.root, env={**self.environment, **environment},
                              capture_output=True, text=True, check=False)

    def test_success_is_after_client_and_sql_resolves_outside_checkout(self):
        run_id = str(uuid4())
        result = self.run_job('refresh-member-pool', TDD_TRIAL_RUN_ID=run_id)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertLess(result.stdout.index('START'), result.stdout.index('simulated SQL result'))
        self.assertLess(result.stdout.index('simulated SQL result'), result.stdout.index('SUCCESS'))
        self.assertNotIn('ERROR', result.stderr)
        arguments = json.loads(self.capture.read_text())
        self.assertEqual(arguments['service'], 'maintenance-test')
        self.assertEqual(arguments['connect_timeout'], '10')
        self.assertIn('--set=ON_ERROR_STOP=1', arguments['args'])
        self.assertIn('--no-password', arguments['args'])
        self.assertIn('-X', arguments['args'])
        self.assertIn('--set=trial_run_id=' + run_id, arguments['args'])
        self.assertIn('--file=' + str(JOBS / 'refresh-member-pool.sql'), arguments['args'])

    def test_sql_or_connection_failure_is_nonzero_and_never_success(self):
        for exit_code in ('2', '3'):
            with self.subTest(exit_code=exit_code):
                result = self.run_job('refresh-video-pool', CLIENT_EXIT=exit_code)
                self.assertEqual(result.returncode, int(exit_code))
                self.assertIn('START job=refresh-video-pool', result.stdout)
                self.assertNotIn('SUCCESS', result.stdout)
                self.assertIn('ERROR job=refresh-video-pool', result.stderr)
                self.assertIn('exit=' + exit_code, result.stderr)

    def test_missing_service_fails_before_client_with_error_log(self):
        self.environment.pop('PGSERVICE')
        result = self.run_job('ensure-partitions')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('ERROR job=ensure-partitions', result.stderr)
        self.assertNotIn('SUCCESS', result.stdout)
        self.assertFalse(self.capture.exists())

    def test_unknown_job_cannot_select_an_arbitrary_file(self):
        result = self.run_job('../arbitrary')
        self.assertEqual(result.returncode, 64)
        self.assertIn('ERROR usage:', result.stderr)
        self.assertFalse(self.capture.exists())

    def test_formal_run_passes_an_explicit_empty_trial_id(self):
        result = self.run_job('ensure-partitions')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('--set=trial_run_id=', json.loads(self.capture.read_text())['args'])

    def test_cron_entries_call_expected_jobs_at_approved_shanghai_times(self):
        # Parse actual command lines so a schedule pointing at the wrong job or
        # an absent file fails, rather than merely searching for three times.
        entries = {}
        for line in (JOBS / 'crontab.example').read_text().splitlines():
            tokens = shlex.split(line, comments=True)
            if not tokens or tokens[0].startswith('#') or '=' in tokens[0]:
                continue
            minute, hour, day, month, weekday, *command = tokens
            self.assertEqual((day, month, weekday), ('*', '*', '*'))
            self.assertEqual(command[:2], ['bash', '/path/to/spider/scripts/postgresql/run-maintenance.sh'])
            entries[command[2]] = (int(hour), int(minute))
            self.assertTrue((JOBS / (command[2] + '.sql')).is_file())
        self.assertEqual(entries, {'ensure-partitions': (2, 35),
                                  'refresh-member-pool': (5, 45),
                                  'refresh-video-pool': (9, 45)})


if __name__ == '__main__':
    unittest.main()
