"""The selected INI owns collector connections and shared finite deadlines."""
import configparser
from contextlib import contextmanager
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from psycopg import pq
from psycopg.conninfo import make_conninfo
from conf.conf import get_db_args
from db.ingest import _set_transaction_limits


def option_settings(options):
    tokens = shlex.split(options)
    settings = {}
    for index, token in enumerate(tokens):
        if token == '-c':
            name, value = tokens[index + 1].split('=', 1)
            settings[name] = value
    return settings


class PostgreSQLConfigTest(unittest.TestCase):
    @contextmanager
    def configured(self, values=None, environment=None):
        config = configparser.ConfigParser(interpolation=None)
        config['db_postgresql'] = {'dbname': 'disposable', 'user': 'fixture', 'password': 'fixture-password',
                                   **(values or {})}
        with mock.patch('conf.conf.CONFIG', config), \
             mock.patch.dict(os.environ, environment or {}, clear=True):
            yield config

    def test_default_connections_apply_read_and_idle_deadlines(self):
        with self.configured():
            args = get_db_args()
        self.assertEqual((args['host'], args['hostaddr'], args['port'], args['sslmode'], args['connect_timeout']),
                         ('127.0.0.1', '', 5432, 'prefer', 10))
        self.assertEqual(option_settings(args['options']), {
            'lock_timeout': '10000', 'statement_timeout': '60000', 'transaction_timeout': '120000',
            'idle_in_transaction_session_timeout': '60000'})

    def test_ini_timeouts_override_earlier_zero_options_at_both_application_sites(self):
        values = {'lock_timeout_ms': '1', 'statement_timeout_ms': '300000',
                  'transaction_timeout_ms': '120001',
                  'options': '-c search_path=custom -c statement_timeout=0 -c lock_timeout=0 '
                             '-c transaction_timeout=0 -c idle_in_transaction_session_timeout=0'}
        with self.configured(values):
            settings = option_settings(get_db_args()['options'])
            conn = mock.Mock()
            _set_transaction_limits(conn)
        self.assertEqual(settings, {
            'search_path': 'custom', 'lock_timeout': '1', 'statement_timeout': '300000',
            'transaction_timeout': '120001', 'idle_in_transaction_session_timeout': '60000'})
        query, params = conn.execute.call_args.args
        self.assertEqual(query, "SELECT set_config('lock_timeout',%s,true),"
                                "set_config('statement_timeout',%s,true),"
                                "set_config('transaction_timeout',%s,true)")
        self.assertEqual(params, ['1ms', '300000ms', '120001ms'])

    def test_inherited_target_credentials_options_and_timeouts_do_not_override_ini(self):
        env = {'PGHOST': 'wrong.example', 'PGHOSTADDR': '192.0.2.1', 'PGPORT': '9999',
               'PGDATABASE': 'wrong', 'PGUSER': 'wrong', 'PGPASSWORD': 'wrong-password',
               'PGSSLMODE': 'require', 'PGOPTIONS': '-c statement_timeout=0',
               'PGCONNECT_TIMEOUT': '999', 'TDD_DB_LOCK_TIMEOUT_MS': '0',
               'TDD_DB_STATEMENT_TIMEOUT_MS': '0', 'TDD_DB_TRANSACTION_TIMEOUT_MS': '0'}
        with self.configured(environment=env):
            args = get_db_args()
            conn = mock.Mock()
            _set_transaction_limits(conn)
            # An invalid SSL mode stops libpq before any socket is opened. Its
            # resolved connection info still reveals hidden environment fallback.
            probe = pq.PGconn.connect_start(make_conninfo(**{**args, 'sslmode': 'invalid-test-mode'}).encode())
            try:
                resolved = {o.keyword.decode(): o.val.decode() if o.val is not None else None for o in probe.info}
                self.assertIn(b'invalid sslmode', probe.error_message)
            finally:
                probe.finish()
        self.assertEqual([resolved[k] for k in ('host', 'hostaddr', 'port', 'dbname', 'user', 'password')],
                         ['127.0.0.1', '', '5432', 'disposable', 'fixture', 'fixture-password'])
        self.assertEqual(args['sslmode'], 'prefer')
        self.assertEqual(resolved['connect_timeout'], '10')
        self.assertEqual(option_settings(resolved['options'])['statement_timeout'], '60000')
        self.assertEqual(conn.execute.call_args.args[1], ['10000ms', '60000ms', '120000ms'])

    def test_unconfigured_external_libpq_options_raise_instead_of_silently_applying(self):
        for variable in ('PGSERVICE', 'PGSSLROOTCERT', 'PGAPPNAME', 'PGREQUIREAUTH'):
            with self.subTest(variable=variable), self.configured(environment={variable: 'external'}):
                with self.assertRaisesRegex(ValueError, variable):
                    get_db_args()
        with self.configured({'sslrootcert': '/configured/ca.pem'}, {'PGSSLROOTCERT': '/wrong/ca.pem'}):
            self.assertEqual(get_db_args()['sslrootcert'], '/configured/ca.pem')
        with self.configured({'service': 'external'}):
            with self.assertRaisesRegex(ValueError, 'Named services'):
                get_db_args()

    def test_missing_required_ini_settings_cannot_fall_back_to_environment(self):
        with mock.patch('conf.conf.CONFIG', configparser.ConfigParser(interpolation=None)), \
             mock.patch.dict(os.environ, {'PGDATABASE': 'wrong'}, clear=True):
            with self.assertRaisesRegex(RuntimeError, 'selected INI'):
                get_db_args()
        for field in ('dbname', 'user', 'password', 'host', 'port', 'sslmode'):
            with self.subTest(field=field), self.configured({field: ''}):
                with self.assertRaisesRegex(ValueError, field):
                    get_db_args()

    def test_invalid_timeouts_fail_before_connection_or_transaction_sql(self):
        for name in ('lock_timeout', 'statement_timeout', 'transaction_timeout'):
            for value in ('0', '-1', '300001', '1.5', 'invalid', ''):
                with self.subTest(name=name, value=value), self.configured({name + '_ms': value}):
                    with self.assertRaises(ValueError):
                        get_db_args()
                    conn = mock.Mock()
                    with self.assertRaises(ValueError):
                        _set_transaction_limits(conn)
                    conn.execute.assert_not_called()
        for value in ('0', '-1', '301'):
            with self.configured({'connect_timeout': value}), self.assertRaises(ValueError):
                get_db_args()

    def test_config_path_selects_the_file_before_import(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'collector.ini'
            path.write_text('[db_postgresql]\ndbname = chosen\nuser = chosen_user\npassword = chosen_password\n'
                            'statement_timeout_ms = 12345\n')
            result = subprocess.run([sys.executable, '-c',
                'import json; from conf import get_db_args; print(json.dumps(get_db_args()))'],
                env={**os.environ, 'TDD_CONFIG_PATH': str(path), 'PGDATABASE': 'wrong'},
                capture_output=True, text=True, check=True)
        args = json.loads(result.stdout)
        self.assertEqual((args['dbname'], args['user']), ('chosen', 'chosen_user'))
        self.assertEqual(option_settings(args['options'])['statement_timeout'], '12345')
