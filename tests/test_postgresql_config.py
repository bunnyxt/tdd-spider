"""Ordinary connections retain finite query deadlines before any SQL runs."""
import configparser
import os
import shlex
import unittest
from unittest import mock

from conf.conf import get_db_args
from db.ingest import _set_transaction_limits


def option_settings(options):
    tokens = shlex.split(options)
    settings = {}
    for index,token in enumerate(tokens):
        if token == '-c':
            name,value = tokens[index+1].split('=',1)
            settings[name] = value
    return settings


class PostgreSQLConfigTest(unittest.TestCase):
    def args(self,environment=None,options=None):
        config = configparser.ConfigParser(interpolation=None)
        if options is not None:
            config['db_postgresql'] = {'options':options}
        with mock.patch('conf.conf.CONFIG',config), \
             mock.patch.dict(os.environ,{'PGDATABASE':'disposable',**(environment or {})},clear=True):
            return get_db_args()

    def test_default_connections_apply_read_and_idle_deadlines(self):
        args = self.args()
        self.assertEqual(args['connect_timeout'],10)
        self.assertEqual(option_settings(args['options']),{
            'lock_timeout':'10000','statement_timeout':'60000','transaction_timeout':'120000',
            'idle_in_transaction_session_timeout':'60000'})

    def test_finite_environment_overrides_are_applied_after_legacy_options(self):
        args = self.args({'TDD_DB_LOCK_TIMEOUT_MS':'1','TDD_DB_STATEMENT_TIMEOUT_MS':'300000',
                          'TDD_DB_TRANSACTION_TIMEOUT_MS':'120001'},
                         '-c search_path=custom -c statement_timeout=0 -c lock_timeout=0 '
                         '-c transaction_timeout=0 -c idle_in_transaction_session_timeout=0')
        self.assertEqual(option_settings(args['options']),{
            'search_path':'custom','lock_timeout':'1','statement_timeout':'300000','transaction_timeout':'120001',
            'idle_in_transaction_session_timeout':'60000'})

    def test_pgoptions_remains_usable_but_cannot_disable_bounds(self):
        args = self.args({'PGOPTIONS':'-c application_name=replay -c statement_timeout=0'})
        settings = option_settings(args['options'])
        self.assertEqual(settings['application_name'],'replay')
        self.assertEqual(settings['statement_timeout'],'60000')

    def test_invalid_or_unbounded_deadlines_are_rejected(self):
        for name in ('LOCK','STATEMENT','TRANSACTION'):
            for value in ('0','-1','300001','1.5','invalid',''):
                with self.subTest(name=name,value=value),self.assertRaises(ValueError):
                    self.args({'TDD_DB_'+name+'_TIMEOUT_MS':value})

    def test_connection_and_transaction_share_default_and_override_policy(self):
        for environment in ({}, {'TDD_DB_LOCK_TIMEOUT_MS': '1',
                                 'TDD_DB_STATEMENT_TIMEOUT_MS': '300000',
                                 'TDD_DB_TRANSACTION_TIMEOUT_MS': '120001'}):
            with self.subTest(environment=environment):
                settings = option_settings(self.args(environment)['options'])
                conn = mock.Mock()
                with mock.patch.dict(os.environ, environment, clear=True):
                    _set_transaction_limits(conn)
                query, values = conn.execute.call_args.args
                self.assertEqual(query,
                                 "SELECT set_config('lock_timeout',%s,true),"
                                 "set_config('statement_timeout',%s,true),"
                                 "set_config('transaction_timeout',%s,true)")
                self.assertEqual(values, [settings[name] + 'ms' for name in
                                         ('lock_timeout', 'statement_timeout', 'transaction_timeout')])

    def test_invalid_transaction_deadline_fails_before_sql(self):
        for name in ('LOCK', 'STATEMENT', 'TRANSACTION'):
            for value in ('0', '-1', '300001', '1.5', 'invalid', ''):
                with self.subTest(name=name, value=value):
                    conn = mock.Mock()
                    with mock.patch.dict(os.environ, {'TDD_DB_' + name + '_TIMEOUT_MS': value}, clear=True):
                        with self.assertRaises(ValueError):
                            _set_transaction_limits(conn)
                    conn.execute.assert_not_called()
