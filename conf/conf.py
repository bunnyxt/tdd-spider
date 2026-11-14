import configparser
import os

__all__ = ['CONFIG_PATH', 'CONFIG', 'get_db_args']
CONFIG_PATH = os.environ.get('TDD_CONFIG_PATH', os.path.join(os.path.dirname(__file__), 'conf.ini'))
CONFIG = configparser.ConfigParser(interpolation=None)
CONFIG.read(CONFIG_PATH)


def get_db_args():
    values = dict(CONFIG.items('db_postgresql')) if CONFIG.has_section('db_postgresql') else {}
    for field, variable in (('host','PGHOST'), ('port','PGPORT'), ('dbname','PGDATABASE'),
                            ('user','PGUSER'), ('password','PGPASSWORD'), ('sslmode','PGSSLMODE')):
        if variable in os.environ:
            values[field] = os.environ[variable]
    if not values.get('dbname'):
        raise RuntimeError('Configure [db_postgresql] or PGDATABASE before opening the database')
    values.setdefault('connect_timeout', 10)
    limits = []
    for name,default in (('lock_timeout',10000),('statement_timeout',60000),
                          ('transaction_timeout',120000)):
        value = int(os.environ.get('TDD_DB_' + name.upper() + '_MS',default))
        if not 1 <= value <= 300000:
            raise ValueError('Database connection timeouts must be 1..300000 milliseconds')
        limits.append((name,value))
    limits.append(('idle_in_transaction_session_timeout',60000))
    # Startup options protect ordinary SELECTs too. Append application limits
    # after existing options so an earlier zero cannot disable these bounds.
    options = values.get('options',os.environ.get('PGOPTIONS','')).strip()
    guards = ' '.join('-c ' + name + '=' + str(value) for name,value in limits)
    values['options'] = (options + ' ' + guards).strip()
    return values
