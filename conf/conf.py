import configparser
import os

__all__ = ['CONFIG_PATH', 'CONFIG', 'get_db_args', 'get_db_timeouts', 'get_db_pool_max_size']
CONFIG_PATH = os.environ.get('TDD_CONFIG_PATH', os.path.join(os.path.dirname(__file__), 'conf.ini'))
CONFIG = configparser.ConfigParser(interpolation=None)
CONFIG.read(CONFIG_PATH)


def get_db_pool_max_size():
    """Maximum PostgreSQL connections held by this collector process."""
    try:
        value = CONFIG.getint('db_postgresql', 'pool_max_size', fallback=4)
    except ValueError as exc:
        raise ValueError('Database pool_max_size must be a positive integer') from exc
    if value < 1:
        raise ValueError('Database pool_max_size must be a positive integer')
    return value


def get_db_timeouts():
    """Shared millisecond limits for connection startup and write transactions."""
    limits = {}
    for name, default in (('lock_timeout', 10000), ('statement_timeout', 60000),
                          ('transaction_timeout', 120000)):
        value = CONFIG.getint('db_postgresql', name + '_ms', fallback=default)
        if not 1 <= value <= 300000:
            raise ValueError('Database timeouts must be 1..300000 milliseconds')
        limits[name] = value
    return limits


def get_db_args():
    from psycopg import pq

    if not CONFIG.has_section('db_postgresql'):
        raise RuntimeError('Configure [db_postgresql] in the selected INI file before opening the database')
    values = dict(CONFIG.items('db_postgresql'))
    # Collector pool options are not libpq connection parameters.
    values.pop('pool_max_size', None)
    for field in ('dbname', 'user', 'password'):
        if not values.get(field):
            raise ValueError('[db_postgresql] requires a nonempty ' + field)
    if 'service' in values or 'PGSERVICE' in os.environ:
        raise ValueError('Named services are not supported by the collector; unset PGSERVICE and use [db_postgresql]')
    for name, default in (('host', '127.0.0.1'), ('hostaddr', ''), ('port', '5432'),
                          ('sslmode', 'prefer'), ('connect_timeout', '10')):
        values.setdefault(name, default)
    for field in ('host', 'port', 'sslmode'):
        if not values[field].strip():
            raise ValueError('[db_postgresql] requires a nonempty ' + field)
    port = int(values['port'])
    if not 1 <= port <= 65535:
        raise ValueError('Database port must be 1..65535')
    values['port'] = port
    connect_timeout = int(values['connect_timeout'])
    if not 1 <= connect_timeout <= 300:
        raise ValueError('Database connect_timeout must be 1..300 seconds')
    values['connect_timeout'] = connect_timeout
    limits = get_db_timeouts()
    for name in limits:
        values.pop(name + '_ms', None)
    limits['idle_in_transaction_session_timeout'] = 60000
    # Startup options protect ordinary SELECTs too. Append application limits
    # after existing options so an earlier zero cannot disable these bounds.
    options = values.get('options', '').strip()
    guards = ' '.join('-c ' + name + '=' + str(value) for name, value in limits.items())
    values['options'] = (options + ' ' + guards).strip()
    # libpq also consults PGHOSTADDR, SSL/auth settings and other defaults.
    # Never let an unspecified INI option silently inherit one of those values.
    # parse(b'') exposes option metadata without loading environment/service defaults.
    for option in pq.Conninfo.parse(b''):
        if option.envvar and option.keyword.decode() not in values:
            variable = option.envvar.decode()
            if variable in os.environ:
                raise ValueError('Unset ' + variable + ' or configure its setting in [db_postgresql]')
    return values
