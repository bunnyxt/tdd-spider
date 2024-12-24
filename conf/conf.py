import configparser
import os

__all__ = ['CONFIG_PATH', 'CONFIG',
           'get_db_args', 'get_sckey', 'get_proxy_pool_url']

# use config parser to load config
CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'conf.ini')
CONFIG = configparser.ConfigParser()
CONFIG.read(CONFIG_PATH)


def get_db_args():
    return dict(CONFIG.items('db_mysql'))


def get_sckey():
    return CONFIG.get('serverchan', 'sckey')


def get_proxy_pool_url():
    return CONFIG.get('proxy', 'proxy_pool_url')
