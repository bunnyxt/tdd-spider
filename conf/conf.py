import configparser
import os

# use config parser to load config
CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'conf.ini')
CONFIG = configparser.ConfigParser()
CONFIG.read(CONFIG_PATH)


def get_db_args():
    return dict(CONFIG.items('db_mysql'))


def get_library_cookie():
    return CONFIG.get('library', 'cookie')


if __name__ == '__main__':
    print(get_library_cookie())
