import logging
import logging.config
import os

__all__ = ['BASE_DIR', 'LOG_DIR', 'logger_01', 'logger_11', 'logger_11_c0', 'logger_11_c30',
           'logger_12', 'logger_13', 'logger_14', 'logger_15', 'logger_db']

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
LOG_DIR = os.path.join(BASE_DIR, 'log')
if not os.path.exists(LOG_DIR):
    os.mkdir(LOG_DIR)


class InfoPlusFilter(logging.Filter):
    def filter(self, record):
        if record.levelno >= logging.INFO:
            return super().filter(record)
        else:
            return 0


class WarningPlusFilter(logging.Filter):
    def filter(self, record):
        if record.levelno >= logging.WARNING:
            return super().filter(record)
        else:
            return 0


LOG_CONFIG_DICT = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'simple': {
            'class': 'logging.Formatter',
            'format': '[%(asctime)s][%(name)s][%(levelname)s]: %(message)s'
        }
    },
    'filters': {
        'info_plus_filter': {
            '()': InfoPlusFilter
        },
        'warning_plus_filter': {
            '()': WarningPlusFilter
        }
    },
    'handlers': {
        'console_info': {
            'level': 'INFO',
            'formatter': 'simple',
            'class': 'logging.StreamHandler'
        },
        'file_info_01': {
            'level': 'INFO',
            'formatter': 'simple',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': os.path.join(LOG_DIR, '01_info.log'),
            'when': "d",
            'interval': 1,
            'encoding': 'utf8',
            'backupCount': 30,
            'filters': ['info_plus_filter']
        },
        'file_warning_01': {
            'level': 'WARNING',
            'formatter': 'simple',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': os.path.join(LOG_DIR, '01_warning.log'),
            'when': "d",
            'interval': 1,
            'encoding': 'utf8',
            'backupCount': 30,
            'filters': ['warning_plus_filter']
        },
        'file_info_11': {
            'level': 'INFO',
            'formatter': 'simple',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': os.path.join(LOG_DIR, '11_info.log'),
            'when': "d",
            'interval': 1,
            'encoding': 'utf8',
            'backupCount': 30,
            'filters': ['info_plus_filter']
        },
        'file_warning_11': {
            'level': 'WARNING',
            'formatter': 'simple',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': os.path.join(LOG_DIR, '11_warning.log'),
            'when': "d",
            'interval': 1,
            'encoding': 'utf8',
            'backupCount': 30,
            'filters': ['warning_plus_filter']
        },
        'file_info_11_c0': {
            'level': 'INFO',
            'formatter': 'simple',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': os.path.join(LOG_DIR, '11_info_c0.log'),
            'when': "d",
            'interval': 1,
            'encoding': 'utf8',
            'backupCount': 30,
            'filters': ['info_plus_filter']
        },
        'file_warning_11_c0': {
            'level': 'WARNING',
            'formatter': 'simple',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': os.path.join(LOG_DIR, '11_warning_c0.log'),
            'when': "d",
            'interval': 1,
            'encoding': 'utf8',
            'backupCount': 30,
            'filters': ['warning_plus_filter']
        },
        'file_info_11_c30': {
            'level': 'INFO',
            'formatter': 'simple',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': os.path.join(LOG_DIR, '11_info_c30.log'),
            'when': "d",
            'interval': 1,
            'encoding': 'utf8',
            'backupCount': 30,
            'filters': ['info_plus_filter']
        },
        'file_warning_11_c30': {
            'level': 'WARNING',
            'formatter': 'simple',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': os.path.join(LOG_DIR, '11_warning_c30.log'),
            'when': "d",
            'interval': 1,
            'encoding': 'utf8',
            'backupCount': 30,
            'filters': ['warning_plus_filter']
        },
        'file_info_12': {
            'level': 'INFO',
            'formatter': 'simple',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': os.path.join(LOG_DIR, '12_info.log'),
            'when': "d",
            'interval': 1,
            'encoding': 'utf8',
            'backupCount': 30,
            'filters': ['info_plus_filter']
        },
        'file_warning_12': {
            'level': 'WARNING',
            'formatter': 'simple',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': os.path.join(LOG_DIR, '12_warning.log'),
            'when': "d",
            'interval': 1,
            'encoding': 'utf8',
            'backupCount': 30,
            'filters': ['warning_plus_filter']
        },
        'file_info_13': {
            'level': 'INFO',
            'formatter': 'simple',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': os.path.join(LOG_DIR, '13_info.log'),
            'when': "d",
            'interval': 1,
            'encoding': 'utf8',
            'backupCount': 30,
            'filters': ['info_plus_filter']
        },
        'file_warning_13': {
            'level': 'WARNING',
            'formatter': 'simple',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': os.path.join(LOG_DIR, '13_warning.log'),
            'when': "d",
            'interval': 1,
            'encoding': 'utf8',
            'backupCount': 30,
            'filters': ['warning_plus_filter']
        },
        'file_info_14': {
            'level': 'INFO',
            'formatter': 'simple',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': os.path.join(LOG_DIR, '14_info.log'),
            'when': "d",
            'interval': 1,
            'encoding': 'utf8',
            'backupCount': 30,
            'filters': ['info_plus_filter']
        },
        'file_warning_14': {
            'level': 'WARNING',
            'formatter': 'simple',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': os.path.join(LOG_DIR, '14_warning.log'),
            'when': "d",
            'interval': 1,
            'encoding': 'utf8',
            'backupCount': 30,
            'filters': ['warning_plus_filter']
        },
        'file_info_15': {
            'level': 'INFO',
            'formatter': 'simple',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': os.path.join(LOG_DIR, '15_info.log'),
            'when': "d",
            'interval': 1,
            'encoding': 'utf8',
            'backupCount': 30,
            'filters': ['info_plus_filter']
        },
        'file_warning_15': {
            'level': 'WARNING',
            'formatter': 'simple',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': os.path.join(LOG_DIR, '15_warning.log'),
            'when': "d",
            'interval': 1,
            'encoding': 'utf8',
            'backupCount': 30,
            'filters': ['warning_plus_filter']
        },
        'file_info_db': {
            'level': 'INFO',
            'formatter': 'simple',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': os.path.join(LOG_DIR, 'db_info.log'),
            'when': "d",
            'interval': 1,
            'encoding': 'utf8',
            'backupCount': 30,
            'filters': ['info_plus_filter']
        },
        'file_warning_db': {
            'level': 'WARNING',
            'formatter': 'simple',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': os.path.join(LOG_DIR, 'db_warning.log'),
            'when': "d",
            'interval': 1,
            'encoding': 'utf8',
            'backupCount': 30,
            'filters': ['warning_plus_filter']
        }
    },
    'loggers': {
        'logger_01': {
            'handlers': ['console_info', 'file_info_01', 'file_warning_01'],
            'level': 'INFO'
        },
        'logger_11': {
            'handlers': ['console_info', 'file_info_11', 'file_warning_11'],
            'level': 'INFO'
        },
        'logger_11_c0': {
            'handlers': ['console_info', 'file_info_11_c0', 'file_warning_11_c0'],
            'level': 'INFO'
        },
        'logger_11_c30': {
            'handlers': ['console_info', 'file_info_11_c30', 'file_warning_11_c30'],
            'level': 'INFO'
        },
        'logger_12': {
            'handlers': ['console_info', 'file_info_12', 'file_warning_12'],
            'level': 'INFO'
        },
        'logger_13': {
            'handlers': ['console_info', 'file_info_13', 'file_warning_13'],
            'level': 'INFO'
        },
        'logger_14': {
            'handlers': ['console_info', 'file_info_14', 'file_warning_14'],
            'level': 'INFO'
        },
        'logger_15': {
            'handlers': ['console_info', 'file_info_15', 'file_warning_15'],
            'level': 'INFO'
        },
        'logger_db': {
            'handlers': ['console_info', 'file_info_db', 'file_warning_db'],
            'level': 'INFO'
        }
    }
}

logging.config.dictConfig(LOG_CONFIG_DICT)

logger_01 = logging.getLogger('logger_01')
logger_11 = logging.getLogger('logger_11')
logger_11_c0 = logging.getLogger('logger_11_c0')
logger_11_c30 = logging.getLogger('logger_11_c30')
logger_12 = logging.getLogger('logger_12')
logger_13 = logging.getLogger('logger_13')
logger_14 = logging.getLogger('logger_14')
logger_15 = logging.getLogger('logger_15')
logger_db = logging.getLogger('logger_db')
