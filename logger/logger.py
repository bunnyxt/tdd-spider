import logging
import logging.config
import os

__all__ = ['BASE_DIR', 'LOG_DIR',
           'logger_71', 'logger_72']

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
        'file_info_71': {
            'level': 'INFO',
            'formatter': 'simple',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': os.path.join(LOG_DIR, '71_info.log'),
            'when': "d",
            'interval': 1,
            'encoding': 'utf8',
            'backupCount': 30,
            'filters': ['info_plus_filter']
        },
        'file_info_72': {
            'level': 'INFO',
            'formatter': 'simple',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': os.path.join(LOG_DIR, '72_info.log'),
            'when': "d",
            'interval': 1,
            'encoding': 'utf8',
            'backupCount': 30,
            'filters': ['info_plus_filter']
        },
    },
    'loggers': {
        'logger_71': {
            'handlers': ['console_info', 'file_info_71'],
            'level': 'INFO'
        },
        'logger_72': {
            'handlers': ['console_info', 'file_info_72'],
            'level': 'INFO'
        },
    }
}

logging.config.dictConfig(LOG_CONFIG_DICT)

logger_71 = logging.getLogger('logger_71')
logger_72 = logging.getLogger('logger_72')
