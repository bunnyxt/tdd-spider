import logging
import os

__all__ = ['DEFAULT_LOG_FORMAT', 'BASE_DIR', 'LOG_DIR', 'logging_init']

DEFAULT_LOG_FORMAT = '[%(asctime)s][%(name)s][%(levelname)s]: %(message)s'
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
LOG_DIR = os.path.join(BASE_DIR, 'log')
if not os.path.exists(LOG_DIR):
    os.mkdir(LOG_DIR)


def logging_init(format=DEFAULT_LOG_FORMAT,
                 console_handler_enable=True, console_handler_level=logging.INFO,
                 file_prefix='default', file_handler_levels=(logging.INFO, logging.WARNING)):
    handlers = []

    if console_handler_enable:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(console_handler_level)
        handlers.append(console_handler)

    for level in file_handler_levels:
        level_name = logging.getLevelName(level)
        file_handler = logging.FileHandler(filename=os.path.join(LOG_DIR, '%s_%s.log' % (file_prefix, level_name)))
        file_handler.setLevel(level)
        handlers.append(file_handler)

    logging.basicConfig(
        format=format,
        handlers=handlers,
        level=-1  # must set level = -1, or handlers level will not work
    )
