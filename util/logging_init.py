import logging
import os

__all__ = ['logging_init']

DEFAULT_LOG_FORMAT = '[%(asctime)s][%(name)s][%(levelname)s]: %(message)s'
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
LOG_DIR = os.path.join(BASE_DIR, 'log')
if not os.path.exists(LOG_DIR):
    os.mkdir(LOG_DIR)


class UnescapeFormatter(logging.Formatter):
    """
    Unescape escaped character for logging. For example, print '\n' as '\\n'.
    """

    def format(self, record):
        original = logging.Formatter.format(self, record)
        escaped = original.encode('unicode_escape').decode()
        return escaped


def logging_init(format=DEFAULT_LOG_FORMAT,
                 unescape=True,
                 console_handler_enable=True, console_handler_level=logging.INFO,
                 file_prefix='default', file_handler_levels=(logging.INFO, logging.WARNING)):
    handlers = []

    if console_handler_enable:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(console_handler_level)
        handlers.append(console_handler)

    for level in file_handler_levels:
        level_name = logging.getLevelName(level)
        file_handler = logging.FileHandler(filename=os.path.join(LOG_DIR, f'{file_prefix}_{level_name}.log'))
        file_handler.setLevel(level)
        handlers.append(file_handler)

    if unescape:
        for handler in handlers:
            handler.setFormatter(UnescapeFormatter(format))

    logging.basicConfig(
        format=format,
        handlers=handlers,
        level=-1  # must set level = -1, or handlers level will not work
    )
