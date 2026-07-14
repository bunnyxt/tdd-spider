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
    Collapse a log record onto a single line by escaping line-breaking control
    characters (newline, carriage return, tab) -- video descriptions contain
    newlines, and an un-collapsed record would break across many lines and
    wreck line-based grep.

    Only those control chars are escaped. The previous implementation used
    str.encode('unicode_escape'), which ALSO escaped every non-ASCII character
    to \\uXXXX -- turning CJK text (titles, descriptions) into unreadable escape
    sequences in the log. Printable Unicode is now left intact (the file
    handlers are opened as utf-8).
    """

    def format(self, record):
        original = logging.Formatter.format(self, record)
        return (original
                .replace('\\', '\\\\')
                .replace('\r', '\\r')
                .replace('\n', '\\n')
                .replace('\t', '\\t'))


def logging_init(format=DEFAULT_LOG_FORMAT,
                 unescape=True,
                 console_handler_enable=True, console_handler_level=logging.INFO,
                 file_prefix='default', file_handler_levels=(logging.INFO, logging.WARNING),
                 debug=False):
    # debug=True adds a {file_prefix}_DEBUG.log file handler capturing ALL log
    # records (DEBUG and above, every logger). Opt-in only: the file is large
    # (per-aid TIMING lines, per-request Service lines, SYSSTAT samples), meant
    # for offline analysis of a specific run.
    if debug:
        file_handler_levels = (logging.DEBUG,) + tuple(file_handler_levels)

    handlers = []

    if console_handler_enable:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(console_handler_level)
        handlers.append(console_handler)

    for level in file_handler_levels:
        level_name = logging.getLevelName(level)
        file_handler = logging.FileHandler(
            filename=os.path.join(LOG_DIR, f'{file_prefix}_{level_name}.log'),
            encoding='utf-8')  # CJK titles/descs are no longer escaped, write them as utf-8
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
