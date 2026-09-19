import datetime
import time

__all__ = ['get_ts_s', 'ts_s_to_str', 'get_ts_s_str', 'str_to_ts_s', 'format_ts_s',
           'get_ts_ms', 'ts_ms_to_str', 'format_ts_ms', 'format_duration_summary',
           'get_week_day']


def get_ts_s() -> int:
    """
    Get current timestamp in seconds.
    :return: timestamp (seconds)
    """
    return int(round(datetime.datetime.now().timestamp()))


def ts_s_to_str(ts: int) -> str:
    """
    Convert timestamp (seconds) to string.
    :param ts: timestamp (seconds)
    :return: time string, format: "%Y-%m-%d %H:%M:%S"
    """
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))


def get_ts_s_str() -> str:
    """
    Get current timestamp in seconds, and convert it to string.
    :return: time string, format: "%Y-%m-%d %H:%M:%S"
    """
    return ts_s_to_str(get_ts_s())


def str_to_ts_s(s: str, mask='%Y-%m-%d %H:%M:%S') -> int:
    """
    Convert time string to timestamp (seconds).
    :param s: time string
    :param mask: time string mask, default: "%Y-%m-%d %H:%M:%S"
    :return: timestamp (seconds)
    """
    return int(time.mktime(time.strptime(s, mask)))


def format_ts_s(ts_s: int) -> str:
    """
    Format timestamp (seconds) to timespan string.
    :param ts_s: timestamp (seconds)
    :return: timespan string, format: "Xh Xm Xs"
    """
    hours = ts_s // 3600
    minutes = (ts_s % 3600) // 60
    remaining_seconds = ts_s % 60

    formatted_ts_s = ''
    if hours > 0:
        formatted_ts_s += f'{hours}h '
    if hours > 0 or minutes > 0:
        formatted_ts_s += f'{minutes}m '
    formatted_ts_s += f'{remaining_seconds}s'

    return formatted_ts_s


def get_ts_ms() -> int:
    """
    Get current timestamp in milliseconds.
    :return: timestamp (milliseconds)
    """
    return int(round(datetime.datetime.now().timestamp() * 1000))


def ts_ms_to_str(ts_ms: int) -> str:
    """
    Convert timestamp (milliseconds) to string.
    :param ts_ms: timestamp (milliseconds)
    :return: time string, format: "%Y-%m-%d %H:%M:%S" + "," + "%ms"
    """
    return f'{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts_ms // 1000))},{str(ts_ms % 1000).zfill(3)}'


def format_ts_ms(ts_ms: int) -> str:
    """
    Format timestamp (milliseconds) to timespan string.
    :param ts_ms: timestamp (milliseconds)
    :return: timespan string, format: "Xh Xm Xs Xms"
    """
    ts_s = ts_ms // 1000
    remaining_ms = ts_ms % 1000

    formatted_ts_ms = ''
    if ts_s > 0:
        formatted_ts_ms = f'{format_ts_s(ts_s)} '
    formatted_ts_ms += f'{remaining_ms}ms'

    return formatted_ts_ms


def format_duration_summary(start_ts_ms: int, end_ts_ms: int) -> str:
    """
    Format a wall-clock span as one human-readable summary line.
    :param start_ts_ms: span start timestamp (milliseconds)
    :param end_ts_ms: span end timestamp (milliseconds)
    :return: summary string, format: "start: ..., end: ..., duration: ..."
    """
    return f'start: {ts_ms_to_str(start_ts_ms)}, ' \
           f'end: {ts_ms_to_str(end_ts_ms)}, ' \
           f'duration: {format_ts_ms(end_ts_ms - start_ts_ms)}'


def get_week_day() -> int:
    """
    Get index of current day in a week.
    e.g. Mon -> 0, Tue -> 1, ..., Sun -> 6
    :return: index of current day in a week
    """
    return datetime.datetime.now().weekday()
