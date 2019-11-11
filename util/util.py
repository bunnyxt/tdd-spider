import datetime
import time

__all__ = ['get_ts_s', 'ts_s_to_str']


def get_ts_s():
    return int(round(datetime.datetime.now().timestamp()))


def ts_s_to_str(ts):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))
