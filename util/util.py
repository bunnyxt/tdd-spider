import datetime

__all__ = ['get_ts_s']


def get_ts_s():
    return int(round(datetime.datetime.now().timestamp()))
