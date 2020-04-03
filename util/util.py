import datetime
import time

__all__ = ['get_ts_s', 'ts_s_to_str', 'b2a', 'a2b']


def get_ts_s():
    return int(round(datetime.datetime.now().timestamp()))


def ts_s_to_str(ts):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))


def print_obj(obj):
    for key in dir(obj):
        print(key, obj.__getattribute__(key))


# ref: https://www.zhihu.com/question/381784377/answer/1099438784
table = 'fZodR9XQDSUm21yCkr6zBqiveYah8bt4xsWpHnJE7jL5VG3guMTKNPAwcF'
tr = {}
for i in range(58):
    tr[table[i]] = i
s = [11, 10, 3, 8, 4, 6]
xor = 177451812
add = 8728348608


def b2a(x):
    r = 0
    for i in range(6):
        r += tr[x[s[i] - 2]] * 58 ** i  # no BV prefix provided
    return (r - add) ^ xor


def a2b(x):
    x = (x ^ xor) + add
    r = list('BV1  4 1 7  ')
    for i in range(6):
        r[s[i]] = table[x // 58 ** i % 58]
    return ''.join(r)[2:]  # remove BV prefix
