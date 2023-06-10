import datetime
import time
import math

__all__ = ['get_ts_s', 'ts_s_to_str', 'get_ts_s_str', 'str_to_ts_s', 'format_ts_s',
           'get_ts_ms', 'format_ts_ms',
           'is_all_zero_record', 'print_obj', 'null_or_str',
           'b2a', 'a2b', 'get_week_day', 'zk_calc']


def get_ts_s() -> int:
    return int(round(datetime.datetime.now().timestamp()))


def ts_s_to_str(ts):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))


def get_ts_s_str():
    return ts_s_to_str(get_ts_s())


def str_to_ts_s(s, mask='%Y-%m-%d %H:%M:%S'):
    return int(time.mktime(time.strptime(s, mask)))


def format_ts_s(ts_s: int) -> str:
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
    return int(round(datetime.datetime.now().timestamp() * 1000))


def format_ts_ms(ts_ms: int) -> str:
    ts_s = ts_ms // 1000
    remaining_ms = ts_ms % 1000

    formatted_ts_ms = ''
    if ts_s > 0:
        formatted_ts_ms = f'{format_ts_s(ts_s)} '
    formatted_ts_ms += f'{remaining_ms}ms'

    return formatted_ts_ms


def is_all_zero_record(record):
    # TODO: support dislike, vt, vv
    attributes = ['view', 'danmaku', 'reply', 'favorite', 'coin', 'share', 'like']
    for idx, attribute in enumerate(attributes, 3):
        if record[idx] > 0:
            return False
    return True


def print_obj(obj):
    for key in dir(obj):
        print(key, obj.__getattribute__(key))


def null_or_str(value):
    """
    convert value to string via `str(value)`, if value is None, return 'null'
    """
    return 'null' if value is None else str(value)


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


def get_week_day():
    # Mon -> 0, Tue -> 1, ..., Sun -> 6
    return datetime.datetime.now().weekday()


def zk_calc(view, danmaku, reply, favorite, page=1):
    jichu = view / page
    if jichu > 10000:
        bofang = jichu * 0.5 + 5000
    else:
        bofang = jichu

    if view == 0:
        xiub = 50
    else:
        xiub = round(favorite / view * 250, 2)
        if favorite < 0:  # 负收藏以绝对值计算修正B，不受50上限和10反馈影响
            xiub = abs(xiub)
        elif xiub > 50 or xiub < 0:
            xiub = 50

    bofang_ori = bofang
    if xiub < 10:
        bofang = bofang * xiub * 0.1

    if danmaku < 0 or reply < 0:  # 负弹幕/评论按照0计算修正A
        xiua = 0
    elif bofang_ori + favorite + danmaku * 10 + reply * 20 == 0:
        xiua = 1
    else:
        xiua = round(
            (bofang_ori + favorite) /
            (bofang_ori + favorite + danmaku * 10 + reply * 20)
            , 2)

    point = math.floor(bofang + (reply * 25 + danmaku) * xiua + favorite * xiub)

    return point, xiua, xiub
