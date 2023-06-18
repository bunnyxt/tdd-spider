import math
import re
import logging
from typing import Optional

logger = logging.getLogger('util')

__all__ = ['parse_pic_url', 'same_pic_url',
           'is_all_zero_record', 'print_obj', 'null_or_str',
           'zk_calc']


def parse_pic_url(url: str) -> Optional[dict]:
    pattern = r"(?P<protocol>https?)://(?P<subdomain>[^.]+)\.(?P<domain>[^.]+\.[^.]+)/(?P<fsname>[^/]+)/(?P<dirname>[^/]+)/(?P<filename>[^.]+)\.(?P<extension>[^.]+)"
    match = re.search(pattern, url)
    if match is None:
        return None
    return match.groupdict()


def same_pic_url(url1: str, url2: str) -> bool:
    group1 = parse_pic_url(url1)
    group2 = parse_pic_url(url2)

    if group1 is None or group2 is None:
        return False

    for key in ['domain', 'fsname', 'filename', 'extension']:
        if group1[key] != group2[key]:
            return False

    if group1['dirname'] != group2['dirname']:
        logger.warning(f'pic url dirname not equal: {url1} vs {url2}')

    return True


def is_all_zero_record(record) -> bool:
    # TODO: support dislike, vt, vv
    attributes = ['view', 'danmaku', 'reply', 'favorite', 'coin', 'share', 'like']
    for idx, attribute in enumerate(attributes, 3):
        if record[idx] > 0:
            return False
    return True


def print_obj(obj):
    for key in dir(obj):
        print(key, obj.__getattribute__(key))


def null_or_str(value: any) -> str:
    """
    convert value to string via `str(value)`, if value is None, return 'null'
    """
    return 'null' if value is None else str(value)


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
