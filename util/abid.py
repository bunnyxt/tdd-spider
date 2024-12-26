__all__ = ['b2a', 'a2b']

# ref: https://github.com/SocialSisterYi/bilibili-API-collect/blob/master/docs/misc/bvid_desc.md#python

XOR_CODE = 23442827791579
MASK_CODE = 2251799813685247
MAX_AID = 1 << 51
ALPHABET = "FcwAPNKTMug3GV5Lj7EJnHpWsx4tb8haYeviqBz6rkCy12mUSDQX9RdoZf"
ENCODE_MAP = 8, 7, 0, 5, 1, 3, 2, 4, 6
DECODE_MAP = tuple(reversed(ENCODE_MAP))

BASE = len(ALPHABET)
PREFIX = "BV1"
PREFIX_LEN = len(PREFIX)
CODE_LEN = len(ENCODE_MAP)


def b2a(bvid: str) -> int:
    """
    Convert bvid to aid.
    :param bvid: bvid string without BV prefix
    :return: aid
    """
    bvid = bvid[PREFIX_LEN-2:]
    tmp = 0
    for i in range(CODE_LEN):
        idx = ALPHABET.index(bvid[DECODE_MAP[i]])
        tmp = tmp * BASE + idx
    return (tmp & MASK_CODE) ^ XOR_CODE


def a2b(aid: int) -> str:
    """
    Convert aid to bvid.
    :param aid: aid
    :return: bvid string without BV prefix
    """
    bvid = [""] * 9
    tmp = (MAX_AID | aid) ^ XOR_CODE
    for i in range(CODE_LEN):
        bvid[ENCODE_MAP[i]] = ALPHABET[tmp % BASE]
        tmp //= BASE
    return (PREFIX + "".join(bvid))[2:]  # remove BV prefix


# int aid
assert a2b(456930) == '19x411F7kL'
assert b2a('19x411F7kL') == 456930

# bigint aid
assert a2b(113672802272752) == '1HWkwYWETx'
assert b2a('1HWkwYWETx') == 113672802272752
