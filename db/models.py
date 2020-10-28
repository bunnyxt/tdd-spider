from .basic import Base
from sqlalchemy import Column, Integer, String, Boolean, ForeignKey, BigInteger
from sqlalchemy.dialects.mysql import LONGTEXT, TINYINT, DOUBLE

__all__ = ['TddVideo', 'TddMember', 'TddVideoStaff', 'TddVideoRecord', 'TddMemberFollowerRecord', 'TddStatDaily',
           'TddVideoLog', 'TddMemberLog', 'TddMemberTotalStatRecord', 'TddTaskVisitVideoRecord',
           'TddVideoRecordAbnormalChange']


class TddVideo(Base):
    """tdd_video table"""

    __tablename__ = 'tdd_video'

    id = Column(BigInteger, primary_key=True, nullable=False, unique=True, autoincrement=True)
    added = Column(Integer, nullable=False)
    aid = Column(Integer, nullable=False, unique=True)
    bvid = Column(String(10), nullable=False)
    videos = Column(Integer, default=None)
    tid = Column(Integer, default=None)
    tname = Column(String(30), default=None)
    copyright = Column(Integer, default=None)
    pic = Column(String(200), default=None)
    title = Column(String(200), nullable=False)
    pubdate = Column(Integer, default=None)
    desc = Column(LONGTEXT, default=None)
    tags = Column(String(500), default=None)
    mid = Column(Integer, default=None)
    code = Column(Integer, nullable=False, default=0)  # TODO how to enable nullable=False and have default value
    attribute = Column(Integer)
    hasstaff = Column(TINYINT, nullable=False, default=-1)  #
    singer = Column(String(200), nullable=False, default='未定义')  #
    solo = Column(TINYINT, nullable=False, default=-1)  #
    original = Column(TINYINT, nullable=False, default=-1)  #
    employed = Column(Integer, nullable=False, default=-1)  #
    isvc = Column(TINYINT, nullable=False, default=-1)  #
    engine = Column(TINYINT, nullable=False, default=-1)  #
    freq = Column(TINYINT, nullable=False, default=0)  #
    activity = Column(TINYINT, nullable=False, default=0)  #
    recent = Column(TINYINT, nullable=False, default=0)  #
    laststat = Column(BigInteger, ForeignKey('tdd_video_record.id'), default=None)

    def __repr__(self):
        return "<TddVideo(aid=%d,title=%s)>" % (self.aid, self.title)


class TddMember(Base):
    """tdd_member table"""

    __tablename__ = 'tdd_member'

    id = Column(BigInteger, primary_key=True, nullable=False, unique=True, autoincrement=True)
    added = Column(Integer, nullable=False)
    mid = Column(Integer, nullable=False, unique=True)
    sex = Column(String(20), nullable=False)
    name = Column(String(100), nullable=False)
    face = Column(String(200), nullable=False)
    sign = Column(String(500), nullable=False, default='')  #
    video_count = Column(Integer, nullable=False, default=0)
    last_video = Column(BigInteger)  # fk
    last_total_stat = Column(BigInteger)  # fk
    last_follower = Column(BigInteger)  # fk

    def __repr__(self):
        return "<TddMember(mid=%d,name=%s)>" % (self.mid, self.name)


class TddVideoStaff(Base):
    """tdd_video_staff table"""

    __tablename__ = 'tdd_video_staff'

    id = Column(BigInteger, primary_key=True, nullable=False, unique=True, autoincrement=True)
    added = Column(Integer, nullable=False)
    aid = Column(Integer, nullable=False)
    bvid = Column(String(10), nullable=False)
    mid = Column(Integer, nullable=False)
    title = Column(String(30), nullable=False)

    # UNIQUE KEY `aid_mid_UNIQUE` (`aid`,`mid`)

    def __repr__(self):
        return "<TddVideoStaff(aid=%d,mid=%d)>" % (self.aid, self.mid)


class TddVideoRecord(Base):
    """tdd_video_record table"""

    __tablename__ = 'tdd_video_record'

    id = Column(BigInteger, primary_key=True, nullable=False, unique=True, autoincrement=True)
    added = Column(Integer, nullable=False)
    aid = Column(Integer, nullable=False)
    view = Column(Integer, nullable=False)  # maybe '--' from api, set -1 instead
    danmaku = Column(Integer, nullable=False)
    reply = Column(Integer, nullable=False)
    favorite = Column(Integer, nullable=False)
    coin = Column(Integer, nullable=False)
    share = Column(Integer, nullable=False)
    like = Column(Integer, nullable=False)

    # UNIQUE KEY `added_aid_UNIQUE` (`added`,`aid`)

    # Foreign key here?

    def __repr__(self):
        return "<TddVideoRecord(aid=%d,view=%d)>" % (self.aid, self.view)


class TddMemberFollowerRecord(Base):
    """tdd_member_follower_record table"""

    __tablename__ = 'tdd_member_follower_record'

    id = Column(BigInteger, primary_key=True, nullable=False, unique=True, autoincrement=True)
    added = Column(Integer, nullable=False)
    mid = Column(Integer, nullable=False)
    follower = Column(Integer, nullable=False)

    # UNIQUE KEY `added_mid_UNIQUE` (`added`,`mid`)

    def __repr__(self):
        return '<TddMemberFollowerRecord(mid=%d,follower=%d)>' % (self.mid, self.follower)


class TddStatDaily(Base):
    """tdd_stat_daily table"""

    __tablename__ = 'tdd_stat_daily'

    id = Column(BigInteger, primary_key=True, nullable=False, unique=True, autoincrement=True)
    added = Column(Integer, nullable=False)
    video_count = Column(BigInteger, nullable=False)
    member_count = Column(BigInteger, nullable=False)
    video_record_count = Column(BigInteger, nullable=False)

    def __repr__(self):
        return '<TddStatDaily(added=%d,video_count=%d,member_count=%d,video_record_count=%d)>' \
               % (self.added, self.video_count, self.member_count, self.video_record_count)


class TddVideoLog(Base):
    """tdd_video_log_table"""

    __tablename__ = 'tdd_video_log'

    id = Column(BigInteger, primary_key=True, nullable=False, unique=True, autoincrement=True)
    added = Column(Integer, nullable=False)
    aid = Column(Integer, nullable=False)
    bvid = Column(String(10), nullable=False)
    attr = Column(String(30), nullable=False)
    oldval = Column(LONGTEXT)
    newval = Column(LONGTEXT)

    def __init__(self, added, aid, bvid, attr, oldval, newval):
        self.added = added
        self.aid = aid
        self.bvid = bvid
        self.attr = attr
        self.oldval = oldval
        self.newval = newval

    def __repr__(self):
        return '<TddVideoLog(aid=%d,attr=%s)>' % (self.aid, self.attr)


class TddMemberLog(Base):
    """tdd_member_log_table"""

    __tablename__ = 'tdd_member_log'

    id = Column(BigInteger, primary_key=True, nullable=False, unique=True, autoincrement=True)
    added = Column(Integer, nullable=False)
    mid = Column(Integer, nullable=False)
    attr = Column(String(30), nullable=False)
    oldval = Column(LONGTEXT)
    newval = Column(LONGTEXT)

    def __init__(self, added, mid, attr, oldval, newval):
        self.added = added
        self.mid = mid
        self.attr = attr
        self.oldval = oldval
        self.newval = newval

    def __repr__(self):
        return '<TddMemberLog(mid=%d,attr=%s)>' % (self.mid, self.attr)


class TddMemberTotalStatRecord(Base):
    """tdd_member_total_stat_record"""

    __tablename__ = 'tdd_member_total_stat_record'

    id = Column(BigInteger, primary_key=True, nullable=False, unique=True, autoincrement=True)
    added = Column(Integer, nullable=False)
    mid = Column(Integer, nullable=False)
    video_count = Column(Integer, nullable=False)
    view = Column(Integer, nullable=False)  # maybe '--' from api, set -1 instead
    danmaku = Column(Integer, nullable=False)
    reply = Column(Integer, nullable=False)
    favorite = Column(Integer, nullable=False)
    coin = Column(Integer, nullable=False)
    share = Column(Integer, nullable=False)
    like = Column(Integer, nullable=False)

    def __init__(self, added, mid, video_count=0, view=0, danmaku=0, reply=0, favorite=0, coin=0, share=0, like=0):
        self.added = added
        self.mid = mid
        self.video_count = video_count
        self.view = view
        self.danmaku = danmaku
        self.reply = reply
        self.favorite = favorite
        self.coin = coin
        self.share = share
        self.like = like

    def __repr__(self):
        return '<TddMemberTotalStatRecord(mid=%d,video_count=%d)>' % (self.mid, self.video_count)


class TddTaskVisitVideoRecord(Base):
    """tdd_task_visit_video_record"""

    __tablename__ = 'tdd_task_visit_video_record'

    id = Column(BigInteger, primary_key=True, nullable=False, unique=True, autoincrement=True)
    added = Column(Integer, nullable=False)
    aid = Column(Integer, nullable=False)
    userid = Column(BigInteger, nullable=False)
    status = Column(TINYINT, nullable=False)

    def __repr__(self):
        return '<TddTaskVisitVideoRecord(added=%d,aid=%d)>' % (self.added, self.aid)


class TddVideoRecordAbnormalChange(Base):
    """tdd_video_record_abnormal_change"""

    __tablename__ = 'tdd_video_record_abnormal_change'

    id = Column(BigInteger, primary_key=True, nullable=False, unique=True, autoincrement=True)
    added = Column(Integer, nullable=False)
    aid = Column(Integer, nullable=False)
    attr = Column(String, nullable=False)
    speed_now = Column(DOUBLE, nullable=False)
    speed_last = Column(DOUBLE, nullable=False)
    speed_now_incr_rate = Column(DOUBLE, nullable=False)
    period_range = Column(Integer, nullable=False)
    speed_period = Column(DOUBLE, nullable=False)
    speed_overall = Column(DOUBLE, nullable=False)
    this_record_id = Column(BigInteger, nullable=False)
    this_added = Column(Integer, nullable=True)
    this_view = Column(Integer, nullable=True)
    this_danmaku = Column(Integer, nullable=True)
    this_reply = Column(Integer, nullable=True)
    this_favorite = Column(Integer, nullable=True)
    this_coin = Column(Integer, nullable=True)
    this_share = Column(Integer, nullable=True)
    this_like = Column(Integer, nullable=True)
    last_record_id = Column(BigInteger, nullable=False)
    last_added = Column(Integer, nullable=True)
    last_view = Column(Integer, nullable=True)
    last_danmaku = Column(Integer, nullable=True)
    last_reply = Column(Integer, nullable=True)
    last_favorite = Column(Integer, nullable=True)
    last_coin = Column(Integer, nullable=True)
    last_share = Column(Integer, nullable=True)
    last_like = Column(Integer, nullable=True)
    description = Column(String, default='')
    comment = Column(String, default='')

    def __repr__(self):
        return '<TddVideoRecordAbnormalChange(aid=%d,attr=%s)>' % (self.aid, self.attr)
