from .basic import Base
from sqlalchemy import Column, Integer, String, Boolean, ForeignKey, BigInteger
from sqlalchemy.dialects.mysql import LONGTEXT, TINYINT

__all__ = ['TddVideo', 'TddMember', 'TddVideoStaff', 'TddVideoRecord', 'TddMemberFollowerRecord', 'TddStatDaily']


class TddVideo(Base):
    """tdd_video table"""

    __tablename__ = 'tdd_video'

    id = Column(BigInteger, primary_key=True, nullable=False, unique=True, autoincrement=True)
    added = Column(Integer, nullable=False)
    aid = Column(Integer, nullable=False, unique=True)
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

    def __repr__(self):
        return "<TddMember(mid=%d,name=%s)>" % (self.mid, self.name)


class TddVideoStaff(Base):
    """tdd_video_staff table"""

    __tablename__ = 'tdd_video_staff'

    id = Column(BigInteger, primary_key=True, nullable=False, unique=True, autoincrement=True)
    added = Column(Integer, nullable=False)
    aid = Column(Integer, nullable=False)
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
