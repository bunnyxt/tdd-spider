from logger import logger_db
from .models import TddVideo, TddMember
from sqlalchemy import text

__all__ = ['DBOperation']


class DBOperation:

    @classmethod
    def add(cls, obj, session):
        try:
            session.add(obj)
            session.commit()
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' % (e, {'obj': obj}), exc_info=True)
            session.rollback()

    @classmethod
    def update_video_code(cls, aid, code, session):
        try:
            video = session.query(TddVideo).filter(TddVideo.aid == aid).first()
            video.code = code
            session.commit()
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' % (e, {'aid': aid, 'code': code}), exc_info=True)
            return None

    @classmethod
    def update_video_tid(cls, aid, tid, session):
        try:
            video = session.query(TddVideo).filter(TddVideo.aid == aid).first()
            video.tid = tid
            session.commit()
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' % (e, {'aid': aid, 'tid': tid}), exc_info=True)
            return None

    @classmethod
    def update_video_isvc(cls, aid, isvc, session):
        try:
            video = session.query(TddVideo).filter(TddVideo.aid == aid).first()
            video.isvc = isvc
            session.commit()
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' % (e, {'aid': aid, 'isvc': isvc}), exc_info=True)
            return None

    @classmethod
    def query(cls, table, session):
        try:
            result = session.query(table).all()
            return result
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' % (e, {'table': table}), exc_info=True)
            return None

    @classmethod
    def query_video_via_aid(cls, aid, session):
        try:
            result = session.query(TddVideo).filter(TddVideo.aid == aid).first()
            return result
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' % (e, {'aid': aid}), exc_info=True)
            return None

    @classmethod
    def query_update_c30_aids(cls, freq, session):
        try:
            result = session.query(TddVideo.aid).filter(TddVideo.tid == 30, TddVideo.code == 0,
                                                        TddVideo.freq == freq).all()
            return list(r[0] for r in result)
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' % (e, {'freq': freq}), exc_info=True)
            return None

    @classmethod
    def query_update_c0_aids(cls, freq, session):
        try:
            result = session.query(TddVideo.aid).filter(TddVideo.tid != 30, TddVideo.code == 0,
                                                        TddVideo.freq == freq).all()
            return list(r[0] for r in result)
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' % (e, {'freq': freq}), exc_info=True)
            return None

    @classmethod
    def query_last_x_video(cls, x, session):
        try:
            result = session.query(TddVideo).filter(TddVideo.tid == 30).order_by(TddVideo.pubdate.desc()).limit(x).all()
            return result
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' % (e, {'x': x}), exc_info=True)
            return None

    @classmethod
    def query_member_via_mid(cls, mid, session):
        try:
            result = session.query(TddMember).filter(TddMember.mid == mid).first()
            return result
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' % (e, {'mid': mid}), exc_info=True)
            return None

    @classmethod
    def query_not_added_video_member_mids(cls, session):
        try:
            result = session.execute(
                'select distinct(mid) from tdd_video where mid not in (select mid from tdd_member);')
            return list(r[0] for r in result)
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' % (e, {}), exc_info=True)
            return None

    @classmethod
    def query_not_added_video_staff_mids(cls, session):
        try:
            result = session.execute(
                'select distinct(mid) from tdd_video_staff where mid not in (select mid from tdd_member);')
            return list(r[0] for r in result)
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' % (e, {}), exc_info=True)
            return None
