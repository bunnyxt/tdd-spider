from logger import logger_db
from .models import TddVideo

__all__ = ['DBOperation']


class DBOperation:

    @classmethod
    def query(cls, table, session):
        try:
            result = session.query(table).all()
            return result
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' % (e, {'table': table}), exc_info=True)
            return None

    @classmethod
    def query_tdd_video_via_aid(cls, aid, session):
        try:
            result = session.query(TddVideo).filter(TddVideo.aid == aid).first()
            return result
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' % (e, {'aid': aid}), exc_info=True)
            return None

    @classmethod
    def add(cls, obj, session):
        try:
            session.add(obj)
            session.commit()
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' % (e, {'obj': obj}), exc_info=True)
            session.rollback()
