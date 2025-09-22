from .models import TddVideo, TddMember, TddVideoStaff
import logging
logger_db = logging.getLogger('db')

__all__ = ['DBOperation']


class DBOperation:

    @classmethod
    def add(cls, obj, session):
        try:
            session.add(obj)
            session.commit()
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' %
                            (e, {'obj': obj}), exc_info=True)
            session.rollback()

    @classmethod
    def query_video_via_aid(cls, aid, session):
        try:
            result = session.query(TddVideo).filter(
                TddVideo.aid == aid).first()
            return result
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' %
                            (e, {'aid': aid}), exc_info=True)
            return None

    @classmethod
    def query_video_staff_via_aid_mid(cls, aid, mid, session):
        try:
            result = session.query(TddVideoStaff).filter(
                TddVideoStaff.aid == aid, TddVideoStaff.mid == mid).first()
            return result
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' %
                            (e, {'aid': aid, 'mid': mid}), exc_info=True)
            return None

    @classmethod
    def delete_video_staff_via_aid_mid(cls, aid, mid, session):
        try:
            session.query(TddVideoStaff).filter(
                TddVideoStaff.aid == aid, TddVideoStaff.mid == mid).delete()
            session.commit()
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' %
                            (e, {'aid': aid, 'mid': mid}), exc_info=True)
            return None

    @classmethod
    def query_video_staff_via_aid(cls, aid, session):
        try:
            result = session.query(TddVideoStaff).filter(
                TddVideoStaff.aid == aid).all()
            return result
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' %
                            (e, {'aid': aid}), exc_info=True)
            return None

    @classmethod
    def delete_video_staff_via_id(cls, id, session):
        try:
            session.query(TddVideoStaff).filter(
                TddVideoStaff.id == id).delete()
            session.commit()
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' %
                            (e, {'id': id}), exc_info=True)
            return None

    @classmethod
    def query_member_via_mid(cls, mid, session):
        try:
            result = session.query(TddMember).filter(
                TddMember.mid == mid).first()
            return result
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' %
                            (e, {'mid': mid}), exc_info=True)
            return None

    @classmethod
    def count_table_until_ts(cls, table_name, ts, session):
        try:
            result = session.execute(
                'select count(1) from %s where added <= %d;' % (table_name, ts))
            return list(r[0] for r in result)
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' %
                            (e, {}), exc_info=True)
            return None

    @classmethod
    def query_all_video_bvids(cls, session):
        try:
            result = session.execute('select bvid from tdd_video')
            return list(r[0] for r in result)
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' %
                            (e, {}), exc_info=True)
            return None

    @classmethod
    def query_member_mids(cls, offset, size, session):
        try:
            result = session.execute(
                'select mid from tdd_member limit %d, %d' % (offset, size))
            return list(r[0] for r in result)
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' % (
                e, {'offset': offset, 'size': size}), exc_info=True)
            return None

    @classmethod
    def query_all_member_mids(cls, session):
        try:
            result = session.execute('select mid from tdd_member')
            return list(r[0] for r in result)
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' %
                            (e, {}), exc_info=True)
            return None

    @classmethod
    def query_all_update_video_aids(cls, is_tid_30, session):
        try:
            if is_tid_30:
                result = session.query(TddVideo.aid).filter(
                    TddVideo.tid == 30, TddVideo.code == 0, TddVideo.state == 0).all()
            else:
                result = session.query(TddVideo.aid).filter(
                    TddVideo.tid != 30, TddVideo.code == 0, TddVideo.state == 0).all()
            return list(r[0] for r in result)
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' %
                            (e, {'is_tid_30': is_tid_30}), exc_info=True)
            return None

    @classmethod
    def query_freq_update_video_aids(cls, freq, is_tid_30, session):
        try:
            if is_tid_30:
                result = session.query(TddVideo.aid).filter(
                    TddVideo.tid == 30, TddVideo.code == 0, TddVideo.state == 0, TddVideo.freq == freq).all()
            else:
                result = session.query(TddVideo.aid).filter(
                    TddVideo.tid != 30, TddVideo.code == 0, TddVideo.state == 0, TddVideo.freq == freq).all()
            return list(r[0] for r in result)
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' % (
                e, {'freq': freq, 'is_tid_30': is_tid_30}), exc_info=True)
            return None

    @classmethod
    def query_403_video_aids(cls, session):
        try:
            result = session.query(TddVideo.aid).filter(
                TddVideo.code == -403).all()
            return list(r[0] for r in result)
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' %
                            (e, {}), exc_info=True)
            return None

    @classmethod
    def query_video_pubdate_all(cls, session):
        try:
            result = session.query(TddVideo.aid, TddVideo.pubdate).all()
            return list((r[0], r[1]) for r in result)
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' %
                            (e, {}), exc_info=True)
            return None

    @classmethod
    def query_video_records_of_given_aid_added_before_given_ts(cls, aid, ts, session):
        try:
            result = session.query(TddVideo.aid).filter(
                TddVideo.aid == aid, TddVideo.added < ts).all()
            return result
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' %
                            (e, {'aid': aid, 'ts': ts}), exc_info=True)
            return None
