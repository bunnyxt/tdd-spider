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
            # Roll back so a transient failure (e.g. 'too many connections')
            # does not leave the session stuck in an invalid transaction that
            # breaks every following query on it. NOTE: on failure this still
            # returns None, which callers like update_video treat as "not
            # exist" -- an infra error can thus be mislabeled; revisit if that
            # distinction matters.
            try:
                session.rollback()
            except Exception:
                pass
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
    def _tid_filters(cls, is_tid_30):
        # is_tid_30: True -> tid == 30, False -> tid != 30, None -> no tid
        # filter. tid is nullable and in SQL both `== 30` and `!= 30` exclude
        # NULL, so None is a strict superset of the two (kept as a param to
        # document the retired c0/c30 partition; the merged pipeline passes None).
        if is_tid_30 is None:
            return []
        return [TddVideo.tid == 30] if is_tid_30 else [TddVideo.tid != 30]

    @classmethod
    def query_all_update_video_aids(cls, session, is_tid_30=None):
        try:
            result = session.query(TddVideo.aid).filter(
                TddVideo.code == 0, TddVideo.state == 0,
                *cls._tid_filters(is_tid_30)).all()
            return list(r[0] for r in result)
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' %
                            (e, {'is_tid_30': is_tid_30}), exc_info=True)
            return None

    @classmethod
    def query_freq_update_video_aids(cls, freq, session, is_tid_30=None):
        try:
            result = session.query(TddVideo.aid).filter(
                TddVideo.code == 0, TddVideo.state == 0, TddVideo.freq == freq,
                *cls._tid_filters(is_tid_30)).all()
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
