from logger import logger_db
from .models import TddVideo, TddMember, TddVideoStaff, TddTaskVisitVideoRecord
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
    def query_video_via_bvid(cls, bvid, session):
        try:
            result = session.query(TddVideo).filter(TddVideo.bvid == bvid).first()
            return result
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' % (e, {'bvid': bvid}), exc_info=True)
            return None

    @classmethod
    def query_video_staff_via_aid_mid(cls, aid, mid, session):
        try:
            result = session.query(TddVideoStaff).filter(TddVideoStaff.aid == aid, TddVideoStaff.mid == mid).first()
            return result
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' % (e, {'aid': aid, 'mid': mid}), exc_info=True)
            return None

    @classmethod
    def query_video_staff_via_bvid_mid(cls, bvid, mid, session):
        try:
            result = session.query(TddVideoStaff).filter(TddVideoStaff.bvid == bvid, TddVideoStaff.mid == mid).first()
            return result
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' % (e, {'bvid': bvid, 'mid': mid}), exc_info=True)
            return None

    @classmethod
    def query_video_staff_via_aid(cls, aid, session):
        try:
            result = session.query(TddVideoStaff).filter(TddVideoStaff.aid == aid).all()
            return result
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' % (e, {'aid': aid}), exc_info=True)
            return None

    @classmethod
    def delete_video_staff_via_id(cls, id, session):
        try:
            session.query(TddVideoStaff).filter(TddVideoStaff.id == id).delete()
            session.commit()
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' % (e, {'id': id}), exc_info=True)
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
    def query_update_c30_aids_all(cls, session):
        try:
            result = session.query(TddVideo.aid).filter(TddVideo.tid == 30, TddVideo.code == 0,
                                                        TddVideo.freq >= 0).all()
            return list(r[0] for r in result)
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' % (e, {}), exc_info=True)
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
    def query_update_c0_aids_all(cls, session):
        try:
            result = session.query(TddVideo.aid).filter(TddVideo.tid != 30, TddVideo.code == 0,
                                                        TddVideo.freq >= 0).all()
            return list(r[0] for r in result)
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' % (e, {}), exc_info=True)
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

    @classmethod
    def count_table_until_ts(cls, table_name, ts, session):
        try:
            result = session.execute(
                'select count(1) from %s where added <= %d;' % (table_name, ts))
            return list(r[0] for r in result)
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' % (e, {}), exc_info=True)
            return None

    @classmethod
    def query_active_video_aids(cls, is_tid_30, session):
        try:
            if is_tid_30:
                result = session.query(TddVideo.aid).filter(TddVideo.tid == 30, TddVideo.code == 0,
                                                            TddVideo.activity == 1).all()
            else:
                result = session.query(TddVideo.aid).filter(TddVideo.tid != 30, TddVideo.code == 0,
                                                            TddVideo.activity == 1).all()
            return list(r[0] for r in result)
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' % (e, {'is_tid_30': is_tid_30}), exc_info=True)
            return None

    @classmethod
    def query_hot_video_aids(cls, is_tid_30, session):
        try:
            if is_tid_30:
                result = session.query(TddVideo.aid).filter(TddVideo.tid == 30, TddVideo.code == 0,
                                                            TddVideo.activity == 2).all()
            else:
                result = session.query(TddVideo.aid).filter(TddVideo.tid != 30, TddVideo.code == 0,
                                                            TddVideo.activity == 2).all()
            return list(r[0] for r in result)
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' % (e, {'is_tid_30': is_tid_30}), exc_info=True)
            return None

    @classmethod
    def query_weekly_new_video_aids(cls, is_tid_30, session):
        try:
            if is_tid_30:
                result = session.query(TddVideo.aid).filter(TddVideo.tid == 30, TddVideo.code == 0,
                                                            TddVideo.recent == 1).all()
            else:
                result = session.query(TddVideo.aid).filter(TddVideo.tid != 30, TddVideo.code == 0,
                                                            TddVideo.recent == 1).all()
            return list(r[0] for r in result)
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' % (e, {'is_tid_30': is_tid_30}), exc_info=True)
            return None

    @classmethod
    def query_daily_new_video_aids(cls, is_tid_30, session):
        try:
            if is_tid_30:
                result = session.query(TddVideo.aid).filter(TddVideo.tid == 30, TddVideo.code == 0,
                                                            TddVideo.recent == 2).all()
            else:
                result = session.query(TddVideo.aid).filter(TddVideo.tid != 30, TddVideo.code == 0,
                                                            TddVideo.recent == 2).all()
            return list(r[0] for r in result)
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' % (e, {'is_tid_30': is_tid_30}), exc_info=True)
            return None

    @classmethod
    def query_video_aids(cls, offset, size, session):
        try:
            result = session.execute(
                'select aid from tdd_video limit %d, %d' % (offset, size))
            return list(r[0] for r in result)
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' % (e, {'offset': offset, 'size': size}), exc_info=True)
            return None

    @classmethod
    def query_video_bvids(cls, offset, size, session):
        try:
            result = session.execute(
                'select bvid from tdd_video limit %d, %d' % (offset, size))
            return list(r[0] for r in result)
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' % (e, {'offset': offset, 'size': size}), exc_info=True)
            return None

    @classmethod
    def query_member_mids(cls, offset, size, session):
        try:
            result = session.execute(
                'select mid from tdd_member limit %d, %d' % (offset, size))
            return list(r[0] for r in result)
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' % (e, {'offset': offset, 'size': size}), exc_info=True)
            return None

    @classmethod
    def query_all_member_mids(cls, session):
        try:
            result = session.execute('select mid from tdd_member')
            return list(r[0] for r in result)
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' % (e, {}), exc_info=True)
            return None

    @classmethod
    def query_task_video_record(cls, session):
        try:
            return session.query(TddTaskVisitVideoRecord).filter(TddTaskVisitVideoRecord.status == 0).all()
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' % (e, {}), exc_info=True)
            return None

    @classmethod
    def query_last_added_via_aid(cls, aid, session):
        try:
            result = session.execute(
                'select r.added from tdd_video v left join tdd_video_record r on v.laststat=r.id where v.aid = %d'
                % aid).first()
            return int(result[0])
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' % (e, {'aid': aid}), exc_info=True)
            return None

    @classmethod
    def query_all_update_video_aids(cls, is_tid_30, session):
        try:
            if is_tid_30:
                result = session.query(TddVideo.aid).filter(TddVideo.tid == 30, TddVideo.code == 0).all()
            else:
                result = session.query(TddVideo.aid).filter(TddVideo.tid != 30, TddVideo.code == 0).all()
            return list(r[0] for r in result)
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' % (e, {'is_tid_30': is_tid_30}), exc_info=True)
            return None

    @classmethod
    def query_freq_update_video_aids(cls, freq, is_tid_30, session):
        try:
            if is_tid_30:
                result = session.query(TddVideo.aid).filter(TddVideo.tid == 30, TddVideo.code == 0,
                                                            TddVideo.freq == freq).all()
            else:
                result = session.query(TddVideo.aid).filter(TddVideo.tid != 30, TddVideo.code == 0,
                                                            TddVideo.freq == freq).all()
            return list(r[0] for r in result)
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' % (e, {'freq': freq, 'is_tid_30': is_tid_30}), exc_info=True)
            return None

    @classmethod
    def query_video_pubdate_all(cls, session):
        try:
            result = session.query(TddVideo.aid, TddVideo.pubdate).all()
            return list((r[0], r[1]) for r in result)
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' % (e, {}), exc_info=True)
            return None

    @classmethod
    def query_video_record_rank_weekly_base_dict(cls, session):
        try:
            result = session.execute('select * from tdd_video_record_rank_hourly_base')
            d = {}
            for r in result:
                d[r[1]] = (r[0], r[2], r[3], r[4], r[5], r[6], r[7], r[8])
            return d
        except Exception as e:
            logger_db.error('Exception: %s, params: %s' % (e, {}), exc_info=True)
            return None
