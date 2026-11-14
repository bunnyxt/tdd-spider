"""Parameterized PostgreSQL operations; errors propagate to transaction owners."""
from .models import TddVideo, TddMember, TddVideoStaff
from .values import database_value, timestamp

__all__ = ['DBOperation']


class DBOperation:
    @classmethod
    def add(cls, obj, session):
        from .write import append_objects
        if obj.table in ('tdd_video', 'tdd_member', 'tdd_video_staff'):
            columns = [field for field in obj.fields if field not in ('id','last_video_aid')]
            quoted = ', '.join('"' + field + '"' for field in columns)
            params = tuple(database_value(field, getattr(obj, field)) for field in columns)
            row = session.execute('INSERT INTO ' + obj.table + ' (' + quoted + ') VALUES (' +
                                  ', '.join(['%s'] * len(columns)) + ') RETURNING id', params).first()
            obj.id = row[0]
        else:
            if session._depth:
                append_objects(session, [obj])
            else:
                from .replay import observe
                persisted = observe('append', {'objects':[obj]})
                obj.id = persisted[0].id
        return obj

    @classmethod
    def update(cls, obj, fields, session):
        if (set(fields) & {'id','added','last_video_aid'}) or not set(fields).issubset(obj.fields) or obj.table not in ('tdd_video','tdd_member','tdd_video_staff'):
            raise ValueError('Unsupported update columns')
        if not fields:
            return
        assignments = ', '.join('"' + field + '" = %s' for field in fields)
        session.execute('UPDATE ' + obj.table + ' SET ' + assignments + ' WHERE id = %s',
                        tuple(database_value(field, getattr(obj, field)) for field in fields) + (obj.id,))

    @classmethod
    def _one(cls, model, where, params, session, lock=False):
        row = session.execute('SELECT * FROM ' + model.table + ' WHERE ' + where +
                              (' FOR UPDATE' if lock else ''), params).first()
        return model(**row) if row else None

    @classmethod
    def query_video_via_aid(cls, aid, session, lock=False):
        return cls._one(TddVideo, 'aid = %s', (aid,), session, lock)

    @classmethod
    def query_member_via_mid(cls, mid, session, lock=False):
        return cls._one(TddMember, 'mid = %s', (mid,), session, lock)

    @classmethod
    def query_video_staff_via_aid_mid(cls, aid, mid, session):
        return cls._one(TddVideoStaff, 'aid = %s AND mid = %s', (aid,mid), session)

    @classmethod
    def query_video_staff_via_aid(cls, aid, session):
        return [TddVideoStaff(**r) for r in session.execute('SELECT * FROM tdd_video_staff WHERE aid = %s', (aid,))]

    @classmethod
    def delete_video_staff_via_aid_mid(cls, aid, mid, session):
        session.execute('DELETE FROM tdd_video_staff WHERE aid = %s AND mid = %s', (aid,mid))

    @classmethod
    def delete_video_staff_via_id(cls, id, session):
        session.execute('DELETE FROM tdd_video_staff WHERE id = %s', (id,))

    @classmethod
    def query_all_video_bvids(cls, session):
        # Preserve the existing candidate enumeration; no implicit regrouping.
        return [r[0] for r in session.execute('SELECT bvid FROM tdd_video')]

    @classmethod
    def query_member_mids(cls, offset, size, session):
        return [r[0] for r in session.execute('SELECT mid FROM tdd_member LIMIT %s OFFSET %s', (size,offset))]

    @classmethod
    def query_all_member_mids(cls, session):
        return [r[0] for r in session.execute('SELECT mid FROM tdd_member')]

    @classmethod
    def _aids(cls, session, freq=None, is_tid_30=None):
        conditions, params = ['code = 0', 'state = 0'], []
        if freq is not None:
            conditions.append('freq = %s')
            params.append(freq)
        if is_tid_30 is not None:
            conditions.append('tid = %s' if is_tid_30 else 'tid <> %s')
            params.append(30)
        return [r[0] for r in session.execute('SELECT aid FROM tdd_video WHERE ' + ' AND '.join(conditions), tuple(params))]

    @classmethod
    def query_all_update_video_aids(cls, session, is_tid_30=None):
        return cls._aids(session, is_tid_30=is_tid_30)

    @classmethod
    def query_freq_update_video_aids(cls, freq, session, is_tid_30=None):
        return cls._aids(session, freq, is_tid_30)

    @classmethod
    def query_403_video_aids(cls, session):
        return [r[0] for r in session.execute('SELECT aid FROM tdd_video WHERE code = %s', (-403,))]

    @classmethod
    def query_video_pubdate_all(cls, session):
        return [(r[0],r[1]) for r in session.execute('SELECT aid, pubdate FROM tdd_video')]

    @classmethod
    def query_video_records_of_given_aid_added_before_given_ts(cls, aid, ts, session):
        return session.execute('SELECT aid FROM tdd_video WHERE aid = %s AND added < %s', (aid,timestamp(ts)))
