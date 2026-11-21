"""Detached read results; each query releases its pooled connection before return."""
from psycopg.rows import dict_row

from .basic import engine
from .models import TddVideo, TddMember
from .values import epoch, timestamp


def query_video_via_aid(aid):
    with engine.connection() as conn, conn.cursor(row_factory=dict_row) as cursor:
        cursor.execute('SELECT * FROM tdd_video WHERE aid = %s', (aid,))
        row = cursor.fetchone()
        return TddVideo(**{key: epoch(value) for key, value in row.items()}) if row else None


def query_member_via_mid(mid):
    with engine.connection() as conn, conn.cursor(row_factory=dict_row) as cursor:
        cursor.execute('SELECT * FROM tdd_member WHERE mid = %s', (mid,))
        row = cursor.fetchone()
        return TddMember(**{key: epoch(value) for key, value in row.items()}) if row else None


def query_all_video_bvids():
    # Preserve the existing candidate enumeration; no implicit regrouping.
    with engine.connection() as conn:
        return [row[0] for row in conn.execute('SELECT bvid FROM tdd_video').fetchall()]


def query_all_member_mids():
    with engine.connection() as conn:
        return [row[0] for row in conn.execute('SELECT mid FROM tdd_member').fetchall()]


def _update_video_aids(freq=None, is_tid_30=None):
    conditions, params = ['code = 0', 'state = 0'], []
    if freq is not None:
        conditions.append('freq = %s')
        params.append(freq)
    if is_tid_30 is not None:
        conditions.append('tid = %s' if is_tid_30 else 'tid <> %s')
        params.append(30)
    with engine.connection() as conn:
        return [row[0] for row in conn.execute(
            'SELECT aid FROM tdd_video WHERE ' + ' AND '.join(conditions), tuple(params)).fetchall()]


def query_all_update_video_aids(is_tid_30=None):
    return _update_video_aids(is_tid_30=is_tid_30)


def query_freq_update_video_aids(freq, is_tid_30=None):
    return _update_video_aids(freq=freq, is_tid_30=is_tid_30)


def query_video_pubdate_all():
    with engine.connection() as conn:
        return conn.execute('SELECT aid, pubdate FROM tdd_video').fetchall()


def query_video_records_of_given_aid_added_before_given_ts(aid, ts):
    with engine.connection() as conn:
        return conn.execute('SELECT aid FROM tdd_video WHERE aid = %s AND added < %s',
                            (aid, timestamp(ts))).fetchall()


def query_member_total_stat_inputs():
    """Return the known member ids and video/latest/staff rows used by 18_."""
    with engine.connection() as conn:
        members = dict(conn.execute(
            'SELECT m.mid, r.id FROM tdd_member m '
            'LEFT JOIN tdd_member_total_stat_record_latest r ON m.mid = r.mid WHERE m.mid > 0').fetchall())
        # Include previous aggregate entities whose profile has never arrived.
        members.update(conn.execute(
            'SELECT mid, id FROM tdd_member_total_stat_record_latest WHERE mid > 0').fetchall())
        rows = conn.execute(
            'SELECT v.aid, v.mid AS v_mid, '
            'r.view, r.danmaku, r.reply, r.favorite, r.coin, r.share, r.like, '
            's.mid AS s_mid, v.code, v.state, r.id '
            'FROM tdd_video v '
            'LEFT JOIN tdd_video_record_latest r ON v.aid = r.aid '
            'LEFT JOIN tdd_video_staff s ON v.aid = s.aid '
            'WHERE v.mid IS NOT NULL AND r.id IS NOT NULL').fetchall()
        return members, rows


def query_processing_sprint_aids():
    with engine.connection() as conn:
        return [row[0] for row in conn.execute(
            'SELECT aid FROM tdd_sprint_video WHERE status = %s', ('processing',)).fetchall()]


def query_sprint_records_in_window(start_ts, end_ts):
    with engine.connection() as conn:
        return conn.execute('SELECT aid, "view" FROM tdd_sprint_video_record '
                            'WHERE added >= %s AND added < %s',
                            (timestamp(start_ts), timestamp(end_ts))).fetchall()


def query_first_sprint_record(aid):
    """Return (aid, view), including a nullable view, or None when no row exists."""
    with engine.connection() as conn:
        return conn.execute('SELECT aid, "view" FROM tdd_sprint_video_record '
                            'WHERE aid = %s ORDER BY added LIMIT 1', (aid,)).fetchone()


def query_sprint_video_created(aid):
    with engine.connection() as conn:
        row = conn.execute('SELECT created FROM tdd_sprint_video WHERE aid = %s', (aid,)).fetchone()
        return epoch(row[0]) if row else None


def query_last_sprint_daily_view_incr():
    with engine.connection() as conn:
        row = conn.execute('SELECT viewincr FROM tdd_sprint_daily '
                            'WHERE viewincr IS NOT NULL ORDER BY id DESC LIMIT 1').fetchone()
        return row[0] if row else None
