"""Small row objects. SQL and transaction ownership live in the data layer."""
from types import SimpleNamespace


class Row(SimpleNamespace):
    table = None
    fields = ()
    defaults = {}

    def __init__(self, **values):
        unknown = set(values) - set(self.fields)
        if unknown:
            raise TypeError(f'Unknown {self.table} fields: {sorted(unknown)}')
        super().__init__(**{**dict.fromkeys(self.fields), **self.defaults, **values})


STAT_FIELDS = ('view', 'danmaku', 'reply', 'favorite', 'coin', 'share', 'like')


def row_type(name, table, fields, defaults=None):
    return type(name, (Row,), {'table': table, 'fields': tuple(fields), 'defaults': defaults or {}})


TddVideo = row_type('TddVideo', 'tdd_video',
    ('id','added','aid','bvid','videos','tid','tname','copyright','pic','title','pubdate','desc','tags','mid',
     'code','attribute','state','forward','hasstaff','solo','original','employed','isvc','engine','freq','activity','recent'),
    dict(hasstaff=-1, solo=-1, original=-1, employed=-1, isvc=-1, engine=-1, freq=0, activity=0, recent=0))
TddMember = row_type('TddMember', 'tdd_member',
    ('id','added','mid','sex','name','face','sign','code','last_video_aid'))
TddVideoStaff = row_type('TddVideoStaff', 'tdd_video_staff', ('id','added','aid','bvid','mid','title'))
TddVideoRecord = row_type('TddVideoRecord', 'tdd_video_record', ('id','added','aid') + STAT_FIELDS)
TddMemberFollowerRecord = row_type('TddMemberFollowerRecord', 'tdd_member_follower_record', ('id','added','mid','follower'))
TddMemberTotalStatRecord = row_type('TddMemberTotalStatRecord', 'tdd_member_total_stat_record', ('id','added','mid','video_count') + STAT_FIELDS)
TddSprintVideoRecord = row_type('TddSprintVideoRecord', 'tdd_sprint_video_record', ('id','added','aid') + STAT_FIELDS)
TddVideoRecordAbnormalChange = row_type('TddVideoRecordAbnormalChange', 'tdd_video_record_abnormal_change',
    ('id','added','aid','attr','speed_now','speed_last','speed_now_incr_rate','period_range','speed_period','speed_overall',
     'this_record_id','this_added') + tuple('this_' + f for f in STAT_FIELDS) +
    ('last_record_id','last_added') + tuple('last_' + f for f in STAT_FIELDS) + ('description','comment'))


class TddVideoLog(Row):
    table = 'tdd_video_log'
    fields = ('id','added','aid','attr','oldval','newval')
    def __init__(self, added=None, aid=None, bvid=None, attr=None, oldval=None, newval=None, **values):
        super().__init__(added=added, aid=aid, attr=attr, oldval=oldval, newval=newval, **values)


class TddMemberLog(Row):
    table = 'tdd_member_log'
    fields = ('id','added','mid','attr','oldval','newval')
    def __init__(self, added=None, mid=None, attr=None, oldval=None, newval=None, **values):
        super().__init__(added=added, mid=mid, attr=attr, oldval=oldval, newval=newval, **values)


__all__ = [name for name in globals() if name.startswith('Tdd')]
