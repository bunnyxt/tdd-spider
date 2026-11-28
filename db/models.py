"""Small row objects. SQL and transaction ownership live in the data layer."""
from collections.abc import Iterable, Mapping
from types import SimpleNamespace
from typing import Any


class Row(SimpleNamespace):
    table = None
    fields = ()
    defaults = {}

    def __init__(self, **values: Any) -> None:
        unknown = set(values) - set(self.fields)
        if unknown:
            raise TypeError(f'Unknown {self.table} fields: {sorted(unknown)}')
        super().__init__(**{**dict.fromkeys(self.fields), **self.defaults, **values})


STAT_FIELDS = ('view', 'danmaku', 'reply', 'favorite', 'coin', 'share', 'like')


def row_type(name: str, table: str, fields: Iterable[str],
             defaults: Mapping[str, Any] | None = None) -> type[Row]:
    return type(name, (Row,), {'table': table, 'fields': tuple(fields), 'defaults': defaults or {}})


TddVideo = row_type('TddVideo', 'tdd_video',
    ('id','added','aid','bvid','videos','tid','tname','copyright','pic','title','pubdate','desc','tags','mid',
     'code','attribute','state','forward','hasstaff','solo','original','employed','isvc','engine','freq','activity','recent'),
    dict(hasstaff=-1, solo=-1, original=-1, employed=-1, isvc=-1, engine=-1, freq=0, activity=0, recent=0))
TddMember = row_type('TddMember', 'tdd_member',
    ('id','added','mid','sex','name','face','sign','code','last_video_aid'))
TddVideoStaff = row_type('TddVideoStaff', 'tdd_video_staff', ('id','added','aid','bvid','mid','title'))
TddSprintVideoRecord = row_type('TddSprintVideoRecord', 'tdd_sprint_video_record', ('id','added','aid') + STAT_FIELDS)


TddVideoLog = row_type('TddVideoLog', 'tdd_video_log', ('id','added','aid','attr','oldval','newval'))
TddMemberLog = row_type('TddMemberLog', 'tdd_member_log', ('id','added','mid','attr','oldval','newval'))


__all__ = [name for name in globals() if name.startswith('Tdd')]
