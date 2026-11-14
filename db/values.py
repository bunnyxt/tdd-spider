"""Normalize database boundaries without changing domain null or text semantics."""
from datetime import datetime, timezone

TIME_FIELDS = frozenset(('added', 'this_added', 'last_added'))


def clean_text(value):
    return value.replace('\x00', '') if isinstance(value, str) else value


def timestamp(value):
    if value is None or isinstance(value, datetime):
        return value
    if type(value) is not int:
        raise TypeError('Database timestamps require integer Unix seconds')
    return datetime.fromtimestamp(value, timezone.utc)


def database_value(field, value):
    return timestamp(value) if field in TIME_FIELDS else clean_text(value)


def epoch(value):
    return int(value.timestamp()) if isinstance(value, datetime) else value
