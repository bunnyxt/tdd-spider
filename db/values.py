"""Normalize database boundaries without changing domain null or text semantics."""
from __future__ import annotations
from datetime import datetime, timezone
from typing import Any, TypeVar, overload

_Value = TypeVar('_Value')

TIME_FIELDS = frozenset(('added', 'this_added', 'last_added'))


@overload
def clean_text(value: str) -> str: ...


@overload
def clean_text(value: _Value) -> _Value: ...


def clean_text(value: Any) -> Any:
    return value.replace('\x00', '') if isinstance(value, str) else value


def timestamp(value: int | datetime | None) -> datetime | None:
    if value is None or isinstance(value, datetime):
        return value
    if type(value) is not int:
        raise TypeError('Database timestamps require integer Unix seconds')
    return datetime.fromtimestamp(value, timezone.utc)


def database_value(field: str, value: Any) -> Any:
    return timestamp(value) if field in TIME_FIELDS else clean_text(value)


def epoch(value: Any) -> Any:
    return int(value.timestamp()) if isinstance(value, datetime) else value
