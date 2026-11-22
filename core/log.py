"""Profile change-log inputs; stored row snapshots are returned by the database."""
from typing import NamedTuple

__all__ = ['NewVideoLog', 'NewMemberLog']


class NewVideoLog(NamedTuple):
    added: int
    aid: int
    attr: str
    oldval: str | None = None
    newval: str | None = None


class NewMemberLog(NamedTuple):
    added: int
    mid: int
    attr: str
    oldval: str | None = None
    newval: str | None = None
