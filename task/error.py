"""Compatibility exports for callers handling persistence errors."""
from db.errors import TaskError, NotExistError, AlreadyExistError

__all__ = ['TaskError', 'NotExistError', 'AlreadyExistError']
