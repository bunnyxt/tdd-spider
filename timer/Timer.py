from typing import Optional
from util import get_ts_ms, ts_ms_to_str, format_ts_ms

__all__ = ['Timer']


class Timer:
    def __init__(self):
        self.start_ts_ms: Optional[int] = None
        self.end_ts_ms: Optional[int] = None

    def start(self):
        self.start_ts_ms = get_ts_ms()

    def stop(self):
        self.end_ts_ms = get_ts_ms()

    def reset(self):
        self.start_ts_ms = None
        self.end_ts_ms = None

    def get_duration_ms(self) -> Optional[int]:
        if self.start_ts_ms is None or self.end_ts_ms is None:
            return None
        return self.end_ts_ms - self.start_ts_ms

    def get_summary(self) -> str:
        duration_ms = self.get_duration_ms()
        if duration_ms is None:
            return 'Timer not started or ended.'
        return f'start: {ts_ms_to_str(self.start_ts_ms)}, ' \
               f'end: {ts_ms_to_str(self.end_ts_ms)}, ' \
               f'duration: {format_ts_ms(duration_ms)}'
