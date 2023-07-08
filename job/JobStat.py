from collections import Counter
from util import format_ts_ms
from typing import Optional

__all__ = ['JobStat']


class JobStat:
    def __init__(self):
        self.start_ts_ms: Optional[int] = None
        self.end_ts_ms: Optional[int] = None
        self.total_count = 0
        self.total_duration_ms = 0
        self.condition: Counter[str] = Counter()

    def get_duration_ms(self) -> int:
        return self.end_ts_ms - self.start_ts_ms

    def get_avg_duration_ms(self) -> int:
        if self.total_count == 0:
            return 0
        return self.total_duration_ms // self.total_count

    def get_summary(self) -> str:
        time_str = f'total count: {self.total_count}, average duration: {format_ts_ms(self.get_avg_duration_ms())}'
        condition_str = '\n\n'.join([f'- {key}: {value}' for key, value in self.condition.items()])
        return '\n\n'.join(['## job stat summary', time_str, '### conditions', condition_str])

    def __add__(self, other):
        new_job_stat = JobStat()
        if self.start_ts_ms is not None and other.start_ts_ms is not None:
            new_job_stat.start_ts_ms = min(self.start_ts_ms, other.start_ts_ms)
            new_job_stat.end_ts_ms = max(self.end_ts_ms, other.end_ts_ms)
        new_job_stat.total_count = self.total_count + other.total_count
        new_job_stat.total_duration_ms = self.total_duration_ms + other.total_duration_ms
        new_job_stat.condition = self.condition + other.condition
        return new_job_stat
