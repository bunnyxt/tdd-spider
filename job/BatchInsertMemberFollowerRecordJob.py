import time
from .Job import Job
from queue import Queue, Empty
from db import Session
from core import MemberFollowerRecordNew
from task import commit_member_follower_records_batch
from typing import Optional

__all__ = ['BatchInsertMemberFollowerRecordJob']


class BatchInsertMemberFollowerRecordJob(Job):
    """
    Dedicated DB writer: drains MemberFollowerRecordNew from record_queue and
    persists them with multi-row INSERTs, one commit per batch. A single writer
    with batches removes the per-record commit/fsync contention of the old
    per-worker-insert path (17_ at 20 workers each committing per member).

    A failing batch is split and retried (1000 -> 500 -> ...) so a transient
    blip loses at most one record, not a whole batch. Terminates on a None
    sentinel: the producer puts one None after all fetch workers finish (FIFO
    guarantees it arrives behind every record).

    Follower records are an append-only time series and there is no downstream
    consumer, so (unlike the video writer) records are not forwarded on and
    there is no recovery file -- a dropped record is picked up next run.
    """

    def __init__(self, name: str, record_queue: 'Queue[Optional[MemberFollowerRecordNew]]',
                 batch_size: int = 1000, poll_timeout_s: float = 1.0):
        super().__init__(name)
        self.record_queue = record_queue
        self.batch_size = batch_size
        self.poll_timeout_s = poll_timeout_s
        self.session = Session()

    def _rollback_quietly(self):
        try:
            self.session.rollback()
        except Exception:
            pass

    def _insert_splitting(self, batch: list, depth: int = 0) -> int:
        if not batch:
            return 0
        try:
            commit_member_follower_records_batch(batch, self.session)
        except Exception as e:
            self._rollback_quietly()
            if len(batch) == 1:
                self.logger.error(
                    f'Fail to insert follower record after splitting to one row. '
                    f'mid: {batch[0].mid}, error: {e}')
                self.stat.condition['batch_insert_fail'] += 1
                return 0
            self.logger.warning(
                f'Batch insert failed, splitting and retrying. '
                f'size: {len(batch)}, mids: {batch[0].mid}..{batch[-1].mid}, error: {e}')
            self.stat.condition['batch_insert_split'] += 1
            mid = len(batch) // 2
            return (self._insert_splitting(batch[:mid], depth + 1)
                    + self._insert_splitting(batch[mid:], depth + 1))
        else:
            self.stat.condition['batch_insert' if depth == 0 else 'batch_insert_split_ok'] += 1
            return len(batch)

    def _flush(self, batch: list):
        if not batch:
            return
        flush_start = time.perf_counter()
        persisted = self._insert_splitting(batch)
        flush_ms = int((time.perf_counter() - flush_start) * 1000)
        if persisted < len(batch):
            self.logger.error(
                f'{len(batch) - persisted} of {len(batch)} follower record(s) not persisted.')
        self.stat.condition['db_ms'] += flush_ms
        self.logger.debug(f'BATCH size={len(batch)} persisted={persisted} db={flush_ms}ms')
        self.stat.total_count += len(batch)
        self.stat.total_duration_ms += flush_ms

    def process(self):
        batch = []
        while True:
            try:
                item = self.record_queue.get(timeout=self.poll_timeout_s)
            except Empty:
                self._flush(batch)
                batch = []
                continue
            if item is None:  # sentinel: all producers finished
                self._flush(batch)
                batch = []
                break
            batch.append(item)
            if len(batch) >= self.batch_size:
                self._flush(batch)
                batch = []

    def cleanup(self):
        self.session.close()
