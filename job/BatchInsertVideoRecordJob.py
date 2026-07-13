import time
from .Job import Job
from queue import Queue, Empty
from db import Session
from core import RecordNew
from task import commit_video_records_batch
from typing import Optional

__all__ = ['BatchInsertVideoRecordJob']


class BatchInsertVideoRecordJob(Job):
    """
    Dedicated DB writer: drains RecordNew from record_queue and persists them
    with multi-row INSERTs, one commit per batch (commit_video_records_batch).
    Profiling showed per-record commits under high fetch concurrency cost
    ~157ms/record from commit/fsync contention; a single writer with batches
    removes that entirely.

    Each record is forwarded to downstream_queue after its batch is processed
    (insert failures are logged and counted, records still forwarded so the
    downstream csv/analysis stays complete -- same policy as the per-record
    path, where DBOperation.add swallows insert errors).

    Terminates on a None sentinel: the producer side must put one None per
    writer AFTER all fetch workers finished; being FIFO, the sentinel is
    guaranteed to arrive after every record.
    """

    def __init__(self, name: str, record_queue: 'Queue[Optional[RecordNew]]',
                 downstream_queue: Queue, batch_size: int = 1000, poll_timeout_s: float = 1.0):
        super().__init__(name)
        self.record_queue = record_queue
        self.downstream_queue = downstream_queue
        self.batch_size = batch_size
        self.poll_timeout_s = poll_timeout_s
        self.session = Session()

    def _flush(self, batch: list):
        if not batch:
            return
        flush_start = time.perf_counter()
        try:
            commit_video_records_batch(batch, self.session)
        except Exception as e:
            # Recover the session so a failed batch does not leave it in an
            # invalid-transaction state; records are still forwarded downstream.
            try:
                self.session.rollback()
            except Exception:
                pass
            self.logger.error(f'Fail to batch insert {len(batch)} video record(s). error: {e}')
            self.stat.condition['batch_insert_fail'] += 1
        else:
            self.stat.condition['batch_insert'] += 1
        flush_ms = int((time.perf_counter() - flush_start) * 1000)

        for record in batch:
            self.downstream_queue.put(record)

        # db_ms feeds JobPool's heartbeat as per-record stage average
        self.stat.condition['db_ms'] += flush_ms
        self.logger.debug(f'BATCH size={len(batch)} db={flush_ms}ms')
        self.stat.total_count += len(batch)
        self.stat.total_duration_ms += flush_ms

    def process(self):
        batch = []
        while True:
            try:
                item = self.record_queue.get(timeout=self.poll_timeout_s)
            except Empty:
                # queue momentarily empty: flush what we have so records don't
                # sit here while fetching is slow
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
