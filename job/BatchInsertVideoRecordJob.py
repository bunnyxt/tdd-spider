import csv
import time
from pathlib import Path
from .Job import Job
from queue import Queue, Empty
from db import Session
from core import RecordNew
from task import commit_video_records_batch
from typing import Optional

__all__ = ['BatchInsertVideoRecordJob']


def _short(e: Exception, limit: int = 300) -> str:
    # SQLAlchemy embeds the whole failing statement in the exception, which for
    # a 1000-row multi-row INSERT is ~20k characters -- unreadable in the log
    # and it bloats the file. Keep the error, drop the statement dump.
    text = str(e).split('\n[SQL:')[0]
    return text if len(text) <= limit else text[:limit] + '...'


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

    A failing batch is split and retried rather than dropped: a whole-batch
    failure used to lose up to `batch_size` records from tdd_video_record
    (seen in production when a 1000-row insert hit MySQL's timeout at 87s).
    Halving isolates the bad/slow rows and lets the rest land; anything still
    failing at a single record is written to a recovery csv so it can be
    replayed instead of vanishing.

    Terminates on a None sentinel: the producer side must put one None per
    writer AFTER all fetch workers finished; being FIFO, the sentinel is
    guaranteed to arrive after every record.
    """

    def __init__(self, name: str, record_queue: 'Queue[Optional[RecordNew]]',
                 downstream_queue: Queue, batch_size: int = 1000, poll_timeout_s: float = 1.0,
                 recovery_path: Optional[str] = None):
        super().__init__(name)
        self.record_queue = record_queue
        self.downstream_queue = downstream_queue
        self.batch_size = batch_size
        self.poll_timeout_s = poll_timeout_s
        self.recovery_path = recovery_path
        self.session = Session()

    def _rollback_quietly(self):
        try:
            self.session.rollback()
        except Exception:
            pass

    def _to_recovery_file(self, records: list, error: str):
        # last resort: records that no amount of splitting could persist. Write
        # them out so a failed insert is recoverable instead of silently absent
        # from tdd_video_record.
        if self.recovery_path is None:
            return
        try:
            path = Path(self.recovery_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            is_new = not path.exists()
            with path.open('a', newline='') as f:
                writer = csv.writer(f)
                if is_new:
                    writer.writerow(list(RecordNew._fields) + ['error'])
                for r in records:
                    writer.writerow(list(r) + [error])
        except Exception as e:
            self.logger.error(f'Fail to write {len(records)} record(s) to recovery file '
                              f'{self.recovery_path}. error: {_short(e)}')

    def _insert(self, batch: list) -> bool:
        # one attempt; caller owns the splitting policy
        try:
            commit_video_records_batch(batch, self.session)
        except Exception as e:
            # recover the session, else the failed transaction poisons every
            # subsequent statement on it
            self._rollback_quietly()
            self._last_error = _short(e)
            return False
        return True

    def _insert_splitting(self, batch: list, depth: int = 0) -> int:
        """Insert `batch`, halving on failure. Returns count persisted."""
        if not batch:
            return 0

        if self._insert(batch):
            if depth == 0:
                self.stat.condition['batch_insert'] += 1
            else:
                self.stat.condition['batch_insert_split_ok'] += 1
            return len(batch)

        # a single record that still fails is genuinely unpersistable (bad row,
        # or the DB is down) -- park it rather than spin
        if len(batch) == 1:
            self.logger.error(
                f'Fail to insert video record after splitting to a single row. '
                f'aid: {batch[0].aid}, error: {self._last_error}')
            self.stat.condition['batch_insert_fail'] += 1
            self._to_recovery_file(batch, self._last_error)
            return 0

        self.logger.warning(
            f'Batch insert failed, splitting and retrying. '
            f'size: {len(batch)}, aids: {batch[0].aid}..{batch[-1].aid}, error: {self._last_error}')
        self.stat.condition['batch_insert_split'] += 1

        mid = len(batch) // 2
        return (self._insert_splitting(batch[:mid], depth + 1)
                + self._insert_splitting(batch[mid:], depth + 1))

    def _flush(self, batch: list):
        if not batch:
            return
        flush_start = time.perf_counter()
        persisted = self._insert_splitting(batch)
        flush_ms = int((time.perf_counter() - flush_start) * 1000)

        if persisted < len(batch):
            self.logger.error(
                f'{len(batch) - persisted} of {len(batch)} record(s) not persisted to '
                f'tdd_video_record in this batch.')

        for record in batch:
            self.downstream_queue.put(record)

        # db_ms feeds JobPool's heartbeat as per-record stage average
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
