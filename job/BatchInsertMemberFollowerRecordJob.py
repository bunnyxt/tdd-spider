import time
from queue import Empty
from .Job import Job
from task import commit_member_follower_records_batch

__all__ = ['BatchInsertMemberFollowerRecordJob']


class BatchInsertMemberFollowerRecordJob(Job):
    """One follower writer; count only confirmed database batches."""
    def __init__(self, name, record_queue, batch_size=1000, poll_timeout_s=1.0):
        super().__init__(name)
        self.record_queue, self.batch_size = record_queue,batch_size
        self.poll_timeout_s = poll_timeout_s

    def _flush(self, batch):
        if not batch:
            return
        start = time.perf_counter()
        try:
            commit_member_follower_records_batch(batch)
        except Exception as error:
            self.stat.condition['batch_insert_fail'] += len(batch)
            self.logger.error('Follower batch write failed: rows=%d, error=%s',len(batch),str(error)[:300])
        else:
            self.stat.condition['batch_insert'] += 1
            self.stat.condition['persisted'] += len(batch)
        elapsed = int((time.perf_counter()-start)*1000)
        self.stat.condition['db_ms'] += elapsed
        self.stat.total_count += len(batch)
        self.stat.total_duration_ms += elapsed

    def process(self):
        batch = []
        while True:
            try:
                item = self.record_queue.get(timeout=self.poll_timeout_s)
            except Empty:
                self._flush(batch)
                batch = []
                continue
            if item is None:
                self._flush(batch)
                return
            batch.append(item)
            if len(batch) >= self.batch_size:
                self._flush(batch)
                batch = []
