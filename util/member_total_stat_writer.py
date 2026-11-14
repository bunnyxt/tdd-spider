"""Commit independent aggregate batches through the shared receipt protocol."""
from itertools import islice
from uuid import uuid4
from db import TddMemberTotalStatRecord
from db.replay import observe
from .member_total_stat import STAT_FIELDS

FIELDS = ('added','mid','video_count') + STAT_FIELDS


def write_member_stats(session, records, logger, batch_size=1000):
    if type(batch_size) is not int or batch_size <= 0:
        raise ValueError('batch_size must be a positive integer')
    records, committed = iter(records), 0
    while True:
        batch = list(islice(records,batch_size))
        if not batch:
            return committed
        batch_id = uuid4()
        try:
            observe('append',{'objects':[TddMemberTotalStatRecord(**record) for record in batch]},batch_id=batch_id)
        except Exception:
            logger.critical('Incomplete aggregate write: confirmed_committed_rows=%d, failed_batch_rows=%d, '
                            'batch_id=%s. Resolve the receipt before replaying this observation.',
                            committed,len(batch),batch_id)
            raise
        committed += len(batch)
        logger.info('Aggregate batch committed: rows=%d, committed_rows=%d, batch_id=%s',len(batch),committed,batch_id)
