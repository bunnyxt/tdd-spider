"""Commit independent aggregate batches in explicit business transactions."""
from itertools import islice
from db.write import insert_member_total_stat_records


def write_member_stats(records, logger, batch_size=1000):
    if type(batch_size) is not int or batch_size <= 0:
        raise ValueError('batch_size must be a positive integer')
    records, committed = iter(records), 0
    while True:
        batch = list(islice(records,batch_size))
        if not batch:
            return committed
        try:
            insert_member_total_stat_records(batch)
        except Exception as error:
            logger.critical('Aggregate batch write failed: rows=%d, committed_rows=%d, error=%s',
                            len(batch),committed,error)
            raise
        committed += len(batch)
        logger.info('Aggregate batch committed: rows=%d, committed_rows=%d',len(batch),committed)
