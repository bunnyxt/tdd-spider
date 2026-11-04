"""Parameterized multi-row writes; the caller rolls back the active batch."""

from itertools import islice

from .member_total_stat import STAT_FIELDS

FIELDS = ('added', 'mid', 'video_count') + STAT_FIELDS


def write_member_stats(connection, records, logger, batch_size=1000):
    if type(batch_size) is not int or batch_size <= 0:
        raise ValueError('batch_size must be a positive integer')
    records = iter(records)
    committed = 0
    cursor = connection.cursor()
    batch = []
    try:
        while True:
            batch = list(islice(records, batch_size))
            if not batch:
                break
            columns = ', '.join('`'+field+'`' for field in FIELDS)
            placeholders = '('+', '.join(['%s']*len(FIELDS))+')'
            sql = ('INSERT INTO tdd_member_total_stat_record ('+columns+') VALUES '+
                   ', '.join([placeholders]*len(batch)))
            params = tuple(record[field] for record in batch for field in FIELDS)
            cursor.execute(sql, params)
            connection.commit()
            committed += len(batch)
            logger.info('Aggregate batch committed: rows=%d, committed_rows=%d', len(batch), committed)
    except Exception:
        logger.critical('Incomplete aggregate write: confirmed_committed_rows=%d, '
                        'failed_batch_rows=%d, failed_batch_start=%d. '
                        'Earlier batches remain; the caller must roll back the active batch. '
                        'A failed commit may have an unknown outcome. '
                        'Do not rerun blindly: there is no (mid, added) uniqueness constraint.',
                        committed, len(batch), committed + 1)
        raise
    finally:
        cursor.close()
    return committed
