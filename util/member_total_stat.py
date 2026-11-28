"""Calculate future member snapshots without modifying source statistics."""

from collections import Counter

STAT_FIELDS = ('view', 'danmaku', 'reply', 'favorite', 'coin', 'share', 'like')


def aggregate_member_stats(rows, members, added, logger):
    """Keep staff attribution; members with a previous latest get a zero row.

    rows: aid, owner_mid, seven statistics, staff_mid, code, state, record_id.
    members: mid -> previous latest record id. Missing profiles do not discard
    attributable statistics. Eligibility matches acquisition's code=0 AND
    state=0; NULL state is explicitly excluded, with a warning.
    """
    def zero(mid):
        return dict(added=added, mid=mid, video_count=0,
                    **dict.fromkeys(STAT_FIELDS, 0))

    totals = {mid: zero(mid) for mid, latest in members.items() if latest is not None}
    counts, samples = Counter(), {}

    def note(kind, sample):
        counts[kind] += 1
        bucket = samples.setdefault(kind, [])
        if len(bucket) < 5:
            bucket.append(sample)

    for row in rows:
        aid, owner_mid = row[:2]
        staff_mid, code, state, record_id = row[9:13]
        mid = owner_mid if staff_mid is None else staff_mid
        if type(mid) is not int or mid <= 0:
            note('invalid_mid', (aid, owner_mid, staff_mid))
            continue
        # Remember the member before filtering so a newly excluded member
        # receives a zero latest, even if its profile has not been fetched yet.
        if mid not in totals:
            totals[mid] = zero(mid)
        total = totals[mid]
        if mid not in members:
            note('missing_member', (aid, mid))
        if code != 0:
            counts['excluded_code'] += 1
            continue
        if state is None:
            note('null_state', (aid, mid))
            continue
        if state != 0:
            note('excluded_state', (aid, mid, state))
            continue
        if record_id is None:
            raise ValueError(f'Missing latest video record: aid={aid}, mid={mid}')
        for field, value in zip(STAT_FIELDS, row[2:9]):
            if type(value) is not int or value < -1:
                raise ValueError(f'Invalid {field}: aid={aid}, mid={mid}, value={value!r}')
            if value == -1:
                note('unknown_stat', (aid, field))
                value = 0
            total[field] += value
        total['video_count'] += 1
        counts['eligible_rows'] += 1

    counts['zero_members'] = sum(total['video_count'] == 0 for total in totals.values())
    for kind, sample in samples.items():
        logger.warning('Aggregate %s: count=%d, sample=%s', kind, counts[kind], sample)
    logger.info('Aggregate input summary: %s', dict(counts))
    return totals
