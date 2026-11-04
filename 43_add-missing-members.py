"""Audit aggregate member IDs; explicitly apply real profile backfills.

Run without --apply to write an inventory. With --apply, --input must name a
completed inventory; --limit and --max-seconds bound each serial repair run.
Only profiles and a missing latest pointer are changed, never statistics.
"""

import argparse
from collections import Counter
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import time
import uuid


PAGE_SQL = ('SELECT mid FROM tdd_member_total_stat_record FORCE INDEX(mid_INDEX) '
            'WHERE mid > %s GROUP BY mid ORDER BY mid LIMIT %s')
LATEST_SQL = ('SELECT id FROM tdd_member_total_stat_record FORCE INDEX(mid_INDEX) '
              'WHERE mid = :mid ORDER BY id DESC LIMIT 1')


def now():
    return datetime.now(timezone.utc).isoformat()


def error_detail(error):
    # Exception text can include responses or connection details.
    code = getattr(error, 'code', None)
    if code is None and error.args and type(error.args[0]) is int:
        code = error.args[0]
    detail = {'error_type': type(error).__name__, 'error_code': code}
    reason = getattr(error, 'reason', None)
    if reason in ('request_exception', 'body_exception', 'deadline_exceeded',
                  'json_error', 'parse_error') or (
            isinstance(reason, str) and reason.startswith('http_')
            and len(reason) == 8 and reason[5:].isdigit()):
        detail['reason'] = reason
    return detail


def audit_missing_members(connection, page_size=1000, max_seconds=90):
    started = time.monotonic()
    report = dict(schema_version=1, scope='all_aggregate_distinct_mids',
                  observed_utc=now(), complete=False, page_size=page_size,
                  aggregate_distinct_mids=0, pages=0, missing_positive_mids=[],
                  missing_nonpositive_mids=[])
    cursor = connection.cursor()
    try:
        cursor.execute('SET SESSION MAX_EXECUTION_TIME=2000')
        cursor.execute('SET SESSION lock_wait_timeout=2')
        cursor.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
        cursor.execute('START TRANSACTION READ ONLY')
        cursor.execute('SELECT MIN(mid),MAX(mid) FROM tdd_member_total_stat_record FORCE INDEX(mid_INDEX)')
        lower, upper = cursor.fetchone()
        report['mid_bounds'] = [lower, upper]
        cursor.execute('SELECT mid FROM tdd_member')
        members = {row[0] for row in cursor.fetchall()}
        report['member_count'] = len(members)
        if lower is None:
            report['complete'] = True
        else:
            last = lower - 1
            cursor.execute('EXPLAIN ' + PAGE_SQL, (last, page_size))
            columns = [column[0] for column in cursor.description]
            plan = [dict(zip(columns, row)) for row in cursor.fetchall()]
            report['plan'] = plan
            if not plan or any('Using index for group-by' not in row.get('Extra', '')
                               or 'scanning' in row.get('Extra', '').lower()
                               or row.get('key') != 'mid_INDEX' for row in plan):
                raise RuntimeError('Unsafe aggregate history query plan')
            while True:
                if time.monotonic() - started >= max_seconds:
                    raise TimeoutError('Inventory time budget exceeded')
                cursor.execute(PAGE_SQL, (last, page_size))
                page = [row[0] for row in cursor.fetchall()]
                if not page:
                    report['complete'] = True
                    break
                if page != sorted(set(page)) or page[0] <= last:
                    raise RuntimeError('Inventory page did not advance')
                report['pages'] += 1
                report['aggregate_distinct_mids'] += len(page)
                for mid in page:
                    if mid not in members:
                        key = 'missing_positive_mids' if mid > 0 else 'missing_nonpositive_mids'
                        report[key].append(mid)
                last = page[-1]
                report['last_mid'] = last
                if len(page) < page_size:
                    report['complete'] = True
                    break
    except Exception as error:
        report['error'] = error_detail(error)
    finally:
        connection.rollback()
        cursor.close()
    report['missing_positive_count'] = len(report['missing_positive_mids'])
    report['missing_nonpositive_count'] = len(report['missing_nonpositive_mids'])
    report['elapsed_seconds'] = round(time.monotonic() - started, 3)
    report['finished_utc'] = now()
    return report


def inventory_mids(report):
    if report.get('complete') is not True or report.get('scope') != 'all_aggregate_distinct_mids':
        raise ValueError('Apply requires a completed aggregate inventory')
    mids = report['missing_positive_mids']
    if any(type(mid) is not int or mid <= 0 or mid > 9223372036854775807 for mid in mids):
        raise ValueError('Inventory contains an invalid positive member ID')
    return sorted(set(mids))


def attach_latest(session, mid, latest):
    # A concurrent natural aggregate may already have set a newer pointer.
    session.execute('UPDATE tdd_member SET last_total_stat = :latest '
                    'WHERE mid = :mid AND last_total_stat IS NULL',
                    {'latest': latest, 'mid': mid})
    session.commit()
    return session.execute('SELECT last_total_stat FROM tdd_member WHERE mid = :mid',
                           {'mid': mid}).scalar()


def backfill_members(mids, session, service, limit=100, offset=0,
                     max_seconds=300, delay=1.0, progress=None):
    from task import add_member, AlreadyExistError
    from service import CodeError, ContentError, RateLimitError, ServiceError

    started = time.monotonic()
    report = dict(schema_version=1, observed_utc=now(), apply=True,
                  results=[], requested_count=len(mids), offset=offset, complete=False)
    selected = mids[offset:offset + limit]
    consecutive_errors = 0

    def checkpoint(finished=False):
        report['next_offset'] = offset + len(report['results'])
        report['pending_mids'] = mids[report['next_offset']:]
        report['complete'] = not report['pending_mids']
        report['counts'] = dict(Counter(row['status'] for row in report['results']))
        report['unresolved_mids'] = [row['mid'] for row in report['results']
            if row['status'] not in ('created', 'already_exists', 'not_referenced')]
        report['unresolved_mids'].extend(report['pending_mids'])
        report['unchecked_prefix_count'] = offset
        report['all_resolved'] = offset == 0 and not report['unresolved_mids']
        report['elapsed_seconds'] = round(time.monotonic() - started, 3)
        report['finished_utc'] = now() if finished else None
        if progress is not None:
            progress(report)

    for mid in selected:
        if time.monotonic() - started >= max_seconds:
            report['stop_reason'] = 'time_budget'
            break
        result = {'mid': mid}
        api_attempted = False
        try:
            latest = session.execute(LATEST_SQL, {'mid': mid}).scalar()
            if latest is None:
                result['status'] = 'not_referenced'
            else:
                try:
                    api_attempted = True
                    add_member(mid, service, session)
                    result['status'] = 'created'
                    result['profile_created'] = True
                except AlreadyExistError:
                    api_attempted = False
                    result['status'] = 'already_exists'
                result['last_total_stat'] = attach_latest(session, mid, latest)
                if result['last_total_stat'] is None:
                    raise RuntimeError('Latest pointer was not attached')
            consecutive_errors = 0
        except CodeError as error:
            session.rollback()
            result.update(error_detail(error))
            result['status'] = 'unavailable' if error.code == -404 else 'api_error'
            consecutive_errors = 0
            if error.code != -404:
                report['stop_reason'] = 'api_error'
        except ContentError as error:
            session.rollback()
            result.update(error_detail(error), status='rejected_profile')
            consecutive_errors += 1
        except RateLimitError as error:
            session.rollback()
            result.update(error_detail(error), status='rate_limited')
            report['stop_reason'] = 'rate_limited'
        except ServiceError as error:
            session.rollback()
            result.update(error_detail(error), status='service_error')
            consecutive_errors += 1
        except Exception as error:
            session.rollback()
            result.update(error_detail(error), status='database_or_validation_error')
            report['stop_reason'] = 'database_or_validation_error'
        report['results'].append(result)
        if consecutive_errors >= 3:
            report['stop_reason'] = 'consecutive_service_errors'
        checkpoint()
        if 'stop_reason' in report:
            break
        if api_attempted and delay:
            time.sleep(delay)
    checkpoint(finished=True)
    return report


def positive_int(value):
    value = int(value)
    if value <= 0:
        raise argparse.ArgumentTypeError('Must be positive')
    return value


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true', help='Create real profiles from a completed inventory')
    parser.add_argument('--input', type=Path, help='Completed inventory JSON, required with --apply')
    parser.add_argument('--output', required=True, type=Path, help='Private JSON inventory or per-ID result report')
    parser.add_argument('--limit', type=positive_int, default=100)
    parser.add_argument('--offset', type=int, default=0)
    parser.add_argument('--max-seconds', type=positive_int, default=300)
    parser.add_argument('--page-size', type=positive_int, default=1000)
    parser.add_argument('--delay', type=float, default=1.0, help='Seconds between serial API attempts')
    args = parser.parse_args(argv)
    if args.offset < 0 or not math.isfinite(args.delay) or args.delay < 0 or args.page_size > 5000:
        parser.error('offset/delay must be nonnegative and page-size at most 5000')
    if args.apply and args.input is None:
        parser.error('--apply requires --input')
    if args.input is not None and args.output.resolve() == args.input.resolve():
        parser.error('--output must preserve the input inventory')
    if args.output.exists():
        parser.error('--output already exists; preserve the previous report')

    mids = inventory_mids(json.loads(args.input.read_text())) if args.apply else None
    if mids is not None and args.offset > len(mids):
        parser.error('--offset is beyond the inventory')
    args.output.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(args.output), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w') as output:
        output.write(json.dumps({'started_utc': now(), 'apply': args.apply})+'\n')

    def save_report(report):
        temporary = args.output.with_name(args.output.name+'.'+uuid.uuid4().hex+'.tmp')
        fd = os.open(str(temporary), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'w') as output:
            output.write(json.dumps(report, ensure_ascii=False, indent=2)+'\n')
        os.replace(temporary, args.output)

    if args.apply:
        from db import Session
        from service import Service
        session = Session()
        try:
            report = backfill_members(mids, session, Service(mode='worker', retry=3, pool_maxsize=1),
                                      args.limit, args.offset, args.max_seconds, args.delay,
                                      progress=save_report)
        finally:
            session.close()
    else:
        from db import engine
        connection = engine.raw_connection()
        try:
            report = audit_missing_members(connection, args.page_size, args.max_seconds)
        finally:
            connection.close()
    save_report(report)
    print(json.dumps({key: report[key] for key in ('complete', 'counts', 'missing_positive_count',
                      'missing_nonpositive_count', 'stop_reason', 'next_offset') if key in report}))
    return 1 if report.get('error') or report.get('stop_reason') or report.get('unresolved_mids') else 0


if __name__ == '__main__':
    raise SystemExit(main())
