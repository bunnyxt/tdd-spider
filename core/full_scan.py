"""
Daily full-scan snapshots and the weekly-growth activity rule derived from them.

The 04:00 run of 51 fetches every in-scope video once. Its records are kept as
one gzip CSV per day under ``SNAPSHOT_DIR`` (``YYYY-MM-DD.csv.gz``), separate
from the hourly ``data/<date> <hh>:00.csv`` files: the 23:00 packing step, its
3-day csv removal and the manual clean-up scripts all glob top-level,
date-prefixed names in ``data/`` and never descend into the sub-directory.

Activity compares this full scan with the one taken 7 days earlier. Only videos
present with a valid view count in *both* scans are classified; a video missing
on either side keeps whatever activity it already has. That makes a partial
scan (e.g. one cut short by rate limiting) safe: the videos it did reach are
updated, the rest are left alone.

Pure functions only -- no DB access -- so the rule is unit-testable.
"""

import csv
import datetime
import gzip
import os
import re
from typing import Dict, Iterable, List, NamedTuple, Optional

__all__ = ['SNAPSHOT_DIR', 'SNAPSHOT_RETENTION_DAYS', 'ACTIVE_THRESHOLD', 'HOT_THRESHOLD',
           'LOW_PAIR_RATIO', 'ActivityResult', 'snapshot_path', 'write_snapshot',
           'prune_snapshots', 'load_snapshot_views', 'record_views', 'compute_activity']

SNAPSHOT_DIR = 'data/0400'
SNAPSHOT_RETENTION_DAYS = 30

# weekly view growth thresholds: >= HOT -> 2 (fetched hourly),
# >= ACTIVE -> 1 (fetched every 4 hours), otherwise 0 (daily full scan only)
ACTIVE_THRESHOLD = 1000
HOT_THRESHOLD = 5000

# below this share of this scan's videos paired with last week's scan, the
# update still runs for the paired ones but is flagged as suspicious
LOW_PAIR_RATIO = 0.9

_SNAPSHOT_NAME = re.compile(r'^(\d{4}-\d{2}-\d{2})\.csv\.gz$')
_HEADER = ['added', 'aid', 'bvid', 'view', 'danmaku', 'reply', 'favorite', 'coin', 'share', 'like']


class ActivityResult(NamedTuple):
    activity: Dict[int, int]  # aid -> 0 / 1 / 2, paired videos only
    this_count: int           # videos with a valid view in this scan
    last_count: int           # videos with a valid view in last week's scan
    paired: int
    hot: int
    active: int

    @property
    def pair_ratio(self) -> float:
        return self.paired / self.this_count if self.this_count else 0.0


def snapshot_path(folder: str, date: datetime.date) -> str:
    return os.path.join(folder, f'{date.isoformat()}.csv.gz')


def write_snapshot(records: Iterable, folder: str, date: datetime.date) -> str:
    """
    Write records (RecordNew-like) to the snapshot for `date`, atomically: a
    crash mid-write leaves a stray ``.tmp``, never a truncated snapshot that a
    later run would read as a complete scan.
    """
    os.makedirs(folder, exist_ok=True)
    path = snapshot_path(folder, date)
    tmp_path = path + '.tmp'
    with gzip.open(tmp_path, 'wt', newline='', compresslevel=6) as f:
        writer = csv.writer(f)
        writer.writerow(_HEADER)
        for r in records:
            writer.writerow([r.added, r.aid, r.bvid, r.view, r.danmaku, r.reply,
                             r.favorite, r.coin, r.share, r.like])
    os.replace(tmp_path, path)
    return path


def prune_snapshots(folder: str, today: datetime.date,
                    retention_days: int = SNAPSHOT_RETENTION_DAYS) -> List[str]:
    """
    Remove snapshots dated more than `retention_days` before `today`. Only
    files named exactly ``YYYY-MM-DD.csv.gz`` are considered; anything else in
    the folder is left untouched. Returns the removed paths.
    """
    if not os.path.isdir(folder):
        return []
    cutoff = today - datetime.timedelta(days=retention_days)
    removed = []
    for name in sorted(os.listdir(folder)):
        match = _SNAPSHOT_NAME.match(name)
        if match is None:
            continue
        try:
            date = datetime.date.fromisoformat(match.group(1))
        except ValueError:
            continue
        if date < cutoff:
            path = os.path.join(folder, name)
            os.remove(path)
            removed.append(path)
    return removed


def _keep(views: Dict[int, int], aid: int, view: int, prefer_max: bool) -> None:
    # a view of -1 stands for '--' from the API: treat it as missing
    if view < 0:
        return
    old = views.get(aid)
    if old is None or (view > old if prefer_max else view < old):
        views[aid] = view


def record_views(records: Iterable) -> Dict[int, int]:
    """aid -> view of this scan's records (max view if an aid repeats)."""
    views: Dict[int, int] = {}
    for r in records:
        _keep(views, r.aid, r.view, prefer_max=True)
    return views


def load_snapshot_views(path: str) -> Optional[Dict[int, int]]:
    """
    aid -> view of a snapshot (min view if an aid repeats, so a duplicate can
    only widen the weekly growth, matching the max taken on this side).
    Returns None when the snapshot does not exist.
    """
    if not os.path.isfile(path):
        return None
    views: Dict[int, int] = {}
    with gzip.open(path, 'rt', newline='') as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if header is None:
            return views
        aid_idx, view_idx = header.index('aid'), header.index('view')
        for row in reader:
            _keep(views, int(row[aid_idx]), int(row[view_idx]), prefer_max=False)
    return views


def compute_activity(this_views: Dict[int, int], last_views: Dict[int, int],
                     active_threshold: int = ACTIVE_THRESHOLD,
                     hot_threshold: int = HOT_THRESHOLD) -> ActivityResult:
    activity: Dict[int, int] = {}
    hot = active = 0
    for aid, view in this_views.items():
        last = last_views.get(aid)
        if last is None:
            continue
        growth = view - last
        if growth >= hot_threshold:
            activity[aid] = 2
            hot += 1
        elif growth >= active_threshold:
            activity[aid] = 1
            active += 1
        else:
            activity[aid] = 0
    return ActivityResult(activity=activity, this_count=len(this_views),
                          last_count=len(last_views), paired=len(activity),
                          hot=hot, active=active)
