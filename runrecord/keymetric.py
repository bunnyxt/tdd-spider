"""
The key-metric convention (Phase 1.5).

A *key metric* is generic display metadata attached to a metric itself: it means
only "reasonable to surface by default in a cross-script overview". It does not
express health, a good/bad direction, a threshold or an alert, it never changes
a run's persisted status, and there is deliberately no per-script key-metric
config table anywhere.

``is_key_metric(name, is_key)`` resolves the effective flag:

* an explicit, non-``None`` ``is_key`` (persisted in ``run_metric.is_key`` as
  1 or 0 by a recorder that knows better) wins as-is -- including an explicit
  ``0`` that suppresses the convention for an otherwise key-looking name;
* otherwise a name-based convention applies, so rows written before the v2
  column existed (all ``is_key IS NULL``) still get sensible defaults:
  - ``total_count`` -- the JobStat row count -- is key;
  - any name containing ``exception`` / ``error`` / ``fail`` / ``dropped``
    (case-insensitive substring) is key;
  - everything else is not key.
"""

__all__ = ['KEY_NAME_MARKERS', 'is_key_metric']

# error-like counters worth surfacing by default; matched as a case-insensitive
# substring so `other_exception`, `code_error`, `batch_insert_fail` and
# `record_dropped_queue_full` are all covered.
KEY_NAME_MARKERS = ('exception', 'error', 'fail', 'dropped')


def is_key_metric(name, is_key=None):
    """Return whether a metric is a key metric (see the module docstring)."""
    if is_key is not None:
        return bool(is_key)
    lowered = (name or '').lower()
    if lowered == 'total_count':
        return True
    return any(marker in lowered for marker in KEY_NAME_MARKERS)
