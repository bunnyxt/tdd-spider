from dataclasses import dataclass
import logging
import random
from threading import Lock
import time
from typing import Callable, Iterable, Mapping

from .error import RateLimitError


logger = logging.getLogger('Service')

__all__ = ['WorkerConfigurationError', 'WorkerEndpoint', 'WorkerSelector']


class WorkerConfigurationError(ValueError):
    pass


@dataclass(frozen=True)
class WorkerEndpoint:
    id: str
    url: str
    platform: str
    weight: int = 1
    enabled: bool = True


@dataclass(frozen=True)
class RateLimitState:
    first_seen: float
    retry_at: float


class WorkerSelector:
    """Process-local worker selection and rate-limit cooldown tracking."""

    def __init__(self, endpoints: Mapping[str, dict], *,
                 clock: Callable[[], float] = time.monotonic):
        self._clock = clock
        self._lock = Lock()
        self._workers: dict[str, tuple[WorkerEndpoint, ...]] = {}
        self._rate_limits: dict[tuple[str, str], RateLimitState] = {}

        for target, endpoint_config in endpoints.items():
            raw_workers = endpoint_config.get('workers', [])
            workers = tuple(self._parse_worker(target, index, raw)
                            for index, raw in enumerate(raw_workers))
            worker_ids = [worker.id for worker in workers]
            if len(worker_ids) != len(set(worker_ids)):
                raise WorkerConfigurationError(
                    f'Duplicate worker id for {target!r}.')
            self._workers[target] = self._weighted(workers)

    @staticmethod
    def _parse_worker(target: str, index: int, raw) -> WorkerEndpoint:
        if isinstance(raw, str):
            if not raw:
                raise WorkerConfigurationError(
                    f'Worker URL for {target!r} must not be empty.')
            return WorkerEndpoint(
                id=f'{target}:legacy:{index}', url=raw, platform='unknown')

        if not isinstance(raw, dict):
            raise WorkerConfigurationError(
                f'Worker entry for {target!r} must be a URL or object.')

        unknown = set(raw) - {'id', 'url', 'platform', 'weight', 'enabled'}
        if unknown:
            raise WorkerConfigurationError(
                f'Unknown worker field(s) for {target!r}: {sorted(unknown)!r}.')

        worker_id = raw.get('id')
        url = raw.get('url')
        platform = raw.get('platform')
        weight = raw.get('weight', 1)
        enabled = raw.get('enabled', True)
        if not isinstance(worker_id, str) or not worker_id:
            raise WorkerConfigurationError(
                f'Worker id for {target!r} must be a non-empty string.')
        if not isinstance(url, str) or not url:
            raise WorkerConfigurationError(
                f'Worker URL for {worker_id!r} must be a non-empty string.')
        if not isinstance(platform, str) or not platform:
            raise WorkerConfigurationError(
                f'Worker platform for {worker_id!r} must be a non-empty string.')
        if isinstance(weight, bool) or not isinstance(weight, int) or weight <= 0:
            raise WorkerConfigurationError(
                f'Worker weight for {worker_id!r} must be a positive integer.')
        if not isinstance(enabled, bool):
            raise WorkerConfigurationError(
                f'Worker enabled for {worker_id!r} must be a boolean.')
        return WorkerEndpoint(worker_id, url, platform, weight, enabled)

    def select(self, target: str) -> WorkerEndpoint:
        workers = self._enabled_workers(target)
        now = self._clock()
        recovered: list[WorkerEndpoint] = []

        with self._lock:
            for worker in workers:
                key = (target, worker.id)
                state = self._rate_limits.get(key)
                if state is not None and state.retry_at <= now:
                    del self._rate_limits[key]
                    recovered.append(worker)
            available = tuple(
                worker for worker in workers
                if (target, worker.id) not in self._rate_limits)
            window = self._rate_limit_window(target, workers, now)

        for worker in recovered:
            logger.info('worker_rate_limit_cleared target=%s worker_id=%s platform=%s',
                        target, worker.id, worker.platform)
        if not available:
            self._raise_rate_limited(target, 'all_workers_rate_limited', window)
        return random.choice(available)

    def mark_rate_limited(self, target: str, worker: WorkerEndpoint, *,
                          reason: str, cooldown_s: int) -> None:
        workers = self._enabled_workers(target)
        now = self._clock()
        retry_at = now + cooldown_s
        key = (target, worker.id)

        with self._lock:
            previous = self._rate_limits.get(key)
            transitioned = previous is None or previous.retry_at <= now
            first_seen = now if transitioned else previous.first_seen
            self._rate_limits[key] = RateLimitState(
                first_seen=first_seen,
                retry_at=max(previous.retry_at if previous else retry_at,
                             retry_at))
            exhausted = all(
                (state := self._rate_limits.get((target, candidate.id))) is not None
                and state.retry_at > now for candidate in workers)
            window = self._rate_limit_window(target, workers, now)

        if transitioned:
            logger.warning(
                'worker_rate_limited target=%s worker_id=%s platform=%s reason=%s cooldown_s=%s',
                target, worker.id, worker.platform, reason, cooldown_s)
        if exhausted:
            self._raise_rate_limited(target, reason, window)

    def rate_limit_window(self, target: str) -> tuple[float | None, float | None]:
        workers = self._enabled_workers(target)
        with self._lock:
            return self._rate_limit_window(target, workers, self._clock())

    def _enabled_workers(self, target: str) -> tuple[WorkerEndpoint, ...]:
        workers = self._workers.get(target, ())
        if not workers:
            raise WorkerConfigurationError(
                f'Endpoint {target!r} has no enabled worker configured.')
        return workers

    def _rate_limit_window(
            self, target: str, workers: Iterable[WorkerEndpoint], now: float
    ) -> tuple[float | None, float | None]:
        states = [self._rate_limits[(target, worker.id)]
                  for worker in workers
                  if (target, worker.id) in self._rate_limits
                  and self._rate_limits[(target, worker.id)].retry_at > now]
        return (min((state.first_seen for state in states), default=None),
                min((state.retry_at for state in states), default=None))

    def _raise_rate_limited(
            self, target: str, reason: str,
            window: tuple[float | None, float | None]) -> None:
        first_seen, retry_at = window
        now = self._clock()
        logger.error(
            'worker_pool_rate_limited target=%s limited_for_s=%s earliest_retry_in_s=%s',
            target,
            None if first_seen is None else max(0, int(now - first_seen)),
            None if retry_at is None else max(0, int(retry_at - now)))
        raise RateLimitError(target, reason, first_seen, retry_at)

    @staticmethod
    def _weighted(workers: Iterable[WorkerEndpoint]) -> tuple[WorkerEndpoint, ...]:
        return tuple(worker for worker in workers if worker.enabled
                     for _ in range(worker.weight))
