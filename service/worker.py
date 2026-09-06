from dataclasses import dataclass
import logging
import random
from typing import Iterable, Mapping


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


class WorkerSelector:
    """
    Process-local worker selection.

    Deliberately stateless beyond the parsed config. It used to cool a worker
    down on a rate limit so another could take over, which assumed the limit is
    per-worker. Measurement says otherwise: the limit is on request *rate* --
    roughly 24-30 req/s across the member-card fleet -- so every worker crosses
    it at the same moment and there is never one with headroom to take over.
    Five of the six targets run a single worker anyway. Staying under the rate
    is the fix; taking workers out of the pool never was.
    """

    def __init__(self, endpoints: Mapping[str, dict]):
        self._workers: dict[str, tuple[WorkerEndpoint, ...]] = {}

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
        # `_weighted` repeats each worker `weight` times, so a uniform choice
        # here gives the configured weight ratio.
        return random.choice(self._enabled_workers(target))

    def _enabled_workers(self, target: str) -> tuple[WorkerEndpoint, ...]:
        workers = self._workers.get(target, ())
        if not workers:
            raise WorkerConfigurationError(
                f'Endpoint {target!r} has no enabled worker configured.')
        return workers

    @staticmethod
    def _weighted(workers: Iterable[WorkerEndpoint]) -> tuple[WorkerEndpoint, ...]:
        return tuple(worker for worker in workers if worker.enabled
                     for _ in range(worker.weight))
