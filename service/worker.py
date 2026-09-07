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
    weight: int
    enabled: bool


class WorkerSelector:
    """Process-local worker selection.

    Stateless beyond the parsed config. An earlier version cooled a worker down
    on a rate limit so another could take over, which only helps if the limit is
    per worker; it is on request rate, so they cross it together.
    """

    def __init__(self, endpoints: Mapping[str, dict]):
        self._workers: dict[str, tuple[WorkerEndpoint, ...]] = {}

        for target, endpoint_config in endpoints.items():
            raw_workers = endpoint_config.get('workers', [])
            workers = tuple(self._parse_worker(target, raw)
                            for raw in raw_workers)
            worker_ids = [worker.id for worker in workers]
            if len(worker_ids) != len(set(worker_ids)):
                raise WorkerConfigurationError(
                    f'Duplicate worker id for {target!r}.')
            weighted = self._weighted(workers)
            if not weighted:
                raise WorkerConfigurationError(
                    f'Endpoint {target!r} has no enabled worker configured.')
            self._workers[target] = weighted

    @staticmethod
    def _parse_worker(target: str, raw) -> WorkerEndpoint:
        if not isinstance(raw, dict):
            raise WorkerConfigurationError(
                f'Worker entry for {target!r} must be an object.')

        fields = {'id', 'url', 'platform', 'weight', 'enabled'}
        missing = fields - set(raw)
        if missing:
            raise WorkerConfigurationError(
                f'Missing worker field(s) for {target!r}: {sorted(missing)!r}.')
        unknown = set(raw) - fields
        if unknown:
            raise WorkerConfigurationError(
                f'Unknown worker field(s) for {target!r}: {sorted(unknown)!r}.')

        worker_id = raw['id']
        url = raw['url']
        platform = raw['platform']
        weight = raw['weight']
        enabled = raw['enabled']
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
