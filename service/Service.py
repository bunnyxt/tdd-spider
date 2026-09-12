import requests
from requests.adapters import HTTPAdapter
from http.cookiejar import DefaultCookiePolicy
from dataclasses import dataclass
import json
import time
import random
from pathlib import Path
from typing import Optional, Callable, Literal, Union
from .error import (ResponseError, RateLimitError,
                    FormatError, CodeError)
from .worker import WorkerConfigurationError, WorkerSelector
from .apistat import ApiStatTracker, NullApiStatTracker
from .ua import UA_LIST
from .endpoints import (get_member_card, get_member_relation, get_video_tags,
                        get_video_view, get_video_view_trimmed, get_newlist)
from .response import \
    VideoView, VideoViewTrimmed, VideoTags, \
    MemberCard, \
    MemberRelation, \
    Newlist
from util import a2b
import logging

logger = logging.getLogger('Service')

RequestMode = Literal['direct', 'worker']

__all__ = ['Service', 'RequestMode',
           'RateLimit', 'default_rate_limit', 'member_card_rate_limit']

@dataclass(frozen=True)
class RateLimit:
    reason: str


@dataclass
class FetchResponse:
    response: Optional[requests.Response]
    failure: Optional[str]
    duration_ms: int
    content_length: Optional[str] = None
    deadline_s: Optional[float] = None
    error: Optional[Exception] = None
    body: Optional[bytes] = None


def default_rate_limit(response: requests.Response) -> Optional[RateLimit]:
    if response.status_code == 412:
        return RateLimit('http_412')
    return None


def member_card_rate_limit(response: requests.Response) -> Optional[RateLimit]:
    limited = default_rate_limit(response)
    if limited is not None:
        return limited
    if response.status_code != 200:
        return None
    try:
        body = response.json()
    except (json.JSONDecodeError, requests.exceptions.JSONDecodeError):
        return None
    if isinstance(body, dict) and body.get('code') == -352:
        return RateLimit('code_-352')
    return None


RATE_LIMIT_CHECKERS: dict[str, Callable[[requests.Response], Optional[RateLimit]]] = {
    'get_member_card': member_card_rate_limit,
}


class Service:

    def __init__(
            self, headers: Optional[dict] = None, retry: int = 3, timeout: float = 5.0, colddown_factor: float = 1.0,
            mode: RequestMode = 'direct', pool_maxsize: int = 256, deadline: float = 10.0,
            min_throughput_bps: float = 80_000.0, endpoints: Optional[dict] = None,
            stats: Optional[Union[ApiStatTracker, bool]] = None
    ):
        if mode not in ('direct', 'worker'):
            logger.critical(f'Invalid request mode: {mode}.')
            raise SystemExit(1)

        # one tracker per Service by default, matching the "one Service shared
        # by all worker threads" lifetime. Pass an existing tracker to share
        # counters across Services, or stats=False to disable.
        if stats is False:
            self.stats = NullApiStatTracker()
        elif stats is None:
            self.stats = ApiStatTracker()
        else:
            self.stats = stats

        # set default config
        self._headers = headers if headers is not None else {}
        self._retry = retry
        self._timeout = timeout
        self._colddown_factor = colddown_factor
        self._mode = mode
        # total wall-clock budget per trial, see _get() for why this differs
        # from timeout
        self._deadline = deadline
        # some responses (season/multi-part videos) run 200KB-2.8MB against a
        # typical few-KB payload; production measured p10 throughput of 94.5
        # KB/s on those, so a flat deadline kills healthy large transfers
        # (measured: 1974/1975 deadline hits were >200KB, not stalls). Deadline
        # scales with Content-Length at this floor, see _get()
        self._min_throughput_bps = min_throughput_bps

        # pooled session for HTTP keep-alive: reuse TCP+TLS connections across
        # requests instead of a fresh handshake per call (big win when many
        # workers hammer a single endpoint). One Service is shared by all worker
        # threads; urllib3's connection pool is thread-safe. pool_maxsize must
        # cover the concurrent worker count hitting one host, or overflow
        # connections get opened-then-discarded (no keep-alive benefit).
        self._session = requests.Session()
        # these API calls are stateless (no cookies needed). Reject all cookies
        # so responses never write the shared cookie jar -- that concurrent
        # write is the one real thread-safety hazard of sharing a Session across
        # worker threads; without it, the connection pool is thread-safe.
        self._session.cookies.set_policy(DefaultCookiePolicy(allowed_domains=[]))
        adapter = HTTPAdapter(pool_connections=32, pool_maxsize=pool_maxsize)
        self._session.mount('http://', adapter)
        self._session.mount('https://', adapter)

        # load endpoints (injectable for tests; production always loads the
        # git-ignored endpoints.json next to this file)
        if endpoints is not None:
            self.endpoints = endpoints
        else:
            try:
                with Path(__file__).with_name('endpoints.json').open('r') as f:
                    self.endpoints = json.load(f)
            except FileNotFoundError:
                logger.critical("The file 'endpoints.json' was not found.")
                raise SystemExit(1)
            except json.JSONDecodeError:
                logger.critical('Invalid JSON format in endpoints.json.')
                raise SystemExit(1)
            except Exception as e:
                logger.critical(
                    f'An unexpected error occurred when load and parse endpoints.json file. {e}')
                raise SystemExit(1)

        self._worker_selector = None
        if self._mode == 'worker':
            try:
                self._worker_selector = WorkerSelector(self.endpoints)
            except WorkerConfigurationError as e:
                logger.critical(f'Invalid worker configuration: {e}')
                raise SystemExit(1)


    # since default configs are designed to be immutable, we should use following getters

    def get_default_headers(self) -> dict:
        return self._headers

    def get_default_retry(self) -> int:
        return self._retry

    def get_default_timeout(self) -> float:
        return self._timeout

    def get_default_colddown_factor(self) -> float:
        return self._colddown_factor

    def get_default_mode(self) -> RequestMode:
        return self._mode

    def get_default_deadline(self) -> float:
        return self._deadline

    # default config getters end

    def _get(
            self, endpoint: str,
            params: Optional[dict] = None, headers: Optional[dict] = None,
            retry: Optional[int] = None, timeout: Optional[float] = None, colddown_factor: Optional[float] = None,
            deadline: Optional[float] = None,
            parser: Optional[Callable[[str], Optional[dict]]] = None
    ) -> dict:
        # assemble headers
        # copy, don't alias: the User-Agent set below would otherwise be
        # written into self._headers and reused by every later request
        if headers is None:
            headers = dict(self._headers)
        else:
            headers = {**self._headers, **headers}
        ua_given_by_caller = 'User-Agent' in headers
        ua_pool = self.endpoints.get(endpoint, {}).get('user_agents') or UA_LIST

        # config
        retry = retry if retry is not None else self._retry
        timeout = timeout if timeout is not None else self._timeout
        colddown_factor = colddown_factor if colddown_factor is not None else self._colddown_factor
        deadline = deadline if deadline is not None else self._deadline
        rate_limit_checker = RATE_LIMIT_CHECKERS.get(endpoint, default_rate_limit)

        if self._mode == 'direct':
            try:
                direct_url = self.endpoints[endpoint]['direct']
            except KeyError:
                logger.critical(f'Endpoint "{endpoint}" not found.')
                raise SystemExit(1)
        elif self._mode == 'worker':
            direct_url = None
        else:
            logger.critical(f'Invalid request mode: {self._mode}.')
            raise SystemExit(1)

        # go request
        response = None
        last_failure = None
        rate_limited_trials = 0

        for trial in range(1, retry + 1):
            if not ua_given_by_caller:
                # redrawn every attempt; not guaranteed to differ from the last
                headers['User-Agent'] = random.choice(ua_pool)
            selected_worker = None
            request_url = direct_url
            if self._mode == 'worker':
                try:
                    selected_worker = self._worker_selector.select(endpoint)
                except WorkerConfigurationError as e:
                    logger.critical(f'Invalid worker configuration: {e}')
                    raise SystemExit(1)
                request_url = selected_worker.url
            worker_id = selected_worker.id if selected_worker else 'direct'

            # colddown for retry
            if trial > 1:
                # fluctuation range 0.75 ~ 1.25
                time.sleep((trial - 1) * (random.random()
                           * 0.5 + 0.75) * colddown_factor)

            fetched = self._fetch_response(
                request_url, params, headers, timeout, deadline)
            if fetched.failure is not None:
                parsed_response, outcome, limited = None, fetched.failure, False
            else:
                parsed_response, outcome, limited = self._classify_response(
                    fetched.response, parser, rate_limit_checker)
            self._record_trial(endpoint, worker_id, trial, outcome, fetched,
                               params, request_url, headers.get('User-Agent'))
            if outcome == 'ok':
                response = parsed_response
                break
            last_failure = outcome
            if limited:
                rate_limited_trials += 1
        if response is None:
            if retry > 0 and rate_limited_trials == retry:
                raise RateLimitError(endpoint, last_failure)
            raise ResponseError(endpoint, params or {}, last_failure, retry)
        return response

    def _fetch_response(self, request_url, params, headers, timeout, deadline) -> FetchResponse:
        trial_start = time.perf_counter()
        try:
            response = self._session.get(request_url, params=params, headers=headers,
                                         timeout=timeout, stream=True)
        except requests.exceptions.RequestException as error:
            return FetchResponse(None, 'request_exception',
                                 int((time.perf_counter() - trial_start) * 1000), error=error)
        content_length = response.headers.get('Content-Length')
        trial_deadline = deadline
        if content_length is not None:
            try:
                trial_deadline = max(deadline, int(content_length) / self._min_throughput_bps)
            except ValueError:
                pass
        body = bytearray()
        try:
            for chunk in response.iter_content(chunk_size=65536):
                body += chunk
                if time.perf_counter() - trial_start > trial_deadline:
                    response.close()
                    return FetchResponse(response, 'deadline_exceeded',
                                         int((time.perf_counter() - trial_start) * 1000),
                                         content_length, trial_deadline, body=bytes(body))
        except requests.exceptions.RequestException as error:
            response.close()
            return FetchResponse(response, 'body_exception',
                                 int((time.perf_counter() - trial_start) * 1000),
                                 content_length, trial_deadline, error, bytes(body))
        body_bytes = bytes(body)
        response._content = body_bytes
        response._content_consumed = True
        return FetchResponse(response, None, int((time.perf_counter() - trial_start) * 1000),
                             content_length, trial_deadline, body=body_bytes)

    def _classify_response(self, response, parser, rate_limit_checker):
        limited = rate_limit_checker(response)
        if limited is not None:
            return None, limited.reason, True
        if response.status_code != 200:
            return None, f'http_{response.status_code}', False
        if parser is None:
            try:
                return response.json(), 'ok', False
            except json.JSONDecodeError:
                return None, 'json_error', False
        parsed_response = parser(response.text)
        return parsed_response, 'ok' if parsed_response is not None else 'parse_error', False

    def _record_trial(self, endpoint, worker, trial, outcome, fetched,
                      params, request_url, user_agent) -> None:
        self.stats.record(endpoint, worker, trial, outcome)
        status = fetched.response.status_code if fetched.response is not None else None
        response_body = (fetched.body.decode('utf-8', errors='replace')
                         if fetched.body is not None else None)
        logger.debug(
            f'API endpoint={endpoint} params={params!r} url={request_url!r} '
            f'user_agent={user_agent!r} worker={worker} trial={trial} outcome={outcome} '
            f'status={status} duration_ms={fetched.duration_ms} '
            f'content_length={fetched.content_length} deadline_s={fetched.deadline_s} '
            f'error={fetched.error!r} response_body={response_body!r}')

    def get_video_view(
            self, params: Optional[dict] = None, headers: Optional[dict] = None,
            retry: Optional[int] = None, timeout: Optional[float] = None,
            colddown_factor: Optional[float] = None
    ) -> VideoView:
        return get_video_view.get(
            self._get, params=params, headers=headers, retry=retry,
            timeout=timeout, colddown_factor=colddown_factor)

    def get_video_view_trimmed(
            self, params: Optional[dict] = None, headers: Optional[dict] = None,
            retry: Optional[int] = None, timeout: Optional[float] = None,
            colddown_factor: Optional[float] = None
    ) -> VideoViewTrimmed:
        """
        params: { aid: int }
        Stat-only variant of get_video_view for record jobs: hits the trimmed
        video_view worker (service/workers/video_view/), whose ~250B response
        avoids shipping the 200KB-2.8MB season/UGC bloat of the full view
        payload. Worker-only: no bilibili API serves the trimmed contract
        (endpoints.json marks direct as invalid://...); callers needing direct
        mode should use get_video_view, whose response is a superset. The full
        view response would parse here too, which is what makes the full-view
        worker URL a valid drop-in workers entry before the trimmed Lambda is
        deployed.
        """
        if self._mode != 'worker':
            logger.critical(f'Endpoint "get_video_view_trimmed" is worker-only '
                            f'(no direct API serves the trimmed contract), got mode: {self._mode}. '
                            f'Use get_video_view for direct mode.')
            raise SystemExit(1)

        return get_video_view_trimmed.get(
            self._get, params=params, headers=headers, retry=retry,
            timeout=timeout, colddown_factor=colddown_factor)

    def get_video_tags(
            self, params: Optional[dict] = None, headers: Optional[dict] = None,
            retry: Optional[int] = None, timeout: Optional[float] = None,
            colddown_factor: Optional[float] = None
    ) -> VideoTags:
        return get_video_tags.get(
            self._get, params=params, headers=headers, retry=retry,
            timeout=timeout, colddown_factor=colddown_factor)

    def get_member_card(
            self, params: Optional[dict] = None, headers: Optional[dict] = None,
            retry: Optional[int] = None, timeout: Optional[float] = None,
            colddown_factor: Optional[float] = None
    ) -> MemberCard:
        return get_member_card.get(
            self._get, params=params, headers=headers, retry=retry,
            timeout=timeout, colddown_factor=colddown_factor)

    def get_member_relation(
            self, params: Optional[dict] = None, headers: Optional[dict] = None,
            retry: Optional[int] = None, timeout: Optional[float] = None,
            colddown_factor: Optional[float] = None
    ) -> MemberRelation:
        return get_member_relation.get(
            self._get, params=params, headers=headers, retry=retry,
            timeout=timeout, colddown_factor=colddown_factor)

    def get_newlist(
            self, params: Optional[dict] = None, headers: Optional[dict] = None,
            retry: Optional[int] = None, timeout: Optional[float] = None,
            colddown_factor: Optional[float] = None
    ) -> Newlist:
        return get_newlist.get(
            self._get, params=params, headers=headers, retry=retry,
            timeout=timeout, colddown_factor=colddown_factor)
