import requests
from requests.adapters import HTTPAdapter
from http.cookiejar import DefaultCookiePolicy
from dataclasses import dataclass
import json
import time
import random
from pathlib import Path
from typing import Optional, Callable, Literal
from .error import (ResponseError, RateLimitError,
                    FormatError, CodeError, MisalignmentError)
from .worker import WorkerConfigurationError, WorkerSelector
from .response import \
    VideoViewOwner, VideoViewStat, VideoViewStaffItem, VideoView, VideoViewTrimmed, \
    VideoViewTrimmedBatchItem, \
    VideoTag, VideoTags, \
    MemberCard, \
    MemberRelation, \
    NewlistPage, NewlistArchiveStat, NewlistArchiveOwner, NewlistArchive, Newlist
from util import a2b
import logging

logger = logging.getLogger('Service')

RequestMode = Literal['direct', 'worker']

__all__ = ['Service', 'RequestMode',
           'RateLimit', 'default_rate_limit', 'member_card_rate_limit',
           'VIDEO_VIEW_TRIMMED_BATCH_PROTOCOL_VERSION',
           'VIDEO_VIEW_TRIMMED_BATCH_READ_TIMEOUT_S',
           'VIDEO_VIEW_TRIMMED_BATCH_DEADLINE_S']

# --- trimmed video_view batch path tunables ---------------------------------
# INITIAL HYPOTHESES pending load-test calibration against the deployed batch
# worker -- do not treat as approved final values. The timeout-chain invariant
# that MUST hold whatever the calibrated numbers become:
#
#   client deadline (12s) > client read timeout (8s)
#     > batch fn timeout (6s, Lambda side) > per-item timeout (4.5s, worker)
#
# i.e. each inner layer gives up before the outer one abandons it, with >= 2s
# of headroom per step -- the single-path incident where client timeout ==
# function timeout (5s == 5s) turned worker-side timeouts into opaque
# client-side ones, and this chain is how that stays fixed.
VIDEO_VIEW_TRIMMED_BATCH_PROTOCOL_VERSION = 1
VIDEO_VIEW_TRIMMED_BATCH_READ_TIMEOUT_S = 8.0
VIDEO_VIEW_TRIMMED_BATCH_DEADLINE_S = 12.0

WORKER_HTTP_412_COOLDOWN_S = 30 * 60
MEMBER_CARD_352_COOLDOWN_S = 5 * 60


@dataclass(frozen=True)
class RateLimit:
    reason: str
    cooldown_s: int


def default_rate_limit(response: requests.Response) -> Optional[RateLimit]:
    if response.status_code == 412:
        return RateLimit('http_412', WORKER_HTTP_412_COOLDOWN_S)
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
        return RateLimit('code_-352', MEMBER_CARD_352_COOLDOWN_S)
    return None


RATE_LIMIT_CHECKERS: dict[str, Callable[[requests.Response], Optional[RateLimit]]] = {
    'get_member_card': member_card_rate_limit,
}


class Service:

    def __init__(
            self, headers: Optional[dict] = None, retry: int = 3, timeout: float = 5.0, colddown_factor: float = 1.0,
            mode: RequestMode = 'direct', pool_maxsize: int = 256, deadline: float = 10.0,
            min_throughput_bps: float = 80_000.0, endpoints: Optional[dict] = None
    ):
        if mode not in ('direct', 'worker'):
            logger.critical(f'Invalid request mode: {mode}.')
            raise SystemExit(1)

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
                exit(1)
            except json.JSONDecodeError:
                logger.critical('Invalid JSON format in endpoints.json.')
                exit(1)
            except Exception as e:
                logger.critical(
                    f'An unexpected error occurred when load and parse endpoints.json file. {e}')
                exit(1)

        try:
            self._worker_selector = WorkerSelector(self.endpoints)
        except WorkerConfigurationError as e:
            logger.critical(f'Invalid worker configuration: {e}')
            raise SystemExit(1)

        # define User Agent list
        self._ua_list = [
            # PC Browser
            # Google,win
            r'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36',
            # Google,mac
            r'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36',
            # Google,linux
            r'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36',
            # Opera,win
            r'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.87 Safari/537.36 OPR/37.0.2178.31',
            # Opera,mac
            r'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.87 Safari/537.36 OPR/37.0.2178.31',
            # Firefox,win
            r'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:46.0) Gecko/20100101 Firefox/46.0',
            # Firefox,mac
            r'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.11; rv:46.0) Gecko/20100101 Firefox/46.0',
            # Safari,mac
            r'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/7046A194A',
            # 360 browser
            r'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; 360SE)',
            # Sogou browser
            r'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SE 2.X MetaSr 1.0; SE 2.X MetaSr 1.0; .NET CLR 2.0.50727; SE 2.X MetaSr 1.0)',
            # UC browser
            r'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 UBrowser/6.2.4094.1 Safari/537.36',
            # Internet Explorer 8
            r'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0)',
            # Internet Explorer 9
            r'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)',

            # Mobile Browser
            # Android QQ browser For android
            r'MQQBrowser/26 Mozilla/5.0 (Linux; U; Android 2.3.7; zh-cn; MB200 Build/GRJ22; CyanogenMod-7) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1',
            # Android Opera Mobile
            r'Opera/9.80 (Android 2.3.4; Linux; Opera Mobi/build-1107180945; U; en-GB) Presto/2.8.149 Version/11.10',
            # BlackBerry
            r'Mozilla/5.0 (BlackBerry; U; BlackBerry 9800; en) AppleWebKit/534.1+ (KHTML, like Gecko) Version/6.0.0.337 Mobile Safari/534.1+',
            # Nokia N97
            r'Mozilla/5.0 (SymbianOS/9.4; Series60/5.0 NokiaN97-1/20.0.019; Profile/MIDP-2.1 Configuration/CLDC-1.1) AppleWebKit/525 (KHTML, like Gecko) BrowserNG/7.1.18124',
            # Android N1
            r'Mozilla/5.0 (Linux; U; Android 2.3.7; en-us; Nexus One Build/FRF91) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1'
        ]

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
            self, target: str, mode: RequestMode,
            params: Optional[dict] = None, headers: Optional[dict] = None,
            retry: Optional[int] = None, timeout: Optional[float] = None, colddown_factor: Optional[float] = None,
            deadline: Optional[float] = None,
            parser: Optional[Callable[[str], Optional[dict]]] = None
    ) -> Optional[dict]:
        # assemble headers
        if headers is None:
            headers = self._headers
        else:
            headers = {**self._headers, **headers}
        # add User-Agent if not exists
        if 'User-Agent' not in headers:
            headers['User-Agent'] = random.choice(self._ua_list)

        # config
        retry = retry if retry is not None else self._retry
        timeout = timeout if timeout is not None else self._timeout
        colddown_factor = colddown_factor if colddown_factor is not None else self._colddown_factor
        deadline = deadline if deadline is not None else self._deadline
        rate_limit_checker = RATE_LIMIT_CHECKERS.get(target, default_rate_limit)

        if mode == 'direct':
            try:
                direct_url = self.endpoints[target]['direct']
            except KeyError:
                logger.critical(f'Endpoint "{target}" not found.')
                raise SystemExit(1)
        elif mode == 'worker':
            direct_url = None
        else:
            logger.critical(f'Invalid request mode: {mode}.')
            raise SystemExit(1)

        # go request
        response = None
        for trial in range(1, retry + 1):
            selected_worker = None
            request_url = direct_url
            if mode == 'worker':
                try:
                    selected_worker = self._worker_selector.select(target)
                except WorkerConfigurationError as e:
                    logger.critical(f'Invalid worker configuration: {e}')
                    raise SystemExit(1)
                request_url = selected_worker.url

            # colddown for retry
            if trial > 1:
                # fluctuation range 0.75 ~ 1.25
                time.sleep((trial - 1) * (random.random()
                           * 0.5 + 0.75) * colddown_factor)

            # try to get response
            trial_start = time.perf_counter()
            try:
                r = self._session.get(request_url, params=params, headers=headers,
                                      timeout=timeout, stream=True)
            except requests.exceptions.RequestException as e:
                logger.debug(
                    f'Fail to get response. '
                    f'url: {request_url}, params: {params}, trial: {trial}, error: {e}'
                )
                continue

            # `timeout` above only bounds the gap between individual socket
            # reads. A response that trickles bytes slower than that gap but
            # never actually stalls sails right through it -- observed in
            # production as 100+ second single-trial requests against the
            # worker Lambda, each one parking a fetch thread the whole time.
            # Enforce a real wall-clock budget across the whole download and
            # abandon (close) the connection the moment it's blown, so the
            # thread is freed instead of stuck waiting on a slow socket.
            #
            # A flat budget isn't enough though: some responses (season /
            # multi-part videos) run 200KB-2.8MB against a typical few-KB
            # payload, and legitimately need more wall-clock time under
            # concurrent load. Scale the budget by the declared response size
            # so those aren't killed mid-transfer.
            trial_deadline = deadline
            content_length = r.headers.get('Content-Length')
            if content_length is not None:
                try:
                    trial_deadline = max(deadline, int(content_length) / self._min_throughput_bps)
                except ValueError:
                    pass

            body = bytearray()
            deadline_exceeded = False
            try:
                for chunk in r.iter_content(chunk_size=65536):
                    body += chunk
                    if time.perf_counter() - trial_start > trial_deadline:
                        deadline_exceeded = True
                        break
            except requests.exceptions.RequestException as e:
                r.close()
                logger.debug(
                    f'Fail to read response body. '
                    f'url: {request_url}, params: {params}, trial: {trial}, error: {e}'
                )
                continue
            if deadline_exceeded:
                r.close()
                trial_ms = int((time.perf_counter() - trial_start) * 1000)
                logger.debug(
                    f'Deadline exceeded while downloading response body. '
                    f'url: {request_url}, params: {params}, trial: {trial}, deadline: {trial_deadline:.1f}s, '
                    f'content_length: {content_length}, duration: {trial_ms}ms'
                )
                continue
            # populate requests' internal cache with what we already
            # downloaded, so r.json()/r.text below read it directly instead
            # of trying to re-read the now-exhausted stream
            r._content = bytes(body)
            r._content_consumed = True

            trial_ms = int((time.perf_counter() - trial_start) * 1000)

            limited = rate_limit_checker(r)
            if limited is not None:
                if mode == 'direct':
                    now = time.monotonic()
                    raise RateLimitError(
                        target, limited.reason, now, now + limited.cooldown_s)
                self._worker_selector.mark_rate_limited(
                    target, selected_worker, reason=limited.reason,
                    cooldown_s=limited.cooldown_s)
                continue

            # check status code
            if r.status_code != 200:
                logger.debug(
                    f'Fail to get response with status code {r.status_code}. '
                    f'url: {request_url}, params: {params}, trial: {trial}, duration: {trial_ms}ms'
                )
                continue

            # greppable per-request line (REQUEST): trial > 1 means retries
            # happened; duration is the pure network round-trip of this trial
            logger.debug(
                f'REQUEST url: {request_url}, params: {params}, trial: {trial}, duration: {trial_ms}ms')

            # parse response
            if parser is None:
                try:
                    response = r.json()
                    break
                except json.JSONDecodeError:
                    logger.debug(
                        f'Fail to decode response to json. '
                        f'response: {r.text}, url: {request_url}, params: {params}, trial: {trial}'
                    )
                    continue
            else:
                response = parser(r.text)
                if response is not None:
                    break
        return response

    def get_video_view(
            self, params: Optional[dict] = None, headers: Optional[dict] = None,
            retry: Optional[int] = None, timeout: Optional[float] = None, colddown_factor: Optional[float] = None,
            mode: Optional[RequestMode] = None
    ) -> VideoView:
        """
        params: { aid: int }
        mode: 'direct' | 'worker'
        """
        # config mode
        mode = mode if mode is not None else self._mode

        # validate params
        if mode not in ['direct', 'worker']:
            logger.critical(f'Invalid request mode: {mode}.')
            exit(1)

        # get response
        response = self._get('get_video_view', mode, params=params, headers=headers,
                             retry=retry, timeout=timeout, colddown_factor=colddown_factor)
        if response is None:
            raise ResponseError('video_view', params)

        # validate format

        # response should contain keys
        for key in ['code', 'message', 'ttl']:
            if key not in response.keys():
                raise FormatError('video_view', params, response,
                                  f'Response should contain key {key}.')
        # response code should be 0
        if response['code'] != 0:
            raise CodeError('video_view', params, response, response['code'])
        # response data should be a dict
        if type(response['data']) != dict:
            raise FormatError('video_view', params, response,
                              'Response data should be a dict.')
        # data should contain keys
        for key in ['bvid', 'aid', 'videos', 'tid', 'tname', 'copyright', 'pic', 'title', 'pubdate', 'ctime', 'desc',
                    'state', 'duration', 'owner', 'stat']:
            if key not in response['data'].keys():
                raise FormatError('video_view', params, response,
                                  f'Response data should contain key {key}.')
        # response data owner should be a dict
        if type(response['data']['owner']) != dict:
            raise FormatError('video_view', params, response,
                              'Response data owner should be a dict.')
        # data owner should contain keys
        for key in ['mid', 'name', 'face']:
            if key not in response['data']['owner'].keys():
                raise FormatError('video_view', params, response,
                                  f'Response data owner should contain key {key}.')
        # response data stat should be a dict
        if type(response['data']['stat']) != dict:
            raise FormatError('video_view', params, response,
                              'Response data stat should be a dict.')
        # data stat should contain keys
        for key in ['aid', 'view', 'danmaku', 'reply', 'favorite', 'coin', 'share', 'now_rank', 'his_rank', 'like',
                    'dislike']:
            if key not in response['data']['stat'].keys():
                raise FormatError('video_view', params, response,
                                  f'Response data stat should contain key {key}.')
        # response data staff should be a list if exists
        if 'staff' in response['data'].keys():
            if type(response['data']['staff']) != list:
                raise FormatError('video_view', params, response,
                                  'Response data staff should be a list.')
            # staff item should be a dict
            for staff_item in response['data']['staff']:
                if type(staff_item) != dict:
                    raise FormatError(
                        'video_view', params, response, 'Response data staff item should be a dict.')
                # staff item should contain keys
                for key in ['mid', 'title', 'name', 'face']:
                    if key not in staff_item.keys():
                        raise FormatError('video_view', params, response,
                                          f'Response data staff item should contain key {key}.')

        # assemble data
        staff = None
        if 'staff' in response['data'].keys():
            staff = []
            for staff_item in response['data']['staff']:
                staff.append(VideoViewStaffItem(
                    mid=staff_item['mid'],
                    title=staff_item['title'],
                    name=staff_item['name'],
                    face=staff_item['face']
                ))
        return VideoView(
            bvid=response['data']['bvid'],
            aid=response['data']['aid'],
            videos=response['data']['videos'],
            tid=response['data']['tid'],
            tname=response['data']['tname'],
            copyright=response['data']['copyright'],
            pic=response['data']['pic'],
            title=response['data']['title'],
            pubdate=response['data']['pubdate'],
            ctime=response['data']['ctime'],
            desc=response['data']['desc'],
            state=response['data']['state'],
            duration=response['data']['duration'],
            owner=VideoViewOwner(
                mid=response['data']['owner']['mid'],
                name=response['data']['owner']['name'],
                face=response['data']['owner']['face']
            ),
            stat=VideoViewStat(
                aid=response['data']['stat']['aid'],
                view=response['data']['stat']['view'],
                danmaku=response['data']['stat']['danmaku'],
                reply=response['data']['stat']['reply'],
                favorite=response['data']['stat']['favorite'],
                coin=response['data']['stat']['coin'],
                share=response['data']['stat']['share'],
                now_rank=response['data']['stat']['now_rank'],
                his_rank=response['data']['stat']['his_rank'],
                like=response['data']['stat']['like'],
                dislike=response['data']['stat']['dislike'],
                vt=response['data']['stat'].get('vt', None),
                vv=response['data']['stat'].get('vv', None),
            ),
            attribute=response['data'].get('attribute', None),
            forward=response['data'].get('forward', None),
            staff=staff
        )

    def get_video_view_trimmed(
            self, params: Optional[dict] = None, headers: Optional[dict] = None,
            retry: Optional[int] = None, timeout: Optional[float] = None, colddown_factor: Optional[float] = None,
            mode: Optional[RequestMode] = None
    ) -> VideoViewTrimmed:
        """
        params: { aid: int }
        mode: 'worker' only

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
        # config mode
        mode = mode if mode is not None else self._mode

        # validate params
        if mode != 'worker':
            logger.critical(f'Endpoint "get_video_view_trimmed" is worker-only '
                            f'(no direct API serves the trimmed contract), got mode: {mode}. '
                            f'Use get_video_view for direct mode.')
            exit(1)

        # get response
        response = self._get('get_video_view_trimmed', 'worker', params=params, headers=headers,
                             retry=retry, timeout=timeout, colddown_factor=colddown_factor)
        if response is None:
            raise ResponseError('video_view_trimmed', params)

        # validate format

        # response should contain keys
        for key in ['code', 'message', 'ttl']:
            if key not in response.keys():
                raise FormatError('video_view_trimmed', params, response,
                                  f'Response should contain key {key}.')
        # response code should be 0
        if response['code'] != 0:
            raise CodeError('video_view_trimmed', params,
                            response, response['code'])
        # response data should be a dict
        if type(response['data']) != dict:
            raise FormatError('video_view_trimmed', params, response,
                              'Response data should be a dict.')
        # data should contain keys
        for key in ['bvid', 'aid', 'stat']:
            if key not in response['data'].keys():
                raise FormatError('video_view_trimmed', params, response,
                                  f'Response data should contain key {key}.')
        # response data stat should be a dict
        if type(response['data']['stat']) != dict:
            raise FormatError('video_view_trimmed', params, response,
                              'Response data stat should be a dict.')
        # data stat should contain keys
        for key in ['aid', 'view', 'danmaku', 'reply', 'favorite', 'coin', 'share', 'now_rank', 'his_rank', 'like',
                    'dislike']:
            if key not in response['data']['stat'].keys():
                raise FormatError('video_view_trimmed', params, response,
                                  f'Response data stat should contain key {key}.')

        # assemble data
        return VideoViewTrimmed(
            bvid=response['data']['bvid'],
            aid=response['data']['aid'],
            stat=VideoViewStat(
                aid=response['data']['stat']['aid'],
                view=response['data']['stat']['view'],
                danmaku=response['data']['stat']['danmaku'],
                reply=response['data']['stat']['reply'],
                favorite=response['data']['stat']['favorite'],
                coin=response['data']['stat']['coin'],
                share=response['data']['stat']['share'],
                now_rank=response['data']['stat']['now_rank'],
                his_rank=response['data']['stat']['his_rank'],
                like=response['data']['stat']['like'],
                dislike=response['data']['stat']['dislike'],
                vt=response['data']['stat'].get('vt', None),
                vv=response['data']['stat'].get('vv', None),
            ),
        )

    def get_video_view_trimmed_batch(
            self, aids: list[int], headers: Optional[dict] = None,
            mode: Optional[RequestMode] = None
    ) -> list[VideoViewTrimmedBatchItem]:
        """
        aids: aid ints, already de-duplicated by the caller (the batch worker
              forwards duplicate tokens as-is per its contract)
        mode: 'worker' only

        Batched counterpart of get_video_view_trimmed against the batch worker
        (service/workers/video_view/aws_lambda_batch.mjs, contract v1): one GET
        with ?aids=a,b,c returns per-item envelopes, same order as the input.

        Error model, three tiers. A top-level HTTP 200 does NOT mean the items
        succeeded -- every item is judged on its own status + kind + body.code:

        - returns list[VideoViewTrimmedBatchItem], same length/order as aids.
          An item is trusted (view set) only when kind == 'json' AND upstream
          status == 200 AND body.code == 0 AND the full single-path format
          check set AND the identity checks all pass. kind == 'json' with
          status == 200 and code != 0 carries a CodeError (identical routing
          to the single path). Everything transient (item_timeout, fetch_error,
          non_json, or ANY non-200 upstream status -- the single path never
          parses non-200 bodies, so a code==0 body on a 500 is NOT trusted)
          carries a ResponseError: retry this item only.
        - raises ResponseError: whole-batch transport failure -- HTTP != 200 /
          unparsable top level (includes the worker's own 400/500 envelopes).
          The caller owns the retry and counts one attempt for every aid in
          the batch.
        - raises MisalignmentError: top level parsed but violates the batch
          contract (v != 1, requested/results length mismatch) or an item
          fails an identity/format check (aid echo, body.data.aid, bvid,
          stat.aid, missing keys). The whole batch result is untrustworthy:
          the caller must discard it entirely and trip its kill-switch.

        No internal retry (retry=1 below): whole-batch retries belong to the
        caller's per-aid attempt accounting; stacking Service-level retries
        under them would multiply Lambda invocations invisibly.

        Missing worker configuration is treated as a startup/configuration
        error consistently with the sibling methods and exits when this target
        is first used.
        """
        # config mode
        mode = mode if mode is not None else self._mode

        # validate params
        if mode != 'worker':
            logger.critical(f'Endpoint "get_video_view_trimmed_batch" is worker-only '
                            f'(no direct API serves the batch contract), got mode: {mode}.')
            exit(1)

        params = {'aids': ','.join(str(aid) for aid in aids)}

        # get response (batch-specific timeout/deadline; single whole-batch
        # trial, see docstring)
        response = self._get('get_video_view_trimmed_batch', 'worker', params=params, headers=headers,
                             retry=1, timeout=VIDEO_VIEW_TRIMMED_BATCH_READ_TIMEOUT_S,
                             deadline=VIDEO_VIEW_TRIMMED_BATCH_DEADLINE_S)
        if response is None:
            raise ResponseError('video_view_trimmed_batch', params)

        def misaligned(message: str):
            return MisalignmentError('video_view_trimmed_batch', params, response, message)

        # validate the batch envelope. The v check is the double insurance
        # against a config error pointing this path at a non-batch endpoint
        # (e.g. the single-aid worker answers ?aids= with an upstream -400
        # passthrough -- valid JSON, no v). Protocol fields are checked
        # strictly by TYPE as well as value: bool == int in Python, so
        # without `type(x) is int` a JSON `true` would pass a `== 1` check.
        version = response.get('v') if isinstance(response, dict) else None
        if type(version) is not int or version != VIDEO_VIEW_TRIMMED_BATCH_PROTOCOL_VERSION:
            raise misaligned(f'Response is not a batch protocol '
                             f'v{VIDEO_VIEW_TRIMMED_BATCH_PROTOCOL_VERSION} envelope.')
        requested = response.get('requested')
        if type(requested) is not int or requested != len(aids):
            raise misaligned(f'Response requested {requested!r} != {len(aids)} aids sent.')
        results = response.get('results')
        if not isinstance(results, list) or len(results) != len(aids):
            raise misaligned(f'Response results is not a list of length {len(aids)}.')

        # validate items: same order as the input, each judged independently
        items: list[VideoViewTrimmedBatchItem] = []
        for index, (aid, item) in enumerate(zip(aids, results)):
            item_params = {'aid': aid}
            if not isinstance(item, dict):
                raise misaligned(f'Result {index} is not a dict.')
            # aid echo: the worker echoes the raw token, and we sent str(aid)
            if item.get('aid') != str(aid):
                raise misaligned(
                    f'Result {index} echoes aid {item.get("aid")!r}, expected {str(aid)!r}.')
            kind = item.get('kind')

            if kind in ('item_timeout', 'fetch_error', 'non_json'):
                # transient, this item only. non_json matches the single path,
                # where an unparsable body is retried then given up on.
                items.append(VideoViewTrimmedBatchItem(
                    aid=aid, view=None,
                    error=ResponseError('video_view_trimmed_batch_item',
                                        {**item_params, 'kind': kind})))
                continue

            if kind != 'json':
                raise misaligned(f'Result {index} has unknown kind {kind!r}.')

            body = item.get('body')
            if not isinstance(body, dict):
                raise misaligned(f'Result {index} kind json has no dict body.')

            # the worker always sets an integer upstream status on json items;
            # a missing or non-int one (bool included) is a contract violation,
            # not an upstream hiccup to retry
            status = item.get('status')
            if type(status) is not int:
                raise misaligned(f'Result {index} kind json has non-int status {status!r}.')

            # non-200 upstream: NEVER trust the body, code == 0 included --
            # the single path retries non-200 without parsing (see _get), so
            # trusting it here would be a semantic change, not a batch detail
            if status != 200:
                items.append(VideoViewTrimmedBatchItem(
                    aid=aid, view=None,
                    error=ResponseError('video_view_trimmed_batch_item',
                                        {**item_params, 'kind': kind,
                                         'status': status})))
                continue

            # from here on: same check set as get_video_view_trimmed, except
            # any violation is a misalignment (the batch worker BUILT this
            # envelope; a malformed one means the path is broken, not that one
            # video is odd), plus the identity checks
            for key in ['code', 'message', 'ttl']:
                if key not in body.keys():
                    raise misaligned(f'Result {index} body should contain key {key}.')

            if body['code'] != 0:
                # upstream said no -- identical semantics to the single-path
                # CodeError (same target so downstream logging matches)
                items.append(VideoViewTrimmedBatchItem(
                    aid=aid, view=None,
                    error=CodeError('video_view_trimmed', item_params, body, body['code'])))
                continue

            if type(body.get('data')) != dict:
                raise misaligned(f'Result {index} body data should be a dict.')
            data = body['data']
            for key in ['bvid', 'aid', 'stat']:
                if key not in data.keys():
                    raise misaligned(f'Result {index} body data should contain key {key}.')
            if type(data['stat']) != dict:
                raise misaligned(f'Result {index} body data stat should be a dict.')
            stat = data['stat']
            for key in ['aid', 'view', 'danmaku', 'reply', 'favorite', 'coin', 'share',
                        'now_rank', 'his_rank', 'like', 'dislike']:
                if key not in stat.keys():
                    raise misaligned(f'Result {index} body data stat should contain key {key}.')

            # identity checks: strong misrouting/misalignment detectors. bvid
            # is independently computable from aid, so a payload from another
            # video (or another endpoint) cannot satisfy both.
            if data['aid'] != aid:
                raise misaligned(f'Result {index} body data aid {data["aid"]!r} != {aid}.')
            if data['bvid'] != 'BV' + a2b(aid):
                raise misaligned(
                    f'Result {index} body data bvid {data["bvid"]!r} != computed for aid {aid}.')
            if stat['aid'] not in (aid, 0):  # 0 has upstream precedent (see task.py)
                raise misaligned(f'Result {index} body data stat aid {stat["aid"]!r} != {aid} or 0.')

            items.append(VideoViewTrimmedBatchItem(
                aid=aid,
                view=VideoViewTrimmed(
                    bvid=data['bvid'],
                    aid=data['aid'],
                    stat=VideoViewStat(
                        aid=stat['aid'],
                        view=stat['view'],
                        danmaku=stat['danmaku'],
                        reply=stat['reply'],
                        favorite=stat['favorite'],
                        coin=stat['coin'],
                        share=stat['share'],
                        now_rank=stat['now_rank'],
                        his_rank=stat['his_rank'],
                        like=stat['like'],
                        dislike=stat['dislike'],
                        vt=stat.get('vt', None),
                        vv=stat.get('vv', None),
                    ),
                ),
                error=None))

        return items

    def get_video_tags(
            self, params: Optional[dict] = None, headers: Optional[dict] = None,
            retry: Optional[int] = None, timeout: Optional[float] = None, colddown_factor: Optional[float] = None,
            mode: Optional[RequestMode] = None
    ) -> VideoTags:
        """
        params: { aid: int }
        mode: 'direct' | 'worker'
        """
        # config mode
        mode = mode if mode is not None else self._mode

        # validate params
        if mode not in ['direct', 'worker']:
            logger.critical(f'Invalid request mode: {mode}.')
            exit(1)

        # define parser
        def parser(text: str) -> Optional[dict]:
            logger.debug(
                f'Try to parse video tags response text. text: {text}.')
            parsed_response = None
            try:
                parsed_response = json.loads(text)
            except json.JSONDecodeError:
                logger.debug(f'Fail to decode text to json. Return None.')
            if parsed_response is not None:
                code = parsed_response['code']
                if code in [-500, -504]:
                    logger.debug(
                        f'Status code {code} found. Server timeout occurred, return None for retry.')
                    parsed_response = None
            return parsed_response

        # get response
        response = self._get('get_video_tags', mode, params=params, headers=headers,
                             retry=retry, timeout=timeout, colddown_factor=colddown_factor,
                             parser=parser)
        if response is None:
            raise ResponseError('video_tags', params)

        # validate format

        # response should contain keys
        for key in ['code', 'message', 'ttl']:
            if key not in response.keys():
                raise FormatError('video_tags', params, response,
                                  f'Response should contain key {key}.')
        # response code should be 0
        if response['code'] != 0:
            raise CodeError('video_tags', params, response, response['code'])
        # response data should be a list
        if type(response['data']) != list:
            raise FormatError('video_tags', params, response,
                              'Response data should be a list.')
        # for each data item
        for data_item in response['data']:
            # data item should be a dict
            if type(data_item) != dict:
                raise FormatError('video_tags', params, response,
                                  'Response data item should be a dict.')
            # data item should contain keys
            for key in ['tag_id', 'tag_name']:
                if key not in data_item.keys():
                    raise FormatError('video_tags', params, response,
                                      f'Response data item should contain key {key}.')

        # assemble data
        videoTags = VideoTags(tags=[])
        for data_item in response['data']:
            videoTags.tags.append(VideoTag(
                tag_id=data_item['tag_id'],
                tag_name=data_item['tag_name']
            ))
        return videoTags

    def get_member_card(
            self, params: Optional[dict] = None, headers: Optional[dict] = None,
            retry: Optional[int] = None, timeout: Optional[float] = None, colddown_factor: Optional[float] = None,
            mode: Optional[RequestMode] = None
    ) -> MemberCard:
        """
        params: { mid: int }
        mode: 'direct' | 'worker'
        """
        # config mode
        mode = mode if mode is not None else self._mode

        # validate params
        if mode not in ['direct', 'worker']:
            logger.critical(f'Invalid request mode: {mode}.')
            exit(1)

        # get response
        response = self._get('get_member_card', mode, params=params, headers=headers,
                             retry=retry, timeout=timeout, colddown_factor=colddown_factor)
        if response is None:
            raise ResponseError('member_card', params)

        # validate format

        # response should contain keys
        for key in ['code', 'message', 'ttl']:
            if key not in response.keys():
                raise FormatError('member_card', params, response,
                                  f'Response should contain key {key}.')
        # response code should be 0
        if response['code'] != 0:
            raise CodeError('member_card', params, response, response['code'])
        # response data should be a dict
        if type(response['data']) != dict:
            raise FormatError('member_card', params, response,
                              'Response data should be a dict.')
        # data should contain keys
        for key in ['card']:
            if key not in response['data'].keys():
                raise FormatError('member_card', params, response,
                                  f'Response data should contain key {key}.')
        # data card should be a dict
        if type(response['data']['card']) != dict:
            raise FormatError('member_card', params, response,
                              'Response data card should be a dict.')
        # data card should contain keys
        for key in ['mid', 'name', 'sex', 'face', 'sign']:
            if key not in response['data']['card'].keys():
                raise FormatError('member_card', params, response,
                                  f'Response data card should contain key {key}.')

        # assemble data
        return MemberCard(
            mid=response['data']['card']['mid'],
            name=response['data']['card']['name'],
            sex=response['data']['card']['sex'],
            face=response['data']['card']['face'],
            sign=response['data']['card']['sign']
        )

    def get_member_relation(
            self, params: Optional[dict] = None, headers: Optional[dict] = None,
            retry: Optional[int] = None, timeout: Optional[float] = None, colddown_factor: Optional[float] = None,
            mode: Optional[RequestMode] = None
    ) -> MemberRelation:
        """
        params: { vmid: int }
        mode: 'direct' | 'worker'
        """
        # config mode
        mode = mode if mode is not None else self._mode

        # validate params
        if mode not in ['direct', 'worker']:
            logger.critical(f'Invalid request mode: {mode}.')
            exit(1)

        # get response
        response = self._get('get_member_relation', mode, params=params, headers=headers,
                             retry=retry, timeout=timeout, colddown_factor=colddown_factor)
        if response is None:
            raise ResponseError('member_relation', params)

        # validate format

        # response should contain keys
        for key in ['code', 'message', 'ttl']:
            if key not in response.keys():
                raise FormatError('member_relation', params,
                                  response, f'Response should contain key {key}.')
        # response code should be 0
        if response['code'] != 0:
            raise CodeError('member_relation', params,
                            response, response['code'])
        # response data should be a dict
        if type(response['data']) != dict:
            raise FormatError('member_relation', params,
                              response, 'Response data should be a dict.')
        # data should contain keys
        for key in ['mid', 'following', 'follower']:
            if key not in response['data'].keys():
                raise FormatError('member_relation', params, response,
                                  f'Response data should contain key {key}.')

        # assemble data
        return MemberRelation(
            mid=response['data']['mid'],
            following=response['data']['following'],
            follower=response['data']['follower']
        )

    def get_newlist(
            self, params: Optional[dict] = None, headers: Optional[dict] = None,
            retry: Optional[int] = None, timeout: Optional[float] = None, colddown_factor: Optional[float] = None,
            mode: Optional[RequestMode] = None
    ) -> Newlist:
        """
        params: { rid: int, pn: int, ps: int }
        mode: 'direct' | 'worker'
        """
        # config mode
        mode = mode if mode is not None else self._mode

        # validate params
        if mode not in ['direct', 'worker']:
            logger.critical(f'Invalid request mode: {mode}.')
            exit(1)

        # get endpoint url
        # define parser
        def parser(text: str) -> Optional[dict]:
            logger.debug(f'Try to parse newlist response text. text: {text}.')
            parsed_response = None
            try:
                parsed_response = json.loads(text)
            except json.JSONDecodeError:
                logger.debug(f'Fail to decode text to json. Return None.')
            if parsed_response is not None:
                code = parsed_response['code']
                if code in [-40002]:
                    logger.debug(
                        f'Status code {code} found. Server timeout occurred, return None for retry.')
                    parsed_response = None
            return parsed_response

        # get response
        response = self._get('get_newlist', mode, params=params, headers=headers,
                             retry=retry, timeout=timeout, colddown_factor=colddown_factor,
                             parser=parser)
        if response is None:
            raise ResponseError('newlist', params)

        # validate format

        # response should contain keys
        for key in ['code', 'message']:
            if key not in response.keys():
                raise FormatError('newlist', params, response,
                                  f'Response should contain key {key}.')
        # response code should be 0
        if response['code'] != 0:
            raise CodeError('newlist', params, response, response['code'])
        # response data should be a dict
        if type(response['data']) != dict:
            raise FormatError('newlist', params, response,
                              'Response data should be a dict.')
        # data should contain keys
        for key in ['archives', 'page']:
            if key not in response['data'].keys():
                raise FormatError('newlist', params, response,
                                  f'Response data should contain key {key}.')
        # data archives should be a list
        if type(response['data']['archives']) != list:
            raise FormatError('newlist', params, response,
                              'Response data archives should be a list.')
        # for each data archives item
        for data_archives_item in response['data']['archives']:
            # data archives item should be a dict
            if type(data_archives_item) != dict:
                raise FormatError('newlist', params, response,
                                  'Response data archives item should be a dict.')
            # data archives item should contain keys
            for key in ['aid', 'videos', 'tid', 'tname', 'copyright', 'pic', 'title', 'stat', 'bvid', 'desc', 'owner']:
                if key not in data_archives_item.keys():
                    raise FormatError('newlist', params, response,
                                      f'Response data archives item should contain key {key}.')
                # data archives item stat should be a dict
                if type(data_archives_item['stat']) != dict:
                    raise FormatError('newlist', params, response,
                                      'Response data archives item stat should be a dict.')
                # data archives item stat should contain keys
                for key2 in ['aid', 'view', 'danmaku', 'reply', 'favorite', 'coin', 'share', 'now_rank', 'his_rank',
                             'like', 'dislike', 'vt', 'vv']:
                    if key2 not in data_archives_item['stat'].keys():
                        raise FormatError('newlist', params, response,
                                          f'Response data archives item stat should contain key {key2}.')
                # data archives item owner should be a dict
                if type(data_archives_item['owner']) != dict:
                    raise FormatError('newlist', params, response,
                                      'Response data archives item owner should be a dict.')
                # data archives item stat should contain keys
                for key2 in ['mid', 'name', 'face']:
                    if key2 not in data_archives_item['owner'].keys():
                        raise FormatError('newlist', params, response,
                                          f'Response data archives item owner should contain key {key2}.')
        # data page should be a dict
        if type(response['data']['page']) != dict:
            raise FormatError('newlist', params, response,
                              'Response data page should be a dict.')
        # data page should contain keys
        for key in ['count', 'num', 'size']:
            if key not in response['data']['page'].keys():
                raise FormatError('newlist', params, response,
                                  f'Response data page should contain key {key}.')

        # assemble data
        newlistPage = NewlistPage(
            count=response['data']['page']['count'],
            num=response['data']['page']['num'],
            size=response['data']['page']['size']
        )
        newlistArchives = []
        for data_archives_item in response['data']['archives']:
            newlistArchives.append(NewlistArchive(
                aid=data_archives_item['aid'],
                videos=data_archives_item['videos'],
                tid=data_archives_item['tid'],
                tname=data_archives_item['tname'],
                copyright=data_archives_item['copyright'],
                pic=data_archives_item['pic'],
                title=data_archives_item['title'],
                stat=NewlistArchiveStat(
                    aid=data_archives_item['stat']['aid'],
                    view=data_archives_item['stat']['view'],
                    danmaku=data_archives_item['stat']['danmaku'],
                    reply=data_archives_item['stat']['reply'],
                    favorite=data_archives_item['stat']['favorite'],
                    coin=data_archives_item['stat']['coin'],
                    share=data_archives_item['stat']['share'],
                    now_rank=data_archives_item['stat']['now_rank'],
                    his_rank=data_archives_item['stat']['his_rank'],
                    like=data_archives_item['stat']['like'],
                    dislike=data_archives_item['stat']['dislike'],
                    vt=data_archives_item['stat']['vt'],
                    vv=data_archives_item['stat']['vv']
                ),
                bvid=data_archives_item['bvid'],
                desc=data_archives_item['desc'],
                owner=NewlistArchiveOwner(
                    mid=data_archives_item['owner']['mid'],
                    name=data_archives_item['owner']['name'],
                    face=data_archives_item['owner']['face']
                ),
            ))
        return Newlist(
            archives=newlistArchives,
            page=newlistPage
        )
