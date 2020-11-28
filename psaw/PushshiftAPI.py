from collections import namedtuple, deque, Counter
import copy
import json
import logging
import requests
import time
from datetime import datetime as dt
import warnings

log = logging.getLogger(__name__)

class RateLimitCache(object):
    def __init__(self, n, t=60):
        self.n = n
        self.t = t
        self.cache = deque()
    @property
    def delta(self):
        """Time since earliest call"""
        if len(self.cache) == 0:
            return 0
        return (time.time() - self.cache[0])
    def update(self):
        while self.delta > self.t:
            try:
                self.cache.popleft()
            except IndexError:
                return
    @property
    def blocked(self):
        """Test if additional calls need to be blocked"""
        self.update()
        return len(self.cache) >= self.n
    @property
    def interval(self):
        self.update()
        if self.t > self.delta:
             return self.t - self.delta
        else:
            return 0
    def new(self):
        self.update()
        if self.blocked:
            raise Exception("RateLimitCache is blocked.")
        self.cache.append(time.time())


class PushshiftAPIMinimal(object):
    #base_url = {'search':'https://api.pushshift.io/reddit/{}/search/',
    #            'meta':'https://api.pushshift.io/meta/'}
    _base_url = 'https://{domain}.pushshift.io/{{endpoint}}'
    _limited_args = ('aggs', 'ids')
    _thing_prefix = {
        	'Comment':'t1_',
        	'Account':'t2_',
        	'Submission':'t3_',
        	'Message':'t4_',
        	'Subreddit':'t5_',
        	'Award':'t6_'
    }
    def __init__(self,
                 max_retries=20,
                 max_sleep=3600,
                 backoff=2,
                 rate_limit_per_minute=None,
                 max_results_per_request=1000,
                 detect_local_tz=True,
                 utc_offset_secs=None,
                 domain='api',
                 https_proxy=None,
                 shards_down_behavior='warn' # must be one of ['warn','stop' or None] # To do: add 'retry'
                ):
        assert max_results_per_request <= 1000
        assert backoff >= 1

        self.max_retries = max_retries
        self.max_sleep   = max_sleep
        self.backoff     = backoff
        self.max_results_per_request = max_results_per_request

        self._utc_offset_secs = utc_offset_secs
        self._detect_local_tz = detect_local_tz

        self.domain = domain
        if https_proxy is not None:
            self.proxies = {"https": https_proxy }
        else:
            self.proxies = {}
        self.shards_down_behavior = shards_down_behavior
        self.metadata_ = {}

        if rate_limit_per_minute is None:
            log.debug("Connecting to /meta endpoint to learn rate limit.")
            response = self._get(self.base_url.format(endpoint='meta'))
            rate_limit_per_minute = response['server_ratelimit_per_minute']
            log.debug("server_ratelimit_per_minute: %s" % rate_limit_per_minute)
        self._rlcache = RateLimitCache(n=rate_limit_per_minute, t=60)

    @property
    def base_url(self):
        return self._base_url.format(domain=self.domain)

    @property
    def utc_offset_secs(self):
        if self._utc_offset_secs is not None:
            return self._utc_offset_secs

        if self._detect_local_tz:
            try:
                self._utc_offset_secs = dt.utcnow().astimezone().utcoffset().total_seconds()
            except ValueError:
                self._utc_offset_secs = 0
        else:
            self._utc_offset_secs = 0

        return self._utc_offset_secs
    
    @property
    def shards_are_down(self):
        shards = self.metadata_.get('shards')
        if shards is None:
            return
        if shards['successful'] != shards['total']:
            return True
        return False

    def _limited(self, payload):
        """Turn off bells and whistles for special API endpoints"""
        return any(arg in payload for arg in self._limited_args)

    def _epoch_utc_to_local(self, epoch):
        return epoch - self.utc_offset_secs

    def _wrap_thing(self, thing, kind):
        """Mimic praw.Submission and praw.Comment API"""
        thing['created'] = self._epoch_utc_to_local(thing['created_utc'])
        thing['d_'] = copy.deepcopy(thing)
        ThingType = namedtuple(kind, thing.keys())
        thing = ThingType(**thing)
        return thing

    def _impose_rate_limit(self, nth_request=0):
        interval = 0
        if  hasattr(self, '_rlcache'):
            if self._rlcache.blocked:
                interval = self._rlcache.interval
        interval = max(interval, self.backoff*nth_request)
        interval = min(interval, self.max_sleep)
        if interval > 0:
            log.debug("Imposing rate limit, sleeping for %s" % interval)
            time.sleep(interval)

    def _add_nec_args(self, payload):
        """Adds 'limit' and 'created_utc' arguments to the payload as necessary."""
        if self._limited(payload):
            # Do nothing I guess? Not sure how paging works on this endpoint...
            return
        if 'limit' not in payload:
            payload['limit'] = self.max_results_per_request
        if 'metadata' not in payload:
            payload['metadata'] = 'true'
        if 'sort' not in payload:
            # Getting weird results if this is not made explicit. Unclear why.
            payload['sort'] = 'desc'
        if 'filter' in payload: #and payload.get('created_utc', None) is None:
            if not isinstance(payload['filter'], list):
                if isinstance(payload['filter'], str):
                    payload['filter'] = [payload['filter']]
                else:
                    payload['filter'] = list(payload['filter'])
            if 'created_utc' not in payload['filter']:
                payload['filter'].append('created_utc')

    def _get(self, url, payload={}):
        log.debug('URL: %s' % url)
        log.debug('Payload: %s' % payload)
        i, success = 0, False
        while (not success) and (i<self.max_retries):
            if i > 0:
                warnings.warn("Unable to connect to pushshift.io. Retrying after backoff.")
            self._impose_rate_limit(i)
            i+=1
            try:
                response = requests.get(url, params=payload, proxies=self.proxies)
                log.info(response.url)
                log.debug('Response status code: %s' % response.status_code)
            except requests.ConnectionError:
                log.debug("Connection error caught, retrying. Connection attempts so far: %s" % str(i+1))
                continue
            success = response.status_code == 200
            if not success:
                warnings.warn("Got non 200 code %s" % response.status_code)
        if not success:
            raise Exception("Unable to connect to pushshift.io. Max retries exceeded.")
        return json.loads(response.text)

    def _handle_paging(self, url):
        limit = self.payload.get('limit', None)
        #n = 0
        while True:
            if limit is not None:
                if limit > self.max_results_per_request:
                    self.payload['limit'] = self.max_results_per_request
                    limit -= self.max_results_per_request
                else:
                    self.payload['limit'] = limit
                    limit = 0
            elif 'ids' in self.payload:
                limit = 0
                if len(self.payload['ids']) > self.max_results_per_request:
                    err_msg = "When searching by ID, number of IDs must be fewer than the max number of objects in a single request ({})."
                    raise NotImplementedError(err_msg.format(self.max_results_per_request))
            self._add_nec_args(self.payload)

            data = self._get(url, self.payload)
            yield data
            if limit is not None:
                received_size = int(data['metadata']['size'])
                requested_size = self.payload['limit']
                # The API can decide to send less data than desired.
                # We need to send another request in that case requesting the missing amount
                if received_size < requested_size:
                    limit += requested_size - received_size

                if limit == 0:
                    return

    def _search(self,
                kind,
                stop_condition=lambda x: False,
                return_batch=False,
                dataset='reddit',
                **kwargs):
        self.metadata_ = {}
        self.payload = copy.deepcopy(kwargs)
        endpoint = '{dataset}/{kind}/search'.format(dataset=dataset, kind=kind)
        url = self.base_url.format(endpoint=endpoint)
        for response in self._handle_paging(url):
            if 'aggs' in response:
                yield response['aggs']
                # Aggs responses are unreliable in subsequent batches with
                # current search paging implementation. Enforce aggs result
                # is only returned once.
                self.payload.pop('aggs')
            self.metadata_ = response.get('metadata', {})
            log.debug('Metadata: %s' % self.metadata_)
            results = response['data']
            
            shards_down_message = "Not all PushShift shards are active. Query results may be incomplete"
            if self.shards_are_down and (self.shards_down_behavior is not None) :
                if self.shards_down_behavior == 'warn':
                    warnings.warn(shards_down_message)
                if self.shards_down_behavior == 'stop':
                    raise RuntimeError(shards_down_message)
            
            if len(results) == 0:
                return
            if return_batch:
                batch = []

            for thing in results:
                thing = self._wrap_thing(thing, kind)

                if return_batch:
                    batch.append(thing)
                else:
                    yield thing

                if stop_condition(thing):
                    if return_batch:
                        return batch
                    return

            if return_batch:
                yield batch

            # For paging.
            if self.payload.get('sort') == 'desc':
                self.payload['before'] = thing.created_utc
            else:
                self.payload['after'] = thing.created_utc


#class PushshiftAPI(PushshiftAPIMinimal):
    # Fill out this class with more user-friendly features later
#    pass

class PushshiftAPI(PushshiftAPIMinimal):
    def __init__(self, r=None, *args, **kwargs):
        """
        Helper class for interacting with the PushShift API for searching public reddit archival data.
        
        :param r: :class:`praw.Reddit` instance. If provided, PushShift will be used to fetch thing IDs, then data will be fetched directly from the reddit API via praw.
        :type r: class:`praw.Reddit`, optional
        
        :param max_retries: Maximum number of retries to attempt before quitting, defaults to 20.
        :type max_retries: int, optional
        
        :param max_sleep: Threshold waiting time (in seconds) between requests, to limit exponential backoff behavior, defaults to 3600 (1 hour).
        :type max_sleep: int, optional
        
        :param backoff: Multiplier for exponential backoff of wait time between failed requests, defaults to 2.
        :type backoff: int or float, optional
        
        :param rate_limit_per_minute: Maximum number of requests per 60 second period. If not provided, inferred from PushShift /meta endpoint.
        :type rate_limit_per_minute: int, optional
        
        :param max_results_per_request: Maximum number of items to return in a single request, defaults to 1000.
        :type max_results_per_request: int, optional
        
        :param detect_local_tz: Whether or not to attempt to detect the local time zone to infer `utc_offset_secs`, defaults to True.
        :type detect_local_tz: boolean, optional
        
        :param utc_offset_secs: Number of seconds local timezone is offset from UTC, defaults to None for automatic detection.
        :type utc_offset_secs: int, optional
        
        :param domain: PushShift subdomain, e.g. for accessing features only available in beta, defaults to 'api'.
        :type domain: str, optional
        
        :param https_proxy: URL of HTTPS proxy server to be used for GET requests, defaults to None.
        :type https_proxy: str, optional
        
        :param shards_down_behavior: How PSAW should behave if PushShift reports that some shards were down during a query. Options are "warn" to only emit a warning, "stop" to throw a RuntimeError, or None to take no action. Defaults to "warn".
        :type shards_down_behavior: str, optional
        """
        super().__init__(*args, **kwargs)
        self.r = r
        self._search_func = self._search
        if r is not None:
            self._search_func = self._praw_search

    def search_comments(self, **kwargs):
        return self._search_func(kind='comment', **kwargs)

    def search_submissions(self, **kwargs):
        return self._search_func(kind='submission', **kwargs)

    def redditor_subreddit_activity(self, author, **kwargs):
        """
        :param author: Redditor to be profiled
        :type author: str
        """
        outv = {}
        for k in ('comment', 'submission'):
            agg = next(self._search(kind=k, author=author, aggs='subreddit', **kwargs))
            outv[k] = Counter({rec['key']:rec['doc_count'] for rec in agg['subreddit']})
        return outv

    def _get_submission_comment_ids(self, submission_id, **kwargs):
        self.payload = copy.deepcopy(kwargs)
        endpoint = 'reddit/submission/comment_ids/{}'.format(submission_id)
        url = self.base_url.format(endpoint=endpoint)
        return self._get(url, self.payload)['data']

    def _praw_search(self, **kwargs):
        prefix = self._thing_prefix[kwargs['kind'].title()]

        self.payload = copy.deepcopy(kwargs)

        client_return_batch = kwargs.get('return_batch')
        if client_return_batch is False:
            self.payload.pop('return_batch')

        if 'filter' in kwargs:
            self.payload.pop('filter')


        gen = self._search(return_batch=True, filter='id', **self.payload)
        using_gsci = False
        if kwargs.get('kind') == 'comment' and self.payload.get('submission_id'):
            using_gsci = True
            gen = [self._get_submission_comment_ids(**kwargs)]

        for batch in gen:
            if not batch:
                return
            if using_gsci:
                fullnames = [prefix + base36id for base36id in batch]
            else:
                fullnames = [prefix + c.id for c in batch]
            praw_batch = self.r.info(fullnames=fullnames)
            if client_return_batch:
                yield praw_batch
            else:
                for praw_thing in praw_batch:
                    yield praw_thing
