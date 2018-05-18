from collections import namedtuple, deque
import copy
import json
import requests
import time
from datetime import datetime as dt

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
    _limited_args = ('aggs')
    _thing_prefix = {
        	'Comment':'t1_',
        	'Account':'t2_',
        	'Link':'t3_',
        	'Message':'t4_',
        	'Subreddit':'t5_',
        	'Award':'t6_'
    }
    def __init__(self,
                 max_retries=20,
                 max_sleep=3600,
                 backoff=2,
                 rate_limit_per_minute=None,
                 max_results_per_request=500,
                 detect_local_tz=True,
                 utc_offset_secs=None,
                 domain='api'
                ):
        assert max_results_per_request <= 500
        assert backoff >= 1

        self.max_retries = max_retries
        self.max_sleep   = max_sleep
        self.backoff     = backoff
        self.max_results_per_request = max_results_per_request

        self._utc_offset_secs = utc_offset_secs
        self._detect_local_tz = detect_local_tz

        self.domain = domain

        if rate_limit_per_minute is None:
            response = self._get(self.base_url.format(endpoint='meta'))
            rate_limit_per_minute = response['server_ratelimit_per_minute']
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
        if not hasattr(self, '_rlcache'):
            return
        interval = 0
        if self._rlcache.blocked:
            interval = self._rlcache.interval
        interval = max(interval, self.backoff*nth_request)
        interval = min(interval, self.max_sleep)
        if interval > 0:
            time.sleep(interval)

    def _add_nec_args(self, payload):
        """Adds 'limit' and 'created_utc' arguments to the payload as necessary."""
        if self._limited(payload):
            # Do nothing I guess? Not sure how paging works on this endpoint...
            return
        if 'limit' not in payload:
            payload['limit'] = self.max_results_per_request
        if 'filter' in payload: #and payload.get('created_utc', None) is None:
            if not isinstance(payload['filter'], list):
                if isinstance(payload['filter'], str):
                    payload['filter'] = [payload['filter']]
                else:
                    payload['filter'] = list(payload['filter'])
            if 'created_utc' not in payload['filter']:
                payload['filter'].append('created_utc')

    def _get(self, url, payload={}, endpoint='search'):
        i, success = 0, False
        while (not success) and (i<self.max_retries):
            self._impose_rate_limit(i)
            response = requests.get(url, params=payload)
            success = response.status_code == 200
            i+=1
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
            self._add_nec_args(self.payload)

            yield self._get(url, self.payload)

            if (limit is not None) & (limit == 0):
                return

    def _search(self,
                kind,
                stop_condition=lambda x: False,
                return_batch=False,
                **kwargs):
        self.payload = copy.deepcopy(kwargs)
        endpoint = 'reddit/{}/search'.format(kind)
        url = self.base_url.format(endpoint=endpoint)

        for response in self._handle_paging(url):
            results = response['data']
            if len(results) == 0:
                return
            if return_batch:
                #print("initializing batch after request")
                batch = []

            for thing in results:
                #n+=1
                thing = self._wrap_thing(thing, kind)

                if return_batch:
                    #print("appending to batch")
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
            self.payload['before'] = thing.created_utc

    def search_submissions(self, **kwargs):
        return self._search(kind='submission', **kwargs)

    def search_comments(self, **kwargs):
        return self._search(kind='comment', **kwargs)

    def get_submission_comment_ids(self, submission_id, **kwargs):
        self.payload = copy.deepcopy(kwargs)
        endpoint = 'reddit/submission/comment_ids'
        url = self.base_url.format(endpoint=endpoint)


class PushshiftAPI(PushshiftAPIMinimal):
    # Fill out this class with more user-friendly features later
    pass

class WithPraw(PushshiftAPIMinimal):
    def __init__(self, r, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.r = r

    def search_comments(self, **kwargs):
        return self._praw_search(kind='comment', **kwargs)

    def search_submissions(self, **kwargs):
        return self._praw_search(kind='submission', **kwargs)

    def _praw_search(self, **kwargs):
        prefix = self._thing_prefix[kwargs['kind'].title()]
        client_return_batch = kwargs.get('return_batch')
        if client_return_batch is False:
            kwargs.pop('return_batch')

        for batch in self._search(return_batch=True, **kwargs):
            fullnames = [prefix + thing.id for thing in batch]
            praw_batch = self.r.info(fullnames=fullnames)
            if client_return_batch:
                yield praw_batch
            else:
                for praw_thing in praw_batch:
                    yield praw_thing
