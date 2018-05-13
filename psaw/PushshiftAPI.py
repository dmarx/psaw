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
    base_url = {'search':'https://api.pushshift.io/reddit/{}/search/',
                'meta':'https://api.pushshift.io/meta/'}
    _limited_args = ('aggs')
    def __init__(self,
                 max_retries=20,
                 max_sleep=3600,
                 backoff=2,
                 rate_limit_per_minute=None,
                 max_results_per_request=500,
                 detect_local_tz=True,
                 utc_offset_secs=None
                ):
        assert max_results_per_request <= 500
        assert backoff >= 1

        self.max_retries = max_retries
        self.max_sleep   = max_sleep
        self.backoff     = backoff
        self.max_results_per_request = max_results_per_request

        self._utc_offset_secs = utc_offset_secs
        self._detect_local_tz = detect_local_tz

        if rate_limit_per_minute is None:
            rate_limit_per_minute = self._get(self.base_url['meta'])['server_ratelimit_per_minute']
        self._rlcache = RateLimitCache(n=rate_limit_per_minute, t=60)

    @property
    def utc_offset_secs(self):
        if self._utc_offset_secs is None:
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

    def _query(self, kind, stop_condition=lambda x: False, **kwargs):
        limit = kwargs.get('limit', None)
        payload = copy.deepcopy(kwargs)
        n = 0
        while True:
            if limit is not None:
                if limit > self.max_results_per_request:
                    payload['limit'] = self.max_results_per_request
                    limit -= self.max_results_per_request
                else:
                    payload['limit'] = limit
                    limit = 0
            self._add_nec_args(payload)
            url = self.base_url['search'].format(kind)
            results = self._get(url, payload)
            if self._limited(payload):
                yield results
                return

            results = results['data']
            if len(results) == 0:
                return
            for thing in results:
                n+=1
                thing = self._wrap_thing(thing, kind)
                yield thing
                if stop_condition(thing):
                    return
            payload['before'] = thing.created_utc
            if (limit is not None) & (limit == 0):
                return
    def search_submissions(self, **kwargs):
        return self._query(kind='submission', **kwargs)

    def search_comments(self, **kwargs):
        return self._query(kind='comment', **kwargs)

class PushshiftAPI(PushshiftAPIMinimal):
    # Fill out this class with more user-friendly features later
    pass
