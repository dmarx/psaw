from collections import namedtuple
import copy
import json
import requests
import time
from datetime import datetime as dt

class PushshiftAPIMinimal(object):
    base_url = 'https://api.pushshift.io/reddit/{}/search/'
    _limited_args = ('aggs')
    def __init__(self,
                 max_retries=20,
                 max_sleep=3600,
                 backoff=2,
                 rate_limit=1,
                 max_results_per_request=500
                ):
        assert rate_limit >=1
        assert max_results_per_request <= 500
        assert backoff >= 1

        self.max_retries = max_retries
        self.max_sleep   = max_sleep
        self.backoff     = backoff
        self.rate_limit  = rate_limit
        self.max_results_per_request = max_results_per_request
        self._last_request_time = 0
        self._utc_offset_secs = None

    @property
    def utc_offset_secs(self):
        if not self._utc_offset_secs:
            #self._utc_offset_secs = dt.datetime.utcnow().astimezone().utcoffset().total_seconds()
            self._utc_offset_secs =  dt.utcnow().astimezone().utcoffset().total_seconds()
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

    def _rate_limit(self, nth_request=0):
        d = time.time() - self._last_request_time
        interval = max(self.rate_limit, self.backoff*nth_request)
        interval = min(interval, self.max_sleep)
        if d < interval:
            time.sleep(interval-d)
        self._last_request_time = time.time()

    def _add_nec_args(self, payload):
        #if 'aggs' in payload:
        if self._limited(payload):
            # Do nothing I guess? Not sure how paging works on this endpoint...
            return
        if 'limit' not in payload:
            payload['limit'] = self.max_results_per_request
        if 'filter' in payload and payload.get('created_utc', None) is None:
            if not isinstance(payload['filter'], list):
                payload['filter'] = list(payload['filter'])
            payload['filter'].append('created_utc')

    def _get(self, kind, payload):
        self._add_nec_args(payload)
        url = self.base_url.format(kind)
        i, success = 0, False
        while (not success) and (i<self.max_retries):
            self._rate_limit(i)
            response = requests.get(url, params=payload)
            success = response.status_code == 200
            i+=1
        response_json = json.loads(response.text)
        outv = response_json['data']
        #if 'aggs' in payload:
        if self._limited(payload):
            outv = response_json
        return outv

    def _query(self, kind, stop_condition=lambda **x: False, **kwargs):
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

            results = self._get(kind, payload)
            #if 'aggs' in payload:
            if self._limited(payload):
                yield results
                return

            if len(results) == 0:
                return
            for thing in results:
                n+=1
                if stop_condition(**thing):
                    return
                thing = self._wrap_thing(thing, kind)
                yield thing
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
