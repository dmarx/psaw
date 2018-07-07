import copy
import time
import json
from collections import namedtuple
import requests
from psaw.rate_limit_cache import RateLimitCache

# pylint: disable=too-many-instance-attributes
class PushshiftAPIMinimal(object):
    # base_url = {'search':'https://api.pushshift.io/reddit/{}/search/',
    #            'meta':'https://api.pushshift.io/meta/'}
    _base_url = "https://{domain}.pushshift.io/{{endpoint}}"
    _limited_args = "aggs"
    _thing_prefix = {
        "Comment": "t1_",
        "Account": "t2_",
        "Link": "t3_",
        "Message": "t4_",
        "Subreddit": "t5_",
        "Award": "t6_",
    }

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        max_retries=20,
        max_sleep=3600,
        backoff=2,
        rate_limit_per_minute=None,
        max_results_per_request=500,
        detect_local_tz=True,
        utc_offset_secs=None,
        domain="apiv2",
    ):
        assert max_results_per_request <= 500
        assert backoff >= 1

        self.max_retries = max_retries
        self.max_sleep = max_sleep
        self.backoff = backoff
        self.max_results_per_request = max_results_per_request

        self._utc_offset_secs = utc_offset_secs
        self._detect_local_tz = detect_local_tz

        self.domain = domain
        self._after_id = None

        if rate_limit_per_minute is None:
            response = self._get(self.base_url.format(endpoint="meta"))
            rate_limit_per_minute = response["server_ratelimit_per_minute"]

        self._rlcache = RateLimitCache(
            max_storage=rate_limit_per_minute, interval_secs=60
        )

    @property
    def base_url(self):
        return self._base_url.format(domain=self.domain)

    @property
    def utc_offset_secs(self):
        if self._utc_offset_secs is None:
            self._utc_offset_secs = 0

            if self._detect_local_tz:
                self._utc_offset_secs = time.localtime().tm_gmtoff

        return self._utc_offset_secs

    @classmethod
    def _limited(cls, payload):
        """Turn off bells and whistles for special API endpoints"""
        return any(arg in payload for arg in cls._limited_args)

    def _epoch_utc_to_local(self, epoch):
        return epoch - self.utc_offset_secs

    def _wrap_thing(self, thing, kind):
        """Mimic praw.Submission and praw.Comment API"""
        # Avoid altering the given input
        thing = copy.deepcopy(thing)

        thing["d_"] = copy.deepcopy(thing)
        thing["created"] = self._epoch_utc_to_local(thing["created_utc"])
        thing_type = namedtuple(kind, thing.keys())
        thing = thing_type(**thing)
        return thing

    def _impose_rate_limit(self, nth_request=0):
        if not hasattr(self, "_rlcache"):
            return

        interval = 0
        if self._rlcache.blocked:
            interval = self._rlcache.interval

        interval = max(interval, self.backoff * nth_request)
        interval = min(interval, self.max_sleep)

        if interval > 0:
            time.sleep(interval)

    def _add_nec_args(self, payload):
        """Adds 'limit' and 'created_utc' arguments to the payload as necessary."""
        payload = copy.deepcopy(payload)

        # Do nothing when limited I guess?
        # Not sure how paging works on this endpoint...
        if not self._limited(payload):
            if "limit" not in payload:
                payload["limit"] = self.max_results_per_request
            if "filter" in payload:  # and payload.get('created_utc', None) is None:
                if not isinstance(payload["filter"], list):
                    if isinstance(payload["filter"], str):
                        payload["filter"] = [payload["filter"]]
                    payload["filter"] = list(payload["filter"])
                if "created_utc" not in payload["filter"]:
                    payload["filter"].append("created_utc")

        return payload

    def _get(self, url, payload=None):
        if not payload:
            # See https://stackoverflow.com/q/26320899/9970453
            # for why we don't set payload={} in the signature.
            payload = {}

        i, complete = 0, False
        while (not complete) and (i < self.max_retries):
            self._impose_rate_limit(i)
            response = requests.get(url, params=payload)

            complete = response.status_code == 200
            i += 1

        # We omit 429 from raise_for_status because it's a rate limit code
        # 429 should resolve after some period of time
        if response.status_code != 429:
            # In case we hit an error that didn't resolve on retries
            response.raise_for_status()

        return json.loads(response.text)

    def _handle_paging(self, url, payload):
        limit = payload.get("limit", None)

        # Default limit value
        payload["limit"] = self.max_results_per_request
        # Transforms filter format
        payload = self._add_nec_args(payload)

        # If no limit is provided, the loop continues indefinitely
        while limit is None or limit > 0:
            if limit is not None:
                if limit > self.max_results_per_request:
                    limit -= self.max_results_per_request
                else:
                    payload["limit"] = limit
                    limit = 0

            if self._after_id is not None:
                payload["after_id"] = self._after_id

            results = self._get(url, payload)

            # Set the latest retrieved id, if it exists
            if "data" in results and results["data"]:
                self._after_id = results["data"][-1].get("id", self._after_id)

            yield results

    def _search(
        self, kind, stop_condition=lambda x: False, return_batch=False, **kwargs
    ):
        payload = copy.deepcopy(kwargs)
        endpoint = "reddit/{}/search".format(kind)
        url = self.base_url.format(endpoint=endpoint)

        for response in self._handle_paging(url, payload):
            results = response["data"]
            if not results:
                return
            if return_batch:
                batch = []

            for thing in results:
                thing = self._wrap_thing(thing, kind)

                if stop_condition(thing):
                    if return_batch:
                        yield batch
                    return

                if return_batch:
                    batch.append(thing)
                else:
                    yield thing

            if return_batch:
                yield batch
