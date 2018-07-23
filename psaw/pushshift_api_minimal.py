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

    # TODO evaluate which params work with aggregates
    _limited_args = "aggs"
    _thing_prefix = {
        "Comment": "t1_",
        "Account": "t2_",
        "Link": "t3_",
        "Message": "t4_",
        "Subreddit": "t5_",
        "Award": "t6_",
    }
    _page_error_msg = "Paging is only supported for sort_type == created_utc."

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
        self._last_timestamp = None

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

        # We omit 429 from raise_for_status because it's a rate limit code.
        # 429 should resolve after some period of time.
        if response.status_code != 429:
            # In case we hit an error that didn't resolve on retries
            response.raise_for_status()

        return json.loads(response.text)

    def _raise_for_unpageable(self, payload):
        sort_type = payload.get("sort_type", None)

        # Currently, the only way to paginate results is by date
        if sort_type and sort_type != "created_utc":
            limit = payload.get("limit", None)

            if not limit:
                raise NotImplementedError(
                    "{}\n{}".format(
                        self._page_error_msg,
                        "You must provide a limit to run this query.",
                    )
                )
            if limit > self.max_results_per_request:
                raise NotImplementedError(
                    "{}\n{}".format(
                        self._page_error_msg,
                        "Non-default sort queries require limit <= max_results_per_request",
                    )
                )

    def _apply_timestamp(self, payload):
        # NOTE See the Pushshift maintainer's comment here https://bit.ly/2NyhPUN
        # He asserts that timestamp has been "fixed" so that a call will always
        # return everything within an epoch second, so we don't have to subtract a
        # second to get everything.
        payload = copy.deepcopy(payload)

        if not self._last_timestamp:
            return payload

        sort = payload.get("sort", "desc")
        if sort == "desc":
            payload["before"] = self._last_timestamp
        else:
            payload["after"] = self._last_timestamp

        return payload

    def _handle_paging(self, url, payload):

        # Raise an exception if the request will not return all data
        self._raise_for_unpageable(payload)

        # Original limit
        limit = payload.get("limit", None)

        # Default limit
        payload["limit"] = self.max_results_per_request

        # Transforms filter format
        payload = self._add_nec_args(payload)

        # If no limit is provided, the loop continues indefinitely
        while limit is None or limit > 0:
            if limit is not None:
                # NOTE limit cannot be relied on to strictly limit the result count.
                # This comment (https://bit.ly/2NyhPUN) indicates that a batch will
                # contain more than the limit if the final result has multiple comments
                # with the same utc_created time.
                if limit > self.max_results_per_request:
                    limit -= self.max_results_per_request
                else:
                    payload["limit"] = limit
                    limit = 0

            payload = self._apply_timestamp(payload)
            results = self._get(url, payload)

            # Set the latest retrieved timestamp, if it exists
            if "data" in results and results["data"]:
                # Track backwards through the data until we hit a timestamp
                for idx in range(len(results["data"]) - 1, -1, -1):
                    timestamp = results["data"][idx].get("created_utc", None)

                    if timestamp:
                        self._last_timestamp = timestamp
                        break

            yield results

    def _search(
        self, kind, stop_condition=lambda x: False, return_batch=False, **kwargs
    ):
        # Reset timestamp data with every request
        self._last_timestamp = None

        payload = copy.deepcopy(kwargs)
        endpoint = "reddit/{}/search".format(kind)
        url = self.base_url.format(endpoint=endpoint)

        for response in self._handle_paging(url, payload):
            results = response["data"]
            if not results:
                return

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
