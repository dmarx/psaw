from unittest import mock, TestCase
import os
import time
import json
from datetime import datetime as dt
from requests.exceptions import HTTPError
import pytz
from psaw.pushshift_api_minimal import PushshiftAPIMinimal
from psaw.tests.mock_response import MockResponse


class TestPushshiftAPIMinimal(TestCase):
    pushshift_args = [
        "sort",
        "sort_type",
        "after",
        "before",
        "after_id",
        "before_id",
        "created_utc",
        "score",
        "gilded",
        "edited",
        "author",
        "subreddit",
        "distinguished",
        "retrieved_on",
        "last_updated",
        "q",
        "id",
        "metadata",
        "unique",
        "pretty",
        "html_decode",
        "permalink",
        "user_removed",
        "mod_removed",
        "subreddit_type",
        "author_flair_css_class",
        "author_flair_text",
        "reply_delay",
        "nest_level",
        "sub_reply_delay",
        "utc_hour_of_week",
        "link_id",
        "parent_id",
        "over_",
        "locked",
        "spoiler",
        "is_video",
        "is_self",
        "is_original_content",
        "is_reddit_media_domain",
        "whitelist_status",
        "parent_whitelist_status",
        "is_crosspostable",
        "can_gild",
        "suggested_sort",
        "no_follow",
        "send_replies",
        "link_flair_css_class",
        "link_flair_text",
        "num_crossposts",
        "title",
        "selftext",
        "quarantine",
        "pinned",
        "stickied",
        "category",
        "contest_mode",
        "subreddit_subscribers",
        "url",
        "domain",
        "thumbnail",
        "description",
        "public_description",
        "title",
        "header_title",
        "submit_text",
        "subscribers",
        "comment_score_hide_mins",
        "suggested_comment_sort",
        "submission_type",
        "spoilers_enabled",
        "lang",
        "is_enrolled_in_new_modmail",
        "audience_target",
        "allow_videos",
        "allow_images",
        "allow_videogifs",
        "advertiser_category",
        "hide_ad",
        "subreddit_type",
        "wiki_enabled",
        "user_sr_theme_enabled",
        "whitelist_status",
        "submit_link_label",
        "show_media_preview",
    ]

    # pylint: disable=protected-access
    def test_init(self):
        api = PushshiftAPIMinimal(
            max_retries=27,
            max_sleep=2390,
            backoff=7,
            rate_limit_per_minute=123,
            max_results_per_request=500,
            detect_local_tz=False,
            utc_offset_secs=11,
            domain="testapi",
        )

        self.assertEqual(27, api.max_retries)
        self.assertEqual(2390, api.max_sleep)
        self.assertEqual(7, api.backoff)
        self.assertEqual(500, api.max_results_per_request)
        self.assertEqual("testapi", api.domain)

        self.assertEqual(False, api._detect_local_tz)
        self.assertEqual(11, api._utc_offset_secs)

        self.assertEqual(123, api._rlcache.max_storage)

    @mock.patch("psaw.pushshift_api_minimal.PushshiftAPIMinimal._get")
    def test_init_none_rate_limit(self, mock_get):
        mock_get.return_value = {"server_ratelimit_per_minute": 420}
        api = PushshiftAPIMinimal(rate_limit_per_minute=None)
        self.assertEqual(420, api._rlcache.max_storage)

    def test_base_url(self):
        api = PushshiftAPIMinimal(domain="test-domain", rate_limit_per_minute=77)
        self.assertEqual("https://test-domain.pushshift.io/{endpoint}", api.base_url)

    def test_utc_offset_secs(self):
        api = PushshiftAPIMinimal(detect_local_tz=False)
        self.assertEqual(0, api.utc_offset_secs)

        api = PushshiftAPIMinimal(detect_local_tz=True)
        for timezone in pytz.common_timezones:
            api._utc_offset_secs = None
            os.environ["TZ"] = timezone
            time.tzset()

            expected_secs = dt.utcnow().astimezone().utcoffset().total_seconds()
            actual_secs = api.utc_offset_secs

            self.assertEqual(expected_secs, actual_secs)

    def test_limited(self):
        # Test all of the arguments listed at
        # https://pushshift.io/api-parameters/
        for arg in self.pushshift_args:
            self.assertFalse(PushshiftAPIMinimal._limited({arg: True}))

        # Test the limited arguments
        for arg in PushshiftAPIMinimal._limited_args:
            self.assertTrue(PushshiftAPIMinimal._limited({arg: True}))

    def test_epoch_utc_to_local(self):
        timestamps = [
            1429981843,
            1519981843,
            1528981843,
            1529781843,
            1529881843,
            1529931843,
            1529981843,
        ]

        for timestamp in timestamps:
            api = PushshiftAPIMinimal(detect_local_tz=False)
            self.assertEqual(timestamp, api._epoch_utc_to_local(timestamp))

            api = PushshiftAPIMinimal(detect_local_tz=True)
            for timezone in pytz.common_timezones:
                api._utc_offset_secs = None
                os.environ["TZ"] = timezone
                time.tzset()

                expected_secs = (
                    timestamp - dt.utcnow().astimezone().utcoffset().total_seconds()
                )
                actual_secs = api._epoch_utc_to_local(timestamp)

                self.assertEqual(expected_secs, actual_secs)

    def test_wrap_thing(self):
        test_data = {
            "created_utc": dt.utcnow().timestamp(),
            "some": 12,
            "arbitrary": True,
            "Set": "of random",
            "keys": "to",
            "test": 15.0,
        }

        kind = "TestKind"

        api = PushshiftAPIMinimal(detect_local_tz=False)
        wrapped = api._wrap_thing(test_data, kind)

        self.assertIn(kind, str(wrapped))
        self.assertEqual(test_data["created_utc"], wrapped.created)

        self.assertDictEqual(test_data, wrapped.d_)

        for key, val in test_data.items():
            self.assertEqual(val, getattr(wrapped, key))

    # pylint: disable=no-self-use
    @mock.patch(
        "psaw.rate_limit_cache.RateLimitCache.interval", new_callable=mock.PropertyMock
    )
    @mock.patch(
        "psaw.rate_limit_cache.RateLimitCache.blocked", new_callable=mock.PropertyMock
    )
    @mock.patch("time.sleep")
    def test_impose_rate_limit(self, mock_sleep, mock_blocked, mock_interval):
        max_sleep = 69
        backoff = 11
        api = PushshiftAPIMinimal(max_sleep=max_sleep, backoff=backoff)

        mock_blocked.return_value = False
        mock_interval.return_value = 13

        api._impose_rate_limit()
        mock_sleep.assert_not_called()

        mock_blocked.return_value = True

        api._impose_rate_limit()
        mock_sleep.assert_called_with(13)

        mock_interval.return_value = 87

        api._impose_rate_limit()
        mock_sleep.assert_called_with(max_sleep)

        mock_interval.return_value = 0

        api._impose_rate_limit(6)
        mock_sleep.assert_called_with(6 * backoff)

    def test_add_nec_args(self):
        max_results_per_request = 127
        api = PushshiftAPIMinimal(max_results_per_request=max_results_per_request)

        expected_payload = {key: True for key in PushshiftAPIMinimal._limited_args}

        # Ensure limited calls aren't altered
        self.assertDictEqual(
            expected_payload,
            api._add_nec_args({key: True for key in PushshiftAPIMinimal._limited_args}),
        )

        # Ensure limit is added as expected
        self.assertDictEqual(
            {"arbitrary": "value", "limit": max_results_per_request},
            api._add_nec_args({"arbitrary": "value"}),
        )

        # Ensure created_utc is appended to filter
        self.assertDictEqual(
            {
                "more_arbitrary": "more_value",
                "limit": max_results_per_request,
                "filter": ["created_utc"],
            },
            api._add_nec_args({"more_arbitrary": "more_value", "filter": []}),
        )

        # Ensure string filter turned to list
        self.assertDictEqual(
            {
                "more_arbitrary": "more_value",
                "limit": max_results_per_request,
                "filter": ["some_string", "created_utc"],
            },
            api._add_nec_args(
                {"more_arbitrary": "more_value", "filter": "some_string"}
            ),
        )

        # Ensure iterable-but-not-list filter turned to list
        self.assertDictEqual(
            {
                "more_arbitrary": "more_value",
                "limit": max_results_per_request,
                "filter": [0, 1, 2, "created_utc"],
            },
            api._add_nec_args(
                {"more_arbitrary": "more_value", "filter": set(x for x in range(0, 3))}
            ),
        )

        # Ensure "created_utc" string filter turned to list
        self.assertDictEqual(
            {
                "more_arbitrary": "more_value",
                "limit": max_results_per_request,
                "filter": ["created_utc"],
            },
            api._add_nec_args(
                {"more_arbitrary": "more_value", "filter": "created_utc"}
            ),
        )

    @mock.patch("psaw.pushshift_api_minimal.PushshiftAPIMinimal._impose_rate_limit")
    @mock.patch("psaw.pushshift_api_minimal.requests.get")
    def test_get(self, mock_get, mock_rate_limit):
        max_retries = 7
        expected_result = "test_text"
        test_url = "example.com/route"

        api = PushshiftAPIMinimal(max_retries=max_retries, rate_limit_per_minute=60)

        mock_get.return_value = MockResponse(
            status_code=200, text=json.dumps(expected_result)
        )

        self.assertEqual(expected_result, api._get(test_url))

        # Ensure the correct count of retries triggered
        mock_get.assert_called_with(test_url, params={})
        self.assertEqual(1, mock_get.call_count)

        # Ensure the rate limit was applied
        self.assertEqual(1, mock_rate_limit.call_count)
        mock_rate_limit.assert_has_calls([mock.call(0)])

        mock_get.return_value.raise_for_status.assert_called_once()

    @mock.patch("psaw.pushshift_api_minimal.PushshiftAPIMinimal._impose_rate_limit")
    @mock.patch("psaw.pushshift_api_minimal.requests.get")
    def test_get_429(self, mock_get, mock_rate_limit):
        max_retries = 7
        expected_result = "test_text"
        test_url = "example.com/route"

        api = PushshiftAPIMinimal(max_retries=max_retries, rate_limit_per_minute=60)

        mock_get.return_value = MockResponse(
            status_code=429, text=json.dumps(expected_result)
        )

        self.assertEqual(expected_result, api._get(test_url))

        # Ensure the correct count of retries triggered
        mock_get.assert_called_with(test_url, params={})
        self.assertEqual(max_retries, mock_get.call_count)

        # Ensure the rate limit was applied
        self.assertEqual(max_retries, mock_rate_limit.call_count)
        mock_rate_limit.assert_has_calls(
            [mock.call(idx) for idx in range(0, max_retries)]
        )

        # This is the key difference with code 429
        mock_get.return_value.raise_for_status.assert_not_called()

    @mock.patch("psaw.pushshift_api_minimal.PushshiftAPIMinimal._impose_rate_limit")
    @mock.patch("psaw.pushshift_api_minimal.requests.get")
    def test_get_raise_for_status(self, mock_get, mock_rate_limit):
        max_retries = 7
        expected_result = "test_text"
        test_url = "example.com/route"

        api = PushshiftAPIMinimal(max_retries=max_retries, rate_limit_per_minute=60)

        # Test a subset of codes that should cause an outright failure
        for idx, status_code in enumerate(
            [400, 401, 403, 404, 405, 500, 502, 503, 504]
        ):
            mock_get.return_value = MockResponse(
                status_code=status_code, text=json.dumps(expected_result)
            )

            try:
                api._get(test_url)
                self.fail("call failed to trigger expected exception")
            except HTTPError as exc:
                self.assertIn(
                    "{} {} Error".format(
                        status_code, "Server" if status_code >= 500 else "Client"
                    ),
                    str(exc),
                )

            expected_calls = max_retries * (idx + 1)

            # Ensure the correct count of retries triggered
            mock_get.assert_called_with(test_url, params={})
            self.assertEqual(expected_calls, mock_get.call_count)

            # Ensure the rate limit was applied
            self.assertEqual(expected_calls, mock_rate_limit.call_count)
            mock_rate_limit.assert_has_calls(
                [mock.call(idx) for idx in range(0, max_retries)]
            )

            mock_get.return_value.raise_for_status.assert_called_once()

    def test_handle_paging(self):
        # TODO
        pass

    def test_search(self):
        # TODO
        pass
