from unittest import mock
from pushshift_py.tests.test_pushshift_api_minimal import TestPushshiftAPIMinimal
from pushshift_py import PushshiftAPI

# pylint: disable=too-many-public-methods
class TestPushshiftAPI(TestPushshiftAPIMinimal):

    # pylint: disable=protected-access
    def test_init(self):
        # Ensure everything gets passed through
        api = PushshiftAPI(**self._base_init_kwargs)
        self.assertEqual(api._search, api._search_func)
        self._test_base_init(api)

        # Check everything with praw provided
        praw = mock.MagicMock()
        api = PushshiftAPI(praw, **self._base_init_kwargs)
        self.assertEqual(praw, api.praw)
        self.assertEqual(api._praw_search, api._search_func)
        self._test_base_init(api)

    # pylint: disable=no-self-use
    @mock.patch("pushshift_py.PushshiftAPI._search")
    def test_search_comments(self, mock_search):
        kwargs = {
            "limit": 10,
            "sort": "desc",
            "sort_type": "score",
            "filter": "created_utc",
            "q": "test query",
        }

        api = PushshiftAPI(rate_limit_per_minute=self._rate_limit)
        api.search_comments(**kwargs)

        mock_search.assert_called_once_with(kind="comment", **kwargs)

    # pylint: disable=no-self-use
    @mock.patch("pushshift_py.PushshiftAPI._search")
    def test_search_submissions(self, mock_search):
        kwargs = {
            "limit": 10,
            "sort": "desc",
            "sort_type": "score",
            "filter": "created_utc",
            "q": "test query",
        }

        api = PushshiftAPI(rate_limit_per_minute=self._rate_limit)
        api.search_submissions(**kwargs)

        mock_search.assert_called_once_with(kind="submission", **kwargs)

    @mock.patch("pushshift_py.PushshiftAPI._get")
    def test_get_submission_comment_ids(self, mock_get):
        kwargs = {
            "limit": 10,
            "sort": "desc",
            "sort_type": "score",
            "filter": "created_utc",
            "q": "test query",
        }
        submission_id = "8irmhj"
        expected_result = ["dyu1k9y", "dyu1mg7"]
        mock_get.return_value = {"data": expected_result}
        expected_url = "https://testapi.pushshift.io/reddit/submission/comment_ids/{}".format(
            submission_id
        )

        api = PushshiftAPI(**self._base_init_kwargs)

        self.assertEqual(
            expected_result, api._get_submission_comment_ids(submission_id, **kwargs)
        )

        mock_get.assert_called_once_with(expected_url, kwargs)

    def test_praw_search(self):
        # TODO
        pass
