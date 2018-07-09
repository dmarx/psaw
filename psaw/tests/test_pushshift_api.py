from unittest import mock
from psaw.tests.test_pushshift_api_minimal import TestPushshiftAPIMinimal
from psaw.pushshift_api import PushshiftAPI

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
    @mock.patch("psaw.pushshift_api.PushshiftAPI._search")
    def test_search_comments(self, mock_search):
        kwargs = {
            "limit": 10,
            "sort": "desc",
            "sort_type": "score",
            "filter": "created_utc",
            "q": "test query",
        }

        api = PushshiftAPI(rate_limit_per_minute=77)
        api.search_comments(**kwargs)

        mock_search.assert_called_once_with(kind="comment", **kwargs)

    # pylint: disable=no-self-use
    @mock.patch("psaw.pushshift_api.PushshiftAPI._search")
    def test_search_submissions(self, mock_search):
        kwargs = {
            "limit": 10,
            "sort": "desc",
            "sort_type": "score",
            "filter": "created_utc",
            "q": "test query",
        }

        api = PushshiftAPI(rate_limit_per_minute=77)
        api.search_submissions(**kwargs)

        mock_search.assert_called_once_with(kind="submission", **kwargs)

    def test_praw_search(self):
        # TODO
        pass
