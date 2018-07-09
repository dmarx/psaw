import copy
from psaw.pushshift_api_minimal import PushshiftAPIMinimal


class PushshiftAPI(PushshiftAPIMinimal):
    # pylint: disable=keyword-arg-before-vararg
    def __init__(self, praw=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.praw = praw
        self._search_func = self._search
        if praw is not None:
            self._search_func = self._praw_search

    def search_comments(self, **kwargs):
        return self._search_func(kind="comment", **kwargs)

    def search_submissions(self, **kwargs):
        return self._search_func(kind="submission", **kwargs)

    def _get_submission_comment_ids(self, submission_id, **kwargs):
        payload = copy.deepcopy(kwargs)
        endpoint = "reddit/submission/comment_ids/{}".format(submission_id)
        url = self.base_url.format(endpoint=endpoint)
        return self._get(url, payload)["data"]

    def _praw_search(self, **kwargs):
        prefix = self._thing_prefix[kwargs["kind"].title()]
        payload = copy.deepcopy(kwargs)

        client_return_batch = kwargs.get("return_batch")
        if client_return_batch is False:
            payload.pop("return_batch")

        if "filter" in kwargs:
            payload.pop("filter")

        gen = self._search(return_batch=True, filter="id", **payload)
        using_gsci = False
        if kwargs.get("kind") == "comment" and payload.get("submission_id"):
            using_gsci = True
            gen = [self._get_submission_comment_ids(**kwargs)]

        for batch in gen:
            if using_gsci:
                fullnames = [prefix + base36id for base36id in batch]
            else:
                fullnames = [prefix + c.id for c in batch]
            praw_batch = self.praw.info(fullnames=fullnames)
            if client_return_batch:
                yield praw_batch
            else:
                for praw_thing in praw_batch:
                    yield praw_thing
