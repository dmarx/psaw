Change Log
==========

0.0.10 (...)
------------
* Added metadata fetching.

0.0.9 (2020/03/08)
------------------
* Added (contributed) CLI functionality. Needs README documentation.

0.0.8 (2020/02/29)
------------------
* Fixed infinite loop when requesting objects by ID.
* More descriptive warnings on unsuccessful GET requests (status codes).
* Minor documentation fix.

0.0.7 (2018/08/13)
------------------
* Add `redditor_subreddit_activity` convenience method
* Fixed bug that blocked backoff on request issued to determine current rate limit when
  a new psaw instance is created.
* Fixed bug when using praw to get submission comments and no results returned.

0.0.6 (2018/08/06)
------------------
* Fixed `aggs` support.
  * If an aggs parameter is provided to a search method, it will be returned as the first result.
  * Subsequent results will be yielded as though there was no aggs parameter.
  * To the best of my understanding, PushShift only returns 100 results per agg, and there is not
    way to page for additional results.

0.0.5 (2018/08/05)
------------------
* New parameter documentation added to README.
* Fixed bug with non-default sort.
* Fixed bug with using praw.Reddit instance with submission search.

0.0.4 (2018/05/18)
------------------

* Updates to README

0.0.3 (2018/05/18)
------------------

* Added praw support. If ``praw.Reddit`` object provided to ``PushshiftAPI``,
  gets ids from pushshift and passes them to praw.
* Added support for ``/reddit/submission/comment_ids/`` endpoint.
* Added change log.

0.0.2 (2018/05/13)
------------------

* Improved rate limit handling
* Misc bug fixes.

0.0.1 (2018/04/14)
------------------

* Dirty support for ``reddit/comment/search`` and ``reddit/comment/search``.
