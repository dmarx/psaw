Python Pushshift.io API Wrapper (for comment/submission search)
===============================================================
.. image:: https://img.shields.io/badge/license-BSD--2-blue.svg
   :target: https://github.com/typenil/psaw/blob/master/LICENSE
   :alt: License
.. image:: https://travis-ci.org/typenil/psaw.svg?branch=master
   :target: https://travis-ci.org/typenil/psaw
   :alt: Build
.. image:: https://api.codeclimate.com/v1/badges/0ec8ed5374c3131a1459/maintainability
   :target: https://codeclimate.com/github/typenil/psaw/maintainability
   :alt: Maintainability
.. image:: https://api.codeclimate.com/v1/badges/0ec8ed5374c3131a1459/test_coverage
   :target: https://codeclimate.com/github/typenil/psaw/test_coverage
   :alt: Test Coverage



.. _installation:

Installation
------------

.. code-block:: bash

    pip install psaw

At present, only python 3 is supported.

Description
-----------

A minimalist wrapper for searching public reddit comments/submissions via the pushshift.io API.

Pushshift is an extremely useful resource, but the API is poorly documented. As such, this API wrapper
is currently designed to make it easy to pass pretty much any search parameter the user wants to try.

Although it is not necessarily reflective of the current status of the API, you should
attempt to familiarize yourself with the Pushshift API documentation to better understand
what search arguments are likely to work.

* `API Documentation on github <https://github.com/pushshift/api>`_
* `/r/pushshift <https://www.reddit.com/r/pushshift/>`_


Features
--------

* Handles rate limiting and exponential backoff subject to maximum retries and
  maximum backoff limits. A minimum rate limit of 1 request per second is used
  as a default per consultation with Pushshift's maintainer,
  `/u/Stuck_in_the_matrix <https://www.reddit.com/u/Stuck_in_the_matrix>`_.
* Handles paging of results. Returns all historical results for a given query by default.
* Optionally handles incorporation of ``praw`` to fetch objects after getting ids from pushshift
* If not using ``praw``, returns results in ``comment`` and ``submission`` objects whose
  API is similar to the corresponding ``praw`` objects. Additionally, result objects have
  an additional ``.d_`` attribute that offers dict access to the associated data attributes.
* Optionally adds a ``created`` attribute which converts a comment/submission's ``created_utc``
  timestamp to the user's local time. (may raise exceptions for users with certain timezone
  settings).
* Simple interface to pass query arguments to the API. The API is sparsely documented,
  so it's often fruitful to just try an argument and see if it works.
* Limited support for pushshift's ``aggs`` argument.
* A ``stop_condition`` argument to make it simple to stop yielding results given arbitrary user-defined criteria

WARNINGS
--------

* Using non-default sort may result in unexpected behavior.
* Default behavior is to continuously hit the pushshift api. If a query is taking
  longer than expected to return results, it's possible that psaw is pulling more data
  than you may want or is caught in some kind of loop.
* I strongly recommend prototyping queries by printing to stdout to ensure you're getting the 
  desired behavior.

Demo usage
----------

.. code-block:: python

    from psaw import PushshiftAPI

    api = PushshiftAPI()

Or to use pushshift search to fetch ids and then use praw to fetch objects:

.. code-block:: python

    import praw
    from psaw import PushshiftAPI

    r = praw.Reddit(...)
    api = PushshiftAPI(r)


100 most recent submissions
^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # The `search_comments` and `search_submissions` methods return generator objects
    gen = api.search_submissions(limit=100)
    results = list(gen)

First 10 submissions to /r/politics in 2017, filtering results to url/author/title/subreddit fields.
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``created_utc`` field will be added automatically (it's used for paging).

.. code-block:: python

    import datetime as dt

    start_epoch=int(dt.datetime(2017, 1, 1).timestamp())

    list(api.search_submissions(after=start_epoch,
                                subreddit='politics',
                                filter=['url','author', 'title', 'subreddit'],
                                limit=10))

Trying a search argument that doesn't actually work
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

According to the pushshift.io API documentation, we should be able to search submissions by url,
but (at the time of this writing) this doesn't actually work in practice.
The API should still respect the ``limit`` argument and possibly other supported arguments,
but no guarantees. If you find that an argument you have passed is not supported by the API,
best thing is to just remove it from the query and modify your api call to only utilize
supported arguments to mitigate risks from of unexpected behavior.

.. code-block:: python

    url = 'http://www.politico.com/story/2017/02/mike-flynn-russia-ties-investigation-235272'
    url_results = list(api.search_submissions(url=url, limit=500))

    len(url_results), any(r.url == url for r in url_results)
    # 500, False

All AskReddit comments containing the text "OP"
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Use the ``q`` parameter to search text. Omitting the ``limit`` parameter does a full
historical search. Requests are performed in batches of size specified by the
``max_results_per_request`` parameter (default=500). Omitting the "max_reponse_cache"
test in the demo below will return all results. Otherwise, this demo will perform two
API requests returning 500 comments each. Alternatively, the generator can be queried for additional results.

.. code-block:: python

    gen = api.search_comments(q='OP', subreddit='askreddit')

    max_response_cache = 1000
    cache = []

    for c in gen:
        cache.append(c)

        # Omit this test to actually return all results. Wouldn't recommend it though: could take a while, but you do you.
        if len(cache) >= max_response_cache:
            break

    # If you really want to: pick up where we left off to get the rest of the results.
    if False:
        for c in gen:
            cache.append(c)

Using the ``aggs`` argument to count comments mentioning trump each hour in past week
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Replicating the example from the pushshift documentation:

https://api.pushshift.io/reddit/search/comment/?q=trump&after=7d&aggs=created_utc&frequency=hour&size=0

I haven't really experimented much with this functionality of the API, so I figured
the simplest way to support it would be to just disable most of the bells and whistles
provided by the API wrapper when the ``aggs`` argument is provided (i.e. paging, converting
the result to a namedtuple for dot notation attribute access).

.. code-block:: python

    api = PushshiftAPI()
    gen = api.search_comments(q='trump',
                              after='7d',
                              aggs='created_utc',
                              frequency='hour',
                              size=0,
                             )

    result = next(gen)

Using the ``stop_condition`` argument to get the most recent submission by a bot account
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    gen = api.search_submissions(stop_condition=lambda x: 'bot' in x.author)

    for subm in gen:
        pass

    print(subm.author)


License
-------

PSAW's source is provided under the `Simplified BSD License
<https://github.com/dmarx/psaw/master/LICENSE>`_.

* Copyright (c), 2018, David Marx
