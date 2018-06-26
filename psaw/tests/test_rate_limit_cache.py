from unittest import TestCase, mock
from collections import deque
from ..rate_limit_cache import RateLimitCache


class TestRateLimitCache(TestCase):
    def test_init(self):
        test_params = [(1, 10), (3, 14), (23, 80), (15, 10), (99, 3)]

        for max_storage, interval_secs in test_params:
            cache = RateLimitCache(max_storage, interval_secs=interval_secs)
            self.assertIsInstance(cache.cache, deque)
            self.assertEqual(max_storage, cache.max_storage)
            self.assertEqual(interval_secs, cache.interval_secs)

    @mock.patch("time.time")
    def test_delta(self, mock_time):
        cache = RateLimitCache(5, interval_secs=30)

        # Should be 0 when the cache is empty
        self.assertEqual(0, cache.delta)

        start_time = 90812408.0
        end_time = 90822410.0

        mock_time.return_value = start_time
        cache.new()
        mock_time.return_value = end_time

        # Should be the difference between mock times
        self.assertEqual(end_time - start_time, cache.delta)

    @mock.patch("time.time")
    def test_update(self, mock_time):
        cache = RateLimitCache(5, interval_secs=30)
        cache.cache = deque(
            [
                90812408.0,
                90812412.0,
                90812414.0,
                90812419.0,
                90813408.0,
                90813412.0,
                90813414.0,
            ]
        )

        self.assertEqual(7, len(cache.cache))

        mock_time.return_value = 90813424.0
        cache.update()

        self.assertEqual(3, len(cache.cache))

        mock_time.return_value = 91813424.0
        cache.update()

        self.assertEqual(0, len(cache.cache))

    @mock.patch("time.time")
    def test_blocked(self, mock_time):
        cache = RateLimitCache(5, interval_secs=30)
        cache.cache = deque(
            [
                90812408.0,
                90812412.0,
                90812414.0,
                90812419.0,
                90813408.0,
                90813412.0,
                90813414.0,
            ]
        )

        mock_time.return_value = 90812421.0
        self.assertTrue(cache.blocked)

        mock_time.return_value = 90812444.0
        self.assertTrue(cache.blocked)

        mock_time.return_value = 90813408.0
        self.assertFalse(cache.blocked)

        mock_time.return_value = 90813440.0
        self.assertFalse(cache.blocked)

        mock_time.return_value = 91813440.0
        self.assertFalse(cache.blocked)

    @mock.patch(
        "psaw.rate_limit_cache.RateLimitCache.delta",
        new_callable=mock.PropertyMock,
        return_value=45,
    )
    def test_interval(self, _):
        self.assertEqual(0, RateLimitCache(5, interval_secs=30).interval)
        self.assertEqual(15, RateLimitCache(5, interval_secs=60).interval)
        self.assertEqual(30, RateLimitCache(5, interval_secs=75).interval)

    @mock.patch("time.time")
    def test_new(self, mock_time):
        cache = RateLimitCache(5, interval_secs=30)

        mock_time.return_value = 90812408.0
        cache.new()
        mock_time.return_value = 90812412.0
        cache.new()
        mock_time.return_value = 90812416.0
        cache.new()
        mock_time.return_value = 90812432.0
        cache.new()
        mock_time.return_value = 90812438.0
        cache.new()

        self.assertEqual(5, len(cache.cache))
        self.assertEqual(mock_time.return_value, cache.cache[-1])

        mock_time.return_value = 90822410.0
        cache.new()

        self.assertEqual(1, len(cache.cache))
        self.assertEqual(mock_time.return_value, cache.cache[-1])

        mock_time.return_value = 90822810.0
        cache.new()

        self.assertEqual(1, len(cache.cache))
        self.assertEqual(mock_time.return_value, cache.cache[-1])

    @mock.patch(
        "psaw.rate_limit_cache.RateLimitCache.blocked",
        new_callable=mock.PropertyMock,
        return_value=True,
    )
    def test_new_blocked(self, _):
        cache = RateLimitCache(5, interval_secs=30)

        try:
            cache.new()
            self.fail("Expected exception did not raise")

        # pylint: disable=broad-except
        except Exception as exc:
            self.assertIn("RateLimitCache is blocked", str(exc))
