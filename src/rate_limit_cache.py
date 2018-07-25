from collections import deque
import time


class RateLimitCache(object):
    def __init__(self, max_storage, interval_secs=60):
        self.max_storage = max_storage
        self.interval_secs = interval_secs
        self.cache = deque()

    @property
    def delta(self):
        """Time since earliest call"""
        if not self.cache:
            return 0
        return time.time() - self.cache[0]

    def update(self):
        while self.delta > self.interval_secs:
            try:
                self.cache.popleft()
            except IndexError:
                return

    @property
    def blocked(self):
        """Test if additional calls need to be blocked"""
        self.update()
        return len(self.cache) >= self.max_storage

    @property
    def interval(self):
        self.update()
        if self.interval_secs > self.delta:
            return self.interval_secs - self.delta

        return 0

    def new(self):
        self.update()
        if self.blocked:
            raise Exception("RateLimitCache is blocked.")
        self.cache.append(time.time())
