from asyncio import Future, all_tasks

import asynctest

from python_project.requestcache import RandomNumberCache, RequestCache

CACHE_TIMEOUT = 0.01


class MockCache(RandomNumberCache):
    def __init__(self, request_cache: RequestCache) -> None:
        super(MockCache, self).__init__(request_cache, u"mock")
        self.timed_out = Future()

    @property
    def timeout_delay(self) -> float:
        return CACHE_TIMEOUT

    def on_timeout(self) -> None:
        self.timed_out.set_result(None)


class MockRegisteredCache(RandomNumberCache):
    def __init__(self, request_cache: RequestCache) -> None:
        super(MockRegisteredCache, self).__init__(request_cache, u"mock")
        self.timed_out = Future()
        self.register_future(self.timed_out)

    @property
    def timeout_delay(self) -> float:
        return CACHE_TIMEOUT

    def on_timeout(self) -> None:
        pass


class TestRequestCache(asynctest.TestCase):
    async def test_shutdown(self) -> None:
        """
        Test if RequestCache does not allow new Caches after shutdown().
        """
        num_tasks = len(all_tasks())
        request_cache = RequestCache()
        request_cache.add(MockCache(request_cache))
        self.assertEqual(len(all_tasks()), num_tasks + 2)
        await request_cache.shutdown()
        self.assertEqual(len(all_tasks()), num_tasks)
        request_cache.add(MockCache(request_cache))
        self.assertEqual(len(all_tasks()), num_tasks)

    async def test_timeout(self) -> None:
        """
        Test if the cache.on_timeout() is called after the cache.timeout_delay.
        """
        request_cache = RequestCache()
        cache = MockCache(request_cache)
        request_cache.add(cache)
        await cache.timed_out

    async def test_add_duplicate(self) -> None:
        """
        Test if adding a cache twice returns None as the newly added cache.
        """
        request_cache = RequestCache()
        cache = MockCache(request_cache)
        request_cache.add(cache)

        self.assertIsNone(request_cache.add(cache))

        await request_cache.shutdown()

    async def test_timeout_future_default_value(self) -> None:
        """
        Test if a registered future gets set to None on timeout.
        """
        request_cache = RequestCache()
        cache = MockRegisteredCache(request_cache)
        request_cache.add(cache)
        self.assertEqual(None, (await cache.timed_out))
        await request_cache.shutdown()

    async def test_timeout_future_custom_value(self) -> None:
        """
        Test if a registered future gets set to a value on timeout.
        """
        request_cache = RequestCache()
        cache = MockRegisteredCache(request_cache)
        request_cache.add(cache)

        cache.managed_futures[0] = (cache.managed_futures[0][0], 123)
        self.assertEqual(123, (await cache.timed_out))

        await request_cache.shutdown()

    async def test_timeout_future_exception(self) -> None:
        """
        Test if a registered future raises an exception on timeout.
        """
        request_cache = RequestCache()
        cache = MockRegisteredCache(request_cache)
        request_cache.add(cache)

        cache.managed_futures[0] = (cache.managed_futures[0][0], RuntimeError())
        with self.assertRaises(RuntimeError):
            await cache.timed_out

        await request_cache.shutdown()

    async def test_cancel_future(self) -> None:
        """
        Test if a registered future gets canceled at shutdown.
        """
        request_cache = RequestCache()
        cache = MockRegisteredCache(request_cache)
        request_cache.add(cache)
        await request_cache.shutdown()
        self.assertTrue(cache.timed_out.cancelled())
