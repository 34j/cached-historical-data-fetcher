from __future__ import annotations

import asyncio
from typing import Any
from unittest import IsolatedAsyncioTestCase

import pandas as pd
from pandas import DataFrame, Timedelta, Timestamp
from pandas.testing import assert_frame_equal

from cached_historical_data_fetcher import (
    HistoricalDataCache,
    HistoricalDataCacheWithChunk,
    HistoricalDataCacheWithFixedChunk,
)

_INTERVAL = Timedelta(seconds=0.5)
_WAIT_SECONDS_MULTIPLIER = 2
_WAIT_SECONDS = _INTERVAL.total_seconds() * _WAIT_SECONDS_MULTIPLIER
_START_INIT = Timestamp.utcnow() - _INTERVAL * 5


class MyCache(HistoricalDataCache):
    count = -1
    interval: Timedelta = _INTERVAL
    add_interval: bool = False

    async def get(
        self, start: Timestamp | Any | None, *args: Any, **kwargs: Any
    ) -> DataFrame:
        if start is None:
            start = _START_INIT
        r = pd.date_range(start, Timestamp.utcnow(), freq=_INTERVAL)
        self.count += 1
        return DataFrame({"count": self.count}, index=r)


class MyCacheWithChunk(HistoricalDataCacheWithChunk):
    count = -1
    start_init: Timestamp = _START_INIT
    interval: Timedelta = _INTERVAL
    delay_seconds: float = 0

    async def get_one(self, start: Timestamp, *args: Any, **kwargs: Any) -> DataFrame:
        self.count += 1
        return DataFrame({"count": [self.count]}, index=[start])


class MyCacheWithFixedChunk(HistoricalDataCacheWithFixedChunk):
    count = -1
    start_init: Timestamp = _START_INIT
    interval: Timedelta = _INTERVAL
    delay_seconds: float = 0

    async def get_one(self, start: Timestamp, *args: Any, **kwargs: Any) -> DataFrame:
        self.count += 1
        return DataFrame({"count": [self.count]}, index=[start])


class TestCache(IsolatedAsyncioTestCase):
    async def test_cache(self):
        cache = MyCache()
        df = await cache.update(reload=True)

        # update immediately
        df2 = await cache.update()
        assert_frame_equal(df, df2)

        # update after interval
        await asyncio.sleep(_WAIT_SECONDS)
        with self.assertWarns(RuntimeWarning):
            df_new = await cache.update()
        self.assertEqual(df_new["count"].sum(), _WAIT_SECONDS_MULTIPLIER + 1)
        print(df, df_new)

    async def test_cache_keep_first(self):
        cache = MyCache()
        cache.keep = "first"
        df = await cache.update(reload=True)

        # update immediately
        df2 = await cache.update()
        assert_frame_equal(df, df2)

        # update after interval
        await asyncio.sleep(_WAIT_SECONDS)
        with self.assertWarns(RuntimeWarning):
            df_new = await cache.update()
        self.assertEqual(df_new["count"].sum(), _WAIT_SECONDS_MULTIPLIER)
        print(df, df_new)

    async def test_cache_with_chunk(self):
        cache = MyCacheWithChunk()
        df = await cache.update(reload=True)

        # update immediately
        df2 = await cache.update()
        assert_frame_equal(df, df2)

        # update after interval
        await asyncio.sleep(_WAIT_SECONDS)
        df_new = await cache.update()
        print(df, df_new)

    async def test_cache_with_fixed_chunk(self):
        cache = MyCacheWithFixedChunk()
        df = await cache.update(reload=True)

        # update immediately
        df2 = await cache.update()
        assert_frame_equal(df, df2)

        # update after interval
        await asyncio.sleep(_WAIT_SECONDS)
        df_new = await cache.update()
        print(df, df_new)

    async def test_docs_code(self):
        from pandas import DataFrame, Timedelta, Timestamp, date_range

        from cached_historical_data_fetcher import HistoricalDataCacheWithFixedChunk

        class MyCache(HistoricalDataCache):
            interval: Timedelta = Timedelta(days=1)

            async def get(
                self, start: Timestamp | None, *args: Any, **kwargs: Any
            ) -> DataFrame:
                date_range_chunk = date_range(start, Timestamp.utcnow(), freq="D")
                return DataFrame(
                    {"day": [d.day for d in date_range_chunk]}, index=date_range_chunk
                )

        df = await MyCache().update()
        print(df)

        class MyCacheWithChunk(HistoricalDataCacheWithChunk):
            delay_seconds: float = 0
            interval: Timedelta = Timedelta(days=1)
            start_init: Timestamp = Timestamp.utcnow().floor("10D")

            async def get_one(
                self, start: Timestamp, *args: Any, **kwargs: Any
            ) -> DataFrame:
                return DataFrame({"day": [start.day]}, index=[start])

        df = await MyCacheWithChunk().update()
        print(df)

        class MyCacheWithFixedChunk(HistoricalDataCacheWithFixedChunk):
            delay_seconds: float = 0
            interval: Timedelta = Timedelta(days=1)
            start_init: Timestamp = Timestamp.utcnow().floor("10D")

            async def get_one(
                self, start: Timestamp, *args: Any, **kwargs: Any
            ) -> DataFrame:
                return DataFrame({"day": [start.day]}, index=[start])

        df = await MyCacheWithFixedChunk().update()
        print(df)
