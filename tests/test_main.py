from __future__ import annotations

import asyncio
from typing import Any
from unittest import IsolatedAsyncioTestCase

import pandas as pd
from pandas import DataFrame, Timedelta, Timestamp
from pandas.testing import assert_frame_equal
from parameterized import parameterized_class

from cached_historical_data_fetcher import (
    HistoricalDataCache,
    HistoricalDataCacheWithChunk,
    HistoricalDataCacheWithFixedChunk,
    IdCacheWithFixedChunk,
)

_INTERVAL = Timedelta(seconds=1)
_WAIT_SECONDS_MULTIPLIER = 2
_WAIT_SECONDS = _INTERVAL.total_seconds() * _WAIT_SECONDS_MULTIPLIER
_start_index = Timestamp.utcnow() - _INTERVAL * 5


class MyCache(HistoricalDataCache[Timestamp, Timedelta, Any]):
    count = -1
    interval: Timedelta = _INTERVAL
    add_interval_to_start_index: bool = False

    async def get(
        self, start: Timestamp | Any | None, *args: Any, **kwargs: Any
    ) -> DataFrame:
        if start is None:
            start = _start_index
        r = pd.date_range(start, Timestamp.utcnow(), freq=_INTERVAL)
        self.count += 1
        return DataFrame({"count": self.count}, index=r)


class MyCacheInt(HistoricalDataCache[int, int, Any]):
    interval = 1
    end_index = 0

    async def get(self, start: int | None, *args: Any, **kwargs: Any) -> DataFrame:
        if start is None:
            start = 0
        return DataFrame({"count": [start]}, index=[start])


class MyCacheWithChunk(HistoricalDataCacheWithChunk[Timestamp, Timedelta, Any]):
    count = -1
    start_index: Timestamp = _start_index
    interval: Timedelta = _INTERVAL
    delay_seconds: float = 0

    async def get_one(self, start: Timestamp, *args: Any, **kwargs: Any) -> DataFrame:
        self.count += 1
        return DataFrame({"count": [self.count]}, index=[start])


class MyCacheWithFixedChunk(
    HistoricalDataCacheWithFixedChunk[Timestamp, Timedelta, Any]
):
    count = -1
    start_index: Timestamp = _start_index
    interval: Timedelta = _INTERVAL
    delay_seconds: float = 0

    async def get_one(self, start: Timestamp, *args: Any, **kwargs: Any) -> DataFrame:
        self.count += 1
        return DataFrame({"count": [self.count]}, index=[start])


class MyIdCache(IdCacheWithFixedChunk[str, Any]):
    count = -1
    delay_seconds: float = 0

    async def get_one(self, start: str, *args: Any, **kwargs: Any) -> DataFrame:
        self.count += 1
        return DataFrame({"count": [self.count]}, index=[start])


@parameterized_class(
    ("cache",),
    [
        (MyCache(),),
        (MyCacheInt(),),
        (MyCacheWithChunk(),),
        (MyCacheWithFixedChunk(),),
    ],
)
class TestCache(IsolatedAsyncioTestCase):
    cache: HistoricalDataCache[Any, Any, Any]

    async def test_cache(self) -> None:
        cache = self.cache
        df = await cache.update(reload=True)

        # update immediately
        df2 = await cache.update()
        assert_frame_equal(df, df2)

        # update after interval
        await asyncio.sleep(_WAIT_SECONDS)
        if isinstance(cache, MyCache):
            with self.assertWarns(RuntimeWarning):
                df_new = await cache.update()
            self.assertEqual(
                df_new["count"].sum(),
                _WAIT_SECONDS_MULTIPLIER + (1 if cache.keep == "last" else 0),
            )
        else:
            df_new = await cache.update()
        print(df, df_new)


class TestIdCache(IsolatedAsyncioTestCase):
    async def test_id_cache(self) -> None:
        cache = MyIdCache()
        ids = ["apple", "banana", "cherry"]

        cache.set_ids(ids[:2])
        self.assertEqual(cache.ids.to_list(), ids[:2])
        df = await cache.update(reload=True)
        print(df)
        self.assertEqual(len(df), 2)

        cache.set_ids(ids)
        df2 = await cache.update()
        self.assertEqual(df2["count"].sum(), 3)
        self.assertEqual(len(df2), 3)

        print(df, df2)


class TestDocs(IsolatedAsyncioTestCase):
    async def test_docs_code(self) -> None:
        class MyCache_(HistoricalDataCache[Timestamp, Timedelta, Any]):
            interval: Timedelta = Timedelta(days=1)

            async def get(
                self, start: Timestamp | None, *args: Any, **kwargs: Any
            ) -> DataFrame:
                start = start or Timestamp.utcnow().floor("10D")
                date_range_chunk = pd.date_range(start, Timestamp.utcnow(), freq="D")
                return DataFrame(
                    {"day": [d.day for d in date_range_chunk]}, index=date_range_chunk
                )

        df = await MyCache_().update()
        print("\n")
        print(df)

    async def test_docs_code2(self) -> None:
        class MyCacheWithChunk_(
            HistoricalDataCacheWithChunk[Timestamp, Timedelta, Any]
        ):
            delay_seconds: float = 0
            interval: Timedelta = Timedelta(days=1)
            start_index: Timestamp = Timestamp.utcnow().floor("10D")

            async def get_one(
                self, start: Timestamp, *args: Any, **kwargs: Any
            ) -> DataFrame:
                return DataFrame({"day": [start.day]}, index=[start])

        df = await MyCacheWithChunk_().update()
        print(df)

    async def test_docs_code3(self) -> None:
        class MyCacheWithFixedChunk_(
            HistoricalDataCacheWithFixedChunk[Timestamp, Timedelta, Any]
        ):
            delay_seconds: float = 0
            interval: Timedelta = Timedelta(days=1)
            start_index: Timestamp = Timestamp.utcnow().floor("10D")

            async def get_one(
                self, start: Timestamp, *args: Any, **kwargs: Any
            ) -> DataFrame:
                return DataFrame({"day": [start.day]}, index=[start])

        df = await MyCacheWithFixedChunk_().update()
        print(df)
