from __future__ import annotations

import asyncio
import warnings
from abc import ABCMeta, abstractmethod
from logging import getLogger
from typing import Any, final

import pandas as pd
from pandas import DataFrame, Timestamp, concat
from tqdm.auto import tqdm

from .base import HistoricalDataCache

LOG = getLogger(__name__)


class HistoricalDataCacheWithChunk(HistoricalDataCache, metaclass=ABCMeta):
    """Base class for historical data cache with chunk.

    Usage
    -----
    1. Override `self.get_one()` to implement the logic.
    2. Override `self.to_update()` if the index is not Timestamp or interval is not fixed.
    3. Call `self.update()` to get historical data.

    Examples
    --------
    .. code-block:: python
    from cached_historical_data_fetcher import HistoricalDataCacheWithChunk
    from pandas import DataFrame, Timedelta, Timestamp

    class MyCacheWithChunk(HistoricalDataCacheWithChunk):
        delay_seconds: float = 0
        interval: Timedelta = Timedelta(days=1)
        start_init: Timestamp = Timestamp.utcnow().floor("10D")

        async def get_one(self, start: Timestamp, *args: Any, **kwargs: Any) -> DataFrame:
            return DataFrame({"day": [start.day]}, index=[start])

    df = await MyCacheWithChunk().update()
    """

    delay_seconds: float
    """Delay between chunks in seconds."""
    start_init: Timestamp
    """The initial start index of historical data.
    Used when no cache file exists."""
    get_latest_uncompleted_chunk: bool = False
    """Whether to get the latest uncompleted chunk.
    If True, make sure to set `self.add_interval` to False
    to avoid uncompleted chunk left in cache file."""

    def __init__(self) -> None:
        super().__init__()
        if self.get_latest_uncompleted_chunk and self.add_interval:
            warnings.warn(
                "If `self.get_latest_uncompleted_chunk` is True, "
                "make sure to set `self.add_interval` to False "
                "to avoid uncompleted chunk left in cache file.",
                RuntimeWarning,
            )

    @property
    def delay(self) -> float:
        """Delay between chunks in seconds. (Alias of `self.delay_seconds`.)"""
        return self.delay_seconds

    @delay.setter
    def delay(self, value: float) -> None:
        """Delay between chunks in seconds. (Alias of `self.delay_seconds`.)"""
        self.delay_seconds = value

    @abstractmethod
    async def get_one(self, start: Timestamp, *args: Any, **kwargs: Any) -> DataFrame:
        """Get one chunk of historical data. Override this method to implement the logic.

        Parameters
        ----------
        start : Timestamp
            The start index of historical data.

        Returns
        -------
        DataFrame
            The chunk of historical data.
            It is recommended to set index to Timestamp or unique incremental number.
            If the index is not Timestamp,
            override `self.to_update()` to implement the logic as well.
            Multiindex is supported. It is recommended to set the first level to Timestamp.
        """

    async def get(
        self, start: Timestamp | None, *args: Any, **kwargs: Any
    ) -> DataFrame:
        """Get historical data. This method does not need to be overridden.

        Parameters
        ----------
        start : Timestamp | Any | None
            The last index of historical data.

        Returns
        -------
        DataFrame
            The historical data.
            It is recommended to set index to Timestamp or unique incremental number.
            If the index is not Timestamp,
            override `self.to_update()` to implement the logic as well.
        """
        start_init: Timestamp = start or self.start_init
        dfs = []

        # The progress bar is not accurate because chunk size may not be fixed.
        pbar = tqdm(
            pd.date_range(
                start_init.tz_convert(tz="UTC"),
                (Timestamp.utcnow())
                if self.get_latest_uncompleted_chunk
                else (Timestamp.utcnow() - self.interval),
                freq=self.interval,
            )
        )
        start_current = start_init
        while self.to_update(start_current, *args, **kwargs):
            df = await self.get_one(start_current, *args, **kwargs)
            if not isinstance(df, DataFrame):
                raise TypeError(f"get_one must return DataFrame: {type(df)}")

            dfs.append(df)
            start_current = df.index.max()
            if isinstance(start_current, tuple):
                start_current = start_current[0]
            if self.add_interval:
                start_current += self.interval
            pbar.update()
            pbar.set_description(
                f"{self.__class__.__name__} {start_current}"
                f"|{' '.join(map(str, args))}|{' '.join([f'{k}={v}' for k, v in kwargs.items()])}"
            )
            await asyncio.sleep(self.delay_seconds)
        return concat(dfs) if len(dfs) > 0 else DataFrame()


class HistoricalDataCacheWithFixedChunk(
    HistoricalDataCacheWithChunk, metaclass=ABCMeta
):
    """Base class for historical data cache with chunk.

    This class only supports fixed interval.
    To support variable interval, use `HistoricalDataCacheWithChunk` instead.

    As `HistoricalDataCacheWithChunk` calls `self.get_one()` one by one,
    `HistoricalDataCacheWithFixedChunk` calls `self.get_one()` in parallel.
    This makes it impossible to guarantee that rate limits are not exceeded, because
    depending on network conditions etc.,
    it might theoretically be possible for all the requests to reach the server at the same time.
    Make sure to set `self.delay_seconds` large enough to avoid server overload or ban.

    Usage
    -----
    1. Override `self.get_one()` to implement the logic.
    2. Call `self.update()` to get historical data.

    Examples
    --------
    .. code-block:: python
    from cached_historical_data_fetcher import HistoricalDataCacheWithFixedChunk
    from pandas import DataFrame, Timedelta, Timestamp

    class MyCacheWithFixedChunk(HistoricalDataCacheWithFixedChunk):
        delay_seconds: float = 0
        interval: Timedelta = Timedelta(days=1)
        start_init: Timestamp = Timestamp.utcnow().floor("10D")

        async def get_one(self, start: Timestamp, *args: Any, **kwargs: Any) -> DataFrame:
            return DataFrame({"day": [start.day]}, index=[start])

    df = await MyCacheWithFixedChunk().update()
    """

    @final
    def to_update(self, end: Timestamp | None, *args: Any, **kwargs: Any) -> bool:
        return super().to_update(end, *args, **kwargs)

    async def get(
        self, start: Timestamp | None, *args: Any, **kwargs: Any
    ) -> DataFrame:
        start_init: Timestamp = start or self.start_init
        tasks = []
        pbar = tqdm(
            pd.date_range(
                start_init.tz_convert(tz="UTC"),
                (Timestamp.utcnow())
                if self.get_latest_uncompleted_chunk
                else (Timestamp.utcnow() - self.interval),
                freq=self.interval,
            )
        )

        for start_current in pbar:
            tasks.append(
                asyncio.create_task(self.get_one(start_current, *args, **kwargs))
            )
            pbar.update()
            pbar.set_description(
                f"{self.__class__.__name__} {start_current}"
                f"|{' '.join(map(str, args))}|{' '.join([f'{k}={v}' for k, v in kwargs.items()])}"
            )
            await asyncio.sleep(self.delay_seconds)
        dfs = await asyncio.gather(*tasks)
        return concat(dfs) if len(dfs) > 0 else DataFrame()
