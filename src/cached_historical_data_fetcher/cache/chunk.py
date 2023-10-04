from __future__ import annotations

import asyncio
import warnings
from abc import ABCMeta, abstractmethod
from logging import getLogger
from typing import Generic, Sequence

import numpy as np
import pandas as pd
from pandas import DataFrame, Timestamp, concat
from tqdm.auto import tqdm

from .base import HistoricalDataCache, PGet, TIndex, TInterval

LOG = getLogger(__name__)


class HistoricalDataCacheWithChunk(
    HistoricalDataCache[TIndex, TInterval, PGet],
    Generic[TIndex, TInterval, PGet],
    metaclass=ABCMeta,
):
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

        class MyCacheWithChunk(HistoricalDataCacheWithChunk[Timestamp, Timedelta, Any]):
            delay_seconds = 0.0
            interval = Timedelta(days=1)
            start_index = Timestamp.utcnow().floor("10D")

            async def get_one(self, start: Timestamp, *args: Any, **kwargs: Any) -> DataFrame:
                return DataFrame({"day": [start.day]}, index=[start])

        df = await MyCacheWithChunk().update()
    """

    delay_seconds: float
    """Delay between chunks in seconds."""

    @property
    def start_index(self) -> TIndex:
        """The start index of historical data.
        Used when cache file exists."""
        raise NotImplementedError()

    def new_indices(
        self, start: TIndex, end: TIndex, old_indices: Sequence[TIndex]
    ) -> Sequence[TIndex]:
        """Get new indices to update.

        Parameters
        ----------
        start : TIndex
            The start index of historical data.
        end : TIndex
            The end index of historical data.
        old_indices : Sequence[TIndex]
            The old indices of historical data

        Returns
        -------
        Sequence[TIndex]
            The new indices to update.

        Raises
        ------
        TypeError
            If `start` or `end` is not supported
            (not Timestamp or not supported by `np.arange`).
        """
        if isinstance(start, Timestamp) and isinstance(end, Timestamp):
            return pd.date_range(start, end, freq=self.interval)
        else:
            try:
                return np.arange(start, end, self.interval)
            except TypeError:
                raise TypeError(
                    f"Please override self.range() to support {type(start)} and {type(end)}"
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
    async def get_one(
        self, start: TIndex, *args: PGet.args, **kwargs: PGet.kwargs
    ) -> DataFrame:
        """Get one chunk of historical data. Override this method to implement the logic.

        Parameters
        ----------
        start : TIndex
            The start index of historical data.

        Returns
        -------
        DataFrame
            The chunk of historical data.
            It is recommended to set index to Timestamp or unique incremental number.
            If the index is not Timestamp,
            override `self.to_update()` to implement the logic as well.
            If MultiIndex is used, the first level will be passed to this method
            or `self.to_update()`.
        """

    async def get(
        self, start: TIndex | None, *args: PGet.args, **kwargs: PGet.kwargs
    ) -> DataFrame:
        """Get historical data. This method does not need to be overridden.

        Parameters
        ----------
        start : TIndex
            The last index of historical data.

        Returns
        -------
        DataFrame
            The historical data.
            It is recommended to set index to Timestamp or unique incremental number.
            If the index is not Timestamp,
            override `self.to_update()` to implement the logic as well.
        """
        start_index: TIndex = start or self.start_index
        dfs = []

        # The progress bar is not accurate because chunk size may not be fixed.
        try:
            new_indices = self.new_indices(
                start_index, self.end_index, self.df_old.index
            )
        except Exception as e:
            warnings.warn(
                f"self.new_indices() failed. Progress bar may be inaccurate: {e}"
            )
            new_indices = []

        pbar = tqdm(new_indices)
        start_current: TIndex = start_index
        while self.to_update(start_current, *args, **kwargs):
            df = await self.get_one(start_current, *args, **kwargs)
            if not isinstance(df, DataFrame):
                raise TypeError(f"get_one must return DataFrame: {type(df)}")

            dfs.append(df)
            start_current = df.index.max()
            if isinstance(start_current, tuple):
                start_current = start_current[0]
            if self.add_interval_to_start_index:
                start_current += self.interval  # type: ignore
            pbar.update()
            pbar.set_description(
                f"{self.__class__.__name__} {start_current}"
                f"|{' '.join(map(str, args))}|{' '.join([f'{k}={v}' for k, v in kwargs.items()])}"
            )
            await asyncio.sleep(self.delay_seconds)
        return concat(dfs) if len(dfs) > 0 else DataFrame()


class HistoricalDataCacheWithFixedChunk(
    HistoricalDataCacheWithChunk[TIndex, TInterval, PGet],
    Generic[TIndex, TInterval, PGet],
    metaclass=ABCMeta,
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

        class MyCacheWithFixedChunk(HistoricalDataCacheWithFixedChunk[Timestamp, Timedelta, Any]):
            delay_seconds = 0.0
            interval = Timedelta(days=1)
            start_index = Timestamp.utcnow().floor("10D")

            async def get_one(self, start: Timestamp, *args: Any, **kwargs: Any) -> DataFrame:
                return DataFrame({"day": [start.day]}, index=[start])

        df = await MyCacheWithFixedChunk().update()
    """

    async def get(
        self, start: TIndex | None, *args: PGet.args, **kwargs: PGet.kwargs
    ) -> DataFrame:
        start_index: TIndex = start or self.start_index
        tasks = []

        pbar = tqdm(self.new_indices(start_index, self.end_index, self.df_old.index))
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
