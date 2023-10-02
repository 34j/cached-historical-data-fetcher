from __future__ import annotations

from abc import ABCMeta, abstractmethod
from logging import getLogger
from pathlib import Path
from typing import Any, Literal

from pandas import DataFrame, Timedelta, Timestamp
from slugify import slugify

from ..io import get_path, read, update

BASE_PATH = Path(f"~/.cache/{__name__.split('.')[0]}").expanduser()
LOG = getLogger(__name__)


class HistoricalDataCache(metaclass=ABCMeta):
    """Base class for historical data cache.

    Usage
    -----
    1. Override `self.get()` to implement the logic.
    2. Override `self.to_update()` if the index is not Timestamp.
    3. Call `self.update()` to get historical data.

    Examples
    --------
    .. code-block:: python
    from cached_historical_data_fetcher import HistoricalDataCache
    from pandas import DataFrame, Timedelta, Timestamp, date_range

    class MyCache(HistoricalDataCache):
        interval: Timedelta = Timedelta(days=1)

        async def get(self, start: Timestamp | None, *args: Any, **kwargs: Any) -> DataFrame:
            start = start or Timestamp.utcnow().floor("10D")
            date_range_chunk = date_range(start, Timestamp.utcnow(), freq="D")
            return DataFrame({"day": [d.day for d in date_range_chunk]}, index=date_range_chunk)

    df = await MyCache().update()
    """

    add_interval: bool = True
    """If True, `start` in `self.get()` is the last index of historical data + `self.interval`.
    If False, `start` in `self.get()` is the last index of historical data."""
    folder: str
    """The folder name. By default, the class name."""
    interval: Timedelta
    """The interval to update cache file."""
    mismatch: Literal["warn", "raise"] | int | None = "warn"
    """The action when data mismatch. If int, log level. If None, do nothing."""
    keep: Literal["first", "last"] = "last"
    """Which duplicated index to keep."""
    compress: int | str | tuple[str, int] = ("lz4", 3)
    """The compression level."""
    protocol: int | None = None
    """The pickle protocol."""

    def __init__(self) -> None:
        """Initialize HistoricalDataCache."""
        self.folder = self.__class__.__name__

    def path(self, name: str) -> Path:
        return get_path(self.folder, name)

    async def update(
        self,
        reload: bool = False,
        *args: Any,
        **kwargs: Any,
    ) -> DataFrame:
        """Update cache file with DataFrame.

        Parameters
        ----------
        reload : bool, optional
            Whether to ignore cache file and reload, by default False
        *args : Any
            The arguments for `self.get()` and `self.to_update()`.
        **kwargs : Any
            The keyword arguments for `self.get()` and `self.to_update()`.

        Returns
        -------
        DataFrame
            The DataFrame read from cache file.

        Raises
        ------
        RuntimeError
            If unexpected type read from cache file
            or `self.get()` does not return DataFrame
            or `self.to_update()` does not return bool.
        """
        # generate name
        name = "_".join([str(arg) for arg in args]) + "_".join(
            [f"{key}-{value}" for key, value in kwargs.items()]
        )
        name = slugify(name)

        # read
        df = await read(self.path(name)) if not reload else DataFrame()
        if not isinstance(df, DataFrame):
            raise TypeError(f"Unexpected type read from {self.path(name)}: {type(df)}")

        # check if need to update
        start = None
        if not df.empty:
            start = df.index.max()
        if isinstance(start, tuple):
            start = start[0]
        to_update = self.to_update(start, *args, **kwargs)
        if not isinstance(to_update, bool):
            raise TypeError(f"to_update must return bool: {type(to_update)}")

        # update
        if to_update:
            df = await self.get(
                ((start + self.interval) if self.add_interval else start)
                if not df.empty
                else None,
                *args,
                **kwargs,
            )
            if not isinstance(df, DataFrame):
                raise TypeError(f"get must return DataFrame: {type(df)}")
            old_len = len(df)

            df = await update(
                self.path(name),
                df,
                reload=reload,
                mismatch=self.mismatch,
                keep=self.keep,
                compress=self.compress,
                protocol=self.protocol,
            )
            LOG.debug(
                f"Updated {name} from {self.path}, [{df.index.min()}~{df.index.max()}]"
                f" ({old_len}->{len(df)} rows)"
            )
        else:
            LOG.debug(
                f"Loaded {name} from {self.path}, [{df.index.min()}~{df.index.max()}]"
                f" ({len(df)} rows)"
            )
        return df

    @abstractmethod
    async def get(
        self, start: Timestamp | Any | None, *args: Any, **kwargs: Any
    ) -> DataFrame:
        """Get historical data. Override this method to implement the logic.

        Parameters
        ----------
        start : Timestamp | Any | None
            The last index of historical data.

        Returns
        -------
        DataFrame
            The historical data.
            It is recommended to set index to Timestamp or unique incremental number.
            If the index is not Timestamp, override `self.to_update()`
            to implement the logic as well.
            Multiindex is supported. It is recommended to set the first level to Timestamp.
        """

    def to_update(self, end: Timestamp | Any | None, *args: Any, **kwargs: Any) -> bool:
        """Check if need to update cache file.
        Override this method to implement the logic.
        By default, update if cache file is older than self.interval.

        Parameters
        ----------
        end : Timestamp | Any | None
            The last index of historical data.
            If the cache file is empty, end is None.

        Returns
        -------
        bool
            Whether to update cache file.
        """
        return end is None or end + self.interval < Timestamp.utcnow().tz_convert(
            tz=end.tz
        )
