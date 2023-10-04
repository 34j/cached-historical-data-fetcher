from __future__ import annotations

import warnings
from abc import ABCMeta, abstractmethod
from logging import getLogger
from pathlib import Path
from typing import Any, Generic, Literal, ParamSpec, Protocol, TypeVar

from pandas import DataFrame, Timedelta, Timestamp
from slugify import slugify
from typing_extensions import Self

from ..io import get_path, read, update


class AddableAndSubtractableAndComparable(Protocol):
    def __add__(self, other: Any) -> Self:
        ...

    def __sub__(self, other: Any) -> Self:
        ...

    def __lt__(self, other: Any) -> bool:
        ...

    def __gt__(self, other: Any) -> bool:
        ...

    def __ge__(self, other: Any) -> bool:
        ...

    def __le__(self, other: Any) -> bool:
        ...

    def __eq__(self, other: Any) -> bool:
        ...

    def __ne__(self, other: Any) -> bool:
        ...


BASE_PATH = Path(f"~/.cache/{__name__.split('.')[0]}").expanduser()
LOG = getLogger(__name__)
TIndex = TypeVar("TIndex", bound=AddableAndSubtractableAndComparable)
TInterval = TypeVar("TInterval")
PGet = ParamSpec("PGet")


class HistoricalDataCache(Generic[TIndex, TInterval, PGet], metaclass=ABCMeta):
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

        class MyCache(HistoricalDataCache[Timestamp, Timedelta, Any]):
            interval = Timedelta(days=1)

            async def get(self, start: Timestamp | None, *args: Any, **kwargs: Any) -> DataFrame:
                start = start or Timestamp.utcnow().floor("10D")
                date_range_chunk = date_range(start, Timestamp.utcnow(), freq="D")
                return DataFrame({"day": [d.day for d in date_range_chunk]}, index=date_range_chunk)

        df = await MyCache().update()
    """

    add_interval_to_start_index: bool = True
    """If True, `start` in `self.get()` is the last index of historical data + `self.interval`.
    If False, `start` in `self.get()` is the last index of historical data."""
    folder: str
    """The folder name. By default, the class name."""
    interval: TInterval
    """The interval to update cache file."""
    mismatch: Literal["warn", "raise"] | int | None = "warn"
    """The action when data mismatch. If int, log level. If None, do nothing."""
    keep: Literal["first", "last"] = "last"
    """Which duplicated index to keep."""
    compress: int | str | tuple[str, int] = ("lz4", 3)
    """The compression level."""
    protocol: int | None = None
    """The pickle protocol."""
    subtract_interval_from_end_index: bool = True
    """Whether to get the latest uncompleted chunk.
    If False, make sure to set `self.add_interval_to_start_index` to False
    to avoid uncompleted chunk left in cache file."""

    @property
    def end_index_base(self) -> TIndex:
        """The end index of historical data.
        This property is only used in default `self.to_update()`."""
        if isinstance(self.interval, Timedelta):
            return Timestamp.utcnow()
        raise NotImplementedError

    @property
    def end_index(self) -> TIndex:
        """The end index of historical data. If `self.subtract_interval_from_end_index` is False,
        the end index is the latest uncompleted chunk.
        This property is only used in default `self.to_update()`.
        Consider overriding `end_index_base` instead of this property."""
        if not self.subtract_interval_from_end_index:
            return self.end_index_base
        return self.end_index_base - self.interval

    def __init__(self) -> None:
        """Initialize HistoricalDataCache."""
        self.folder = self.__class__.__name__

        if (
            not self.subtract_interval_from_end_index
            and self.add_interval_to_start_index
        ):
            warnings.warn(
                "If `self.subtract_interval_from_end_index` is False, "
                "make sure to set `self.add_interval_to_start_index` to False "
                "to avoid uncompleted chunk left in cache file.",
                RuntimeWarning,
            )

    def path(self, name: str) -> Path:
        return get_path(self.folder, name)

    def name_from_args_kwargs(self, *args: Any, **kwargs: Any) -> str:
        """Generate path from args and kwargs."""
        name = "_".join([str(arg) for arg in args]) + "_".join(
            [f"{key}-{value}" for key, value in kwargs.items()]
        )
        name = slugify(name)
        return name

    def path_from_args_kwargs(self, *args: Any, **kwargs: Any) -> Path:
        return self.path(self.name_from_args_kwargs(*args, **kwargs))

    async def update(
        self,
        reload: bool = False,
        *args: PGet.args,
        **kwargs: PGet.kwargs,
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
        name = self.name_from_args_kwargs(*args, **kwargs)
        path = self.path(name)

        # read
        df = await read(path) if not reload else DataFrame()
        self.df_old = df.copy()
        if not isinstance(df, DataFrame):
            warnings.warn(
                f"Unexpected type read from {path}: {type(df)}\n",
                RuntimeWarning,
            )

        # check if need to update
        start = None
        if not df.empty:
            start = df.index.max()
        if isinstance(start, tuple):
            start = start[0]
        to_update = self.to_update(start, *args, **kwargs)

        # update
        if to_update:
            df = await self.get(
                ((start + self.interval) if self.add_interval_to_start_index else start)
                if not df.empty
                else None,
                *args,
                **kwargs,
            )
            if not isinstance(df, DataFrame):
                warnings.warn(
                    f"Unexpected type returned from `self.get()`: {type(df)}\n"
                    "Expected: DataFrame",
                )
            old_len = len(df)

            df = await update(
                path,
                df,
                df_old=self.df_old,
                mismatch=self.mismatch,
                keep=self.keep,
                compress=self.compress,
                protocol=self.protocol,
            )
            LOG.debug(
                f"Updated {name} from {path}, [{df.index.min()}~{df.index.max()}]"
                f" ({old_len}->{len(df)} rows)"
            )
        else:
            LOG.debug(
                f"Loaded {name} from {path}, [{df.index.min()}~{df.index.max()}]"
                f" ({len(df)} rows)"
            )
        del self.df_old
        return df

    @abstractmethod
    async def get(
        self, start: TIndex, *args: PGet.args, **kwargs: PGet.kwargs
    ) -> DataFrame:
        """Get historical data. Override this method to implement the logic.

        Parameters
        ----------
        start : TIndex
            The last index of historical data.

        Returns
        -------
        DataFrame
            The historical data.
            It is recommended to set index to Timestamp or unique incremental number.
            If the index is not Timestamp, override `self.to_update()`
            to implement the logic as well.
            If MultiIndex is used, the first level will be passed to this method
            or `self.to_update()`.
        """

    def to_update(self, end: TIndex, *args: Any, **kwargs: Any) -> bool:
        """Check if need to update cache file.
        Override this method to implement the logic.
        By default, update if cache file is older than self.interval.
        If it cannot be determined, this method can always return True
        if this class is not `HistoricalDataCacheWithChunk`.

        Parameters
        ----------
        end : TIndex
            The last index of historical data.
            If the cache file is empty, end is None.

        Returns
        -------
        bool
            Whether to update cache file.
        """
        return end is None or end < self.end_index
