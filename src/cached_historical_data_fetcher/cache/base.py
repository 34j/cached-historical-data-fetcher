from __future__ import annotations

import warnings
from abc import ABCMeta, abstractmethod
from logging import getLogger
from pathlib import Path
from typing import Any, Generic, Literal, Protocol, TypeVar

from pandas import DataFrame, Timedelta, Timestamp
from slugify import slugify
from typing_extensions import ParamSpec, Self

from ..io import get_path, read, update

T = TypeVar("T", contravariant=True)


class AddableAndSubtractableAndComparable(Protocol[T]):
    """A protocol that requires __add__, __sub__, __lt__, __gt__
    (+, -, <, >) operators."""

    def __add__(self, other: Self | T) -> Self:
        ...

    def __sub__(self, other: Self | T) -> Self:
        ...

    def __lt__(self, other: Self) -> bool:
        ...

    def __gt__(self, other: Self) -> bool:
        ...


BASE_PATH = Path(f"~/.cache/{__name__.split('.')[0]}").expanduser()
"""The folder to store cache files."""
LOG = getLogger(__name__)
TInterval = TypeVar("TInterval")
"""The type of interval which can be added to index."""
TIndex = TypeVar("TIndex")
"""The type of index in DataFrame."""
PGet = ParamSpec("PGet")
"""The type of arguments for `self.get()`."""


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
        return self.end_index_base - self.interval  # type: ignore

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
        """Get path to cache file.

        Parameters
        ----------
        name : str
            The name of cache file.

        Returns
        -------
        Path
            The path to cache file.
        """
        return get_path(self.folder, name)

    def name_from_args_kwargs(self, *args: Any, **kwargs: Any) -> str:
        """Generate safe name from arguments using slugify.

        Returns
        -------
        str
            The name of cache file.
        """
        name = "_".join([str(arg) for arg in args]) + "_".join(
            [f"{key}-{value}" for key, value in kwargs.items()]
        )
        name = slugify(name)
        return name

    def path_from_args_kwargs(self, *args: Any, **kwargs: Any) -> Path:
        """Get path to cache file from arguments.

        Returns
        -------
        Path
            The path to cache file.
        """
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
            try:
                min_, max_ = df.index.min(), df.index.max()
            except TypeError:
                min_, max_ = df.index[0], df.index[-1]
            LOG.debug(
                f"Updated {name} from {path}, [{min_}~{max_}]"
                f" ({old_len}->{len(df)} rows)"
            )
        else:
            try:
                min_, max_ = df.index.min(), df.index.max()
            except TypeError:
                min_, max_ = df.index[0], df.index[-1]
            LOG.debug(
                f"Loaded {name} from {path}, [{min_}~{max_}]" f" ({len(df)} rows)"
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
        return end is None or end < self.end_index  # type: ignore
