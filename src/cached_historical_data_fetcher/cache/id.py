from __future__ import annotations

from abc import ABCMeta
from typing import Any, Generic, Sequence

from pandas import Index

from .base import PGet, TIndex
from .chunk import HistoricalDataCacheWithFixedChunk


class IdCacheWithFixedChunk(
    HistoricalDataCacheWithFixedChunk[TIndex, Any, PGet],
    Generic[TIndex, PGet],
    metaclass=ABCMeta,
):
    """Base class for data cache indexed by ids.

    Examples
    --------
    .. code-block:: python

       from cached_historical_data_fetcher import IdCacheWithFixedChunk

       class MyIdCache(IdCacheWithFixedChunk[str, Any]):
           delay_seconds = 0.0

           async def get_one(self, start: str, *args: Any, **kwargs: Any) -> DataFrame:
               return DataFrame({"id+hello": [start + "+hello"]}, index=[start])

       cache = MyIdCache()
       cache.set_ids(["a", "b", "c"])
       df = await cache.update()

    Output:

       id+hello
    a   a+hello
    b   b+hello
    c   c+hello
    """

    add_interval_to_start_index: bool = False
    subtract_interval_from_end_index: bool = False

    @property
    def start_index(self) -> TIndex | None:  # type: ignore
        return None

    @property
    def end_index(self) -> TIndex | None:  # type: ignore
        return None

    def set_ids(self, ids: Sequence[TIndex]) -> None:
        self.ids = Index(ids)

    def new_indices(
        self, start: Any, end: Any, old_indices: Sequence[TIndex]
    ) -> Sequence[TIndex]:
        return self.ids.difference(Index(old_indices)).to_list()

    def to_update(self, end: TIndex, *args: Any, **kwargs: Any) -> bool:
        return True
