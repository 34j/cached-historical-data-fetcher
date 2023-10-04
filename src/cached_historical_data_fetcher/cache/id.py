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
