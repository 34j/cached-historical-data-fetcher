from __future__ import annotations

import warnings
from io import BytesIO
from logging import getLogger
from pathlib import Path
from typing import Literal

import aiofiles
import joblib
from pandas import DataFrame, concat
from pandas.testing import assert_frame_equal

BASE_PATH = Path(f"~/.cache/{__name__.split('.')[0]}").expanduser()
LOG = getLogger(__name__)


def get_path(folder: str, name: str) -> Path:
    """Get path to cache file.

    Parameters
    ----------
    folder : str
        The folder name.
    name : str
        The file name.

    Returns
    -------
    Path
        The path to cache file.
    """
    return BASE_PATH / folder / f"{name}.lz4"


async def read(path: Path) -> DataFrame:
    """Read cache file using joblib and aiofiles, and return DataFrame.
    If cache file does not exist, return empty DataFrame.

    Parameters
    ----------
    path : Path
        The path to cache file.

    Returns
    -------
    DataFrame
        The DataFrame read from cache file.
    """
    if path.exists():
        async with aiofiles.open(path, "rb") as f:
            with BytesIO(await f.read()) as f2:
                return joblib.load(f2)
    return DataFrame()


async def save(
    path: Path,
    df: DataFrame,
    *,
    compress: int | str | tuple[str, int] = ("lz4", 3),
    protocol: int | None = None,
) -> None:
    """Save DataFrame to cache file using joblib and aiofiles.

    Parameters
    ----------
    path : Path
        The path to cache file.
    df : DataFrame
        The DataFrame to save.
    compress : int | str | tuple[str, int], optional
        The compression level, by default ("lz4", 3)
    protocol : int | None, optional
        The pickle protocol, by default None (latest protocol)
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with BytesIO() as f:
        joblib.dump(df, f, compress=compress, protocol=protocol)
        async with aiofiles.open(path, "wb") as f2:
            await f2.write(f.getvalue())


async def update(
    path: Path,
    df: DataFrame,
    *,
    df_old: DataFrame | None = None,
    reload: bool = False,
    mismatch: Literal["warn", "raise"] | int | None = "warn",
    keep: Literal["first", "last"] = "last",
    compress: int | str | tuple[str, int] = ("lz4", 3),
    protocol: int | None = None,
) -> DataFrame:
    """Update cache file with DataFrame.

    Parameters
    ----------
    path : Path
        The path to cache file.
    df : DataFrame
        The DataFrame to save.
    df_old : DataFrame, optional
        The DataFrame read from cache file, by default None
        If not None, reload is ignored.
        Intended to avoid reading cache file multiple times.
    reload : bool, optional
        Whether to ignore cache file and reload, by default False
        If df_old is not None, reload is ignored.
    mismatch : Literal["warn", "raise"] | int | None, optional
        The action when data mismatch, by default "warn"
        If int, log level. If None, do nothing.
    keep : Literal["first", "last"], optional
        Which duplicated index to keep, by default "last" (has no effect if mismatch is "raise")
    compress : int | str | tuple[str, int], optional
        The compression level, by default ("lz4", 3)
    protocol : int | None, optional
        The pickle protocol, by default None (latest protocol)

    Returns
    -------
    DataFrame
        The updated DataFrame.
    """
    if df_old is None:
        df_old = await read(path) if not reload else DataFrame()

    # check if duplicated data is same
    if len(df_old) > 0:
        idx = df.index.intersection(df_old.index)
        try:
            assert_frame_equal(df_old.loc[idx], df.loc[idx], rtol=1e-3)
        except AssertionError as e:
            if mismatch == "raise":
                raise e
            elif mismatch == "warn":
                warnings.warn(
                    f"Data mismatch. Left: cache, Right: new\n{e}", RuntimeWarning
                )
            elif isinstance(mismatch, int):
                LOG.log(mismatch, f"Data mismatch. Left: cache, Right: new\n{e}")
            else:
                pass

    # concat
    df = concat([df_old, df], axis=0)
    # drop duplicated index
    df = df[~df.index.duplicated(keep=keep)]
    await save(path, df, compress=compress, protocol=protocol)
    return df
