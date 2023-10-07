# Cached Historical Data Fetcher

<p align="center">
  <a href="https://github.com/34j/cached-historical-data-fetcher/actions/workflows/ci.yml?query=branch%3Amain">
    <img src="https://img.shields.io/github/actions/workflow/status/34j/cached-historical-data-fetcher/ci.yml?branch=main&label=CI&logo=github&style=flat-square" alt="CI Status" >
  </a>
  <a href="https://cached-historical-data-fetcher.readthedocs.io">
    <img src="https://img.shields.io/readthedocs/cached-historical-data-fetcher.svg?logo=read-the-docs&logoColor=fff&style=flat-square" alt="Documentation Status">
  </a>
  <a href="https://codecov.io/gh/34j/cached-historical-data-fetcher">
    <img src="https://img.shields.io/codecov/c/github/34j/cached-historical-data-fetcher.svg?logo=codecov&logoColor=fff&style=flat-square" alt="Test coverage percentage">
  </a>
</p>
<p align="center">
  <a href="https://python-poetry.org/">
    <img src="https://img.shields.io/badge/packaging-poetry-299bd7?style=flat-square&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA4AAAASCAYAAABrXO8xAAAACXBIWXMAAAsTAAALEwEAmpwYAAAAAXNSR0IArs4c6QAAAARnQU1BAACxjwv8YQUAAAJJSURBVHgBfZLPa1NBEMe/s7tNXoxW1KJQKaUHkXhQvHgW6UHQQ09CBS/6V3hKc/AP8CqCrUcpmop3Cx48eDB4yEECjVQrlZb80CRN8t6OM/teagVxYZi38+Yz853dJbzoMV3MM8cJUcLMSUKIE8AzQ2PieZzFxEJOHMOgMQQ+dUgSAckNXhapU/NMhDSWLs1B24A8sO1xrN4NECkcAC9ASkiIJc6k5TRiUDPhnyMMdhKc+Zx19l6SgyeW76BEONY9exVQMzKExGKwwPsCzza7KGSSWRWEQhyEaDXp6ZHEr416ygbiKYOd7TEWvvcQIeusHYMJGhTwF9y7sGnSwaWyFAiyoxzqW0PM/RjghPxF2pWReAowTEXnDh0xgcLs8l2YQmOrj3N7ByiqEoH0cARs4u78WgAVkoEDIDoOi3AkcLOHU60RIg5wC4ZuTC7FaHKQm8Hq1fQuSOBvX/sodmNJSB5geaF5CPIkUeecdMxieoRO5jz9bheL6/tXjrwCyX/UYBUcjCaWHljx1xiX6z9xEjkYAzbGVnB8pvLmyXm9ep+W8CmsSHQQY77Zx1zboxAV0w7ybMhQmfqdmmw3nEp1I0Z+FGO6M8LZdoyZnuzzBdjISicKRnpxzI9fPb+0oYXsNdyi+d3h9bm9MWYHFtPeIZfLwzmFDKy1ai3p+PDls1Llz4yyFpferxjnyjJDSEy9CaCx5m2cJPerq6Xm34eTrZt3PqxYO1XOwDYZrFlH1fWnpU38Y9HRze3lj0vOujZcXKuuXm3jP+s3KbZVra7y2EAAAAAASUVORK5CYII=" alt="Poetry">
  </a>
  <a href="https://github.com/ambv/black">
    <img src="https://img.shields.io/badge/code%20style-black-000000.svg?style=flat-square" alt="black">
  </a>
  <a href="https://github.com/pre-commit/pre-commit">
    <img src="https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white&style=flat-square" alt="pre-commit">
  </a>
</p>
<p align="center">
  <a href="https://pypi.org/project/cached-historical-data-fetcher/">
    <img src="https://img.shields.io/pypi/v/cached-historical-data-fetcher.svg?logo=python&logoColor=fff&style=flat-square" alt="PyPI Version">
  </a>
  <img src="https://img.shields.io/pypi/pyversions/cached-historical-data-fetcher.svg?style=flat-square&logo=python&amp;logoColor=fff" alt="Supported Python versions">
  <img src="https://img.shields.io/pypi/l/cached-historical-data-fetcher.svg?style=flat-square" alt="License">
</p>

Python utility for fetching any historical data using caching.
Suitable for acquiring data that is added frequently and incrementally, e.g. news, posts, weather, etc.

## Installation

Install this via pip (or your favourite package manager):

```shell
pip install cached-historical-data-fetcher
```

## Features

- Uses cache built on top of [`joblib`](https://github.com/joblib/joblib), [`lz4`](https://github.com/lz4/lz4) and [`aiofiles`](https://github.com/Tinche/aiofiles).
- Ready to use with [`asyncio`](https://docs.python.org/3/library/asyncio.html), [`aiohttp`](https://github.com/aio-libs/aiohttp), [`aiohttp-client-cache`](https://github.com/requests-cache/aiohttp-client-cache). Uses `asyncio.gather` for fetching chunks in parallel. (For performance reasons, only using `aiohttp-client-cache` is probably not a good idea when fetching large number of chunks (web requests).)
- Based on [`pandas`](https://github.com/pandas-dev/pandas) and supports `MultiIndex`.

## Usage

### `HistoricalDataCache`, `HistoricalDataCacheWithChunk` and `HistoricalDataCacheWithFixedChunk`

Override `get_one()` method to fetch data for one chunk. `update()` method will call `get_one()` for each unfetched chunk and concatenate results, then save to cache.

```python
from cached_historical_data_fetcher import HistoricalDataCacheWithFixedChunk
from pandas import DataFrame, Timedelta, Timestamp
from typing import Any

# define cache class
class MyCacheWithFixedChunk(HistoricalDataCacheWithFixedChunk[Timestamp, Timedelta, Any]):
    delay_seconds = 0.0 # delay between chunks (requests) in seconds
    interval = Timedelta(days=1) # interval between chunks, can be any type
    start_index = Timestamp.utcnow().floor("10D") # start index, can be any type

    async def get_one(self, start: Timestamp, *args: Any, **kwargs: Any) -> DataFrame:
        """Fetch data for one chunk."""
        return DataFrame({"day": [start.day]}, index=[start])

# get complete data
print(await MyCacheWithFixedChunk().update())
```

```shell
                           day
2023-09-30 00:00:00+00:00   30
2023-10-01 00:00:00+00:00    1
2023-10-02 00:00:00+00:00    2
```

**See [example.ipynb](example.ipynb) for real-world example.**

### `IdCacheWithFixedChunk`

Override `get_one` method to fetch data for one chunk in the same way as in `HistoricalDataCacheWithFixedChunk`.
After updating `ids` by calling `set_ids()`, `update()` method will call `get_one()` for every unfetched id and concatenate results, then save to cache.

```python
from cached_historical_data_fetcher import IdCacheWithFixedChunk
from pandas import DataFrame
from typing import Any

class MyIdCache(IdCacheWithFixedChunk[str, Any]):
    delay_seconds = 0.0 # delay between chunks (requests) in seconds

    async def get_one(self, start: str, *args: Any, **kwargs: Any) -> DataFrame:
        """Fetch data for one chunk."""
        return DataFrame({"id+hello": [start + "+hello"]}, index=[start])

cache = MyIdCache() # create cache
cache.set_ids(["a"]) # set ids
cache.set_ids(["b"]) # set ids again, now `cache.ids` is ["a", "b"]
print(await cache.update(reload=True)) # discard previous cache and fetch again
cache.set_ids(["b", "c"]) # set ids again, now `cache.ids` is ["a", "b", "c"]
print(await cache.update()) # fetch only new data
```

```shell
       id+hello
    a   a+hello
    b   b+hello
       id+hello
    a   a+hello
    b   b+hello
    c   c+hello
```

## Contributors ✨

Thanks goes to these wonderful people ([emoji key](https://allcontributors.org/docs/en/emoji-key)):

<!-- prettier-ignore-start -->
<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- markdownlint-disable -->
<!-- markdownlint-enable -->
<!-- ALL-CONTRIBUTORS-LIST:END -->
<!-- prettier-ignore-end -->

This project follows the [all-contributors](https://github.com/all-contributors/all-contributors) specification. Contributions of any kind welcome!
