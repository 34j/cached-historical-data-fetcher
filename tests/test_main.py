from cached_historical_data_fetcher.main import add


def test_add():
    assert add(1, 1) == 2
