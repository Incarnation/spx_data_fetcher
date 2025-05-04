import pytest

from app import fetcher


def test_get_next_expirations():
    expirations = fetcher.get_next_expirations()
    assert isinstance(expirations, list)
    assert len(expirations) > 0


def test_fetch_option_chain():
    expirations = fetcher.get_next_expirations()
    if expirations:
        options = fetcher.fetch_option_chain(expiration=expirations[0])
        assert isinstance(options, list)
