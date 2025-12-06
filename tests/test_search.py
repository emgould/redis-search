
import pytest
from src.services.search_service import autocomplete, full_search

@pytest.mark.asyncio
async def test_autocomplete():
    res = await autocomplete("a")
    assert isinstance(res, list)

@pytest.mark.asyncio
async def test_full_search():
    res = await full_search("test")
    assert isinstance(res, list)
