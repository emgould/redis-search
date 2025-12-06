
import pytest
from src.services.etl_service import run_etl

@pytest.mark.asyncio
async def test_etl():
    await run_etl()
    assert True
