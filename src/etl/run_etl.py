
import asyncio

from src.services.etl_service import run_etl

if __name__ == "__main__":
    asyncio.run(run_etl())
