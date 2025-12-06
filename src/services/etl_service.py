
from src.adapters.external_api import fetch_all_media
from src.adapters.redis_repository import RedisRepository
from src.core.normalize import derive_search_title

repo = RedisRepository()

async def run_etl():
    data = await fetch_all_media()
    for item in data:
        item["search_title"] = derive_search_title(item)
        key = f"media:{item.get('id')}"
        await repo.set_document(key, item)
