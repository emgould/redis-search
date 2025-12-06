
from typing import Any

from pydantic import BaseModel


class MediaDocument(BaseModel):
    id: str
    type: str | None = None
    search_title: str | None = None
    original: dict[str, Any]
