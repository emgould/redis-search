from typing import Any

from pydantic.main import BaseModel


class BaseModelWithMethods(BaseModel):
    """Base model with compatibility methods for dataclass_json migration."""

    def to_json(self, **kwargs: Any) -> str:
        """Compatibility method for dataclass_json's to_json()"""
        return self.model_dump_json(**kwargs)

    def to_dict(self) -> dict[str, Any]:
        """Compatibility method for dataclass_json's to_dict()"""
        return self.model_dump()
