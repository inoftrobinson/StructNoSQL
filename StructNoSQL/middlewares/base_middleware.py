from abc import abstractmethod
from typing import Optional, List, Dict, Any

from StructNoSQL.dynamodb.dynamodb_core import PrimaryIndex
from StructNoSQL.dynamodb.models import DatabasePathElement


class BaseMiddleware:
    def __init__(self, primary_index: PrimaryIndex):
        self._primary_index_name = primary_index.index_custom_name or primary_index.hash_key_name

    @property
    def primary_index_name(self) -> str:
        return self._primary_index_name

    @abstractmethod
    def get_field(
            self, has_multiple_fields_path: bool, field_path_elements: List[DatabasePathElement] or Dict[str, List[DatabasePathElement]],
            key_value: str, index_name: Optional[str] = None
    ) -> Any:
        raise Exception("get_field not implemented")
