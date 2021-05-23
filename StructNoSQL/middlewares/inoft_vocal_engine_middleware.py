import requests
from typing import Optional, List, Dict, Any
from StructNoSQL.dynamodb.dynamodb_core import DynamoDbCoreAdapter, PrimaryIndex, GlobalSecondaryIndex
from StructNoSQL.dynamodb.models import DatabasePathElement
from StructNoSQL.middlewares.base_middleware import BaseMiddleware


class InoftVocalEngineMiddleware(BaseMiddleware):
    def __init__(self, table_name: str, region_name: str, primary_index: PrimaryIndex):
        super().__init__(primary_index=primary_index)

    @staticmethod
    def _api_handler(payload: dict) -> Optional[Any]:
        response = requests.post(url='127.0.0.1:5000/api/database-client', json=payload)
        response_data: Optional[dict] = response.json()
        if response_data is not None and isinstance(response_data, dict):
            success: bool = response_data.get('success', False)
            if success is True:
                return response_data.get('data', None)
        return None

    def get_field(
            self, has_multiple_fields_path: bool, field_path_elements: List[DatabasePathElement] or Dict[str, List[DatabasePathElement]],
            key_value: str, index_name: Optional[str] = None
    ) -> Any:
        if has_multiple_fields_path is not True:
            field_path_elements: List[DatabasePathElement]
            return self._api_handler(payload={
                'operationType': 'getSingleValueInPathTarget',
                'fieldPathElements': field_path_elements,
            })
        else:
            field_path_elements: Dict[str, List[DatabasePathElement]]
            return self._api_handler(payload={
                'operationType': 'getValuesInMultiplePathTarget',
                'fieldsPathsElements': field_path_elements,
            })
