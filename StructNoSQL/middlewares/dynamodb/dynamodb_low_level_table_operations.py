from typing import List, Optional, Dict, Any

from StructNoSQL.middlewares.dynamodb.backend.models import Response
from StructNoSQL.models import DatabasePathElement
from StructNoSQL.middlewares.dynamodb.dynamodb_table_connectors import DynamoDBTableConnectors


class DynamoDBLowLevelTableOperations(DynamoDBTableConnectors):
    def _get_field_middleware(
            self, has_multiple_fields_path: bool,
            field_path_elements: List[DatabasePathElement] or Dict[str, List[DatabasePathElement]],
            key_value: str, index_name: Optional[str] = None
    ) -> Any:
        if has_multiple_fields_path is not True:
            field_path_elements: List[DatabasePathElement]
            response_data = self.dynamodb_client.get_value_in_path_target(
                index_name=index_name or self.primary_index_name,
                key_value=key_value, field_path_elements=field_path_elements
            )
            return response_data
        else:
            field_path_elements: Dict[str, List[DatabasePathElement]]
            response_data = self.dynamodb_client.get_values_in_multiple_path_target(
                index_name=index_name or self.primary_index_name,
                key_value=key_value, fields_path_elements=field_path_elements
            )
            return response_data

    def update_field(self, key_value: str, field_path_elements: List[DatabasePathElement], validated_data: Any) -> bool:
        response: Optional[Response] = self.dynamodb_client.set_update_data_element_to_map_with_default_initialization(
            index_name=self.primary_index_name,
            key_value=key_value, value=validated_data,
            field_path_elements=field_path_elements
        )
        return True if response is not None else False
