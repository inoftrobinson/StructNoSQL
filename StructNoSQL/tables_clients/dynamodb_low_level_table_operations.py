from typing import List, Optional, Dict, Any, Union

from StructNoSQL.base_tables import BaseTable
from StructNoSQL.tables_clients.backend import Response
from StructNoSQL.models import DatabasePathElement
from StructNoSQL.tables_clients.dynamodb_table_connectors import DynamoDBTableConnectors


class DynamoDBLowLevelTableOperations(DynamoDBTableConnectors):
    def __init__(self, table: BaseTable):
        self.table = table

    def _get_field_middleware(
            self, is_multi_selector: bool,
            field_path_elements: Union[List[DatabasePathElement], Dict[str, List[DatabasePathElement]]],
            key_value: str, index_name: Optional[str] = None
    ) -> Any:
        primary_key_field = self.table._get_primary_key_field()
        transformed_key_value = primary_key_field.transform_from_write(value=key_value)

        if is_multi_selector is not True:
            field_path_elements: List[DatabasePathElement]
            response_data: Optional[Any] = self.dynamodb_client.get_value_in_path_target(
                index_name=index_name or self.primary_index_name,
                key_value=transformed_key_value, field_path_elements=field_path_elements
            )
            return response_data
        else:
            field_path_elements: Dict[str, List[DatabasePathElement]]
            response_data: Optional[Dict[str, Any]] = self.dynamodb_client.get_values_in_multiple_path_target(
                index_name=index_name or self.primary_index_name,
                key_value=transformed_key_value, fields_path_elements=field_path_elements
            )
            if response_data is not None:
                return {
                    key: response_data.get(key, None)
                    for key in field_path_elements.keys()
                }
            else:
                # When using multi-selectors, the middleware is always expected to return a dict with all the
                # 'getters' keys present, hence why the dict with None values if the response_data is None.
                return {key: None for key in field_path_elements.keys()}

    def _update_field_middleware(self, key_value: str, field_path_elements: List[DatabasePathElement], validated_data: Any) -> bool:
        primary_key_field = self.table._get_primary_key_field()
        transformed_key_value = primary_key_field.transform_from_write(value=key_value)
        response: Optional[Response] = self.dynamodb_client.set_update_data_element_to_map_with_default_initialization(
            index_name=self.primary_index_name,
            key_value=transformed_key_value, value=validated_data,
            field_path_elements=field_path_elements
        )
        return True if response is not None else False

    def _get_multiple_fields_middleware(
            self, fields_path_elements: List[List[DatabasePathElement]],
            key_value: str, index_name: Optional[str] = None
    ):
        primary_key_field = self.table._get_primary_key_field()
        transformed_key_value = primary_key_field.transform_from_write(value=key_value)
        return self.dynamodb_client.get_or_query_single_item(
            index_name=index_name or self.primary_index_name,
            key_value=transformed_key_value,
            fields_path_elements=fields_path_elements,
        )
