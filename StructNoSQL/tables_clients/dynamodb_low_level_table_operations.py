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
        processed_key_value: str = self.table._append_leading_key_if_need_to(value=key_value)

        if is_multi_selector is not True:
            field_path_elements: List[DatabasePathElement]
            response_data: Optional[Any] = self.dynamodb_client.get_value_in_path_target(
                index_name=index_name or self.primary_index_name,
                key_value=processed_key_value, field_path_elements=field_path_elements
            )
            return self.table._remove_leading_key_if_need_to(field_path_elements=field_path_elements, raw_field_data=response_data)
        else:
            field_path_elements: Dict[str, List[DatabasePathElement]]
            response_data: Optional[Dict[str, Any]] = self.dynamodb_client.get_values_in_multiple_path_target(
                index_name=index_name or self.primary_index_name,
                key_value=processed_key_value, fields_path_elements=field_path_elements
            )
            if response_data is not None:
                return {
                    key: self.table._remove_leading_key_if_need_to(
                        field_path_elements=field_path_elements_items,
                        raw_field_data=response_data.get(key, None)
                    )
                    for key, field_path_elements_items in field_path_elements.items()
                }
            else:
                # When using multi-selectors, the middleware is always expected to return a dict with all the
                # 'getters' keys present, hence why the dict with None values if the response_data is None.
                return {key: None for key in field_path_elements.keys()}

    def _update_field_middleware(self, key_value: str, field_path_elements: List[DatabasePathElement], validated_data: Any) -> bool:
        processed_key_value: str = self.table._append_leading_key_if_need_to(value=key_value)
        response: Optional[Response] = self.dynamodb_client.set_update_data_element_to_map_with_default_initialization(
            index_name=self.primary_index_name,
            key_value=processed_key_value, value=validated_data,
            field_path_elements=field_path_elements
        )
        return True if response is not None else False

    def _get_multiple_fields_middleware(
            self, fields_path_elements: List[List[DatabasePathElement]],
            key_value: str, index_name: Optional[str] = None
    ):
        processed_key_value: str = self.table._append_leading_key_if_need_to(value=key_value)
        return self.dynamodb_client.get_or_query_single_item(
            index_name=index_name or self.primary_index_name,
            key_value=processed_key_value,
            fields_path_elements=fields_path_elements,
        )
