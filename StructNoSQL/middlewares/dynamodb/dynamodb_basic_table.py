from typing import Optional, List, Dict, Any

from StructNoSQL.middlewares.dynamodb.backend.dynamodb_core import DynamoDbCoreAdapter, PrimaryIndex, GlobalSecondaryIndex
from StructNoSQL.models import DatabasePathElement, FieldGetter, FieldSetter, UnsafeFieldSetter, FieldRemover, FieldPathSetter
from StructNoSQL.practical_logger import message_with_vars
from StructNoSQL.tables.base_basic_table import BaseBasicTable
from StructNoSQL.middlewares.dynamodb.dynamodb_low_level_table_operations import DynamoDBLowLevelTableOperations


class DynamoDBBasicTable(BaseBasicTable, DynamoDBLowLevelTableOperations):
    def __init__(
            self, table_name: str, region_name: str,
            data_model, primary_index: PrimaryIndex,
            billing_mode: str = DynamoDbCoreAdapter.PAY_PER_REQUEST,
            global_secondary_indexes: List[GlobalSecondaryIndex] = None,
            auto_create_table: bool = True
    ):
        super().__init__(data_model=data_model)
        super().__setup_connectors__(
            table_name=table_name, region_name=region_name,
            primary_index=primary_index, global_secondary_indexes=global_secondary_indexes,
            billing_mode=billing_mode, auto_create_table=auto_create_table
        )

    def put_record(self, record_dict_data: dict) -> bool:
        self.model_virtual_map_field.populate(value=record_dict_data)
        validated_data, is_valid = self.model_virtual_map_field.validate_data()
        if is_valid is True:
            return self.dynamodb_client.put_record(item_dict=validated_data)
        else:
            return False

    def delete_record(self, indexes_keys_selectors: dict) -> bool:
        found_all_indexes = True
        for index_key, index_target_value in indexes_keys_selectors.items():
            index_matching_field = getattr(self.model, index_key, None)
            if index_matching_field is None:
                found_all_indexes = False
                print(message_with_vars(
                    message="An index key selector passed to the delete_record function, was not found, in the table model. Operation not executed.",
                    vars_dict={"index_key": index_key, "index_target_value": index_target_value, "index_matching_field": index_matching_field, "table.model": self.model}
                ))

        if found_all_indexes is True:
            return self.dynamodb_client.delete_record(indexes_keys_selectors=indexes_keys_selectors)
        else:
            return False

    def get_field(self, key_value: str, field_path: str, query_kwargs: Optional[dict] = None, index_name: Optional[str] = None) -> Any:
        def middleware(field_path_elements: List[DatabasePathElement] or Dict[str, List[DatabasePathElement]], has_multiple_fields_path: bool):
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
        return self._get_field(middleware=middleware, field_path=field_path, query_kwargs=query_kwargs)

    def get_multiple_fields(self, key_value: str, getters: Dict[str, FieldGetter], index_name: Optional[str] = None) -> Optional[dict]:
        def middleware(fields_path_elements: List[List[DatabasePathElement]]):
            return self.dynamodb_client.get_or_query_single_item(
                index_name=index_name or self.primary_index_name,
                key_value=key_value, fields_path_elements=fields_path_elements,
            )
        return self._get_multiple_fields(middleware=middleware, getters=getters)

    # todo: deprecated
    """
    def query(self, key_value: str, fields_paths: List[str], query_kwargs: Optional[dict] = None, limit: Optional[int] = None,
              filter_expression: Optional[Any] = None,  index_name: Optional[str] = None, **additional_kwargs) -> Optional[List[Any]]:
        fields_paths_objects = process_and_get_fields_paths_objects_from_fields_paths(
            fields_paths=fields_paths, fields_switch=self.fields_switch
        )
        query_field_path_elements: List[List[DatabasePathElement]] = []
        for field_path in fields_paths:
            field_path_elements, has_multiple_fields_path = process_and_make_single_rendered_database_path(
                field_path=field_path, fields_switch=self.fields_switch, query_kwargs=query_kwargs
            )
            query_field_path_elements.append(field_path_elements)

        response = self.dynamodb_client.query_by_key(
            index_name=index_name or self.primary_index_name,
            index_name=key_name, key_value=key_value,
            fields_path_elements=query_field_path_elements,
            query_limit=limit, filter_expression=filter_expression, 
            **additional_kwargs
        )
        if response is not None:
            for current_item in response.items:
                if isinstance(current_item, dict):
                    for current_item_key, current_item_value in current_item.items():
                        matching_field_path_object = fields_paths_objects.get(current_item_key, None)
                        if matching_field_path_object is not None:
                            if matching_field_path_object.database_path is not None:
                                matching_field_path_object.populate(value=current_item_value)
                                current_item[current_item_key], valid = matching_field_path_object.validate_data()
                                # todo: remove this non centralized response validation system
            return response.items
        else:
            return None
    """

    def update_field(self, key_value: str, field_path: str, value_to_set: Any, query_kwargs: Optional[dict] = None, index_name: Optional[str] = None) -> bool:
        def middleware(field_path_elements: List[DatabasePathElement], validated_data: Any):
            response = self.dynamodb_client.set_update_data_element_to_map_with_default_initialization(
                index_name=index_name or self.primary_index_name,
                key_value=key_value, value=validated_data,
                field_path_elements=field_path_elements
            )
            return True if response is not None else False
        return self._update_field(middleware=middleware, field_path=field_path, value_to_set=value_to_set, query_kwargs=query_kwargs)

    def update_multiple_fields(self, key_value: str, setters: List[FieldSetter or UnsafeFieldSetter], index_name: Optional[str] = None) -> bool:
        def middleware(dynamodb_setters: List[FieldPathSetter]):
            return self.dynamodb_client.set_update_multiple_data_elements_to_map(
                index_name=index_name or self.primary_index_name,
                key_value=key_value, setters=dynamodb_setters
            )
        return self._update_multiple_fields(middleware=middleware, setters=setters)

    def remove_field(self, key_value: str, field_path: str, query_kwargs: Optional[dict] = None, index_name: Optional[str] = None) -> Optional[Any]:
        def middleware(fields_path_elements: List[List[DatabasePathElement]]):
            return self.dynamodb_client.remove_data_elements_from_map(
                index_name=index_name or self.primary_index_name,
                key_value=key_value, targets_path_elements=fields_path_elements,
                retrieve_removed_elements=True
            )
        return self._remove_field(middleware=middleware, field_path=field_path, query_kwargs=query_kwargs)

    def remove_multiple_fields(self, key_value: str, removers: Dict[str, FieldRemover], index_name: Optional[str] = None) -> Dict[str, Any]:
        def task_executor(remover_item: FieldRemover):
            return self.remove_field(
                key_value=key_value, index_name=index_name,
                field_path=remover_item.field_path,
                query_kwargs=remover_item.query_kwargs
            )
        return self._async_field_removers_executor(task_executor=task_executor, removers=removers)

    def delete_field(self, key_value: str, field_path: str, query_kwargs: Optional[dict] = None, index_name: Optional[str] = None) -> bool:
        def middleware(fields_path_elements: List[List[DatabasePathElement]]):
            return self.dynamodb_client.remove_data_elements_from_map(
                index_name=index_name or self.primary_index_name,
                key_value=key_value, targets_path_elements=fields_path_elements,
                retrieve_removed_elements=False
            )
        return self._delete_field(middleware=middleware, field_path=field_path, query_kwargs=query_kwargs)

    def delete_multiple_fields(self, key_value: str, removers: Dict[str, FieldRemover], index_name: Optional[str] = None) -> Dict[str, bool]:
        def task_executor(remover_item: FieldRemover):
            return self.delete_field(
                key_value=key_value, index_name=index_name,
                field_path=remover_item.field_path,
                query_kwargs=remover_item.query_kwargs
            )
        return self._async_field_removers_executor(task_executor=task_executor, removers=removers)

    def grouped_remove_multiple_fields(self, key_value: str, removers: Dict[str, FieldRemover], index_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
        def middleware(fields_path_elements: List[List[DatabasePathElement]]):
            return self.dynamodb_client.remove_data_elements_from_map(
                index_name=index_name or self.primary_index_name,
                key_value=key_value, targets_path_elements=fields_path_elements,
                retrieve_removed_elements=True
            )
        return self._grouped_remove_multiple_fields(middleware=middleware, removers=removers)

    def grouped_delete_multiple_fields(self, key_value: str, removers: List[FieldRemover], index_name: Optional[str] = None) -> bool:
        def middleware(fields_path_elements: List[List[DatabasePathElement]]):
            return self.dynamodb_client.remove_data_elements_from_map(
                index_name=index_name or self.primary_index_name,
                key_value=key_value, targets_path_elements=fields_path_elements,
                retrieve_removed_elements=False
            )
        return self._grouped_delete_multiple_fields(middleware=middleware, removers=removers)
