from typing import Optional, List, Dict, Any

from StructNoSQL.middlewares.dynamodb.backend.dynamodb_core import DynamoDbCoreAdapter, PrimaryIndex, GlobalSecondaryIndex
from StructNoSQL.middlewares.dynamodb.backend.models import Response
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
        super().__init__(data_model=data_model, primary_index=primary_index)
        super().__setup_connectors__(
            table_name=table_name, region_name=region_name,
            primary_index=primary_index, global_secondary_indexes=global_secondary_indexes,
            billing_mode=billing_mode, auto_create_table=auto_create_table
        )

    def put_record(self, record_dict_data: dict) -> bool:
        def middleware(validated_record_item: dict) -> bool:
            return self.dynamodb_client.put_record(item_dict=validated_record_item)
        return self._put_record(middleware=middleware, record_dict_data=record_dict_data)

    def delete_record(self, indexes_keys_selectors: dict) -> bool:
        def middleware(indexes_keys: dict) -> bool:
            return self.dynamodb_client.delete_record(indexes_keys_selectors=indexes_keys)
        return self._delete_record(middleware=middleware, indexes_keys_selectors=indexes_keys_selectors)

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


    def query_field(
            self, key_value: str, field_path: str, query_kwargs: Optional[dict] = None, index_name: Optional[str] = None,
            records_query_limit: Optional[int] = None, filter_expression: Optional[Any] = None, **additional_kwargs
    ) -> Optional[dict]:
        def middleware(field_path_elements: List[DatabasePathElement] or Dict[str, List[DatabasePathElement]], has_multiple_fields_path: bool) -> List[dict]:
            return self.dynamodb_client.query_items_by_key(
                index_name=index_name or self.primary_index_name,
                key_value=key_value, field_path_elements=field_path_elements,
                has_multiple_fields_path=has_multiple_fields_path,
                query_limit=records_query_limit, filter_expression=filter_expression,
                **additional_kwargs
            )
        return self._query_field(middleware=middleware, key_value=key_value, field_path=field_path, query_kwargs=query_kwargs, index_name=index_name)

    """def query_field(
            self, key_value: str, field_path: str, query_kwargs: Optional[dict] = None, index_name: Optional[str] = None,
            records_query_limit: Optional[int] = None, filter_expression: Optional[Any] = None, **additional_kwargs
    ) -> Optional[dict]:

        from StructNoSQL.utils.process_render_fields_paths import process_and_make_single_rendered_database_path
        field_path_elements, has_multiple_fields_path = process_and_make_single_rendered_database_path(
            field_path=field_path, fields_switch=self.fields_switch, query_kwargs=query_kwargs
        )
        final_fields_path_elements: List[List[DatabasePathElement]] = (
            [field_path_elements] if has_multiple_fields_path is not True else list(field_path_elements.values())
        )

        response = self.dynamodb_client.query_response_by_key(
            index_name=index_name or self.primary_index_name, key_value=key_value,
            fields_path_elements=final_fields_path_elements,
            query_limit=records_query_limit, filter_expression=filter_expression,
            **additional_kwargs
        )
        if response is None:
            return None

        output: List[Any] = []
        for record_item_data in response.items:
            if isinstance(record_item_data, dict):
                from StructNoSQL.utils.data_processing import navigate_into_data_with_field_path_elements
                if has_multiple_fields_path is not True:
                    output.append(navigate_into_data_with_field_path_elements(
                        data=record_item_data, field_path_elements=field_path_elements,
                        num_keys_to_navigation_into=len(field_path_elements)
                    ))
                else:
                    output.append(self.dynamodb_client._unpack_multiple_retrieved_fields(
                        item_data=record_item_data, fields_path_elements=field_path_elements,
                        num_keys_to_stop_at_before_reaching_end_of_item=0, metadata=False
                    ))
                # todo: add data validation
        return output"""

    def query_multiple_fields(
            self, key_value: str, getters: Dict[str, FieldGetter], index_name: Optional[str] = None,
            records_query_limit: Optional[int] = None, filter_expression: Optional[Any] = None, **additional_kwargs
    ):
        def middleware(fields_path_elements: Dict[str, List[DatabasePathElement]], _) -> List[dict]:
            return self.dynamodb_client.query_items_by_key(
                index_name=index_name or self.primary_index_name,
                key_value=key_value, field_path_elements=fields_path_elements,
                has_multiple_fields_path=True,
                                query_limit=records_query_limit, filter_expression=filter_expression,
                **additional_kwargs
            )
        return self._query_multiple_fields(middleware=middleware, key_value=key_value, getters=getters)

    def query_multiple_fields(
            self, key_value: str, getters: Dict[str, FieldGetter], index_name: Optional[str] = None,
            records_query_limit: Optional[int] = None, filter_expression: Optional[Any] = None, **additional_kwargs
    ) -> Optional[List[Dict[str, Any]]]:

        getters_database_paths, single_getters_database_paths_elements, grouped_getters_database_paths_elements = self._prepare_getters(getters=getters)
        response = self.dynamodb_client.query_response_by_key(
            index_name=index_name or self.primary_index_name, key_value=key_value,
            fields_path_elements=getters_database_paths,
            query_limit=records_query_limit, filter_expression=filter_expression,
            **additional_kwargs
        )
        if response is None:
            return None

        output: List[Dict[str, Any]] = []
        for record_item_data in response.items:
            if isinstance(record_item_data, dict):
                output.append(self._unpack_getters_response_item(
                    response_item=record_item_data,
                    single_getters_database_paths_elements=single_getters_database_paths_elements,
                    grouped_getters_database_paths_elements=grouped_getters_database_paths_elements
                ))
                # todo: add data validation
        return output

        """fields_paths_objects = process_and_get_fields_paths_objects_from_fields_paths(
            fields_paths=fields_paths, fields_switch=self.fields_switch
        )
        query_field_path_elements: List[List[DatabasePathElement]] = []
        for field_path in fields_paths:
            field_path_elements, has_multiple_fields_path = process_and_make_single_rendered_database_path(
                field_path=field_path, fields_switch=self.fields_switch, query_kwargs=query_kwargs
            )
            query_field_path_elements.append(field_path_elements)

        response = self.dynamodb_client.query_response_by_key(
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

    def update_field(self, key_value: str, field_path: str, value_to_set: Any, query_kwargs: Optional[dict] = None) -> bool:
        def middleware(field_path_elements: List[DatabasePathElement], validated_data: Any):
            response: Optional[Response] = self.dynamodb_client.set_update_data_element_to_map_with_default_initialization(
                index_name=self.primary_index_name,
                key_value=key_value, value=validated_data,
                field_path_elements=field_path_elements
            )
            return True if response is not None else False
        return self._update_field(middleware=middleware, field_path=field_path, value_to_set=value_to_set, query_kwargs=query_kwargs)

    def update_multiple_fields(self, key_value: str, setters: List[FieldSetter or UnsafeFieldSetter]) -> bool:
        def middleware(dynamodb_setters: List[FieldPathSetter]):
            return self.dynamodb_client.set_update_multiple_data_elements_to_map(
                index_name=self.primary_index_name, key_value=key_value, setters=dynamodb_setters
            )
        return self._update_multiple_fields(middleware=middleware, setters=setters)

    def remove_field(self, key_value: str, field_path: str, query_kwargs: Optional[dict] = None) -> Optional[Any]:
        def middleware(fields_path_elements: List[List[DatabasePathElement]]):
            return self.dynamodb_client.remove_data_elements_from_map(
                index_name=self.primary_index_name,
                key_value=key_value, targets_path_elements=fields_path_elements,
                retrieve_removed_elements=True
            )
        return self._remove_field(middleware=middleware, field_path=field_path, query_kwargs=query_kwargs)

    def remove_multiple_fields(self, key_value: str, removers: Dict[str, FieldRemover]) -> Dict[str, Any]:
        def task_executor(remover_item: FieldRemover):
            return self.remove_field(
                key_value=key_value,
                field_path=remover_item.field_path,
                query_kwargs=remover_item.query_kwargs
            )
        return self._async_field_removers_executor(task_executor=task_executor, removers=removers)

    def delete_field(self, key_value: str, field_path: str, query_kwargs: Optional[dict] = None) -> bool:
        def middleware(fields_path_elements: List[List[DatabasePathElement]]):
            return self.dynamodb_client.remove_data_elements_from_map(
                index_name=self.primary_index_name,
                key_value=key_value, targets_path_elements=fields_path_elements,
                retrieve_removed_elements=False
            )
        return self._delete_field(middleware=middleware, field_path=field_path, query_kwargs=query_kwargs)

    def delete_multiple_fields(self, key_value: str, removers: Dict[str, FieldRemover]) -> Dict[str, bool]:
        def task_executor(remover_item: FieldRemover):
            return self.delete_field(
                key_value=key_value,
                field_path=remover_item.field_path,
                query_kwargs=remover_item.query_kwargs
            )
        return self._async_field_removers_executor(task_executor=task_executor, removers=removers)

    def grouped_remove_multiple_fields(self, key_value: str, removers: Dict[str, FieldRemover]) -> Optional[Dict[str, Any]]:
        def middleware(fields_path_elements: List[List[DatabasePathElement]]):
            return self.dynamodb_client.remove_data_elements_from_map(
                index_name=self.primary_index_name,
                key_value=key_value, targets_path_elements=fields_path_elements,
                retrieve_removed_elements=True
            )
        return self._grouped_remove_multiple_fields(middleware=middleware, removers=removers)

    def grouped_delete_multiple_fields(self, key_value: str, removers: List[FieldRemover]) -> bool:
        def middleware(fields_path_elements: List[List[DatabasePathElement]]):
            return self.dynamodb_client.remove_data_elements_from_map(
                index_name=self.primary_index_name,
                key_value=key_value, targets_path_elements=fields_path_elements,
                retrieve_removed_elements=False
            )
        return self._grouped_delete_multiple_fields(middleware=middleware, removers=removers)
