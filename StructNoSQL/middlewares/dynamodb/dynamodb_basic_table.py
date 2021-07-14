from typing import Optional, List, Dict, Any, Tuple

from StructNoSQL.middlewares.dynamodb.backend.dynamodb_core import DynamoDbCoreAdapter, PrimaryIndex, GlobalSecondaryIndex
from StructNoSQL.middlewares.dynamodb.backend.dynamodb_utils import DynamoDBUtils
from StructNoSQL.middlewares.dynamodb.backend.models import Response
from StructNoSQL.models import DatabasePathElement, FieldGetter, FieldSetter, UnsafeFieldSetter, FieldRemover, FieldPathSetter
from StructNoSQL.practical_logger import message_with_vars
from StructNoSQL.tables.base_basic_table import BaseBasicTable
from StructNoSQL.middlewares.dynamodb.dynamodb_low_level_table_operations import DynamoDBLowLevelTableOperations
from StructNoSQL.tables.shared_table_behaviors import _prepare_getters


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
        return self._record_deletion(middleware=middleware, indexes_keys_selectors=indexes_keys_selectors)

    def remove_record(self, indexes_keys_selectors: dict) -> Optional[dict]:
        def middleware(indexes_keys: dict) -> Optional[dict]:
            return self.dynamodb_client.remove_record(indexes_keys_selectors=indexes_keys)
        return self._record_deletion(middleware=middleware, indexes_keys_selectors=indexes_keys_selectors)

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

    def query_multiple_fields(
            self, key_value: str, getters: Dict[str, FieldGetter], index_name: Optional[str] = None,
            records_query_limit: Optional[int] = None, filter_expression: Optional[Any] = None, **additional_kwargs
    ) -> Optional[Dict[str, dict]]:
        def middleware(fields_path_elements: Dict[str, List[DatabasePathElement]], _) -> List[dict]:
            return self.dynamodb_client.query_items_by_key(
                index_name=index_name or self.primary_index_name, has_multiple_fields_path=True,
                key_value=key_value, field_path_elements=fields_path_elements,
                query_limit=records_query_limit, filter_expression=filter_expression,
                **additional_kwargs
            )
        return self._query_multiple_fields(middleware=middleware, key_value=key_value, getters=getters, index_name=index_name)

    def lightweight_query_multiple_fields(
            self, key_value: str, getters: Dict[str, FieldGetter], index_name: Optional[str] = None,
            records_query_limit: Optional[int] = None, filter_expression: Optional[Any] = None, **additional_kwargs
    ) -> Optional[List[dict]]:

        getters_database_paths, single_getters_database_paths_elements, grouped_getters_database_paths_elements = (
            _prepare_getters(fields_switch=self.fields_switch, getters=getters)
        )
        response = self.dynamodb_client.query_response_by_key(
            index_name=index_name or self.primary_index_name, key_value=key_value,
            fields_path_elements=getters_database_paths,
            query_limit=records_query_limit, filter_expression=filter_expression,
            **additional_kwargs
        )
        if response is None:
            return None

        output: List[Dict[str, Any]] = [
            self._unpack_getters_response_item(
                response_item=record_item_data,
                single_getters_database_paths_elements=single_getters_database_paths_elements,
                grouped_getters_database_paths_elements=grouped_getters_database_paths_elements
            )
            # todo: add data validation
            for record_item_data in response.items
            if isinstance(record_item_data, dict)
        ]
        return output

    def update_field(self, key_value: str, field_path: str, value_to_set: Any, query_kwargs: Optional[dict] = None) -> bool:
        def middleware(field_path_elements: List[DatabasePathElement], validated_data: Any):
            response: Optional[Response] = self.dynamodb_client.set_update_data_element_to_map_with_default_initialization(
                index_name=self.primary_index_name,
                key_value=key_value, value=validated_data,
                field_path_elements=field_path_elements
            )
            return response is not None
        return self._update_field(middleware=middleware, field_path=field_path, value_to_set=value_to_set, query_kwargs=query_kwargs)

    def update_field_return_old(self, key_value: str, field_path: str, value_to_set: Any, query_kwargs: Optional[dict] = None) -> Tuple[bool, Optional[Any]]:
        def middleware(field_path_elements: List[DatabasePathElement], validated_data: Any) -> Tuple[bool, Optional[dict]]:
            response: Optional[Response] = self.dynamodb_client.set_update_data_element_to_map_with_default_initialization(
                index_name=self.primary_index_name,
                key_value=key_value, value=validated_data,
                field_path_elements=field_path_elements,
                return_old_value=True
            )
            if response is None:
                return False, None

            response_attributes: Optional[dict] = (
                DynamoDBUtils.dynamodb_to_python_higher_level(response.attributes)
                if response.attributes is not None else None
            )
            return True, response_attributes
        return self._update_field_return_old(middleware=middleware, field_path=field_path, value_to_set=value_to_set, query_kwargs=query_kwargs)

    def update_multiple_fields(self, key_value: str, setters: List[FieldSetter or UnsafeFieldSetter]) -> bool:
        def middleware(dynamodb_setters: List[FieldPathSetter]) -> bool:
            response: Optional[Response] = self.dynamodb_client.set_update_multiple_data_elements_to_map(
                index_name=self.primary_index_name, key_value=key_value,
                setters=dynamodb_setters, return_old_values=False
            )
            return response is not None
        return self._update_multiple_fields(middleware=middleware, setters=setters)

    def update_multiple_fields_return_old(self, key_value: str, setters: Dict[str, FieldSetter]) -> Tuple[bool, Dict[str, Optional[Any]]]:
        def middleware(dynamodb_setters: Dict[str, FieldPathSetter]) -> Tuple[bool, Optional[dict]]:
            response: Optional[Response] = self.dynamodb_client.set_update_multiple_data_elements_to_map(
                index_name=self.primary_index_name, key_value=key_value,
                setters=list(dynamodb_setters.values()), return_old_values=True
            )
            if response is None:
                return False, None

            python_response_attributes: Optional[dict] = (
                DynamoDBUtils.dynamodb_to_python_higher_level(response.attributes)
                if response.attributes is not None else None
            )
            return True, python_response_attributes
        return self._update_multiple_fields_return_old(middleware=middleware, setters=setters)

    def remove_field(self, key_value: str, field_path: str, query_kwargs: Optional[dict] = None) -> Optional[Any]:
        def middleware(fields_path_elements: List[List[DatabasePathElement]]) -> Optional[Dict[str, Any]]:
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
        def middleware(fields_path_elements: List[List[DatabasePathElement]]) -> bool:
            removed_items = self.dynamodb_client.remove_data_elements_from_map(
                index_name=self.primary_index_name,
                key_value=key_value, targets_path_elements=fields_path_elements,
                retrieve_removed_elements=False
            )
            return removed_items is not None
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
