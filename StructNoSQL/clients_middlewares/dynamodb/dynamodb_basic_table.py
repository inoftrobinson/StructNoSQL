import boto3
from typing import Optional, List, Dict, Any, Tuple, Union, Generator, Type

from StructNoSQL import TableDataModel
from StructNoSQL.clients_middlewares.dynamodb.backend.dynamodb_core import DynamoDbCoreAdapter, PrimaryIndex, GlobalSecondaryIndex
from StructNoSQL.clients_middlewares.dynamodb.backend.dynamodb_utils import DynamoDBUtils
from StructNoSQL.clients_middlewares.dynamodb.backend.models import Response
from StructNoSQL.models import DatabasePathElement, FieldGetter, FieldSetter, UnsafeFieldSetter, FieldRemover, \
    FieldPathSetter, QueryMetadata
from StructNoSQL.practical_logger import message_with_vars
from StructNoSQL.tables.base_basic_table import BaseBasicTable
from StructNoSQL.clients_middlewares.dynamodb.dynamodb_low_level_table_operations import DynamoDBLowLevelTableOperations
from StructNoSQL.tables.shared_table_behaviors import _prepare_getters


class DynamoDBBasicTable(BaseBasicTable, DynamoDBLowLevelTableOperations):
    def __init__(
            self, table_name: str, region_name: str,
            data_model: Type[TableDataModel], primary_index: PrimaryIndex,
            billing_mode: str = DynamoDbCoreAdapter.PAY_PER_REQUEST,
            global_secondary_indexes: List[GlobalSecondaryIndex] = None,
            auto_create_table: bool = True,
            boto_session: Optional[boto3.Session] = None
    ):
        super().__init__(data_model=data_model, primary_index=primary_index)
        super().__setup_connectors__(
            table_name=table_name, region_name=region_name,
            primary_index=primary_index, global_secondary_indexes=global_secondary_indexes,
            billing_mode=billing_mode, auto_create_table=auto_create_table,
            boto_session=boto_session
        )

    def put_record(self, record_dict_data: dict) -> bool:
        def middleware(validated_record_item: dict) -> bool:
            return self.dynamodb_client.put_record(item_dict=validated_record_item)
        return self._put_record(middleware=middleware, record_dict_data=record_dict_data)

    def delete_record(self, indexes_keys_selectors: dict) -> bool:
        def middleware(indexes_keys: dict) -> bool:
            return self.dynamodb_client.delete_record(indexes_keys_selectors=indexes_keys)
        return self._delete_record(middleware=middleware, indexes_keys_selectors=indexes_keys_selectors)

    def remove_record(self, indexes_keys_selectors: dict, data_validation: bool = True) -> Optional[dict]:
        def middleware(indexes_keys: dict) -> Optional[dict]:
            return self.dynamodb_client.remove_record(indexes_keys_selectors=indexes_keys)
        return self._remove_record(middleware=middleware, indexes_keys_selectors=indexes_keys_selectors, data_validation=data_validation)

    def get_field(self, key_value: str, field_path: str, query_kwargs: Optional[dict] = None, index_name: Optional[str] = None, data_validation: bool = True) -> Any:
        def middleware(field_path_elements: Union[List[DatabasePathElement], Dict[str, List[DatabasePathElement]]], is_multi_selector: bool):
            if is_multi_selector is not True:
                field_path_elements: List[DatabasePathElement]
                response_data: Optional[Any] = self.dynamodb_client.get_value_in_path_target(
                    index_name=index_name or self.primary_index_name,
                    key_value=key_value, field_path_elements=field_path_elements
                )
                return response_data
            else:
                field_path_elements: Dict[str, List[DatabasePathElement]]
                response_data: Optional[Dict[str, Any]] = self.dynamodb_client.get_values_in_multiple_path_target(
                    index_name=index_name or self.primary_index_name,
                    key_value=key_value, fields_path_elements=field_path_elements
                )
                if response_data is not None:
                    return {key: response_data.get(key, None) for key in field_path_elements.keys()}
                else:
                    # When using multi-selectors, the middleware is always expected to return a dict with all the
                    # 'getters' keys present, hence why the dict with None values if the response_data is None.
                    return {key: None for key in field_path_elements.keys()}
        return self._get_field(middleware=middleware, field_path=field_path, query_kwargs=query_kwargs, data_validation=data_validation)

    def get_multiple_fields(self, key_value: str, getters: Dict[str, FieldGetter], index_name: Optional[str] = None, data_validation: bool = True) -> Optional[dict]:
        def middleware(fields_path_elements: List[List[DatabasePathElement]]):
            return self.dynamodb_client.get_or_query_single_item(
                index_name=index_name or self.primary_index_name,
                key_value=key_value, fields_path_elements=fields_path_elements,
            )
        return self._get_multiple_fields(middleware=middleware, getters=getters, data_validation=data_validation)

    def query_field(
            self, key_value: str, field_path: str, query_kwargs: Optional[dict] = None, index_name: Optional[str] = None,
            filter_expression: Optional[Any] = None, pagination_records_limit: Optional[int] = None, exclusive_start_key: Optional[Any] = None,
            data_validation: bool = True, **additional_kwargs
    ) -> Tuple[Optional[dict], QueryMetadata]:
        def middleware(fields_path_elements: List[List[DatabasePathElement]]) -> Tuple[Optional[List[dict]], QueryMetadata]:
            return self.dynamodb_client.query_items_by_key(
                index_name=index_name or self.primary_index_name,
                key_value=key_value, fields_path_elements=fields_path_elements,
                pagination_records_limit=pagination_records_limit, filter_expression=filter_expression,
                exclusive_start_key=exclusive_start_key,
                **additional_kwargs
            )
        return self._query_field(
            middleware=middleware, key_value=key_value, field_path=field_path,
            query_kwargs=query_kwargs, index_name=index_name, data_validation=data_validation
        )

    def paginated_query_field(
            self, key_value: str, field_path: str, query_kwargs: Optional[dict] = None, index_name: Optional[str] = None,
            filter_expression: Optional[Any] = None, pagination_records_limit: Optional[int] = None, exclusive_start_key: Optional[Any] = None,
            data_validation: bool = True, **additional_kwargs
    ) -> Generator[Tuple[Optional[dict], QueryMetadata], None, None]:
        current_exclusive_start_key: Optional[Any] = exclusive_start_key
        while True:
            records_data, query_metadata = self.query_field(
                index_name=index_name, key_value=key_value, field_path=field_path, query_kwargs=query_kwargs,
                pagination_records_limit=pagination_records_limit, filter_expression=filter_expression,
                exclusive_start_key=current_exclusive_start_key, data_validation=data_validation, **additional_kwargs
            )
            yield records_data, query_metadata
            if query_metadata.last_evaluated_key is None:
                break
            current_exclusive_start_key = query_metadata.last_evaluated_key

    def query_multiple_fields(
            self, key_value: str, getters: Dict[str, FieldGetter], index_name: Optional[str] = None,
            filter_expression: Optional[Any] = None, pagination_records_limit: Optional[int] = None, exclusive_start_key: Optional[Any] = None,
            data_validation: bool = True, **additional_kwargs
    ) -> Tuple[Optional[Dict[str, dict]], QueryMetadata]:
        def middleware(fields_path_elements: List[List[DatabasePathElement]]) -> Tuple[List[dict], QueryMetadata]:
            return self.dynamodb_client.query_items_by_key(
                index_name=index_name or self.primary_index_name, key_value=key_value,
                fields_path_elements=fields_path_elements,
                pagination_records_limit=pagination_records_limit, filter_expression=filter_expression,
                exclusive_start_key=exclusive_start_key,
                **additional_kwargs
            )
        return self._query_multiple_fields(
            middleware=middleware, key_value=key_value, getters=getters,
            index_name=index_name, data_validation=data_validation
        )

    def paginated_query_multiple_fields(
            self, key_value: str, getters: Dict[str, FieldGetter], index_name: Optional[str] = None,
            filter_expression: Optional[Any] = None, pagination_records_limit: Optional[int] = None, exclusive_start_key: Optional[Any] = None,
            data_validation: bool = True, **additional_kwargs
    ) -> Generator[Tuple[Optional[Dict[str, dict]], QueryMetadata], None, None]:
        current_exclusive_start_key: Optional[Any] = exclusive_start_key
        while True:
            records_data, query_metadata = self.query_multiple_fields(
                key_value=key_value, getters=getters, index_name=index_name,
                pagination_records_limit=pagination_records_limit,
                filter_expression=filter_expression,
                exclusive_start_key=current_exclusive_start_key,
                data_validation=data_validation, **additional_kwargs
            )
            yield records_data, query_metadata
            if query_metadata.last_evaluated_key is None:
                break
            current_exclusive_start_key = query_metadata.last_evaluated_key

    def lightweight_query_multiple_fields(
            self, key_value: str, getters: Dict[str, FieldGetter], index_name: Optional[str] = None,
            pagination_records_limit: Optional[int] = None, filter_expression: Optional[Any] = None, **additional_kwargs
    ) -> Optional[List[dict]]:

        getters_database_paths, single_getters_database_paths_elements, grouped_getters_database_paths_elements = (
            _prepare_getters(fields_switch=self.fields_switch, getters=getters)
        )
        response = self.dynamodb_client.query_response_by_key(
            index_name=index_name or self.primary_index_name, key_value=key_value,
            fields_path_elements=getters_database_paths,
            pagination_records_limit=pagination_records_limit, filter_expression=filter_expression,
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

    def update_field_return_old(
            self, key_value: str, field_path: str, value_to_set: Any,
            query_kwargs: Optional[dict] = None, data_validation: bool = True
    ) -> Tuple[bool, Optional[Any]]:
        def middleware(field_path_elements: List[DatabasePathElement], validated_data: Any) -> Tuple[bool, Optional[dict]]:
            response: Optional[Response] = self.dynamodb_client.set_update_data_element_to_map_with_default_initialization(
                index_name=self.primary_index_name,
                key_value=key_value, value=validated_data,
                field_path_elements=field_path_elements,
                return_old_value=True
            )
            if response is None:
                return False, None

            python_response_attributes: Optional[dict] = (
                DynamoDBUtils.dynamodb_to_python(response.attributes)
                if response.attributes is not None else None
            )
            return True, python_response_attributes
        return self._update_field_return_old(
            middleware=middleware, field_path=field_path, value_to_set=value_to_set,
            query_kwargs=query_kwargs, data_validation=data_validation
        )

    def update_multiple_fields(self, key_value: str, setters: List[FieldSetter or UnsafeFieldSetter]) -> bool:
        def middleware(dynamodb_setters: List[FieldPathSetter]) -> bool:
            response: Optional[Response] = self.dynamodb_client.set_update_multiple_data_elements_to_map(
                index_name=self.primary_index_name, key_value=key_value,
                setters=dynamodb_setters, return_old_values=False
            )
            return response is not None
        return self._update_multiple_fields(middleware=middleware, setters=setters)

    def update_multiple_fields_return_old(self, key_value: str, setters: Dict[str, FieldSetter], data_validation: bool = True) -> Tuple[bool, Dict[str, Optional[Any]]]:
        def middleware(dynamodb_setters: Dict[str, FieldPathSetter]) -> Tuple[bool, Optional[dict]]:
            response: Optional[Response] = self.dynamodb_client.set_update_multiple_data_elements_to_map(
                index_name=self.primary_index_name, key_value=key_value,
                setters=list(dynamodb_setters.values()), return_old_values=True
            )
            if response is None:
                return False, None

            python_response_attributes: Optional[dict] = (
                DynamoDBUtils.dynamodb_to_python(response.attributes)
                if response.attributes is not None else None
            )
            return True, python_response_attributes
        return self._update_multiple_fields_return_old(middleware=middleware, setters=setters, data_validation=data_validation)

    def remove_field(self, key_value: str, field_path: str, query_kwargs: Optional[dict] = None, data_validation: bool = True) -> Optional[Any]:
        def middleware(fields_path_elements: List[List[DatabasePathElement]]) -> Optional[Dict[str, Any]]:
            return self.dynamodb_client.remove_data_elements_from_map(
                index_name=self.primary_index_name,
                key_value=key_value, targets_path_elements=fields_path_elements,
                retrieve_removed_elements=True
            )
        return self._remove_field(middleware=middleware, field_path=field_path, query_kwargs=query_kwargs, data_validation=data_validation)

    def remove_multiple_fields(self, key_value: str, removers: Dict[str, FieldRemover], data_validation: bool = True) -> Dict[str, Optional[Any]]:
        def task_executor(remover_item: FieldRemover):
            return self.remove_field(
                key_value=key_value,
                field_path=remover_item.field_path,
                query_kwargs=remover_item.query_kwargs,
                data_validation=data_validation
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

    def grouped_remove_multiple_fields(self, key_value: str, removers: Dict[str, FieldRemover], data_validation: bool = True) -> Optional[Dict[str, Any]]:
        def middleware(fields_path_elements: List[List[DatabasePathElement]]):
            return self.dynamodb_client.remove_data_elements_from_map(
                index_name=self.primary_index_name,
                key_value=key_value, targets_path_elements=fields_path_elements,
                retrieve_removed_elements=True
            )
        return self._grouped_remove_multiple_fields(middleware=middleware, removers=removers, data_validation=data_validation)

    def grouped_delete_multiple_fields(self, key_value: str, removers: List[FieldRemover]) -> bool:
        def middleware(fields_path_elements: List[List[DatabasePathElement]]):
            return self.dynamodb_client.remove_data_elements_from_map(
                index_name=self.primary_index_name,
                key_value=key_value, targets_path_elements=fields_path_elements,
                retrieve_removed_elements=False
            )
        return self._grouped_delete_multiple_fields(middleware=middleware, removers=removers)
