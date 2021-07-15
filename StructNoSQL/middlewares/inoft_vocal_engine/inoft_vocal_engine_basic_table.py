from typing import Optional, List, Dict, Any, Tuple, Union

from StructNoSQL import PrimaryIndex
from StructNoSQL.models import DatabasePathElement, FieldGetter, FieldSetter, UnsafeFieldSetter, FieldRemover, FieldPathSetter
from StructNoSQL.practical_logger import message_with_vars
from StructNoSQL.tables.base_basic_table import BaseBasicTable
from StructNoSQL.middlewares.inoft_vocal_engine.inoft_vocal_engine_table_connectors import InoftVocalEngineTableConnectors


class InoftVocalEngineBasicTable(BaseBasicTable, InoftVocalEngineTableConnectors):
    def __init__(
            self, engine_account_id: str, engine_project_id: str, engine_api_key: str,
            table_id: str, region_name: str, data_model
    ):
        super().__init__(data_model=data_model, primary_index=PrimaryIndex(
            hash_key_name='accountProjectUserId', hash_key_variable_python_type=str
        ))
        self.__setup_connectors__(
            engine_account_id=engine_account_id,
            engine_project_id=engine_project_id,
            engine_api_key=engine_api_key,
            table_id=table_id, region_name=region_name
        )

    def put_record(self, record_dict_data: dict) -> bool:
        def middleware(validated_record_item: dict) -> bool:
            return self._put_record_request(record_item_data=validated_record_item)
        return self._put_record(middleware=middleware, record_dict_data=record_dict_data)

    def delete_record(self, indexes_keys_selectors: dict) -> bool:
        def middleware(indexes_keys: dict) -> bool:
            return self._delete_record_request(indexes_keys_selectors=indexes_keys)
        return self._record_deletion(middleware=middleware, indexes_keys_selectors=indexes_keys_selectors)

    def remove_record(self, indexes_keys_selectors: dict) -> Optional[dict]:
        def middleware(indexes_keys: dict) -> Optional[dict]:
            return self._remove_record_request(indexes_keys_selectors=indexes_keys)
        return self._record_deletion(middleware=middleware, indexes_keys_selectors=indexes_keys_selectors)

    def query_field(
            self, key_value: str, field_path: str, query_kwargs: Optional[dict] = None, records_query_limit: Optional[int] = None,
            filter_expression: Optional[Any] = None, data_validation: bool = True, **additional_kwargs
    ) -> Optional[dict]:
        def middleware(field_path_elements: List[DatabasePathElement] or Dict[str, List[DatabasePathElement]], has_multiple_fields_path: bool) -> List[dict]:
            return self._query_items_by_key(
                key_value=key_value, field_path_elements=field_path_elements,
                has_multiple_fields_path=has_multiple_fields_path,
                query_limit=records_query_limit, filter_expression=filter_expression,
                **additional_kwargs
            )
        return self._query_field(
            middleware=middleware, key_value=key_value, field_path=field_path,
            query_kwargs=query_kwargs, index_name=None, data_validation=data_validation
        )

    def query_multiple_fields(
            self, key_value: str, getters: Dict[str, FieldGetter], records_query_limit: Optional[int] = None,
            filter_expression: Optional[Any] = None, data_validation: bool = True, **additional_kwargs
    ):
        def middleware(fields_path_elements: Dict[str, List[DatabasePathElement]], _) -> List[dict]:
            return self._query_items_by_key(
                key_value=key_value, field_path_elements=fields_path_elements, has_multiple_fields_path=True,
                query_limit=records_query_limit, filter_expression=filter_expression, **additional_kwargs
            )
        return self._query_multiple_fields(
            middleware=middleware, key_value=key_value, getters=getters,
            index_name=None, data_validation=data_validation
        )

    def get_field(self, key_value: str, field_path: str, query_kwargs: Optional[dict] = None, data_validation: bool = True) -> Any:
        def middleware(field_path_elements: Union[List[DatabasePathElement], Dict[str, List[DatabasePathElement]]], has_multiple_fields_path: bool):
            if has_multiple_fields_path is not True:
                field_path_elements: List[DatabasePathElement]
                return self._get_single_value_in_path_target(key_value=key_value, field_path_elements=field_path_elements)
            else:
                field_path_elements: Dict[str, List[DatabasePathElement]]
                return self._get_values_in_multiple_path_target(key_value=key_value, fields_path_elements=field_path_elements)
        return self._get_field(middleware=middleware, field_path=field_path, query_kwargs=query_kwargs, data_validation=data_validation)

    def get_multiple_fields(self, key_value: str, getters: Dict[str, FieldGetter], data_validation: bool = True) -> Optional[dict]:
        def middleware(fields_path_elements: List[List[DatabasePathElement]]):
            return self._get_or_query_single_item(
                key_value=key_value, fields_path_elements=fields_path_elements,
            )
        return self._get_multiple_fields(middleware=middleware, getters=getters, data_validation=data_validation)
        
    # todo: implement query_field and query_multiple_fields

    def update_field(self, key_value: str, field_path: str, value_to_set: Any, query_kwargs: Optional[dict] = None) -> bool:
        def middleware(field_path_elements: List[DatabasePathElement], validated_data: Any):
            response = self._set_update_data_element_to_map_with_default_initialization(
                key_value=key_value, value=validated_data,
                field_path_elements=field_path_elements
            )
            return True if response is not None else False
        return self._update_field(middleware=middleware, field_path=field_path, value_to_set=value_to_set, query_kwargs=query_kwargs)

    def update_field_return_old(
            self, key_value: str, field_path: str, value_to_set: Any,
            query_kwargs: Optional[dict] = None, data_validation: bool = True
    ) -> Tuple[bool, Optional[Any]]:
        def middleware(field_path_elements: List[DatabasePathElement], validated_data: Any) -> Tuple[bool, Optional[Any]]:
            update_success, response_attributes = self._set_update_data_element_to_map_with_default_initialization_return_old(
                key_value=key_value, value=validated_data,
                field_path_elements=field_path_elements
            )
            return update_success, response_attributes
        return self._update_field_return_old(
            middleware=middleware, field_path=field_path, value_to_set=value_to_set,
            query_kwargs=query_kwargs, data_validation=data_validation
        )

    def update_multiple_fields(self, key_value: str, setters: List[FieldSetter or UnsafeFieldSetter]) -> bool:
        def middleware(dynamodb_setters: List[FieldPathSetter]) -> bool:
            update_success: bool = self._set_update_multiple_data_elements_to_map(
                key_value=key_value, setters=dynamodb_setters
            )
            return update_success
        return self._update_multiple_fields(middleware=middleware, setters=setters)

    def update_multiple_fields_return_old(self, key_value: str, setters: Dict[str, FieldSetter], data_validation: bool = True) -> Tuple[bool, Dict[str, Optional[Any]]]:
        def middleware(dynamodb_setters: Dict[str, FieldPathSetter]) -> Tuple[bool, dict]:
            update_success, response_attributes = self._set_update_multiple_data_elements_to_map_return_old(
                key_value=key_value, setters=list(dynamodb_setters.values())
            )
            return update_success, response_attributes
        return self._update_multiple_fields_return_old(middleware=middleware, setters=setters, data_validation=data_validation)

    def remove_field(self, key_value: str, field_path: str, query_kwargs: Optional[dict] = None, data_validation: bool = True) -> Optional[Any]:
        def middleware(fields_path_elements: List[List[DatabasePathElement]]) -> Optional[dict]:
            return self._remove_data_elements_from_map(
                key_value=key_value, fields_path_elements=fields_path_elements,
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
            return self._remove_data_elements_from_map(key_value=key_value, fields_path_elements=fields_path_elements)
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
            return self._remove_data_elements_from_map(
                key_value=key_value, fields_path_elements=fields_path_elements,
            )
        return self._grouped_remove_multiple_fields(middleware=middleware, removers=removers)

    def grouped_delete_multiple_fields(self, key_value: str, removers: List[FieldRemover]) -> bool:
        def middleware(fields_path_elements: List[List[DatabasePathElement]]):
            return self._delete_data_elements_from_map(
                key_value=key_value, fields_path_elements=fields_path_elements,
            )
        return self._grouped_delete_multiple_fields(middleware=middleware, removers=removers)

