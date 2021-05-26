from typing import Optional, List, Dict, Any
from StructNoSQL.models import DatabasePathElement, FieldGetter, FieldSetter, UnsafeFieldSetter, FieldRemover
from StructNoSQL.practical_logger import message_with_vars
from StructNoSQL.tables.base_caching_table import BaseCachingTable
from StructNoSQL.middlewares.inoft_vocal_engine.inoft_vocal_engine_table_connectors import InoftVocalEngineTableConnectors


class InoftVocalEngineCachingTable(BaseCachingTable, InoftVocalEngineTableConnectors):
    def __init__(
            self, engine_account_id: str, engine_project_id: str, engine_api_key: str,
            table_id: str, region_name: str, data_model
    ):
        super().__init__(data_model=data_model, primary_index=None)
        self.__setup_connectors__(
            engine_account_id=engine_account_id,
            engine_project_id=engine_project_id,
            engine_api_key=engine_api_key,
            table_id=table_id, region_name=region_name
        )

    def commit_update_operations(self) -> bool:
        for formatted_index_key_value, dynamodb_setters in self._pending_update_operations.items():
            index_name, key_value = formatted_index_key_value.split('|', maxsplit=1)
            return self._set_update_multiple_data_elements_to_map(key_value=key_value, setters=list(dynamodb_setters.values()))
        return True  # todo: create a real success status instead of always True

    def commit_remove_operations(self) -> bool:
        for formatted_index_key_value, fields_path_elements in self._pending_remove_operations.items():
            index_name, key_value = formatted_index_key_value.split('|', maxsplit=1)
            return self._remove_data_elements_from_map(key_value=key_value, fields_path_elements=list(fields_path_elements.values()))
        return True  # todo: create a real success status instead of always True

    def commit_operations(self):
        self.commit_update_operations()
        self.commit_remove_operations()
        return True

    # todo: finish put_record and delete_record
    def put_record(self, record_dict_data: dict) -> bool:
        # todo: integrate with caching
        self.model_virtual_map_field.populate(value=record_dict_data)
        validated_data, is_valid = self.model_virtual_map_field.validate_data()
        if is_valid is True:
            return self.dynamodb_client.put_record(item_dict=validated_data)
        else:
            return False

    def delete_record(self, indexes_keys_selectors: dict) -> bool:
        # todo: integrate with caching
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

    def get_field(self, key_value: str, field_path: str, query_kwargs: Optional[dict] = None) -> Any:
        def middleware(field_path_elements: List[DatabasePathElement] or Dict[str, List[DatabasePathElement]], has_multiple_fields_path: bool):
            if has_multiple_fields_path is not True:
                field_path_elements: List[DatabasePathElement]
                return self._get_single_value_in_path_target(key_value=key_value, field_path_elements=field_path_elements)
            else:
                field_path_elements: Dict[str, List[DatabasePathElement]]
                return self._get_values_in_multiple_path_target(key_value=key_value, fields_path_elements=field_path_elements)
        return self._get_field(middleware=middleware, key_value=key_value, field_path=field_path, query_kwargs=query_kwargs)

    def get_multiple_fields(self, key_value: str, getters: Dict[str, FieldGetter]) -> Optional[dict]:
        def middleware(fields_path_elements: List[List[DatabasePathElement]]):
            return self._get_or_query_single_item(key_value=key_value, fields_path_elements=fields_path_elements)
        return self._get_multiple_fields(middleware=middleware, key_value=key_value, getters=getters)

    def update_field(self, key_value: str, field_path: str, value_to_set: Any, query_kwargs: Optional[dict] = None) -> bool:
        return self._update_field(key_value=key_value, field_path=field_path, value_to_set=value_to_set, query_kwargs=query_kwargs)

    def update_multiple_fields(self, key_value: str, setters: List[FieldSetter or UnsafeFieldSetter]) -> bool:
        return self._update_multiple_fields(key_value=key_value, setters=setters)

    def remove_field(self, key_value: str, field_path: str, query_kwargs: Optional[dict] = None, index_name: Optional[str] = None) -> Optional[Any]:
        def middleware(fields_path_elements: List[List[DatabasePathElement]]):
            return self._remove_data_elements_from_map(key_value=key_value, fields_path_elements=fields_path_elements)
        return self._remove_field(middleware=middleware, key_value=key_value, field_path=field_path, query_kwargs=query_kwargs, index_name=index_name)

    def remove_multiple_fields(self, key_value: str, removers: Dict[str, FieldRemover]) -> Optional[Dict[str, Any]]:
        return {key: self.remove_field(
            key_value=key_value, field_path=item.field_path, query_kwargs=item.query_kwargs
        ) for key, item in removers.items()}

    def delete_field(self, key_value: str, field_path: str, query_kwargs: Optional[dict] = None) -> bool:
        return self._delete_field(key_value=key_value, field_path=field_path, query_kwargs=query_kwargs)

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
                key_value=key_value, fields_path_elements=fields_path_elements
            )
        return self._grouped_remove_multiple_fields(middleware=middleware, key_value=key_value, removers=removers)

    def grouped_delete_multiple_fields(self, key_value: str, removers: List[FieldRemover]) -> bool:
        return self._grouped_delete_multiple_fields(key_value=key_value, removers=removers)
