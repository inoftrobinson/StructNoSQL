from typing import Optional, List, Dict, Any
from StructNoSQL.middlewares.dynamodb.backend.dynamodb_core import DynamoDbCoreAdapter, PrimaryIndex, GlobalSecondaryIndex
from StructNoSQL.models import DatabasePathElement, FieldGetter, FieldSetter, UnsafeFieldSetter, FieldRemover
from StructNoSQL.practical_logger import message_with_vars
from StructNoSQL.tables.base_caching_table import BaseCachingTable
from StructNoSQL.middlewares.dynamodb.dynamodb_table_connectors import DynamoDBTableConnectors
from StructNoSQL.utils.process_render_fields_paths import join_field_path_elements


class DynamoDBCachingTable(BaseCachingTable, DynamoDBTableConnectors):
    def __init__(
            self, table_name: str, region_name: str, data_model,
            primary_index: PrimaryIndex,
            billing_mode: str = DynamoDbCoreAdapter.PAY_PER_REQUEST,
            global_secondary_indexes: List[GlobalSecondaryIndex] = None,
            auto_create_table: bool = True
    ):
        super().__init__(data_model=data_model, primary_index=None)
        super().__setup_connectors__(
            table_name=table_name, region_name=region_name, primary_index=primary_index,
            billing_mode=billing_mode, global_secondary_indexes=global_secondary_indexes,
            auto_create_table=auto_create_table
        )

    def commit_update_operations(self) -> bool:
        for formatted_index_key_value, dynamodb_setters in self._pending_update_operations.items():
            index_name, key_value = formatted_index_key_value.split('|', maxsplit=1)
            response = self.dynamodb_client.set_update_multiple_data_elements_to_map(
                index_name=index_name, key_value=key_value, setters=list(dynamodb_setters.values())
            )
            print(response)
        return True  # todo: create a real success status instead of always True

    def commit_remove_operations(self) -> bool:
        for formatted_index_key_value, dynamodb_setters in self._pending_remove_operations.items():
            index_name, key_value = formatted_index_key_value.split('|', maxsplit=1)
            response = self.dynamodb_client.remove_data_elements_from_map(
                index_name=index_name, key_value=key_value,
                targets_path_elements=list(dynamodb_setters.values())
            )
            # delete operations can be cached, where as remove operations need to be executed immediately
            print(response)
        return True  # todo: create a real success status instead of always True

    def commit_operations(self):
        self.commit_update_operations()
        self.commit_remove_operations()
        return True

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
                return self.dynamodb_client.get_value_in_path_target(
                    index_name=index_name or self.primary_index_name,
                    key_value=key_value, field_path_elements=field_path_elements
                )
            else:
                field_path_elements: Dict[str, List[DatabasePathElement]]
                response_data = self.dynamodb_client.get_values_in_multiple_path_target(
                    index_name=index_name or self.primary_index_name,
                    key_value=key_value, fields_path_elements=field_path_elements,
                    metadata=True
                )
                if response_data is not None:
                    output: Dict[str, Any] = {}
                    for key, item in response_data.items():
                        # We access the item attributes with brackets, because the attributes
                        # are required, and we should cause an exception if they are missing.
                        item_value: Any = item['value']
                        item_field_path_elements: List[DatabasePathElement] = item['field_path_elements']
                        output[join_field_path_elements(item_field_path_elements)] = {'value': item_value, 'key': key}
                    return output
                return None
        return self._get_field(middleware=middleware, key_value=key_value, field_path=field_path, query_kwargs=query_kwargs)

    def get_multiple_fields(self, key_value: str, getters: Dict[str, FieldGetter], index_name: Optional[str] = None) -> Optional[dict]:
        def middleware(fields_path_elements: List[List[DatabasePathElement]]):
            return self.dynamodb_client.get_or_query_single_item(
                index_name=index_name or self.primary_index_name,
                key_value=key_value, fields_path_elements=fields_path_elements,
            )
        return self._get_multiple_fields(middleware=middleware, key_value=key_value, getters=getters)

    def update_field(self, key_value: str, field_path: str, value_to_set: Any, query_kwargs: Optional[dict] = None, index_name: Optional[str] = None) -> bool:
        return self._update_field(key_value=key_value, field_path=field_path, value_to_set=value_to_set, query_kwargs=query_kwargs, index_name=index_name)

    def update_multiple_fields(self, key_value: str, setters: List[FieldSetter or UnsafeFieldSetter], index_name: Optional[str] = None) -> bool:
        return self._update_multiple_fields(key_value=key_value, setters=setters, index_name=index_name)

    def remove_field(self, key_value: str, field_path: str, query_kwargs: Optional[dict] = None, index_name: Optional[str] = None) -> Optional[Any]:
        def middleware(fields_path_elements: List[List[DatabasePathElement]]):
            return self.dynamodb_client.remove_data_elements_from_map(
                index_name=index_name or self.primary_index_name,
                key_value=key_value, targets_path_elements=fields_path_elements,
                retrieve_removed_elements=True
            )
        return self._remove_field(middleware=middleware, key_value=key_value, field_path=field_path, query_kwargs=query_kwargs, index_name=index_name)

    def remove_multiple_fields(self, key_value: str, removers: Dict[str, FieldRemover], index_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
        def task_executor(remover_item: FieldRemover):
            return self.remove_field(
                key_value=key_value, index_name=index_name,
                field_path=remover_item.field_path,
                query_kwargs=remover_item.query_kwargs
            )
        return self._async_field_removers_executor(task_executor=task_executor, removers=removers)

    def delete_field(self, key_value: str, field_path: str, query_kwargs: Optional[dict] = None, index_name: Optional[str] = None) -> bool:
        return self._delete_field(key_value=key_value, field_path=field_path, query_kwargs=query_kwargs, index_name=index_name)

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
        return self._grouped_remove_multiple_fields(middleware=middleware, key_value=key_value, removers=removers)

    def grouped_delete_multiple_fields(self, key_value: str, removers: List[FieldRemover], index_name: Optional[str] = None) -> bool:
        return self._grouped_delete_multiple_fields(key_value=key_value, removers=removers, index_name=index_name)
