from typing import Optional, List, Dict, Any, Tuple
from StructNoSQL.dynamodb.dynamodb_core import DynamoDbCoreAdapter, PrimaryIndex, GlobalSecondaryIndex, DynamoDBMapObjectSetter, Response
from StructNoSQL.dynamodb.models import DatabasePathElement, FieldGetter, FieldSetter, UnsafeFieldSetter, FieldRemover
from StructNoSQL.practical_logger import message_with_vars
from StructNoSQL.tables.base_caching_table import BaseCachingTable
from StructNoSQL.tables.base_dynamodb_table import BaseTable
from StructNoSQL.tables.base_inoft_vocal_engine_table import BaseInoftVocalEngineTable
from StructNoSQL.utils.process_render_fields_paths import process_and_get_fields_paths_objects_from_fields_paths, \
    process_and_make_single_rendered_database_path, process_validate_data_and_make_single_rendered_database_path, \
    process_and_get_field_path_object_from_field_path, make_rendered_database_path
from StructNoSQL.utils.decimals import float_to_decimal_serializer


def join_field_path_elements(field_path_elements) -> str:
    return '.'.join((f'{item.element_key}' for item in field_path_elements))


class InoftVocalEngineCachingTable(BaseInoftVocalEngineTable, BaseCachingTable):
    def __init__(self, table_id: str, region_name: str, data_model):
        super().__init__(table_id=table_id, region_name=region_name, data_model=data_model)
        self._cached_data = dict()
        self._pending_update_operations: Dict[str, Dict[str, DynamoDBMapObjectSetter]] = dict()
        self._pending_remove_operations: Dict[str, Dict[str, List[DatabasePathElement]]] = dict()
        self._debug = False

    @property
    def debug(self) -> bool:
        return self._debug

    @debug.setter
    def debug(self, debug: bool):
        self._debug = debug

    def _index_cached_data(self, index_name: Optional[str], key_value: str) -> dict:
        index_name = f"{index_name or self.primary_index_name}|{key_value}"
        if index_name not in self._cached_data:
            self._cached_data[index_name] = dict()
        return self._cached_data[index_name]

    def _index_pending_update_operations(self, index_name: Optional[str], key_value: str) -> dict:
        index_name = f"{index_name or self.primary_index_name}|{key_value}"
        if index_name not in self._pending_update_operations:
            self._pending_update_operations[index_name] = dict()
        return self._pending_update_operations[index_name]

    def _index_pending_remove_operations(self, index_name: Optional[str], key_value: str) -> dict:
        index_name = f"{index_name or self.primary_index_name}|{key_value}"
        if index_name not in self._pending_remove_operations:
            self._pending_remove_operations[index_name] = dict()
        return self._pending_remove_operations[index_name]

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

    @staticmethod
    def _cache_put_data(index_cached_data: dict, field_path_elements: List[DatabasePathElement], data: Any):
        if len(field_path_elements) > 0:
            navigated_cached_data = index_cached_data
            for path_element in field_path_elements[:-1]:
                stringed_element_key = f'{path_element.element_key}'
                # We wrap the element_key inside a string, to handle a scenario where we would put an item from a list,
                # where the element_key will be an int, that could be above zero, and cannot be handled by a classical list.

                if stringed_element_key not in navigated_cached_data:
                    navigated_cached_data[stringed_element_key] = dict()
                navigated_cached_data = navigated_cached_data[stringed_element_key]

            last_field_path_element = field_path_elements[-1]
            navigated_cached_data[f'{last_field_path_element.element_key}'] = data
            # todo: handle list's and set's

    @staticmethod
    def _cache_get_data(index_cached_data: dict, field_path_elements: List[DatabasePathElement]) -> Tuple[bool, Any]:
        if len(field_path_elements) > 0:
            navigated_cached_data = index_cached_data
            for path_element in field_path_elements[:-1]:
                stringed_element_key = f'{path_element.element_key}'
                # We wrap the element_key inside a string, to handle a scenario where we would put an item from a list,
                # where the element_key will be an int, that could be above zero, and cannot be handled by a classical list.
                navigated_cached_data = (
                    navigated_cached_data[stringed_element_key]
                    if stringed_element_key in navigated_cached_data
                    else path_element.get_default_value()
                )

            last_field_path_element = field_path_elements[-1]
            if last_field_path_element.element_key in navigated_cached_data:
                retrieved_item_value = navigated_cached_data[last_field_path_element.element_key]
                return True, retrieved_item_value
        return False, None

    def _cache_delete_field(self, index_cached_data: dict, index_name: str, key_value: str, field_path_elements: List[DatabasePathElement]) -> str:
        """Will remove the element value from the cache, and remove any update operations associated with the same field in the same index and key_value"""
        CachingTable._cache_put_data(index_cached_data=index_cached_data, field_path_elements=field_path_elements, data=None)

        pending_update_operations = self._index_pending_update_operations(index_name=index_name, key_value=key_value)
        item_joined_field_path = join_field_path_elements(field_path_elements)

        if item_joined_field_path in pending_update_operations:
            pending_update_operations.pop(item_joined_field_path)
        return item_joined_field_path

    def _cache_remove_field(self, index_cached_data: dict, index_name: str, key_value: str, field_path_elements: List[DatabasePathElement]):
        """Unlike the _cache_delete_field, this must be used when a remove operation to the database will be performed right away"""
        item_joined_field_path = self._cache_delete_field(
            index_cached_data=index_cached_data, index_name=index_name,
            key_value=key_value, field_path_elements=field_path_elements
        )
        pending_remove_operations = self._index_pending_remove_operations(index_name=index_name, key_value=key_value)
        if item_joined_field_path in pending_remove_operations:
            pending_remove_operations.pop(item_joined_field_path)

    def _cache_add_delete_operation(self, index_cached_data: dict, pending_remove_operations: dict, field_path_elements: List[DatabasePathElement]):
        self._cache_put_data(index_cached_data=index_cached_data, field_path_elements=field_path_elements, data=None)
        joined_field_path = join_field_path_elements(field_path_elements)
        pending_remove_operations[joined_field_path] = field_path_elements

    def _cache_process_add_delete_operation(self, index_cached_data: dict, pending_remove_operations: dict, field_path: str, query_kwargs: Optional[dict] = None):
        field_path_elements, has_multiple_fields_path = process_and_make_single_rendered_database_path(
            field_path=field_path, fields_switch=self.fields_switch, query_kwargs=query_kwargs
        )
        if has_multiple_fields_path is not True:
            field_path_elements: List[DatabasePathElement]
            self._cache_add_delete_operation(
                index_cached_data=index_cached_data,
                pending_remove_operations=pending_remove_operations,
                field_path_elements=field_path_elements
            )
        else:
            field_path_elements: Dict[str, List[DatabasePathElement]]
            for item_field_path_elements_value in field_path_elements.values():
                self._cache_add_delete_operation(
                    index_cached_data=index_cached_data,
                    pending_remove_operations=pending_remove_operations,
                    field_path_elements=item_field_path_elements_value
                )

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

    def get_field(self, key_value: str, field_path: str, query_kwargs: Optional[dict] = None, index_name: Optional[str] = None) -> Any:
        index_cached_data = self._index_cached_data(index_name=index_name, key_value=key_value)
        field_path_elements, has_multiple_fields_path = process_and_make_single_rendered_database_path(
            field_path=field_path, fields_switch=self.fields_switch, query_kwargs=query_kwargs
        )
        if has_multiple_fields_path is not True:
            field_path_elements: List[DatabasePathElement]

            found_in_cache, field_value_from_cache = CachingTable._cache_get_data(
                index_cached_data=index_cached_data, field_path_elements=field_path_elements
            )
            if found_in_cache is True:
                return field_value_from_cache if self.debug is not True else {'value': field_value_from_cache, 'fromCache': True}

            response_data = self.dynamodb_client.get_value_in_path_target(
                index_name=index_name or self.primary_index_name,
                key_value=key_value, field_path_elements=field_path_elements
            )
            CachingTable._cache_put_data(index_cached_data=index_cached_data, field_path_elements=field_path_elements, data=response_data)
            return response_data if self.debug is not True else {'value': response_data, 'fromCache': False}
        else:
            field_path_elements: Dict[str, List[DatabasePathElement]]
            response_items_values: Dict[str, Any] = dict()

            keys_fields_already_cached_to_pop: List[str] = list()
            for item_key, item_field_path_elements in field_path_elements.items():
                found_item_value_in_cache, field_item_value_from_cache = CachingTable._cache_get_data(
                    index_cached_data=index_cached_data, field_path_elements=item_field_path_elements
                )
                if found_item_value_in_cache is True:
                    # We do not use a .get('key', None), because None can be a valid value for a field
                    response_items_values[item_key] = field_item_value_from_cache if self.debug is not True else {'value': field_item_value_from_cache, 'fromCache': True}
                    keys_fields_already_cached_to_pop.append(item_key)

            for key_to_pop in keys_fields_already_cached_to_pop:
                field_path_elements.pop(key_to_pop)

            if len(field_path_elements) > 0:
                response_data = self.dynamodb_client.get_values_in_multiple_path_target(
                    index_name=index_name or self.primary_index_name,
                    key_value=key_value, fields_paths_elements=field_path_elements,
                    metadata=True
                )
                if response_data is not None:
                    for key, item in response_data.items():
                        # We access the item attributes with brackets, because the attributes
                        # are required, and we should cause an exception if they are missing.
                        item_value: Any = item['value']
                        item_field_path_elements: List[DatabasePathElement] = item['field_path_elements']
                        index_cached_data[join_field_path_elements(item_field_path_elements)] = item_value
                        response_items_values[key] = item_value if self.debug is not True else {'value': item_value, 'fromCache': False}
            return response_items_values if self.debug is not True else {'value': response_items_values, 'fromCache': None}

    def get_multiple_fields(self, key_value: str, getters: Dict[str, FieldGetter], index_name: Optional[str] = None) -> Optional[dict]:
        output_data: Dict[str, Any] = dict()
        index_cached_data = self._index_cached_data(index_name=index_name, key_value=key_value)

        single_getters_database_paths_elements: Dict[str, List[DatabasePathElement]] = dict()
        grouped_getters_database_paths_elements: Dict[str, Dict[str, List[DatabasePathElement]]] = dict()

        getters_database_paths: List[List[DatabasePathElement]] = list()
        for getter_key, getter_item in getters.items():
            field_path_elements, has_multiple_fields_path = process_and_make_single_rendered_database_path(
                field_path=getter_item.field_path, fields_switch=self.fields_switch, query_kwargs=getter_item.query_kwargs
            )
            if has_multiple_fields_path is not True:
                field_path_elements: List[DatabasePathElement]
                found_value_in_cache, field_value_from_cache = CachingTable._cache_get_data(
                    index_cached_data=index_cached_data, field_path_elements=field_path_elements
                )
                if found_value_in_cache is True:
                    output_data[getter_key] = field_value_from_cache if self.debug is not True else {'value': field_value_from_cache, 'fromCache': True}
                else:
                    single_getters_database_paths_elements[getter_key] = field_path_elements
                    getters_database_paths.append(field_path_elements)
            else:
                field_path_elements: Dict[str, List[DatabasePathElement]]
                current_getter_grouped_database_paths_elements: Dict[str, List[DatabasePathElement]] = dict()
                container_data: Dict[str, Any] = dict()

                for child_item_key, child_item_field_path_elements in field_path_elements.items():
                    found_item_value_in_cache, field_item_value_from_cache = CachingTable._cache_get_data(
                        index_cached_data=index_cached_data, field_path_elements=child_item_field_path_elements
                    )
                    if found_item_value_in_cache is True:
                        container_data[child_item_key] = field_item_value_from_cache \
                            if self.debug is not True else {'value': field_item_value_from_cache, 'fromCache': True}
                    else:
                        current_getter_grouped_database_paths_elements[child_item_key] = child_item_field_path_elements
                        getters_database_paths.append(child_item_field_path_elements)

                output_data[getter_key] = container_data
                grouped_getters_database_paths_elements[getter_key] = current_getter_grouped_database_paths_elements

        response_data = self.dynamodb_client.get_or_query_single_item(
            index_name=index_name or self.primary_index_name,
            key_value=key_value, fields_paths_elements=getters_database_paths,
        )
        if response_data is None:
            return None
        else:
            for item_key, item_field_path_elements in single_getters_database_paths_elements.items():
                item_data = self.dynamodb_client.navigate_into_data_with_field_path_elements(
                    data=response_data, field_path_elements=item_field_path_elements,
                    num_keys_to_navigation_into=len(item_field_path_elements)
                )
                output_data[item_key] = item_data if self.debug is not True else {'value': item_data, 'fromCache': False}

            for container_key, container_items_field_path_elements in grouped_getters_database_paths_elements.items():
                container_data: Dict[str, Any] = dict()
                for child_item_key, child_item_field_path_elements in container_items_field_path_elements.items():
                    child_item_value = self.dynamodb_client.navigate_into_data_with_field_path_elements(
                        data=response_data, field_path_elements=child_item_field_path_elements,
                        num_keys_to_navigation_into=len(child_item_field_path_elements)
                    )
                    container_data[child_item_key] = child_item_value if self.debug is not True else {'value': child_item_value, 'fromCache': False}
                output_data[container_key] = container_data if container_key not in output_data else {**container_data, **output_data[container_key]}
            return output_data if self.debug is not True else {'value': output_data, 'fromCache': None}

    """def get_multiple_fields(self, key_value: str, getters: Dict[str, FieldGetter], index_name: Optional[str] = None) -> Optional[dict]:
        # raise Exception("Not implemented with caching table")
        getters_database_paths = self._getters_to_database_paths(getters=getters)

        index_cached_data = self._index_cached_data(index_name=index_name, key_value=key_value)
        field_path_elements, has_multiple_fields_path = process_and_make_single_rendered_database_path(
            field_path=field_path, fields_switch=self.fields_switch, query_kwargs=query_kwargs
        )
        if has_multiple_fields_path is not True:
            field_path_elements: List[DatabasePathElement]

            found_in_cache, field_value_from_cache = CachingTable._cache_get_data(
                index_cached_data=index_cached_data, field_path_elements=field_path_elements
            )
            if found_in_cache is True:
                return field_value_from_cache if self.debug is not True else {'value': field_value_from_cache, 'fromCache': True}

            response_data = self.dynamodb_client.get_value_in_path_target(
                index_name=index_name or self.primary_index_name,
                key_value=key_value, field_path_elements=field_path_elements
            )
            CachingTable._cache_put_data(index_cached_data=index_cached_data, field_path_elements=field_path_elements, data=response_data)
            return response_data if self.debug is not True else {'value': response_data, 'fromCache': False}

        response_data = self.dynamodb_client.get_values_in_multiple_path_target(
            index_name=index_name or self.primary_index_name,
            key_value=key_value, fields_paths_elements=getters_database_paths,
        )
        return response_data
    """

    def update_field(self, key_value: str, field_path: str, value_to_set: Any, query_kwargs: Optional[dict] = None, index_name: Optional[str] = None) -> bool:
        validated_data, valid, field_path_elements = process_validate_data_and_make_single_rendered_database_path(
            field_path=field_path, fields_switch=self.fields_switch, query_kwargs=query_kwargs, data_to_validate=value_to_set
        )
        if valid is True and field_path_elements is not None:
            index_cached_data = self._index_cached_data(index_name=index_name, key_value=key_value)
            CachingTable._cache_put_data(index_cached_data=index_cached_data, field_path_elements=field_path_elements, data=validated_data)

            joined_field_path = join_field_path_elements(field_path_elements)
            pending_update_operations = self._index_pending_update_operations(index_name=index_name, key_value=key_value)
            pending_update_operations[joined_field_path] = DynamoDBMapObjectSetter(
                field_path_elements=field_path_elements, value_to_set=validated_data
            )
            return True
        return False

    def update_multiple_fields(self, key_value: str, setters: List[FieldSetter or UnsafeFieldSetter], index_name: Optional[str] = None) -> bool:
        index_cached_data = self._index_cached_data(index_name=index_name, key_value=key_value)
        for current_setter in setters:
            if isinstance(current_setter, FieldSetter):
                validated_data, valid, field_path_elements = process_validate_data_and_make_single_rendered_database_path(
                    field_path=current_setter.field_path, fields_switch=self.fields_switch,
                    query_kwargs=current_setter.query_kwargs, data_to_validate=current_setter.value_to_set
                )
                if valid is True:
                    CachingTable._cache_put_data(index_cached_data=index_cached_data, field_path_elements=field_path_elements, data=validated_data)
                    joined_field_path = join_field_path_elements(field_path_elements)
                    pending_update_operations = self._index_pending_update_operations(index_name=index_name, key_value=key_value)
                    pending_update_operations[joined_field_path] = DynamoDBMapObjectSetter(
                        field_path_elements=field_path_elements, value_to_set=validated_data
                    )
            elif isinstance(current_setter, UnsafeFieldSetter):
                raise Exception(f"UnsafeFieldSetter not supported in caching_table")
                """safe_field_path_object, has_multiple_fields_path = process_and_get_field_path_object_from_field_path(
                    field_path_key=current_setter.safe_base_field_path, fields_switch=self.fields_switch
                )
                # todo: add support for multiple fields path
                if current_setter.unsafe_path_continuation is None:
                    field_path_elements = safe_field_path_object.database_path
                else:
                    field_path_elements = safe_field_path_object.database_path + current_setter.unsafe_path_continuation

                processed_value_to_set: Any = float_to_decimal_serializer(current_setter.value_to_set)
                # Since the data is not validated, we pass it to the float_to_decimal_serializer
                # function (which normally should be called by the data validation function)

                rendered_field_path_elements = make_rendered_database_path(
                    database_path_elements=field_path_elements,
                    query_kwargs=current_setter.query_kwargs
                )
                dynamodb_setters.append(DynamoDBMapObjectSetter(
                    field_path_elements=rendered_field_path_elements,
                    value_to_set=processed_value_to_set
                ))"""
        return True

    def remove_field(self, key_value: str, field_path: str, query_kwargs: Optional[dict] = None, index_name: Optional[str] = None) -> Optional[Any]:
        field_path_elements, has_multiple_fields_path = process_and_make_single_rendered_database_path(
            field_path=field_path, fields_switch=self.fields_switch, query_kwargs=query_kwargs
        )
        index_cached_data = self._index_cached_data(index_name=index_name, key_value=key_value)

        if has_multiple_fields_path is not True:
            field_path_elements: List[DatabasePathElement]
            found_value_in_cache, field_value_from_cache = CachingTable._cache_get_data(
                index_cached_data=index_cached_data, field_path_elements=field_path_elements
            )
            if found_value_in_cache is True:
                pending_remove_operations = self._index_pending_remove_operations(index_name=index_name, key_value=key_value)
                self._cache_add_delete_operation(
                    index_cached_data=index_cached_data,
                    pending_remove_operations=pending_remove_operations,
                    field_path_elements=field_path_elements
                )
                # Even when we retrieve a removed value from the cache, and that we do not need to perform a remove operation right away to retrieve
                # the removed value, we still want to add a delete_operation that will be performed on operation commits, because if we remove a value
                # from the cache, it does not remove a potential older value present in the database, that the remove operation should remove.
                return field_value_from_cache if self.debug is not True else {'value': field_value_from_cache, 'fromCache': True}
            else:
                target_path_elements: List[List[DatabasePathElement]] = [field_path_elements]
                self._cache_remove_field(
                    index_cached_data=index_cached_data, index_name=index_name,
                    key_value=key_value, field_path_elements=field_path_elements
                )
                response_attributes = self.dynamodb_client.remove_data_elements_from_map(
                    index_name=index_name or self.primary_index_name,
                    key_value=key_value, targets_path_elements=target_path_elements,
                    retrieve_removed_elements=True
                )
                if response_attributes is not None:
                    removed_item_data = self.dynamodb_client.navigate_into_data_with_field_path_elements(
                        data=response_attributes, field_path_elements=field_path_elements,
                        num_keys_to_navigation_into=len(field_path_elements)
                    )
                    return removed_item_data if self.debug is not True else {'value': removed_item_data, 'fromCache': False}
                else:
                    return None if self.debug is not True else {'value': None, 'fromCache': False}
        else:
            field_path_elements: Dict[str, List[DatabasePathElement]]
            target_path_elements: List[List[DatabasePathElement]] = list()
            container_output_data: Dict[str, Any] = dict()

            for item_field_path_elements_value in field_path_elements.values():
                item_last_path_element = item_field_path_elements_value[-1]

                found_item_value_in_cache, field_item_value_from_cache = CachingTable._cache_get_data(
                    index_cached_data=index_cached_data, field_path_elements=item_field_path_elements_value
                )
                if found_item_value_in_cache is True:
                    pending_remove_operations = self._index_pending_remove_operations(index_name=index_name, key_value=key_value)
                    self._cache_add_delete_operation(
                        index_cached_data=index_cached_data,
                        pending_remove_operations=pending_remove_operations,
                        field_path_elements=item_field_path_elements_value
                    )
                    container_output_data[item_last_path_element.element_key] = (
                        field_item_value_from_cache if self.debug is not True else {'value': field_item_value_from_cache, 'fromCache': True}
                    )
                else:
                    target_path_elements.append(item_field_path_elements_value)
                    self._cache_remove_field(
                        index_cached_data=index_cached_data, index_name=index_name,
                        key_value=key_value, field_path_elements=item_field_path_elements_value
                    )

            if len(target_path_elements) > 0:
                response_attributes = self.dynamodb_client.remove_data_elements_from_map(
                    index_name=index_name or self.primary_index_name,
                    key_value=key_value, targets_path_elements=target_path_elements,
                    retrieve_removed_elements=True
                )
                if response_attributes is not None:
                    for key, child_item_field_path_elements in field_path_elements.items():
                        removed_item_data = self.dynamodb_client.navigate_into_data_with_field_path_elements(
                            data=response_attributes, field_path_elements=child_item_field_path_elements,
                            num_keys_to_navigation_into=len(child_item_field_path_elements)
                        )
                        container_output_data[key] = removed_item_data if self.debug is not True else {'value': removed_item_data, 'fromCache': False}

            return container_output_data if self.debug is not True else {'value': container_output_data, 'fromCache': None}

    def remove_multiple_fields(self, key_value: str, removers: Dict[str, FieldRemover], index_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
        return {key: self.remove_field(
            key_value=key_value, index_name=index_name,
            field_path=item.field_path, query_kwargs=item.query_kwargs
        ) for key, item in removers.items()}

    def delete_field(self, key_value: str, field_path: str, query_kwargs: Optional[dict] = None, index_name: Optional[str] = None) -> bool:
        index_cached_data = self._index_cached_data(index_name=index_name, key_value=key_value)
        pending_remove_operations = self._index_pending_remove_operations(index_name=index_name, key_value=key_value)
        self._cache_process_add_delete_operation(
            index_cached_data=index_cached_data,
            pending_remove_operations=pending_remove_operations,
            field_path=field_path, query_kwargs=query_kwargs
        )
        return True

    def delete_multiple_fields(self, key_value: str, removers: Dict[str, FieldRemover], index_name: Optional[str] = None) -> Dict[str, bool]:
        return {key: self.delete_field(
            key_value=key_value, index_name=index_name,
            field_path=item.field_path, query_kwargs=item.query_kwargs
        ) for key, item in removers.items()}

    def grouped_remove_multiple_fields(self, key_value: str, removers: Dict[str, FieldRemover], index_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
        # todo: do not perform the operation but store it as pending if a matching value exists in the cache
        if not len(removers) > 0:
            # If no remover has been specified, we do not run the database
            # operation, and since no value has been removed, we return None.
            return None
        else:
            index_cached_data = self._index_cached_data(index_name=index_name, key_value=key_value)

            removers_field_paths_elements: Dict[str, List[DatabasePathElement]] = dict()
            grouped_removers_field_paths_elements: Dict[str, Dict[str, List[DatabasePathElement]]] = dict()

            removers_database_paths: List[List[DatabasePathElement]] = list()
            for remover_key, remover_item in removers.items():
                field_path_elements, has_multiple_fields_path = process_and_make_single_rendered_database_path(
                    field_path=remover_item.field_path, fields_switch=self.fields_switch,
                    query_kwargs=remover_item.query_kwargs
                )
                if has_multiple_fields_path is not True:
                    field_path_elements: List[DatabasePathElement]
                    removers_field_paths_elements[remover_key] = field_path_elements
                    removers_database_paths.append(field_path_elements)
                    self._cache_remove_field(
                        index_cached_data=index_cached_data, index_name=index_name,
                        key_value=key_value, field_path_elements=field_path_elements
                    )
                else:
                    field_path_elements: Dict[str, List[DatabasePathElement]]
                    grouped_removers_field_paths_elements[remover_key] = field_path_elements
                    field_path_elements_values = list(field_path_elements.values())
                    for item_field_path_elements_values in field_path_elements_values:
                        removers_database_paths.append(item_field_path_elements_values)
                        self._cache_remove_field(
                            index_cached_data=index_cached_data, index_name=index_name,
                            key_value=key_value, field_path_elements=item_field_path_elements_values
                        )

            response_attributes = self.dynamodb_client.remove_data_elements_from_map(
                index_name=index_name or self.primary_index_name, key_value=key_value,
                targets_path_elements=removers_database_paths,
                retrieve_removed_elements=True
            )
            if response_attributes is None:
                return None
            else:
                output_data: Dict[str, Any] = dict()
                for item_key, item_field_path_elements in removers_field_paths_elements.items():
                    removed_item_data = self.dynamodb_client.navigate_into_data_with_field_path_elements(
                        data=response_attributes, field_path_elements=item_field_path_elements,
                        num_keys_to_navigation_into=len(item_field_path_elements)
                    )
                    output_data[item_key] = removed_item_data

                for container_key, container_items_field_path_elements in grouped_removers_field_paths_elements.items():
                    container_data: Dict[str, Any] = dict()
                    for child_item_key, child_item_field_path_elements in container_items_field_path_elements.items():
                        container_data[child_item_key] = self.dynamodb_client.navigate_into_data_with_field_path_elements(
                            data=response_attributes, field_path_elements=child_item_field_path_elements,
                            num_keys_to_navigation_into=len(child_item_field_path_elements)
                        )
                    output_data[container_key] = container_data
                return output_data

    def grouped_delete_multiple_fields(self, key_value: str, removers: List[FieldRemover], index_name: Optional[str] = None) -> bool:
        index_cached_data = self._index_cached_data(index_name=index_name, key_value=key_value)
        pending_remove_operations = self._index_pending_remove_operations(index_name=index_name, key_value=key_value)

        for current_remover in removers:
            self._cache_process_add_delete_operation(
                index_cached_data=index_cached_data,
                pending_remove_operations=pending_remove_operations,
                field_path=current_remover.field_path,
                query_kwargs=current_remover.query_kwargs
            )
        return True
