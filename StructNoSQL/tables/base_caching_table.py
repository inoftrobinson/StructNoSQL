from typing import Optional, List, Dict, Any, Tuple, Callable
from StructNoSQL.middlewares.dynamodb.backend.dynamodb_core import PrimaryIndex
from StructNoSQL.models import DatabasePathElement, FieldGetter, FieldRemover, FieldSetter, UnsafeFieldSetter, FieldPathSetter
from StructNoSQL.practical_logger import message_with_vars
from StructNoSQL.tables.base_table import BaseTable
from StructNoSQL.tables.shared_table_behaviors import _prepare_getters
from StructNoSQL.utils.data_processing import navigate_into_data_with_field_path_elements
from StructNoSQL.utils.process_render_fields_paths import process_and_make_single_rendered_database_path, \
    process_validate_data_and_make_single_rendered_database_path, join_field_path_elements


class BaseCachingTable(BaseTable):
    def __init__(self, data_model, primary_index: PrimaryIndex):
        super().__init__(data_model=data_model, primary_index=primary_index)
        self._cached_data_per_primary_key: Dict[str, Any] = {}
        self._pending_update_operations_per_primary_key: Dict[str, Dict[str, FieldPathSetter]] = {}
        self._pending_remove_operations_per_primary_key: Dict[str, Dict[str, List[DatabasePathElement]]] = {}
        self._debug = False

    @property
    def debug(self) -> bool:
        return self._debug

    @debug.setter
    def debug(self, debug: bool):
        self._debug = debug

    def clear_cached_data(self):
        self._cached_data_per_primary_key = {}

    def clear_cached_data_for_record(self, record_primary_key: str):
        self._cached_data_per_primary_key.pop(record_primary_key, None)

    def clear_pending_update_operations(self):
        self._pending_update_operations_per_primary_key = {}

    def clear_pending_remove_operations(self):
        self._pending_remove_operations_per_primary_key = {}

    def clear_pending_operations(self):
        self.clear_pending_update_operations()
        self.clear_pending_remove_operations()

    def clear_cached_data_and_pending_operations(self):
        self.clear_cached_data()
        self.clear_pending_operations()

    def _index_cached_data(self, primary_key_value: str) -> dict:
        if primary_key_value not in self._cached_data_per_primary_key:
            self._cached_data_per_primary_key[primary_key_value] = {}
        return self._cached_data_per_primary_key[primary_key_value]

    def _index_pending_update_operations(self, primary_key_value: str) -> dict:
        if primary_key_value not in self._pending_update_operations_per_primary_key:
            self._pending_update_operations_per_primary_key[primary_key_value] = {}
        return self._pending_update_operations_per_primary_key[primary_key_value]

    def _index_pending_remove_operations(self, primary_key_value: str) -> dict:
        if primary_key_value not in self._pending_remove_operations_per_primary_key:
            self._pending_remove_operations_per_primary_key[primary_key_value] = {}
        return self._pending_remove_operations_per_primary_key[primary_key_value]

    def has_pending_update_operations(self) -> bool:
        for index_operations in self._pending_update_operations_per_primary_key.values():
            if len(index_operations) > 0:
                return True
        return False

    def has_pending_remove_operations(self) -> bool:
        for index_operations in self._pending_remove_operations_per_primary_key.values():
            if len(index_operations) > 0:
                return True
        return False

    def has_pending_operations(self) -> bool:
        return self.has_pending_update_operations() or self.has_pending_remove_operations()

    def commit_update_operations(self) -> bool:
        raise Exception("commit_update_operations not implemented")

    def commit_remove_operations(self) -> bool:
        raise Exception("commit_remove_operations not implemented")

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
                    navigated_cached_data[stringed_element_key] = {}
                navigated_cached_data = navigated_cached_data[stringed_element_key]

            last_field_path_element = field_path_elements[-1]
            navigated_cached_data[f'{last_field_path_element.element_key}'] = data
            # todo: handle list's and set's

    def _cache_get_data(self, primary_key_value: str, field_path_elements: List[DatabasePathElement]) -> Tuple[bool, Any]:
        if len(field_path_elements) > 0:
            if field_path_elements[0].element_key == self.primary_index_name:  # todo: replace primary_index_name by primary field name
                return True, primary_key_value
            else:
                index_cached_data: dict = self._index_cached_data(primary_key_value=primary_key_value)
                navigated_cached_data: dict = index_cached_data
                
                for path_element in field_path_elements[:-1]:
                    stringed_element_key = f'{path_element.element_key}'
                    # We wrap the element_key inside a string, to handle a scenario where we would put an item from a list,
                    # where the element_key will be an int, that could be above zero, and cannot be handled by a classical list.
                    navigated_cached_data = (
                        navigated_cached_data[stringed_element_key]
                        if stringed_element_key in navigated_cached_data
                        else path_element.get_default_value()
                    )

                last_field_path_element: DatabasePathElement = field_path_elements[-1]
                if last_field_path_element.element_key in navigated_cached_data:
                    retrieved_item_value: Any = navigated_cached_data[last_field_path_element.element_key]
                    return True, retrieved_item_value
        return False, None

    def _cache_delete_field(self, index_cached_data: dict, primary_key_value: str, field_path_elements: List[DatabasePathElement]) -> str:
        """Will remove the element value from the cache, and remove any update operations associated with the same field in the same index and key_value"""
        BaseCachingTable._cache_put_data(index_cached_data=index_cached_data, field_path_elements=field_path_elements, data=None)

        pending_update_operations: dict = self._index_pending_update_operations(primary_key_value=primary_key_value)
        item_joined_field_path: str = join_field_path_elements(field_path_elements)

        if item_joined_field_path in pending_update_operations:
            pending_update_operations.pop(item_joined_field_path)
        return item_joined_field_path

    def _cache_remove_field(self, index_cached_data: dict, primary_key_value: str, field_path_elements: List[DatabasePathElement]):
        """Unlike the _cache_delete_field, this must be used when a remove operation to the database will be performed right away"""
        item_joined_field_path = self._cache_delete_field(
            index_cached_data=index_cached_data,
            primary_key_value=primary_key_value,
            field_path_elements=field_path_elements
        )
        pending_remove_operations = self._index_pending_remove_operations(primary_key_value=primary_key_value)
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

    def _put_record(self, middleware: Callable[[dict], bool], record_dict_data: dict) -> bool:
        # todo: integrate with caching
        self.model_virtual_map_field.populate(value=record_dict_data)
        validated_data, is_valid = self.model_virtual_map_field.validate_data()
        return middleware(validated_data) if is_valid is True else False

    def _delete_record(self, middleware: Callable[[dict], bool], indexes_keys_selectors: dict) -> bool:
        # todo: integrate with caching
        found_all_indexes = True
        for index_key, index_target_value in indexes_keys_selectors.items():
            index_matching_field = getattr(self.model, index_key, None)
            if index_matching_field is None:
                found_all_indexes = False
                print(message_with_vars(
                    message="An index key selector passed to the delete_record function, was not found, in the table model. Operation not executed.",
                    vars_dict={'index_key': index_key, 'index_target_value': index_target_value, 'index_matching_field': index_matching_field, 'table.model': self.model}
                ))
        return middleware(indexes_keys_selectors) if found_all_indexes is True else False

    def _add_primary_key_to_path_elements(self, source_path_elements: List[List[DatabasePathElement]]) -> List[List[DatabasePathElement]]:
        for item_path_elements in source_path_elements:
            if len(item_path_elements) > 0:
                first_path_element = item_path_elements[0]
                if first_path_element.element_key == self.primary_index_name:
                    return source_path_elements

        return [self._get_primary_key_database_path(), *source_path_elements]

    def _get_field(
            self, middleware: Callable[[List[DatabasePathElement] or Dict[str, List[DatabasePathElement]], bool], Any],
            key_value: str, field_path: str, query_kwargs: Optional[dict] = None
    ) -> Any:

        field_path_elements, has_multiple_fields_path = process_and_make_single_rendered_database_path(
            field_path=field_path, fields_switch=self.fields_switch, query_kwargs=query_kwargs
        )
        if has_multiple_fields_path is not True:
            field_path_elements: List[DatabasePathElement]
            found_in_cache, field_value_from_cache = self._cache_get_data(
                primary_key_value=key_value, field_path_elements=field_path_elements
            )
            if found_in_cache is True:
                return field_value_from_cache if self.debug is not True else {'value': field_value_from_cache, 'fromCache': True}

            response_data = middleware(field_path_elements, has_multiple_fields_path)

            index_cached_data = self._index_cached_data(primary_key_value=key_value)
            BaseCachingTable._cache_put_data(index_cached_data=index_cached_data, field_path_elements=field_path_elements, data=response_data)
            return response_data if self.debug is not True else {'value': response_data, 'fromCache': False}
        else:
            field_path_elements: Dict[str, List[DatabasePathElement]]
            output_items: Dict[str, Any] = {}

            keys_fields_already_cached_to_pop: List[str] = []
            for item_key, item_field_path_elements in field_path_elements.items():
                found_item_value_in_cache, field_item_value_from_cache = self._cache_get_data(
                    primary_key_value=key_value, field_path_elements=item_field_path_elements
                )
                if found_item_value_in_cache is True:
                    # We do not use a .get('key', None), because None can be a valid value for a field
                    output_items[item_key] = (
                        field_item_value_from_cache if self.debug is not True else
                        {'value': field_item_value_from_cache, 'fromCache': True}
                    )
                    keys_fields_already_cached_to_pop.append(item_key)

            for key_to_pop in keys_fields_already_cached_to_pop:
                field_path_elements.pop(key_to_pop)

            if len(field_path_elements) > 0:
                retrieved_items: Optional[dict] = middleware(field_path_elements, has_multiple_fields_path)
                if retrieved_items is not None:
                    for item_key, item_value in retrieved_items.items():
                        output_items[item_key] = (
                            item_value if self.debug is not True else
                            {'fromCache': False, 'value': item_value}
                        )

            return output_items

    def _process_cache_record_value(self, value: Any, primary_key_value: Any, field_path_elements: List[DatabasePathElement]):
        output = (
            value if self.debug is not True else
            {'value': value, 'fromCache': False}
        )
        record_cached_data: dict = self._index_cached_data(primary_key_value=primary_key_value)
        BaseCachingTable._cache_put_data(index_cached_data=record_cached_data, field_path_elements=field_path_elements, data=value)
        return output

    def _process_cache_record_item(self, record_item_data: dict, primary_key_value: str, fields_path_elements: Dict[str, List[DatabasePathElement]]) -> dict:
        output: dict = {}
        record_cached_data: dict = self._index_cached_data(primary_key_value=primary_key_value)
        for item_key, item_field_path_elements in fields_path_elements.items():
            record_item_matching_item_data: Optional[Any] = record_item_data.get(item_key, None)
            BaseCachingTable._cache_put_data(
                index_cached_data=record_cached_data,
                field_path_elements=item_field_path_elements,
                data=record_item_matching_item_data
            )
            output[item_key] = (
                record_item_matching_item_data if self.debug is not True else
                {'fromCache': False, 'value': record_item_matching_item_data}
            )
        return output

    def inner_query_fields_secondary_index(
            self, middleware: Callable[[List[DatabasePathElement] or Dict[str, List[DatabasePathElement]], bool], Any],
            field_path_elements: List[DatabasePathElement] or Dict[str, List[DatabasePathElement]], has_multiple_fields_path: bool,
    ) -> Optional[dict]:
        from StructNoSQL.tables.shared_table_behaviors import _inner_query_fields_secondary_index
        return _inner_query_fields_secondary_index(
            process_record_value=self._process_cache_record_value,
            process_record_item=self._process_cache_record_item,
            primary_index_name=self.primary_index_name,
            get_primary_key_database_path=self._get_primary_key_database_path,
            middleware=middleware,
            field_path_elements=field_path_elements,
            has_multiple_fields_path=has_multiple_fields_path
        )

    def _shared_rar(
            self, middleware: Callable[[List[DatabasePathElement] or Dict[str, List[DatabasePathElement]], bool], List[Any]],
            fields_path_elements: Dict[str, List[DatabasePathElement]], key_value: str
    ):
        existing_record_data: dict = {}

        keys_fields_already_cached_to_pop: List[str] = []
        for item_key, item_field_path_elements in fields_path_elements.items():
            found_item_value_in_cache, field_item_value_from_cache = self._cache_get_data(
                primary_key_value=key_value, field_path_elements=item_field_path_elements
            )
            if found_item_value_in_cache is True:
                existing_record_data[item_key] = (
                    field_item_value_from_cache if self.debug is not True else
                    {'value': field_item_value_from_cache, 'fromCache': True}
                )
                keys_fields_already_cached_to_pop.append(item_key)

        for key_to_pop in keys_fields_already_cached_to_pop:
            fields_path_elements.pop(key_to_pop)

        if len(fields_path_elements) > 0:
            retrieved_records_items_data: Optional[List[dict]] = middleware(fields_path_elements, True)
            if retrieved_records_items_data is not None and len(retrieved_records_items_data) > 0:
                # Since we query the primary_index, we know for a fact that we will never be returned more than
                # one record item. Hence why we do not have a loop that iterate over the records_items_data,
                # and that we return a dict with the only one record item being the requested key_value.
                return {key_value: {
                    **existing_record_data,
                    **self._process_cache_record_item(
                        record_item_data=retrieved_records_items_data[0],
                        primary_key_value=key_value,
                        fields_path_elements=fields_path_elements
                    )
                }}
        return {key_value: existing_record_data}

    def _query_field(
            self, middleware: Callable[[List[DatabasePathElement] or Dict[str, List[DatabasePathElement]], bool], List[Any]],
            key_value: str, field_path: str, query_kwargs: Optional[dict] = None, index_name: Optional[str] = None
    ) -> Optional[dict]:

        field_path_elements, has_multiple_fields_path = process_and_make_single_rendered_database_path(
            field_path=field_path, fields_switch=self.fields_switch, query_kwargs=query_kwargs
        )
        if index_name is not None and index_name != self.primary_index_name:
            return self.inner_query_fields_secondary_index(
                middleware=middleware,
                field_path_elements=field_path_elements,
                has_multiple_fields_path=has_multiple_fields_path
            )
        else:
            # If requested index is primary index
            if has_multiple_fields_path is not True:
                field_path_elements: List[DatabasePathElement]
                found_in_cache, field_value_from_cache = self._cache_get_data(
                    primary_key_value=key_value, field_path_elements=field_path_elements
                )
                if found_in_cache is True:
                    # A single field was requested from a primary index, which means that only one record will have been 
                    # found for the specified index (primary index values are unique). Since the query_field function needs 
                    # to return a Dict[str, Any] with keys being the primary_key_value of the record, the the item being 
                    # the request data, we need to create a dict that wrap our retrieved value as a record result.
                    return {key_value: (
                        field_value_from_cache if self.debug is not True else
                        {'value': field_value_from_cache, 'fromCache': True}
                    )}

                retrieved_records_items_data: Optional[List[Any]] = middleware(field_path_elements, has_multiple_fields_path)
                if retrieved_records_items_data is None:
                    return None

                records_output: dict = {}
                for record_primary_key_value in retrieved_records_items_data:
                    records_output[record_primary_key_value] = self._process_cache_record_value(
                        value=record_primary_key_value,
                        primary_key_value=record_primary_key_value,
                        field_path_elements=field_path_elements
                    )
                return records_output
            else:
                field_path_elements: Dict[str, List[DatabasePathElement]]
                return self._shared_rar(middleware=middleware, fields_path_elements=field_path_elements, key_value=key_value)

    def _query_multiple_fields(
            self, middleware: Callable[[Dict[str, List[DatabasePathElement]]], List[Any]],
            key_value: str, getters: Dict[str, FieldGetter], index_name: Optional[str] = None
    ):
        getters_database_paths, single_getters_database_paths_elements, grouped_getters_database_paths_elements = (
            _prepare_getters(fields_switch=self.fields_switch, getters=getters)
        )
        if len(grouped_getters_database_paths_elements) > 0:
            raise Exception(f"grouped_getters_database_paths_elements not yet supported")

        if index_name is None or index_name == self.primary_index_name:
            return self._shared_rar(middleware=middleware, fields_path_elements=single_getters_database_paths_elements, key_value=key_value)
        else:
            return self.inner_query_fields_secondary_index(
                middleware=middleware,
                field_path_elements=single_getters_database_paths_elements,
                has_multiple_fields_path=True
            )

    def _unpack_getters_response_item(
            self, response_item: dict,
            single_getters_database_paths_elements: Dict[str, List[DatabasePathElement]],
            grouped_getters_database_paths_elements: Dict[str, Dict[str, List[DatabasePathElement]]]
    ):
        def item_mutator(item: Any):
            return item if self.debug is not True else {'value': item, 'fromCache': False}

        from StructNoSQL.tables.shared_table_behaviors import _base_unpack_getters_response_item
        return _base_unpack_getters_response_item(
            item_mutator=item_mutator, response_item=response_item,
            single_getters_database_paths_elements=single_getters_database_paths_elements,
            grouped_getters_database_paths_elements=grouped_getters_database_paths_elements
        )

    def _get_multiple_fields(
            self, middleware: Callable[[List[List[DatabasePathElement]]], Any],
            key_value: str, getters: Dict[str, FieldGetter]
    ) -> Optional[dict]:
        output_data: Dict[str, Any] = {}
        index_cached_data: dict = self._index_cached_data(primary_key_value=key_value)

        single_getters_database_paths_elements: Dict[str, List[DatabasePathElement]] = {}
        grouped_getters_database_paths_elements: Dict[str, Dict[str, List[DatabasePathElement]]] = {}

        getters_database_paths: List[List[DatabasePathElement]] = []
        for getter_key, getter_item in getters.items():
            field_path_elements, has_multiple_fields_path = process_and_make_single_rendered_database_path(
                field_path=getter_item.field_path, fields_switch=self.fields_switch, query_kwargs=getter_item.query_kwargs
            )
            if has_multiple_fields_path is not True:
                field_path_elements: List[DatabasePathElement]
                found_value_in_cache, field_value_from_cache = self._cache_get_data(
                    primary_key_value=key_value, field_path_elements=field_path_elements
                )
                if found_value_in_cache is True:
                    output_data[getter_key] = (
                        field_value_from_cache if self.debug is not True else
                        {'value': field_value_from_cache, 'fromCache': True}
                    )
                else:
                    single_getters_database_paths_elements[getter_key] = field_path_elements
                    getters_database_paths.append(field_path_elements)
            else:
                field_path_elements: Dict[str, List[DatabasePathElement]]
                current_getter_grouped_database_paths_elements: Dict[str, List[DatabasePathElement]] = {}
                container_data: Dict[str, Any] = {}

                for child_item_key, child_item_field_path_elements in field_path_elements.items():
                    found_item_value_in_cache, field_item_value_from_cache = self._cache_get_data(
                        primary_key_value=key_value, field_path_elements=child_item_field_path_elements
                    )
                    if found_item_value_in_cache is True:
                        container_data[child_item_key] = (
                            field_item_value_from_cache if self.debug is not True else
                            {'value': field_item_value_from_cache, 'fromCache': True}
                        )
                    else:
                        current_getter_grouped_database_paths_elements[child_item_key] = child_item_field_path_elements
                        getters_database_paths.append(child_item_field_path_elements)

                output_data[getter_key] = container_data
                grouped_getters_database_paths_elements[getter_key] = current_getter_grouped_database_paths_elements

        response_data = middleware(getters_database_paths)
        if response_data is None:
            return output_data

        unpacked_retrieved_items: dict = self._unpack_getters_response_item(
            response_item=response_data,
            single_getters_database_paths_elements=single_getters_database_paths_elements,
            grouped_getters_database_paths_elements=grouped_getters_database_paths_elements
        )
        return {**output_data, **unpacked_retrieved_items}

    def _update_field(self, key_value: str, field_path: str, value_to_set: Any, query_kwargs: Optional[dict] = None) -> bool:
        validated_data, valid, field_path_elements = process_validate_data_and_make_single_rendered_database_path(
            field_path=field_path, fields_switch=self.fields_switch, query_kwargs=query_kwargs, data_to_validate=value_to_set
        )
        if valid is True and field_path_elements is not None:
            index_cached_data = self._index_cached_data(primary_key_value=key_value)
            BaseCachingTable._cache_put_data(index_cached_data=index_cached_data, field_path_elements=field_path_elements, data=validated_data)

            joined_field_path = join_field_path_elements(field_path_elements)
            pending_update_operations = self._index_pending_update_operations(primary_key_value=key_value)
            pending_update_operations[joined_field_path] = FieldPathSetter(
                field_path_elements=field_path_elements, value_to_set=validated_data
            )
            return True
        return False

    def _update_multiple_fields(self, key_value: str, setters: List[FieldSetter or UnsafeFieldSetter]) -> bool:
        index_cached_data = self._index_cached_data(primary_key_value=key_value)
        for current_setter in setters:
            if isinstance(current_setter, FieldSetter):
                validated_data, valid, field_path_elements = process_validate_data_and_make_single_rendered_database_path(
                    field_path=current_setter.field_path, fields_switch=self.fields_switch,
                    query_kwargs=current_setter.query_kwargs, data_to_validate=current_setter.value_to_set
                )
                if valid is True:
                    BaseCachingTable._cache_put_data(index_cached_data=index_cached_data, field_path_elements=field_path_elements, data=validated_data)
                    joined_field_path = join_field_path_elements(field_path_elements)
                    pending_update_operations = self._index_pending_update_operations(primary_key_value=key_value)
                    pending_update_operations[joined_field_path] = FieldPathSetter(
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
                dynamodb_setters.append(FieldPathSetter(
                    field_path_elements=rendered_field_path_elements,
                    value_to_set=processed_value_to_set
                ))"""
        return True

    def _remove_field(
            self, middleware: Callable[[List[List[DatabasePathElement]]], Any],
            key_value: str, field_path: str, query_kwargs: Optional[dict] = None
    ) -> Optional[Any]:

        field_path_elements, has_multiple_fields_path = process_and_make_single_rendered_database_path(
            field_path=field_path, fields_switch=self.fields_switch, query_kwargs=query_kwargs
        )
        index_cached_data = self._index_cached_data(primary_key_value=key_value)

        if has_multiple_fields_path is not True:
            field_path_elements: List[DatabasePathElement]
            found_value_in_cache, field_value_from_cache = self._cache_get_data(
                primary_key_value=key_value, field_path_elements=field_path_elements
            )
            if found_value_in_cache is True:
                pending_remove_operations = self._index_pending_remove_operations(primary_key_value=key_value)
                self._cache_add_delete_operation(
                    index_cached_data=index_cached_data,
                    pending_remove_operations=pending_remove_operations,
                    field_path_elements=field_path_elements
                )
                # Even when we retrieve a removed value from the cache, and that we do not need to perform a remove operation right away to retrieve
                # the removed value, we still want to add a delete_operation that will be performed on operation commits, because if we remove a value
                # from the cache, it does not remove a potential older value present in the database, that the remove operation should remove.
                return (
                    field_value_from_cache if self.debug is not True else
                    {'value': field_value_from_cache, 'fromCache': True}
                )
            else:
                target_path_elements: List[List[DatabasePathElement]] = [field_path_elements]
                self._cache_remove_field(
                    index_cached_data=index_cached_data,
                    primary_key_value=key_value,
                    field_path_elements=field_path_elements
                )
                response_attributes = middleware(target_path_elements)
                if response_attributes is not None:
                    removed_item_data = navigate_into_data_with_field_path_elements(
                        data=response_attributes, field_path_elements=field_path_elements,
                        num_keys_to_navigation_into=len(field_path_elements)
                    )
                    return (
                        removed_item_data if self.debug is not True else
                        {'value': removed_item_data, 'fromCache': False}
                    )
                else:
                    return (
                        None if self.debug is not True else
                        {'value': None, 'fromCache': False}
                    )
        else:
            field_path_elements: Dict[str, List[DatabasePathElement]]
            target_path_elements: List[List[DatabasePathElement]] = []
            container_output_data: Dict[str, Any] = {}

            for item_field_path_elements_value in field_path_elements.values():
                item_last_path_element = item_field_path_elements_value[-1]

                found_item_value_in_cache, field_item_value_from_cache = self._cache_get_data(
                    primary_key_value=key_value, field_path_elements=item_field_path_elements_value
                )
                if found_item_value_in_cache is True:
                    pending_remove_operations = self._index_pending_remove_operations(primary_key_value=key_value)
                    self._cache_add_delete_operation(
                        index_cached_data=index_cached_data,
                        pending_remove_operations=pending_remove_operations,
                        field_path_elements=item_field_path_elements_value
                    )
                    container_output_data[item_last_path_element.element_key] = (
                        field_item_value_from_cache if self.debug is not True else
                        {'value': field_item_value_from_cache, 'fromCache': True}
                    )
                else:
                    target_path_elements.append(item_field_path_elements_value)
                    self._cache_remove_field(
                        index_cached_data=index_cached_data,
                        primary_key_value=key_value,
                        field_path_elements=item_field_path_elements_value
                    )

            if len(target_path_elements) > 0:
                response_attributes = middleware(target_path_elements)
                if response_attributes is not None:
                    for key, child_item_field_path_elements in field_path_elements.items():
                        removed_item_data = navigate_into_data_with_field_path_elements(
                            data=response_attributes, field_path_elements=child_item_field_path_elements,
                            num_keys_to_navigation_into=len(child_item_field_path_elements)
                        )
                        container_output_data[key] = (
                            removed_item_data if self.debug is not True else
                            {'value': removed_item_data, 'fromCache': False}
                        )

            return container_output_data

    def _remove_multiple_fields(
            self, middleware: Callable[[List[List[DatabasePathElement]]], Any],
            key_value: str, removers: Dict[str, FieldRemover]
    ) -> Optional[Dict[str, Any]]:

        return {key: self._remove_field(
            middleware=middleware, key_value=key_value,
            field_path=item.field_path, query_kwargs=item.query_kwargs
        ) for key, item in removers.items()}

    def _delete_field(self, key_value: str, field_path: str, query_kwargs: Optional[dict] = None) -> bool:
        index_cached_data = self._index_cached_data(primary_key_value=key_value)
        pending_remove_operations = self._index_pending_remove_operations(primary_key_value=key_value)
        self._cache_process_add_delete_operation(
            index_cached_data=index_cached_data,
            pending_remove_operations=pending_remove_operations,
            field_path=field_path, query_kwargs=query_kwargs
        )
        return True

    def _grouped_remove_multiple_fields(
            self, middleware: Callable[[List[List[DatabasePathElement]]], Any],
            key_value: str, removers: Dict[str, FieldRemover]
    ) -> Optional[Dict[str, Any]]:
        # todo: do not perform the operation but store it as pending if a matching value exists in the cache
        if not len(removers) > 0:
            # If no remover has been specified, we do not run the database
            # operation, and since no value has been removed, we return None.
            return None
        else:
            output_data: dict = {}
            index_cached_data = self._index_cached_data(primary_key_value=key_value)

            removers_field_paths_elements: Dict[str, List[DatabasePathElement]] = {}
            grouped_removers_field_paths_elements: Dict[str, Dict[str, List[DatabasePathElement]]] = {}

            # todo: add the remove items present in cache to the output_data instead of only returning the items not removed from cache

            removers_database_paths: List[List[DatabasePathElement]] = []
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
                        index_cached_data=index_cached_data,
                        primary_key_value=key_value,
                        field_path_elements=field_path_elements
                    )
                else:
                    field_path_elements: Dict[str, List[DatabasePathElement]]
                    grouped_removers_field_paths_elements[remover_key] = field_path_elements
                    field_path_elements_values = list(field_path_elements.values())
                    for item_field_path_elements_values in field_path_elements_values:
                        removers_database_paths.append(item_field_path_elements_values)
                        self._cache_remove_field(
                            index_cached_data=index_cached_data,
                            primary_key_value=key_value,
                            field_path_elements=item_field_path_elements_values
                        )

            response_attributes = middleware(removers_database_paths)
            if response_attributes is None:
                return None

            return self._unpack_getters_response_item(
                response_item=response_attributes,
                single_getters_database_paths_elements=removers_field_paths_elements,
                grouped_getters_database_paths_elements=grouped_removers_field_paths_elements
            )

    def _grouped_delete_multiple_fields(self, key_value: str, removers: List[FieldRemover]) -> bool:
        index_cached_data = self._index_cached_data(primary_key_value=key_value)
        pending_remove_operations = self._index_pending_remove_operations(primary_key_value=key_value)

        for current_remover in removers:
            self._cache_process_add_delete_operation(
                index_cached_data=index_cached_data,
                pending_remove_operations=pending_remove_operations,
                field_path=current_remover.field_path,
                query_kwargs=current_remover.query_kwargs
            )
        return True
