from typing import Optional, List, Dict, Any, Tuple, Callable, Iterable

from StructNoSQL import PrimaryIndex
from StructNoSQL.models import DatabasePathElement, FieldGetter, FieldSetter, UnsafeFieldSetter, FieldRemover, FieldPathSetter
from StructNoSQL.practical_logger import message_with_vars
from StructNoSQL.tables.base_table import BaseTable
from StructNoSQL.tables.shared_table_behaviors import _prepare_getters, _model_contain_all_index_keys
from StructNoSQL.utils.data_processing import navigate_into_data_with_field_path_elements
from StructNoSQL.utils.process_render_fields_paths import process_and_make_single_rendered_database_path, \
    process_validate_data_and_make_single_rendered_database_path


class BaseBasicTable(BaseTable):
    def __init__(self, data_model, primary_index: PrimaryIndex):
        super().__init__(data_model=data_model, primary_index=primary_index)

    def _put_record(self, middleware: Callable[[dict], bool], record_dict_data: dict) -> bool:
        self.model_virtual_map_field.populate(value=record_dict_data)
        validated_data, is_valid = self.model_virtual_map_field.validate_data()
        return middleware(validated_data) if is_valid is True else False

    def _record_deletion(self, middleware: Callable[[dict], Any], indexes_keys_selectors: dict) -> Any:
        """Used by both the delete_record and remove_record operation. Hence, the Any in the return type of the middleware."""
        found_all_indexes: bool = _model_contain_all_index_keys(model=self.model, indexes_keys=indexes_keys_selectors.keys())
        return middleware(indexes_keys_selectors) if found_all_indexes is True else False

    def _get_field(
            self, middleware: Callable[[List[DatabasePathElement] or Dict[str, List[DatabasePathElement]], bool], Any],
            field_path: str, query_kwargs: Optional[dict] = None
    ) -> Any:
        field_path_elements, has_multiple_fields_path = process_and_make_single_rendered_database_path(
            field_path=field_path, fields_switch=self.fields_switch, query_kwargs=query_kwargs
        )
        return middleware(field_path_elements, has_multiple_fields_path)

    @staticmethod
    def _process_cache_record_value(value: Any, primary_key_value: Any, field_path_elements: List[DatabasePathElement]):
        return value

    @staticmethod
    def _process_cache_record_item(record_item_data: dict, primary_key_value: str, fields_path_elements: Dict[str, List[DatabasePathElement]]) -> dict:
        return {item_key: record_item_data.get(item_key, None) for item_key in fields_path_elements.keys()}

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
                retrieved_records_items_data: Optional[List[Any]] = middleware(field_path_elements, has_multiple_fields_path)
                if retrieved_records_items_data is None:
                    return None

                if retrieved_records_items_data is not None and len(retrieved_records_items_data) > 0:
                    # Since we query the primary_index, we know for a fact that we will never be returned more than
                    # one record item. Hence why we do not have a loop that iterate over the records_items_data,
                    # and that we return a dict with the only one record item being the requested key_value.
                    return {key_value: self._process_cache_record_value(
                        value=retrieved_records_items_data[0],
                        primary_key_value=key_value,
                        field_path_elements=field_path_elements
                    )}
            else:
                field_path_elements: Dict[str, List[DatabasePathElement]]
                if not len(field_path_elements) > 0:
                    return {key_value: {}}

                retrieved_records_items_data: Optional[List[dict]] = middleware(field_path_elements, has_multiple_fields_path)
                if retrieved_records_items_data is not None and len(retrieved_records_items_data) > 0:
                    # Since we query the primary_index, we know for a fact that we will never be returned more than
                    # one record item. Hence why we do not have a loop that iterate over the records_items_data,
                    # and that we return a dict with the only one record item being the requested key_value.
                    return {key_value: self._process_cache_record_item(
                        record_item_data=retrieved_records_items_data[0],
                        primary_key_value=key_value,
                        fields_path_elements=field_path_elements
                    )}

    def _query_multiple_fields(
            self, middleware: Callable[[Dict[str, List[DatabasePathElement]], bool], List[Any]],
            key_value: str, getters: Dict[str, FieldGetter], index_name: Optional[str] = None
    ):
        getters_database_paths, single_getters_database_paths_elements, grouped_getters_database_paths_elements = (
            _prepare_getters(fields_switch=self.fields_switch, getters=getters)
        )
        if len(grouped_getters_database_paths_elements) > 0:
            raise Exception(f"grouped_getters_database_paths_elements not yet supported")

        if index_name is None or index_name == self.primary_index_name:
            retrieved_records_items_data: Optional[List[Any]] = middleware(single_getters_database_paths_elements, True)
            if retrieved_records_items_data is not None and len(retrieved_records_items_data) > 0:
                return {key_value: retrieved_records_items_data[0]}
            return None
        else:
            return self.inner_query_fields_secondary_index(
                middleware=middleware,
                field_path_elements=single_getters_database_paths_elements,
                has_multiple_fields_path=True
            )

    @staticmethod
    def _unpack_getters_response_item(
            response_item: dict,
            single_getters_database_paths_elements: Dict[str, List[DatabasePathElement]],
            grouped_getters_database_paths_elements: Dict[str, Dict[str, List[DatabasePathElement]]]
    ):
        def item_mutator(item: Any):
            return item

        from StructNoSQL.tables.shared_table_behaviors import _base_unpack_getters_response_item
        return _base_unpack_getters_response_item(
            item_mutator=item_mutator, response_item=response_item,
            single_getters_database_paths_elements=single_getters_database_paths_elements,
            grouped_getters_database_paths_elements=grouped_getters_database_paths_elements
        )

    def _get_multiple_fields(
            self, middleware: Callable[[List[List[DatabasePathElement]]], Any], getters: Dict[str, FieldGetter]
    ) -> Optional[dict]:

        getters_database_paths, single_getters_database_paths_elements, grouped_getters_database_paths_elements = (
            _prepare_getters(fields_switch=self.fields_switch, getters=getters)
        )
        response_data = middleware(getters_database_paths)
        if response_data is None:
            return None

        return self._unpack_getters_response_item(
            response_item=response_data,
            single_getters_database_paths_elements=single_getters_database_paths_elements,
            grouped_getters_database_paths_elements=grouped_getters_database_paths_elements
        )

    def _update_field(
            self, middleware: Callable[[List[DatabasePathElement], Any], Any],
            field_path: str, value_to_set: Any, query_kwargs: Optional[dict] = None
    ) -> bool:
        validated_data, valid, field_path_elements = process_validate_data_and_make_single_rendered_database_path(
            field_path=field_path, fields_switch=self.fields_switch, query_kwargs=query_kwargs, data_to_validate=value_to_set
        )
        if valid is True and field_path_elements is not None:
            return middleware(field_path_elements, validated_data)
        return False

    def _update_multiple_fields(
            self, middleware: Callable[[List[FieldPathSetter]], Any],
            setters: List[FieldSetter or UnsafeFieldSetter]
    ) -> bool:
        dynamodb_setters: List[FieldPathSetter] = []
        for current_setter in setters:
            if isinstance(current_setter, FieldSetter):
                validated_data, valid, field_path_elements = process_validate_data_and_make_single_rendered_database_path(
                    field_path=current_setter.field_path, fields_switch=self.fields_switch,
                    query_kwargs=current_setter.query_kwargs, data_to_validate=current_setter.value_to_set
                )
                if valid is True:
                    dynamodb_setters.append(FieldPathSetter(
                        field_path_elements=field_path_elements, value_to_set=validated_data
                    ))
            elif isinstance(current_setter, UnsafeFieldSetter):
                raise Exception(f"UnsafeFieldSetter not supported in basic_table")

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
        response = middleware(dynamodb_setters)
        return True if response is not None else False

    def _base_removal(
            self, middleware: Callable[[List[List[DatabasePathElement]]], Any],
            field_path: str, query_kwargs: Optional[dict] = None
    ) -> Tuple[Optional[Dict[str, Any]], List[List[DatabasePathElement]]]:

        field_path_elements, has_multiple_fields_path = process_and_make_single_rendered_database_path(
            field_path=field_path, fields_switch=self.fields_switch, query_kwargs=query_kwargs
        )
        target_path_elements: List[List[DatabasePathElement]] = (
            [field_path_elements]
            if has_multiple_fields_path is not True else
            list(field_path_elements.values())
        )
        # The remove_data_elements_from_map function expect a List[List[DatabasePathElement]]. If we have a
        # single field_path, we wrap the field_path_elements inside a list. And if we have multiple fields_paths
        # (which will be structured inside a dict), we turn the convert the values of the dict to a list.
        return middleware(target_path_elements), target_path_elements

    def _remove_field(
            self, middleware: Callable[[List[List[DatabasePathElement]]], Any],
            field_path: str, query_kwargs: Optional[dict] = None
    ) -> Optional[Any]:
        response_attributes, all_fields_items_path_elements = self._base_removal(
            middleware=middleware, field_path=field_path, query_kwargs=query_kwargs
        )
        if response_attributes is not None:
            if not len(all_fields_items_path_elements) > 0:
                return None
            elif len(all_fields_items_path_elements) == 1:
                field_path_elements = all_fields_items_path_elements[0]
                removed_item_data = navigate_into_data_with_field_path_elements(
                    data=response_attributes, field_path_elements=field_path_elements,
                    num_keys_to_navigation_into=len(field_path_elements)
                )
                return removed_item_data
            else:
                removed_items_values: Dict[str, Any] = {}
                for field_path_elements in all_fields_items_path_elements:
                    # Even the remove_field function can potentially remove multiple
                    # field_path_elements if the field_path expression is selecting multiple fields.
                    last_path_element = field_path_elements[len(field_path_elements) - 1]
                    removed_items_values[last_path_element.element_key] = navigate_into_data_with_field_path_elements(
                        data=response_attributes, field_path_elements=field_path_elements,
                        num_keys_to_navigation_into=len(field_path_elements)
                    )
                return removed_items_values
        return None

    def _delete_field(
            self, middleware: Callable[[List[List[DatabasePathElement]]], Any],
            field_path: str, query_kwargs: Optional[dict] = None
    ) -> bool:
        response_attributes, _ = self._base_removal(
            middleware=middleware, field_path=field_path, query_kwargs=query_kwargs
        )
        return True if response_attributes is not None else False

    def _grouped_remove_multiple_fields(
            self, middleware: Callable[[List[List[DatabasePathElement]]], Any], removers: Dict[str, FieldRemover]
    ) -> Optional[Dict[str, Any]]:
        if not len(removers) > 0:
            # If no remover has been specified, we do not run the database
            # operation, and since no value has been removed, we return None.
            return None
        else:
            removers_field_paths_elements: Dict[str, List[DatabasePathElement]] = {}
            grouped_removers_field_paths_elements: Dict[str, Dict[str, List[DatabasePathElement]]] = {}

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
                else:
                    field_path_elements: Dict[str, List[DatabasePathElement]]
                    grouped_removers_field_paths_elements[remover_key] = field_path_elements
                    removers_database_paths.extend(field_path_elements.values())

            response_attributes: Optional[Any] = middleware(removers_database_paths)
            if response_attributes is None:
                return None

            return self._unpack_getters_response_item(
                response_item=response_attributes,
                single_getters_database_paths_elements=removers_field_paths_elements,
                grouped_getters_database_paths_elements=grouped_removers_field_paths_elements
            )

    def _grouped_delete_multiple_fields(
            self, middleware: Callable[[List[List[DatabasePathElement]]], Any], removers: List[FieldRemover],
    ) -> bool:
        if not len(removers) > 0:
            # If no remover has been specified, we do not run the database operation, yet we still
            # return True, since technically, what needed to be performed (nothing) was performed.
            return True

        removers_database_paths: List[List[DatabasePathElement]] = []
        for current_remover in removers:
            field_path_elements, has_multiple_fields_path = process_and_make_single_rendered_database_path(
                field_path=current_remover.field_path, fields_switch=self.fields_switch,
                query_kwargs=current_remover.query_kwargs
            )
            if has_multiple_fields_path is not True:
                field_path_elements: List[DatabasePathElement]
                removers_database_paths.append(field_path_elements)
            else:
                field_path_elements: Dict[str, List[DatabasePathElement]]
                removers_database_paths.append(*field_path_elements.values())

        response = middleware(removers_database_paths)
        return True if response is not None else False
