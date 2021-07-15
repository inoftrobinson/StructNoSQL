from typing import Optional, List, Dict, Any, Tuple, Callable, Iterable, Union

from StructNoSQL import PrimaryIndex, BaseField
from StructNoSQL.models import DatabasePathElement, FieldGetter, FieldSetter, UnsafeFieldSetter, FieldRemover, FieldPathSetter
from StructNoSQL.practical_logger import message_with_vars
from StructNoSQL.tables.base_table import BaseTable
from StructNoSQL.tables.shared_table_behaviors import _prepare_getters, _model_contain_all_index_keys
from StructNoSQL.utils.data_processing import navigate_into_data_with_field_path_elements
from StructNoSQL.utils.process_render_fields_paths import process_and_make_single_rendered_database_path,\
    process_validate_data_and_make_single_rendered_database_path


class BaseBasicTable(BaseTable):
    def __init__(self, data_model, primary_index: PrimaryIndex):
        super().__init__(data_model=data_model, primary_index=primary_index)

    def _put_record(self, middleware: Callable[[dict], bool], record_dict_data: dict) -> bool:
        self.model_virtual_map_field.populate(value=record_dict_data)
        validated_data, is_valid = self.model_virtual_map_field.validate_data()
        return middleware(validated_data) if is_valid is True else False

    def _delete_record(self, middleware: Callable[[dict], bool], indexes_keys_selectors: dict) -> bool:
        found_all_indexes: bool = _model_contain_all_index_keys(model=self.model, indexes_keys=indexes_keys_selectors.keys())
        if found_all_indexes is not True:
            return False
        return middleware(indexes_keys_selectors)

    def _remove_record(
            self, middleware: Callable[[dict], Optional[dict]],
            indexes_keys_selectors: dict, data_validation: bool
    ) -> Optional[dict]:
        found_all_indexes: bool = _model_contain_all_index_keys(model=self.model, indexes_keys=indexes_keys_selectors.keys())
        if found_all_indexes is not True:
            return None

        removed_record_data: Optional[dict] = middleware(indexes_keys_selectors)
        if removed_record_data is None:
            return None

        if data_validation is True:
            self.model_virtual_map_field.populate(value=removed_record_data)
            validated_data, is_valid = self.model_virtual_map_field.validate_data()
            return validated_data
        else:
            return removed_record_data

    def _get_field(
            self, middleware: Callable[[List[DatabasePathElement] or Dict[str, List[DatabasePathElement]], bool], Optional[Any]],
            field_path: str, query_kwargs: Optional[dict], data_validation: bool
    ) -> Optional[Any]:
        target_field_container, has_multiple_fields_path = process_and_make_single_rendered_database_path(
            field_path=field_path, fields_switch=self.fields_switch, query_kwargs=query_kwargs
        )
        if has_multiple_fields_path is not True:
            target_field_container: Tuple[BaseField, List[DatabasePathElement]]
            field_object, field_path_elements = target_field_container

            retrieved_item_data: Optional[Any] = middleware(field_path_elements, False)
            if data_validation is not True:
                return retrieved_item_data
            else:
                field_object.populate(value=retrieved_item_data)
                validated_data, is_valid = field_object.validate_data()
                return validated_data
        else:
            target_field_container: Dict[str, Tuple[BaseField, List[DatabasePathElement]]]

            fields_paths_elements: Dict[str, List[DatabasePathElement]] = {key: item[1] for key, item in target_field_container.items()}
            retrieved_items_data: Dict[str, Optional[Any]] = middleware(fields_paths_elements, True)

            output_data: Dict[str, Optional[Any]] = {}
            for item_key, item_container in target_field_container.items():
                field_object, field_path_elements = item_container

                matching_item_data: Optional[Any] = retrieved_items_data.get(item_key, None)
                if data_validation is not True:
                    output_data[item_key] = matching_item_data
                else:
                    field_object.populate(value=matching_item_data)
                    validated_data, is_valid = field_object.validate_data()
                    output_data[item_key] = validated_data

            return output_data

    @staticmethod
    def _process_cache_record_value(
            data_validation: bool, value: Any, primary_key_value: Any,
            target_field_container: Tuple[BaseField, List[DatabasePathElement]]
    ) -> Optional[Any]:
        if data_validation is True:
            field_object, field_path_elements = target_field_container
            field_object.populate(value=value)
            validated_data, valid = field_object.validate_data()
            return validated_data
        else:
            return value

    @staticmethod
    def _process_cache_record_item(
            data_validation: bool, record_item_data: dict, primary_key_value: str,
            target_fields_containers: Dict[str, Tuple[BaseField, List[DatabasePathElement]]]
    ) -> dict:
        output_data: dict = {}

        if data_validation is True:
            for item_key, item_container in target_fields_containers.items():
                item_field_object, item_field_path_elements = item_container

                matching_item_data: Optional[Any] = record_item_data.get(item_key, None)
                item_field_object.populate(value=matching_item_data)
                validated_data, is_valid = item_field_object.validate_data()
                output_data[item_key] = validated_data
        else:
            for item_key in target_fields_containers.keys():
                matching_item_data: Optional[Any] = record_item_data.get(item_key, None)
                output_data[item_key] = matching_item_data

        return output_data

    def inner_query_fields_secondary_index(
            self, middleware: Callable[[List[DatabasePathElement] or Dict[str, List[DatabasePathElement]], bool], Any],
            target_field_container: Union[Tuple[BaseField, List[DatabasePathElement]], Dict[str, Tuple[BaseField, List[DatabasePathElement]]]],
            has_multiple_fields_path: bool, data_validation: bool,
    ) -> Optional[dict]:
        from StructNoSQL.tables.shared_table_behaviors import _inner_query_fields_secondary_index
        return _inner_query_fields_secondary_index(
            process_record_value=self._process_cache_record_value,
            process_record_item=self._process_cache_record_item,
            data_validation=data_validation,
            primary_index_name=self.primary_index_name,
            get_primary_key_database_path=self._get_primary_key_database_path,
            middleware=middleware,
            target_field_container=target_field_container,
            has_multiple_fields_path=has_multiple_fields_path
        )

    def _query_field(
            self, middleware: Callable[[List[DatabasePathElement] or Dict[str, List[DatabasePathElement]], bool], List[Any]],
            key_value: str, field_path: str, query_kwargs: Optional[dict], index_name: Optional[str], data_validation: bool
    ) -> Optional[dict]:

        target_field_container, has_multiple_fields_path = process_and_make_single_rendered_database_path(
            field_path=field_path, fields_switch=self.fields_switch, query_kwargs=query_kwargs
        )
        if index_name is not None and index_name != self.primary_index_name:
            return self.inner_query_fields_secondary_index(
                middleware=middleware,
                data_validation=data_validation,
                target_field_container=target_field_container,
                has_multiple_fields_path=has_multiple_fields_path
            )
        else:
            # If requested index is primary index
            if has_multiple_fields_path is not True:
                target_field_container: Tuple[BaseField, List[DatabasePathElement]]
                field_object, field_path_elements = target_field_container

                retrieved_records_items_data: Optional[List[Any]] = middleware(field_path_elements, False)
                if retrieved_records_items_data is not None and len(retrieved_records_items_data) > 0:
                    # Since we query the primary_index, we know for a fact that we will never be returned more than
                    # one record item. Hence why we do not have a loop that iterate over the records_items_data,
                    # and that we return a dict with the only one record item being the requested key_value.
                    return {key_value: self._process_cache_record_value(
                        data_validation=data_validation,
                        value=retrieved_records_items_data[0],
                        primary_key_value=key_value,
                        target_field_container=target_field_container
                    )}
                return None
            else:
                target_field_container: Dict[str, Tuple[BaseField, List[DatabasePathElement]]]

                if not len(target_field_container) > 0:
                    return {key_value: {}}

                fields_paths_elements: Dict[str, List[DatabasePathElement]] = {key: item[1] for key, item in target_field_container.items()}
                retrieved_records_items_data: Optional[List[dict]] = middleware(fields_paths_elements, True)
                if retrieved_records_items_data is not None and len(retrieved_records_items_data) > 0:
                    # Since we query the primary_index, we know for a fact that we will never be returned more than
                    # one record item. Hence why we do not have a loop that iterate over the records_items_data,
                    # and that we return a dict with the only one record item being the requested key_value.
                    return {key_value: self._process_cache_record_item(
                        data_validation=data_validation,
                        record_item_data=retrieved_records_items_data[0],
                        primary_key_value=key_value,
                        target_fields_containers=target_field_container
                    )}

    def _query_multiple_fields(
            self, middleware: Callable[[Dict[str, List[DatabasePathElement]], bool], List[Any]],
            key_value: str, getters: Dict[str, FieldGetter], index_name: Optional[str], data_validation: bool
    ):
        getters_database_paths, single_getters_target_fields_containers, grouped_getters_database_paths_elements = (
            _prepare_getters(fields_switch=self.fields_switch, getters=getters)
        )
        if len(grouped_getters_database_paths_elements) > 0:
            raise Exception(f"grouped_getters_database_paths_elements not yet supported")

        if index_name is None or index_name == self.primary_index_name:
            single_getters_database_paths_elements: Dict[str, List[DatabasePathElement]] = (
                {key: item[1] for key, item in single_getters_target_fields_containers.items()}
            )
            retrieved_records_items_data: Optional[List[Any]] = middleware(single_getters_database_paths_elements, True)
            if retrieved_records_items_data is not None and len(retrieved_records_items_data) > 0:
                # Since we query the primary_index, we know for a fact that we will never be returned more than
                # one record item. Hence why we do not have a loop that iterate over the records_items_data,
                # and that we return a dict with the only one record item being the requested key_value.
                return {key_value: self._process_cache_record_item(
                    data_validation=data_validation,
                    record_item_data=retrieved_records_items_data[0],
                    primary_key_value=key_value,
                    target_fields_containers=single_getters_target_fields_containers
                )}
            return None
        else:
            return self.inner_query_fields_secondary_index(
                middleware=middleware,
                data_validation=data_validation,
                target_field_container=single_getters_target_fields_containers,
                has_multiple_fields_path=True
            )

    @staticmethod
    def _unpack_getters_response_item(
            data_validation: bool, response_item: dict,
            single_getters_database_paths_elements: Dict[str, Tuple[BaseField, List[DatabasePathElement]]],
            grouped_getters_database_paths_elements: Dict[str, Dict[str, Tuple[BaseField, List[DatabasePathElement]]]],
    ):
        def item_mutator(item: Any):
            return item

        from StructNoSQL.tables.shared_table_behaviors import _base_unpack_getters_response_item_v2
        return _base_unpack_getters_response_item_v2(
            item_mutator=item_mutator, data_validation=data_validation, response_item=response_item,
            single_getters_database_paths_elements=single_getters_database_paths_elements,
            grouped_getters_database_paths_elements=grouped_getters_database_paths_elements
        )

    def _get_multiple_fields(
            self, middleware: Callable[[List[List[DatabasePathElement]]], Any],
            getters: Dict[str, FieldGetter], data_validation: bool
    ) -> Dict[str, Optional[Any]]:

        getters_database_paths, single_getters_database_paths_elements, grouped_getters_database_paths_elements = (
            _prepare_getters(fields_switch=self.fields_switch, getters=getters)
        )
        response_data: Optional[dict] = middleware(getters_database_paths)
        if response_data is None:
            return {getter_key: None for getter_key in getters.keys()}

        return self._unpack_getters_response_item(
            response_item=response_data,
            single_getters_database_paths_elements=single_getters_database_paths_elements,
            grouped_getters_database_paths_elements=grouped_getters_database_paths_elements,
            data_validation=data_validation
        )

    def _update_field(
            self, middleware: Callable[[List[DatabasePathElement], Any], bool],
            field_path: str, value_to_set: Any, query_kwargs: Optional[dict] = None
    ) -> bool:
        field_object, field_path_elements, validated_data, is_valid = process_validate_data_and_make_single_rendered_database_path(
            field_path=field_path, fields_switch=self.fields_switch, query_kwargs=query_kwargs, data_to_validate=value_to_set
        )
        return middleware(field_path_elements, validated_data) if is_valid is True else False

    def _update_field_return_old(
            self, middleware: Callable[[List[DatabasePathElement], Any], Tuple[bool, Any]],
            field_path: str, value_to_set: Any, query_kwargs: Optional[dict], data_validation: bool
    ) -> Tuple[bool, Optional[Any]]:
        field_object, field_path_elements, validated_update_data, update_data_is_valid = process_validate_data_and_make_single_rendered_database_path(
            field_path=field_path, fields_switch=self.fields_switch, query_kwargs=query_kwargs, data_to_validate=value_to_set
        )
        if update_data_is_valid is not True:
            return False, None

        update_success, response_attributes = middleware(field_path_elements, validated_update_data)

        from StructNoSQL.utils.data_processing import navigate_into_data_with_field_path_elements
        old_item_data: Optional[Any] = navigate_into_data_with_field_path_elements(
            data=response_attributes, field_path_elements=field_path_elements,
            num_keys_to_navigation_into=len(field_path_elements)
        ) if response_attributes is not None else None

        if data_validation is True:
            field_object.populate(value=old_item_data)
            validated_removed_data, removed_data_is_valid = field_object.validate_data()
        else:
            validated_removed_data = old_item_data

        return update_success, validated_removed_data

    def _update_multiple_fields(
            self, middleware: Callable[[List[FieldPathSetter]], bool],
            setters: List[FieldSetter or UnsafeFieldSetter]
    ) -> bool:
        dynamodb_setters: List[FieldPathSetter] = []
        for current_setter in setters:
            if isinstance(current_setter, FieldSetter):
                field_object, field_path_elements, validated_data, is_valid = process_validate_data_and_make_single_rendered_database_path(
                    field_path=current_setter.field_path, fields_switch=self.fields_switch,
                    query_kwargs=current_setter.query_kwargs, data_to_validate=current_setter.value_to_set
                )
                if is_valid is True:
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
        update_success: bool = middleware(dynamodb_setters)
        return update_success

    def _update_multiple_fields_return_old(
            self, middleware: Callable[[Dict[str, FieldPathSetter]], Tuple[bool, Dict[str, Optional[Any]]]],
            setters: Dict[str, FieldSetter], data_validation: bool
    ) -> Tuple[bool, Dict[str, Optional[Any]]]:

        setters_containers: Dict[str, Tuple[BaseField, List[DatabasePathElement]]] = {}
        dynamodb_setters: Dict[str, FieldPathSetter] = {}
        for setter_key, setter_item in setters.items():
            field_object, field_path_elements, validated_data, is_valid = process_validate_data_and_make_single_rendered_database_path(
                field_path=setter_item.field_path, fields_switch=self.fields_switch,
                query_kwargs=setter_item.query_kwargs, data_to_validate=setter_item.value_to_set
            )
            if is_valid is True:
                setters_containers[setter_key] = (field_object, field_path_elements)
                dynamodb_setters[setter_key] = FieldPathSetter(
                    field_path_elements=field_path_elements, value_to_set=validated_data
                )
        update_success, setters_response_attributes = middleware(dynamodb_setters)

        output_data: Dict[str, Optional[Any]] = {}
        for item_key, item_container in setters_containers.items():
            item_field_object, item_field_path_elements = item_container

            item_data: Optional[Any] = navigate_into_data_with_field_path_elements(
                data=setters_response_attributes, field_path_elements=item_field_path_elements,
                num_keys_to_navigation_into=len(item_field_path_elements)
            )
            if data_validation is True:
                item_field_object.populate(value=item_data)
                validated_data, valid = item_field_object.validate_data()
                output_data[item_key] = validated_data
            else:
                output_data[item_key] = item_data

        return update_success, output_data

    def _remove_field(
            self, middleware: Callable[[List[List[DatabasePathElement]]], Optional[dict]],
            field_path: str, query_kwargs: Optional[dict], data_validation: bool
    ) -> Optional[Any]:
        target_field_container, has_multiple_fields_path = process_and_make_single_rendered_database_path(
            field_path=field_path, fields_switch=self.fields_switch, query_kwargs=query_kwargs
        )

        if has_multiple_fields_path is not True:
            target_field_container: Tuple[BaseField, List[DatabasePathElement]]
            field_path_object, field_path_elements = target_field_container

            removed_item_attributes: Optional[dict] = middleware([field_path_elements])
            if removed_item_attributes is None:
                return None

            item_removed_data: Optional[Any] = navigate_into_data_with_field_path_elements(
                data=removed_item_attributes, field_path_elements=field_path_elements,
                num_keys_to_navigation_into=len(field_path_elements)
            )
            if data_validation is True:
                field_path_object.populate(value=item_removed_data)
                validated_data, is_valid = field_path_object.validate_data()
                return validated_data
            else:
                return item_removed_data
        else:
            target_field_container: Dict[str, Tuple[BaseField, List[DatabasePathElement]]]

            fields_paths_elements: List[List[DatabasePathElement]] = [item[1] for item in target_field_container.values()]
            removed_items_attributes: Optional[dict] = middleware(fields_paths_elements)
            # The attributes of all the removed items are packed inside the same dictionary,
            # because all the remove's are expected to be done by a single database operation.
            if removed_items_attributes is None:
                return None

            removed_items_values: Dict[str, Optional[Any]] = {}
            for item_key, item_container in target_field_container.items():
                item_field_object, item_field_path_elements = item_container

                item_removed_data: Optional[Any] = navigate_into_data_with_field_path_elements(
                    data=removed_items_attributes, field_path_elements=item_field_path_elements,
                    num_keys_to_navigation_into=len(item_field_path_elements)
                )
                if data_validation is True:
                    item_field_object.populate(value=item_removed_data)
                    validated_data, is_valid = item_field_object.validate_data()
                    removed_items_values[item_key] = validated_data
                else:
                    removed_items_values[item_key] = item_removed_data

            return removed_items_values

    def _delete_field(
            self, middleware: Callable[[List[List[DatabasePathElement]]], bool],
            field_path: str, query_kwargs: Optional[dict] = None
    ) -> bool:
        target_field_container, has_multiple_fields_path = process_and_make_single_rendered_database_path(
            field_path=field_path, fields_switch=self.fields_switch, query_kwargs=query_kwargs
        )
        if has_multiple_fields_path is not None:
            target_field_container: Tuple[BaseField, List[DatabasePathElement]]
            return middleware([target_field_container[1]])
        else:
            target_field_container: Dict[str, Tuple[BaseField, List[DatabasePathElement]]]
            targets_paths_elements: List[List[DatabasePathElement]] = [item[1] for item in target_field_container.values()]
            return middleware(targets_paths_elements)

    def _grouped_remove_multiple_fields(
            self, middleware: Callable[[List[List[DatabasePathElement]]], Any],
            removers: Dict[str, FieldRemover], data_validation: bool
    ) -> Optional[Dict[str, Any]]:
        if not len(removers) > 0:
            # If no remover has been specified, we do not run the database
            # operation, and since no value has been removed, we return None.
            return None

        removers_field_paths_elements: Dict[str, Tuple[BaseField, List[DatabasePathElement]]] = {}
        grouped_removers_field_paths_elements: Dict[str, Dict[str, Tuple[BaseField, List[DatabasePathElement]]]] = {}

        removers_database_paths: List[List[DatabasePathElement]] = []
        for remover_key, remover_item in removers.items():
            target_field_container, has_multiple_fields_path = process_and_make_single_rendered_database_path(
                field_path=remover_item.field_path, fields_switch=self.fields_switch,
                query_kwargs=remover_item.query_kwargs
            )
            if has_multiple_fields_path is not True:
                target_field_container: Tuple[BaseField, List[DatabasePathElement]]
                field_object, field_path_elements = target_field_container

                removers_field_paths_elements[remover_key] = target_field_container
                removers_database_paths.append(field_path_elements)
            else:
                target_field_container: Dict[str, Tuple[BaseField, List[DatabasePathElement]]]
                fields_paths_elements: List[List[DatabasePathElement]] = [item[1] for item in target_field_container.values()]

                grouped_removers_field_paths_elements[remover_key] = target_field_container
                removers_database_paths.extend(fields_paths_elements)

        response_attributes: Optional[Any] = middleware(removers_database_paths)
        if response_attributes is None:
            return None

        return self._unpack_getters_response_item(
            data_validation=data_validation, response_item=response_attributes,
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
            target_field_container, has_multiple_fields_path = process_and_make_single_rendered_database_path(
                field_path=current_remover.field_path, fields_switch=self.fields_switch,
                query_kwargs=current_remover.query_kwargs
            )
            if has_multiple_fields_path is not True:
                target_field_container: Tuple[BaseField, List[DatabasePathElement]]
                removers_database_paths.append(target_field_container[1])
            else:
                target_field_container: Dict[str, Tuple[BaseField, List[DatabasePathElement]]]
                fields_paths_elements: List[List[DatabasePathElement]] = [item[1] for item in target_field_container.values()]
                removers_database_paths.extend(fields_paths_elements)

        response: Optional[Any] = middleware(removers_database_paths)
        return response is not None
