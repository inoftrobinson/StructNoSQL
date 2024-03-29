from typing import Optional, List, Dict, Any, Tuple, Callable, Union, Type

from StructNoSQL import PrimaryIndex, BaseField, TableDataModel
from StructNoSQL.models import DatabasePathElement, FieldGetter, FieldSetter, UnsafeFieldSetter, FieldRemover, \
    FieldPathSetter, QueryMetadata
from StructNoSQL.base_tables.base_table import BaseTable
from StructNoSQL.base_tables.shared_table_behaviors import _prepare_getters, _model_contain_all_index_keys, \
    unpack_validate_retrieved_field_if_need_to, unpack_validate_multiple_retrieved_fields_if_need_to
from StructNoSQL.utils.data_processing import navigate_into_data_with_field_path_elements
from StructNoSQL.utils.process_render_fields_paths import process_and_make_single_rendered_database_path,\
    process_transforme_validate_data_from_write_and_make_single_rendered_database_path


class BaseBasicTable(BaseTable):
    def __init__(
            self, data_model: Type[TableDataModel], primary_index: PrimaryIndex,
            auto_leading_key: Optional[str] = None
    ):
        super().__init__(data_model=data_model, primary_index=primary_index, auto_leading_key=auto_leading_key)

    def _put_record(self, middleware: Callable[[dict], bool], record_dict_data: dict, data_validation: bool) -> bool:
        validated_data, is_valid = self.model_virtual_map_field.transform_validate_from_write(
            value=record_dict_data, data_validation=data_validation
        )
        if is_valid is not True:
            return False
        return middleware(validated_data)

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

        validated_removed_record_data, is_valid = self.model_virtual_map_field.transform_validate_from_read(
            value=removed_record_data, data_validation=data_validation
        )
        return validated_removed_record_data

    def _get_field(
            self, middleware: Callable[[Union[List[DatabasePathElement], Dict[str, List[DatabasePathElement]]], bool], Optional[Any]],
            field_path: str, query_kwargs: Optional[dict], data_validation: bool
    ) -> Optional[Any]:
        target_field_container, is_multi_selector = process_and_make_single_rendered_database_path(
            field_path=field_path, fields_switch=self.fields_switch, query_kwargs=query_kwargs
        )
        if is_multi_selector is not True:
            target_field_container: Tuple[BaseField, List[DatabasePathElement]]
            field_object, field_path_elements = target_field_container

            retrieved_item_data: Optional[Any] = middleware(field_path_elements, False)
            transformed_validated_data, is_valid = field_object.transform_validate_from_read(value=retrieved_item_data, data_validation=data_validation)
            return transformed_validated_data
        else:
            target_field_container: Dict[str, Tuple[BaseField, List[DatabasePathElement]]]

            fields_paths_elements: Dict[str, List[DatabasePathElement]] = {key: item[1] for key, item in target_field_container.items()}
            retrieved_items_data: Dict[str, Optional[Any]] = middleware(fields_paths_elements, True)

            output_data: Dict[str, Optional[Any]] = {}
            for item_key, item_container in target_field_container.items():
                field_object, field_path_elements = item_container

                matching_item_data: Optional[Any] = retrieved_items_data.get(item_key, None)
                transformed_validated_item_data, is_valid = field_object.transform_validate_from_read(
                    value=matching_item_data, data_validation=data_validation
                )
                output_data[item_key] = transformed_validated_item_data

            return output_data

    def inner_query_fields_secondary_index(
            self, middleware: Callable[[List[List[DatabasePathElement]]], Tuple[Optional[List[Any]], QueryMetadata]],
            fields_database_paths: List[List[DatabasePathElement]],
    ) -> Tuple[Optional[dict], QueryMetadata]:
        from StructNoSQL.base_tables.shared_table_behaviors import _inner_query_fields_secondary_index
        return _inner_query_fields_secondary_index(
            primary_index_name=self.primary_index_name,
            get_primary_key_database_path=self._get_primary_key_database_path,
            middleware=middleware,
            fields_paths_elements=fields_database_paths,
        )

    def unpack_validate_getters_record_attributes_if_need_to(
            self, data_validation: bool, record_attributes: dict,
            single_getters_target_fields_containers: Dict[str, Tuple[BaseField, List[DatabasePathElement]]],
            grouped_getters_target_fields_containers: Dict[str, Dict[str, Tuple[BaseField, List[DatabasePathElement]]]],
    ):
        def item_mutator(item_value: Any, item_field_path_elements: List[DatabasePathElement]) -> Any:
            return item_value

        from StructNoSQL.base_tables.shared_table_behaviors import _unpack_validate_getters_record_attributes_if_need_to
        return _unpack_validate_getters_record_attributes_if_need_to(
            item_mutator=item_mutator, data_validation=data_validation, record_attributes=record_attributes,
            single_getters_target_fields_containers=single_getters_target_fields_containers,
            grouped_getters_target_fields_containers=grouped_getters_target_fields_containers
        )

    def _query_field(
            self, middleware: Callable[[List[List[DatabasePathElement]]], Tuple[Optional[List[Any]], QueryMetadata]],
            key_value: str, field_path: str, query_kwargs: Optional[dict], index_name: Optional[str], data_validation: bool
    ) -> Tuple[Optional[dict], QueryMetadata]:

        target_field_container, is_multi_selector = process_and_make_single_rendered_database_path(
            field_path=field_path, fields_switch=self.fields_switch, query_kwargs=query_kwargs
        )
        if index_name is not None and index_name != self.primary_index_name:
            output_records_values: dict = {}
            if is_multi_selector is not True:
                target_field_container: Tuple[BaseField, List[DatabasePathElement]]
                fields_database_paths: List[List[DatabasePathElement]] = [target_field_container[1]]

                records_attributes, query_metadata = self.inner_query_fields_secondary_index(
                    middleware=middleware, fields_database_paths=fields_database_paths,
                )
                for record_key, record_item_attributes in records_attributes.items():
                    output_records_values[record_key] = unpack_validate_retrieved_field_if_need_to(
                        record_attributes=record_item_attributes, 
                        target_field_container=target_field_container,
                        data_validation=data_validation
                    )
            else:
                target_field_container: Dict[str, Tuple[BaseField, List[DatabasePathElement]]]
                fields_database_paths: List[List[DatabasePathElement]] = [item[1] for item in target_field_container.values()]

                records_attributes, query_metadata = self.inner_query_fields_secondary_index(
                    middleware=middleware, fields_database_paths=fields_database_paths,
                )
                for record_key, record_item_attributes in records_attributes.items():
                    output_records_values[record_key] = unpack_validate_multiple_retrieved_fields_if_need_to(
                        record_attributes=record_item_attributes,
                        target_fields_containers=target_field_container,
                        data_validation=data_validation
                    )
            return output_records_values, query_metadata
        else:
            # If requested index is primary index
            if is_multi_selector is not True:
                target_field_container: Tuple[BaseField, List[DatabasePathElement]]
                field_object, field_path_elements = target_field_container

                retrieved_records_items_data, query_metadata = middleware([field_path_elements])
                if retrieved_records_items_data is not None and len(retrieved_records_items_data) > 0:
                    # Since we query the primary_index, we know for a fact that we will never be returned more than
                    # one record item. Hence why we do not have a loop that iterate over the records_items_data,
                    # and that we return a dict with the only one record item being the requested key_value.
                    return {key_value: unpack_validate_retrieved_field_if_need_to(
                        data_validation=data_validation,
                        record_attributes=retrieved_records_items_data[0],
                        target_field_container=target_field_container
                    )}, query_metadata
                return None, query_metadata
            else:
                target_field_container: Dict[str, Tuple[BaseField, List[DatabasePathElement]]]

                fields_paths_elements: List[List[DatabasePathElement]] = [item[1] for item in target_field_container.values()]
                retrieved_records_items_data, query_metadata = middleware(fields_paths_elements)
                if retrieved_records_items_data is not None and len(retrieved_records_items_data) > 0:
                    # Since we query the primary_index, we know for a fact that we will never be returned more than
                    # one record item. Hence why we do not have a loop that iterate over the records_items_data,
                    # and that we return a dict with the only one record item being the requested key_value.
                    return {key_value: unpack_validate_multiple_retrieved_fields_if_need_to(
                        data_validation=data_validation,
                        record_attributes=retrieved_records_items_data[0],
                        target_fields_containers=target_field_container
                    )}, query_metadata
                return None, query_metadata

    def _query_multiple_fields(
            self, middleware: Callable[[List[List[DatabasePathElement]]], Tuple[Optional[List[Any]], QueryMetadata]],
            key_value: str, getters: Dict[str, FieldGetter], index_name: Optional[str], data_validation: bool
    ) -> Tuple[Optional[dict], QueryMetadata]:
        getters_database_paths, single_getters_target_fields_containers, grouped_getters_target_fields_containers = (
            _prepare_getters(fields_switch=self.fields_switch, getters=getters)
        )

        if index_name is None or index_name == self.primary_index_name:
            retrieved_records_items_data, query_metadata = middleware(getters_database_paths)
            if retrieved_records_items_data is not None and len(retrieved_records_items_data) > 0:
                # Since we query the primary_index, we know for a fact that we will never be returned more than
                # one record item. Hence why we do not have a loop that iterate over the records_items_data,
                # and that we return a dict with the only one record item being the requested key_value.
                record_item_value: Dict[str, Any] = self.unpack_validate_getters_record_attributes_if_need_to(
                    data_validation=data_validation, record_attributes=retrieved_records_items_data[0],
                    single_getters_target_fields_containers=single_getters_target_fields_containers,
                    grouped_getters_target_fields_containers=grouped_getters_target_fields_containers
                )
                return {key_value: record_item_value}, query_metadata
            return None, query_metadata
        else:
            records_attributes, query_metadata = self.inner_query_fields_secondary_index(
                middleware=middleware, fields_database_paths=getters_database_paths,
            )
            output_records_values: dict = {
                record_key: self.unpack_validate_getters_record_attributes_if_need_to(
                    data_validation=data_validation, record_attributes=record_item_attributes,
                    single_getters_target_fields_containers=single_getters_target_fields_containers,
                    grouped_getters_target_fields_containers=grouped_getters_target_fields_containers
                ) for record_key, record_item_attributes in records_attributes.items()
            }
            return output_records_values, query_metadata

    def _get_multiple_fields(
            self, middleware: Callable[[List[List[DatabasePathElement]]], Any],
            getters: Dict[str, FieldGetter], data_validation: bool
    ) -> Dict[str, Optional[Any]]:

        getters_database_paths, single_getters_target_fields_containers, grouped_getters_target_fields_containers = (
            _prepare_getters(fields_switch=self.fields_switch, getters=getters)
        )
        record_attributes: Optional[dict] = middleware(getters_database_paths)
        if record_attributes is None:
            return {getter_key: None for getter_key in getters.keys()}

        return self.unpack_validate_getters_record_attributes_if_need_to(
            data_validation=data_validation, record_attributes=record_attributes,
            single_getters_target_fields_containers=single_getters_target_fields_containers,
            grouped_getters_target_fields_containers=grouped_getters_target_fields_containers,
        )

    def _update_field(
            self, middleware: Callable[[List[DatabasePathElement], Any], bool],
            field_path: str, value_to_set: Any, query_kwargs: Optional[dict] = None
    ) -> bool:
        field_object, field_path_elements, validated_data, is_valid = process_transforme_validate_data_from_write_and_make_single_rendered_database_path(
            field_path=field_path, fields_switch=self.fields_switch, query_kwargs=query_kwargs, data_to_validate=value_to_set
        )
        return middleware(field_path_elements, validated_data) if is_valid is True else False

    def _update_field_return_old(
            self, middleware: Callable[[List[DatabasePathElement], Any], Tuple[bool, Any]],
            field_path: str, value_to_set: Any, query_kwargs: Optional[dict], data_validation: bool
    ) -> Tuple[bool, Optional[Any]]:
        field_object, field_path_elements, validated_update_data, update_data_is_valid = process_transforme_validate_data_from_write_and_make_single_rendered_database_path(
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

        validated_removed_data, removed_data_is_valid = field_object.transform_validate_from_read(
            value=old_item_data, data_validation=data_validation
        )
        return update_success, validated_removed_data

    def _update_multiple_fields(
            self, middleware: Callable[[List[FieldPathSetter]], bool],
            setters: List[FieldSetter or UnsafeFieldSetter]
    ) -> bool:
        dynamodb_setters: List[FieldPathSetter] = []
        for current_setter in setters:
            if isinstance(current_setter, FieldSetter):
                field_object, field_path_elements, validated_data, is_valid = process_transforme_validate_data_from_write_and_make_single_rendered_database_path(
                    field_path=current_setter.field_path, fields_switch=self.fields_switch,
                    query_kwargs=current_setter.query_kwargs, data_to_validate=current_setter.value_to_set
                )
                if is_valid is True:
                    dynamodb_setters.append(FieldPathSetter(
                        field_path_elements=field_path_elements, value_to_set=validated_data
                    ))
            elif isinstance(current_setter, UnsafeFieldSetter):
                raise Exception(f"UnsafeFieldSetter not supported in basic_table")

                """safe_field_path_object, is_multi_selector = process_and_get_field_path_object_from_field_path(
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
            field_object, field_path_elements, validated_data, is_valid = process_transforme_validate_data_from_write_and_make_single_rendered_database_path(
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
            transformed_validated_item_data, is_valid = item_field_object.transform_validate_from_read(
                value=item_data, data_validation=data_validation
            )
            output_data[item_key] = transformed_validated_item_data

        return update_success, output_data

    def _remove_field(
            self, middleware: Callable[[List[List[DatabasePathElement]]], Optional[dict]],
            field_path: str, query_kwargs: Optional[dict], data_validation: bool
    ) -> Optional[Any]:
        target_field_container, is_multi_selector = process_and_make_single_rendered_database_path(
            field_path=field_path, fields_switch=self.fields_switch, query_kwargs=query_kwargs
        )

        if is_multi_selector is not True:
            target_field_container: Tuple[BaseField, List[DatabasePathElement]]
            field_path_object, field_path_elements = target_field_container

            removed_item_attributes: Optional[dict] = middleware([field_path_elements])
            if removed_item_attributes is None:
                return None

            item_removed_data: Optional[Any] = navigate_into_data_with_field_path_elements(
                data=removed_item_attributes, field_path_elements=field_path_elements,
                num_keys_to_navigation_into=len(field_path_elements)
            )
            transformed_validated_data, is_valid = field_path_object.transform_validate_from_read(
                value=item_removed_data, data_validation=data_validation
            )
            return transformed_validated_data
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
                transformed_validated_item_data, is_valid = item_field_object.transform_validate_from_read(
                    value=item_removed_data, data_validation=data_validation
                )
                removed_items_values[item_key] = transformed_validated_item_data

            return removed_items_values

    def _delete_field(
            self, middleware: Callable[[List[List[DatabasePathElement]]], bool],
            field_path: str, query_kwargs: Optional[dict] = None
    ) -> bool:
        target_field_container, is_multi_selector = process_and_make_single_rendered_database_path(
            field_path=field_path, fields_switch=self.fields_switch, query_kwargs=query_kwargs
        )
        if is_multi_selector is not None:
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
            target_field_container, is_multi_selector = process_and_make_single_rendered_database_path(
                field_path=remover_item.field_path, fields_switch=self.fields_switch,
                query_kwargs=remover_item.query_kwargs
            )
            if is_multi_selector is not True:
                target_field_container: Tuple[BaseField, List[DatabasePathElement]]
                field_object, field_path_elements = target_field_container

                removers_field_paths_elements[remover_key] = target_field_container
                removers_database_paths.append(field_path_elements)
            else:
                target_field_container: Dict[str, Tuple[BaseField, List[DatabasePathElement]]]
                fields_paths_elements: List[List[DatabasePathElement]] = [item[1] for item in target_field_container.values()]

                grouped_removers_field_paths_elements[remover_key] = target_field_container
                removers_database_paths.extend(fields_paths_elements)

        record_attributes: Optional[dict] = middleware(removers_database_paths)
        if record_attributes is None:
            return None

        return self.unpack_validate_getters_record_attributes_if_need_to(
            data_validation=data_validation, record_attributes=record_attributes,
            single_getters_target_fields_containers=removers_field_paths_elements,
            grouped_getters_target_fields_containers=grouped_removers_field_paths_elements
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
            target_field_container, is_multi_selector = process_and_make_single_rendered_database_path(
                field_path=current_remover.field_path, fields_switch=self.fields_switch,
                query_kwargs=current_remover.query_kwargs
            )
            if is_multi_selector is not True:
                target_field_container: Tuple[BaseField, List[DatabasePathElement]]
                removers_database_paths.append(target_field_container[1])
            else:
                target_field_container: Dict[str, Tuple[BaseField, List[DatabasePathElement]]]
                fields_paths_elements: List[List[DatabasePathElement]] = [item[1] for item in target_field_container.values()]
                removers_database_paths.extend(fields_paths_elements)

        response: Optional[Any] = middleware(removers_database_paths)
        return response is not None
