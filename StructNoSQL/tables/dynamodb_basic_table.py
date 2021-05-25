from concurrent.futures.thread import ThreadPoolExecutor
from typing import Optional, List, Dict, Any, Tuple

from StructNoSQL import DynamoDBTableConnectors
from StructNoSQL.dynamodb.dynamodb_core import DynamoDbCoreAdapter, PrimaryIndex, GlobalSecondaryIndex, DynamoDBMapObjectSetter
from StructNoSQL.dynamodb.models import DatabasePathElement, FieldGetter, FieldSetter, UnsafeFieldSetter, FieldRemover
from StructNoSQL.practical_logger import message_with_vars
from StructNoSQL.tables.base_table import BaseTable
from StructNoSQL.tables.dynamodb_low_level_table_operations import DynamoDBLowLevelTableOperations
from StructNoSQL.utils.process_render_fields_paths import process_and_make_single_rendered_database_path, \
    process_validate_data_and_make_single_rendered_database_path


class DynamoDBBasicTable(BaseTable, DynamoDBLowLevelTableOperations):
    def __init__(
            self, table_name: str, region_name: str,
            data_model, primary_index: PrimaryIndex,
            billing_mode: str = DynamoDbCoreAdapter.PAY_PER_REQUEST,
            global_secondary_indexes: List[GlobalSecondaryIndex] = None,
            auto_create_table: bool = True
    ):
        super().__init__(data_model=data_model)
        super().__setup_connectors__(
            table_name=table_name, region_name=region_name,
            primary_index=primary_index, global_secondary_indexes=global_secondary_indexes,
            billing_mode=billing_mode, auto_create_table=auto_create_table
        )

    def put_record(self, record_dict_data: dict) -> bool:
        self.model_virtual_map_field.populate(value=record_dict_data)
        validated_data, is_valid = self.model_virtual_map_field.validate_data()
        if is_valid is True:
            return self.dynamodb_client.put_record(item_dict=validated_data)
        else:
            return False

    def delete_record(self, indexes_keys_selectors: dict) -> bool:
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
        field_path_elements, has_multiple_fields_path = process_and_make_single_rendered_database_path(
            field_path=field_path, fields_switch=self.fields_switch, query_kwargs=query_kwargs
        )
        return super().get_field(
            key_value=key_value, index_name=index_name,
            has_multiple_fields_path=has_multiple_fields_path,
            field_path_elements=field_path_elements
        )

    def get_multiple_fields(self, key_value: str, getters: Dict[str, FieldGetter], index_name: Optional[str] = None) -> Optional[dict]:
        single_getters_database_paths_elements: Dict[str, List[DatabasePathElement]] = dict()
        grouped_getters_database_paths_elements: Dict[str, Dict[str, List[DatabasePathElement]]] = dict()

        getters_database_paths: List[List[DatabasePathElement]] = list()
        for getter_key, getter_item in getters.items():
            field_path_elements, has_multiple_fields_path = process_and_make_single_rendered_database_path(
                field_path=getter_item.field_path, fields_switch=self.fields_switch, query_kwargs=getter_item.query_kwargs
            )
            if has_multiple_fields_path is not True:
                getter_field_path_elements: List[DatabasePathElement]
                single_getters_database_paths_elements[getter_key] = field_path_elements
                getters_database_paths.append(field_path_elements)
            else:
                getter_field_path_elements: Dict[str, List[DatabasePathElement]]
                grouped_getters_database_paths_elements[getter_key] = field_path_elements
                getters_database_paths.extend(field_path_elements.values())

        response_data = self.dynamodb_client.get_or_query_single_item(
            index_name=index_name or self.primary_index_name,
            key_value=key_value, fields_paths_elements=getters_database_paths,
        )
        if response_data is None:
            return None
        else:
            output_data: Dict[str, Any] = dict()
            for item_key, item_field_path_elements in single_getters_database_paths_elements.items():
                retrieved_item_data = self.dynamodb_client.navigate_into_data_with_field_path_elements(
                    data=response_data, field_path_elements=item_field_path_elements,
                    num_keys_to_navigation_into=len(item_field_path_elements)
                )
                output_data[item_key] = retrieved_item_data

            for container_key, container_items_field_path_elements in grouped_getters_database_paths_elements.items():
                container_data: Dict[str, Any] = dict()
                for child_item_key, child_item_field_path_elements in container_items_field_path_elements.items():
                    container_data[child_item_key] = self.dynamodb_client.navigate_into_data_with_field_path_elements(
                        data=response_data, field_path_elements=child_item_field_path_elements,
                        num_keys_to_navigation_into=len(child_item_field_path_elements)
                    )
                output_data[container_key] = container_data
            return output_data

    # todo: deprecated
    """
    def query(self, key_value: str, fields_paths: List[str], query_kwargs: Optional[dict] = None, limit: Optional[int] = None,
              filter_expression: Optional[Any] = None,  index_name: Optional[str] = None, **additional_kwargs) -> Optional[List[Any]]:
        fields_paths_objects = process_and_get_fields_paths_objects_from_fields_paths(
            fields_paths=fields_paths, fields_switch=self.fields_switch
        )
        query_field_path_elements: List[List[DatabasePathElement]] = list()
        for field_path in fields_paths:
            field_path_elements, has_multiple_fields_path = process_and_make_single_rendered_database_path(
                field_path=field_path, fields_switch=self.fields_switch, query_kwargs=query_kwargs
            )
            query_field_path_elements.append(field_path_elements)

        response = self.dynamodb_client.query_by_key(
            index_name=index_name or self.primary_index_name,
            index_name=key_name, key_value=key_value,
            fields_paths_elements=query_field_path_elements,
            query_limit=limit, filter_expression=filter_expression, 
            **additional_kwargs
        )
        if response is not None:
            for current_item in response.items:
                if isinstance(current_item, dict):
                    for current_item_key, current_item_value in current_item.items():
                        matching_field_path_object = fields_paths_objects.get(current_item_key, None)
                        if matching_field_path_object is not None:
                            if matching_field_path_object.database_path is not None:
                                matching_field_path_object.populate(value=current_item_value)
                                current_item[current_item_key], valid = matching_field_path_object.validate_data()
                                # todo: remove this non centralized response validation system
            return response.items
        else:
            return None
    """

    def update_field(self, key_value: str, field_path: str, value_to_set: Any, query_kwargs: Optional[dict] = None, index_name: Optional[str] = None) -> bool:
        validated_data, valid, field_path_elements = process_validate_data_and_make_single_rendered_database_path(
            field_path=field_path, fields_switch=self.fields_switch, query_kwargs=query_kwargs, data_to_validate=value_to_set
        )
        if valid is True and field_path_elements is not None:
            return super().update_field(
                key_value=key_value, index_name=index_name,
                validated_data=validated_data,
                field_path_elements=field_path_elements
            )
        return False

    def update_multiple_fields(self, key_value: str, setters: List[FieldSetter or UnsafeFieldSetter], index_name: Optional[str] = None) -> bool:
        dynamodb_setters: List[DynamoDBMapObjectSetter] = list()
        for current_setter in setters:
            if isinstance(current_setter, FieldSetter):
                validated_data, valid, field_path_elements = process_validate_data_and_make_single_rendered_database_path(
                    field_path=current_setter.field_path, fields_switch=self.fields_switch,
                    query_kwargs=current_setter.query_kwargs, data_to_validate=current_setter.value_to_set
                )
                if valid is True:
                    dynamodb_setters.append(DynamoDBMapObjectSetter(
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
                dynamodb_setters.append(DynamoDBMapObjectSetter(
                    field_path_elements=rendered_field_path_elements,
                    value_to_set=processed_value_to_set
                ))"""

        response = self.dynamodb_client.set_update_multiple_data_elements_to_map(
            index_name=index_name or self.primary_index_name,
            key_value=key_value, setters=dynamodb_setters
        )
        return True if response is not None else False

    def _base_removal(
            self, retrieve_removed_elements: bool, key_value: str, field_path: str,
            query_kwargs: Optional[dict] = None, index_name: Optional[str] = None
    ) -> Tuple[Optional[Dict[str, Any]], List[List[DatabasePathElement]]]:

        field_path_elements, has_multiple_fields_path = process_and_make_single_rendered_database_path(
            field_path=field_path, fields_switch=self.fields_switch, query_kwargs=query_kwargs
        )
        target_path_elements = [field_path_elements] if has_multiple_fields_path is not True else list(field_path_elements.values())
        # The remove_data_elements_from_map function expect a List[List[DatabasePathElement]]. If we have a single field_path, we wrap the field_path_elements
        # inside a list. And if we have multiple fields_paths (which will be structured inside a dict), we turn the convert the values of the dict to a list.

        return self.dynamodb_client.remove_data_elements_from_map(
            index_name=index_name or self.primary_index_name,
            key_value=key_value, targets_path_elements=target_path_elements,
            retrieve_removed_elements=retrieve_removed_elements
        ), target_path_elements

    def remove_field(self, key_value: str, field_path: str, query_kwargs: Optional[dict] = None, index_name: Optional[str] = None) -> Optional[Any]:
        response_attributes, all_fields_items_path_elements = self._base_removal(
            retrieve_removed_elements=True, key_value=key_value,
            field_path=field_path, query_kwargs=query_kwargs, index_name=index_name
        )
        if response_attributes is not None:
            if not len(all_fields_items_path_elements) > 0:
                return None
            elif len(all_fields_items_path_elements) == 1:
                field_path_elements = all_fields_items_path_elements[0]
                removed_item_data = self.dynamodb_client.navigate_into_data_with_field_path_elements(
                    data=response_attributes, field_path_elements=field_path_elements,
                    num_keys_to_navigation_into=len(field_path_elements)
                )
                return removed_item_data
            else:
                removed_items_values: Dict[str, Any] = dict()
                for field_path_elements in all_fields_items_path_elements:
                    # Even the remove_field function can potentially remove multiple
                    # field_path_elements if the field_path expression is selecting multiple fields.
                    last_path_element = field_path_elements[len(field_path_elements) - 1]
                    removed_items_values[last_path_element.element_key] = self.dynamodb_client.navigate_into_data_with_field_path_elements(
                        data=response_attributes, field_path_elements=field_path_elements,
                        num_keys_to_navigation_into=len(field_path_elements)
                    )
                return removed_items_values
        return None

    """def remove_multiple_fields(self, key_value: str, removers: Dict[str, FieldRemover], index_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
        return {key: self.remove_field(
            key_value=key_value, index_name=index_name,
            field_path=item.field_path, query_kwargs=item.query_kwargs
        ) for key, item in removers.items()}"""

    def remove_multiple_fields(self, key_value: str, removers: Dict[str, FieldRemover], index_name: Optional[str] = None) -> Dict[str, Any]:
        if not len(removers) > 0:
            return {}

        def _task_executor(remover_item: FieldRemover):
            return self.remove_field(
                key_value=key_value, index_name=index_name,
                field_path=remover_item.field_path,
                query_kwargs=remover_item.query_kwargs
            )
        with ThreadPoolExecutor(max_workers=len(removers)) as executor:
            return {key: executor.submit(_task_executor, item).result() for key, item in removers.items()}

    def delete_field(self, key_value: str, field_path: str, query_kwargs: Optional[dict] = None, index_name: Optional[str] = None) -> bool:
        response_attributes, _ = self._base_removal(
            retrieve_removed_elements=False, key_value=key_value,
            field_path=field_path, query_kwargs=query_kwargs, index_name=index_name
        )
        return True if response_attributes is not None else False

    """def delete_multiple_fields(self, key_value: str, removers: Dict[str, FieldRemover], index_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
        return {key: self.delete_field(
            key_value=key_value, index_name=index_name,
            field_path=item.field_path, query_kwargs=item.query_kwargs
        ) for key, item in removers.items()}"""

    def delete_multiple_fields(self, key_value: str, removers: Dict[str, FieldRemover], index_name: Optional[str] = None) -> Dict[str, bool]:
        if not len(removers) > 0:
            return {}

        def _task_executor(remover_item: FieldRemover):
            return self.delete_field(
                key_value=key_value, index_name=index_name,
                field_path=remover_item.field_path,
                query_kwargs=remover_item.query_kwargs
            )
        with ThreadPoolExecutor(max_workers=len(removers)) as executor:
            return {key: executor.submit(_task_executor, item).result() for key, item in removers.items()}

    def grouped_remove_multiple_fields(self, key_value: str, removers: Dict[str, FieldRemover], index_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
        if not len(removers) > 0:
            # If no remover has been specified, we do not run the database
            # operation, and since no value has been removed, we return None.
            return None
        else:
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
                else:
                    field_path_elements: Dict[str, List[DatabasePathElement]]
                    grouped_removers_field_paths_elements[remover_key] = field_path_elements
                    removers_database_paths.extend(field_path_elements.values())

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
        if len(removers) > 0:
            removers_database_paths: List[List[DatabasePathElement]] = list()
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

            response = self.dynamodb_client.remove_data_elements_from_map(
                index_name=index_name or self.primary_index_name,
                key_value=key_value, targets_path_elements=removers_database_paths,
                retrieve_removed_elements=False
            )
            return True if response is not None else False
        else:
            # If no remover has been specified, we do not run the database operation, yet we still
            # return True, since technically, what needed to be performed (nothing) was performed.
            return True
