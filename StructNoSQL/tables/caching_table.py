from typing import Optional, List, Dict, Any, Set, Tuple
from copy import copy

from StructNoSQL.dynamodb.dynamodb_core import DynamoDbCoreAdapter, PrimaryIndex, GlobalSecondaryIndex, DynamoDBMapObjectSetter, Response
from StructNoSQL.dynamodb.models import DatabasePathElement, FieldGetter, FieldSetter, UnsafeFieldSetter, FieldRemover
from StructNoSQL.fields import BaseField, MapField, MapItem, TableDataModel
from StructNoSQL.practical_logger import message_with_vars
from StructNoSQL.tables import BaseTable
from StructNoSQL.utils.process_render_fields_paths import process_and_get_fields_paths_objects_from_fields_paths, \
    process_and_make_single_rendered_database_path, process_validate_data_and_make_single_rendered_database_path, \
    process_and_get_field_path_object_from_field_path, make_rendered_database_path
from StructNoSQL.utils.types import PRIMITIVE_TYPES
from StructNoSQL.utils.decimals import float_to_decimal_serializer


def join_field_path_elements(field_path_elements) -> str:
    return '.'.join((item.element_key for item in field_path_elements))


class CachingTable(BaseTable):
    def __init__(
        self, table_name: str, region_name: str,
        data_model, primary_index: PrimaryIndex,
        billing_mode: str = DynamoDbCoreAdapter.PAY_PER_REQUEST,
        global_secondary_indexes: List[GlobalSecondaryIndex] = None,
        auto_create_table: bool = True
    ):
        super().__init__(
            table_name=table_name, region_name=region_name, data_model=data_model,
            primary_index=primary_index, global_secondary_indexes=global_secondary_indexes,
            billing_mode=billing_mode, auto_create_table=auto_create_table
        )
        self._cached_data = dict()

    def _index_cached_data(self, index_name: str) -> dict:
        index_name = index_name or self.primary_index_name
        if index_name not in self._cached_data:
            self._cached_data[index_name] = dict()
        return self._cached_data[index_name]

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
        index_cached_data = self._index_cached_data(index_name)
        field_path_elements, has_multiple_fields_path = process_and_make_single_rendered_database_path(
            field_path=field_path, fields_switch=self.fields_switch, query_kwargs=query_kwargs
        )
        if has_multiple_fields_path is not True:
            field_path_elements: List[DatabasePathElement]

            joined_field_path = join_field_path_elements(field_path_elements)
            if joined_field_path in index_cached_data:
                return index_cached_data[joined_field_path]

            response_data = self.dynamodb_client.get_value_in_path_target(
                index_name=index_name or self.primary_index_name,
                key_value=key_value, field_path_elements=field_path_elements
            )
            self._index_cached_data(index_name)[joined_field_path] = response_data
            return response_data
        else:
            field_path_elements: Dict[str, List[DatabasePathElement]]
            response_items_values: Dict[str, Any] = dict()

            keys_fields_already_cached_to_pop: List[str] = list()
            for item_key, item_field_path_elements in field_path_elements.items():
                joined_field_path = join_field_path_elements(item_field_path_elements)
                if joined_field_path in index_cached_data:
                    # We do not use a .get('key', None), because None can be a valid value for a field
                    response_items_values[item_key] = index_cached_data[joined_field_path]
                    keys_fields_already_cached_to_pop.append(item_key)

            for key_to_pop in keys_fields_already_cached_to_pop:
                field_path_elements.pop(key_to_pop)

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
                    response_items_values[key] = item_value
                return response_items_values
            return None

    def get_multiple_fields(self, key_value: str, getters: Dict[str, FieldGetter], index_name: Optional[str] = None) -> Optional[dict]:
        getters_database_paths = self._getters_to_database_paths(getters=getters)
        response_data = self.dynamodb_client.get_values_in_multiple_path_target(
            index_name=index_name or self.primary_index_name,
            key_value=key_value, fields_paths_elements=getters_database_paths,
        )
        return response_data

    def update_field(self, key_value: str, field_path: str, value_to_set: Any,
                     query_kwargs: Optional[dict] = None, index_name: Optional[str] = None) -> bool:
        validated_data, valid, field_path_elements = process_validate_data_and_make_single_rendered_database_path(
            field_path=field_path, fields_switch=self.fields_switch, query_kwargs=query_kwargs, data_to_validate=value_to_set
        )
        if valid is True and field_path_elements is not None:
            response = self.dynamodb_client.set_update_data_element_to_map(
                index_name=index_name or self.primary_index_name,
                key_value=key_value, value=validated_data,
                field_path_elements=field_path_elements
            )
            return True if response is not None else False
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
                safe_field_path_object, has_multiple_fields_path = process_and_get_field_path_object_from_field_path(
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
                ))

        response = self.dynamodb_client.set_update_multiple_data_elements_to_map(
            index_name=index_name or self.primary_index_name,
            key_value=key_value, setters=dynamodb_setters
        )
        return True if response is not None else False

    def _base_removal(
            self, retrieve_removed_elements: bool, key_value: str, field_path: str,
            query_kwargs: Optional[dict] = None, index_name: Optional[str] = None
    ) -> Tuple[Optional[Response], List[List[DatabasePathElement]]]:

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
        response, all_fields_items_path_elements = self._base_removal(
            retrieve_removed_elements=True, key_value=key_value,
            field_path=field_path, query_kwargs=query_kwargs, index_name=index_name
        )
        if response is not None and response.attributes is not None:
            if not len(all_fields_items_path_elements) > 0:
                return None
            elif len(all_fields_items_path_elements) == 1:
                field_path_elements = all_fields_items_path_elements[0]
                removed_item_data = self.dynamodb_client.navigate_into_data_with_field_path_elements(
                    data=response.attributes, field_path_elements=field_path_elements,
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
                        data=response.attributes, field_path_elements=field_path_elements,
                        num_keys_to_navigation_into=len(field_path_elements)
                    )
                return removed_items_values
        return None

    def delete_field(self, key_value: str, field_path: str, query_kwargs: Optional[dict] = None, index_name: Optional[str] = None) -> bool:
        response, _ = self._base_removal(
            retrieve_removed_elements=False, key_value=key_value,
            field_path=field_path, query_kwargs=query_kwargs, index_name=index_name
        )
        return True if response is not None else False

    def _base_multi_removal(
            self, retrieve_removed_elements: bool, key_value: str,
            removers: List[FieldRemover], index_name: Optional[str] = None
    ) -> Optional[Response]:

        removers_database_paths: List[List[DatabasePathElement]] = list()
        for current_remover in removers:
            field_path_elements, has_multiple_fields_path = process_and_make_single_rendered_database_path(
                field_path=current_remover.field_path, fields_switch=self.fields_switch, query_kwargs=current_remover.query_kwargs
            )
            if has_multiple_fields_path is not True:
                field_path_elements: List[DatabasePathElement]
                removers_database_paths.append(field_path_elements)
            else:
                field_path_elements: Dict[str, List[DatabasePathElement]]
                for field_paths_elements_item in field_path_elements.values():
                    removers_database_paths.append(field_paths_elements_item)

        return self.dynamodb_client.remove_data_elements_from_map(
            index_name=index_name or self.primary_index_name,
            key_value=key_value, targets_path_elements=removers_database_paths,
            retrieve_removed_elements=retrieve_removed_elements
        )

    def remove_multiple_fields(self, key_value: str, removers: List[FieldRemover], index_name: Optional[str] = None) -> Optional[Any]:
        if len(removers) > 0:
            response: Optional[Response] = self._base_multi_removal(
                retrieve_removed_elements=True, key_value=key_value,
                removers=removers, index_name=index_name
            )
            return response.items if response is not None and response.items is not None else None
        else:
            # If no remover has been specified, we do not run the database
            # operation, and since no value has been removed, we return None.
            return None

    def delete_multiple_fields(self, key_value: str, removers: List[FieldRemover], index_name: Optional[str] = None) -> bool:
        if len(removers) > 0:
            response: Optional[Response] = self._base_multi_removal(
                retrieve_removed_elements=False, key_value=key_value,
                removers=removers, index_name=index_name
            )
            return True if response is not None else False
        else:
            # If no remover has been specified, we do not run the database operation, yet we still
            # return True, since technically, what needed to be performed (nothing) was performed.
            return True