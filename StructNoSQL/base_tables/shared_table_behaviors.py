from typing import Callable, List, Dict, Optional, Any, Tuple, Iterable

from StructNoSQL import BaseField
from StructNoSQL.models import DatabasePathElement, FieldGetter, QueryMetadata
from StructNoSQL.practical_logger import message_with_vars
from StructNoSQL.base_tables.base_table import FieldsSwitch
from StructNoSQL.utils.data_processing import navigate_into_data_with_field_path_elements
from StructNoSQL.utils.process_render_fields_paths import process_and_make_single_rendered_database_path


def unpack_validate_retrieved_field(
        record_attributes: dict, target_field_container: Tuple[BaseField, List[DatabasePathElement]],
) -> Optional[Any]:
    item_field_object, item_field_path_elements = target_field_container
    item_data: Optional[Any] = navigate_into_data_with_field_path_elements(
        data=record_attributes, field_path_elements=item_field_path_elements,
        num_keys_to_navigation_into=len(item_field_path_elements)
    )
    validated_data, is_valid = item_field_object.transform_validate_from_read(value=item_data, data_validation=True)
    return validated_data

def unpack_validate_retrieved_field_if_need_to(
        target_field_container: Tuple[BaseField, List[DatabasePathElement]],
        data_validation: bool, record_attributes: dict,
        item_mutator: Optional[Callable[[Any, List[DatabasePathElement]], Any]] = lambda item_value, _: item_value
) -> Optional[Any]:
    field_object, field_path_elements = target_field_container
    if data_validation is True:
        return item_mutator(unpack_validate_retrieved_field(
            record_attributes=record_attributes, 
            target_field_container=target_field_container
        ), field_path_elements)
    else:
        return item_mutator(navigate_into_data_with_field_path_elements(
            data=record_attributes, field_path_elements=field_path_elements,
            num_keys_to_navigation_into=len(field_path_elements)
        ), field_path_elements)

def unpack_validate_multiple_retrieved_fields_if_need_to(
        target_fields_containers: Dict[str, Tuple[BaseField, List[DatabasePathElement]]],
        data_validation: bool, record_attributes: dict,
        item_mutator: Optional[Callable[[Any, List[DatabasePathElement]], Any]] = lambda item_value, _: item_value,
        base_output_values: Optional[Dict[str, Any]] = None
):
    output_values: Dict[str, Any] = base_output_values or {}
    if data_validation is True:
        for item_key, item_container in target_fields_containers.items():
            item_field_object, item_field_path_elements = item_container

            output_values[item_key] = item_mutator(unpack_validate_retrieved_field(
                record_attributes=record_attributes, target_field_container=item_container
            ), item_field_path_elements)
    else:
        for item_key, item_container in target_fields_containers.items():
            item_field_object, item_field_path_elements = item_container

            output_values[item_key] = item_mutator(navigate_into_data_with_field_path_elements(
                data=record_attributes, field_path_elements=item_field_path_elements,
                num_keys_to_navigation_into=len(item_field_path_elements)
            ), item_field_path_elements)
    return output_values


# todo: remove the _base_unpack_getters_response_item function
def _base_unpack_getters_response_item(
        item_mutator: Callable[[Any], Any], response_item: dict,
        single_getters_target_fields_containers: Dict[str, List[DatabasePathElement]],
        grouped_getters_target_fields_containers: Dict[str, Dict[str, List[DatabasePathElement]]]
) -> Dict[str, Any]:
    output_data: Dict[str, Any] = {}
    for item_key, item_field_path_elements in single_getters_target_fields_containers.items():
        retrieved_item_data = navigate_into_data_with_field_path_elements(
            data=response_item, field_path_elements=item_field_path_elements,
            num_keys_to_navigation_into=len(item_field_path_elements)
        )
        output_data[item_key] = item_mutator(retrieved_item_data)

    for container_key, container_items_field_path_elements in grouped_getters_target_fields_containers.items():
        container_data: Dict[str, Any] = {}
        for child_item_key, child_item_field_path_elements in container_items_field_path_elements.items():
            container_data[child_item_key] = navigate_into_data_with_field_path_elements(
                data=response_item, field_path_elements=child_item_field_path_elements,
                num_keys_to_navigation_into=len(child_item_field_path_elements)
            )
        output_data[container_key] = item_mutator(container_data)
    return output_data

# todo: make a function that validate, and one that does not, to have only one if check on validation
def _unpack_validate_getters_record_attributes_if_need_to(
        single_getters_target_fields_containers: Dict[str, Tuple[BaseField, List[DatabasePathElement]]],
        grouped_getters_target_fields_containers: Dict[str, Dict[str, Tuple[BaseField, List[DatabasePathElement]]]],
        item_mutator: Callable[[Any, List[DatabasePathElement]], Any], data_validation: bool,
        record_attributes: dict, base_output_values: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    output_values: Dict[str, Any] = base_output_values or {}

    for item_key, item_container in single_getters_target_fields_containers.items():
        item_field_object, item_field_path_elements = item_container
        item_data: Optional[Any] = navigate_into_data_with_field_path_elements(
            data=record_attributes, field_path_elements=item_field_path_elements,
            num_keys_to_navigation_into=len(item_field_path_elements)
        )
        validated_data, is_valid = item_field_object.transform_validate_from_read(value=item_data, data_validation=data_validation)
        output_values[item_key] = item_mutator(validated_data, item_field_path_elements)

    for fields_container_key, fields_container_item in grouped_getters_target_fields_containers.items():
        current_container_fields_data: Dict[str, Any] = output_values.get(fields_container_key, {})

        for child_field_key, child_field_container in fields_container_item.items():
            child_field_object, child_field_path_elements = child_field_container
            item_data: Optional[Any] = navigate_into_data_with_field_path_elements(
                data=record_attributes, field_path_elements=child_field_path_elements,
                num_keys_to_navigation_into=len(child_field_path_elements)
            )
            validated_data, is_valid = child_field_object.transform_validate_from_read(value=item_data, data_validation=data_validation)
            current_container_fields_data[child_field_key] = item_mutator(validated_data, child_field_path_elements)

        output_values[fields_container_key] = current_container_fields_data
    return output_values


def _has_primary_key_in_path_elements(primary_index_name: str, fields_path_elements: Dict[str, List[DatabasePathElement]]) -> Tuple[bool, Optional[str]]:
    for client_key, item_path_elements in fields_path_elements.items():
        if len(item_path_elements) > 0:
            first_path_element = item_path_elements[0]
            if first_path_element.element_key == primary_index_name:
                return True, client_key
    return False, None

def _has_primary_key_in_path_elements_v2(primary_index_name: str, fields_path_elements: List[List[DatabasePathElement]]) -> bool:
    for item_path_elements in fields_path_elements:
        if len(item_path_elements) > 0:
            first_path_element = item_path_elements[0]
            if first_path_element.element_key == primary_index_name:
                return True
    return False

# todo: deprecate ?
def _inner_query_fields_secondary_index(
        middleware: Callable[[List[List[DatabasePathElement]]], Tuple[Optional[List[Any]], QueryMetadata]],
        primary_index_name: str, get_primary_key_database_path: Callable[[], List[DatabasePathElement]],
        fields_paths_elements: List[List[DatabasePathElement]]
) -> Tuple[Optional[dict], QueryMetadata]:
    """
    This function will force the retrieving of the primary key of the records returned by a
    secondary index query operations and retrieve a dict with the primary key of each record as key.
    """

    primary_key_is_being_requested_by_client: bool = (
        _has_primary_key_in_path_elements_v2(primary_index_name=primary_index_name, fields_path_elements=fields_paths_elements)
    )
    super_fields_path_elements: List[List[DatabasePathElement]] = (
        fields_paths_elements if primary_key_is_being_requested_by_client is True else
        [*fields_paths_elements, get_primary_key_database_path()]
    )
    retrieved_records_items_data, query_metadata = middleware(super_fields_path_elements)
    if retrieved_records_items_data is None:
        return None, query_metadata

    records_attributes: dict = {}
    for record_item_data in retrieved_records_items_data:
        # todo: stop using the primary_index_name and start using the field_name of the primary index (different)
        record_primary_key_value: Optional[Any] = record_item_data.get(primary_index_name, None)
        records_attributes[record_primary_key_value] = record_item_data
    return records_attributes, query_metadata


def _prepare_getters(fields_switch: FieldsSwitch, getters: Dict[str, FieldGetter]) -> Tuple[
    List[List[DatabasePathElement]],
    Dict[str, Tuple[BaseField, List[DatabasePathElement]]],
    Dict[str, Dict[str, Tuple[BaseField, List[DatabasePathElement]]]]
]:
    getters_database_paths: List[List[DatabasePathElement]] = []
    single_getters_target_fields_containers: Dict[str, Tuple[BaseField, List[DatabasePathElement]]] = {}
    grouped_getters_target_fields_containers: Dict[str, Dict[str, Tuple[BaseField, List[DatabasePathElement]]]] = {}

    for getter_key, getter_item in getters.items():
        target_field_container, is_multi_selector = process_and_make_single_rendered_database_path(
            field_path=getter_item.field_path, fields_switch=fields_switch, query_kwargs=getter_item.query_kwargs
        )
        if is_multi_selector is not True:
            target_field_container: Tuple[BaseField, List[DatabasePathElement]]
            single_getters_target_fields_containers[getter_key] = target_field_container
            getters_database_paths.append(target_field_container[1])
        else:
            target_field_container: Dict[str, Tuple[BaseField, List[DatabasePathElement]]]

            field_path_object: Dict[str, BaseField]
            getter_field_path_elements: Dict[str, List[DatabasePathElement]]

            grouped_getters_target_fields_containers[getter_key] = target_field_container
            for item_target_field_container in target_field_container.values():
                getters_database_paths.append(item_target_field_container[1])

    return getters_database_paths, single_getters_target_fields_containers, grouped_getters_target_fields_containers


def _model_contain_all_index_keys(model: Any, indexes_keys: Iterable[str]) -> bool:
    # todo: make sure this function works now that model is a type and not an instance
    for index_key in indexes_keys:
        index_matching_field: Optional[Any] = getattr(model, index_key, None)
        if index_matching_field is None:
            print(message_with_vars(
                message="An index key selector was not found in the table model. Operation not executed.",
                vars_dict={'index_key': index_key, 'index_matching_field': index_matching_field, 'table.model': model}
            ))
            return False
    return True
