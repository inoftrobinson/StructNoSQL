import string
from typing import Callable, List, Dict, Optional, Any, Tuple, Iterable, Union

from StructNoSQL import BaseField
from StructNoSQL.middlewares.dynamodb.backend.models import QueryMetadata
from StructNoSQL.models import DatabasePathElement, FieldGetter
from StructNoSQL.practical_logger import message_with_vars
from StructNoSQL.tables.base_table import FieldsSwitch
from StructNoSQL.utils.data_processing import navigate_into_data_with_field_path_elements
from StructNoSQL.utils.process_render_fields_paths import process_and_make_single_rendered_database_path


def _base_unpack_getters_response_item(
        item_mutator: Callable[[Any], Any], response_item: dict,
        single_getters_database_paths_elements: Dict[str, List[DatabasePathElement]],
        grouped_getters_database_paths_elements: Dict[str, Dict[str, List[DatabasePathElement]]]
) -> Dict[str, Any]:
    output_data: Dict[str, Any] = {}
    for item_key, item_field_path_elements in single_getters_database_paths_elements.items():
        retrieved_item_data = navigate_into_data_with_field_path_elements(
            data=response_item, field_path_elements=item_field_path_elements,
            num_keys_to_navigation_into=len(item_field_path_elements)
        )
        output_data[item_key] = item_mutator(retrieved_item_data)

    for container_key, container_items_field_path_elements in grouped_getters_database_paths_elements.items():
        container_data: Dict[str, Any] = {}
        for child_item_key, child_item_field_path_elements in container_items_field_path_elements.items():
            container_data[child_item_key] = navigate_into_data_with_field_path_elements(
                data=response_item, field_path_elements=child_item_field_path_elements,
                num_keys_to_navigation_into=len(child_item_field_path_elements)
            )
        output_data[container_key] = item_mutator(container_data)
    return output_data

def _base_unpack_getters_response_item_v2(
        item_mutator: Callable[[Any], Any], data_validation: bool, response_item: dict,
        single_getters_database_paths_elements: Dict[str, Tuple[BaseField, List[DatabasePathElement]]],
        grouped_getters_database_paths_elements: Dict[str, Dict[str, Tuple[BaseField, List[DatabasePathElement]]]]
) -> Dict[str, Any]:
    output_data: Dict[str, Any] = {}
    for item_key, item_container in single_getters_database_paths_elements.items():
        item_field_object, item_field_path_elements = item_container
        item_data: Optional[Any] = navigate_into_data_with_field_path_elements(
            data=response_item, field_path_elements=item_field_path_elements,
            num_keys_to_navigation_into=len(item_field_path_elements)
        )
        if data_validation is True:
            item_field_object.populate(value=item_data)
            validated_data, is_valid = item_field_object.validate_data()
        else:
            validated_data = item_data

        output_data[item_key] = item_mutator(validated_data)

    for fields_container_key, fields_container_item in grouped_getters_database_paths_elements.items():
        current_container_fields_data: Dict[str, Any] = {}
        for child_field_key, child_field_container in fields_container_item.items():
            child_field_object, child_field_path_elements = child_field_container
            item_data: Optional[Any] = navigate_into_data_with_field_path_elements(
                data=response_item, field_path_elements=child_field_path_elements,
                num_keys_to_navigation_into=len(child_field_path_elements)
            )
            if data_validation is True:
                child_field_object.populate(value=item_data)
                validated_data, is_valid = child_field_object.validate_data()
            else:
                validated_data = item_data

            current_container_fields_data[child_field_key] = item_mutator(validated_data)

        output_data[fields_container_key] = current_container_fields_data
    return output_data


def _has_primary_key_in_path_elements(primary_index_name: str, fields_path_elements: Dict[str, List[DatabasePathElement]]) -> Tuple[bool, Optional[str]]:
    for client_key, item_path_elements in fields_path_elements.items():
        if len(item_path_elements) > 0:
            first_path_element = item_path_elements[0]
            if first_path_element.element_key == primary_index_name:
                return True, client_key
    return False, None

def _inner_query_fields_secondary_index(
        middleware: Callable[[Union[List[DatabasePathElement], Dict[str, List[DatabasePathElement]]], bool], Tuple[Optional[List[Any]], QueryMetadata]],
        process_record_value: Callable[[bool, Optional[Any], Any, Tuple[BaseField, List[DatabasePathElement]]], Any],
        process_record_item: Callable[[bool, dict, Any, Dict[str, Tuple[BaseField, List[DatabasePathElement]]]], Any],
        data_validation: bool, primary_index_name: str, get_primary_key_database_path: Callable[[], List[DatabasePathElement]],
        target_field_container: Union[Tuple[BaseField, List[DatabasePathElement]], Dict[str, Tuple[BaseField, List[DatabasePathElement]]]], is_multi_selector: bool
) -> Tuple[Optional[dict], QueryMetadata]:
    """
    This function will force the retrieving of the primary key of the records returned by a secondary index query operations,
    and retrieve a dict with the primary key of each record as key.

    There is four different behaviors :
    1 - A single field is requested, and it is the primary key field. No additional field will be requested, and the retrieved
        values for each record from the middleware function will be use both as the key and value of the output dict.
    2 - A single field is requested, but it is not the primary key index. We will force the requesting of the primary key's of
        the records, by constructing a dict of fields to request with both the primary index key, and the field the user is actually
        requesting. We then force the middleware to request multiple fields, and when we receiving the response containing the records
        items, we will use the primary key as the key's for each record, and the client requested values as value.
    3 - Multiple fields are requested, and the primary key field is one of the requested field. We will get the client key of the getter that
        is tasked to retrieved the primary key field, and once we receive the records items, we can use the value found at the primary key
        getter, as the key's for the output dict.
    4 - Multiple fields are requested, and the primary key is not requested. Similarly to the second case, we will force the retrieval of the
        primary key field. But this time, we add the primary key field getter to the existing field to retrieve, by using the '__PRIMARY_KEY__'
        key_name. If the key_name is already being used by the client, we will randomly add characters to the key_name, until we find a free to
        use key_name. Then, the primary key's will be popped from the response items of each records and be used as the keys of the output dict.

    :param middleware:
    :param process_record_value: Callable[[data_validation: bool, value: Optional[Any], primary_key_value: Any, target_field_container: Tuple[BaseField, List[DatabasePathElement]]], Any]
    :param process_record_item: Callable[[data_validation: bool, record_item_data: dict, primary_key_value: Any, target_fields_containers: Dict[str, Tuple[BaseField, List[DatabasePathElement]]]], Any]
    :param primary_index_name: str
    :param get_primary_key_database_path: Callable[[], List[DatabasePathElement]]
    :param field_path_elements: Union[Tuple[BaseField, List[DatabasePathElement]], Dict[str, Tuple[BaseField, List[DatabasePathElement]]]]
    :param is_multi_selector: bool
    :return: Optional[dict]
    """
    if is_multi_selector is not True:
        target_field_container: Tuple[BaseField, List[DatabasePathElement]]
        field_object, field_path_elements = target_field_container

        if field_path_elements[0].element_key == primary_index_name:
            # If the specified field is the primary key
            retrieved_records_items_data, query_metadata = middleware(field_path_elements, False)
            if retrieved_records_items_data is None:
                return None, query_metadata

            records_output: dict = {}
            for record_primary_key_value in retrieved_records_items_data:
                records_output[record_primary_key_value] = process_record_value(
                    data_validation, record_primary_key_value, record_primary_key_value, target_field_container
                )
            return records_output, query_metadata
        else:
            # If a single field is requested, but the primary_key is not being requested and needs to be artificially added
            super_target_path_elements = {'__VALUE__': field_path_elements, '__PRIMARY_KEY__': get_primary_key_database_path()}
            retrieved_records_items_data, query_metadata = middleware(super_target_path_elements, True)
            if retrieved_records_items_data is None:
                return None, query_metadata

            records_output: dict = {}
            for record_item_data in retrieved_records_items_data:
                record_client_requested_value_data: Optional[Any] = record_item_data.get('__VALUE__', None)
                record_primary_key_value: Optional[Any] = record_item_data.get('__PRIMARY_KEY__', None)
                records_output[record_primary_key_value] = process_record_value(
                    data_validation, record_client_requested_value_data, record_primary_key_value, target_field_container
                )
            return records_output, query_metadata
    else:
        # If multiple fields are requested
        target_field_container: Dict[str, Tuple[BaseField, List[DatabasePathElement]]]
        fields_paths_elements: Dict[str, List[DatabasePathElement]] = {key: item[1] for key, item in target_field_container.items()}

        primary_key_is_being_requested_by_client, primary_key_client_retrieval_key = (
            _has_primary_key_in_path_elements(primary_index_name=primary_index_name, fields_path_elements=fields_paths_elements)
        )
        if primary_key_is_being_requested_by_client is True:
            retrieved_records_items_data, query_metadata = middleware(fields_paths_elements, True)
            if retrieved_records_items_data is None:
                return None, query_metadata

            records_output: dict = {}
            for record_item_data in retrieved_records_items_data:
                record_primary_key_value: Optional[Any] = record_item_data.get(primary_key_client_retrieval_key, None)
                if record_primary_key_value is not None:
                    records_output[record_primary_key_value] = process_record_item(
                        data_validation, record_item_data, record_primary_key_value, target_field_container
                    )
            return records_output, query_metadata
        else:
            free_to_use_getter_key_name: str = '__PRIMARY_KEY__'
            while True:
                # If the key_name is already being used by the client, we will randomly
                # add characters to the key_name, until we find a free to use key_name.
                if free_to_use_getter_key_name not in fields_paths_elements:
                    break
                import random
                free_to_use_getter_key_name += random.choice(string.ascii_letters)

            super_fields_path_elements = {**fields_paths_elements, free_to_use_getter_key_name: get_primary_key_database_path()}
            retrieved_records_items_data, query_metadata = middleware(super_fields_path_elements, True)
            if retrieved_records_items_data is None:
                return None, query_metadata

            records_output: dict = {}
            for record_item_data in retrieved_records_items_data:
                record_primary_key_value: Optional[Any] = record_item_data.pop(free_to_use_getter_key_name, None)
                if record_primary_key_value is not None:
                    records_output[record_primary_key_value] = process_record_item(
                        data_validation, record_item_data, record_primary_key_value, target_field_container
                    )
            return records_output, query_metadata


def _prepare_getters(fields_switch: FieldsSwitch, getters: Dict[str, FieldGetter]) -> Tuple[
    List[List[DatabasePathElement]],
    Dict[str, Tuple[BaseField, List[DatabasePathElement]]],
    Dict[str, Dict[str, Tuple[BaseField, List[DatabasePathElement]]]]
]:
    getters_database_paths: List[List[DatabasePathElement]] = []
    single_getters_database_paths_elements: Dict[str, Tuple[BaseField, List[DatabasePathElement]]] = {}
    grouped_getters_database_paths_elements: Dict[str, Dict[str, Tuple[BaseField, List[DatabasePathElement]]]] = {}

    for getter_key, getter_item in getters.items():
        target_field_container, is_multi_selector = process_and_make_single_rendered_database_path(
            field_path=getter_item.field_path, fields_switch=fields_switch, query_kwargs=getter_item.query_kwargs
        )
        if is_multi_selector is not True:
            target_field_container: Tuple[BaseField, List[DatabasePathElement]]
            single_getters_database_paths_elements[getter_key] = target_field_container
            getters_database_paths.append(target_field_container[1])
        else:
            target_field_container: Dict[str, Tuple[BaseField, List[DatabasePathElement]]]

            field_path_object: Dict[str, BaseField]
            getter_field_path_elements: Dict[str, List[DatabasePathElement]]

            grouped_getters_database_paths_elements[getter_key] = target_field_container
            for item_target_field_container in target_field_container.values():
                getters_database_paths.append(item_target_field_container[1])

    return getters_database_paths, single_getters_database_paths_elements, grouped_getters_database_paths_elements


def _model_contain_all_index_keys(model: Any, indexes_keys: Iterable[str]) -> bool:
    for index_key in indexes_keys:
        index_matching_field: Optional[Any] = getattr(model, index_key, None)
        if index_matching_field is None:
            print(message_with_vars(
                message="An index key selector was not found in the table model. Operation not executed.",
                vars_dict={'index_key': index_key, 'index_matching_field': index_matching_field, 'table.model': model}
            ))
            return False
    return True
