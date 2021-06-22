from typing import Callable, List, Dict, Optional, Any, Tuple

from StructNoSQL.models import DatabasePathElement


def _has_primary_key_in_path_elements(primary_index_name: str, fields_path_elements: Dict[str, List[DatabasePathElement]]) -> Tuple[bool, Optional[str]]:
    for client_key, item_path_elements in fields_path_elements.items():
        if len(item_path_elements) > 0:
            first_path_element = item_path_elements[0]
            if first_path_element.element_key == primary_index_name:
                return True, client_key
    return False, None

def _inner_query_fields_secondary_index(
        middleware: Callable[[List[DatabasePathElement] or Dict[str, List[DatabasePathElement]], bool], Any],
        process_record_value: Callable[[Optional[Any], Any, List[DatabasePathElement]], Any],  # value: Optional[Any], primary_key_value: Any, field_path_elements: List[DatabasePathElement]
        process_record_item: Callable[[Optional[Any], Any, Dict[str, List[DatabasePathElement]]], Any],  # record_item_data: dict, primary_key_value: str, fields_path_elements: Dict[str, List[DatabasePathElement]]
        primary_index_name: str, get_primary_key_database_path: Callable[[], List[DatabasePathElement]],
        field_path_elements: List[DatabasePathElement] or Dict[str, List[DatabasePathElement]], has_multiple_fields_path: bool
) -> Optional[dict]:
    if has_multiple_fields_path is not True:
        field_path_elements: List[DatabasePathElement]
        if field_path_elements[0].element_key == primary_index_name:
            # If the specified field is the primary key
            retrieved_records_items_data: Optional[List[Any]] = middleware(field_path_elements, False)
            if retrieved_records_items_data is None:
                return None

            records_output: dict = {}
            for record_primary_key_value in retrieved_records_items_data:
                records_output[record_primary_key_value] = process_record_value(
                    record_primary_key_value, record_primary_key_value, field_path_elements
                )
            return records_output
        else:
            # If a single field is requested, but the primary_key is not being requested and needs to be artificially added
            super_target_path_elements = {'__VALUE__': field_path_elements, '__PRIMARY_KEY__': get_primary_key_database_path()}
            retrieved_records_items_data: Optional[List[Any]] = middleware(super_target_path_elements, True)
            if retrieved_records_items_data is None:
                return None

            records_output: dict = {}
            for record_item_data in retrieved_records_items_data:
                record_client_requested_value_data: Optional[Any] = record_item_data.get('__VALUE__', None)
                record_primary_key_value: Optional[Any] = record_item_data.get('__PRIMARY_KEY__', None)
                records_output[record_primary_key_value] = process_record_value(
                    record_client_requested_value_data, record_primary_key_value, field_path_elements
                )
            return records_output
    else:
        # If multiple fields are requested
        field_path_elements: Dict[str, List[DatabasePathElement]]
        primary_key_is_being_requested_by_client, primary_key_client_retrieval_key = (
            _has_primary_key_in_path_elements(primary_index_name=primary_index_name, fields_path_elements=field_path_elements)
        )
        if primary_key_is_being_requested_by_client is True:
            retrieved_records_items_data: Optional[List[Any]] = middleware(field_path_elements, True)
            if retrieved_records_items_data is None:
                return None

            records_output: dict = {}
            for record_item_data in retrieved_records_items_data:
                record_primary_key_value: Optional[Any] = record_item_data.get(primary_key_client_retrieval_key, None)
                if record_primary_key_value is not None:
                    records_output[record_primary_key_value] = process_record_item(
                        record_item_data, record_primary_key_value, field_path_elements
                    )
            return records_output
        else:
            super_fields_path_elements = {**field_path_elements, '__PRIMARY_KEY__': get_primary_key_database_path()}
            retrieved_records_items_data: Optional[List[dict]] = middleware(super_fields_path_elements, True)
            if retrieved_records_items_data is None:
                return None

            records_output: dict = {}
            for record_item_data in retrieved_records_items_data:
                record_primary_key_value: Optional[Any] = record_item_data.pop('__PRIMARY_KEY__', None)
                if record_primary_key_value is not None:
                    records_output[record_primary_key_value] = process_record_item(
                        record_item_data, record_primary_key_value, field_path_elements
                    )
            return records_output
