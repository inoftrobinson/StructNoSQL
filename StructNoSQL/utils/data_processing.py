from typing import List, Any, Optional

from StructNoSQL.models import DatabasePathElement


def remove_null_elements_from_dict(dict_instance: dict) -> dict:
    output_clean_dict = {}
    for key, value in dict_instance.items():
        if isinstance(value, dict):
            nested = remove_null_elements_from_dict(value)
            if len(nested.keys()) > 0:
                output_clean_dict[key] = nested
        elif value is not None:
            output_clean_dict[key] = value
    return output_clean_dict


def navigate_into_data_with_field_path_elements(
        data: Any, field_path_elements: List[DatabasePathElement], num_keys_to_navigation_into: int
) -> Optional[Any]:

    if data is not None:
        num_field_path_elements = len(field_path_elements)
        for i, path_element in enumerate(field_path_elements):
            if i + 1 > num_keys_to_navigation_into:
                break
            elif i > 0 and field_path_elements[i - 1].default_type == list:
                data = data[0]
            else:
                if not isinstance(data, dict):
                    break

                if path_element.default_type == set and data == {} and num_field_path_elements > i + 1:
                    data = field_path_elements[i + 1].element_key
                    break

                data = data.get(path_element.element_key, None)
                if data is None:
                    # If the response_item is None, it means that one key has not been found,
                    # so we need to break the loop in order to try to call get on a None object,
                    # and then we will return the response_item, so we will return None.
                    break
    return data
