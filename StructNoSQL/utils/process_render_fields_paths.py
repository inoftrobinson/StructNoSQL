import re
from typing import List, Dict, Any, Optional, Tuple
from StructNoSQL.dynamodb.models import DatabasePathElement
from StructNoSQL.exceptions import FieldTargetNotFoundException
from StructNoSQL.fields import BaseItem
from StructNoSQL.practical_logger import message_with_vars

MULTI_ATTRIBUTES_SELECTOR_REGEX_EXPRESSION = r'(\()(.*)(\))'


def _get_field_object_from_field_path(field_path_key: str, fields_switch: dict) -> BaseItem:
    current_field_object: Optional[BaseItem] = fields_switch.get(field_path_key, None)
    if current_field_object is not None:
        return current_field_object
    else:
        raise FieldTargetNotFoundException(message_with_vars(
            message=f"A field target to get was not found.",
            vars_dict={"field_path_key": field_path_key, "fields_switch": fields_switch}
        ))

def process_and_get_field_path_object_from_field_path(field_path_key: str, fields_switch: dict) -> (BaseItem or List[BaseItem], bool):
    matches: Optional[List[tuple]] = re.findall(pattern=MULTI_ATTRIBUTES_SELECTOR_REGEX_EXPRESSION, string=field_path_key)
    if matches is not None and len(matches) > 0:
        for match in matches:
            selected_string = ''.join([selector for selector in match])
            attributes_selectors_string: str = match[1]
            attributes_selector_list = attributes_selectors_string.replace(' ', '').split(',')

            attributes_fields_objets: List[BaseItem] = list()
            for attribute_selector in attributes_selector_list:
                current_attribute_field_path = field_path_key.replace(selected_string, attribute_selector)
                """if len(current_attribute_field_path) > 0 and current_attribute_field_path[0] == ".":
                    # If the start of the field path is a multi selector, the replace will unfortunately add an
                    # invalid point, which we will remove if we see that the first char of the field path is a point.
                    current_attribute_field_path = current_attribute_field_path[1:]"""

                attributes_fields_objets.append(_get_field_object_from_field_path(
                    field_path_key=current_attribute_field_path, fields_switch=fields_switch
                ))

            num_attributes_fields = len(attributes_fields_objets)
            if not num_attributes_fields > 0:
                raise Exception(message_with_vars(
                    message="Cannot use an attribute selector [ ] without specifying any attribute inside it.",
                    vars_dict={'field_path_key': field_path_key, 'attributes_selectors_string': attributes_selectors_string}
                ))
            elif num_attributes_fields == 1:
                return attributes_fields_objets[0], False
            else:
                return attributes_fields_objets, True

    return _get_field_object_from_field_path(field_path_key=field_path_key, fields_switch=fields_switch), False

def process_and_get_fields_paths_objects_from_fields_paths(fields_paths: List[str], fields_switch: dict) -> Dict[str, BaseItem]:
    fields_objects_to_get = dict()
    for field_key in fields_paths:
        field_path_object, has_multiple_fields_path = process_and_get_field_path_object_from_field_path(
            field_path_key=field_key, fields_switch=fields_switch
        )
        # todo: add support for multiple fields path's
        fields_objects_to_get[field_key] = field_path_object
    return fields_objects_to_get


def make_rendered_database_path(database_path_elements: List[DatabasePathElement], query_kwargs: dict) -> List[DatabasePathElement]:
    output_database_path_elements: List[DatabasePathElement] = list()
    for path_element in database_path_elements:
        if "$key$:" not in path_element.element_key:
            # If the path_element do not contains a key that need to be modified, we can use the current
            # instance of the path element, since it will not be modified, and so will not cause issue
            # when other invocations of queries and operations will use the same path element instance.
            output_database_path_elements.append(path_element)
        else:
            variable_name = path_element.element_key.replace("$key$:", "")
            if query_kwargs is not None:
                matching_kwarg = query_kwargs.get(variable_name, None)
                if matching_kwarg is not None:
                    # If the key of the path_element needs to be modified, we do not modify the existing path element,
                    # but we create a new instance of path element. Since the database_path_elements variable is retrieved
                    # using the static _database_path variable, the path elements in database_path_elements needs to
                    # remained unmodified, so that other invocations of queries and operations will be able to have
                    # cleans element keys that will properly be filled with the query_kwargs specified in the request.
                    output_database_path_elements.append(DatabasePathElement(
                        element_key=matching_kwarg,
                        default_type=path_element.default_type,
                        custom_default_value=path_element.custom_default_value
                    ))
                else:
                    raise Exception(message_with_vars(
                        message="A variable was required but not found in the query_kwargs dict passed to the make_rendered_database_path function.",
                        vars_dict={"keyVariableName": variable_name, "matchingKwarg": matching_kwarg,
                                   "queryKwargs": query_kwargs, "databasePathElements": database_path_elements}
                    ))
            else:
                raise Exception(message_with_vars(
                    message="A variable was required but not query_kwargs have been passed to the make_rendered_database_path function.",
                    vars_dict={"keyVariableName": variable_name, "queryKwargs": query_kwargs, "databasePathElements": database_path_elements}
                ))
    return output_database_path_elements

def process_and_make_single_rendered_database_path(
        field_path: str, fields_switch: dict, query_kwargs: dict
) -> (List[DatabasePathElement] or Dict[str, List[DatabasePathElement]], bool):

    field_path_object, has_multiple_fields_path = process_and_get_field_path_object_from_field_path(
        field_path_key=field_path, fields_switch=fields_switch
    )
    if has_multiple_fields_path is not True:
        rendered_database_path_elements = make_rendered_database_path(
            database_path_elements=field_path_object.database_path, query_kwargs=query_kwargs
        )
        return rendered_database_path_elements, False
    else:
        fields_rendered_database_path_elements: Dict[str, List[DatabasePathElement]] = dict()
        for single_field_path_object in field_path_object:
            fields_rendered_database_path_elements[single_field_path_object.field_name] = make_rendered_database_path(
                database_path_elements=single_field_path_object.database_path, query_kwargs=query_kwargs
            )
        return fields_rendered_database_path_elements, True

def process_validate_data_and_make_single_rendered_database_path(
        field_path: str, fields_switch: dict, query_kwargs: dict, data_to_validate: Any
) -> Tuple[Optional[Any], bool, Optional[List[DatabasePathElement]]]:

    field_path_object, has_multiple_fields_path = process_and_get_field_path_object_from_field_path(
        field_path_key=field_path, fields_switch=fields_switch
    )
    # todo: add support for multiple fields path
    field_path_object.populate(value=data_to_validate)
    validated_data, valid = field_path_object.validate_data()
    if valid is True:
        rendered_database_path_elements = make_rendered_database_path(
            database_path_elements=field_path_object.database_path, query_kwargs=query_kwargs
        )
        return validated_data, valid, rendered_database_path_elements
    else:
        return None, False, None



def make_rendered_fields_paths(fields_paths: List[str], query_kwargs: dict) -> List[str]:
    for i, field_key in enumerate(fields_paths):
        start_variable_first_char_index = field_key.find("{{")
        if not start_variable_first_char_index == -1:
            end_variable_first_char_index = field_key.find("}}")
            if not end_variable_first_char_index == -1:
                start_variable_last_char_index = start_variable_first_char_index + 2
                end_variable_last_char_index = end_variable_first_char_index + 2

                variable_key_name = field_key[start_variable_last_char_index:end_variable_first_char_index]
                variable_matching_kwarg = query_kwargs.get(variable_key_name, None)
                if variable_matching_kwarg is None:
                    raise Exception(message_with_vars(
                        message="A key was required in a field to get, but no matching query kwarg was found.",
                        vars_dict={"fieldsPaths": fields_paths, "fieldKey": field_key, "variableKeyName": variable_key_name,
                                   "queryKwargs": query_kwargs, "variableMatchingKwarg": variable_matching_kwarg}
                    ))
                fields_paths[i] = f"{field_key[0:start_variable_first_char_index]}{variable_matching_kwarg}{field_key[end_variable_last_char_index:0]}"
    return fields_paths

