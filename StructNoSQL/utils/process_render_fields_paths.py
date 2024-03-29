import re
from typing import List, Dict, Any, Optional, Tuple, Union
from StructNoSQL.models import DatabasePathElement
from StructNoSQL.exceptions import FieldTargetNotFoundException
from StructNoSQL.fields import BaseItem, BaseField, MapModel
from StructNoSQL.practical_logger import message_with_vars
from StructNoSQL.exceptions import MissingQueryKwarg

MULTI_ATTRIBUTES_SELECTOR_REGEX_EXPRESSION = r'(\()(.*)(\))'


def _get_field_object_from_field_path(field_path_key: str, fields_switch: Dict[str, BaseField]) -> BaseField:
    current_field_object: Optional[BaseField] = fields_switch.get(field_path_key, None)
    if current_field_object is not None:
        return current_field_object
    else:
        raise FieldTargetNotFoundException(message_with_vars(
            message=f"A field target to get was not found.",
            vars_dict={"field_path_key": field_path_key, "fields_switch": fields_switch}
        ))

def process_and_get_field_path_object_from_field_path(field_path_key: str, fields_switch: dict) -> Tuple[Union[BaseField, Dict[str, BaseField]], bool]:
    matches: Optional[List[tuple]] = re.findall(pattern=MULTI_ATTRIBUTES_SELECTOR_REGEX_EXPRESSION, string=field_path_key)
    if matches is not None and len(matches) > 0:
        for match in matches:
            selected_string = ''.join([selector for selector in match])
            attributes_selectors_string: str = match[1]
            attributes_selector_list = attributes_selectors_string.replace(' ', '').split(',')

            attributes_fields_objets: Dict[str, BaseField] = {}
            for attribute_selector in attributes_selector_list:
                current_attribute_field_path = field_path_key.replace(selected_string, attribute_selector)
                """if len(current_attribute_field_path) > 0 and current_attribute_field_path[0] == ".":
                    # If the start of the field path is a multi selector, the replace will unfortunately add an
                    # invalid point, which we will remove if we see that the first char of the field path is a point.
                    current_attribute_field_path = current_attribute_field_path[1:]"""

                attributes_fields_objets[attribute_selector] = _get_field_object_from_field_path(
                    field_path_key=current_attribute_field_path, fields_switch=fields_switch
                )

            num_attributes_fields: int = len(attributes_fields_objets)
            if not num_attributes_fields > 0:
                raise Exception(message_with_vars(
                    message="Cannot use an attribute selector ( ) without specifying any attribute inside it.",
                    vars_dict={'field_path_key': field_path_key, 'attributes_selectors_string': attributes_selectors_string}
                ))
            return attributes_fields_objets, True

    return _get_field_object_from_field_path(field_path_key=field_path_key, fields_switch=fields_switch), False


def make_rendered_database_path(database_path_elements: List[DatabasePathElement], query_kwargs: dict) -> List[DatabasePathElement]:
    output_database_path_elements: List[DatabasePathElement] = []
    for path_element in database_path_elements:
        if "$key$:" not in path_element.element_key:
            # If the path_element do not contains a key that need to be modified, we can use the current
            # instance of the path element, since it will not be modified, and so will not cause issue
            # when other invocations of queries and operations will use the same path element instance.
            output_database_path_elements.append(path_element)
        else:
            variable_name = path_element.element_key.replace('$key$:', '')
            if query_kwargs is not None:
                matching_kwarg: Optional[Any] = query_kwargs.get(variable_name, None)
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
                    raise MissingQueryKwarg(message_with_vars(
                        message="A variable was required but not found in the query_kwargs dict passed to the make_rendered_database_path function.",
                        vars_dict={"keyVariableName": variable_name, "matchingKwarg": matching_kwarg,
                                   "queryKwargs": query_kwargs, "databasePathElements": database_path_elements}
                    ))
            else:
                raise Exception(message_with_vars(
                    message="A variable was required but no query_kwargs have been passed to the make_rendered_database_path function.",
                    vars_dict={"keyVariableName": variable_name, "queryKwargs": query_kwargs, "databasePathElements": database_path_elements}
                ))
    return output_database_path_elements

def process_and_make_single_rendered_database_path(field_path: str, fields_switch: dict, query_kwargs: dict) -> Tuple[
    Union[
        Tuple[BaseField, List[DatabasePathElement]],
        Dict[str, Tuple[BaseField, List[DatabasePathElement]]]
    ],
    bool
]:
    field_path_object, is_multi_selector = process_and_get_field_path_object_from_field_path(
        field_path_key=field_path, fields_switch=fields_switch
    )
    if is_multi_selector is not True:
        field_path_object: BaseField
        rendered_database_path_elements: List[DatabasePathElement] = make_rendered_database_path(
            database_path_elements=field_path_object.database_path, query_kwargs=query_kwargs
        )
        return (field_path_object, rendered_database_path_elements), False
    else:
        field_path_object: Dict[str, BaseField]
        fields_rendered_database_path_elements: Dict[str, Tuple[BaseField, List[DatabasePathElement]]] = {}
        for single_field_path_object in field_path_object.values():
            rendered_database_path_elements: List[DatabasePathElement] = make_rendered_database_path(
                database_path_elements=single_field_path_object.database_path, query_kwargs=query_kwargs
            )
            fields_rendered_database_path_elements[single_field_path_object.field_name] = (single_field_path_object, rendered_database_path_elements)
        return fields_rendered_database_path_elements, True

def process_transforme_validate_data_from_write_and_make_single_rendered_database_path(
        field_path: str, fields_switch: dict, query_kwargs: dict, data_to_validate: Any
) -> Tuple[BaseField, List[DatabasePathElement], Optional[Any], bool]:

    # todo: add support for multiple fields path
    field_object, is_multi_selector = process_and_get_field_path_object_from_field_path(
        field_path_key=field_path, fields_switch=fields_switch
    )
    rendered_database_path_elements: List[DatabasePathElement] = make_rendered_database_path(
        database_path_elements=field_object.database_path, query_kwargs=query_kwargs
    )

    validated_data, valid = field_object.transform_validate_from_write(value=data_to_validate, data_validation=True)
    if valid is True:
        return field_object, rendered_database_path_elements, validated_data, True
    else:
        return field_object, rendered_database_path_elements, None, False

def join_field_path_elements(field_path_elements: List[DatabasePathElement]) -> str:
    return '.'.join((f'{item.element_key}' for item in field_path_elements))
