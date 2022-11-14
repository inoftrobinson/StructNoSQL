import logging
from typing import Optional, Tuple, List, Any, Callable

from StructNoSQL.models import DatabasePathElement
from StructNoSQL.fields import BaseItem, BaseField, MapModel, DictModel
from StructNoSQL.practical_logger import message_with_vars
from StructNoSQL.utils.misc_fields_items import try_to_get_primitive_default_type_of_item


def _types_match(type_to_check: type, expected_type: type) -> bool:
    processed_expected_type: type = try_to_get_primitive_default_type_of_item(expected_type)
    # The type_to_check will always be a primitive Python type, where as the expected_type can also be a StructNoSQL
    # model type. In which case, we try_to_get_primitive_default_type_of_item. If the type was not a StructNoSQL
    # model type and did not had a primitive_default_type, the returned type will be the source type we passed.
    if processed_expected_type is Any:
        return True
    elif type_to_check != processed_expected_type:
        return False
    return True

def validate_data(
        value: Any, expected_value_type: Any,
        item_type_to_return_to: Optional[BaseItem] = None,
        data_validation: bool = True,
        value_transformer: Callable[[Any, Optional[BaseItem]], Any] = None
) -> Tuple[Any, bool]:

    transformed_value = value if value_transformer is None else value_transformer(value, item_type_to_return_to)
    value = transformed_value

    value_type = type(value)
    # We do not try_to_get_primitive_default_type_of_item here, because depending on the value_type, we might trigger different behaviors.
    # For example, a list or tuple will be considered as collection of multiple fields types that needs to be looked at individually.

    if expected_value_type == Any:
        return value, True

    if data_validation is True:
        if type(expected_value_type) in [list, tuple]:
            has_found_match = False
            for acceptable_value_type in expected_value_type:
                if _types_match(type_to_check=value_type, expected_type=acceptable_value_type):
                    has_found_match = True
                    break

            if has_found_match is not True:
                vars_dict = {'value': value, 'valueType': value_type, 'expectedValueType': expected_value_type}
                if item_type_to_return_to is not None:
                    vars_dict['itemExpectedTypeDatabasePath'] = item_type_to_return_to.database_path

                logging.warning(message_with_vars(
                    message=f"Primitive value did not match any of the possible "
                            f"expected types. Value of None is being returned.",
                    vars_dict=vars_dict
                ))
                return None, False
        else:
            if not _types_match(type_to_check=value_type, expected_type=expected_value_type):
                vars_dict = {'value': value, 'valueType': value_type, 'expectedValueType': expected_value_type}
                if item_type_to_return_to is not None:
                    vars_dict['itemExpectedTypeDatabasePath'] = item_type_to_return_to.database_path

                logging.warning(message_with_vars(
                    message=f"Primitive value did not match expected "
                            f"type. Value of None is being returned.",
                    vars_dict=vars_dict
                ))
                return None, False

    if value_type == dict:
        value: dict
        # todo: fix a bug, where for some reasons, when calling the get_field function, if what
        #  we get is a dict that has only key and one item, instead of returning the dict, we will return the value in the dict
        item_keys_to_pop: List[str] = []
        if item_type_to_return_to is not None:
            if (
                item_type_to_return_to.map_model is not None and
                not isinstance(item_type_to_return_to.map_model, DictModel)
            ):
                populated_required_fields: List[BaseField] = []

                item_keys_to_pop: List[str] = []
                for key, item in value.items():
                    item_matching_validation_model_variable: Optional[BaseField] = getattr(item_type_to_return_to.map_model, key, None)
                    if item_matching_validation_model_variable is not None:
                        item, valid = validate_data(
                            value=item, item_type_to_return_to=item_matching_validation_model_variable,
                            expected_value_type=item_matching_validation_model_variable.field_type,
                            data_validation=data_validation, value_transformer=value_transformer
                        )
                        if valid is True:
                            value[key] = item
                            if item_matching_validation_model_variable.required is True:
                                populated_required_fields.append(item_matching_validation_model_variable)
                        else:
                            item_keys_to_pop.append(key)
                    else:
                        item_keys_to_pop.append(key)
                        print(message_with_vars(
                            message=f"No map validator was found in a nested item of a dict. Item might not be properly transformed." +
                                    " Item will be removed from data." if data_validation is True else "",
                            vars_dict={"key": key, "item": item}
                        ))

                if data_validation is True:
                    # The below code will return an empty result if not all required_fields are populated, this is disabled if data_validation is not True.
                    map_model_required_fields: Optional[List[BaseField]] = getattr(item_type_to_return_to.map_model, 'required_fields', None)
                    if map_model_required_fields is None:
                        raise Exception("Missing required_fields")

                    if len(map_model_required_fields) != len(populated_required_fields):
                        missing_required_fields_database_paths: List[List[DatabasePathElement]] = []
                        for current_required_field in map_model_required_fields:
                            if current_required_field not in populated_required_fields:
                                missing_required_fields_database_paths.append(current_required_field.database_path)

                        print(message_with_vars(
                            message="Missing required fields on map element. Returning None and valid to False.",
                            vars_dict={"missingRequiredFieldsDatabasePaths": missing_required_fields_database_paths}
                        ))
                        return None, False
            else:
                for key, item in value.items():
                    if item_type_to_return_to.key_expected_type is not None:
                        key_type = type(key)
                        if not _types_match(type_to_check=key_type, expected_type=item_type_to_return_to.key_expected_type):
                            print(message_with_vars(
                                message=f"Key of an item in a dict did not match expected key type. Item will be removed from data.",
                                vars_dict={"key": key, "item": item, "keyType": key_type, "expectedKeyType": item_type_to_return_to.key_expected_type}
                            ))
                            item_keys_to_pop.append(key)
                            continue

                    if item_type_to_return_to.items_excepted_type is not None:
                        if hasattr(item_type_to_return_to.items_excepted_type, '__bases__') and MapModel in item_type_to_return_to.items_excepted_type.__bases__:
                            # We check if the items_excepted_type contains the __bases__ attributes, because form values (like the Any value that is assigned both when
                            # using an untyped dict or when using Any in a typed Dict) will not contain the __bases__ attribute and will raise if trying to access it.
                            element_item_keys_to_pop: List[str] = []

                            item_type = type(item)
                            if not _types_match(type_to_check=item_type, expected_type=dict):
                                print(message_with_vars(
                                    message=f"Received data that should be set inside a nested MapModel "
                                            f"was not of type dict. Item will be removed from data.",
                                    vars_dict={"key": key, "item": item, "itemType": item_type}
                                ))
                                item_keys_to_pop.append(key)
                                continue
                            item: dict

                            item_matching_validation_model_variable: Optional[BaseField] = getattr(item_type_to_return_to.map_model, key, None)
                            if item_matching_validation_model_variable is not None:
                                for element_item_key, element_item_value in item.items():
                                    element_item_matching_validation_model_variable: Optional[BaseField] = getattr(
                                        item_matching_validation_model_variable, element_item_key, None
                                    )
                                    if element_item_matching_validation_model_variable is not None:
                                        element_item_value, valid = validate_data(
                                            value=element_item_value, item_type_to_return_to=element_item_matching_validation_model_variable,
                                            expected_value_type=element_item_matching_validation_model_variable.field_type,
                                            data_validation=data_validation, value_transformer=value_transformer
                                        )
                                        if valid is True:
                                            item[element_item_key] = element_item_value
                                        else:
                                            if element_item_matching_validation_model_variable.required is not True:
                                                element_item_keys_to_pop.append(element_item_key)
                                            else:
                                                item_keys_to_pop.append(key)
                                                break
                                    else:
                                        element_item_keys_to_pop.append(element_item_key)
                                        print(message_with_vars(
                                            message=f"No map validator was found in a nested item of a dict. Item will be removed from data.",
                                            vars_dict={"elementItemKey": key, "elementItemValue": element_item_value}
                                        ))
                                else:
                                    print(message_with_vars(
                                        message=f"No map validator was found in a item of a dict. Item will be removed from data.",
                                        vars_dict={"itemKey": key, "itemValue": item}
                                    ))
                            for element_item_key_to_pop in element_item_keys_to_pop:
                                item.pop(element_item_key_to_pop)
                        else:
                            if not _types_match(type_to_check=type(item), expected_type=item_type_to_return_to.items_excepted_type):
                                item_keys_to_pop.append(key)
                                print(message_with_vars(
                                    message=f"Value of nested item of dict did not match expected type. Item will be removed from data.",
                                    vars_dict={"item": item, "itemKey": key, "expectedItemValueType": item_type_to_return_to.items_excepted_type}
                                ))
                    else:
                        value[key] = item

        if data_validation is not True:
            return value, True
        else:
            num_dict_items = len(value)
            if num_dict_items > 0 and (len(item_keys_to_pop) == num_dict_items):
                print(message_with_vars(
                    message="The value dict to validate was not empty, but all of its items have been "
                            "removed because they did not matched the model. Value of None is returned.",
                    vars_dict={"value": value, "item_keys_to_pop": item_keys_to_pop}
                ))
                return None, False
            else:
                for item_key_to_pop in item_keys_to_pop:
                    value.pop(item_key_to_pop)
                return value, True

    elif value_type == list:
        value: list
        if True:  # list_items_models is not None:  # todo: add type checking fo list models
            indexes_to_pop: List[int] = []
            for i, item in enumerate(value):
                if item_type_to_return_to.map_model is not None:
                    item, valid = validate_data(
                        value=item, expected_value_type=item_type_to_return_to.map_model,
                        data_validation=data_validation, value_transformer=value_transformer
                    )
                    if valid is False:
                        indexes_to_pop.append(i)
                elif item_type_to_return_to.items_excepted_type is not None:
                    item, valid = validate_data(
                        value=item, expected_value_type=item_type_to_return_to.items_excepted_type,
                        data_validation=data_validation, value_transformer=value_transformer
                    )
                    if valid is False:
                        indexes_to_pop.append(i)
                # If no map validator has been found, this means we have an untyped list. So, we will
                # not perform any data validation on the list items and consider all the items valid.
                """else:
                    indexes_to_pop.append(i)
                    print(message_with_vars(
                        message=f"No map validator was found in a nested item of a list. Value will be removed from data.",
                        vars_dict={"listValue": value, "item": item, "itemIndex": i}
                    ))"""

            indexes_to_pop.reverse()
            for index in indexes_to_pop:
                value.pop(index)

    elif value_type == set:
        value: set

        if item_type_to_return_to.items_excepted_type is not None:
            items_keys_values_to_remove = []
            for set_item in value:
                item_type = type(set_item)
                if not _types_match(type_to_check=item_type, expected_type=item_type_to_return_to.items_excepted_type):
                    items_keys_values_to_remove.append(set_item)
                    print(message_with_vars(
                        message=f"Value of item of set did not match expected type. Item will be removed from data.",
                        vars_dict={'item': set_item, 'itemType': item_type, 'expectedItemValueType': item_type_to_return_to.items_excepted_type}
                    ))

            num_set_items = len(value)
            if num_set_items > 0 and (len(items_keys_values_to_remove) == num_set_items):
                print(message_with_vars(
                    message="The value set to validate was not empty, but all of its items have been "
                            "removed because they did not matched the model. Value of None is returned.",
                    vars_dict={'value': value, 'itemsToRemove': items_keys_values_to_remove}
                ))
                return None, False
            else:
                for item_to_remove in items_keys_values_to_remove:
                    value.remove(item_to_remove)
                return value, True
        return value, True

    """
    # Even tough DynamoDB does not support float types, the conversion 
    between floats to Decimal is being done in the DynamoDBCore functions
    elif value_type == float:
        # DynamoDB does not support float types. They must be converted to Decimal's.
        return value, True
    """

    return value, True
