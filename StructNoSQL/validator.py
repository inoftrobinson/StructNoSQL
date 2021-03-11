from typing import Optional, _GenericAlias, Tuple, List, Any

from StructNoSQL.dynamodb.models import DatabasePathElement
from StructNoSQL.fields import BaseItem, BaseField, MapModel
from StructNoSQL.field_loader import load as field_load
from StructNoSQL.practical_logger import message_with_vars
from StructNoSQL.utils.decimals import float_to_decimal, float_to_decimal_serializer

NoneType = type(None)
class ActiveSelf:
    pass


def validate_data(value: Any, expected_value_type: Any, item_type_to_return_to: Optional[BaseItem] = None) -> Tuple[Any, bool]:
    value_type = type(value)

    if expected_value_type == Any:
        return float_to_decimal_serializer(value), True

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

            print(message_with_vars(
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

            print(message_with_vars(
                message=f"Primitive value did not match expected "
                        f"type. Value of None is being returned.",
                vars_dict=vars_dict
            ))
            return None, False

    if value_type == dict:
        value: dict
        # todo: fix a bug, where for some reasons, when calling the get_field function, if what
        #  we get is a dict that has only key and one item, instead of returning the dict, we will return the value in the dict
        item_keys_to_pop: List[str] = list()
        if item_type_to_return_to is not None:
            if item_type_to_return_to.map_model is not None:
                populated_required_fields: List[BaseField] = list()

                item_keys_to_pop: List[str] = list()
                for key, item in value.items():
                    item_matching_validation_model_variable: Optional[BaseField] = getattr(item_type_to_return_to.map_model, key, None)
                    if item_matching_validation_model_variable is not None:
                        item, valid = validate_data(
                            value=item, item_type_to_return_to=item_matching_validation_model_variable,
                            expected_value_type=item_matching_validation_model_variable.field_type,
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
                            message=f"No map validator was found in a nested item of a dict. Item will be removed from data.",
                            vars_dict={"key": key, "item": item}
                        ))

                if len(item_type_to_return_to.map_model.required_fields) != len(populated_required_fields):
                    missing_required_fields_database_paths: List[List[DatabasePathElement]] = list()
                    for current_required_field in item_type_to_return_to.map_model.required_fields:
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
                        if MapModel in item_type_to_return_to.items_excepted_type.__bases__:
                            element_item_keys_to_pop: List[str] = list()

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
            indexes_to_pop: List[int] = list()
            for i, item in enumerate(value):
                if item_type_to_return_to.map_model is not None:
                    item, valid = validate_data(value=item, expected_value_type=item_type_to_return_to.map_model)
                    if valid is False:
                        indexes_to_pop.append(i)
                elif item_type_to_return_to.items_excepted_type is not None:
                    item, valid = validate_data(value=item, expected_value_type=item_type_to_return_to.items_excepted_type)
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
            items_keys_values_to_remove = list()
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

    elif value_type == float:
        # DynamoDB does not support float types. They must be converted to Decimal's.
        return float_to_decimal(float_number=value), True

    return value, True


def _types_match(type_to_check: type, expected_type: type) -> bool:
    if expected_type is Any:
        return True
    elif type_to_check != expected_type:
        return False
    return True
