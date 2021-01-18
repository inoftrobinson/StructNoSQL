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
            print(message_with_vars(
                message=f"Primitive value did not match any of the possible expected types. Value of None is being returned.",
                vars_dict={"itemExpectedTypeDatabasePath": item_type_to_return_to.database_path,
                           "value": value, "valueType": value_type, "acceptableExpectedValueTypes": expected_value_type}
            ))
            return None, False
    else:
        if not _types_match(type_to_check=value_type, expected_type=expected_value_type):
            print(message_with_vars(
                message=f"Primitive value did not match expected type. Value of None is being returned.",
                vars_dict={"itemExpectedTypeDatabasePath": item_type_to_return_to.database_path,
                           "value": value, "valueType": value_type, "expectedValueType": expected_value_type}
            ))
            return None, False

    if value_type == dict:
        value: dict
        # todo: fix a bug, where for some reasons, when calling the get_one_field_value_from_single_record function, if what
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
                    if item_type_to_return_to.dict_key_expected_type is not None:
                        key_type = type(key)
                        if not _types_match(type_to_check=key_type, expected_type=item_type_to_return_to.dict_key_expected_type):
                            print(message_with_vars(
                                message=f"Key of an item in a dict did not match expected key type. Item will be removed from data.",
                                vars_dict={"key": key, "item": item, "keyType": key_type, "expectedKeyType": item_type_to_return_to.dict_key_expected_type}
                            ))
                            item_keys_to_pop.append(key)
                            continue

                    if item_type_to_return_to.dict_items_excepted_type is not None:
                        if MapModel in item_type_to_return_to.dict_items_excepted_type.__bases__:
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
                                        vars_dict={"itemKey": element_item_key, "itemValue": item}
                                    ))
                            for element_item_key_to_pop in element_item_keys_to_pop:
                                item.pop(element_item_key_to_pop)
                        else:
                            if not _types_match(type_to_check=type(item), expected_type=item_type_to_return_to.dict_items_excepted_type):
                                item_keys_to_pop.append(key)
                                print(message_with_vars(
                                    message=f"Value of nested item of dict did not match expected type. Item will be removed from data.",
                                    vars_dict={"item": item, "itemKey": key, "expectedItemValueType": item_type_to_return_to.dict_items_excepted_type}
                                ))
                    else:
                        value[key] = item

        if len(value) > 0 and (len(item_keys_to_pop) == len(value)):
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
                matching_validation_model_variable: Optional[BaseField] = getattr(item_type_to_return_to.map_model, key, None)
                if matching_validation_model_variable is not None:
                    item, valid = validate_data(value=item, expected_value_type=matching_validation_model_variable.field_type)
                    if item is None:
                        indexes_to_pop.append(i)
                else:
                    indexes_to_pop.append(i)
                    print(message_with_vars(
                        message=f"No map validator was found in a nested item of a list. Value will be removed from data.",
                        vars_dict={"listValue": value, "item": item, "itemIndex": i}
                    ))

    elif value_type == float:
        # DynamoDB does not support float types. They must be converted to Decimal's.
        return float_to_decimal(float_number=value), True

    return value, True


def _types_match(type_to_check: type, expected_type: type) -> bool:
    if type_to_check != expected_type:
        return False
    return True
