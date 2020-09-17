from typing import Optional, _GenericAlias, Tuple
from StructNoSQL.fields import BaseItem, MapModel


def validate_data(value, expected_value_type: type, map_model: Optional[MapModel] = None,
                  list_items_models: Optional[MapModel] = None) -> bool:
    # todo: add recursion to validator
    value_type = type(value)

    if isinstance(expected_value_type, type):
        _raise_if_types_did_not_match(type_to_check=value_type, expected_type=expected_value_type)
    else:
        if isinstance(expected_value_type, _GenericAlias):
            alias_variable_name: Optional[str] = expected_value_type.__dict__.get("_name", None)
            if alias_variable_name is not None:
                alias_args: Optional[Tuple] = expected_value_type.__dict__.get("__args__", None)

                if alias_variable_name == "Dict":
                    _raise_if_types_did_not_match(type_to_check=value_type, expected_type=dict)

                    if alias_args is not None and len(alias_args) == 2:
                        dict_key_expected_type = alias_args[0]
                        dict_item_expected_type = alias_args[1]
                        for key, item in value.items():
                            _raise_if_types_did_not_match(type_to_check=type(key), expected_type=dict_key_expected_type)
                            if MapModel in dict_item_expected_type.__bases__:
                                value[key] = dict_item_expected_type(**item)
                                print(value)

                elif alias_variable_name == "List":
                    raise Exception(f"List not yet implemented.")


    if value_type == dict:
        value: dict
        if map_model is not None:
            for key, item in value.items():
                matching_validation_model_variable: Optional[BaseItem] = map_model.__dict__.get(key, None)
                if matching_validation_model_variable is not None:
                    validate_data(value=item, expected_value_type=matching_validation_model_variable.field_type)
                else:
                    raise Exception(f"No map validator was found.")

    elif value_type == list:
        value: list
        if list_items_models is not None:
            for item in value:
                matching_validation_model_variable: Optional[BaseItem] = map_model.__dict__.get(key, None)
                if matching_validation_model_variable is not None:
                    validate_data(value=item, expected_value_type=matching_validation_model_variable.field_type)
                else:
                    raise Exception(f"No map validator was found.")

    return True


def _raise_if_types_did_not_match(type_to_check: type, expected_type: type):
    if type_to_check != expected_type:
        raise Exception(f"Data validation exception. The types of an item did not match"
                        f"\n  type_to_check:{type_to_check}"
                        f"\n  expected_type:{expected_type}")
