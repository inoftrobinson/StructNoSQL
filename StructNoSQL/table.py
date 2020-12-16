from typing import Optional, List, Dict, Any, Set
from StructNoSQL.dynamodb.dynamodb_core import DynamoDbCoreAdapter, PrimaryIndex, GlobalSecondaryIndex, \
    DynamoDBMapObjectSetter, Response
from StructNoSQL.dynamodb.models import DatabasePathElement, FieldGetter, FieldSetter, FieldRemover
from StructNoSQL.fields import BaseField, MapModel, MapField, MapItem, TableDataModel
from StructNoSQL.practical_logger import message_with_vars
from StructNoSQL.utils.process_render_fields_paths import process_and_get_fields_paths_objects_from_fields_paths, \
    process_and_make_single_rendered_database_path, process_validate_data_and_make_single_rendered_database_path
from StructNoSQL.utils.types import PRIMITIVE_TYPES

# todo: add ability to add or remove items from list's

class DatabaseKey(str):
    pass


class BaseTable:
    def __init__(self, table_name: str, region_name: str, data_model, primary_index: PrimaryIndex,
                 create_table: bool = True, billing_mode: str = DynamoDbCoreAdapter.PAY_PER_REQUEST,
                 global_secondary_indexes: List[GlobalSecondaryIndex] = None, auto_create_table: bool = True):

        self.fields_switch = dict()
        self._internal_mapping = dict()
        self._dynamodb_client = DynamoDbCoreAdapter(
            table_name=table_name, region_name=region_name,
            primary_index=primary_index,
            global_secondary_indexes=global_secondary_indexes,
            create_table=auto_create_table
        )

        if not isinstance(data_model, type):
            self._model = data_model
        else:
            self._model = data_model()
        self._model_virtual_map_field = None

        self.processed_class_types: Set[type] = set()
        assign_internal_mapping_from_class(table=self, class_instance=self._model)

    @property
    def model(self) -> TableDataModel:
        return self._model

    @property
    def model_virtual_map_field(self) -> MapField:
        if self._model_virtual_map_field is None:
            self._model_virtual_map_field = MapField(name="", model=self._model)
            # The model_virtual_map_field is a MapField with no name, that use the table model, which easily
            # give us the ability to use the functions of the MapField object (for example, functions for
            # data validation), with the data model of the table itself. For example, the put_record
            # operation, needs to validate its data, based on the table data model, not a MapField.
        return self._model_virtual_map_field

    def put_record(self, record_dict_data: dict) -> bool:
        self.model_virtual_map_field.populate(value=record_dict_data)
        validated_data, is_valid = self.model_virtual_map_field.validate_data()
        if is_valid is True:
            return self.dynamodb_client.put_record(item_dict=validated_data)
        else:
            return False

    def delete_record(self, indexes_keys_selectors: dict) -> bool:
        found_all_indexes = True
        for index_key, index_target_value in indexes_keys_selectors.items():
            index_matching_field = getattr(self.model, index_key, None)
            if index_matching_field is None:
                found_all_indexes = False
                print(message_with_vars(
                    message="An index key selector passed to the delete_record function, was not found, in the table model. Operation not executed.",
                    vars_dict={"index_key": index_key, "index_target_value": index_target_value, "index_matching_field": index_matching_field, "table.model": self.model}
                ))

        if found_all_indexes is True:
            return self.dynamodb_client.delete_record(indexes_keys_selectors=indexes_keys_selectors)
        else:
            return False


    def get_single_field_item_from_single_item(self, key_name: str, key_value: str, field_to_get: str, query_kwargs: Optional[dict] = None) -> Any:
        response_data = self.dynamodb_client.get_item_in_path_target(
            key_name=key_name, key_value=key_value,
            target_path_elements=process_and_make_single_rendered_database_path(
                field_path=field_to_get, fields_switch=self.fields_switch, query_kwargs=query_kwargs
            )
        )
        return response_data

    def get_single_field_value_from_single_item(self, key_name: str, key_value: str, field_to_get: str, query_kwargs: Optional[dict] = None) -> Any:
        response_data = self.dynamodb_client.get_value_in_path_target(
            key_name=key_name, key_value=key_value,
            target_path_elements=process_and_make_single_rendered_database_path(
                field_path=field_to_get, fields_switch=self.fields_switch, query_kwargs=query_kwargs
            )
        )
        return response_data


    def _getters_to_database_paths(self, getters: Dict[str, FieldGetter]) -> Dict[str, List[DatabasePathElement]]:
        getters_database_paths: Dict[str, List[DatabasePathElement]] = dict()
        for getter_key, getter_item in getters.items():
            getters_database_paths[getter_key] = process_and_make_single_rendered_database_path(
                field_path=getter_item.target_path, fields_switch=self.fields_switch, query_kwargs=getter_item.query_kwargs
            )
        return getters_database_paths

    def get_multiple_fields_items_from_single_item(self, key_name: str, key_value: str, getters: Dict[str, FieldGetter]) -> Optional[dict]:
        getters_database_paths = self._getters_to_database_paths(getters=getters)
        response_data = self.dynamodb_client.get_items_in_multiple_path_target(
            key_name=key_name, key_value=key_value, targets_paths_elements=getters_database_paths
        )
        return response_data

    def get_multiple_fields_values_from_single_item(self, key_name: str, key_value: str, getters: Dict[str, FieldGetter]) -> Optional[dict]:
        getters_database_paths = self._getters_to_database_paths(getters=getters)
        response_data = self.dynamodb_client.get_values_in_multiple_path_target(
            key_name=key_name, key_value=key_value, targets_paths_elements=getters_database_paths
        )
        return response_data


    def query(self, key_name: str, key_value: str, fields_to_get: List[str], index_name: Optional[str] = None, limit: Optional[int] = None,
              query_kwargs: Optional[dict] = None, filter_expression: Optional[Any] = None, **additional_kwargs) -> Optional[List[Any]]:
        # rendered_fields_paths = make_rendered_fields_paths(fields_paths=fields_to_get, query_kwargs=query_kwargs)
        fields_paths_objects = process_and_get_fields_paths_objects_from_fields_paths(
            fields_paths=fields_to_get, fields_switch=self.fields_switch
        )
        response = self.dynamodb_client.query_by_key(
            key_name=key_name, key_value=key_value,
            fields_to_get=fields_to_get, index_name=index_name, query_limit=limit,
            filter_expression=filter_expression, **additional_kwargs
        )
        if response is not None:
            for current_item in response.items:
                if isinstance(current_item, dict):
                    for current_item_key, current_item_value in current_item.items():
                        matching_field_path_object = fields_paths_objects.get(current_item_key, None)
                        if matching_field_path_object is not None:
                            if matching_field_path_object.database_path is not None:
                                matching_field_path_object.populate(value=current_item_value)
                                current_item[current_item_key], valid = matching_field_path_object.validate_data(load_data_into_objects=False)
            return response.items
        else:
            return None


    def set_update_one_field(self, key_name: str, key_value: str, target_field: str, value_to_set: Any,
                             index_name: Optional[str] = None, query_kwargs: Optional[dict] = None) -> bool:
        validated_data, valid, target_path_elements = process_validate_data_and_make_single_rendered_database_path(
            field_path=target_field, fields_switch=self.fields_switch, query_kwargs=query_kwargs, data_to_validate=value_to_set
        )
        if valid is True and target_path_elements is not None:
            response = self.dynamodb_client.set_update_data_element_to_map(
                key_name=key_name, key_value=key_value, value=validated_data,
                target_path_elements=target_path_elements
            )
            return True if response is not None else False
        return False

    def set_update_multiple_fields(self, key_name: str, key_value: str, setters: List[FieldSetter]) -> bool:
        dynamodb_setters: List[DynamoDBMapObjectSetter] = list()
        for current_setter in setters:
            validated_data, valid, target_path_elements = process_validate_data_and_make_single_rendered_database_path(
                field_path=current_setter.target_path, fields_switch=self.fields_switch,
                query_kwargs=current_setter.query_kwargs, data_to_validate=current_setter.value_to_set
            )
            if valid is True:
                dynamodb_setters.append(DynamoDBMapObjectSetter(
                    target_path_elements=target_path_elements, value_to_set=validated_data
                ))

        response = self.dynamodb_client.set_update_multiple_data_elements_to_map(
            key_name=key_name, key_value=key_value, setters=dynamodb_setters
        )
        return True if response is not None else False


    def remove_single_item_at_path_target(self, key_name: str, key_value: str, target_field: str,
                                          query_kwargs: Optional[dict] = None) -> bool:
        response: Optional[Response] = self.dynamodb_client.remove_data_elements_from_map(
            key_name=key_name, key_value=key_value,
            targets_path_elements=[process_and_make_single_rendered_database_path(
                field_path=target_field, fields_switch=self.fields_switch, query_kwargs=query_kwargs
            )]
        )
        return True if response is not None else False

    def remove_multiple_items_at_path_targets(self, key_name: str, key_value: str, removers: List[FieldRemover]) -> bool:
        if len(removers) > 0:
            removers_database_paths: List[List[DatabasePathElement]] = list()
            for current_remover in removers:
                removers_database_paths.append(
                    process_and_make_single_rendered_database_path(
                        field_path=current_remover.target_path,
                        fields_switch=self.fields_switch,
                        query_kwargs=current_remover.query_kwargs
                    )
                )

            response: Optional[Response] = self.dynamodb_client.remove_data_elements_from_map(
                key_name=key_name, key_value=key_value, targets_path_elements=removers_database_paths
            )
            return True if response is not None else False
        else:
            # If no remover has been specified, we do not run the database operation, yet we still
            # return True, since technically, what needed to be performed (nothing) was performed.
            return True


    @property
    def internal_mapping(self) -> dict:
        return self._internal_mapping

    @property
    def dynamodb_client(self) -> DynamoDbCoreAdapter:
        return self._dynamodb_client


def make_dict_key_var_name(key_name: str) -> str:
    return f"$key$:{key_name}"

def try_to_get_primitive_default_type_of_item(item_type: Any):
    try:
        return item_type._default_primitive_type
        # Some objects (like a map object), are not primitive types, and instead of being able to use their type
        # as default database type, they have a _default_primitive_type variable that we can use. Trying to get
        # the variable is also faster than checking if the type is one of our types that is not primitive.
    except Exception as e:
        return item_type


def assign_internal_mapping_from_class(table: BaseTable, class_instance: Optional[Any] = None, class_type: Optional[Any] = None,
                                       nested_field_path: Optional[str] = None, current_path_elements: Optional[List[DatabasePathElement]] = None):
    if current_path_elements is None:
        current_path_elements = list()
    output_mapping = dict()

    if class_type is None:
        if class_instance is not None:
            class_type = class_instance.__class__
        else:
            raise Exception(message_with_vars(
                message="class_type or class_instance args must be passed "
                        "to the assign_internal_mapping_from_class function"
            ))

    if class_type in table.processed_class_types:
        return None
    else:
        table.processed_class_types.update({class_type})

    class_variables = class_type.__dict__
    required_fields = list()
    setup_function: Optional[callable] = class_variables.get('__setup__', None)
    if setup_function is not None:
        custom_setup_class_variables: dict = class_type.__setup__()
        if len(custom_setup_class_variables) > 0:
            # The class_variables gotten from calling the __dict__ attribute is a mappingproxy, which cannot be modify.
            # In order to combine the custom_setup_class_variables and the class_variables variables we will iterate
            # over all the class_variables attributes, add them to the dict create by the __setup__ function (only if
            # they are not found in the custom_setup_class_variables dict, since the custom setup override any default
            # class attribute), and assign the class_variables variable to our newly create and setup dict.
            for key, item in class_variables.items():
                if key not in custom_setup_class_variables:
                    custom_setup_class_variables[key] = item
            class_variables = custom_setup_class_variables

    for variable_key, variable_item in class_variables.items():
        current_field_path = "" if nested_field_path is None else f"{nested_field_path}"

        try:
            if isinstance(variable_item, MapField):
                variable_item: MapField

                new_database_path_element = DatabasePathElement(
                    element_key=variable_item.field_name,
                    default_type=variable_item.field_type,
                    custom_default_value=variable_item.custom_default_value
                )
                variable_item._database_path = [*current_path_elements, new_database_path_element]
                variable_item._table = table

                if variable_item.required is True:
                    required_fields.append(variable_item)

                current_field_path += f"{variable_item.field_name}" if len(current_field_path) == 0 else f".{variable_item.field_name}"
                table.fields_switch[current_field_path] = variable_item

                output_mapping[variable_item.field_name] = assign_internal_mapping_from_class(
                    table=table, class_type=variable_item.map_model, nested_field_path=current_field_path,
                    current_path_elements=[*variable_item.database_path]
                )

            elif isinstance(variable_item, BaseField):
                variable_item: BaseField
                new_database_path_element = DatabasePathElement(
                    element_key=variable_item.field_name,
                    default_type=variable_item.default_field_type,
                    custom_default_value=variable_item.custom_default_value
                )
                variable_item._database_path = [*current_path_elements, new_database_path_element]
                variable_item._table = table
                output_mapping[variable_key] = ""

                if variable_item.required is True:
                    required_fields.append(variable_item)

                current_field_path += f"{variable_item.field_name}" if len(current_field_path) == 0 else f".{variable_item.field_name}"
                table.fields_switch[current_field_path] = variable_item

                if variable_item.dict_items_excepted_type is not None:
                    current_field_path += ".{{" + variable_item.key_name + "}}"

                    item_default_type = try_to_get_primitive_default_type_of_item(item_type=variable_item.dict_items_excepted_type)
                    map_item = MapItem(
                        parent_field=variable_item, field_type=item_default_type,
                        model_type=variable_item.dict_items_excepted_type
                    )
                    table.fields_switch[current_field_path] = map_item
                    item_key_name = make_dict_key_var_name(key_name=variable_item.key_name)

                    if variable_item.dict_items_excepted_type not in PRIMITIVE_TYPES:
                        new_database_dict_item_path_element = DatabasePathElement(element_key=item_key_name, default_type=item_default_type)
                        output_mapping[item_key_name] = assign_internal_mapping_from_class(
                            table=table, class_type=variable_item.dict_items_excepted_type, nested_field_path=current_field_path,
                            current_path_elements=[*variable_item.database_path, new_database_dict_item_path_element]
                        )

        except Exception as e:
            print(e)

    setattr(class_type, "required_fields", required_fields)
    # We need to set the attribute, because when we go the required_fields with the get_attr
    # function, we did not get a reference to the attribute, but a copy of the attribute value.

    return output_mapping
