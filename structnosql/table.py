from typing import Optional, List, Dict, Any
from StructNoSQL.dynamodb.dynamodb_core import DynamoDbCoreAdapter, PrimaryIndex, GlobalSecondaryIndex
from StructNoSQL.dynamodb.models import DatabasePathElement
from StructNoSQL.fields import BaseField, MapModel, MapField, MapItem
from StructNoSQL.practical_logger import message_with_vars
from StructNoSQL.utils.process_render_fields_paths import process_and_get_fields_paths_objects_from_fields_paths, \
    process_and_make_single_rendered_database_path, process_validate_data_and_make_single_rendered_database_path


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
            self.model = data_model
        else:
            self.model = data_model()
        assign_internal_mapping_from_class(table=self, class_instance=self.model)

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

    def get_from_single_item(self, key_name: str, key_value: str, fields_to_get: List[str], query_kwargs: Optional[dict] = None) -> Any:
        response = self.dynamodb_client.get_item_in_path_target(
            key_name=key_name, key_value=key_value, target_path_elements=fields_to_get
        )
        # todo: add support for multiple items targets

    def query(self, key_name: str, key_value: str, fields_to_get: List[str], index_name: Optional[str] = None,
              limit: Optional[int] = None, query_kwargs: Optional[dict] = None) -> Optional[List[Any]]:
        # rendered_fields_paths = make_rendered_fields_paths(fields_paths=fields_to_get, query_kwargs=query_kwargs)
        fields_paths_objects = process_and_get_fields_paths_objects_from_fields_paths(
            fields_paths=fields_to_get, fields_switch=self.fields_switch
        )
        response = self.dynamodb_client.query_by_key(
            key_name=key_name, key_value=key_value, fields_to_get=fields_to_get,
            index_name=index_name, query_limit=limit
        )
        if response is not None:
            for current_item in response.items:
                if isinstance(current_item, dict):
                    for current_item_key, current_item_value in current_item.items():
                        matching_field_path_object = fields_paths_objects.get(current_item_key, None)
                        if matching_field_path_object is not None:
                            if matching_field_path_object.database_path is not None:
                                matching_field_path_object.populate(value=current_item_value)
                                current_item[current_item_key] = matching_field_path_object.validate_data(load_data_into_objects=False)
            return response.items
        else:
            return None

    def set_update_one_field(self, key_name: str, key_value: str, target_field: str, value_to_set: Any,
                             index_name: Optional[str] = None, query_kwargs: Optional[dict] = None) -> bool:
        validated_data, target_path_elements = process_validate_data_and_make_single_rendered_database_path(
            field_path=target_field, fields_switch=self.fields_switch, query_kwargs=query_kwargs, data_to_validate=value_to_set
        )
        if validated_data is not None and target_path_elements is not None:
            response = self.dynamodb_client.set_update_data_element_to_map(
                key_name=key_name, key_value=key_value, value=validated_data,
                target_path_elements=target_path_elements
            )
            return True if response is not None else False
        return False

    def set_update_multiple_fields(self):
        raise Exception(f"Not implemented")

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

    class_variables = dict()
    if class_type is not None:
        class_variables = class_type.__dict__
    elif class_instance is not None:
        class_variables = class_instance.__class__.__dict__

    for variable_key, variable_item in class_variables.items():
        current_field_path = "" if nested_field_path is None else f"{nested_field_path}"

        try:
            if not isinstance(variable_item, type):
                variable_bases = variable_item.__class__.__bases__
            else:
                variable_bases = variable_item.__bases__

            # if BaseField in variable_bases:
            if isinstance(variable_item, BaseField):
                variable_item: BaseField
                new_database_path_element = DatabasePathElement(element_key=variable_item.field_name, default_type=variable_item.field_type)
                variable_item._database_path = [*current_path_elements, new_database_path_element]
                variable_item._table = table
                output_mapping[variable_key] = ""

                current_field_path += f"{variable_item.field_name}" if len(current_field_path) == 0 else f".{variable_item.field_name}"
                table.fields_switch[current_field_path] = variable_item

                if variable_item.dict_items_excepted_type is not None:
                    current_field_path += ".{{" + variable_item.key_name + "}}"
                    map_item = MapItem(model_type=variable_item.dict_items_excepted_type, parent_field=variable_item)
                    table.fields_switch[current_field_path] = map_item

                    item_key_name = make_dict_key_var_name(key_name=variable_item.key_name)
                    item_default_type = try_to_get_primitive_default_type_of_item(item_type=variable_item.dict_items_excepted_type)

                    new_database_dict_item_path_element = DatabasePathElement(element_key=item_key_name, default_type=item_default_type)
                    output_mapping[item_key_name] = assign_internal_mapping_from_class(
                        table=table, class_type=variable_item.dict_items_excepted_type, nested_field_path=current_field_path,
                        current_path_elements=[*variable_item.database_path, new_database_dict_item_path_element]
                    )

            elif MapField in variable_bases:
                variable_item: MapField
                new_database_path_element = DatabasePathElement(element_key=variable_item.field_name, default_type=variable_item.field_type)
                variable_item._database_path = [*current_path_elements, new_database_path_element]
                variable_item._table = table

                current_field_path += f"{variable_item.field_name}" if len(current_field_path) == 0 else f".{variable_item.field_name}"
                table.fields_switch[current_field_path] = variable_item

                output_mapping[variable_item.field_name] = assign_internal_mapping_from_class(
                    table=table, class_type=variable_item,
                    nested_field_path=current_field_path,
                    current_path_elements=variable_item.database_path
                )

            elif MapModel in variable_bases:
                variable_item: MapField
                print(variable_item)
                continue

                variable_item: MapModel
                variable_item._database_path = {**current_path_elements}
                output_mapping[variable_key] = assign_internal_mapping_from_class(
                    table=table, class_type=variable_item, current_path_elements=variable_item.database_path
                )


        except Exception as e:
            print(e)

    return output_mapping
