from typing import Optional, List, Dict, Any
from StructNoSQL.dynamodb.dynamodb_core import DynamoDbCoreAdapter, PrimaryIndex, GlobalSecondaryIndex
from StructNoSQL.dynamodb.models import DatabasePathElement
from StructNoSQL.fields import BaseField, BaseField, MapModel, MapField


class DatabaseKey(str):
    pass


class BaseTable:
    def __init__(self, table_name: str, region_name: str, data_model, primary_index: PrimaryIndex,
                 create_table: bool = True, billing_mode: str = DynamoDbCoreAdapter.PAY_PER_REQUEST,
                 global_secondary_indexes: List[GlobalSecondaryIndex] = None, auto_create_table: bool = True):

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
        class_variables = assign_internal_mapping_from_class(table=self, class_instance=self.model)
        print(class_variables)

    def query(self, target: BaseField or MapModel or str, key_name: str, key_value: str, index_name: Optional[str] = None, limit: Optional[int] = None):
        response = self.dynamodb_client.query_by_key(
            key_name=key_name, key_value=key_value, index_name=index_name, query_limit=limit
        )

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
                                       current_path_elements: Optional[List[DatabasePathElement]] = None):
    if current_path_elements is None:
        current_path_elements = list()
    output_mapping = dict()

    class_variables = dict()
    if class_type is not None:
        class_variables = class_type.__dict__
    elif class_instance is not None:
        class_variables = class_instance.__class__.__dict__

    for variable_key, variable_item in class_variables.items():
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

                if variable_item.dict_items_excepted_type is not None:
                    item_key_name = make_dict_key_var_name(key_name=variable_item.key_name)
                    item_default_type = try_to_get_primitive_default_type_of_item(item_type=variable_item.dict_items_excepted_type)

                    new_database_dict_item_path_element = DatabasePathElement(element_key=item_key_name, default_type=item_default_type)
                    output_mapping[item_key_name] = assign_internal_mapping_from_class(
                        table=table, class_type=variable_item.dict_items_excepted_type,
                        current_path_elements=[*variable_item.database_path, new_database_dict_item_path_element]
                    )

            elif MapField in variable_bases:
                variable_item: MapField
                new_database_path_element = DatabasePathElement(element_key=variable_item.field_name, default_type=variable_item.field_type)
                variable_item._database_path = [*current_path_elements, new_database_path_element]
                variable_item._table = table
                output_mapping[variable_item.field_name] = assign_internal_mapping_from_class(
                    table=table, class_type=variable_item, current_path_elements=variable_item.database_path
                )

            elif MapModel in variable_bases:
                continue

                variable_item: MapModel
                variable_item._database_path = {**current_path_elements}
                output_mapping[variable_key] = assign_internal_mapping_from_class(
                    table=table, class_type=variable_item, current_path_elements=variable_item.database_path
                )


        except Exception as e:
            print(e)

    return output_mapping


"""
if __name__ == "__main__":
    users_table.projects.query()
    print(users_table.ProjectsModel.ProjectInfos.primaryUrl.post(value="Yolooooooo"))
    print(users_table.__class__.__dict__)
    # print(signature(users_table.ProjectsModel))
    # print(users_table.ProjectsModel(projectName="yolo").projectName)
    # print(users_table.__class__.__dict__)
"""

