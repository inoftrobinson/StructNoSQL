from typing import Optional, List, Dict
from dynamodb.dynamodb_core import DynamoDbCoreAdapter, PrimaryIndex, GlobalSecondaryIndex
from fields import BaseField, BaseItem, MapModel, MapField, TableDataModel


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


def assign_internal_mapping_from_class(table: BaseTable, class_instance, current_path_elements: Optional[Dict[str, type]] = None):
    if current_path_elements is None:
        current_path_elements = dict()
    output_mapping = dict()

    class_variables = class_instance.__class__.__dict__
    for variable_key, variable_item in class_variables.items():
        try:
            if not isinstance(variable_item, type):
                variable_bases = variable_item.__class__.__bases__
            else:
                variable_bases = variable_item.__bases__

            if BaseItem in variable_bases:
                variable_item: BaseItem
                variable_item._database_path = {**current_path_elements, **{variable_key: variable_item.field_type}}
                variable_item._table = table
                output_mapping[variable_key] = ""

            elif MapField in variable_bases:
                variable_item: MapField
                variable_item._database_path = {**current_path_elements, **{variable_item.field_name: variable_item.field_type}}
                variable_item._table = table
                output_mapping[variable_item.field_name] = assign_internal_mapping_from_class(
                    table=table, class_type=variable_item, current_path_elements=variable_item._database_path
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

