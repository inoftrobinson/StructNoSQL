from inspect import signature
from typing import Optional, List, Dict
from dynamodb.dynamodb_core import DynamoDbCoreAdapter, PrimaryIndex, GlobalSecondaryIndex
from fields import BaseField, BaseItem, MapModel, MapField

# todo: migrate from doing my own data validation library, to using Pydantic !

class BaseTable:
    def __init__(self, table_name: str, table_region: str):
        self._internal_mapping = dict()
        # todo: make indexes dynamic
        self._dynamodb_client = DynamoDbCoreAdapter(
            table_name=table_name, region_name=table_region,
            primary_index=PrimaryIndex(hash_key_name="accountId", hash_key_variable_python_type=str),
            global_secondary_indexes=[
                GlobalSecondaryIndex(hash_key_name="username", hash_key_variable_python_type=str, projection_type="ALL"),
                GlobalSecondaryIndex(hash_key_name="email", hash_key_variable_python_type=str, projection_type="ALL"),
            ]
        )
        class_variables = assign_internal_mapping_from_class(table=self, class_type=self.__class__)
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


def assign_internal_mapping_from_class(table: BaseTable, class_type: type, current_path_elements: Optional[Dict[str, type]] = None):
    if current_path_elements is None:
        current_path_elements = dict()
    output_mapping = dict()

    class_variables = class_type.__dict__
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
                variable_item._database_path = {**current_path_elements, **{variable_item.name: variable_item.field_type}}
                variable_item._table = table
                output_mapping[variable_item.name] = assign_internal_mapping_from_class(
                    table=table, class_type=variable_item, current_path_elements=variable_item._database_path
                )
        except Exception as e:
            print(e)

    return output_mapping


class UsersTable(BaseTable):
    accountId = BaseField(name="accountId", field_type=str)
    class ProjectModel(MapModel):
        projectName = BaseField(name="projectName", field_type=str)
        class ProjectInfosModel(MapModel):
            primaryUrl = BaseField(name="primaryUrl", field_type=str)
        projects_infos = MapField(name="projectsInfos", model=ProjectInfosModel)
    projects = MapField(name="projects", model=ProjectModel)






users_table = UsersTable(table_name="inoft-vocal-engine_accounts-data", table_region="eu-west-2")
print(users_table.projects.query(key_name="accountId", key_value="5ae5938d-d4b5-41a7-ad33-40f3c1476211").first_value())
"""users_table.projects.query()
print(users_table.ProjectsModel.ProjectInfos.primaryUrl.post(value="Yolooooooo"))
print(users_table.__class__.__dict__)"""
# print(signature(users_table.ProjectsModel))
# print(users_table.ProjectsModel(projectName="yolo").projectName)
# print(users_table.__class__.__dict__)

