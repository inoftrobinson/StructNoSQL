import unittest
from dataclasses import dataclass
from typing import Dict, List, Optional
from StructNoSQL import BaseTable, BaseField, MapModel, MapField, TableDataModel, PrimaryIndex, GlobalSecondaryIndex
from StructNoSQL.practical_logger import message_with_vars


class UsersTableModel(TableDataModel):
    accountId = BaseField(name="accountId", field_type=str)
    class ProjectModel(MapModel):
        projectName = BaseField(name="projectName", field_type=str, required=False)
        class InstancesInfosModel(MapModel):
            ya = BaseField(name="ya", field_type=str)
        instancesInfos = MapField(name="instancesInfos", model=InstancesInfosModel)
    projects = BaseField(name="projects", field_type=Dict[str, ProjectModel], key_name="projectId")


class UsersTable(BaseTable):
    def __init__(self):
        primary_index = PrimaryIndex(hash_key_name="accountId", hash_key_variable_python_type=str)
        globals_secondary_indexes = [
            GlobalSecondaryIndex(hash_key_name="username", hash_key_variable_python_type=str, projection_type="ALL"),
            GlobalSecondaryIndex(hash_key_name="email", hash_key_variable_python_type=str, projection_type="ALL"),
        ]
        super().__init__(table_name="inoft-vocal-engine_accounts-data", region_name="eu-west-2", data_model=UsersTableModel(),
                         primary_index=primary_index, global_secondary_indexes=globals_secondary_indexes, auto_create_table=True)
        self.model = UsersTableModel


class TestTableOperations(unittest.TestCase):
    def __init__(self, methodName: str):
        super().__init__(methodName=methodName)
        self.users_table = UsersTable()

    def test_get_all_projects(self):
        projects: List[dict] = list((
            self.users_table.model.projects.query(key_name="accountId", key_value="5ae5938d-d4b5-41a7-ad33-40f3c1476211").first_value()
        ).values())
        for project in projects:
            print(f"Project : {project}")
            project_name = project.get("projectName", None)
            if project_name is not None:
                self.assertIn(project["projectName"], ["Anvers 1944", "Le con", "test", "test2"])

    def test_get_name_of_one_project(self):
        response_data: Optional[str] = self.users_table.model.projects.dict_item.projectName.query(
            key_name="accountId", key_value="5ae5938d-d4b5-41a7-ad33-40f3c1476211",
            query_kwargs={"projectId": "defcc77c-1d6d-46a4-8cbe-506d12b824b7"}
        ).first_value()
        print(response_data)
        self.assertIn(response_data, ["test", "test2"])

    def test_update_project_name(self):
        response = self.users_table.model.projects.dict_item.projectName.query(
            key_name="accountId", key_value="5ae5938d-d4b5-41a7-ad33-40f3c1476211",
            query_kwargs={"projectId": "defcc77c-1d6d-46a4-8cbe-506d12b824b7"}
        ).set_update("test2")
        self.assertIsNotNone(response)

    def test_update_project_data(self):
        project_data = UsersTableModel.ProjectModel(projectName="test").dict
        print(message_with_vars("Running the update query.", vars_dict={"projectDataModel": project_data}))
        e = self.users_table.model.projects.dict_item
        response = self.users_table.model.projects.dict_item.query(
            key_name="accountId", key_value="5ae5938d-d4b5-41a7-ad33-40f3c1476211",
            query_kwargs={"projectId": "defcc77c-1d6d-46a4-8cbe-506d12b824b7"}
        ).set_update(project_data)
        print(response)
        # todo: allow to set the item of a dict (currently, when doing a query on the projects object,
        #  we will perform an operation of the project map, and not on an individual project item).

if __name__ == '__main__':
    unittest.main()
