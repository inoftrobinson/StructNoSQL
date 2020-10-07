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


class TestTableOperations(unittest.TestCase):
    def __init__(self, methodName: str):
        super().__init__(methodName=methodName)
        self.users_table = UsersTable()
        self.test_account_id = "5ae5938d-d4b5-41a7-ad33-40f3c1476211"
        self.test_project_id = "defcc77c-1d6d-46a4-8cbe-506d12b824b7"

    def test_get_all_projects(self):
        response_items: Optional[List[dict]] = self.users_table.query(
            key_name="accountId", key_value=self.test_account_id, fields_to_get=["projects"]
        )
        if response_items is not None:
            for item in response_items:
                current_item_projects = item.get("projects", None)
                if current_item_projects is not None:
                    for project_data in current_item_projects.values():
                        print(f"Project : {project_data}")
                        project_name = project_data.get("projectName", None)
                        if project_name is not None:
                            self.assertIn(project_data["projectName"], ["Anvers 1944", "Le con", "test", "test2"])

    def test_get_name_of_one_project(self):
        response_data: Optional[str] = self.users_table.get_single_field_value_from_single_item(
            key_name="accountId", key_value=self.test_account_id,
            field_to_get="projects.{{projectId}}.projectName",
            query_kwargs={"projectId": self.test_project_id}
        )
        self.assertIn(response_data, ["test", "test2"])

    def test_update_project_name(self):
        success = self.users_table.set_update_one_field(
            key_name="accountId", key_value=self.test_account_id,
            target_field="projects.{{projectId}}.projectName",
            query_kwargs={"projectId": self.test_project_id},
            value_to_set="test2"
        )
        self.assertTrue(success)

        response_data: Optional[str] = self.users_table.get_single_field_value_from_single_item(
            key_name="accountId", key_value=self.test_account_id,
            field_to_get="projects.{{projectId}}.projectName",
            query_kwargs={"projectId": self.test_project_id}
        )
        self.assertEqual(response_data, "test2")

    def test_update_entire_project_model(self):
        success = self.users_table.set_update_one_field(
            key_name="accountId", key_value=self.test_account_id,
            target_field="projects.{{projectId}}", value_to_set={"projectName": "test3"},
            query_kwargs={"projectId": self.test_project_id}
        )
        self.assertTrue(success)

        response_data: Optional[str] = self.users_table.get_single_field_value_from_single_item(
            key_name="accountId", key_value=self.test_account_id,
            field_to_get="projects.{{projectId}}.projectName",
            query_kwargs={"projectId": self.test_project_id}
        )
        self.assertEqual(response_data, "test3")
        # todo: allow to set the item of a dict (currently, when doing a query on the projects object,
        #  we will perform an operation of the project map, and not on an individual project item).

if __name__ == '__main__':
    unittest.main()
